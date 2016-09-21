#!/usr/bin/env python

'''
Generates statistics for a customer on A/B tested data.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014

'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import copy
from datetime import datetime
import dateutil.parser
import happybase
import logging
import pandas
import re
import stats.cluster
import stats.metrics
from stats import statutils
import struct
import utils.neon
from utils.options import options, define
import utils.prod

define("pub_id", default=None, type=str,
       help=("Publisher, in the form of the tracker account id to get the "
             "data for"))
define("impressions", default='views',
       help='Type of metric to consider an impression')
define("conversions", default='clicks',
       help='Type of metric to consider a click')
define("start_time", default=None, type=str,
       help=("If set, the time of the earliest data to pay attention to in "
             "ISO UTC format"))
define("end_time", default=None, type=str,
       help=("If set, the time of the latest data to pay attention to in "
             "ISO UTC format"))
define("start_video_time", default=None, type=str,
       help="If set, only show videos that were published after this time")
define("end_video_time", default=None, type=str,
       help="If set, only show videos that were published before this time")
define("output", default=None, type=str,
       help="Output file. If not set, outputs to STDOUT")
define("min_impressions", default=1000,
       help="Minimum number of impressions for a thumbnail for it to be included.")
define("baseline_types", default="default",
       help="Comma separated list of thumbnail type to treat as baseline")
define("do_mobile", default=0, type=int,
       help="Only collect mobile data if 1")
define("do_desktop", default=0, type=int,
       help="Only collect desktop data if 1")
define("page_url", default=None, type=str,
       help=('Page url to examine data for. Can include wildcards to get '
             'multiple validi pages'))
define("use_cmsdb_ctrs", default=0, type=int,
       help="If 1, use the CTRS in the cmsdb in the calculations")
define("video_ids", default=None, type=str,
       help="File containing video ids to analyze, one per line")
define("use_realtime_data", default=0, type=int,
       help="If 1, use the realtime data instead of the cleaned Impala data")
define("hbase_host", default="hbase3",
       help="Hostname of the HBase machine")
define("stack_name", default=None, type=str,
       help="Stack name to limit search for hosts to")
define("show_bad_experiment_vids", default=0, type=int,
       help=("If 1, include videos where there either is not a Neon thumb or"
             " there is not a baseline"))

_log = logging.getLogger(__name__)


def get_time_range(video_ids):
    '''For a list of video ids get the (min, max) time range in floats.'''
    conn = statutils.impala_connect()
    cursor = conn.cursor()
    cursor.execute(
        """select min(servertime), max(servertime) 
           from eventsequences where
           tai='%s' and 
           regexp_extract(thumbnail_id, '([A-Za-z0-9]+_[A-Za-z0-9~\\.\\-]+)_',
           1) in (%s)
        """ % (options.pub_id,
               ','.join(["'%s'" % x for x in video_ids])))
    return [datetime.utcfromtimestamp(x) for x in cursor.fetchone()]

def get_hourly_stats_from_impala(video_info, impression_metric,
                                 conversion_metric):
    '''Grabs the stats from Impala

    Returns a pandas DataFrame with columns of hr, imp, conv and thumb_id
    '''

    start_time, end_time = get_time_range(video_info.keys())
    
    conn = statutils.impala_connect()
    cursor = conn.cursor()

    _log.info('Getting all the %s and %s counts' % (impression_metric,
                                                    conversion_metric))
    if conversion_metric == statutils.MetricTypes.PLAYS:
        query = (
            """select 
            cast(floor(servertime/3600)*3600 as timestamp) as hr,
            thumbnail_id,
            count(%s) as imp, 
            sum(cast(imclickclienttime is not null and 
            (adplayclienttime is not null or videoplayclienttime is not null) 
            as int)) as conv,
            from EventSequences where tai='%s' and 
            hr is not null and
            %s is not null and
            regexp_extract(thumbnail_id, '([A-Za-z0-9]+_[A-Za-z0-9~\\.\\-]+)_',
            1) in (%s)
            %s
            %s
            %s
            %s
            group by thumbnail_id, hr""" %
            (statutils.impala_col_map[impression_metric], options.pub_id,
             statutils.impala_col_map[impression_metric],
             ','.join(["'%s'" % x for x in video_info.keys()]),
             statutils.get_time_clause(start_time, end_time),
             statutils.get_mobile_clause(options.do_mobile),
             statutils.get_desktop_clause(options.do_desktop),
             statutils.get_page_clause(options.page_url, impression_metric)))
    else:
        query = (
            """select 
            cast(floor(servertime/3600)*3600 as timestamp) as hr,
            thumbnail_id, count(%s) as imp, count(%s) as conv from 
            EventSequences
            where tai='%s' and 
            %s is not null and
            regexp_extract(thumbnail_id, '([A-Za-z0-9]+_[A-Za-z0-9~\\.\\-]+)_',
            1) in (%s) 
            %s
            %s
            %s
            %s
            group by thumbnail_id, hr
            """ % (statutils.impala_col_map[impression_metric],
                   statutils.impala_col_map[conversion_metric],
                   options.pub_id, statutils.impala_col_map[impression_metric],
                   ','.join(["'%s'" % x for x in video_info.keys()]),
                   statutils.get_time_clause(start_time, end_time),
                   statutils.get_mobile_clause(options.do_mobile),
                   statutils.get_desktop_clause(options.do_desktop),
                   statutils.get_page_clause(options.page_url,
                                             impression_metric)))
    cursor.execute(query)

    impala_cols = [metadata[0] for metadata in cursor.description]
    return pandas.DataFrame((dict(zip(impala_cols, row))
                             for row in cursor))

def get_hourly_stats_from_hbase(video_info, thumbnail_info, impression_metric,
                                conversion_metric):
    
    '''Grabs the stats from Hbase

    Returns a pandas DataFrame with columns of hr, imp, conv and thumbnail_id
    '''
    col_map = {
        statutils.MetricTypes.LOADS: 'il',
        statutils.MetricTypes.VIEWS: 'iv',
        statutils.MetricTypes.CLICKS: 'ic',
        statutils.MetricTypes.PLAYS: 'vp'
        }
    start_time, end_time = get_time_range(video_info.keys())
        
    conn = happybase.Connection(utils.prod.find_host_private_address(
        options.hbase_host,
        options.stack_name))
    try:
        table = conn.table('THUMBNAIL_TIMESTAMP_EVENT_COUNTS')
        data = []
        for thumb_id in thumbnail_info.keys():
            start_key = thumb_id
            if start_time is not None:
                start_key += '_' + start_time.strftime('%Y-%m-%dT%H')
            end_key = thumb_id
            if end_time is not None:
                end_key += '_' + end_time.strftime('%Y-%m-%dT%H')
            end_key += 'a'
            
            for key, row in table.scan(row_start=start_key,
                                       row_stop=end_key,
                                       columns=['evts']):
                garb1, garb, hr = key.rpartition('_')
                hr = datetime.strptime(hr, '%Y-%m-%dT%H')
                cur_counts = dict([(k.partition(':')[2],
                                    struct.unpack('>q', v)[0])
                                    for k, v in row.iteritems()])
                row_data = {
                    'hr': hr,
                    'thumbnail_id': thumb_id,
                    'imp': cur_counts.get(col_map[impression_metric], 0),
                    'conv': cur_counts.get(col_map[conversion_metric], 0)
                    }
                data.append(row_data)
        
    finally:
        conn.close()

    return pandas.DataFrame(data)

def collect_stats(thumb_info, video_info,
                  impression_metric=statutils.MetricTypes.LOADS,
                  conversion_metric=statutils.MetricTypes.CLICKS):
    '''Grabs the stats counts from the database and some calculations.

    Inputs:
    thumb_info - thumbnail_id -> ThumbnailMetadata
    video_info - video_id -> VideoMetadata

    Returns: A panda DataFrame with index of (integration_id, video_id, type,
    rank) and columns of (impression_count, conversion_count, CTR, extra conversions, lift, pvalue)
    '''
    video_data = {} # (integration_id, video_id) => DataFrame with stats

    if options.use_realtime_data:
        all_hourly_data = get_hourly_stats_from_hbase(video_info,
                                                      thumb_info,
                                                      impression_metric,
                                                      conversion_metric)
    else:
        all_hourly_data = get_hourly_stats_from_impala(video_info, 
                                                       impression_metric,
                                                       conversion_metric)

    # Add a column for the video id
    all_hourly_data = pandas.concat([
        all_hourly_data,
        pandas.DataFrame({'video_id': 
                          ['_'.join(x.split('_')[0:2]) 
                           for x in all_hourly_data['thumbnail_id']]})],
        axis=1)

    grouped_hourly_data = all_hourly_data.groupby('video_id')
    for video_id in grouped_hourly_data.groups.keys():
        _log.info('Processing video %s' % video_id)
        video = video_info[video_id]
        hourly_data = grouped_hourly_data.get_group(video_id)

        impressions = hourly_data.pivot(index='hr', columns='thumbnail_id',
                                        values='imp')
        conversions = hourly_data.pivot(index='hr', columns='thumbnail_id',
                                        values='conv')
        
        cross_stats = stats.metrics.calc_lift_at_first_significant_hour(
            impressions, conversions, neondata.VideoStatus.get(video_id),
            neondata.ThumbnailStatus.get_many(impressions.columns),
            options.use_cmsdb_ctrs)

        windowed_impressions = impressions
        windowed_conversions = conversions
        if options.start_time is not None:
            start_time = dateutil.parser.parse(options.start_time)
            windowed_impressions = impressions[start_time:]
            windowed_conversions = conversions[start_time:]
        if options.end_time is not None:
            end_time = dateutil.parser.parse(options.end_time)
            windowed_impressions = windowed_impressions[:end_time]
            windowed_conversions = windowed_conversions[:end_time]
        if len(windowed_impressions) == 0 or len(windowed_conversions) == 0:
            continue
        cum_impr = windowed_impressions.cumsum().fillna(method='ffill')
        cum_conv = windowed_conversions.cumsum().fillna(method='ffill')
            
        extra_conv = stats.metrics.calc_extra_conversions(
            windowed_conversions,
            cross_stats['revlift'])

        # Find the baseline thumb
        cum_impr_all = impressions.cumsum().fillna(method='ffill')
        base_thumb_id = statutils.get_baseline_thumb(
            thumb_info,
            cum_impr_all.iloc[-1],
            options.baseline_types.split(','),
            options.min_impressions)
        found_neon = any(thumb_info['type'] == 'neon')

        thumb_stats = pandas.DataFrame({
            'impr' : cum_impr.iloc[-1],
            'conv' : cum_conv.iloc[-1],
            'ctr' : cum_conv.iloc[-1] / cum_impr.iloc[-1]})
        thumb_stats = thumb_stats[thumb_stats['impr'] > options.min_impressions]

        thumb_meta_info = thumb_info[thumb_info['video_id'] == video_id]
        thumb_info['is_base'] = thumb_meta_info.index == base_thumb_id
        thumb_stats = pandas.concat([
            thumb_stats,
            thumb_meta_info],
            axis=1,
            join='inner')

        if base_thumb_id in impressions.columns:
            thumb_stats = pandas.concat([
                thumb_stats,
                cross_stats.minor_xs(base_thumb_id),
                pandas.DataFrame({'extra_conversions':
                                  extra_conv[base_thumb_id]})],
                axis=1,
                join='inner')

        # Put the type and rank columns in the index
        subcols = thumb_stats[[x for x in thumb_stats.columns if x not in 
                               ['type', 'rank']]]
        if len(subcols) == 0:
            continue
        
        idx = pandas.pandas.MultiIndex.from_tuples(
            [x for x in zip(*(thumb_stats['type'], thumb_stats['rank'],
                              thumb_stats.index))],
            names=['type', 'rank', 'thumbnail_id'])
        thumb_stats = pandas.DataFrame(
            subcols.values,
            columns=subcols.columns,
            index=idx).sortlevel()

        video_data[(video.integration_id, video_id)] = thumb_stats

    results = pandas.concat(video_data.itervalues(),
                            keys=video_data.keys()).sortlevel()
    index_names = [u'integration_id', u'video_id', u'type', u'rank',
                   u'thumbnail_id']
    results.index.names = index_names
    results.reset_index(inplace=True)

    # Sort so that the videos with the best lift are first
    sortIdx = results.groupby('video_id').transform(lambda x: x.max()).sort(
        ['lift', 'thumbnail_id'], ascending=False).index
    results = results.ix[sortIdx]

    # Now sort within each video first by type, then by lift
    results = results.groupby('video_id', sort=False).apply(
        lambda x: x.sort(['type', 'lift'], ascending=False))
    results = results.set_index(index_names)
    return results

def calculate_aggregate_stats(video_stats):
    '''Calculates the aggregates stats for some videos.

    Inputs:
    video_stats - Dictionary of stat_type -> DataFrame indexed by (integration_id, video_id, type, rank)

    Outputs:
    DataFrame indexed by stat_type on one dimension and stat_name on the other
    '''
    agg_data = {}

    for stat_name, video_stat in video_stats.items():
        # Calculate the click based stats
        click_stats = stats.metrics.calc_aggregate_click_based_stats_from_dataframe(video_stat)

        agg_data[stat_name] = click_stats

    agg_data = pandas.DataFrame(agg_data)
    return agg_data

    # Old code below here.
    agg_stat_names = ['Mean Lift', 'P Value', 'Lower 95%',
                      'Upper 95%', 'Random Effects Error']
    agg_data = {}
    baseline_types = options.baseline_types.split(',')

    for stat_name, video_stat in video_stats.items():
        video_stat = video_stat.sortlevel()
        
        # Grab the load & click columns
        agg_counts = video_stat.loc[:, ['impr', 'conv']].copy()

        # Aggregate all the neon counts
        agg_counts = agg_counts.groupby(level=[0,1,2]).sum().fillna(0)

        # Reshape the data into the format so that each row is
        # <base impressions>,<base conversions>,
        # <acting impressions>,<acting conversions>
        agg_counts = agg_counts.unstack().swaplevel(0, 1, axis=1).sortlevel(
            axis=1)
        baseline_counts = agg_counts[baseline_types[0]]
        for typ in baseline_types[1:]:
            baseline_counts = baseline_counts.fillna(agg_counts[typ])
        agg_counts = pandas.concat(
            [baseline_counts, agg_counts['neon']],
            axis=1, join='inner').dropna()

        # Calculate the ab metrics
        cur_averages = dict(
            zip([('Video Based', x) for x in agg_stat_names],
                stats.metrics.calc_aggregate_ab_metrics(
                agg_counts.as_matrix())))
        cur_averages.update(dict(
            zip([('Click Based', x) for x in agg_stat_names[0:4]],
                stats.metrics.calc_aggregate_click_based_metrics(
                    agg_counts.as_matrix()))))

        index = pandas.MultiIndex.from_tuples(cur_averages.keys(),
                                              names=['Type', 'Stat'])
        agg_data[stat_name] = pandas.Series(cur_averages.values(),
                                            index=index)

    agg_data = pandas.DataFrame(agg_data)
    agg_data = agg_data.sortlevel()
    return agg_data

def calculate_cmsdb_stats():
    _log.info('Getting some stats from the CMSDB')
    api_key, typ = neondata.TrackerAccountIDMapper.get_neon_account_id(
        options.pub_id)

    videos = list(neondata.NeonUserAccount(
        None, api_key=api_key).iterate_all_videos())

    videos = statutils.filter_video_objects(videos, options.start_video_time,
                                            options.end_video_time)

    return pandas.Series({
        'Video Counts' : len(videos),
        'Total Video Time (s)' : sum([x.duration for x in videos
                                      if x.duration is not None])
        })
    
        
def main():    
    _log.info('Getting metadata about the videos.')
    video_info = statutils.get_video_objects(statutils.MetricTypes.VIEWS,
                                             options.pub_id,
                                             options.start_time,
                                             options.end_time,
                                             options.start_video_time,
                                             options.end_video_time,
                                             options.video_ids
                                             )
    video_info = dict([(x.key, x) for x in video_info if x is not None])

    _log.info('Getting metadata about the thumbnails.')
    thumbnail_info = neondata.ThumbnailMetadata.get_many(
        reduce(lambda x, y: x | y,
               [set(x.thumbnail_ids) for x in video_info.itervalues()], []))

    thumbnail_info = statutils.get_thumb_metadata(video_info.values())
    thumbnail_info = thumbnail_info.set_index(['integration_id', 'video_id',
                                               'type',
                                               'rank', 'thumbnail_id'])
    thumbnail_info.sortlevel()
    
    # Collect the count data for the metrics we care about. This will
    # index by integration id, video_id, type and rank
    _log.info('Calculating per video statistics')
    video_stats = {
        'CTR' : collect_stats(thumbnail_info, video_info,
                              options.impressions,
                              options.conversions),
        #'CTR (Loads)' : collect_stats(thumbnail_info, video_info,
        #                              statutils.MetricTypes.LOADS,
        #                              statutils.MetricTypes.CLICKS),
        #'CTR (Views)' : collect_stats(thumbnail_info, video_info,
        #                              statutils.MetricTypes.VIEWS,
        #                              statutils.MetricTypes.CLICKS),
        #'PTR (Loads)' : collect_stats(thumbnail_info, video_info,
        #                              statutils.MetricTypes.LOADS,
        #                              statutils.MetricTypes.PLAYS),
        #'PTR (Views)' : collect_stats(thumbnail_info, video_info,
        #                              statutils.MetricTypes.VIEWS,
        #                              statutils.MetricTypes.PLAYS),
        #'VTR' : collect_stats(thumbnail_info, video_info,
        #                      statutils.MetricTypes.LOADS,
        #                      statutils.MetricTypes.VIEWS),           
        }
    video_data = pandas.concat(video_stats.values(),
                               keys=video_stats.keys(),
                               axis=1)

    video_data = pandas.merge(video_data, thumbnail_info, how='left',
                              left_index=True, right_index=True, sort=False)
    #video_data = video_data.sortlevel()
    

    _log.info('Calculating aggregate statistics')
    aggregate_sheets = {}
    aggregate_sheets['Overall'] = calculate_aggregate_stats(video_stats)
    aggregate_sheets['Raw Stats']= pandas.DataFrame(stats.statutils.calculate_raw_stats(options.start_time, options.end_time()))
    aggregate_sheets['CMSDB Stats'] = pandas.DataFrame(calculate_cmsdb_stats())


    
    # TODO(mdesnoyer): Figure out why this doesn't work with multiple
    # base types. It seems like the data isn't being copied directly.
    #integration_stats = {}
    #for stat_name, data_frame in video_stats.iteritems():
    #    for integration_id, data in data_frame.groupby(level=[0]):
    #        if integration_id not in integration_stats:
    #            integration_stats[integration_id] = {}
    # 
    #        integration_stats[integration_id][stat_name] = data
    #
    #for integration_id, data_dict in integration_stats.iteritems():
    #    try:
    #        aggregate_sheets['Aggregate %s' % integration_id] = \
    #          calculate_aggregate_stats(data_dict)
    #    except Exception as e:
    #        _log.exception('Error: %s' % e)

    if options.output.endswith('.xls'):
        with pandas.ExcelWriter(options.output, encoding='utf-8') as writer:
            video_data.to_excel(writer, sheet_name='Per Video Stats')
            for sheet_name, data in aggregate_sheets.iteritems():
                data.to_excel(writer, sheet_name=sheet_name)
        
    elif options.output.endswith('.csv'):
        video_data.to_csv(options.output)
        for sheet_name, data in aggregate_sheets.iteritems():
            splits = options.output.rpartition('.')
            fn = '%s_%s.%s' % (splits[0], sheet_name, splits[1])
            data.to_csv(fn)
    else:
        raise Exception('Unknown output format for %s' % options.output)



if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

