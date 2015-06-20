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
import impala.dbapi
import impala.error
import logging
import pandas
import re
import stats.metrics
from stats import statutils
import utils.neon
from utils.options import options, define

define("stats_host", default="127.0.0.1",
        type=str, help="Host to connect to the stats db on.")
define("stats_port", default=21050, type=int,
       help="Port to connect to the stats db on")
define("pub_id", default=None, type=str,
       help=("Publisher, in the form of the tracker account id to get the "
             "data for"))
define("start_time", default=None, type=str,
       help="If set, the start time to pay attention to in ISO UTC format")
define("end_time", default=None, type=str,
       help="If set, the start time to pay attention to in ISO UTC format")
define("output", default=None, type=str,
       help="Output file. If not set, outputs to STDOUT")
define("min_impressions", default=1000,
       help="Minimum number of impressions for a thumbnail for it to be included.")
define("baseline_types", default="centerframe",
       help="Comma separated list of thumbnail type to treat as baseline")
define("do_mobile", default=0, type=int,
       help="Only collect mobile data if 1")

_log = logging.getLogger(__name__)

class MetricTypes:
    LOADS = 'loads'
    VIEWS = 'views'
    CLICKS = 'clicks'
    PLAYS = 'plays'

def connect():
    return impala.dbapi.connect(host=options.stats_host,
                                port=options.stats_port,
                                timeout=600)

def get_video_ids():
    _log.info('Querying for video ids')
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(
    """select distinct regexp_extract(thumbnail_id, 
    '([A-Za-z0-9]+_[A-Za-z0-9\\.\\-]+)_', 1) from imageclicks where 
    thumbnail_id is not NULL and
    tai='%s' %s""" % (options.pub_id, 
                      statutils.get_time_clause(options.start_time,
                                                options.end_time)))

    vidRe = re.compile('[0-9a-zA-Z]+_[0-9a-zA-Z]+')
    retval = [x[0] for x in cursor if vidRe.match(x[0])]
    return retval
    

def collect_stats(thumb_info, video_info,
                  impression_metric=MetricTypes.LOADS,
                  conversion_metric=MetricTypes.CLICKS):
    '''Grabs the stats counts from the database and some calculations.

    Inputs:
    thumb_info - thumbnail_id -> ThumbnailMetadata
    video_info - video_id -> VideoMetadata

    Returns: A panda DataFrame with index of (integration_id, video_id, type,
    rank) and columns of (impression_count, conversion_count, CTR, extra conversions, lift, pvalue)
    '''
    video_data = {} # (integration_id, video_id) => DataFrame with stats

    col_map = {
        MetricTypes.LOADS: 'imloadclienttime',
        MetricTypes.VIEWS: 'imvisclienttime',
        MetricTypes.CLICKS: 'imclickclienttime'}
    
    conn = connect()
    cursor = conn.cursor()

    _log.info('Getting all the %s and %s counts' % (impression_metric,
                                                    conversion_metric))
    if conversion_metric == MetricTypes.PLAYS:
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
            regexp_extract(thumbnail_id, '([A-Za-z0-9]+_[A-Za-z0-9\\.\\-]+)_',
            1) in (%s)
            %s
            %s
            group by thumbnail_id, hr""" %
            (col_map[impression_metric], options.pub_id,
             col_map[impression_metric],
             ','.join(["'%s'" % x for x in video_info.keys()]),
             statutils.get_time_clause(options.start_time,
                                       options.end_time),
             statutils.get_mobile_clause(options.do_mobile)))
    else:
        query = (
            """select 
            cast(floor(servertime/3600)*3600 as timestamp) as hr,
            thumbnail_id, count(%s) as imp, count(%s) as conv from 
            EventSequences
            where tai='%s' and 
            %s is not null and
            regexp_extract(thumbnail_id, '([A-Za-z0-9]+_[A-Za-z0-9\\.\\-]+)_',
            1) in (%s) 
            %s
            %s
            group by thumbnail_id, hr
            """ % (col_map[impression_metric], col_map[conversion_metric],
                   options.pub_id, col_map[impression_metric],
                   ','.join(["'%s'" % x for x in video_info.keys()]),
                   statutils.get_time_clause(options.start_time,
                                             options.end_time),
                   statutils.get_mobile_clause(options.do_mobile)))
    cursor.execute(query)

    impala_cols = [metadata[0] for metadata in cursor.description]
    all_hourly_data = pandas.DataFrame((dict(zip(impala_cols, row))
                                        for row in cursor))

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
            impressions, conversions)

        windowed_impressions = impressions
        windowed_conversions = conversions
        if options.start_time is not None:
            windowed_impressions = impressions[options.start_time:]
            windowed_conversions = conversions[options.start_time:]
        if options.end_time is not None:
            windowed_impressions = impressions[:options.end_time]
            windowed_conversions = conversions[:options.end_time]
        cum_impr = windowed_impressions.cumsum().fillna(method='ffill')
        cum_conv = windowed_conversions.cumsum().fillna(method='ffill')
            
        extra_conv = stats.metrics.calc_extra_conversions(
            windowed_impressions,
            cross_stats['revlift'])

        # Find the baseline thumb
        baseline_types = options.baseline_types.split(',')
        base_thumb = None
        for baseline_type in baseline_types:
            for thumb_id in video.thumbnail_ids:
                cur_thumb = thumb_info[thumb_id]
                if cur_thumb.type == baseline_type:
                    base_thumb = cur_thumb
                    break
            if base_thumb is not None:
                break

        thumb_stats = pandas.DataFrame({
            'impr' : cum_impr.iloc[-1],
            'conv' : cum_conv.iloc[-1],
            'ctr' : cum_conv.iloc[-1] / cum_impr.iloc[-1]})
        thumb_stats = thumb_stats[thumb_stats['impr'] > options.min_impressions]
        thumb_meta_info = dict([
            (x.key,
             {
                 'is_base': False if base_thumb is None else x.key == base_thumb.key,
                 'type' : x.type,
                 'rank': x.rank
                 }) for tid in video.thumbnail_ids 
                 for x in [thumb_info[tid]]])
        thumb_stats = pandas.concat([
            thumb_stats,
            pandas.DataFrame(thumb_meta_info).transpose()],
            axis=1,
            join='inner')

        if base_thumb is not None and base_thumb.key in impressions.columns:
            thumb_stats = pandas.concat([
                thumb_stats,
                cross_stats.minor_xs(base_thumb.key),
                pandas.DataFrame({'extra_conversions':
                                  extra_conv[base_thumb.key]})],
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

def get_video_titles(video_ids):
    '''Returns the video titles for a list of video id'''
    retval = []
    video_datas = neondata.VideoMetadata.get_many(video_ids)

    request_keys = [(x.job_id, x.get_account_id()) for x in video_datas if 
                    x is not None]
    requests = neondata.NeonApiRequest.get_many(request_keys)
    requests = dict([(x.job_id, x) for x in requests if x is not None])
    
    for video_data, video_id in zip(video_datas, video_ids):
        if video_data is None:
            _log.error('Could not find title for video id %s' % video_id)
            retval.append('')
            continue
    
        api_request = requests.get(video_data.job_id, None)
        if api_request is None:
            _log.error('Could not find job for video id %s' % video_id)
            retval.append('')
            continue
        retval.append(api_request.video_title.encode('utf-8'))
    return retval

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
    
        
def main():
    _log.info('Getting metadata about the videos.')
    video_info = neondata.VideoMetadata.get_many(get_video_ids())
    video_info = dict([(x.key, x) for x in video_info if x is not None])

    _log.info('Getting metadata about the thumbnails.')
    thumbnail_info = neondata.ThumbnailMetadata.get_many(
        reduce(lambda x, y: x | y,
               [set(x.thumbnail_ids) for x in video_info.itervalues()]))
    thumbnail_info = dict([(x.key, x) for x in thumbnail_info
                           if x is not None])

    _log.info('Getting urls and video titles')
    titles = get_video_titles([x.video_id for x in thumbnail_info.values() if
	                       x.video_id is not None])
    urls = dict(
        [((video_info[x.video_id].integration_id, x.video_id, x.type, x.rank
           if x.type =='neon' else 0, x.key),
          [x.urls[0] if x.urls is not None else x.url,
           title]) 
         for x, title in zip(thumbnail_info.values(), titles)])
    url_index = pandas.MultiIndex.from_tuples(
        urls.keys(), names=['integration_id', 'video_id', 'type', 'rank', 'thumbnail_id'])
    urls = pandas.DataFrame(
        urls.values(), index=url_index,
        columns=pandas.MultiIndex.from_tuples([('', 'url'), ('', 'title')]))
    urls.sortlevel()
    
    # Collect the count data for the metrics we care about. This will
    # index by integration id, video_id, type and rank
    _log.info('Calculating per video statistics')
    video_stats = {
        #'CTR (Loads)' : collect_stats(thumbnail_info, video_info,
        #                              MetricTypes.LOADS,
        #                              MetricTypes.CLICKS),
        'CTR (Views)' : collect_stats(thumbnail_info, video_info,
                                      MetricTypes.VIEWS,
                                      MetricTypes.CLICKS),
        #'PTR (Loads)' : collect_stats(thumbnail_info, video_info,
        #                              MetricTypes.LOADS,
        #                              MetricTypes.PLAYS),
        #'PTR (Views)' : collect_stats(thumbnail_info, video_info,
        #                              MetricTypes.VIEWS,
        #                              MetricTypes.PLAYS),
        #'VTR' : collect_stats(thumbnail_info, video_info,
        #                      MetricTypes.LOADS,
        #                      MetricTypes.VIEWS),           
        }
    video_data = pandas.concat(video_stats.values(),
                               keys=video_stats.keys(),
                               axis=1)

    video_data = pandas.merge(video_data, urls, how='left',
                              left_index=True, right_index=True, sort=False)
    #video_data = video_data.sortlevel()
    

    _log.info('Calculating aggregate statistics')
    aggregate_sheets = {}
    aggregate_sheets['Overall'] = calculate_aggregate_stats(video_stats)


    
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

