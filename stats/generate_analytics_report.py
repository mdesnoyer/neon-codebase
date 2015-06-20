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

#define("stats_host", default="54.210.126.245",
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

def get_thumbnail_ids():
    _log.info('Querying for thumbnail ids')
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(
    """select distinct thumbnail_id from imageloads where 
    tai='%s' %s""" % (options.pub_id, 
                      statutils.get_time_clause(options.start_time,
                                                  options.end_time)))

    tidRe = re.compile('[0-9a-zA-Z]+_[0-9a-zA-Z]+_[0-9a-zA-Z]+')
    retval = [x[0] for x in cursor if tidRe.match(x[0])]
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
    query_dict = {} # Thumbnail_id -> (impression_count, conversion_count)

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
        """select thumbnail_id, count(%s), 
        sum(cast(imclickclienttime is not null and 
        (adplayclienttime is not null or videoplayclienttime is not null) 
        as int))
        from EventSequences where tai='%s' and %s is not null %s
        group by thumbnail_id""" %
        (col_map[impression_metric], options.pub_id,
         col_map[impression_metric],
         statutils.get_time_clause(options.start_time,
                                     options.end_time)))
    else:
        
        query = (
            """select thumbnail_id, count(%s), count(%s) from 
            EventSequences
            where tai='%s' and %s is not null %s 
            group by thumbnail_id
            """ % (col_map[impression_metric], col_map[conversion_metric],
                   options.pub_id, col_map[impression_metric],
                   statutils.get_time_clause(options.start_time,
                                               options.end_time)))
    cursor.execute(query)

    for row in cursor:
        # Only grab data if there are enough impressions
        if row[1] > options.min_impressions:
            query_dict[row[0]] = (row[1], row[2])

    video_data = {}
    names = ['impr', 'conv', 'CTR', 'Extra Conv', 'Lift',
             'P Value']
    for video_id, type_map in merge_video_stats(thumb_info,
                                                query_dict).iteritems():
        for key, stat in calc_per_video_stats(type_map).items():
            integration_id = 'UKNOWN'
            try:
                integration_id = video_info[video_id].integration_id
            except KeyError: pass
            video_data[(integration_id, video_id, key[0], key[1])] = \
              dict(zip(names, stat))

    index = pandas.MultiIndex.from_tuples(video_data.keys(),
                                          names=['integration_id', 'video_id',
                                                 'type', 'rank'])
    retval= pandas.DataFrame(video_data.values(),
                             index=index,
                             columns=names)
    return retval

def merge_video_stats(thumb_info, counts):
    '''Merges the counts for different thumbnails from a given video by their
    thumbnail type. Keep the neon types separate.

    Inputs:
    thumb_info - thumbnail_id -> ThumbnailMetadata
    counts - thumbnail_id -> (impression, conversion)

    Output:
    video_id ->  {(type, rank) -> [impression, conversion]}
    '''

    retval = {}

    for thumbnail_id, cur_stats in counts.iteritems():
        try:
            tinfo = thumb_info[thumbnail_id]
        except KeyError:
            _log.error('Could not find metadata for thumb: %s' % thumbnail_id)
            continue

        if tinfo.type == 'neon':
            thumb_type = (tinfo.type, tinfo.rank)
        else:
            thumb_type = (tinfo.type, 0)

        try:
            type_map = retval[tinfo.video_id]
        except KeyError:
            type_map = {}
            retval[tinfo.video_id] = type_map

        try:
            stats = type_map[thumb_type]
            stats[0] += cur_stats[0]
            stats[1] += cur_stats[1]
            type_map[thumb_type] = stats
        except KeyError:
            type_map[thumb_type] = list(cur_stats)

    return retval

def calc_per_video_stats(counts):
    '''Calculate the per video stats in a dataframe.

    Inputs:
    counts - (type, rank) -> [impression, conversion]

    Outputs - (type, rank) -> (impression, conversion, ctr, extra conversions, lift, pvalue)
    '''
    retval = {}
    baseline_types = options.baseline_types.split(',')
    baseline = None
    for baseline_type in baseline_types:
        for key, stat in counts.items():
            if key[0] == baseline_type:
                baseline = stat
                break
        if baseline is not None:
            break

    if baseline is None:
        _log.error('Could not find baseline')

    for key, value in counts.items():
        if baseline is None:
            retval[key] = (value[0], value[1], 0.0, 0.0, 0.0, 0.0)
            continue

        value.extend(stats.metrics.calc_thumb_stats(baseline, value[0:2]))
        retval[key] = value

    return retval

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
        retval.append(api_request.video_title)
    return retval

def calculate_aggregate_stats(video_stats):
    '''Calculates the aggregates stats for some videos.

    Inputs:
    video_stats - Dictionary of stat_type -> DataFrame indexed by (integration_id, video_id, type, rank)

    Outputs:
    DataFrame indexed by stat_type on one dimension and stat_name on the other
    '''
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
    _log.info('Getting metadata about the thumbnails.')
    thumbnail_info = neondata.ThumbnailMetadata.get_many(get_thumbnail_ids())
    thumbnail_info = dict([(x.key, x) for x in thumbnail_info
                           if x is not None])

    _log.info('Getting metadata about the videos.')
    video_info = neondata.VideoMetadata.get_many(
        set([x.video_id for x in thumbnail_info.itervalues()]))
    video_info = dict([(x.key, x) for x in video_info if x is not None])

    _log.info('Getting urls and video titles')
    titles = get_video_titles([x.video_id for x in thumbnail_info.values() if
	                       x.video_id is not None])
    urls = dict(
        [((video_info[x.video_id].integration_id, x.video_id, x.type, x.rank
           if x.type =='neon' else 0),
          [x.urls[0] if x.urls is not None else x.url,
           title]) 
         for x, title in zip(thumbnail_info.values(), titles)])
    url_index = pandas.MultiIndex.from_tuples(
        urls.keys(), names=['integration_id', 'video_id', 'type', 'rank'])
    urls = pandas.DataFrame(
        urls.values(), index=url_index,
        columns=pandas.MultiIndex.from_tuples([('', 'url'), ('', 'title')]))
    urls.sortlevel()
    
    # Collect the count data for the metrics we care about. This will
    # index by integration id, video_id, type and rank
    _log.info('Calculating per video statistics')
    video_stats = {
        'CTR (Loads)' : collect_stats(thumbnail_info, video_info,
                                      MetricTypes.LOADS,
                                      MetricTypes.CLICKS),
        'CTR (Views)' : collect_stats(thumbnail_info, video_info,
                                      MetricTypes.VIEWS,
                                      MetricTypes.CLICKS),
        'PTR (Loads)' : collect_stats(thumbnail_info, video_info,
                                      MetricTypes.LOADS,
                                      MetricTypes.PLAYS),
        'PTR (Views)' : collect_stats(thumbnail_info, video_info,
                                      MetricTypes.VIEWS,
                                      MetricTypes.PLAYS),
        'VTR' : collect_stats(thumbnail_info, video_info,
                              MetricTypes.LOADS,
                              MetricTypes.VIEWS),           
        }
    video_data = pandas.concat(video_stats.values(),
                               keys=video_stats.keys(),
                               axis=1).dropna()
    
    video_data = pandas.concat([video_data, urls], join='inner',
                               keys=['data', 'metadata'], axis=1)
    video_data = video_data.sortlevel()
    

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
    
    with pandas.ExcelWriter(options.output) as writer:
        video_data.to_excel(writer, sheet_name='Per Video Stats')
        for sheet_name, data in aggregate_sheets.iteritems():
            data.to_excel(writer, sheet_name=sheet_name)


if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

