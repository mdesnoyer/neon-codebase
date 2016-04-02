#!/usr/bin/env python

'''
Generates statistics for a customer on A/B tested data.

The data is sliced by a potential number of factors.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2016

'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
from datetime import datetime
import dateutil.parser
import logging
import pandas
import re
import stats.cluster
import stats.metrics
from stats import statutils
import utils.neon
from utils.options import options, define

_log = logging.getLogger(__name__)

define("pub_id", default=None, type=str,
       help=("Publisher, in the form of the tracker account id to get the "
             "data for"))
define("impressions", default='views',
       help='Type of metric to consider an impression')
define("conversions", default='clicks',
       help='Type of metric to consider a click')
define("min_impressions", default=500,
       help="Minimum number of impressions for a thumbnail for it to be included.")
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
define("video_ids", default=None, type=str,
       help="File containing video ids to analyze, one per line")
define("output", default=None, type=str,
       help="Output file. If not set, outputs to STDOUT")

define("split_mobile", default=0, type=int,
       help="If 1, split the data by desktop and mobile")
define("page_regex", default=None, type=str,
       help=("An Impala regex that must be matched to the url path "
             "(without host etc) for the event to count. "
             "Data will be split by the first group."))
define("baseline_types", default="default",
       help="Comma separated list of thumbnail type to treat as baseline")
       

def get_video_statuses(video_ids):
    '''Returns a dictionary of video status objects.

    video_id -> VideoStatus
    '''
    return dict([(x.get_id(), x)
                 for x in neondata.VideoStatus.get_many(set(video_ids))])

def get_thumbnail_statuses(thumb_ids):
    '''Returns a dictionary of thumbnail status objects.

    thumb_id -> ThumbnailStatus
    '''
    return dict([(x.get_id(), x)
                 for x in neondata.ThumbnailStatus.get_many(set(thumb_ids))])

def get_key_timepoints(video, video_status, thumb_statuses):
    '''Identifies the key times for each thumb turning having valid data.

    Returns: dict of thumb_id -> (on_timestamp, off_timestamp)
    '''
    retval = {}
    if video_status is None:
        _log.warning('Could not get status of video %s. Using all data'
                     % video.key)
        return retval
    
    # First find the time when the experiment finished
    finish_time = None
    start_time = None
    # If the only entry is a completed experiment, we accept the
    # complete at the beginning.
    if (len(video_status.state_history) == 1 and 
        video_status.state_history[0][1] == neondata.ExperimentState.COMPLETE):
        finish_time = dateutil.parser.parse(video_status.state_history[0][0]
                                            ).strftime('%Y-%m-%d %H:%M:%S')
    else:
        for change_time, new_status in video_status.state_history[1:]:
            if new_status == neondata.ExperimentState.COMPLETE:
                finish_time = dateutil.parser.parse(change_time).strftime(
                    '%Y-%m-%d %H:%M:%S')
                break

    # Now go through each thumbnail and get the latest time there is
    # valid data for it.
    for thumb_id in video.thumbnail_ids:
        thumb_status = thumb_statuses.get(thumb_id, None)
        if thumb_status is None:
            _log.warn_n('Could not get the status of thumb %s' %
                        thumb_id)
            continue

        off_time = None
        on_time = None
        for change_time, serving_frac in thumb_status.serving_history:
            if serving_frac > 0 and on_time is None:
                # The thumbnail was turned on at this time
                on_time = dateutil.parser.parse(change_time).strftime(
                    '%Y-%m-%d %H:%M:%S')
            if serving_frac == 0.0:
                # The thumbnail was turned off at this time
                off_time = dateutil.parser.parse(change_time).strftime(
                    '%Y-%m-%d %H:%M:%S')
                break

        retval[thumb_id] = (on_time, off_time or finish_time)

    return retval

def get_event_data(video_id, key_times, metric, null_metric):
    '''Retrieve from Impala the event counts for the different time periods.

    Inputs:
    video_id - Video id
    key_times - List of key times to query for
    metric - Metric to count
    null_metric - Metric that must be non-null

    Returns:
    
    A pandas DataFrame indexed by the group by fields and columns
    being a list of key_times where the count is for all time less
    than or equal to the key time. Also included is an "all_time"
    column for the counts over all time.
    '''
    groupby_cols = ['thumbnail_id']
    groupby_cols.extend(statutils.get_groupby_select(options.impressions,
                                                     options.page_regex,
                                                     options.split_mobile))
    
    select_cols = ['count(%s) as all_time' % statutils.impala_col_map[metric]]
    select_cols.extend([
        "sum(if(cast(serverTime as timestamp) < '{cur_time}' and {metric} is not null, 1, 0)) as '{cur_time}'".format(
            metric=statutils.impala_col_map[metric],
            cur_time=t)
        for t in key_times if t is not None])
    select_cols.extend(groupby_cols)

    groupby_clauses = ['thumbnail_id']
    groupby_clauses.extend(statutils.get_groupby_clause(options.page_regex,
                                                        options.split_mobile))

    url_clause=''
    if options.page_regex:
        url_clause = (" AND parse_url(imloadpageurl, 'PATH') rlike '%s' " % 
                      options.page_regex)
    
    query = (
        """select
        {select_cols}
        from EventSequences
        where {null_metric} is not NULL and
        tai='{pub_id}' and
        regexp_extract(thumbnail_id, '([A-Za-z0-9]+_[A-Za-z0-9~\\.\\-]+)_',
            1)='{video_id}'
        {url_clause}
        {time_clause}
        group by {groupby_clauses}""".format(
            select_cols=','.join(select_cols),
            null_metric=statutils.impala_col_map[null_metric],
            pub_id=options.pub_id,
            video_id=video_id,
            url_clause=url_clause,
            time_clause=statutils.get_time_clause(options.start_time,
                                                  options.end_time),
            groupby_clauses=','.join(groupby_clauses)))

    conn = statutils.impala_connect()
    cursor = conn.cursor()
    cursor.execute(query)
    
    cols = [metadata[0] for metadata in cursor.description]
    retval = pandas.DataFrame((dict(zip(cols, row))
                               for row in cursor))
    dateRe = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}')
    return retval.set_index(groupby_clauses).sortlevel()

def get_video_stats(imp, conv, thumb_times, base_thumb_id):
    '''Calculate all the stats for a single video.

    Inputs:
    imp - get_event_data DataFrame for the impressions
    conv - get_event_data DataFrame for the conversions
    thumb_times - dict of thumb_id -> (on_timestamp, off_timestamp)
    base_thumb_id - the baseline thumbnail id

    Returns:
    A DataFrame keyed by thumbnail id with columns for the stats
    '''
    if base_thumb_id is None:
        return None
    
    # Build up the stats that have to be counted when both the
    # baseline and this thumbnail was on for experiment purposes.
    slice_stats = {}
    for thumb_id in imp.index.levels[0]:
        if thumb_id == base_thumb_id:
            continue
        
        try:
            times = thumb_times[thumb_id]
            start_col = times[0]
            end_col = times[1]
            if end_col is None:
                end_col = 'all_time'
        except KeyError:
            start_col = None
            end_col = 'all_time'
            
        experiment_imp = imp[end_col]
        experiment_conv = conv[end_col]
        if start_col is not None:
            experiment_imp -= imp[start_col]
            experiment_conv -= conv[start_col]

        try:    
            base_imp = experiment_imp.loc[base_thumb_id]
            base_conv = experiment_conv.loc[base_thumb_id]
            treatment_imp = experiment_imp.loc[thumb_id]
            treatment_conv = experiment_conv.loc[thumb_id]
        except KeyError:
            continue
        
        cur_stats = stats.metrics.calc_thumb_stats(
            base_imp, base_conv, treatment_imp, treatment_conv)
        cur_stats.rename(
            columns = {'extra_conversions': 'xtra_conv_at_sig'},
            inplace = True)
        slice_stats[thumb_id] = cur_stats
        
    vid_stats = pandas.concat(slice_stats.values(), keys=slice_stats.keys(),
                              axis=0)
    vid_stats.index = vid_stats.index.set_names('thumbnail_id', level=0)
    vid_stats = pandas.concat([vid_stats,
                               pandas.Series(imp['all_time'], name='tot_imp'),
                               pandas.Series(conv['all_time'], name='tot_conv')
                               ],
                               axis=1)
    vid_stats['tot_ctr'] = vid_stats['tot_conv'] / vid_stats['tot_imp']
    vid_stats['extra_conversions'] = stats.metrics.calc_extra_conversions(
        vid_stats['tot_conv'], vid_stats['revlift'])
    
    vid_stats['is_base'] = (vid_stats.index.get_level_values('thumbnail_id') 
                            == base_thumb_id)

    return vid_stats

def collect_stats(video_objs, video_statuses, thumb_statuses, thumb_meta):
    '''Build up a stats table.

    Inputs:
    video_objs - List of VideoMetadata objects
    video_statuses - Dictionary of video_id -> VideoStatus objects
    thumb_statuses - Dictionary of thumb_id -> ThumbnailStatus objects

    Outputs:
    pandas DataFrame with an outer index of thumbnail id
    '''
    thumb_stats = [] # List of stats dictionary

    for video in video_objs:
        _log.info('Processing video %s' % video.key)
        thumb_times = get_key_timepoints(video,
                                         video_statuses.get(video.key, None),
                                         thumb_statuses)
                                         
        key_times = reduce(lambda x, y: x|y,
                           [set(x) for x in thumb_times.values()])
        key_times = [x for x in key_times if x is not None]

        imp_data = get_event_data(video.key, key_times, options.impressions,
                                  options.impressions)
        conv_data = get_event_data(video.key, key_times, options.conversions,
                                   options.impressions)
        base_thumb_id = statutils.get_baseline_thumb(
            thumb_meta.loc[thumb_meta['video_id'] == video.key],
            imp_data['all_time'].groupby(level='thumbnail_id').sum(),
            options.baseline_types.split(','),
            min_impressions=options.min_impressions)

        cur_stats = get_video_stats(
            imp_data, conv_data, thumb_times, base_thumb_id)
        if cur_stats is not None:
            thumb_stats.append(cur_stats)

        #if len(thumb_stats) > 10:
        #    break

    return pandas.concat(thumb_stats)

def sort_stats(stats):
    _log.info('Finished collecting the data. Now sorting the table.')
    
    # First group by the index
    stats.sortlevel()

    index_names = stats.index.names
    stats.reset_index(inplace=True)

    # Sort so that the videos with the best lift are first
    sortIdx = stats.groupby('video_id').transform(
        lambda x: x.max()).sort(['lift'], ascending=False).index
    stats = stats.ix[sortIdx]

    # Now sort within each video first by type, then by lift
    stats = stats.groupby('video_id', sort=False).apply(
        lambda x: x.sort(['type', 'lift'], ascending=False))
    stats = stats.set_index(index_names)
    return stats

def main():    
    video_objs = statutils.get_video_objects(statutils.MetricTypes.VIEWS,
                                             options.pub_id,
                                             options.start_time,
                                             options.end_time,
                                             options.start_video_time,
                                             options.end_video_time,
                                             options.video_ids
                                             )
    thumb_meta = statutils.get_thumb_metadata(video_objs)

    video_statuses = get_video_statuses(thumb_meta['video_id'])
    thumb_statuses = get_thumbnail_statuses(thumb_meta.index)

    thumb_stats = collect_stats(video_objs, video_statuses,
                                thumb_statuses, thumb_meta)

    stat_table = pandas.merge(thumb_stats, thumb_meta,
                              how='left', left_index=True,
                              right_index=True)

    # Zero out the non-neon data
    stat_table.loc[stat_table['type'] != 'neon', ['extra_conversions', 'xtra_conv_at_sig']] = float('nan')

    # Set the indices
    groups = stat_table.index.names
    stat_table = stat_table.reset_index()
    stat_table.set_index(['integration_id', 'video_id', 'type', 'rank'] +
                         groups, inplace=True)
    stat_table = sort_stats(stat_table)

    if options.output.endswith('.xls'):
        with pandas.ExcelWriter(options.output, encoding='utf-8') as writer:
            stat_table.to_excel(writer)

    elif options.output.endswith('.csv'):
        stat_table.to_csv(options.output)

    else:
        raise Exception('Unknown output format for %s' % options.output)

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

