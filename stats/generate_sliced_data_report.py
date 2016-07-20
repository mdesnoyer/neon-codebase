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
from datetime import datetime, timedelta
import dateutil.parser
import hashlib
import logging
import numpy as np
import pandas
import re
import scipy.stats
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
define("cache_dir", default=None, type=str,
       help="Directory for cached results from the cluster")

define("split_mobile", default=0, type=int,
       help="If 1, split the data by desktop and mobile")
define("page_regex", default=None, type=str,
       help=("An Impala regex that must be matched to the url path "
             "(without host etc) for the event to count. "
             "Data will be split by the first group."))
define("host_regex", default=None, type=str,
       help=("An Impala regex to extract a portion of the host."
             "Data will be split by the first group."))
define("baseline_types", default="default",
       help="Comma separated list of thumbnail type to treat as baseline")

define("total_video_conversions", default=None,
       help=("File with lines of <video_id>,<# of conversions> that lists "
             "the total number of video conversions, some of which we may "
             "not know about."))
define("time_block", default=3600, type=int,
       help=("When backsolving when the thumbs turned off, what's the "
             "bucket size in seconds"))
define("sequences_table", default="EventSequences",
       help=("Table where the data is stored"))


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

def get_key_timepoints(video, video_status, thumb_statuses, thumb_info):
    '''Identifies the key times for each thumb turning having valid data
    for calculating the lift. 

    Returns a list of on->off timestamps between possible valid time
    segments. Either end of the time segments can be None, in which
    case it means an unbounded segment.

    Returns: dict of thumb_id -> [(on_timestamp, off_timestamp)]
    '''
    TIME_DELAY = timedelta(minutes=5)

    #  If the video is older than 2015-11-20, then we were not
    #  tracking state properly. So, we have to try and figure out what
    #  the serving was based on the the loads seen.
    vid_date = video.publish_date
    if vid_date is None:
        # Get it from the request object
        request = neondata.NeonApiRequest.get(video.job_id,
                                              video.get_account_id())
        vid_date = request.publish_date
    if vid_date and vid_date < '2015-11-20':
        return estimate_key_timepoints_from_data(video.key, thumb_info)
    
    retval = {}
    if video_status is None:
        _log.warning('Could not get status of video %s. Using all data'
                     % video.key)
        return retval
    
    # First find the time blocks when experiments were happening
    experiment_blocks = []
    cur_block = [None, None]
    for change_time, new_status in video_status.state_history:
        if new_status == neondata.ExperimentState.COMPLETE:
            cur_block[1] = dateutil.parser.parse(change_time)
            experiment_blocks.append(cur_block)
            cur_block = [None, None]
        elif (cur_block[0] is None and 
              new_status == neondata.ExperimentState.RUNNING):
            cur_block[0] = dateutil.parser.parse(change_time)

    if cur_block[0] or cur_block[1]:
        experiment_blocks.append(cur_block)

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
        data_blocks = []
        TIME_EPS = timedelta(seconds=5)
        # Find the times in each experiment block when this thumb was
        # on and has valid data. 
        serving_history = sorted(
            [(dateutil.parser.parse(tm), frac)
             for tm, frac in thumb_status.serving_history])
        for start_experiment, end_experiment in experiment_blocks:
            cur_block = [None, None]
            was_on = False
            for change_time, serving_frac in serving_history:
                if start_experiment is not None:
                    if change_time < start_experiment - TIME_EPS:
                        # This change was in a different experiment block
                        was_on = serving_frac > 0.0
                        continue
                    elif abs(change_time - start_experiment) < TIME_EPS:
                        # We've found the start time of this experiment block
                        cur_block[0] = change_time
                    elif cur_block[0] is None and serving_frac > 0.0:
                        # We haven't found the start before, but this
                        # thumbjust turned on in the middle of the
                        # block
                        if was_on:
                            # It was on when the experiment started
                            cur_block[0] = start_experiment
                        else:
                            cur_block[0] = change_time
                        
                if end_experiment is not None:
                    if change_time > end_experiment:
                        # We're past the end of the experiment
                        if cur_block[1] is None:
                            cur_block[1] = end_experiment
                        break
                    elif abs(change_time - end_experiment) < TIME_EPS:
                        # The change happened at the end of the experiment time
                        cur_block[1] = change_time + TIME_DELAY
                        break
                    elif (serving_frac == 0.0 and (
                            start_experiment is None or
                            change_time > start_experiment - TIME_EPS)):
                        # We're in an experiment and saw this thumb turn off
                        cur_block[1] = change_time
                        break

                was_on = serving_frac > 0.0

            if cur_block[0] or cur_block[1]:
                if cur_block[1]:
                    cur_block[1] += TIME_DELAY
                data_blocks.append([x.strftime('%Y-%m-%d %H:%M:%S') 
                                    if x else None
                                    for x in cur_block])

        if len(data_blocks) > 0:
            retval[thumb_id] = data_blocks
        
    return retval

def estimate_key_timepoints_from_data(video_id, thumb_info):
    '''Looks at the image load data to deduce when each thumbnail
    has valid data.

    Returns: dict of thumb_id -> [(on_timestamp, off_timestamp)]
    '''
    query = '''
      SELECT
      cast(floor(servertime/{block_size})*{block_size} as timestamp) as hr,
      thumbnail_id,
      count({imp_type}) as imp,
      count({conv_type}) as conv
      from {table} where tai='{tai}' and
      {imp_type} is not null and
      servertime is not null and
      thumbnail_id like '{video_id}%'
      group by thumbnail_id, hr
    '''.format(tai=options.pub_id,
               video_id=video_id,
               block_size=options.time_block,
               imp_type=statutils.impala_col_map[options.impressions],
               conv_type=statutils.impala_col_map[options.conversions],
               table=options.sequences_table)
    data = get_query_results(query)

    if data is None:
        return {}

    # Pivot the data so that rows are time and cols are thumbnail id
    imp = data.pivot(index='hr', columns='thumbnail_id', values='imp')
    imp.sort(inplace=True, axis=0)
    imp.fillna(0.0, inplace=True)
    conv = data.pivot(index='hr', columns='thumbnail_id', values='conv')
    conv.sort(inplace=True, axis=0)
    conv.fillna(0.0, inplace=True)
    
    imp_sum = imp.sum(axis=0)
    imp_sum.name = 'imp_sum'

    # Figure out the baseline thumbnail
    base_id = stats.statutils.get_baseline_thumb(thumb_info,
                                                 imp_sum,
                                                 get_baseline_types(),
                                                 options.min_impressions)
    if base_id is None:
        return {}

    
    first_time = str(imp[base_id].dropna().index[0])

    cum_imp = imp.cumsum().fillna(method='ffill')
    cum_conv = conv.cumsum().fillna(method='ffill')
    cum_ctr = cum_conv / cum_imp
    cum_var = (cum_ctr * (1-cum_ctr) / cum_imp)

    # For each thumbnail, find out when we hit statistical
    # significance and grab that time for returning.
    rval = {}
    max_sig_time = None
    MAGIC_FUTURE = datetime(9999,1,1)
    for tid in imp.columns:
        if tid == base_id:
            # Deal with the baseline later
            continue

        if cum_imp[base_id].iloc[-1] == 0 or cum_imp[tid].iloc[-1] == 0:
            rval[tid] = [(None, None)]
            continue

        zscore = (cum_ctr[base_id] - cum_ctr[tid]) / \
          (cum_var[base_id] + cum_var[tid]).apply(np.sqrt)
        p_value = zscore.apply(scipy.stats.norm(0, 1).cdf)
        p_value = p_value.where(p_value > 0.5, 1 - p_value)

        # Find the regions where we have statistical significance
        sig = p_value[(p_value > 0.95) & 
                      (cum_imp[base_id] > options.min_impressions) & 
                      (cum_imp[tid] > options.min_impressions) & 
                      (cum_conv[tid] > 20) &
                      (cum_conv[base_id] > 20)]
        if len(sig) == 0:
            # There isn't statistical significance anywhere so use the
            # final numbers
            rval[tid] = [(first_time, None)]
            max_sig_time = MAGIC_FUTURE
        else:
            end_time = sig.index[0]
            rval[tid] = [(first_time, 
                          str(end_time +
                              timedelta(seconds=options.time_block)))]
            if max_sig_time is None or max_sig_time < end_time:
                max_sig_time = end_time

    # Deal with the baseline, which is going to be the time from start
    # to max_sig_time
    if max_sig_time == MAGIC_FUTURE or max_sig_time is None:
        rval[base_id] = [(first_time, None)]
    else:
        rval[base_id] = [(first_time, str(max_sig_time +
                                    timedelta(seconds=options.time_block)))]

    return rval

def get_baseline_types():
    return options.baseline_types.split(',')

def get_event_data(video_id, key_times, metric, null_metric, end_time):
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
    # Get the time window we need data for
    min_time = None
    if len(key_times) > 0:
        min_time = min(key_times)
    max_time = None
    if end_time is not None:
        if len(key_times) == 0:
            max_time = end_time
        else:
            max_time = max([dateutil.parser.parse(str(x)) for x in 
                            key_times] + [end_time])
        
    
    groupby_cols = ['thumbnail_id']
    groupby_cols.extend(statutils.get_groupby_select(options.impressions,
                                                     options.page_regex,
                                                     options.host_regex,
                                                     options.split_mobile))
    
    select_cols = ['count(%s) as all_time' % statutils.impala_col_map[metric],
                   '%s as window_count' % statutils.get_time_window_count(
                       metric, options.start_time, end_time)]
    select_cols.extend([
        "sum(if(cast(imloadserverTime as timestamp) < '{cur_time}' and {metric} is not null, 1, 0)) as '{cur_time}'".format(
            metric=statutils.impala_col_map[metric],
            cur_time=t)
        for t in key_times if t is not None])
    select_cols.extend(groupby_cols)

    groupby_clauses = ['thumbnail_id']
    groupby_clauses.extend(statutils.get_groupby_clause(options.page_regex,
                                                        options.host_regex,
                                                        options.split_mobile))

    url_clause=''
    if options.page_regex:
        url_clause = (" AND parse_url(imloadpageurl, 'PATH') rlike '%s' " % 
                      options.page_regex)
    
    query = (
        """select
        {select_cols}
        from {table}
        where {null_metric} is not NULL and
        tai='{pub_id}' and
        thumbnail_id like '{video_id}_%'
        {url_clause}
        {time_clause}
        group by {groupby_clauses}""".format(
            select_cols=','.join(select_cols),
            null_metric=statutils.impala_col_map[null_metric],
            pub_id=options.pub_id,
            video_id=video_id,
            url_clause=url_clause,
            time_clause=statutils.get_time_clause(min_time, max_time),
            groupby_clauses=','.join(groupby_clauses),
            table=options.sequences_table))

    data = get_query_results(query)

    if data is None:
        return None

    for clause in groupby_clauses:
        data.loc[data[clause].isnull(), clause] = '' 
    if 'page_type' in data.columns:
        data.loc[data['page_type'] == '', 'page_type'] = '<blank>'
    data = data.groupby(groupby_clauses).agg(np.sum)
    if len(groupby_clauses) > 1:
        data=data.sortlevel()
    return data

def get_query_results(query):
    query_hash = hashlib.md5(query).hexdigest()
    cache_file = 'vidcounts_%s.cache' % query_hash

    if options.cache_dir is not None:
        full_cache_fn = os.path.join(options.cache_dir, cache_file)
        if os.path.exists(full_cache_fn):
            data = pandas.read_pickle(full_cache_fn)
            if len(data) == 0:
                data = None
            return data

    # The query isn't in the cache so do the actual call
    conn = statutils.impala_connect()
    cursor = conn.cursor()
    cursor.execute(query)
    
    cols = [metadata[0] for metadata in cursor.description]
    data = pandas.DataFrame((dict(zip(cols, row))
                               for row in cursor))

    # Write the result to the cache
    if options.cache_dir is not None:
        full_cache_fn = os.path.join(options.cache_dir, cache_file)
        if not os.path.exists(options.cache_dir):
            os.makedirs(options.cache_dir)
        data.to_pickle(full_cache_fn)

    if len(data) == 0:
        data = None
    return data

def get_video_stats(imp, conv, thumb_times, base_thumb_id,
                    thumb_statuses,
                    video_status,
                    total_conversions=None):
    '''Calculate all the stats for a single video.

    Inputs:
    imp - get_event_data DataFrame for the impressions
    conv - get_event_data DataFrame for the conversions
    thumb_times - dict of thumb_id -> (on_timestamp, off_timestamp)
    base_thumb_id - the baseline thumbnail id
    winner_tid - the winner thumbnail id
    total_video_conversions - The total conversions from an external source

    Returns:
    A DataFrame keyed by thumbnail id with columns for the stats
    '''
    if base_thumb_id is None:
        return None
    
    # Build up the stats that have to be counted when both the
    # baseline and this thumbnail was on for experiment purposes.
    slice_stats = {}
    slices_exist = True
    try:
        thumbnail_ids = imp.index.levels[0]
    except AttributeError:
        thumbnail_ids = imp.index
        slices_exist = False
    for thumb_id in thumbnail_ids:
        if thumb_id == base_thumb_id:
            continue

        experiment_blocks = []
        times = thumb_times.get(thumb_id, [(None, 'all_time')])
        for start_col, end_col in times:
            if end_col is None:
                end_col = 'all_time'
            cur_block = pandas.DataFrame({'imp': imp[end_col],
                                          'conv': conv[end_col]})
            if start_col is not None:
                cur_block.imp -= imp[start_col]
                cur_block.conv -= conv[start_col]
            
            if len(cur_block.index.names) == 1 and cur_block.index.name is None:
                cur_block.index.name = 'thumbnail_id'    
            experiment_blocks.append(cur_block)
        experiment_blocks = pandas.concat(experiment_blocks,
                                          keys=range(len(experiment_blocks)))

        # Find the block with the most impressions for this thumb and
        # treat that as the best one to work with.
        blockI = experiment_blocks.groupby(
            level=['thumbnail_id', 0]).sum().loc[thumb_id]['imp'].argmax()
        experiment_counts = experiment_blocks.loc[blockI]

        try:    
            base_imp = experiment_counts['imp'][base_thumb_id]
            base_conv = experiment_counts['conv'][base_thumb_id]
            treatment_imp = experiment_counts['imp'][thumb_id]
            treatment_conv = experiment_counts['conv'][thumb_id]
        except KeyError:
            continue
        
        cur_stats = stats.metrics.calc_thumb_stats(
            base_imp, base_conv, treatment_imp, treatment_conv)
        cur_stats.rename(
            columns = {'extra_conversions': 'xtra_conv_at_sig'},
            inplace = True)
        slice_stats[thumb_id] = cur_stats
        
    if len(slice_stats) == 0:
        return None
    vid_stats = pandas.concat(slice_stats.values(), keys=slice_stats.keys(),
                              axis=0)
    vid_stats.index = vid_stats.index.set_names('thumbnail_id', level=0)
    if not slices_exist:
        vid_stats = vid_stats.reset_index(level=1, drop=True)
    vid_stats = pandas.concat([vid_stats,
                               pandas.Series(imp['window_count'],
                                             name='tot_imp'),
                               pandas.Series(conv['window_count'],
                                             name='tot_conv'),
                               ],
                               axis=1)
    if vid_stats.index.names[0] is None:
        vid_stats.index = vid_stats.index.set_names('thumbnail_id')
    vid_stats['tot_ctr'] = vid_stats['tot_conv'] / vid_stats['tot_imp']

    # Calculate the extra conversions
    tot_conv = vid_stats['tot_conv']
    if total_conversions is not None:
        winner_thumb_id = None
        experiment_state = None
        if video_status is not None:
            winner_thumb_id = video_status.winner_tid
            experiment_state = video_status.experiment_state
            if (experiment_state == neondata.ExperimentState.DISABLED and
                winner_thumb_id is None):
                # See if the experiment was completed and then later
                # disabled (which erases the winner. In that case,
                # look for the thumb with serving frac of 0.99
                if (neondata.ExperimentState.COMPLETE 
                    in [x[1] for x in video_status.state_history[1:]]):
                    max_ctr = 0.0
                    max_ctr_tid = None
                    for tid in vid_stats.index.get_level_values(
                            'thumbnail_id'):
                        tstatus = thumb_statuses.get(tid, None)
                        if (tstatus and 
                            len(tstatus.serving_history) > 0 and
                            0.99 in zip(*tstatus.serving_history)[1]):
                            winner_thumb_id = tid
                            break
                        if tstatus and tstatus.ctr > max_ctr:
                            max_ctr = tstatus.ctr
                            max_ctr_tid = tid
                    if winner_thumb_id is None:
                        # For now, pick the thumb with the highest
                        # lift unless it's the baseline, in which
                        # case, pick that as the winner. TODO(remove):
                        # This is a very optimisitic assumption.
                        max_lift = vid_stats['lift'].max()
                        if max_lift > 0:
                            winner_thumb_id = vid_stats['lift'].argmax()
                        else:
                            winner_thumb_id = base_thumb_id
                        
                        # Pick the highest ctr in the database
                        #winner_thumb_id = max_ctr_tid

                        if winner_thumb_id is None:                       
                            # See if there is a thumb with significantly
                            # more impressions
                            if (imp['all_time'].max() > 
                                0.4 * imp['all_time'].sum()):
                                winner_thumb_id = imp['all_time'].argmax()
                            else:
                                # The highest CTR was probably picked
                                winner_thumb_id = vid_stats['tot_ctr'].argmax()
                 
            
        if winner_thumb_id is not None:
            # All the extra conversions go to the winner
            vid_stats['conv_after_winner'] = pandas.Series(
                {winner_thumb_id: total_conversions - conv['all_time'].sum()})
            
        elif (experiment_state == neondata.ExperimentState.RUNNING and
              total_conversions > 10 * conv['all_time'].sum()):
            # We're still running, but the total conversions is
            # nowhere near what we see, so we have to assuming we're
            # missing something, so ignore the total conversions.
            _log.warn("We're missing a ton (%s%%) of data for running video "
                      "%s" % (float(total_conversions)/conv['all_time'].sum(),
                              neondata.InternalVideoID.from_thumbnail_id(
                                  vid_stats.index[0])))
            vid_stats['conv_after_winner'] = 0.0
            
        elif False:
            # Give the extra conversions based on the serving fracs
            serving_frac = pandas.Series(dict(
                [(x, thumb_statuses.get(x, None).serving_frac 
                  if x in thumb_statuses else 0.0) for x in
                 vid_stats.index.get_level_values('thumbnail_id')]))
            if serving_frac.sum() < 0.95:
                # We're missing some data so proportion based on what
                # we've seen
                serving_frac = conv['all_time'] / float(conv['all_time'].sum())
            else:
                serving_frac = serving_frac / serving_frac.sum()
            
            vid_stats['conv_after_winner'] = serving_frac * (
                total_conversions - conv['all_time'].sum())

        else:
            vid_stats['conv_after_winner'] = 0.0
            
        vid_stats['conv_after_winner'].fillna(0, inplace=True)
        tot_conv = vid_stats['conv_after_winner'] + vid_stats['tot_conv']
        
    vid_stats['extra_conversions'] = stats.metrics.calc_extra_conversions(
        tot_conv, vid_stats['revlift'])
    
    vid_stats['is_base'] = (vid_stats.index.get_level_values('thumbnail_id') 
                            == base_thumb_id)

    return vid_stats

def collect_stats(video_objs, video_statuses, thumb_statuses, thumb_meta,
                  total_video_conversions=None, end_time=None):
    '''Build up a stats table.

    Inputs:
    video_objs - List of VideoMetadata objects
    video_statuses - Dictionary of video_id -> VideoStatus objects
    thumb_statuses - Dictionary of thumb_id -> ThumbnailStatus objects
    total_video_conversions - Dict of video_id -> Total conversions

    Outputs:
    pandas DataFrame with an outer index of thumbnail id
    '''
    thumb_stats = [] # List of stats dictionary

    proc_count = 0
    for video in video_objs:
        _log.info('Processing video %s' % video.key)
        proc_count += 1
        if proc_count % 10 == 0:
            _log.info('Processed %d of %d videos' % 
                      (proc_count, len(video_objs)))

        thumb_info = thumb_meta.loc[thumb_meta['video_id'] == video.key]
            
        thumb_times = get_key_timepoints(video,
                                         video_statuses.get(video.key, None),
                                         thumb_statuses,
                                         thumb_info)

        set_join = lambda x, y: x|y
        key_times = reduce(set_join, [reduce(set_join, [set(y) for y in x],
                                             set([])) 
                                      for x in thumb_times.values()], set([]))
        key_times = [x for x in key_times if x is not None]

        imp_data = get_event_data(video.key, key_times, options.impressions,
                                  options.impressions, end_time)
        if imp_data is None:
            continue
        
        conv_data = get_event_data(video.key, key_times, options.conversions,
                                   options.impressions, end_time)
        if conv_data is None:
            continue
        
        base_thumb_id = statutils.get_baseline_thumb(
            thumb_info,
            imp_data['all_time'].groupby(level='thumbnail_id').sum(),
            get_baseline_types(),
            min_impressions=options.min_impressions)

        vstatus = video_statuses.get(video.key, None)

        tot_conv = None
        if total_video_conversions is not None:
            tot_conv = total_video_conversions.get(video.key, None)
            
        cur_stats = get_video_stats(
            imp_data, conv_data, thumb_times, base_thumb_id,
            thumb_statuses,
            video_statuses.get(video.key, None),
            tot_conv)
        if cur_stats is not None:
            thumb_stats.append(cur_stats)

    return pandas.concat(thumb_stats)

def sort_stats(stats_table, slices):
    _log.info('Finished collecting the data. Now sorting the table.')
    
    # First group by the index
    stats_table.sortlevel()

    index_names = stats_table.index.names

    # Filter out any slices with no clicks
    last_level = index_names.index('type')
    stats_table = stats_table.groupby(level=range(0,last_level)).filter(
        lambda x: np.sum(x['tot_conv']) > 1)

    stats_table.reset_index(inplace=True)

    # Sort so that the videos with the best lift are first
    sortIdx = stats_table.groupby('video_id').transform(
        lambda x: x.max()).sort(['extra_conversions'], ascending=False).index
        
    #lambda x: x.max()).sort(['lift'], ascending=False).index
    stats_table = stats_table.ix[sortIdx]

    # Now sort within each video first by type, then by lift
    stats_table = stats_table.groupby(['video_id'] + slices, sort=False).apply(
        lambda x: x.sort(['type', 'lift'], ascending=False))
    stats_table = stats_table.set_index(index_names)
    return stats_table

def get_total_conversions(fn):
    '''Opens a file that lists the total conversions for each video.

    Format is one per line <video_id>,<conversions>

    Returns:
    Dict of video_id -> conversions
    '''
    if fn is None:
        return None
    
    retval = {}
    with open(fn) as f:
        for line in f:
            fields = line.strip().split(',')
            if len(fields) >= 2:
                retval[fields[0]] = float(fields[1])
    return retval

def get_full_stats_table(end_time):

    video_ids = None
    if options.video_ids:
        _log.info('Using video ids from %s' % video_ids)
        with open(options.video_ids) as f:
            video_ids = [x.strip() for x in f]
                
    total_video_conversions = get_total_conversions(
        options.total_video_conversions)
    if total_video_conversions is not None:
        video_ids = total_video_conversions.keys()
        
    video_objs = statutils.get_video_objects(options.impressions,
                                             options.pub_id,
                                             options.start_time,
                                             end_time,
                                             options.start_video_time,
                                             options.end_video_time,
                                             video_ids,
                                             options.min_impressions
                                             )
    thumb_meta = statutils.get_thumb_metadata(video_objs)

    video_statuses = get_video_statuses(thumb_meta['video_id'])
    thumb_statuses = get_thumbnail_statuses(thumb_meta.index)

    status_table = pandas.merge(
        pandas.DataFrame([
            {'video_id' : neondata.InternalVideoID.from_thumbnail_id(
                x.get_id()),
             'thumbnail_id' : x.get_id(),
             'serving_frac' : x.serving_frac
             } for x in thumb_statuses.values()]),
        pandas.DataFrame([
            {'video_id' : x.get_id(),
            'exp_status' : x.experiment_state}
            for x in video_statuses.values()]),
        on='video_id')
    status_table.set_index('thumbnail_id', inplace=True)
    status_table.drop('video_id', axis=1, inplace=True)

    thumb_meta = pandas.merge(thumb_meta, status_table,
                              how='left', left_index=True,
                              right_index=True)

    thumb_stats = collect_stats(video_objs, video_statuses,
                                thumb_statuses, thumb_meta,
                                total_video_conversions,
                                end_time)

    stat_table = pandas.merge(thumb_stats, thumb_meta,
                              how='left', left_index=True,
                              right_index=True)

    # Zero out the non-neon data
    zero_groups = set(options.baseline_types.split(',')) - set(['neon'])
    stat_table.loc[stat_table['type'].isin(zero_groups), 
                   ['extra_conversions', 'xtra_conv_at_sig']] = float('nan')

    # If there is an assumption of conversions after the winner, adjust the tot_cov column
    if 'conv_after_winner' in stat_table.columns:
        stat_table['conv_before_winner'] = stat_table['tot_conv']
        stat_table['tot_conv'] = stat_table['conv_before_winner'] + stat_table['conv_after_winner']

    # Set the indices
    groups = stat_table.index.names
    slices = [x for x in groups if x != 'thumbnail_id']
    stat_table = stat_table.reset_index()
    stat_table.set_index(['integration_id', 'video_id'] + slices +
                         ['type', 'rank', 'thumbnail_id'], inplace=True)
    return sort_stats(stat_table, slices), slices

def calculate_aggregate_stats(full_table, slices):
    full_table = full_table.reset_index()
    full_table = full_table.set_index(slices + ['video_id'])

    return stats.metrics.calc_aggregate_click_based_stats_from_dataframe(
        full_table).transpose()

def main():  

    sheets = {}
    sheets['Raw Stats']= pandas.DataFrame(
        stats.statutils.calculate_raw_stats(options.pub_id,
                                            options.start_time,
                                            options.end_time))

    full_table, slices = get_full_stats_table(
        sheets['Raw Stats'].loc['end time'][0])

    _log.info('Calculating aggregate stats')
    sheets['Per Video Stats']  = full_table
    sheets['Overall'] = calculate_aggregate_stats(full_table, slices)
    
    #sheets['CMSDB Stats'] = pandas.DataFrame(
    #    stats.statutils.calculate_cmsdb_stats(options.pub_id,
    #                                          options.start_video_time,
    #                                          options.end_video_time))


    if options.output.endswith('.xls'):
        with pandas.ExcelWriter(options.output, encoding='utf-8') as writer:
            for sheet_name, data in sheets.iteritems():
                data.to_excel(writer, sheet_name=sheet_name)

    elif options.output.endswith('.csv'):
        for sheet_name, data in sheets.iteritems():
            splits = options.output.rpartition('.')
            fn = '%s_%s.%s' % (splits[0], sheet_name, splits[1])
            data.to_csv(fn)

    else:
        raise Exception('Unknown output format for %s' % options.output)

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

