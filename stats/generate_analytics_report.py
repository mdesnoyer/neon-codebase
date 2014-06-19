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

from datetime import datetime
import dateutil.parser
import impala.dbapi
import impala.error
import logging
import stats.metrics
import stats.utils
from supportServices import neondata
import utils.neon
from utils.options import options, define

define("stats_host", default="54.197.233.118",
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

_log = logging.getLogger(__name__)

class ThumbnailStats(object):
    def __init__(self, loads=0, views=0, clicks=0,
                 clicks_to_video=0):
        self.loads = loads
        self.views = views
        self.clicks = clicks
        self.clicks_to_video = clicks_to_video

def collect_counts():
    '''Grabs the view and click counts from the database.

    Returns: thumbnail_id -> ThumbnailStats
    '''
    retval = {}
    
    conn = impala.dbapi.connect(host=options.stats_host,
                                port=options.stats_port)
    cursor = conn.cursor()

    _log.info('Getting all the loads, clicks and views')
    query = (
    """select thumbnail_id, count(imloadclienttime),
    count(imvisclienttime), count(imclickclienttime) from EventSequences 
    where tai='%s' and imloadclienttime is not null %s group by thumbnail_id
    """ % (options.pub_id, stats.utils.get_time_clause(options.start_time,
                                                       options.end_time)))
    cursor.execute(query)

    for row in cursor:
        retval[row[0]] = ThumbnailStats(loads=row[1], views=row[2],
                                        clicks=row[3])

    _log.info('Getting all the clicks that led to video plays')
    cursor.execute(
        """select thumbnail_id, count(imclickclienttime) from EventSequences
    where tai='%s' and imloadclienttime is not null and 
    (adplayclienttime is not null or videoplayclienttime is not null) %s
    group by thumbnail_id """ % 
    (options.pub_id,
     stats.utils.get_time_clause(options.start_time, options.end_time)))

    for row in cursor:
        try:
            retval[row[0]].clicks_to_video = row[1]
        except KeyError:
            pass

    return retval

def group_videos(thumbnail_info):
    '''

    Inputs: [ThumbnailMetadata]
    Output: video_id -> [ThumbnailMetadata]
    '''
    retval = {}

    for tinfo in thumbnail_info:
        if tinfo is None:
            continue

        try:
            retval[tinfo.video_id].append(tinfo)
        except KeyError:
            retval[tinfo.video_id] = [tinfo]

    return retval

def get_baseline_thumb(thumbs):
    baseThumb = None
    for thumb in thumbs:
        if thumb.type != 'neon':
            if baseThumb is None or thumb.rank < baseThumb.rank:
                baseThumb = thumb

    return baseThumb

def print_per_video_stats(video_info, counts):
    '''Prints statistics, one per line for each thumb.'''

    retval = [','.join(['Video Id', 'Thumbnail Id', 'Loads', 'Views',
                        'Clicks', 'Videos From Clicks', 'View Rate', 
                        'Extra Views', 'View Lift', 'P Value (View Lift)',
                        'CTR (Loads)', 'Extra Clicks (Loads)', 'Lift (Loads)',
                        'P Value (loads)', 'CTR (Views)',
                        'Extra Clicks (Views)', 'Lift (Views)', 'P Value (Views)',
                        'PTR', 'Extra Plays', 'Play Lift', 'P Value (plays)'])]
    
    for video_id, thumbs in video_info.iteritems():
        # Find the baseline thumbnail
        baseThumb = get_baseline_thumb(thumbs)
        if baseThumb is None:
            _log.error('Could not find baseline thumb for video id %s' 
                       % video_id)
            continue
        baseStats = counts[baseThumb.key]

        for thumb in thumbs:
            curCounts = counts[thumb.key]
            curStats = [video_id, thumb.key, curCounts.loads, curCounts.views, 
                        curCounts.clicks, curCounts.clicks_to_video]
            curStats.extend(stats.metrics.calc_thumb_stats(
                (baseStats.loads, baseStats.views),
                (curCounts.loads, curCounts.views)))
            curStats.extend(stats.metrics.calc_thumb_stats(
                (baseStats.loads, baseStats.clicks),
                (curCounts.loads, curCounts.clicks)))
            curStats.extend(stats.metrics.calc_thumb_stats(
                (baseStats.views, baseStats.clicks),
                (curCounts.views, curCounts.clicks)))
            curStats.extend(stats.metrics.calc_thumb_stats(
                (baseStats.views, baseStats.clicks_to_video),
                (curCounts.views, curCounts.clicks_to_video)))
            retval.append(','.join([str(x) for x in curStats]))

    return '\n'.join(retval)

def get_aggregate_metrics(video_info, counts,
                          impression_func=lambda x: x.views,
                          conversion_func=lambda x: x.clicks):
    # Build up the aggregate stats table
    stats_table = []
    for video_id, thumbs in video_info.iteritems():
        baseThumb = get_baseline_thumb(thumbs)
        if baseThumb is None:
            _log.error('Could not find baseline thumb for video id %s' 
                       % video_id)
            continue
        baseStats = counts[baseThumb.key]

        curCounts = [impression_func(baseStats),
                     conversion_func(baseStats),
                     0,
                     0]
        for thumb in thumbs:
            if thumb.type == 'neon':
                tCounts = counts[thumb.key]
                curCounts[2] += impression_func(tCounts)
                curCounts[3] += conversion_func(tCounts)

        stats_table.append(curCounts)

    # Get the metrics
    return stats.metrics.calc_aggregate_ab_metrics(stats_table)
        
def main():
    counts = collect_counts()
    
    _log.info('Getting metadata about the thumbnails.')
    thumbnail_info = neondata.ThumbnailMetadata.get_many(counts.keys())

    _log.info('Calculating per video statistics')
    video_info = group_videos(thumbnail_info)    
    lines = [print_per_video_stats(video_info, counts)]
    lines.append('')

    _log.info('Calculating aggregate statistics')
    lines.append(','.join(['Metric Type', 'Mean Lift', 'P Value', 'Lower 95%',
                           'Upper 95%', 'Random Effects Error']))
    lines.append(','.join(['View Rate'] +
        [str(x) for x in get_aggregate_metrics(video_info, counts, 
                                               lambda x: x.loads,
                                               lambda x: x.views)]))
    lines.append(','.join(['CTR (Loads)'] +
        [str(x) for x in get_aggregate_metrics(video_info, counts,
                                   lambda x: x.loads,
                                   lambda x: x.clicks)]))
    lines.append(','.join(['CTR (Views)'] +
        [str(x) for x in get_aggregate_metrics(video_info, counts,
                                   lambda x: x.views,
                                   lambda x: x.clicks)]))
    lines.append(','.join(['PTR'] +
        [str(x) for x in get_aggregate_metrics(video_info, counts,
                                   lambda x: x.views,
                                   lambda x: x.clicks_to_video)]))

    
    out_stream = sys.stdout
    if options.output:
        _log.info('Outputting results to: %s' % options.output)
        out_stream = open(options.output, 'w')

    out_stream.write('\n'.join(lines))

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

