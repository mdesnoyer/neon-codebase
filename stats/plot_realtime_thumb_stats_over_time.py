#!/usr/bin/env python

'''
Plots the thumbnail statistics for a given video over time using the realtime
data

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
from datetime import datetime
import dateutil.parser
import happybase
import logging
import matplotlib.pyplot as plt
import numpy
import pandas
from stats import statutils
import stats.metrics
import struct
import utils.neon
from utils.options import options, define
import utils.prod

define("stack_name", default=None,
       help="Name of the stack where the database is")
define("stats_host", default="hbase3",
       help="Name of the stack where the database is")
define("video_id", default=None, type=str,
       help=("Video id to get the data for"))
define("start_time", default=None, type=str,
       help="If set, the start time to pay attention to in ISO UTC format")
define("end_time", default=None, type=str,
       help="If set, the start time to pay attention to in ISO UTC format")

_log = logging.getLogger(__name__)

def get_data():
    _log.info('Looking up the video in the CMSDB')
    video = neondata.VideoMetadata.get(options.video_id)
    thumbs = dict([(x.key, x) for x in 
                   neondata.ThumbnailMetadata.get_many(video.thumbnail_ids)
                   if x is not None])
    
    conn = happybase.Connection(utils.prod.find_host_private_address(
        options.stats_host,
        options.stack_name))
    try:
        table = conn.table('THUMBNAIL_TIMESTAMP_EVENT_COUNTS')
        data = []
        if options.video_id is None:
            raise ValueError('video_id must be set')
        for key, row in table.scan(row_start=options.video_id,
                                   row_stop=options.video_id + 'a',
                                   columns=['evts']):
            thumb_id, garb, hr = key.rpartition('_')
            hr = dateutil.parser.parse(hr)
            row_data =  dict([(k.partition(':')[2],
                               struct.unpack('>q', v)[0])
                               for k, v in row.iteritems()])
            row_data['hr'] = hr
            row_data['thumb_id'] = thumb_id
            try:
                thumb = thumbs[thumb_id]
                row_data['rank'] = thumb.rank
                row_data['type'] = thumb.type
                row_data['label'] = '%s_%s' % (thumb.type, thumb.rank)
            except KeyError:
                pass
            data.append(row_data)
    finally:
        conn.close()

    data = pandas.DataFrame(data)
    data = data.sort(['hr', 'label'])
    if options.start_time:
        data = data[data['hr'] > dateutil.parser.parse(options.start_time)]
    if options.end_time:
        data = data[data['hr'] < dateutil.parser.parse(options.end_time)]

    _log.info('Flipping data to get one data frame for each statistic')
    metrics = {}
    for cur_stat in ['il', 'iv', 'ic', 'vp']:
        if cur_stat in data.columns:
            metrics[cur_stat] = data.pivot(index='hr', columns='label',
                                           values=cur_stat)

    metrics['ctr_load'] = metrics['ic'].divide(metrics['il'])
    metrics['ctr_view'] = metrics['ic'].divide(metrics['iv'])
    metrics['cum_loads'] = metrics['il'].cumsum().fillna(method='ffill')
    metrics['cum_views'] = metrics['iv'].cumsum().fillna(method='ffill')
    metrics['cum_clicks'] = metrics['ic'].cumsum().fillna(method='ffill')
    metrics['cum_ctr_load'] = metrics['cum_clicks'].divide(
        metrics['cum_loads'])
    metrics['cum_ctr_view'] = metrics['cum_clicks'].divide(
        metrics['cum_views'])
    return metrics

def main():
    data = get_data()
    st = stats.metrics.calc_lift_at_first_significant_hour(
        data['iv'], data['ic'])
    print('View Lift: %s' % st['lift'])
    for stat, df in data.iteritems():
        df.plot(title=stat)

    plt.show()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
