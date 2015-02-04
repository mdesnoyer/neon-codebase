#!/usr/bin/env python

'''
Plots the thumbnail statistics for a given video over time.

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
import impala.dbapi
import impala.error
import logging
import matplotlib.pyplot as plt
import numpy
import pandas
from stats import statutils
import utils.neon
from utils.options import options, define

define("stats_host", default="54.210.126.245",
        type=str, help="Host to connect to the stats db on.")
define("stats_port", default=21050, type=int,
       help="Port to connect to the stats db on")
define("video_id", default=None, type=str,
       help=("Video id to get the data for"))
define("start_time", default=None, type=str,
       help="If set, the start time to pay attention to in ISO UTC format")
define("end_time", default=None, type=str,
       help="If set, the start time to pay attention to in ISO UTC format")

_log = logging.getLogger(__name__)

def get_data():
    conn = impala.dbapi.connect(host=options.stats_host,
                                port=options.stats_port)
    cursor = conn.cursor()

    _log.info('Finding the interesting thumbnails for video %s' % 
              options.video_id)
    video_info = neondata.VideoMetadata.get(options.video_id)

    _log.info('Talking to database')
    thumb_data = []
    for thumbnail_id in video_info.thumbnail_ids:
        query = (
            """select cast(floor(imloadservertime/3600)*3600 as timestamp) as hr,
            count(imloadclienttime) as loads,
            count(imvisclienttime) as views,
            count(imclickclienttime) as clicks, 
            count(imclickclienttime)/count(imloadclienttime) as ctr_load,
            count(imclickclienttime)/count(imvisclienttime) as ctr_view
            from EventSequences 
            where imloadclienttime is not null and thumbnail_id = '%s' %s 
            group by hr 
            """ % (thumbnail_id,
                   statutils.get_time_clause(options.start_time,
                                             options.end_time)))
        cursor.execute(query)
        names = [metadata[0] for metadata in cursor.description]
        cur_data = [dict(zip(names, row)) for row in cursor]
        if len(cur_data) > 0:
            thumb_info = neondata.ThumbnailMetadata.get(thumbnail_id)
            label = '%s_%i' % (thumb_info.type, thumb_info.rank)
            thumb_data.append((label,
                               pandas.DataFrame(cur_data,
                                                columns=names)))

    _log.info('Flipping data to get one data frame for each statistic')
    data = {}
    for cur_stat in thumb_data[0][1].columns[1:]:
        cols = []
        for thumb_id, table in thumb_data:
            cols.append(pandas.Series(dict(zip(table['hr'],table[cur_stat])),
                                      name=[thumb_id]))

        data[cur_stat] = pandas.concat(cols, axis=1,
                                       keys=[x.name[0] for x in cols])
    return data

def main():
    for stat, df in get_data().iteritems():
        df.plot(title=stat)

    plt.show()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
