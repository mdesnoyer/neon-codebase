#!/usr/bin/env python

'''
Generates statistics for a customer on A/B tested data.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014

'''
import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

from datetime import datetime
import impala.dbapi
import impala.error
import logging
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

_log = logging.getLogger(__name__)

class ThumbnailStats(object):
    def __init__(loads=None, visible=None, clicks=None, clicks_to_video=None)
        self.loads = loads
        self.visible = visible
        self.clicks = clicks
        self.clicks_to_video = clicks_to_video

def get_time_clause():
    '''Returns the where clause to make sure the results are between the
    start and end times.
    '''
    retval = ''
    if options.start_time is not None:
        start_time = 

def collect_counts:
    '''Grabs the visible and click counts from the database.

    Returns: thumbnail_id -> ThumbnailStats
    '''
    conn = impala.dbapi.connect(host=options.stats_host,
                                port=options.stats_port)
    cursor = conn.cursor()

    _log.info('Getting all the loads, clicks and visible')
    cursor.execute(
    """select thumbnail_id, count(imloadclienttime),
    count(imvisclienttime), count(imclickclienttime) from EventSequences 
    where tai='%s' and mnth >= %i and mnth <= %i
    """)

def main():

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

