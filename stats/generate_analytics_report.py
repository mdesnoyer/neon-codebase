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
import dateutil.parser
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
    def __init__(loads=None, visibles=None, clicks=None, clicks_to_video=None)
        self.loads = loads
        self.visibles = visibles
        self.clicks = clicks
        self.clicks_to_video = clicks_to_video

def get_time_clause():
    '''Returns the where clause to make sure the results are between the
    start and end times.
    '''
    clauses = []
    if options.start_time is not None:
        start_time = dateutil.parser.parse(options.start_time)
        clauses.extend([
            'mnth >= %i' % start_time.month,
            'yr >= %i' % start_time.year,
            'serverTime >= %s' % start_time.isoformat()])

    if options.end_time is not None:
        end_time = dateutil.parser.parse(options.end_time)
        clauses.extend([
            'mnth <= %i' % end_time.month,
            'yr <= %i' % end_time.year,
            'serverTime <= %s' % end_time.isoformat()])

    return ' and '.join(clauses)

def collect_counts:
    '''Grabs the visible and click counts from the database.

    Returns: thumbnail_id -> ThumbnailStats
    '''
    retval = {}
    
    conn = impala.dbapi.connect(host=options.stats_host,
                                port=options.stats_port)
    cursor = conn.cursor()

    _log.info('Getting all the loads, clicks and visible')
    cursor.execute(
    """select thumbnail_id, count(imloadclienttime),
    count(imvisclienttime), count(imclickclienttime) from EventSequences 
    where tai='%s' and imloadclienttime is not null and %s
    """ % (options.pub_id, get_time_clause()))

    for row in cursor:
        retval[row[0]] = ThumbnailStats(loads=row[1], visibles=row[2],
                                        clicks=row[3])

    _log.info('Getting all the clicks that led to video plays')
    cursor.execute(
        """select thumbnail_id, count(imclickclienttime) from EventSequences
    where tai='%s' and imloadclienttime is not null and 
    (adplayclienttime is not null or videoplayclienttime is not null) and %s
    """ % (options.pub_id, get_time_clause()))

    for row in cursor:
        try:
            retval[row[0]].clicks_to_video = row[1]
        except KeyError:
            pass

    return retval

def main():
    counts = collect_counts()
    thumbnail_info = neondata.ThumbnailMetadata.get_many(counts.keys())

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

