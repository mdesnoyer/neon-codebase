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
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from datetime import datetime, timedelta
import logging
import stats.statutils
import utils.neon
from utils.options import options, define

define("total_video_conversions", default=None,
       help=("File with lines of <video_id>,<# of conversions> that lists "
             "the total number of video conversions, some of which we may "
             "not know about."))
define("start_time", default=None, type=str,
       help=("If set, the time of the earliest data to pay attention to in "
             "ISO UTC format"))
define("end_time", default=None, type=str,
       help=("If set, the time of the latest data to pay attention to in "
             "ISO UTC format"))

_log = logging.getLogger(__name__)

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

def get_videoids_with_data():
    '''Get the list of video ids we have data for.'''
    conn = stats.statutils.impala_connect()
    cursor = conn.cursor()
    query = """select distinct 
    regexp_extract(thumbnail_id, '([A-Za-z0-9]+_[A-Za-z0-9~\\.\\-]+)_', 1)
      as video_id
    from eventsequences where
    tai = '2089095449' and
    imclickservertime is not NULL
    {time_clause}""".format(time_clause = stats.statutils.get_time_clause(
        options.start_time, options.end_time))
    cursor.execute(query)

    return [x[0] for x in cursor]
    

def main():
    total_conversions = get_total_conversions(options.total_video_conversions)

    valid_vids = get_videoids_with_data()

    valid_conversions = [total_conversions.get(x, 0) for x in valid_vids]

    _log.info('There were %i valid video views over %i videos' %
              (sum(valid_conversions), len(valid_conversions)))

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
