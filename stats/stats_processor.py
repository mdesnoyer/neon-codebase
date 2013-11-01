#!/usr/bin/env python
'''Runs all the necessary map reduce jobs to get statistics.


Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import atexit
import logging
from stats.hourly_event_stats_mr import HourlyEventStats
import signal
import time

from utils.options import define, options

define('mr_conf', help='Config file for the map reduce jobs',
       default='./mrjob.conf')
define('input', default=None,
       help=('Glob specifying the input log files. '
             'Can be an s3 glob of the form s3://bucket/file* or '
             'a local glob'))
define('runner', default='local',
       help='Where to run the MapReduce. "emr", "local" or "hadoop"')
define('run_period', default=10800, type=int,
       help='Time in seconds between when the job should be run.')
define('min_new_files', default=1, type=int,
       help='Minimum number of new files in the input in order to run the job.')

_log = logging.getLogger(__name__)

def main():
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, sys.exit)

    job = HourlyEventStats(args=['--conf-path', options.mr_conf,
                                 '-r', options.runner,
                                 options.input])

    known_input_files = 0
    while True:
        _log.info('Looking for new log files to process from %s' % 
                  options.input)
        with job.make_runner() as runner:
            n_files = len([x for x in runner.fs.ls(options.input)])
            if (n_files - known_input_files) >= options.min_new_files:
                _log.info('Running stats processing job')
                runner.run()
                known_input_files = n_files

        time.sleep(60 * options.run_period)

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
