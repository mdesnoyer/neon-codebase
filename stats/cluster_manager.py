#!/usr/bin/env python
''''
A service that watches the stats cluster, keeps it up, and runs batch
processing jobs as necessary.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
import signal
import stats.batch_processor
import stats.cluster
import time
import threading
import utils.neon

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("cluster_name", default="Event Stats Server",
       help="Name of the EMR cluster to use")
define("input_path", default="s3://neon-tracker-logs-v2/v2.2/*/*/*/*",
       help="Path for the raw input data")
define("cleaned_output_path", default="s3://neon-tracker-logs-v2/cleaned/",
       help="Base path where the cleaned logs will be output")
define("batch_period", default=86400, type=float,
       help='Minimum period in seconds between runs of the batch process.')

from utils import statemon
statemon.define('batch_job_failures', int)

class BatchProcessManager(threading.Thread):
    '''Thread that will manage the batch process runs.'''

    def __init__(self, cluster):

        self.cluster = cluster
        self.last_output_path = None

        self._ready_to_run = threading.Event()
        self.daemon = True

    def run(self):
        self._ready_to_run.set()
        while True:
            self._ready_to_run.wait()
            self._ready_to_run.clear()

            # Schedule the next run
            threading.Timer(options.batch_period, self._ready_to_run.set)

            # Run the job
            try:
                cleaned_output_path = "%s/%s" % (
                    options.cleaned_output_path,
                    time.strftime("%Y-%m-%d-%H-%M"))
                stats.batch_processor.run_batch_cleaning_job(
                    self.cluster, options.input_path,
                    cleaned_output_path)
                stats.batch_processor.build_impala_tables(
                    cleaned_output_path,
                    self.cluster)
            except Exception as e:
                _log.error('Error running the batch pipeline: %s' % e)
                statemon.state.increment('batch_job_failures')

            if self._ready_to_run.is_set():
                _log.warn('The batch process took a very long time to run. '
                          'We need more resources in the cluster.')
            
                

def main():
    

if __name__ == "__main__":
    utils.neon.InitNeon()
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    main()
