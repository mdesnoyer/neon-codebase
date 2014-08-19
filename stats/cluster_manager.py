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
define("cluster_type", default="video_click_stats",
       help="Name of the EMR cluster to use")
define("input_path", default="s3://neon-tracker-logs-v2/v2.2/*/*/*/*",
       help="Path for the raw input data")
define("cleaned_output_path", default="s3://neon-tracker-logs-v2/cleaned/",
       help="Base path where the cleaned logs will be output")
define("batch_period", default=86400, type=float,
       help='Minimum period in seconds between runs of the batch process.')
define("cluster_ip", default=None, type=str,
       help='Elastic ip to assign to the primary cluster')

from utils import statemon
statemon.define('batch_job_failures', int)
statemon.define('cluster_is_alive', int)
statemon.define('cluster_deaths', int)
statemon.define('cluster_resize_failures', int)
statemon.define('successful_batch_runs', int)
statemon.define('last_batch_success', int)

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

                self.cluster.change_instance_group_size('TASK',
                                                        new_size=4)
                stats.batch_processor.run_batch_cleaning_job(
                    self.cluster, options.input_path,
                    cleaned_output_path)
                stats.batch_processor.build_impala_tables(
                    cleaned_output_path,
                    self.cluster)
                self.last_output_path = cleaned_output_path
                statemon.state.increment('successful_batch_runs')
                statemon.state.last_batch_success = 1
            except Exception as e:
                _log.error('Error running the batch pipeline: %s' % e)
                statemon.state.increment('batch_job_failures')
                statemon.state.last_batch_success = 0

            finally:
                try:
                   self.cluster.change_instance_group_size('TASK',
                                                           new_size=0) 
                except Exception as e:
                    _log.exception('Error shrinking task instance group %s'
                                   % s)
                    statemon.state.increment('cluster_resize_failures')

            if self._ready_to_run.is_set():
                _log.warn('The batch process took a very long time to run. '
                          'Adding a machine to the cluster.')
                try:
                    self.cluster.increment_core_size()
                except Exception as e:
                    _log.exception('Error incrementing core instance size %s'
                                   % e)
                    statemon.state.increment('cluster_resize_failures')
            
                

def main():
    _log.info('Looking up cluster %s' % options.cluster_type)
    try:
        cluster = stats.cluster.Cluster(options.cluster_type, 8,
                                        options.cluster_ip)
        cluster.connect()

        batch_processor = BatchProcessManager(cluster)
        batch_processor.start()
    except Exception as e:
        _log.exception('Unexpected error on startup: %s' % e)
        raise

    while True:
        try:
            self.cluster.set_cluster_type(options.cluster_type)
            is_alive = self.cluster.is_alive()
            statemon.state.cluster_is_alive = 1 if is_alive else 0
            if not is_alive:
                _log.error(
                    'Cluster died. Restarting it and building Impala Tables')
                statemon.state.increment('cluster_deaths')
                cluster.connect()
                if batch_processor.last_output_path is None:
                    _log.error('We could not figure out when the last sucessful '
                               'batch job was, so we cannot rebuild the Impala '
                               'tables')
                else:
                    stats.batch_processor.build_impala_tables(
                        batch_processor.last_output_path,
                        cluster)
            self.cluster.set_public_ip(options.cluster_ip)
        except Exception as e:
            _log.exception('Unexpected Error: %s' % e)

        time.sleep(60)
            

if __name__ == "__main__":
    utils.neon.InitNeon()
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    main()
