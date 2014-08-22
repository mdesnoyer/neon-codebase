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
import utils.monitor

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
        super(BatchProcessManager, self).__init__()
        
        self.cluster = cluster
        self.last_output_path = \
          stats.batch_processor.get_last_sucessful_batch_output(self.cluster)

        self._ready_to_run = threading.Event()
        self._stopped = threading.Event()

        # Number of extra task instances to spin up for the batch process.
        self.n_task_instances = 8 
        self.daemon = True

    def run(self):
        self._ready_to_run.set()
        while not self._stopped.is_set():
            self._ready_to_run.clear()

            # Do initialization to get the internal state of the
            # object to be consistent with the state of the data
            # pipeline.
            try:
                was_running = stats.batch_processor.wait_for_running_batch_job(
                    self.cluster)
                if was_running:
                    self.last_output_path = stats.batch_processor.get_last_sucessful_batch_output(self.cluster)
                    if self.last_output_path is not None:
                        stats.batch_processor.build_impala_tables(
                            self.last_output_path,
                            self.cluster)
            except Exception as e:
                _log.exception('Error finding the running batch job: %s' % e)
                continue

            # Schedule the next run
            threading.Timer(options.batch_period, self._ready_to_run.set).start()

            # Run the job
            _log.info('Running batch process.')
            try:
                cleaned_output_path = "%s/%s" % (
                    options.cleaned_output_path,
                    time.strftime("%Y-%m-%d-%H-%M"))

                self.cluster.change_instance_group_size(
                    'TASK', new_size=self.n_task_instances)
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
                _log.exception('Error running the batch pipeline: %s' % e)
                statemon.state.increment('batch_job_failures')
                statemon.state.last_batch_success = 0

            finally:
                try:
                   if not self._ready_to_run.is_set():
                       self.cluster.change_instance_group_size('TASK',
                                                               new_size=0)
                   utils.monitor.send_statemon_data()
                except Exception as e:
                    _log.exception('Error shrinking task instance group: %s'
                                   % e)
                    statemon.state.increment('cluster_resize_failures')

            if self._ready_to_run.is_set():
                _log.warn('The batch process took a very long time to run. '
                          'Adding a machine to the cluster.')
                try:
                    self.cluster.increment_core_size()
                    self.n_task_instances += 4
                except Exception as e:
                    _log.exception('Error incrementing core instance size %s'
                                   % e)
                    statemon.state.increment('cluster_resize_failures')

            self._ready_to_run.wait()

    def stop(self):
        self._stopped.set()
        self._ready_to_run.set()

    def schedule_run(self):
        self._ready_to_run.set()
            
                

def main():
    _log.info('Looking up cluster %s' % options.cluster_type)
    try:
        cluster = stats.cluster.Cluster(options.cluster_type, 8,
                                        options.cluster_ip)
        cluster.connect()

        batch_processor = BatchProcessManager(cluster)
        atexit.register(batch_processor.stop)
        batch_processor.start()
    except Exception as e:
        _log.exception('Unexpected error on startup: %s' % e)
        raise

    while True:
        try:
            cluster.set_cluster_type(options.cluster_type)
            is_alive = cluster.is_alive()
            statemon.state.cluster_is_alive = 1 if is_alive else 0
            if not is_alive:
                _log.error(
                    'Cluster died. Restarting it and building Impala Tables')
                statemon.state.increment('cluster_deaths')
                cluster.connect()
                if batch_processor.last_output_path is None:
                    _log.error('We could not figure out when the last '
                               'sucessful batch job was, so we cannot '
                               'rebuild the Impala tables')
                else:
                    stats.batch_processor.build_impala_tables(
                        batch_processor.last_output_path,
                        cluster)
                    batch_processor.schedule_run()
            cluster.set_public_ip(options.cluster_ip)
        except Exception as e:
            _log.exception('Unexpected Error: %s' % e)

        time.sleep(60)
            

if __name__ == "__main__":
    utils.neon.InitNeon()
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    main()
