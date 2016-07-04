#!/usr/bin/env python
''''
A service that watches the stats cluster and restarts it if necessary.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import code
import os
import signal
import stats.cluster
import subprocess
import time
import utils.neon
import utils.monitor

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define('dag', default='clicklogs', help='Dag whose jobs to clear from the db')
define('task_regex', default='create_table.*',
       help='Regex of the dag tasks to clear')

from utils import statemon
statemon.define('cluster_is_alive', int)
statemon.define('cluster_deaths', int)
statemon.define('tasks_cleared', int, default=1)

def main():

    cluster = stats.cluster.Cluster()
    while True:
        try:
            is_alive = cluster.is_alive()
            statemon.state.cluster_is_alive = 1 if is_alive else 0
            if not is_alive:
                _log.error(
                    'Cluster died. Bringing up a new one')
                statemon.state.increment('cluster_deaths')
                try:
                	_log.info('Pausing the Airflow Dag for the cluster to come up')
                	subprocess.check_output(['airflow', 'pause',
                                             options.dag],
                                             stderr=subprocess.STDOUT,
                        env=os.environ)
                except subprocess.CalledProcessError as e:
                    _log.error('Error pausing the dag: %s' % e.output)

                cluster.connect(os.path.basename(__file__))
                statemon.state.tasks_cleared = 0

            if not statemon.state.tasks_cleared:
                _log.info('Clearing airflow tasks %s' % options.task_regex)
                try:
                    subprocess.check_output(['airflow', 'clear',
                                             '-t', options.task_regex,
                                             '-d',
                                             '-c',
                                             options.dag],
                                             stderr=subprocess.STDOUT,
                        env=os.environ)
                    statemon.state.tasks_cleared = 1
                    _log.info('Airflow jobs cleared successfully')
                    subprocess.check_output(['airflow', 'unpause',
                                             options.dag],
                                             stderr=subprocess.STDOUT,
                        env=os.environ)
                    _log.info('Dag unpaused, set to running')
                except subprocess.CalledProcessError as e:
                    _log.error('Error clearing airflow jobs: %s' % e.output)
                    statemon.state.tasks_cleared = 0
                
        except Exception as e:
            _log.exception('Unexpected Error: %s' % e)

        time.sleep(60)
            

if __name__ == "__main__":
    utils.neon.InitNeon()
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    main()
