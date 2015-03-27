#!/usr/bin/env python
''''
Script to build the impala tables

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import stats.batch_processor
import stats.cluster
import utils.neon
import utils.monitor

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("cluster_type", default="video_click_stats",
       help="Name of the EMR cluster to use")

def main():
    _log.info('Looking up cluster %s' % options.cluster_type)
    cluster = stats.cluster.Cluster(options.cluster_type, 12,
                                    None)
    cluster.connect()

    last_path = stats.batch_processor.get_last_sucessful_batch_output(
        cluster)
    stats.batch_processor.build_impala_tables(last_path, cluster)

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
