#!/usr/bin/env python
''''
Script that runs one cycle of the hadoop stats batch processing.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.emr.connection
import utils.neon

#logging
import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("emr_cluster", default="Event Stats Server",
       help="Name of the EMR cluster to use")
define("hive_port", default=10004, help="Port to talk to hive on")
define("ssh_key", default="s3://neon-keys/emr-runner.pem",
       help="ssh key used to execute jobs on the master node")
define("resource_manager_port", default=9022,
       help="Port to query the resource manager on")
define("schema_bucket", default="neon-avro-schema",
       help=("Bucket that must contain the compiled schema to define the "
             "tables."))

from utils import statemon
statemon.define("stats_cleaning_job_failures", int)
statemon.define("parquet_table_creation_failure", int)

def main():

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
