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

from boto.emr.connection import EmrConnection
from boto.s3.connection import S3Connection
import contextlib
from hive import ThriftHive
from hive.ttypes import HiveServerException
import json
import paramiko
import re
import subprocess
import tempfile
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import time
import urllib2
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
define("input_path", default="s3://neon-tracker-logs-v2/v2.2/*/*/*/*",
       help="Path for the raw input data")
define("cleaned_output_path", default="s3://neon-tracker-logs-v2/cleaned",
       help="Base path where the cleaned logs will be output")
define("mr_jar", default=None, help="Mapreduce jar")

from utils import statemon
statemon.define("stats_cleaning_job_failures", int)
statemon.define("master_connection_error", int)
statemon.define("impala_table_creation_failure", int)

s3AddressRe = re.compile(r's3://([^/]+)/(\S+)')

class NeonException(Exception): pass
class ClusterInfoError(NeonException): pass
class ClusterConnectionError(NeonException): pass
class ExecutionError(NeonException): pass
class MapReduceError(NeonException): pass

class ClusterInfo():
    def __init__(self):
    conn = EmrConnection()

    # First find the right cluster
    self.cluster_id = None
    for cluster in conn.list_clusters().clusters:
        if cluster.status.state in ['WAITING', 'RUNNING'] and cluster.name == options.emr_cluster:
            self.cluster_id = cluster.id
            break

    if self.cluster_id is None:
        raise ClusterInfoError(
            "Could not find cluster %s" % options.emr_cluster)
    else:
        _log.info("Found cluster name %s with id %s" % (options.emr_cluster,
                                                        self.cluster_id))

    # Now find the master node
    self.master_ip = None
    master_id = conn.describe_jobflow(self.cluster_id).masterinstanceid
    for instance in conn.list_instances(self.cluster_id).instances:
        if instance.ec2instanceid == master_id:
            self.master_ip = instance.privateipaddress

    if self.master_ip is None:
        raise ClusterInfoError("Could not find the master ip")
    _log.info("Found master ip address %s" % self.master_ip)
    

class ClusterSSHConnection:
    '''Class that allows an ssh connection to the master cluster node.'''
    def __init__(self, cluster_info):
        self.cluster_info = cluster_info

        # Grab the ssh key from s3
        self.key_file = tempfile.NamedTemporaryFile('w')
        conn = S3Connection()
        bucket_name, key_name = s3AddressRe.match(options.ssh_key).groups()
        bucket = conn.get_bucket(bucket_name)
        key = bucket.get_key(key_name)
        key.get_contents_to_file(self.key_file)

        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        

    def copy_file(self, local_path, remote_path):
        '''Copies a file from the local path to the cluster.

        '''
        self.client.connect(self.cluster_info.master_ip,
                            username='hadoop',
                            key_filename = self.key_file.name)
        try:
            ftp_client = self.client.open_sftp()
            ftp_client.put(local_path, remote_path)

        finally:
            self.client.close()

    def execute_remote_command(self, cmd):
        '''Executes a command on the master node.

        Returns stdout of the process, or raises an Exception if the
        process failed.
        '''

        self.client.connect(self.cluster_info.master_ip,
                            username='hadoop',
                            key_filename = self.key_file.name)
        stdout_msg = []
        stderr_msg = []
        retcode = None
        try:
            stdin, stdout, stderr self.client.exect_command(cmd)
            for line in stdout:
                stdout_msg.append(line)
            for line in stderr:
                stderr_msg.append(line)
            retcode = stdout.channel.recv_exit_status()
            
        finally:
            self.client.close()

        if retcode != 0:
            raise ExecutionError(
                "Error unning command on the cluster: %s. Stderr was: %s" % 
                (cmd, '\n'.join(stderr_msg)))

        return '\n'.join(stdout_msg)

def RunMapReduceJob(cluster_info, ssh_conn, jar, main_class, input_path,
                    output_path):
    '''Runs a mapreduce job.

    Inputs:
    cluster_info - A ClusterInfo object describing the cluster to run on
    ssh_conn - a ClusterSSHConnection object to connect to the master node
    jar - Path to the jar to run. It should be a jar that packs in all the
          dependencies.
    main_class - Name of the main class in the jar of the job to run
    input_path - The input path of the data
    output_path - The output location for the data

    Returns:
    Returns once the job is done. If the job fails, an exception will be thrown.
    '''
    ssh_conn.copy_file(jar, os.path.basename(jar))

    trackURLRe = re.compile(
        r"Tracking URL: https?://(\S+)/proxy/(\S+)/jobhistory/job/(\S+)")
    stdout = ssh_conn.execute_remote_command(
        ('hadoop jar -D mapreduce.output.fileoutputformat.compress=true '
         '-D avro.output.codec=snappy %s %s %s %s') % 
         (jar, main_class, input_path, output_path))
    url_parse = trackURLRe.search(stdout)
    if not url_parse:
        raise MapReduceError(
            "Could not find the tracking url. Stdout was: %s" % stdout)
    job_id = url_parse.group(3)
    application_id = url_parse.group(2)
    host = url_parse.group(1)

    # Now poll the job status until it is done
    error_count = 0
    while True:
        try:
            response = urllib2.urlopen(
                "http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s" % 
                (host, application_id, job_id))

            data = json.loads(response)

            # TODO(mdesnoyer): Send monitoring data about the job
            if data['state'] == 'SUCCEEDED':
                _log.info('Map reduce job %s complete. Results: %s' % 
                          (main_class, 
                           json.dumps(data, indent=4, sort_keys=True)))
                return
            elif data['state'] in ['FAILED', 'KILLED', 'ERROR', 'KILL_WAIT']:
                msg = 'Map reduce job %s failed: %s' %
                           (main_class,
                            json.dumps(data, indent=4, sort_keys=True))
                _log.error(msg)
                raise MapReduceError(msg)

            error_count = 0

            time.sleep(60)
        except URLError as e:
            _log.exception("Error getting job information: %s" % e)
            statemon.state.increment('master_connection_error')
            error_count = error_count + 1
            if error_count > 5:
                _log.error("Tried 5 times and couldn't get there so stop")
                raise
            time.sleep(5)

class ImpalaTableBuilder(threading.Thread):
    '''Thread that will dispatch and monitor the job to build the impala table.'''
    def __init__(self, base_input_path, cluster_info, event):
        super(ImpalaTableBuilder, self).__init__()
        self.event = event
        self.base_input_path = base_input_path
        self.cluster_info = cluster_info
        self.status = 'INIT'

    def run(self):
        # Create the hive client
        transport = TSocket.TSocket(self.cluster_info.master_ip,
                                    options.hive_port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        hive = ThriftHive.Client(protocol)

        try:
            transport.open()

            # Set some parameters
            hive.execute('SET hive.exec.compress.output=true;')
            hive.execute('SET avro.output.codec=snappy;')
            hive.execute('SET hive.exec.dynamic.partition.mode=nonstrict;')
        
            self.status = 'RUNNING'
            
            # Register the new external table
            external_table = 'Avro%ss' % self.event
            hive.execute('DROP TABLE IF EXISTS %s' % external_table)
            hive.execute(
            """create external table %s
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS
            INPUTFORMAT  
            'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 
            'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            LOCATION '%s/%sHive'
            TBLPROPERTIES (
              'avro.schema.url'='s3://%s/%sHive.avsc'
            );""" % 
            (external_table, self.base_input_path, self.event, 
             options.schema_bucket, self.event))

            # Build the parquet table
        except Thrift.TException as e:
            _log.exception("Error connecting to Hive")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'

        except HiveServerException as e:
            _log.exception("Error excuting command")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
        
            
def main():
    cluster_info = ClusterInfo()

    ssh_conn = ClusterSSHConnection(cluster_info)

    cleaned_output_path = "%s/%s" % (options.cleaned_output_path,
                                     time.strftime("%Y-%m-%d-%H-%M"))
    RunMapReduceJob(cluster_info, ssh_conn,
                    options.mr_jar, 'com.neon.stats.RawTrackerMR',
                    options.input_path, cleaned_output_path)

    threads = []
    for event in ['ImageLoad', 'ImageVisible', 'ImageClick', 'AdPlay',
                  'VideoClick', 'VideoPlay']:
        thread = ImpalaTableBuilder(cleaned_output_path, cluster_info,
                                    event)
        thread.start()
        threads.append(thread)

    # Wait for all of the tables to be built
    for thread in threads:
        thread.join()
        if thread.status != 'SUCCESS':
            _log.error("Error building impala table %s. See logs."
                       % thread.table_name)
            return 1

    return 0

if __name__ == "__main__":
    utils.neon.InitNeon()
    return main()
