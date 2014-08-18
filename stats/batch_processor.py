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
sys.path.append(os.path.join(os.path.dirname(__file__), 'gen-py'))

import avro.schema
import boto.s3.key
import contextlib
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
import impala.dbapi
import impala.error
import json
import paramiko
import re
import socket
import subprocess
import tempfile
import threading
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import time
import urllib2
import urlparse
import utils.neon
import utils.monitor

#logging
import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("hive_port", default=10004, help="Port to talk to hive on")
define("impala_port", default=21050, help="Port to talk to impala on")
define("schema_bucket", default="neon-avro-schema",
       help=("Bucket that must contain the compiled schema to define the "
             "tables."))
define("mr_jar", default=None, type=str, help="Mapreduce jar")
define("compiled_schema_path",
       default=os.path.join(__base_path__, 'schema', 'compiled'),
       help='Path to the bucket of compiled avro schema')

from utils import statemon
statemon.define("stats_cleaning_job_failures", int)
statemon.define("impala_table_creation_failure", int)

s3AddressRe = re.compile(r's3://([^/]+)/(\S+)')

class NeonDataPipelineException(Exception): pass
class ExecutionError(NeonDataPipelineException): pass
class MapReduceError(ExecutionError): pass
class ImpalaError(ExecutionError): pass
class IncompatibleSchema(NeonDataPipelineException): pass

class ImpalaTableBuilder(threading.Thread):
    '''Thread that will dispatch and monitor the job to build the impala table.'''
    def __init__(self, base_input_path, cluster, event):
        super(ImpalaTableBuilder, self).__init__()
        self.event = event
        self.base_input_path = base_input_path
        self.cluster = cluster
        self.status = 'INIT'

        self.avro_schema = avro.schema.parse(open(os.path.join(
            options.compiled_schema_path, "%sHive.avsc" % self.event)).read())

    def run(self):
        self.cluster.connect()
        hive_event = '%sHive' % self.event
        
        # Upload the schema for this table to the s3 bucket
        s3conn = S3Connection()
        bucket = s3conn.get_bucket(options.schema_bucket)
        key = boto.s3.key.Key(bucket, '%s.avsc' % hive_event)
        key.set_contents_from_filename(
            os.path.join(options.compiled_schema_path,
                         '%s.avsc' % hive_event),
                         replace=True)
        
        # Create the hive client
        transport = TSocket.TSocket(self.cluster.master_ip,
                                    options.hive_port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        hive = ThriftHive.Client(protocol)

        try:
            transport.open()

            # Connect to Impala
            impala_conn = impala.dbapi.connect(
                host=self.cluster.master_ip,
                port=options.impala_port)

            # Set some parameters
            hive.execute('SET hive.exec.compress.output=true')
            hive.execute('SET avro.output.codec=snappy')
            hive.execute('SET hive.exec.dynamic.partition.mode=nonstrict')
        
            self.status = 'RUNNING'

            external_table = 'Avro%ss' % self.event
            _log.info("Registering external %s table with Hive" % 
                      external_table)
            hive.execute('DROP TABLE IF EXISTS %s' % external_table)
            hive.execute("""
            create external table %s
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS
            INPUTFORMAT  
            'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 
            'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            LOCATION '%s/%s'
            TBLPROPERTIES (
              'avro.schema.url'='s3://%s/%s.avsc'
            )""" % 
            (external_table, self.base_input_path, hive_event, 
             options.schema_bucket, hive_event))

            parq_table = '%ss' % self.event
            _log.info("Building parquet table %s" % parq_table)
            hive.execute( """
            create table if not exists %s
            (%s)
            partitioned by (tai string, yr int, mnth int)
            ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe' 
            STORED AS INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat' 
            OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
            """ % (parq_table, self._generate_table_definition()))
            
            hive.execute("""
            insert overwrite table %s
            partition(tai, yr, mnth)
            select %s, trackerAccountId, year(cast(serverTime as timestamp)),
            month(cast(serverTime as timestamp)) from %s""" %
            (parq_table, ','.join(x.name for x in self.avro_schema.fields),
             external_table))

            _log.info("Refreshing table %s in Impala" % parq_table)
            impala_cursor = impala_conn.cursor()
            impala_cursor.execute("show tables")
            tables = [x[0] for x in impala_cursor.fetchall()]
            if parq_table.lower() in tables:
                # The table is already there, so we just need to refresh it
                impala_cursor.execute("refresh %s" % parq_table)
                impala_cursor.execute("refresh %s" % external_table)
            else:
                # It's not there, so we need to refresh all the metadata
                impala_cursor.execute('invalidate metadata')

            self.status = 'SUCCESS'
            
        except Thrift.TException as e:
            _log.exception("Error connecting to Hive")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'

        except HiveServerException as e:
            _log.exception("Error excuting command")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'

        except impala.error.Error as e:
            _log.exception("Impala error")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'

        except IncompatibleSchema as e:
            _log.error("Incompatible schema found for event %s: %s" %
                       (self.event, self.avro_schema))
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'

    def _generate_table_definition(self):
        '''Generates this hive table definition based on the avro schema.

        This converts the avro types to hive/parquet types.
        '''
        avro2hive_map = {
            'string' : 'string',
            'boolean' : 'boolean',
            'int' : 'int',
            'long' : 'bigint',
            'float' : 'float',
            'double' : 'double',
            'enum' : 'string'
            }
        cols = []
        for field in self.avro_schema.fields:
            field_type = field.type.type
            if field_type == 'union':
                # We deal with [NULL, T] unions specially
                if len(field.type.schemas) == 2:
                    if field.type.schemas[0].type == 'null':
                        field_type = field.type.schemas[1].type
                    elif field.type.schemas[1].type == 'null':
                        field_type = field.type.schemas[0].type
                    else:
                        raise IncompatibleSchema(
                            'Could not find NULL in the union for field %s' % 
                            field.name)
                else:
                    raise IncompatibleSchema(
                            ('Unions are not valid until they are [T, null]. '
                            'Field: %s') % 
                            field.name)
            try:
                cols.append('%s %s' % (field.name, avro2hive_map[field_type]))
            except KeyError:
                raise IncompatibleSchema(
                    'Cannot use a field %s with avro type %s' %
                    (field.name, field_type))
        return ','.join(cols)

def build_impala_tables(input_path, cluster):
    '''Builds the impala tables.

    Blocks until the tables are built.

    Inputs:
    input_path - The input path, which should be the output of the
                 RawTrackerMR job.
    cluster - A Cluster object for working with the cluster

    Returns:
    true on sucess
    '''
    _log.info("Building the impala tables")

    threads = [] 
    for event in ['ImageLoad', 'ImageVisible',
                  'ImageClick', 'AdPlay', 'VideoPlay', 'VideoViewPercentage',
                  'EventSequence']:
        thread = ImpalaTableBuilder(input_path, cluster, event)
        thread.start()
        threads.append(thread)
        time.sleep(1)

    # Wait for all of the tables to be built
    for thread in threads:
        thread.join()
        if thread.status != 'SUCCESS':
            _log.error("Error building impala table %s. See logs."
                       % thread.event)
            raise ImpalaError("Error building impala table")

    _log.info('Finished building Impala tables')
    return True

def run_batch_cleaning_job(cluster, input_path, output_path):
    '''Runs the mapreduce job that cleans the raw events.

    The events are output in a format that can be read by hive as an
    external table.

    Inputs:
    input_path - The s3 path for the raw data
    output_path - The output path for the raw data    
    '''
    _log.info("Starting batch event cleaning job done")
    try:
        cluster.run_map_reduce_job(options.mr_jar,
                                   'com.neon.stats.RawTrackerMR',
                                   input_path,
                                   output_path)
    except Exception as e:
        _log.error('Error running the batch cleaning job: %s' % e)
        statemon.state.increment('stats_cleaning_job_failures')
        raise
    
    _log.info("Batch event cleaning job done")
    
            
def main():
    cluster = ClusterInfo()

    ssh_conn = ClusterSSHConnection(cluster)

    cleaned_output_path = "%s/%s" % (options.cleaned_output_path,
                                     time.strftime("%Y-%m-%d-%H-%M"))

    try:
        RunMapReduceJob(cluster, ssh_conn,
                        options.mr_jar, 'com.neon.stats.RawTrackerMR',
                        options.input_path, cleaned_output_path)
    except Exception as e:
        _log.exception("Error running stats cleaning job %s" % e)
        statemon.state.increment('stats_cleaning_job_failures')
        raise

    _log.info("Batch Processing done, start job that transfers data to the "
              "parquet table")

    threads = [] 
    for event in ['ImageLoad', 'ImageVisible',
                  'ImageClick', 'AdPlay', 'VideoPlay', 'VideoViewPercentage',
                  'EventSequence']:
        thread = ImpalaTableBuilder(cleaned_output_path, cluster, event)
        thread.start()
        threads.append(thread)
        time.sleep(1)

    # Wait for all of the tables to be built
    for thread in threads:
        thread.join()
        if thread.status != 'SUCCESS':
            _log.error("Error building impala table %s. See logs."
                       % thread.event)
            return 1

    _log.info("Sucess!")
    return 0

if __name__ == "__main__":
    utils.neon.InitNeon()
    try:
        exit(main())
    finally:
        # Explicitly send the statemon data at the end of the process
        utils.monitor.send_statemon_data()
