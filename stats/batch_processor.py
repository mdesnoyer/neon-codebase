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
from boto.s3.connection import S3Connection
import boto.s3.key
import datetime
import impala.dbapi
import impala.error
import pyhs2
import re
import threading
from thrift import Thrift
import time
import urllib2

#logging
import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("hive_port", default=10000, help="Port to talk to hive on")
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

class NeonDataPipelineException(Exception): pass
class ExecutionError(NeonDataPipelineException): pass
class ImpalaError(ExecutionError): pass
class IncompatibleSchema(NeonDataPipelineException): pass
class TimeoutException(NeonDataPipelineException): pass
class UnexpectedInfo(NeonDataPipelineException): pass

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
        try:
            self.cluster.connect()
            hive_event = '%sHive' % self.event
        except Exception as e:
            _log.error('Error connecting to the cluster %s' % e)
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            return

        # Upload the schema for this table to the s3 bucket
        try:
            s3conn = S3Connection()
            bucket = s3conn.get_bucket(options.schema_bucket)
            key = boto.s3.key.Key(bucket, '%s.avsc' % hive_event)
            key.set_contents_from_filename(
                os.path.join(options.compiled_schema_path,
                             '%s.avsc' % hive_event),
                             replace=True)
        except Exception as e:
            _log.error('Error uploading the schema %s to s3: %s' % 
                       (self.event, e))
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            return

        try:
            # Connect to Impala
            _log.info('Connecting to impala at %s:%s' %
                      (self.cluster.master_ip, options.impala_port))
            impala_conn = impala.dbapi.connect(
                host=self.cluster.master_ip,
                port=options.impala_port)
            
            # Connect to hive
            _log.info('Connecting to hive at %s:%s' %
                      (self.cluster.master_ip, options.hive_port))
            with pyhs2.connect(host=self.cluster.master_ip,
                               port=options.hive_port,
                               authMechanism='PLAIN',
                               user='hadoop',
                               password='',
                               database='default',
                               timeout=5000) as hive_conn:
                with conn.cursor() as hive:
                    try:
                        # Set some parameters
                        hive.execute('SET hive.exec.compress.output=true')
                        hive.execute('SET avro.output.codec=snappy')
                        hive.execute('SET parquet.compression=SNAPPY')
                        hive.execute(
                            'SET hive.exec.dynamic.partition.mode=nonstrict')
                        
        
                        self.status = 'RUNNING'

                        external_table = 'Avro%ss' % self.event
                        _log.info("Registering external %s table with Hive" % 
                                  external_table)
                        hive.execute('DROP TABLE IF EXISTS %s' % 
                                     external_table)
                        hive.execute("""
                        create external table %s
                        ROW FORMAT SERDE 
                        'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
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
                        STORED AS INPUTFORMAT 
                        'parquet.hive.DeprecatedParquetInputFormat' 
                        OUTPUTFORMAT 
                        'parquet.hive.DeprecatedParquetOutputFormat'
                        """ % (parq_table, self._generate_table_definition()))

                        # Building parquet tables takes a lot of
                        # memory, so make sure we give the job enough.
                        hive.execute("SET mapreduce.reduce.memory.mb=8000")
                        hive.execute(
                            "SET mapreduce.reduce.java.opts=-Xmx6400m")
                        hive.execute("SET mapreduce.map.memory.mb=8000")
                        hive.execute("SET mapreduce.map.java.opts=-Xmx6400m")
                        hive.execute("""
                        insert overwrite table %s
                        partition(tai, yr, mnth)
                        select %s, trackerAccountId,
                        year(cast(serverTime as timestamp)),
                        month(cast(serverTime as timestamp)) from %s""" %
                        (parq_table, ','.join(x.name for x in 
                                              self.avro_schema.fields),
                         external_table))
                    finally:
                        hive.execute('reset')

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
            _log.exception("Error executing command")
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

        except Exception as e:
            _log.exception('Unexpected exception when building the impala '
                           'table %s' % self.event)
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

def build_impala_tables(input_path, cluster, timeout=None):
    '''Builds the impala tables.

    Blocks until the tables are built.

    Inputs:
    input_path - The input path, which should be the output of the
                 RawTrackerMR job.
    cluster - A Cluster object for working with the cluster
    timeout - If specified, it will timeout after this number of seconds

    Returns:
    true on sucess
    '''
    _log.info("Building the impala tables")

    if timeout is not None:
        budget_time = datetime.datetime.now() + \
          datetime.timedelta(seconds=timeout)

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
        time_left = None
        if timeout is not None:
            time_left = (budget_time - datetime.datetime.now()).total_seconds()
            if time_left < 0:
                raise TimeoutException()
        thread.join(time_left)
        if thread.is_alive():
            raise TimeoutException()
        if thread.status != 'SUCCESS':
            _log.error("Error building impala table %s. See logs."
                       % thread.event)
            raise ImpalaError("Error building impala table")

    _log.info('Updating table table_build_times')
    impala_conn = impala.dbapi.connect(host=cluster.master_ip,
                                       port=options.impala_port)
    cursor = impala_conn.cursor()
    cursor.execute('create table if not exists table_build_times '
                   '(done_time timestamp) stored as PARQUET')
    cursor.execute("insert into table_build_times (done_time) values ('%s')" %
                   datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

    _log.info('Finished building Impala tables')
    return True

def run_batch_cleaning_job(cluster, input_path, output_path, timeout=None):
    '''Runs the mapreduce job that cleans the raw events.

    The events are output in a format that can be read by hive as an
    external table.

    Inputs:
    input_path - The s3 path for the raw data
    output_path - The output path for the raw data
    timeout - Time in seconds    
    '''
    _log.info("Starting batch event cleaning job done")
    try:
        cluster.run_map_reduce_job(options.mr_jar,
                                   'com.neon.stats.RawTrackerMR',
                                   input_path,
                                   output_path,
                                   map_memory_mb=2048,
                                   timeout=timeout)
    except Exception as e:
        _log.error('Error running the batch cleaning job: %s' % e)
        statemon.state.increment('stats_cleaning_job_failures')
        raise
    
    _log.info("Batch event cleaning job done")

def wait_for_running_batch_job(cluster, sample_period=30):
    '''Blocks until a currently running batch cleaning job is done.

    If there is no currently running job, this returns quickly

    Returns: True if the job was running at some point.
    '''
    found_job = False
    cluster.connect()

    while True:
        response = \
          cluster.query_resource_manager('/ws/v1/cluster/apps?states=RUNNING,NEW,SUBMITTED,ACCEPTED')

        app = _get_last_batch_app(response)
        if app is None:
            return found_job
        found_job = True

        time.sleep(sample_period)

def _get_last_batch_app(rm_response):
    '''Finds the last batch application from the resource manager response.'''

    if rm_response['apps'] is None:
        # There are no apps running
        return None

    last_app = None
    last_started_time = None
    for app in rm_response['apps']['app']:
        if (app['name'] == 'Raw Tracker Data Cleaning' and 
            (last_app is None or last_started_time < app['startedTime'])):
            last_app = app
            last_started_time = app['startedTime']            
            
    if last_app is None:
        return None

    _log.info('The batch job with id %s is in state %s with '
              'progress of %i%%' % 
              (last_app['id'], last_app['state'], last_app['progress']))
    return last_app

def get_last_sucessful_batch_output(cluster):
    '''Determines the last sucessful batch output path.

    Returns: The s3 patch of the last sucessful job, or None if there wasn't one
    '''
    cluster.connect()

    response = cluster.query_resource_manager(
        '/ws/v1/cluster/apps?finalStatus=SUCCEEDED')
    app = _get_last_batch_app(response)

    if app is None:
        return None

    # Check the config on the history server to get the path
    query = ('/ws/v1/history/mapreduce/jobs/job_%s/conf' % 
             re.compile(r'application_(\S+)').search(app['id']).group(1))
    try:
        conf = cluster.query_history_manager(query)
    except urllib2.HTTPError as e:
        _log.warn('Could not get the job history for job %s. HTTP Code %s' %
                  (app['id'], e.code))
        return None

    if not 'conf' in conf:
        raise UnexpectedInfo('Unexpected response from the history server: %s'
                             % conf)
    for prop in conf['conf']['property']:
        if prop['name'] == 'mapreduce.output.fileoutputformat.outputdir':
            _log.info('Found the last sucessful output directory as %s' %
                      prop['value'])
            return prop['value']

    return None
    
