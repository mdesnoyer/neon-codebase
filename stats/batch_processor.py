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
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
import impala.dbapi
import impala.error
import re
import threading
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import time
import urllib2
import math
from hdfs import InsecureClient

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
statemon.define("hbase_table_load_failure", int)

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
        self._stopped = threading.Event()

        self.avro_schema = avro.schema.parse(open(os.path.join(
            options.compiled_schema_path, "%sHive.avsc" % self.event)).read())

    def stop(self):
        self._stopped.set()

    def run(self):
        self._stopped.clear()
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
            # Connect to hive
            _log.debug('Connecting to hive at %s:%s' %
                      (self.cluster.master_ip, options.hive_port))
            transport = TSocket.TSocket(self.cluster.master_ip,
                                        options.hive_port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            hive = ThriftHive.Client(protocol)
            transport.open()
            
            # Connect to Impala
            _log.debug('Connecting to impala at %s:%s' %
                      (self.cluster.master_ip, options.impala_port))
            impala_conn = impala.dbapi.connect(
                host=self.cluster.master_ip,
                port=options.impala_port)
            
            _log.info('Setting hive parameters')

            # Set some parameters
            hive.execute('SET hive.exec.compress.output=true')
            hive.execute('SET avro.output.codec=snappy')
            hive.execute('SET parquet.compression=SNAPPY')
            hive.execute(
                'SET hive.exec.dynamic.partition.mode=nonstrict')
            hive.execute(
                'SET hive.exec.max.created.files=500000')


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
            STORED AS INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat' 
            OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
            """ % (parq_table, self._generate_table_definition()))

            # Building parquet tables takes a lot of
            # memory, so make sure we give the job enough.
            hive.execute("SET mapreduce.reduce.memory.mb=16000")
            hive.execute(
                "SET mapreduce.reduce.java.opts=-Xmx14000m -XX:+UseConcMarkSweepGC")
            hive.execute("SET mapreduce.map.memory.mb=16000")
            hive.execute("SET mapreduce.map.java.opts=-Xmx14000m -XX:+UseConcMarkSweepGC")
            hive.execute("SET hive.exec.max.dynamic.partitions.pernode=200")
            cmd = ("""
            insert overwrite table %s
            partition(tai, yr, mnth)
            select %s, trackerAccountId,
            year(cast(serverTime as timestamp)),
            month(cast(serverTime as timestamp)) from %s""" %
            (parq_table, ','.join(x.name for x in 
                                  self.avro_schema.fields),
             external_table))
            _log.info("Running command: %s" % cmd)
            hive.execute(cmd)

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

        except Exception as e:
            _log.exception('Unexpected exception when building the impala '
                           'table %s' % self.event)
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'

        finally:
            transport.close()

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
    # TODO(mdesnoyer): Add ImageVisible and ImageClick back in. Disabling for
    # now because the job gets killed by a mysterious force.

    #for event in ['ImageLoad', 'AdPlay', 'VideoPlay', 'VideoViewPercentage',
    #              'EventSequence']:
    for event in ['EventSequence', 'VideoPlay']:
        thread = ImpalaTableBuilder(input_path, cluster, event)
        thread.start()
        threads.append(thread)
        time.sleep(5)

    # Wait for all of the tables to be built
    for thread in threads:
        time_left = None
        if timeout is not None:
            time_left = (budget_time - datetime.datetime.now()).total_seconds()
            if time_left < 0:
                raise TimeoutException()
        thread.join(time_left)
        if thread.is_alive():
            for t2 in threads:
                t2.stop()
            raise TimeoutException()
        if thread.status != 'SUCCESS':
            _log.error("Error building impala table %s. State is %s. See logs."
                       % (thread.event, thread.status))
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

def run_batch_cleaning_job(cluster, input_path, output_path, s3_path, timeout=None):
    '''Runs the mapreduce job that cleans the raw events.

    The events are output in a format that can be read by hive as an
    external table.

    Inputs:
    input_path - The s3 path for the raw data
    output_path - The output path for the raw data
    timeout - Time in seconds    
    '''

    # We set all the mapreduce parameters required for the clean up job here

    # Define extra options for the job
    extra_ops = {
        'mapreduce.output.fileoutputformat.compress' : 'true',
        'avro.output.codec' : 'snappy',
        'mapreduce.job.reduce.slowstart.completedmaps' : '1.0',
        'mapreduce.task.timeout' : 1800000,
        'mapreduce.reduce.speculative': 'false',
        'mapreduce.map.speculative': 'false',
        'io.file.buffer.size': 65536
        }

    map_memory_mb = 2048

    # If the requested map memory is different, set it
    if map_memory_mb is not None:
        extra_ops['mapreduce.map.memory.mb'] = map_memory_mb
        extra_ops['mapreduce.map.java.opts'] = (
            '-Xmx%im' % int(map_memory_mb * 0.8))

    # Figure out the number of reducers to use by aiming for files
    # that are 1GB on average.
    input_data_size = 0
    s3AddressRe = re.compile(r's3://([^/]+)/(\S+)')
    s3AddrMatch = s3AddressRe.match(input_path)
    if s3AddrMatch:

        # First figure out the size of the data
        bucket_name, key_name = s3AddrMatch.groups()
        s3conn = S3Connection()
        prefix = re.compile('([^\*]*)\*').match(key_name).group(1)
        for key in s3conn.get_bucket(bucket_name).list(prefix):
            input_data_size += key.size

        n_reducers = math.ceil(input_data_size / (1073741824. / 2))
        extra_ops['mapreduce.job.reduces'] = str(int(n_reducers))

    # If the cluster's core has larger instances, the memory
    # allocated in the reduce can get very large. However, we max
    # out the reduce to 1GB, so limit the reducer to use at most
    # 5GB of memory.
    core_group = cluster._get_instance_group_info('CORE')
    if core_group is None:
        raise ClusterInfoError('Could not find the CORE instance group')

    if (output_path.startswith("hdfs") and 
        core_group.instancetype in ['r3.2xlarge', 'r3.4xlarge',
                                    'r3.8xlarge', 'i2.8xlarge',
                                    'i2.4xlarge', 'cr1.8xlarge']):
        extra_ops['mapreduce.reduce.memory.mb'] = 5000
        extra_ops['mapreduce.reduce.java.opts'] = '-Xmx4000m'

    _log.info("Starting batch event cleaning job done")

    try:
        cluster.run_map_reduce_job(options.mr_jar,
                                   'com.neon.stats.RawTrackerMR',
                                   input_path,
                                   output_path,
                                   extra_ops,
                                   timeout=timeout)
    except Exception as e:
        _log.error('Error running the batch cleaning job: %s' % e)
        statemon.state.increment('stats_cleaning_job_failures')
        raise

    try:
        jar_location = 's3://us-east-1.elasticmapreduce/libs/s3distcp/1.0/s3distcp.jar'
        
        s3_output_path = ' '
        get_time = re.search(r'(.*)(\d{4}-\d{2}-\d{2}-\d{2}-\d{2})', output_path)
        if get_time:
            s3_output_path = s3_path + get_time.group(2)

            cluster.checkpoint_hdfs_to_s3(jar_location,
                                          output_path,
                                          s3_output_path,
                                          timeout=timeout)
        else:
            _log.error('Incorrect s3 path, the S3 copy step has been skipped')
    except Exception as e:
        _log.error('Copy from hdfs to S3 failed: %s' % e)
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
    s3_checkpoint_dir = ' '

    for app in rm_response['apps']['app']:
        match = re.search(r'S3DistCp: (.*) (->) (.*)',app['name'])
        if match and (last_app is None or last_started_time < app['startedTime']):
            last_app = app
            s3_checkpoint_dir = match.group(3)
            last_started_time = app['startedTime']         
            
    if last_app is None:
        return None

    _log.info('The batch job with id %s is in state %s with '
              'progress of %i%%' % 
              (last_app['id'], last_app['state'], last_app['progress']))
    return s3_checkpoint_dir

def get_last_sucessful_batch_output(cluster):
    '''Determines the last sucessful batch output path.

    Returns: The s3 patch of the last sucessful job, or None if there wasn't one
    '''
    cluster.connect()

    response = cluster.query_resource_manager(
        '/ws/v1/cluster/apps?finalStatus=SUCCEEDED')

    last_successful_output = _get_last_batch_app(response)

    if last_successful_output is None:
        return None

    _log.info('Found the last successful output directory as %s' % last_successful_output)

    return last_successful_output

def cleanup_hdfs(cluster, current_hdfs_dir_time, hdfs_dir):
    # Cleans up all other HDFS directories except the current one. Access the Namenode using the https
    # HDFS client. Check for existence of directory and delete the old ones recursively.

    _log.info("Deleting all old HDFS directories except current %s" % current_hdfs_dir_time)

    http_string = 'http://%s:9101' % cluster.master_ip

    hdfs_conn = InsecureClient(http_string, user='hadoop')

    file_exists = hdfs_conn.status('/'+hdfs_dir, strict=False)
    
    if file_exists:
        list_files = hdfs_conn.list('/'+hdfs_dir, status=False)

        for file in list_files:
            if file == current_hdfs_dir_time:
               continue
            else:
                delete_status = hdfs_conn.delete('/'+hdfs_dir+'/'+file, recursive=True)
                if delete_status is True:
                    _log.info('Deleted hdfs directory %s' % '/mnt/cleaned/'+file)
                else:
                    _log.info('Could not delete hdfs directory %s' % '/mnt/cleaned/'+file)
    else:
        _log.info("HDFS base directory %s does not exist" % hdfs_dir)



    
