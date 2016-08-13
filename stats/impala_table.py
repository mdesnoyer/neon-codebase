''''
Script that runs one cycle of the hadoop stats batch processing.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
        Robb Wagoner (robb@pandastrike.com)
Copyright Neon Labs 2014, 2015, 2016
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
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
import impala.dbapi
import impala.error
import logging
import re
import threading
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import time
import urllib2
from urlparse import urlparse
from utils.options import define, options
from utils import statemon
from datetime import datetime, timedelta

_log = logging.getLogger(__name__)

define("hive_port", default=10004, help="Port to talk to hive on")
define("impala_port", default=21050, help="Port to talk to impala on")
define("schema_bucket", default="neon-avro-schema",
       help=("Bucket that must contain the compiled schema to define the "
             "tables."))
define("mr_jar", default=None, type=str, help="Mapreduce jar")
define("compiled_schema_path",
       default=os.path.join(__base_path__, 'schema', 'compiled'),
       help='Path to the bucket of compiled avro schema')
define("parquet_memory", default=16000, type=int,
       help='Memory (MB) for the parquet loading mapreduce job')

statemon.define("stats_cleaning_job_failures", int)
statemon.define("impala_table_creation_failure", int)
statemon.define("hbase_table_load_failure", int)

class NeonDataPipelineException(Exception): pass
class ExecutionError(NeonDataPipelineException): pass
class ImpalaError(ExecutionError): pass
class IncompatibleSchema(NeonDataPipelineException): pass
class TimeoutException(NeonDataPipelineException): pass
class UnexpectedInfo(NeonDataPipelineException): pass
class SchemaUploadError(ExecutionError): pass
class ImpalaTableLoadError(ExecutionError): pass

class ImpalaTable(object):
    '''Representation of an Impala table'''

    def __init__(self, cluster, event):
        self.cluster = cluster
        self.event = event

        self.event_avro = 'Avro%ss' % self.event
        self.event_parq = '%ss' % self.event

        self.event_schema = '%sHive' % self.event
        self.schema_file = '%s.avsc' % self.event_schema
        self.avro_schema = avro.schema.parse(open(os.path.join(
            options.compiled_schema_path, self.schema_file)).read())

        self.status = 'INIT'

        self.hive = None
        self.transport = self._connect_to_hive()

        self.impala_conn = None
        self._connect_to_impala()

    def _generate_table_definition(self):
        '''Generates this hive table definition based on the avro schema.

        This converts the avro types to hive/parquet types.
        '''
        avro2hive_map = {
            'string': 'string',
            'boolean': 'boolean',
            'int': 'int',
            'long': 'bigint',
            'float': 'float',
            'double': 'double',
            'enum': 'string'
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

    def _refresh_table(self, table):
        """Refresh the Impala metadata for a given table type
           - http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/v2-0-x/topics/impala_refresh.html
           - http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/v2-0-x/topics/impala_invalidate_metadata.html
        """
        _log.debug("Refreshing Impala table %s" % table)
        impala_cursor = self.impala_conn.cursor()
        if self.exists(table):
            # The table is already there, so we just need to refresh it
            return impala_cursor.execute("refresh {table}".format(table=table))
        else:
            # It's not there, so we need to refresh all the metadata
            return impala_cursor.execute('invalidate metadata')

    def exists(self, table):
        """Does the table exist in Impala?"""
        impala_cursor = self.impala_conn.cursor()
        impala_cursor.execute("show tables")
        tables = [x[0] for x in impala_cursor.fetchall()]
        if table.lower() in tables:
            impala_cursor.close()
            return True
        else:
            impala_cursor.close()
            return False

    def _connect_to_hive(self):
        """Connect to Hive on the EMR cluster using Thrift protocol
        """
        try:
            _log.debug('Connecting to Hive at %s:%s' %
                       (self.cluster.master_ip, options.hive_port))
            transport = TSocket.TSocket(self.cluster.master_ip,
                                        options.hive_port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self.hive = ThriftHive.Client(protocol)
            return transport

        except:
            _log.exception("Error connecting to Hive")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise ExecutionError

    def _connect_to_impala(self):
        """Connect to Impala on the EMR cluster
        """
        try:
            _log.debug('Connecting to Impala at %s:%s' %
                       (self.cluster.master_ip, options.impala_port))
            self.impala_conn = impala.dbapi.connect(
                host=self.cluster.master_ip,
                port=options.impala_port)

        except:
            _log.exception("Error connecting to Impala")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise ImpalaError

    def _schema_path(self):
        """Render the Avro schema path"""
        return 's3://%s/%s' % (options.schema_bucket, self.schema_file)

    def _upload_schema(self):
        """
        Upload an Avro schema file (.avsc) to S3
        :return:
        """
        try:
            s3url = urlparse(self._schema_path())
            s3conn = S3Connection()
            bucket = s3conn.get_bucket(s3url.netloc)
            key = boto.s3.key.Key(bucket, s3url.path)
            _log.debug("Uploading compiled schema file %s to s3://%s%s" % (self.schema_file, bucket.name, key.key))
            # Create object in S3
            key.set_contents_from_filename(
                os.path.join(options.compiled_schema_path,
                             self.schema_file),
                replace=True)
        except:
            _log.exception('Error uploading the schema %s to s3' % self.event)
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise SchemaUploadError

    def _avro_table(self, execution_date):
        """
        Generate an Avro table name from an execution_date (datetime) object
        :param execution_date: a datetime.datetime object
        :param_type: datetime.datetime
        """
        return '{prefix}_{dt}'.format(prefix=self.event_avro.lower(),dt=execution_date.strftime("%Y%m%d%H"))

    def _parquet_table(self):
        """
        The name of the Parquet-format Impala table
        """
        return self.event_parq.lower()

    def create_avro_table(self, execution_date, input_path):
        """"""
        self._upload_schema()
        # External Avro table in S3
        table = self._avro_table(execution_date)
        _log.info('Registering event {event} Avro table {table} with Hive'
                  .format(event=self.event, table=table))
        try:
            self.drop_avro_table(execution_date)
            sql = """
            CREATE EXTERNAL TABLE %s
            ROW FORMAT SERDE
            'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS
            INPUTFORMAT
            'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            LOCATION '%s'
            TBLPROPERTIES (
            'avro.schema.url'='%s'
            )""" % (table, os.path.join(input_path, self.event_schema),
                    self._schema_path())
            _log.info('CREATE Avro Table SQL: {sql}'.format(sql=sql))
            self.hive.execute(sql)

        except:
            _log.error("Error creating event %s Avro table %s" % (self.event, table))
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise ExecutionError

    def handle_corner_cases(self, execution_date, is_first_run):
        """
        put in hive parameters
        """
        table = self._avro_table(execution_date)
        _log.info('Registering Avro Temp table with hive')
        
        if is_first_run:
            sql = """
            CREATE TABLE corner_cases_input AS
            SELECT {columns} from {table}
            """.format(columns=','.join(x.name for x in self.avro_schema.fields),
                          table=table)
        else:
            cleaned_previousday = 'avro_cc_cleaned_{dt}'. \
                                  format(dt=(execution_date - timedelta(days=1)). \
                                  strftime("%Y%m%d%H"))
            avro_previousday = 'avroeventsequences_{dt}'. \
                               format(dt=(execution_date - timedelta(hours=3)).
                               strftime("%Y%m%d%H"))
            sql = """
            CREATE TABLE corner_cases_input AS
            SELECT {columns} from
            (
            SELECT {columns} from {table}
            UNION ALL
            SELECT {columns} from {cleaned_previousday}
            UNION ALL
            SELECT {columns} from {avro_previousday}
            ) cc_input
            """.format(columns=','.join(x.name for x in self.avro_schema.fields),
                       table=table,
                       cleaned_previousday=cleaned_previousday)

        _log.info('Corner cases input SQL: {sql}'.format(sql=sql))
        self.hive.execute(sql)

        imload_group = """
        row_number() over (partition by 
        thumbnail_id,
        clientip,
        imloadservertime 
        order by 
        thumbnail_id,
        clientip,
        imloadservertime desc,
        imvisservertime desc,
        imclickservertime desc,
        adplayservertime desc,
        videoplayservertime desc)
        """

        imvis_group = """
        row_number() over (partition by 
        thumbnail_id,
        clientip,
        imvisservertime 
        order by 
        thumbnail_id,
        clientip,
        imvisservertime desc,
        imclickservertime desc,
        adplayservertime desc,
        videoplayservertime desc)
        """

        imclick_group = """
        row_number() over (partition by 
        thumbnail_id,
        clientip,
        imclickservertime 
        order by 
        thumbnail_id,
        clientip,
        imclickservertime desc,
        adplayservertime desc,
        videoplayservertime desc)
        """

        adplay_group = """
        row_number() over (partition by 
        thumbnail_id,
        clientip,
        adplayservertime 
        order by 
        thumbnail_id,
        clientip,
        adplayservertime desc,
        videoplayservertime desc)
        """

        videoplay_group = """
        row_number() over (partition by
        thumbnail_id,
        clientip,
        videoplayservertime
        order by
        thumbnail_id,
        clientip,
        videoplayservertime desc
        )
        """

        sql = """
        CREATE TABLE avro_cc_cleaned_{dt} AS
        select {columns} from 
        (
        select {columns} from 
        (
        select {columns}, {imload_group} as rownum 
        from corner_cases_input 
        where
        thumbnail_id is not null and
        imloadservertime is not null
        ) imload_cleaned
        where rownum = 1
        UNION ALL
        select {columns} from 
        (
        select {columns}, {imvis_group} as rownum 
        from corner_cases_input 
        where
        thumbnail_id is not null and
        imvisservertime is not null and 
        imloadservertime is null
        ) imvis_cleaned 
        where rownum = 1
        UNION ALL
        select {columns} from
        (
        select {columns}, {imclick_group} as rownum
        from corner_cases_input 
        where
        thumbnail_id is not null and
        imclickservertime is not null and 
        imloadservertime is null and 
        imvisservertime is null
        ) imclick_cleaned
        where rownum = 1
        UNION ALL
        select {columns} from
        (
        select {columns}, {adplay_group} as rownum
        from corner_cases_input
        where
        thumbnail_id is not null and
        adplayservertime is not null and 
        imloadservertime is null and 
        imvisservertime is null and 
        imclickservertime is null
        ) adplay_cleaned
        where rownum = 1
        UNION ALL
        select {columns} from
        (
        select {columns}, {videoplay_group} as rownum
        from corner_cases_input
        where
        thumbnail_id is not null and
        videoplayservertime is not null and
        imloadservertime is null and 
        imvisservertime is null and 
        imclickservertime is null and
        adplayservertime is null
        ) videoplay_cleaned
        where rownum = 1
        UNION ALL
        select {columns} 
        from corner_cases_input
        where thumbnail_id is null
        ) 
        overall_cleaned
        """.format(columns=','.join(x.name for x in self.avro_schema.fields),
            imload_group=imload_group,imvis_group=imvis_group,imclick_group=imclick_group,
            adplay_group=adplay_group,videoplay_group=videoplay_group,
            dt=execution_date.strftime("%Y%m%d%H"))

        try:
            _log.info('Corner cases SQL: {sql}'.format(sql=sql))
            self.hive.execute(sql)

            _log.info('Done corner cases')

            sql="""
            DROP IF EXISTS {table}
            """.format(table=cleaned_previousday)

            _log.info('Delete sql is %s' % sql)

            self.hive.execute(sql)

            _log.info('Done delete ')

            sql="""
            DROP TABLE IF EXISTS corner_cases_input
            """
            _log.info('Delete sql for cc input is %s' % sql)

            self.hive.execute(sql)

            _log.info('Done delete ccinput')
        except:
            _log.error("Error creating Avro Temp Table")
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise ExecutionError

    def drop_avro_table(self, execution_date):
        """
        Drop the external Avro table for a given date
        :param execution_date:
        :return:
        """
        try:
            table = self._avro_table(execution_date)
            _log.debug('Dropping Avro table {table}'.format(table=table))
            self.hive.execute('DROP TABLE IF EXISTS {table}'.format(table=table))
            if self.exists(table):
                self.hive.execute('invalidate metadata {0:s}'.format(table))
        except:
            _log.error('Error dropping Avro table {table}'.format(table=table))
            self.status = 'ERROR'
            raise ExecutionError

    def create_parquet_table(self):
        """Create the Impala Parquet-format table"""
        try:
            table = self._parquet_table()
            _log.info("Creating Impala Parquet table: %s" % table)
            self.hive.execute("""
            CREATE TABLE IF NOT EXISTS %s
            (%s)
            partitioned by (tai string, yr int, mnth int, day int)
            ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
            OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
            """ % (table, self._generate_table_definition()))
            self._refresh_table(table)
        except:
            _log.error('Error creating event %s Parquet table %s' % (self.event, table))
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise ImpalaError

    def load_parquet_table(self, execution_date, is_initial_data_load=None):
        '''Load data into the Parquet table from the external Avro table
        :param execution_date: the Airflow execution_date object
        :param_type datetime.datetime:
        :param_type integer:
        '''
        # Building parquet tables takes a lot of
        # memory, so make sure we give the job enough.
        # We partition by tai-year-month to allow for idempotent inserts.
        # https://cwiki.apache.org/confluence/display/Hive/DynamicPartitions
        try:
            parq_table = self._parquet_table()
            avro_table = self._avro_table(execution_date)

            _log.info('Loading to Impala Parquet-format table {parq} from '
                      '{avro}'.format(parq=parq_table, avro=avro_table))
            heap_size = int(options.parquet_memory * 0.9)

            self.hive.execute('SET hive.exec.compress.output=true')
            self.hive.execute('SET avro.output.codec=snappy')
            self.hive.execute('SET parquet.compression=SNAPPY')
            self.hive.execute('SET hive.exec.dynamic.partition.mode=nonstrict')
            self.hive.execute('SET hive.exec.max.created.files=500000')
            self.hive.execute('SET hive.exec.max.dynamic.partitions.pernode=200')

            self.hive.execute('SET mapreduce.reduce.memory.mb=%d' %
                              options.parquet_memory)
            self.hive.execute('SET mapreduce.reduce.java.opts=-Xmx%dm -XX:+UseConcMarkSweepGC' %
                              heap_size)
            self.hive.execute('SET mapreduce.map.memory.mb=%d' %
                              options.parquet_memory)
            self.hive.execute('SET mapreduce.map.java.opts=-Xmx%dm -XX:+UseConcMarkSweepGC' %
                              heap_size)

            _log.info('is_initial_data_load is %s' % is_initial_data_load)

            if is_initial_data_load:
                sql = """
                insert overwrite table %s
                partition(tai, yr, mnth, day=-1)
                select %s, trackerAccountId,
                year(cast(serverTime as timestamp)),
                month(cast(serverTime as timestamp))
                from %s""" % (parq_table,
                              ','.join(x.name for x in self.avro_schema.fields),
                              avro_table)
            else:
                sql = """
                insert overwrite table %s
                partition(tai, yr, mnth, day)
                select %s, trackerAccountId,
                year(cast(serverTime as timestamp)),
                month(cast(serverTime as timestamp)),
                day(cast(serverTime as timestamp))
                from %s""" % (parq_table,
                              ','.join(x.name for x in self.avro_schema.fields),
                              avro_table)

            _log.info('LOAD Impala-Parquet table command: {sql}'.format(
                sql=sql))

            self.hive.execute(sql)
            self._refresh_table(parq_table)

        except:
            _log.error("Error loading event %s Parquet table %s" % (self.event, parq_table))
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise ImpalaTableLoadError


class ImpalaTableBuilder(threading.Thread):
    '''Thread that will dispatch and monitor the job to build the Impala Parquet-format table.'''

    def __init__(self, cluster, event):
        super(ImpalaTableBuilder, self).__init__()
        self.event = event
        self.cluster = cluster
        self.status = 'INIT'
        self._stopped = threading.Event()
        self.table = ImpalaTable(self.cluster, self.event)

    def stop(self):
        self._stopped.set()

    def run(self):
        self._stopped.clear()
        self.status = 'RUNNING'
        _log.debug("Event '%s' table build thread running" % self.event)
        try:
            self.table.cluster.connect()

            table = self.table._parquet_table()
            if self.table.exists(table):
                _log.info("Parquet table for event '%s' exists: %s" % (self.event, table))
            else:
                _log.warning("Parquet table for event '%s' does not exist: %s" % (self.event, table))
                self.table.transport.open()
                self.table.create_parquet_table()

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
                           'table %s' % table)
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'

        except:
            _log.exception("Error building Impala tables for event %s" % self.event)

        finally:
            self.table.transport.close()


class ImpalaTableLoader(threading.Thread):
    """
    Load cleaned data to the Impala Parquet-format table
    """

    def __init__(self, cluster, event, execution_date, corner_cases,
                 is_first_run, is_initial_data_load, input_path):
        super(ImpalaTableLoader, self).__init__()
        self.event = event
        self.cluster = cluster
        self.input_path = input_path
        self.execution_date = execution_date
        self.corner_cases = corner_cases
        self.is_first_run = is_first_run
        self.is_initial_data_load = is_initial_data_load
        self.status = 'INIT'
        self._stopped = threading.Event()
        self.table = ImpalaTable(self.cluster, self.event)

        # Cleanup after ourselves on a failure?
        self._drop_avro_on_failure = False

    def stop(self):
        self._stopped.set()

    def run(self):
        self._stopped.clear()
        self.status = 'RUNNING'
        _log.info("Event '%s' table build thread running" % self.event)

        try:
            self.table.cluster.connect()
            self.table.transport.open()
            avro_table = self.table._avro_table(self.execution_date)
            if self.table.exists(avro_table):
                _log.error("Avro table for event '%s' exists: %s" % 
                           (self.event, avro_table))
            else:
                self.table.create_avro_table(self.execution_date,
                                             self.input_path)

            _log.info('self.event is %s' % self.event)
            _log.info('self.corner_cases is %s' % self.corner_cases)
            _log.info('self.is_first_run is %s' % self.is_first_run)
            _log.info('is_initial_data_load is %s' % self.is_initial_data_load)
            #if self.corner_cases and self.event == 'EventSequence':
            #    self.table.handle_corner_cases(self.execution_date,self.is_first_run)

            parq_table = self.table._parquet_table()
            if self.table.exists(parq_table):
                _log.info("Parquet table for event '%s' exists: %s" % 
                           (self.event, parq_table))
                self.table.load_parquet_table(self.execution_date,
                                              self.is_initial_data_load)
                self.table.drop_avro_table((self.execution_date - timedelta(hours=3)))
            else:
                _log.error("Parquet table for event '%s' missing: %s" %
                           (self.event, parq_table))
                raise ImpalaTableLoadError

            self.status = 'SUCCESS'

        except:
            _log.exception('Error loading Impala table for event %s' %
                           self.event)
            if self._drop_avro_on_failure:
                self.table.drop_avro_table(self.execution_date)
            raise ImpalaTableLoadError

        finally:
            _log.debug("Closing Impala connection")
            self.table.transport.close()

def update_table_build_times(cluster):
    _log.debug("Updating the table build times")
    
    impala_conn = impala.dbapi.connect(host=cluster.master_ip,
                                       port=options.impala_port)
    cursor = impala_conn.cursor()
    cursor.execute("show tables")
    tables = [x[0] for x in cursor.fetchall()]

    if 'table_build_times' in tables:
        _log.info('table_build_times exists, updating it')
        cursor.execute("insert into table_build_times (done_time) values ('%s')" %
            datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    else:
        _log.info('table_build_times does not exist, creating and updating')
        cursor.execute('create table table_build_times '
            '(done_time timestamp) stored as PARQUET')
        cursor.execute("insert into table_build_times (done_time) values ('%s')" %
            datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

    _log.debug('Finished building Impala tables')

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


def get_last_successful_batch_output(cluster):
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
            _log.info('Found the last successful output directory as %s' %
                      prop['value'])
            return prop['value']

    return None
