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
import calendar

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
class MergeEventsExecutionError(ExecutionError): pass

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

    def build_avro_table_name(self, execution_date):
        """
        Generate an Avro table name from an execution_date (datetime) object
        :param execution_date: a datetime.datetime object
        :param_type: datetime.datetime
        """
        return '{prefix}_{dt}'.format(prefix=self.event_avro.lower(),
                                      dt=execution_date.strftime("%Y%m%d%H"))

    def build_parquet_table_name(self):
        """
        The name of the Parquet-format Impala table
        """
        return self.event_parq.lower()

    def create_avro_table(self, execution_date, input_path=None, cc_table=None,
                          cc_location=None):
        """"""
        self._upload_schema()
        # External Avro table in S3
        if cc_table:
            table = cc_table
            self.drop_avro_table(execution_date, cc_table)
        else:
            table = self.build_avro_table_name(execution_date)
            self.drop_avro_table(execution_date)

        _log.info('Registering event {event} Avro table {table} with Hive'
                    .format(event=self.event, table=table))

        # Location of table in S3
        if cc_location:
            location_s3 = cc_location
        else:
            location_s3 = os.path.join(input_path, self.event_schema)

        try:
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
            )""" % (table, location_s3,
                    self._schema_path())
            _log.debug('CREATE Avro Table SQL: {sql}'.format(sql=sql))
            self.hive.execute(sql)

        except:
            _log.error("Error creating event %s Avro table %s" % (self.event, table))
            statemon.state.increment('impala_table_creation_failure')
            self.status = 'ERROR'
            raise ExecutionError

    def drop_avro_table(self, execution_date, corner_case_table=None):
        """
        Drop the external Avro table for a given date
        :param execution_date:
        :param corner_case_table - A table from merge event sequences processing
        :return:
        """
        try:
            if corner_case_table:
                table = corner_case_table
            else:
                table = self.build_avro_table_name(execution_date)

            _log.info('Dropping Avro table {table}'.format(table=table))
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
            table = self.build_parquet_table_name()
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
            parq_table = self.build_parquet_table_name()
            avro_table = self.build_avro_table_name(execution_date)

            _log.info('Loading to Impala Parquet-format table {parq} from '
                      '{avro}'.format(parq=parq_table, avro=avro_table))
            
            # Set the hive parameters required for queries and creating dynamic
            # hive partitions
            self.set_hive_parameters()
            self.hive.execute('SET hive.exec.dynamic.partition.mode=nonstrict')
            self.hive.execute('SET hive.exec.max.created.files=500000')
            self.hive.execute('SET hive.exec.max.dynamic.partitions.pernode=200')

            _log.info('execution date is {dt}'.format(dt=execution_date))

            if is_initial_data_load:
                sql = """
                insert overwrite table {parq_table}
                partition(tai, yr, mnth, day)
                select {columns}, trackerAccountId,
                year(cast(serverTime as timestamp)),
                month(cast(serverTime as timestamp)),
                -1
                from {avro_table}
                where
                serverTime < {epoch_of_today}
                """.format(parq_table=parq_table,
                        columns=','.join(x.name for x in self.avro_schema.fields),
                        avro_table=avro_table,
                        epoch_of_today=calendar.timegm(execution_date.timetuple()))
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

    def set_hive_parameters(self):
        # Sets the hive parameters

        heap_size = int(options.parquet_memory * 0.9)

        self.hive.execute('SET hive.exec.compress.output=true')
        self.hive.execute('SET avro.output.codec=snappy')
        self.hive.execute('SET parquet.compression=SNAPPY')

        self.hive.execute('SET mapreduce.reduce.memory.mb=%d' %
                            options.parquet_memory)
        self.hive.execute('SET mapreduce.reduce.java.opts=-Xmx%dm -XX:+UseConcMarkSweepGC' %
                            heap_size)
        self.hive.execute('SET mapreduce.map.memory.mb=%d' %
                            options.parquet_memory)
        self.hive.execute('SET mapreduce.map.java.opts=-Xmx%dm -XX:+UseConcMarkSweepGC' %
                            heap_size)

        _log.info('Hive parameters set')


    def create_input_for_merge(self, execution_date, is_initial_data_load, cc_cleaned_path_prev):
        """
        Create the input required for merging eventsequences across day boundaries

        The first and big run: is always going to be clean as it is run over the entire input bucket
        in S3. So merge event sequences need not be handled. However we need to carry forward the clean 
        data for this execution date. The next run will use this as merge event sequence cleaned data for
        previous run.

        On subsequent runs: we will take in current cleaned output from mapreduce job and previous merge 
        event sequences cleaned output from merge event sequence S3 bucket. The input from previous merge 
        event sequence clean run is required because for each run we process the data for that entire day. 
        So we should be carrying over the merge event sequences that were cleaned at the first run of this day. 

        Inputs:
        execution_date - The execution date of airflow task 
        is_initial_data_load - Indicates if this is the first and big run
        cc_cleaned_path_prev - The merge event sequences cleaned path of previous run
        """
        try:
            # Upload schema to S3
            self._upload_schema()

            # Set the hive parameters required for queries
            self.set_hive_parameters()

            # Avro table name pointing to mapreduce cleaned ouput for current run
            table = self.build_avro_table_name(execution_date)

            # Drop table for merge event sequences input for current run
            cc_input = 'corner_cases_input_{dt}'.format(dt=execution_date.strftime("%Y%m%d%H"))
            self.drop_avro_table(execution_date, cc_input)
        
            if is_initial_data_load:
                sql = """
                CREATE TABLE corner_cases_input_{dt} AS
                SELECT {columns} from {table}
                WHERE 
                year(cast(serverTime as timestamp)) = {year} AND
                month(cast(serverTime as timestamp)) = {month} AND
                day(cast(serverTime as timestamp)) = {day}
                """.format(columns=','.join(x.name for x in self.avro_schema.fields),
                            table=table,dt=execution_date.strftime("%Y%m%d%H"),
                            year=execution_date.year,
                            month=execution_date.month,
                            day=execution_date.day)
            else:
                # Avro table pointing to s3 bucket of previous merge event sequence run
                cc_cleaned_previous = 'avro_cc_cleaned_{dt}'. \
                                         format(dt=(execution_date - timedelta(hours=3)). \
                                         strftime("%Y%m%d%H"))

                # Create the table pointing to previous merge event sequence run output
                self.create_avro_table(execution_date, cc_table=cc_cleaned_previous, 
                                       cc_location=cc_cleaned_path_prev)

                sql = """
                CREATE TABLE corner_cases_input_{dt} AS
                SELECT {columns} from
                (
                SELECT {columns} from {table}
                UNION ALL
                SELECT {columns} from {cc_cleaned_previous}
                ) cc_input
                """.format(columns=','.join(x.name for x in self.avro_schema.fields),
                           table=table,dt=execution_date.strftime("%Y%m%d%H"),
                           cc_cleaned_previous=cc_cleaned_previous)

            _log.debug('merge event sequences input SQL: {sql}'.format(sql=sql))
            _log.info('Creating input for merge processing')
            self.hive.execute(sql)

        except:
            _log.error("Error creating input for merge event sequences")
            self.status = 'ERROR'
            raise MergeEventsExecutionError

    def merge_event_sequences(self, execution_date, cc_cleaned_path_current):
        """
        For all the possible entry points for an event, define a group. This will be used by the query to 
        pick up a single row with most columns filled in case of duplicates and any non-duplicate rows. 
        """
        # Create Avro table pointing to S3 for copy
        corner_case_copy = "avro_cc_cleaned_{dt}_copy".format(dt=execution_date.strftime("%Y%m%d%H"))
        self.create_avro_table(execution_date, cc_table=corner_case_copy, 
                               cc_location=cc_cleaned_path_current)

        # Define groups for each possible event entry
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

        self.execute_merge_query(execution_date, imload_group, 
                              imvis_group, imclick_group,
                              adplay_group, videoplay_group)

    def execute_merge_query(self, execution_date, imload_group, 
                            imvis_group, imclick_group, 
                            adplay_group, videoplay_group):
        """
        The merge query will combine all the individual merge event sequence cleaned events. There are also some
        rows in the table that have null thumbnail id's with all imclick,imvis,imload,adplay,videoplay being null so
        we will also carry them forward so that we dont drop any rows (are these required?). 
        """

        #Set the hive parameters
        self.set_hive_parameters()

        # Drop the cleaned table for current processing if exists
        corner_case_cleaned = 'avro_cc_cleaned_{dt}'.format(dt=execution_date.strftime("%Y%m%d%H"))
        self.drop_avro_table(execution_date, corner_case_cleaned)

        sql = """
        CREATE TABLE avro_cc_cleaned_{dt} AS
        select {columns} from 
        (
        select {columns} from 
        (
        select {columns}, {imload_group} as rownum 
        from corner_cases_input_{dt}
        where
        thumbnail_id is not null and
        imloadservertime is not null
        ) imload_cleaned
        where rownum = 1
        UNION ALL
        select {columns} from 
        (
        select {columns}, {imvis_group} as rownum 
        from corner_cases_input_{dt} 
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
        from corner_cases_input_{dt} 
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
        from corner_cases_input_{dt}
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
        from corner_cases_input_{dt}
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
        select DISTINCT {columns} 
        from corner_cases_input_{dt}
        where 
        thumbnail_id is null
        ) 
        overall_cleaned
        """.format(columns=','.join(x.name for x in self.avro_schema.fields),
            imload_group=imload_group,
            imvis_group=imvis_group,
            imclick_group=imclick_group,
            adplay_group=adplay_group,
            videoplay_group=videoplay_group,
            dt=execution_date.strftime("%Y%m%d%H"))

        try:
            _log.debug('merge event sequences SQL: {sql}'.format(sql=sql))
            self.hive.execute(sql)

            _log.info('Done merge event sequences')

            # Write the data for the current and previous day to s3 merge event sequence bucket 
            sql="""
            INSERT OVERWRITE TABLE avro_cc_cleaned_{dt}_copy
            select {columns} from avro_cc_cleaned_{dt}
            where
            serverTime > {epoch_previous_day}
            """.format(columns=','.join(x.name for x in self.avro_schema.fields),
                       dt=execution_date.strftime("%Y%m%d%H"),
                       epoch_previous_day=calendar.timegm((execution_date.date() - timedelta(days=1)).timetuple()))

            _log.debug('Moving data to s3: {sql}'.format(sql=sql))

            self.hive.execute(sql)
            _log.info('Done moving data to S3')

        except:
            _log.error("Error resolving merge event sequences")
            self.status = 'ERROR'
            raise MergeEventsExecutionError

    def cleanup_after_merge_processing(self, execution_date):
        """
        Clean up all the tables we created during merge event sequence processing
        """
        # Drop the current merge event sequence cleaned table as this data 
        # has been written to s3 now

        _log.info('Cleaning up after merge events processing')
        
        corner_case_table = 'avro_cc_cleaned_{dt}'.format(dt=execution_date.strftime("%Y%m%d%H"))
        self.drop_avro_table(execution_date, corner_case_table)

        # Drop the previous merge event sequence cleaned table
        corner_case_table = 'avro_cc_cleaned_{dt}'. \
                            format(dt=(execution_date - timedelta(hours=3)). \
                                   strftime("%Y%m%d%H"))
        self.drop_avro_table(execution_date, corner_case_table)

        # Drop the copy table
        corner_case_table = 'avro_cc_cleaned_{dt}_copy'.format(dt=execution_date.strftime("%Y%m%d%H"))
        self.drop_avro_table(execution_date, corner_case_table)

        # Drop table for merge event sequences input for current run
        corner_case_table = 'corner_cases_input_{dt}'.format(dt=execution_date.strftime("%Y%m%d%H"))
        self.drop_avro_table(execution_date, corner_case_table)

        # Drop table pointing to mapreduce output of current run
        corner_case_table = 'avroeventsequences_{dt}'.format(dt=execution_date.strftime("%Y%m%d%H"))
        self.drop_avro_table(execution_date, corner_case_table)

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

            table = self.table.build_parquet_table_name()
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

    def __init__(self, cluster, event, execution_date, is_initial_data_load,
                 input_path):
        super(ImpalaTableLoader, self).__init__()
        self.event = event
        self.cluster = cluster
        self.input_path = input_path
        self.execution_date = execution_date
        self.is_initial_data_load = is_initial_data_load
        self.status = 'INIT'
        self._stopped = threading.Event()
        self.table = ImpalaTable(self.cluster, self.event)

        # Cleanup after ourselves on a failure?
        self._drop_avro_on_failure = True

    def stop(self):
        self._stopped.set()

    def run(self):
        self._stopped.clear()
        self.status = 'RUNNING'
        _log.info("Event '%s' table build thread running" % self.event)

        try:
            self.table.cluster.connect()
            self.table.transport.open()
            avro_table = self.table.build_avro_table_name(self.execution_date)
            if self.table.exists(avro_table):
                _log.error("Avro table for event '%s' exists: %s" % 
                           (self.event, avro_table))
            else:
                self.table.create_avro_table(self.execution_date,
                                             self.input_path)

            parq_table = self.table.build_parquet_table_name()
            if self.table.exists(parq_table):
                _log.info("Parquet table for event '%s' exists: %s" % 
                           (self.event, parq_table))
                self.table.load_parquet_table(self.execution_date,
                                              self.is_initial_data_load)
                self.table.drop_avro_table(self.execution_date)
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

class SequencesAcrossDaysHandler():
    """
    This class merges the eventsequences across days and retains the record with 
    highest number of columns in the sequence. 
    """

    def __init__(self, cluster, execution_date,
                 is_initial_data_load, input_path, 
                 cc_cleaned_path_prev, cc_cleaned_path_current):

        self.event = 'EventSequence'
        self.cluster = cluster
        self.input_path = input_path
        self.execution_date = execution_date
        self.is_initial_data_load = is_initial_data_load
        self.cc_cleaned_path_prev = cc_cleaned_path_prev
        self.cc_cleaned_path_current = cc_cleaned_path_current
        self.table = ImpalaTable(self.cluster, self.event)

        # Cleanup after ourselves on a failure?
        self._drop_avro_on_failure = False

    def run(self):
        _log.info("Event '%s' table build" % self.event)

        try:
            self.table.cluster.connect()
            self.table.transport.open()
            avro_table = self.table.build_avro_table_name(self.execution_date)
            if self.table.exists(avro_table):
                _log.info("Avro table for event '%s' exists: %s" % 
                           (self.event, avro_table))
            else:
                self.table.create_avro_table(self.execution_date,
                                             self.input_path)

            self.table.create_input_for_merge(self.execution_date,
                                              self.is_initial_data_load,
                                              self.cc_cleaned_path_prev)

            self.table.merge_event_sequences(self.execution_date,
                                             self.cc_cleaned_path_current)

        except:
            _log.exception('Error merging event sequences')
            if self._drop_avro_on_failure:
                self.table.drop_avro_table(self.execution_date)
            raise MergeEventsExecutionError

        finally:
            self.table.cleanup_after_merge_processing(self.execution_date)
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