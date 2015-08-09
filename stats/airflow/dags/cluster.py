"""
Airflow DAG for launching an EMR cluster

------------------------------------------------------------------
Airflow commands to reload the Impala tables for an EMR cluster
------------------------------------------------------------------

    EPOCH="2014-05-01 00:00:00"
    airflow clear -t load_impala_tables --start_date "$EPOCH" clicklogs
    airflow backfill -t load_impala_tables --start_date "$EPOCH" clicklogs



"""
__author__ = 'Robb Wagoner (@robbwagoner)'

import os.path
import sys

__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

# PyCharm Debugger - TODO(robbwagoner): remove
sys.path.append(os.path.join(__base_path__, 'pycharm-debug.egg'))
import pydevd

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.subdag_operator import SubDagOperator
# from airflow.operators.sensors import ExternalTaskSensor  # S3KeySensor, S3PrefixSensor, TimeSensor
from datetime import datetime, timedelta
import dateutils
import logging
import optparse
import random
import re
import stats.cluster
import stats.batch_processor
import stats.impala_table
import time
import urlparse
import utils.logs
import utils.statemon

_log = logging.getLogger('airflow.dag.clicklogs')

# ----------------------------------------
# Options and configurations with DAGs
# ----------------------------------------
"""
For a DAG to be able to use the Neon utils.options module, two wrapper objects
are necessary to allow the eponymous Airflow command to work as normal and not
 cause an error with the OptionParser().
"""
from utils.options import define


class DagOptionParser(optparse.OptionParser):
    def error(self, msg):
        """No-op the error() method so that Airflow command args don't print to stderr.
        """
        pass

    def exit(self, code):
        """No-op the exit() method so that Airflow command args don't cause a sys.exit(2).
        """
        pass


class DagOptions(utils.options.OptionParser):
    """

    """

    def parse_options(self, args=None, config_stream=None,
                      usage='%prog [options]', watch_file=True, parse_command_line=True):
        '''Parse the options.

        Inputs:
        args - Argument list. defaults to sys.argv[1:]
        config_stream - Specify a yaml stream to get arguments from.
                        Otherwise looks for the --config flag in the arguments.
        usage - To display the usage.
        watch_file - If true, the config file will be watched for changes
                     that will be incorporated on the fly.
        '''
        with self.__dict__['lock']:
            if parse_command_line:
                # TODO: (robbwagoner): use super().parse_options()
                cmd_options, args = self._parse_command_line(args, usage)

                self._register_command_line_options()
            else:
                cmd_parser = DagOptionParser()
                cmd_parser.add_option('--config', '-c', default=None,
                                      help='Path to the config file')
                self.__dict__['cmd_options'], args = cmd_parser.parse_args(args)

            # Now, process the configuration file if it exists
            self._process_new_config_file(config_stream,
                                          self.cmd_options.config)

            # Start the polling thread if there is a config file to read
            if watch_file and self.cmd_options.config is not None:
                if self.__dict__['_config_poll_timer'] is not None:
                    self.__dict__['_config_poll_timer'].cancel()
                    self.__dict__['_config_poll_timer'] = None
                self._poll_config_file()

        return args


# pydevd.settrace('localhost', port=55555, stdoutToServer=True, stderrToServer=True, suspend=False)
options = DagOptions()
# options.__dict__['main_prefix'] = 'stats.airflow.dags.cluster'
options.define('quiet_period', default=45, type=int, help='Number of minutes to wait before processing the most recent'
                                                          'clicklogs.')

options.define('public_ip', default='52.5.151.15', type=str, help='')
options.define('public_dns', default='', type=str, help='')
options.define('cluster_name', default='Neon Serving Stack V2 test hadoop (Airflow)', type=str, help='')
options.define('cluster_type', default='airflow_test_hadoop', type=str, help='')
options.define('cluster_subnet_id', default='subnet-b0d884c7', type=str, help='VPC Subnet Id where to launch the EMR'
                                                                              'cluster.')
options.define('n_core_instances', default=2, type=int, help='The number of CORE instances for the EMR cluster. This'
                                                             'setting affects the total HDFS storage available to the'
                                                             'cluster.')
options.define('ssh_key', default='')
options.define('cluster_log_uri', default='s3://neon-cluster-logs', type=str, help='')
options.define('mr_jar', default='/opt/neon/neon-codebase/statsmanager/stats/java/target/neon-stats-1.0-job.jar')

options.define('input_path', default='s3://neon-tracker-logs-v2/v2.2', type=str,
               help='S3 URI base path to source files for staging.')
options.define('staging_path', default='s3://neon-tracker-logs-test-hadoop/', type=str,
               help='S3 URI base path where to stage files for input to Map/Reduce jobs.')
options.define('output_path', default='s3://neon-tracker-logs-test-hadoop/', type=str,
               help='S3 URI base path where to put cleaned files from Map/Reduce jobs.')

# Use Neon's options module for configuration parsing
f = open(os.path.join(os.path.dirname(__file__), 'cluster.conf'))  # TODO(robbwagoner): change to basename
# Don't parse command line options, because DAGs are loaded by the airflow command
args = options.parse_options(config_stream=f, watch_file=True, parse_command_line=False)
f.close()



# ----------------------------------
# NEON HELPER METHODS
# ----------------------------------

TAI = re.compile(r'^[\w]{3,15}$')  # Tracker Account Ids: alpha-numeric string 3-15 chars long
EPOCH = dateutils.timezone(datetime(1970, 1, 1), timezone='utc')
PROTECTED_PREFIXES = [r'^/$', r'v[0-9].[0-9]']


def _get_cluster():
    """
    :return: Neon cluster object
    """
    return stats.cluster.Cluster(cluster_type=options.cluster_type, cluster_name=options.cluster_name,
                                 n_core_instances=options.n_core_instances, public_ip=options.public_ip,
                                 cluster_subnet_id=options.cluster_subnet_id, cluster_log_uri=options.cluster_log_uri)


def _cluster_status():
    """Get the cluster status


        :rtype : bool
        :return: True if the cluster is running
        """
    cluster = _get_cluster()
    cluster.connect()
    return cluster.is_alive()


def _create_tables():
    """

    :param kwargs:
    :return:
    """
    cluster = _get_cluster()
    cluster.connect()
    stats.impala_table.build_impala_tables(cluster, action='build')


def _ts_to_datetime(ts):
    """
    Convert Unix Timestamp (seconds) to datetime object
    :return:
    """
    return EPOCH + timedelta(seconds=ts)


def _do_s3_prefix_fixup(prefix):
    """
    Many S3Hook.list_* methods use prefix and delimiter. The prefix should lack a leading slash and have a trailing
    slash.
    :param prefix: S3 prefix string
    :return:
    """
    ret = prefix.lstrip('/')
    if ret == '' or ret[-1] != '/':
        ret += '/'
    return ret


def _get_s3_cleaned_prefix(dag, execution_date, prefix=''):
    """
    Generate an S3 key prefix, specific to the DAG,for the cleaned/merged click event files.
    :param dag:
    :param execution_date:
    :param prefix:
    :return: S3 key prefix string
    :return type: str
    """
    return _do_s3_prefix_fixup(os.path.join(prefix, dag.dag_id, 'cleaned',
                                            execution_date.strftime("%Y/%m/%d/%H"), ''))


def _get_s3_input_files(dag, execution_date, task, input_path):
    """
    Get the list of input files for the task
    :param dag:
    :param execution_date:
    :param task: the
    :param input_path: the base path S3 URL for input files to the DAG
    :return:
    """

    bucket_name, prefix = _get_s3_tuple(input_path)

    # Get the timestamps for our current task
    dt_start = execution_date
    ts_start = int((dateutils.timezone(dt_start, timezone='utc') - EPOCH).total_seconds())
    ts_end = int(((dateutils.timezone(dt_start, timezone='utc') + clicklogs.schedule_interval) - EPOCH).total_seconds())

    s3 = S3Hook(s3_conn_id='s3')

    input_files = []
    for tai in _get_s3_tais(input_path):
        tai_prefix = os.path.join(prefix, tai, dt_start.strftime('%Y/%m/%d'), '')
        _log.debug("looking for keys in s3://{bucket}/{prefix}".format(bucket=bucket_name, prefix=tai_prefix))
        try:
            for key in s3.list_keys(bucket_name=bucket_name, prefix=tai_prefix, delimiter='/'):
                # (merged.)clicklog.<TIMESTAMP_MS>.avro
                if key[-1] == '/':
                    continue  # just the "directory" prefix, not an actual object (file)
                tokens = os.path.basename(key).split('.')
                if tokens[-1] == 'avro':
                    try:
                        ts = int(tokens[-2])  # timestamp(ms) is just before the .avro extension
                    except ValueError:
                        _log.warning("{task}: file {key} lacks a timestamp in its name".format(task=task, key=key))
                        continue
                    _log.debug("{task}: considering avro file {key} ({dt})".format(task=task, key=key,
                                                                                  dt=_ts_to_datetime(ts / 10 ** 3)))
                    if ts_start <= (ts / 10 ** 3) < ts_end:
                        # the timestamp, at second-resolution, is between our ds
                        input_files.append(key)
                        _log.info(
                            "{task}: file {key} is between {start} and {end}".format(task=task, key=key, start=ts_start,
                                                                                     end=ts_end))
        except TypeError as e:
            _log.warning("tai {tai} does not have any objects in S3 prefix {prefix}".format(tai=tai, prefix=tai_prefix))

    return input_files


def _get_s3_staging_prefix(dag, execution_date, prefix=''):
    """
    Generate an S3 key prefix, specific to the DAG execution date, for staging files into a location which can be
    consumed byMapReduce clean/merge.
    job.
    :param dag:
    :param execution_date:
    :param prefix:
    :return: S3 key prefix string
    :return type: str
    """
    return _do_s3_prefix_fixup(os.path.join(prefix, dag.dag_id, 'staging',
                                            execution_date.strftime("%Y/%m/%d/%H"), ''))


def _get_s3_tais(input_path):
    """
    Get the list of Tracker Account Ids for a given input path.
    :param input_path:
    :return:
    """
    bucket_name, prefix = _get_s3_tuple(input_path)

    s3 = S3Hook(s3_conn_id='s3')

    # get the TAIs (Tracker Account Id)
    tais = []
    for key in s3.list_prefixes(bucket_name=bucket_name, prefix=prefix, delimiter='/'):
        tokens = key.split('/')
        if re.search(TAI, tokens[1]):
            tais.append(tokens[1])
    return tais


def _get_s3_tuple(url):
    """
    For an S3 URL return a tuple of bucket name and prefix
    :return: tuple of bucket name, prefix
    :return_type: tuple
    """
    u = urlparse.urlparse(url)
    return u.netloc, _do_s3_prefix_fixup(u.path)


def _delete_s3_prefix(bucket, prefix):
    """

    :param prefix: S3 Key prefix
    :param type: str
    :return:
    """
    for protected in PROTECTED_PREFIXES:
        if re.search(protected, prefix):
            _log.error("deletion of {bucket} prefix {prefix} forbidden".format(bucket=bucket, prefix=prefix))
            return False

    s3 = S3Hook(s3_conn_id='s3')

    for key in s3.get_bucket(bucket).list(prefix):
        key.delete()

    return True


def _run_cluster_command(cmd, **kwargs):
    """
    Run a command on the EMR cluster
    :return:
    """
    cluster = _get_cluster()
    cluster.connect()
    ssh_conn = stats.cluster.ClusterSSHConnection(cluster)
    ssh_conn.execute_remote_command(cmd)


def _check_compute_cluster_capacity(op_kwargs):
    """

    :return:
    """
    ti = op_kwargs['task_instance']
    cluster = _get_cluster()
    cluster.connect()
    if False:
        cluster.change_instance_group_size(group_type='TASK', incr_amount=1)
    # else:
    #     cluster.change_instance_group_size(group_type='TASK', new_size=2)


def _delete_previously_cleaned_files(dag, execution_date, output_path):
    """Cleanup output from previously executed Map/Reduce jobs for the same execution_date
    """
    output_bucket, output_prefix = _get_s3_tuple(output_path)
    cleaned_prefix = _get_s3_cleaned_prefix(dag=dag, execution_date=execution_date, prefix=output_prefix)

    s3 = S3Hook(s3_conn_id='s3')

    _log.debug('deleting previously cleaned files from prefix {prefix}'.format(prefix=cleaned_prefix))
    for key in s3.get_bucket(output_bucket).list(prefix=cleaned_prefix):
        _log.debug('key {key} found'.format(key=key.name))
        # _log.warning("{task}: would delete key {key}".format(task=task, key=key))
        if not re.search('cleaned', key.name):
            _log.error('key prefix, {key}, does not contain the string \'cleaned\''.format(key=key.name))
            return False
        _log.debug('deleting {key}'.format(key=key.name))
        key.delete()

    # delete the _$folder$ key if it exists
    folder_key = s3.get_key(key='{prefix}_$folder$'.format(prefix=cleaned_prefix.rstrip('/')),
                            bucket_name=output_bucket)
    if folder_key:
        folder_key.delete()


# ----------------------------------
# PythonOperator callables
# ----------------------------------
def _quiet_period(**kwargs):
    """
    Sleep while waiting for Trackserver/Flume to write merged.clicklog.*.avro files to S3.
    Typical delay is approximately 30 minutes. Recommended quiet period is 45 minutes.
    :param kwargs:
    :return:
    """
    deadline = kwargs['execution_date'] + kwargs['dag'].schedule_interval + kwargs['quiet_period']
    now = datetime.now()
    if now < deadline:
        _log.info("waiting for clicklog files to be written to S3 (quiet period): {deadline}".format(deadline=deadline))
        time.sleep((deadline - now).total_seconds())


def _stage_files(**kwargs):
    """Copy input path files to staging area for the execution date

    :return:
    """
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    input_bucket, input_prefix = _get_s3_tuple(kwargs['input_path'])
    staging_bucket, staging_prefix = _get_s3_tuple(kwargs['staging_path'])

    s3 = S3Hook(s3_conn_id='s3')

    _log.info("{task}: staging files for map/reduce".format(task=task))
    input_files = _get_s3_input_files(dag=dag, execution_date=execution_date, task=task,
                                      input_path=kwargs['input_path'])
    output_prefix = _get_s3_staging_prefix(dag=dag, execution_date=execution_date, prefix=staging_prefix)

    # Copy files to staging location with Reduced Redundancy enabled
    if input_files:
        for key in input_files:
            # namespace prefix with TAI
            tai = key.lstrip(input_prefix).split('/')[0]
            dst_key = os.path.join(output_prefix, "{tai}.{name}".format(tai=tai, name=os.path.basename(key)))

            _log.info("copying from s3://{src_bucket}/{src} to s3://{dst_bucket}/{dst}".format(src_bucket=input_bucket,
                                                                                               src=key,
                                                                                               dst_bucket=staging_bucket,
                                                                                               dst=dst_key))
            s3.get_key(key, bucket_name=input_bucket).copy(dst_bucket=staging_bucket,
                                                           dst_key=dst_key,
                                                           reduced_redundancy=True)
    else:
        _log.warning("{task}: there were no files to stage".format(task=task))


def _run_mr_cleaning_job(**kwargs):
    """Run the Map/Reduce cleaning job"""
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    staging_bucket, staging_prefix = _get_s3_tuple(kwargs['staging_path'])
    output_bucket, output_prefix = _get_s3_tuple(kwargs['output_path'])

    staging_prefix = _get_s3_staging_prefix(dag=dag, execution_date=execution_date, prefix=staging_prefix)
    cleaned_prefix = _get_s3_cleaned_prefix(dag=dag, execution_date=execution_date, prefix=output_prefix)

    cleaning_job_input_path = os.path.join("s3://", staging_bucket, staging_prefix, '*')
    cleaning_job_output_path = os.path.join("s3://", output_bucket, cleaned_prefix)

    _delete_previously_cleaned_files(dag=dag, execution_date=execution_date, output_path=kwargs['output_path'])
    _log.info("{task}: calling Neon Map/Reduce clicklogs cleaning job".format(task=task))
    cluster = _get_cluster()
    cluster.connect()
    stats.impala_table.run_batch_cleaning_job(cluster, cleaning_job_input_path, cleaning_job_output_path,
                                              jar=options.mr_jar, timeout=kwargs['timeout'])


def _load_impala_tables(**kwargs):
    """
    Load the Impala tables for the execution_date
    :param kwargs:
    :return:
    """
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    output_bucket, output_prefix = _get_s3_tuple(kwargs['output_path'])
    cleaned_prefix = _get_s3_cleaned_prefix(dag=dag, execution_date=execution_date, prefix=output_prefix)

    _log.info("{task}: Loading data!".format(task=task))
    cluster = _get_cluster()
    cluster.connect()
    stats.impala_table.build_impala_tables(cluster, action='load', execution_date=execution_date,
                                           hour_interval=dag.schedule_interval.total_seconds()/3600,
                                           input_path=os.path.join('s3://', output_bucket, cleaned_prefix))
    return "Impala tables loaded"


def _delete_staging_files(**kwargs):
    """
    Cleanup (delete) objects in S3 from S3 staging area for a task
    :param kwargs:
    :return:
    """
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    staging_bucket, staging_prefix = _get_s3_tuple(kwargs['staging_path'])

    staging_full_prefix = _get_s3_staging_prefix(dag, execution_date, staging_prefix)
    return _delete_s3_prefix(staging_bucket, staging_full_prefix)


def _execution_date_has_input_files(**kwargs):
    """
    If the execution date has input files, select the processing branch. If not, send to the no input files end.
    :param kwargs:
    :return:
    """
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    input_path = kwargs['input_path']

    input_bucket, input_prefix = _get_s3_tuple(kwargs['input_path'])

    _log.info("{task}: checking for input files for the execution date {ed}".format(task=task, ed=execution_date))
    input_files = _get_s3_input_files(dag=dag, execution_date=execution_date, task=task, input_path=input_path)
    if input_files:
        return 'stage_files'
    else:
        return 'no_input_files'


def _hdfs_maintenance(**kwargs):
    """
    Do HDFS maintenance
    :return:
    """
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    # remove old temporary files
    days_ago = execution_date - timedelta(days=kwargs['days_ago'])
    tmp_dir = '/tmp/hadoop-yarn/staging/done/{year}/{month}/{day}'.format(
        year=days_ago.strftime('%Y'),
        month=days_ago.strftime('%m'),
        day=days_ago.strftime('%d'))
    _log.info('{task}: remove old temporary files from HDFS'.format(task=task))
    remove_old_tmp = 'if hdfs dfs -test -d {tmp_dir} ; then hdfs dfs -rm -r ${tmp_dir} ; fi'.format(tmp_dir=tmp_dir)
    _run_cluster_command(remove_old_tmp)



# ----------------------------------
# AIRFLOW
# ----------------------------------
default_args = {
    'owner': 'robb',
    'start_date': datetime(2015, 7, 1),
    'email': ['robb@pandastrike.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_interval': timedelta(minutes=1),

    # 'execution_timeout': timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}



# ----------------------------------
# cluster - restart the cluster if it goes away
# ----------------------------------
cluster = DAG('cluster', default_args=default_args,
                schedule_interval=timedelta(hours=1))

# Start the EMR cluster if it isn't running
cluster_cluster_start = PythonOperator(
    task_id='cluster_start',
    dag=cluster,
    python_callable=_cluster_status,
    execution_timeout=timedelta(hours=1))
# Use for alarming on failure
# on_failure_callback=

# Create the Impala Parquet-formatted tables if they don't exist
cluster_create_tables = PythonOperator(
    task_id='create_tables',
    dag=cluster,
    python_callable=_create_tables)
cluster_create_tables.set_upstream(cluster_cluster_start)

# Create Cloudwatch Alarms for the cluster
cluster_cloudwatch_metrics = DummyOperator(
    task_id='cloudwatch_metrics',
    dag=cluster)
cluster_cloudwatch_metrics.set_upstream(cluster_cluster_start)



# ----------------------------------
# clicklogs - stage and clean event clickstream logs (aka 'clicklogs')
# ----------------------------------
clicklogs = DAG('clicklogs', default_args=default_args,
                schedule_interval=timedelta(hours=3))

# Start the EMR cluster if it isn't running
cluster_start = PythonOperator(
    task_id='cluster_start',
    dag=clicklogs,
    python_callable=_cluster_status,
    execution_timeout=timedelta(hours=1))
# Use for alarming on failure
# on_failure_callback=

# Create the Impala Parquet-formatted tables if they don't exist
create_tables = PythonOperator(
    task_id='create_tables',
    dag=clicklogs,
    python_callable=_create_tables)
create_tables.set_upstream(cluster_start)

# Create Cloudwatch Alarms for the cluster
cloudwatch_metrics = DummyOperator(
    task_id='cloudwatch_metrics',
    dag=clicklogs)
cloudwatch_metrics.set_upstream(cluster_start)

# Wait a while after the execution date interval has passed before processing to allow Trackserver/Flume to
# transmit log files to be to S3.
quiet_period = PythonOperator(
    task_id='quiet_period',
    dag=clicklogs,
    python_callable=_quiet_period,
    provide_context=True,
    op_kwargs=dict(quiet_period=timedelta(minutes=options.quiet_period)))
quiet_period.set_upstream(create_tables)

# Determine if the execution date has input files
has_input_files = BranchPythonOperator(
    task_id='has_input_files',
    dag=clicklogs,
    python_callable=_execution_date_has_input_files,
    provide_context=True,
    op_kwargs=dict(input_path=options.input_path))
has_input_files.set_upstream(quiet_period)

# Copy files for the execution date from S3 source location in to an S3 staging location
stage_files = PythonOperator(
    task_id='stage_files',
    dag=clicklogs,
    python_callable=_stage_files,
    provide_context=True,
    op_kwargs=dict(input_path=options.input_path, staging_path=options.staging_path))
stage_files.set_upstream(has_input_files)

# End point for execution dates which don't have input files
no_input_files = DummyOperator(
    task_id='no_input_files',
    dag=clicklogs)
no_input_files.set_upstream(has_input_files)


# Run the Map/Reduce cleaning job on the staged files
mr_cleaning_job = PythonOperator(
    task_id='mr_cleaning_job',
    dag=clicklogs,
    python_callable=_run_mr_cleaning_job,
    provide_context=True,
    op_kwargs=dict(staging_path=options.staging_path, output_path=options.output_path, timeout=60 * 60),
    retry_delay=timedelta(seconds=random.randrange(30,300,step=30)),
    execution_timeout=timedelta(minutes=75),
    on_failure_callback=_check_compute_cluster_capacity,
    on_success_callback=_check_compute_cluster_capacity,
    on_retry_callback=_check_compute_cluster_capacity)
    # depends_on_past=True) # depend on past task executions to serialize the mr_cleaning process
mr_cleaning_job.set_upstream(stage_files)


# Load the cleaned files from Map/Reduce into Impala
load_impala_tables = PythonOperator(
    task_id='load_impala_tables',
    dag=clicklogs,
    python_callable=_load_impala_tables,
    provide_context=True,
    op_kwargs=dict(output_path=options.output_path),
    retry_delay=timedelta(seconds=random.randrange(30,300,step=30)),
    sla=timedelta(minutes=options.quiet_period+90))
    # depends_on_past=True)
load_impala_tables.set_upstream(mr_cleaning_job)


# Delete the staging files once the Map/Reduce job is done
delete_staging_files = PythonOperator(
    task_id='delete_staging_files',
    dag=clicklogs,
    python_callable=_delete_staging_files,
    provide_context=True,
    op_kwargs=dict(staging_path=options.staging_path))
delete_staging_files.set_upstream(mr_cleaning_job)


hdfs_maintenance = PythonOperator(
    task_id='hdfs_maintenance',
    dag=clicklogs,
    python_callable=_hdfs_maintenance,
    provide_context=True,
    op_kwargs=dict(days_ago=30))
hdfs_maintenance.set_upstream(load_impala_tables)

