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

# PyCharm Debugger
#sys.path.append(os.path.join(__base_path__, 'pycharm-debug.egg'))
#import pydevd

from airflow import DAG
from airflow.configuration import conf, AirflowConfigException
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
import stats.impala_table
import socket
import time
import urlparse
import utils.logs
import utils.monitor
from utils import statemon
import os

_log = logging.getLogger('airflow.dag.clicklogs')

statemon.define('stats_cleaning_job_failures', int)
statemon.define('impala_table_load_failure', int)


# ----------------------------------------
# Options and configurations with DAGs
# ----------------------------------------
"""
For a DAG to be able to use the Neon utils.options module, two wrapper objects
are necessary to allow the eponymous Airflow command to work as normal and not
 cause an error with the OptionParser().
"""
from utils.options import options

options.define('quiet_period', default=45, type=int,
               help=('Number of minutes to wait before processing the most '
                     'recent clicklogs.'))
options.define('mr_jar', default='neon-stats-1.0-job.jar', type=str,
               help=('Jar for the map reduce cleaning job to run relative to '
                     'stats/java/target/'))
options.define('input_path', default='s3://neon-tracker-logs-v2/v2.2',
               type=str,
               help='S3 URI base path to source files for staging.')
options.define('staging_path', default='s3://neon-tracker-logs-test-hadoop/',
               type=str,
               help=('S3 URI base path where to stage files for input to '
                     'Map/Reduce jobs. Staged files, relative to this '
                     'setting, are prefixed '
                     '<dag_id>/staging/<YYYY>/<MM>/<DD>/<HH>/.'))
options.define('output_path', default='s3://neon-tracker-logs-test-hadoop/',
               type=str,
               help=('S3 URI base path where to put cleaned files from '
                     'Map/Reduce jobs. Cleaned files, relative to this '
                     'setting, are prefixed '
                     '<dag_id>/cleaned/<YYYY>/<MM>/<DD>/<HH>/'))
options.define('cleaning_mr_memory', default=2048, type=int,
               help='Memory in MB needed for the map of the cleaning job')
options.define('clicklog_period', default=3, type=int,
               help='How often to run the clicklog job in hours')

# Use Neon's options module for configuration parsing
try:
    args = options.parse_options(['-c', conf.get('neon', 'config_file')],
                                 watch_file=True)
except AirflowConfigException:
    # No config file specified, so we'll just use the defaults from above
    pass
# The rest of Neon Init
socket.setdefaulttimeout(30)
utils.logs.AddConfiguredLogger()
utils.monitor.start_agent()



# ----------------------------------
# NEON HELPER METHODS
# ----------------------------------

__EVENTS = ['ImageLoad', 'ImageVisible','ImageClick', 'AdPlay', 'VideoPlay',
          'VideoViewPercentage', 'EventSequence']

# Tracker Account Ids: alpha-numeric string 3-15 chars long
TAI = re.compile(r'^[\w]{3,15}$')  
EPOCH = dateutils.timezone(datetime(1970, 1, 1), timezone='utc')
PROTECTED_PREFIXES = [r'^/$', r'v[0-9].[0-9]']

class ClusterDown(Exception): pass

class ClusterGetter(object):
    cluster = None

    @classmethod
    def get_cluster(cls):
        if cls.cluster is None:
            cls.cluster = stats.cluster.Cluster()
        return cls.cluster


def _cluster_status():
    """Get the cluster status


        :rtype : bool
        :return: True if the cluster is running
        """
    cluster = ClusterGetter.get_cluster()
    if not cluster.is_alive():
        raise ClusterDown()
    return True


def _create_tables(**kwargs):
    """

    :param kwargs:
    :return:
    """
    event = kwargs['event']
    cluster = ClusterGetter.get_cluster()
    cluster.connect(os.path.basename(__file__))

    builder = stats.impala_table.ImpalaTableBuilder(cluster, event)
    builder.run()


def _ts_to_datetime(ts):
    """
    Convert Unix Timestamp (seconds) to datetime object
    :return:
    """
    return EPOCH + timedelta(seconds=ts)


def _do_s3_prefix_fixup(prefix):
    """
    Many S3Hook.list_* methods use prefix and delimiter. The prefix
    should lack a leading slash and have a trailing slash.
    
    :param prefix: S3 prefix string
    :return:
    """
    ret = prefix.lstrip('/')
    if ret == '' or ret[-1] != '/':
        ret += '/'
    return ret


def _get_s3_cleaned_prefix(dag, execution_date, prefix=''):
    """
    Generate an S3 key prefix, specific to the DAG,for the
    cleaned/merged click event files.
    
    :param dag:
    :param execution_date:
    :param prefix:
    :return: S3 key prefix string
    :return type: str
    """
    return _do_s3_prefix_fixup(
        os.path.join(prefix, dag.dag_id, 'cleaned',
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

    s3 = S3Hook(s3_conn_id='s3')

    # # Get the timestamps for our current task
    # dt_start = execution_date
    # ts_start = int((dateutils.timezone(dt_start, timezone='utc') - 
    #                 EPOCH).total_seconds())
    # ts_end = int(((dateutils.timezone(dt_start, timezone='utc') + 
    #                clicklogs.schedule_interval) - EPOCH).total_seconds())

    # input_files = []
    # for tai in _get_s3_tais(input_path):
    #     tai_prefix = os.path.join(prefix, tai, dt_start.strftime('%Y/%m/%d'),
    #                               '')
    #     _log.debug("looking for keys in s3://{bucket}/{prefix}".format(
    #         bucket=bucket_name, prefix=tai_prefix))
    #     try:
    #         for key in s3.list_keys(bucket_name=bucket_name,
    #                                 prefix=tai_prefix, delimiter='/'):
    #             # (merged.)clicklog.<TIMESTAMP_MS>.avro
    #             if key[-1] == '/':
    #                 # just the "directory" prefix, not an actual object (file)
    #                 continue  
    #             tokens = os.path.basename(key).split('.')
    #             if tokens[-1] == 'avro':
    #                 try:
    #                     # timestamp(ms) is just before the .avro extension
    #                     ts = int(tokens[-2])  
    #                 except ValueError:
    #                     _log.warning(("{task}: file {key} lacks a timestamp "
    #                                  "in its name").format(task=task, key=key))
    #                     continue
    #                 _log.debug(("{task}: considering avro file {key} "
    #                             "({dt})").format(
    #                                 task=task, key=key,
    #                                 dt=_ts_to_datetime(ts / 10 ** 3)))
    #                 if ts_start <= (ts / 10 ** 3) < ts_end:
    #                     # the timestamp, at second-resolution, is
    #                     # between our ds
    #                     input_files.append(key)
    #                     _log.info(("{task}: file {key} is between {start} and "
    #                               "{end}").format(task=task, key=key,
    #                                               start=ts_start,
    #                                               end=ts_end))
    #     except TypeError as e:
    #         _log.warning(("tai {tai} does not have any objects in S3 prefix "
    #                      "{prefix}").format(tai=tai, prefix=tai_prefix))

    # return input_files

    #Get the current date
    #processing_date = datetime.utcnow().strftime("%Y/%m/%d")
    processing_date = execution_date.strftime('%Y/%m/%d')
    _log.info('processing date is %s' % processing_date)

    input_files = []
    for tai in _get_s3_tais(input_path):
        tai_prefix = os.path.join(prefix, tai, processing_date, '')

        #check if the prefix exists
        prefix_exists = s3.check_for_prefix(prefix=tai_prefix, 
                                            bucket_name=bucket_name, 
                                            delimiter='/')

        # Get the S3 keys only if prefix exists
        if prefix_exists:
            for key in s3.list_keys(bucket_name=bucket_name, 
                                    prefix=tai_prefix, 
                                    delimiter='/'):
                input_files.append(key)
        else:
            _log.info(("tai {tai} does not have any objects in S3 prefix "
                          "{prefix}").format(tai=tai, prefix=tai_prefix))

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
    return _do_s3_prefix_fixup(os.path.join(prefix,
                                            execution_date.strftime("%Y/%m/%d/%H"), ''))


def _get_s3_tais(input_path):
    """
    Get the list of Tracker Account Ids for a given input path.
    :param input_path:
    :return:
    """
    bucket_name, prefix = _get_s3_tuple(input_path)

    s3 = S3Hook(s3_conn_id='s3')

    _log.info('bucket_name is %s' % bucket_name)
    _log.info('prefix is %s' % prefix)
    _log.info('input path is %s' % input_path)

    # get the TAIs (Tracker Account Id)
    tais = []
    for key in s3.list_prefixes(bucket_name=bucket_name, prefix=prefix,
                                delimiter='/'):
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
            _log.error("deletion of {bucket} prefix {prefix} forbidden".format(
                bucket=bucket, prefix=prefix))
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
    cluster = ClusterGetter.get_cluster()
    cluster.connect(os.path.basename(__file__))
    ssh_conn = stats.cluster.ClusterSSHConnection(cluster)
    ssh_conn.execute_remote_command(cmd)


def _check_compute_cluster_capacity(op_kwargs):
    """

    :return:
    """
    ti = op_kwargs['task_instance']
    cluster = ClusterGetter.get_cluster()
    cluster.connect(os.path.basename(__file__))
    if False:
        cluster.change_instance_group_size(group_type='TASK', incr_amount=1)
    # else:
    #     cluster.change_instance_group_size(group_type='TASK', new_size=2)


def _delete_previously_cleaned_files(dag, execution_date, output_path):
    """

    Cleanup output from previously executed Map/Reduce jobs for the
    same execution_date
    
    """
    output_bucket, output_prefix = _get_s3_tuple(output_path)
    cleaned_prefix = _get_s3_cleaned_prefix(dag=dag,
                                            execution_date=execution_date,
                                            prefix=output_prefix)
    _log.info('d output bucket is %s' % output_bucket)
    _log.info('d cleaned prefix is %s' % cleaned_prefix)
    _log.info('d output prefix is %s' % output_prefix)

    s3 = S3Hook(s3_conn_id='s3')

    _log.debug('deleting previously cleaned files from prefix {prefix}'.format(
        prefix=cleaned_prefix))
    for key in s3.get_bucket(output_bucket).list(prefix=cleaned_prefix):
        _log.info('key {key} found'.format(key=key.name))
        if not re.search('cleaned', key.name):
            _log.error(('key prefix, {key}, does not contain the string '
                       '\'cleaned\'').format(key=key.name))
            return False
        _log.debug('deleting {key}'.format(key=key.name))
        key.delete()

    # delete the _$folder$ key if it exists
    folder_key = s3.get_key(key='{prefix}_$folder$'.format(
        prefix=cleaned_prefix.rstrip('/')),
        bucket_name=output_bucket)
    if folder_key:
        folder_key.delete()


# ----------------------------------
# PythonOperator callables
# ----------------------------------
def _quiet_period(**kwargs):
    """
    Sleep while waiting for Trackserver/Flume to write
    merged.clicklog.*.avro files to S3.  Typical delay is
    approximately 30 minutes. Recommended quiet period is 45 minutes.
    
    :param kwargs:
    :return:
    """
    deadline = (kwargs['execution_date'] + kwargs['dag'].schedule_interval + 
                kwargs['quiet_period'])
    now = datetime.now()
    if now < deadline:
        _log.info("waiting for clicklog files to be written to S3 (quiet period): {deadline}".format(deadline=deadline))
        time.sleep((deadline - now).total_seconds())


# def _stage_files(**kwargs):
#     """Copy input path files to staging area for the execution date

#     :return:
#     """
#     dag = kwargs['dag']
#     execution_date = kwargs['execution_date']
#     task = kwargs['task_instance_key_str']

#     input_bucket, input_prefix = _get_s3_tuple(kwargs['input_path'])
#     staging_bucket, staging_prefix = _get_s3_tuple(kwargs['staging_path'])

#     s3 = S3Hook(s3_conn_id='s3')

#     _log.info("{task}: staging files for map/reduce".format(task=task))
#     input_files = _get_s3_input_files(dag=dag,
#                                       execution_date=execution_date,
#                                       task=task,
#                                       input_path=kwargs['input_path'])
#     output_prefix = _get_s3_staging_prefix(dag=dag,
#                                            execution_date=execution_date,
#                                            prefix=staging_prefix)

#     # Copy files to staging location with Reduced Redundancy enabled
#     if input_files:
#         for key in input_files:
#             # namespace prefix with TAI
#             tai = key.lstrip(input_prefix).split('/')[0]
#             dst_key = os.path.join(output_prefix, "{tai}.{name}".format(
#                 tai=tai, name=os.path.basename(key)))

#             _log.info(("copying from s3://{src_bucket}/{src} to "
#                        "s3://{dst_bucket}/{dst}").format(
#                            src_bucket=input_bucket,
#                            src=key,
#                            dst_bucket=staging_bucket,
#                            dst=dst_key))
#             s3.get_key(key, bucket_name=input_bucket).copy(
#                 dst_bucket=staging_bucket,
#                 dst_key=dst_key,
#                 reduced_redundancy=True)
#     else:
#         _log.warning("{task}: there were no files to stage".format(task=task))

def _stage_files(**kwargs):
    """Copy input path files to staging area for the execution date
    """
    s3 = S3Hook(s3_conn_id='s3')

    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    input_bucket, input_prefix = _get_s3_tuple(kwargs['input_path'])
    staging_bucket, staging_prefix = _get_s3_tuple(kwargs['staging_path'])

    input_files = _get_s3_input_files(dag=dag,
                                      execution_date=execution_date,
                                      task=task,
                                      input_path=kwargs['input_path'])

    output_prefix = _get_s3_staging_prefix(dag=dag,
                                           execution_date=execution_date,
                                           prefix=staging_prefix)

    # Copy files to staging location
    if input_files:
        for keys_to_copy in input_files:
            keys_to_copy_split = keys_to_copy.split('/')

            bucket = s3.get_bucket(input_bucket)

            _log.info(("Copying from bucket {src_bucket} to {dest_bucket}").format(src_bucket=keys_to_copy, 
                                                                                dest_bucket=os.path.join(output_prefix, keys_to_copy_split[-1])))

            _log.info('Output S3 prefix is %s' % output_prefix)

            bucket.copy_key(os.path.join(output_prefix, keys_to_copy_split[-1]), 
                            str(bucket.name), 
                            keys_to_copy,
                            storage_class='REDUCED_REDUNDANCY')
    else:
        _log.warning("{task}: there were no files to stage".format(task=task))


def _run_mr_cleaning_job(**kwargs):
    """Run the Map/Reduce cleaning job"""
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    staging_bucket, staging_prefix = _get_s3_tuple(kwargs['staging_path'])
    output_bucket, output_prefix = _get_s3_tuple(kwargs['output_path'])

    cluster = ClusterGetter.get_cluster()
    cluster.connect(os.path.basename(__file__))

    # hdfs_path = 'hdfs://%s:9000' % cluster.master_ip
    # hdfs_dir = 'mnt/cleaned'
    # run_time = time.strftime("%Y-%m-%d-%H-%M")

    # cleaned_output_path = "%s/%s/%s" % (
    #                 hdfs_path,
    #                 hdfs_dir,
    #                 run_time)

    #_log.info('cleaned output path is %s' % cleaned_output_path)

    staging_prefix = _get_s3_staging_prefix(dag=dag,
                                            execution_date=execution_date,
                                            prefix=staging_prefix)
    cleaned_prefix = _get_s3_cleaned_prefix(dag=dag,
                                            execution_date=execution_date,
                                            prefix=output_prefix)

    cleaning_job_input_path = os.path.join("s3://", staging_bucket,
                                           staging_prefix, '*')
    cleaning_job_output_path = os.path.join("s3://", output_bucket,
                                            cleaned_prefix)

    _delete_previously_cleaned_files(dag=dag, execution_date=execution_date,
                                     output_path=kwargs['output_path'])
    _log.info("{task}: calling Neon Map/Reduce clicklogs cleaning job".format(
        task=task))

    _log.info("Output of clean up job goes to %s" % cleaning_job_output_path)
    _log.info("Output bucket is %s" % output_bucket)
    _log.info("Output prefix is %s" % cleaned_prefix)

    jar_path = os.path.join(os.path.dirname(__file__), '..', '..', 'java',
                            'target', options.mr_jar)
    try:
        cluster.run_map_reduce_job(jar_path,
                                   'com.neon.stats.RawTrackerMR',
                                   cleaning_job_input_path,
                                   cleaning_job_output_path,
                                   #cleaned_output_path,
                                   map_memory_mb=options.cleaning_mr_memory,
                                   timeout=kwargs['timeout'])
    except Exception as e:
        _log.error('Error running the batch cleaning job: %s' % e)
        statemon.state.increment('stats_cleaning_job_failures')
        raise
    _log.info('Batch event cleaning job done')


def _load_impala_table(**kwargs):
    """
    Load the Impala tables for the execution_date
    :param kwargs:
    :return:
    """
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']
    event = kwargs['event']

    output_bucket, output_prefix = _get_s3_tuple(kwargs['output_path'])
    cleaned_prefix = _get_s3_cleaned_prefix(dag=dag,
                                            execution_date=execution_date,
                                            prefix=output_prefix)

    _log.info("{task}: Loading data!".format(task=task))
    cluster = ClusterGetter.get_cluster()
    cluster.connect(os.path.basename(__file__))
    try:
        _log.info("Path to build for impala is %s" % os.path.join('s3://', output_bucket, cleaned_prefix))

        builder = stats.impala_table.ImpalaTableLoader(
            cluster=cluster,
            event=event,
            #execution_date=executation_date,
            execution_date=execution_date,
            hour_interval=dag.schedule_interval.total_seconds()/3600,
            input_path=os.path.join('s3://', output_bucket, cleaned_prefix))

        _log.info("Before run")
    
        builder.run()

        _log.info("After run")
    except:
        statemon.state.increment('impala_table_load_failure')
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

    staging_full_prefix = _get_s3_staging_prefix(dag, execution_date,
                                                 staging_prefix)
    return _delete_s3_prefix(staging_bucket, staging_full_prefix)


def _execution_date_has_input_files(**kwargs):
    """
    If the execution date has input files, select the processing
    branch. If not, send to the no input files end.
    
    :param kwargs:
    :return:
    """
    dag = kwargs['dag']
    execution_date = kwargs['execution_date']
    task = kwargs['task_instance_key_str']

    input_path = kwargs['input_path']

    input_bucket, input_prefix = _get_s3_tuple(kwargs['input_path'])

    _log.info(("{task}: checking for input files for the execution date "
               "{ed}").format(task=task, ed=execution_date))
    input_files = _get_s3_input_files(dag=dag, execution_date=execution_date,
                                      task=task, input_path=input_path)
    if input_files:
        return 'stage_files'
    else:
        return 'no_input_files'

def _update_table_build_times(**kwargs):
    cluster = ClusterGetter.get_cluster()
    cluster.connect(os.path.basename(__file__))
    stats.impala_table.update_table_build_times(cluster)


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
    remove_old_tmp = ('if hdfs dfs -test -d {tmp_dir} ; '
                      'then hdfs dfs -rm -r ${tmp_dir} ; fi').format(
                          tmp_dir=tmp_dir)
    _run_cluster_command(remove_old_tmp)

def _clear_all_tasks(operators=None, **kwargs):
    for op in operators:
        op.clear(**kwargs)


# ----------------------------------
# AIRFLOW
# ----------------------------------
default_args = {
    'owner': 'Ops',
    'start_date': datetime(2014, 5, 26),
    'email': ['nazeer@neon-lab.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_interval': timedelta(minutes=1),

    # 'execution_timeout': timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}


# ----------------------------------
# clicklogs - stage and clean event clickstream logs (aka 'clicklogs')
# ----------------------------------
clicklogs = DAG('clicklogs', default_args=default_args,
                schedule_interval=timedelta(minutes=options.clicklog_period))

# TODO(mdesnoyer): Delete this because a separate process should
# handle bringing the cluster back up.

# Start the EMR cluster if it isn't running
check_cluster = PythonOperator(
    task_id='check_cluster',
    dag=clicklogs,
    python_callable=_cluster_status,
    execution_timeout=timedelta(hours=1))
# Use for alarming on failure
# on_failure_callback=

# Create Cloudwatch Alarms for the cluster
cloudwatch_metrics = DummyOperator(
    task_id='cloudwatch_metrics',
    dag=clicklogs)
cloudwatch_metrics.set_upstream(check_cluster)

# Wait a while after the execution date interval has passed before
# processing to allow Trackserver/Flume to transmit log files to be to
# S3.
# quiet_period = PythonOperator(
#     task_id='quiet_period',
#     dag=clicklogs,
#     python_callable=_quiet_period,
#     provide_context=True,
#     op_kwargs=dict(quiet_period=timedelta(minutes=options.quiet_period)))
# quiet_period.set_upstream(check_cluster)

# Determine if the execution date has input files
has_input_files = BranchPythonOperator(
    task_id='has_input_files',
    dag=clicklogs,
    python_callable=_execution_date_has_input_files,
    provide_context=True,
    op_kwargs=dict(input_path=options.input_path))
has_input_files.set_upstream(cloudwatch_metrics)

# Copy files for the execution date from S3 source location in to an
# S3 staging location
stage_files = PythonOperator(
    task_id='stage_files',
    dag=clicklogs,
    python_callable=_stage_files,
    provide_context=True,
    op_kwargs=dict(input_path=options.input_path,
                   staging_path=options.staging_path))
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
    op_kwargs=dict(staging_path=options.staging_path,
                   output_path=options.output_path, timeout=60 * 60),
    retry_delay=timedelta(seconds=random.randrange(30,300,step=10)),
    execution_timeout=timedelta(minutes=75),
    on_failure_callback=_check_compute_cluster_capacity,
    on_success_callback=_check_compute_cluster_capacity,
    on_retry_callback=_check_compute_cluster_capacity)
    # depends_on_past=True) # depend on past task executions to serialize the mr_cleaning process
mr_cleaning_job.set_upstream(stage_files)


# Load the cleaned files from Map/Reduce into Impala
load_impala_tables = []
for event in __EVENTS:
    # Create the table if it doesn't exist
    create_op = PythonOperator(
        task_id='create_table_%s' % event,
        dag=clicklogs,
        python_callable=_create_tables,
        op_kwargs=dict(event=event))
    create_op.set_upstream(mr_cleaning_job)

    # Load the data into the impala table
    op = PythonOperator(
        task_id='load_impala_table_%s' % event,
        dag=clicklogs,
        python_callable=_load_impala_table,
        provide_context=True,
        op_kwargs=dict(output_path=options.output_path, event=event),
        retry_delay=timedelta(seconds=random.randrange(30,300,step=30)))
    op.set_upstream([create_op, mr_cleaning_job])
    load_impala_tables.append(op)


# Delete the staging files once the Map/Reduce job is done
delete_staging_files = PythonOperator(
    task_id='delete_staging_files',
    dag=clicklogs,
    python_callable=_delete_staging_files,
    provide_context=True,
    op_kwargs=dict(staging_path=options.staging_path))
delete_staging_files.set_upstream(mr_cleaning_job)

# Update the table build times
update_table_build_times = PythonOperator(
    task_id='update_table_build_times',
    dag=clicklogs,
    trigger_rule='all_done',
    provide_context=True,
    python_callable=_update_table_build_times)
update_table_build_times.set_upstream(load_impala_tables)


hdfs_maintenance = PythonOperator(
    task_id='hdfs_maintenance',
    dag=clicklogs,
    python_callable=_hdfs_maintenance,
    provide_context=True,
    op_kwargs=dict(days_ago=30))
hdfs_maintenance.set_upstream(load_impala_tables)


