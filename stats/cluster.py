''''
Utilities to deal with the cluster

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.ec2
import boto.emr
from boto.emr.bootstrap_action import BootstrapAction
from boto.emr.instance_group import InstanceGroup
import boto.emr.step
from boto.s3.connection import S3Connection
import datetime
import dateutil.parser
import json
import math
import numpy as np
import paramiko
import re
import socket
import time
import tempfile
import threading
import urllib2
import urlparse
import utils.monitor
import gzip

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("cluster_name", default="Neon Cluster",
       help="Name of any cluster that is created")
define("cluster_region", default='us-east-1',
       help='Amazon region where the cluster resides')
define("use_public_ip", default=0,
       help="If set, uses the public ip to talk to the cluster.")
define("ssh_key", default="s3://neon-keys/emr-runner.pem",
       help="ssh key used to execute jobs on the master node")
define("resource_manager_port", default=9026,
       help="Port to query the resource manager on")
define("history_server_port", default=19888,
       help='Port to query the history server on')
define("mapreduce_status_port", default=9046,
       help="Port to query the mapreduce status on")
define("s3_jar_bucket", default="neon-emr-packages",
       help='S3 bucket where jobs will be stored')

from utils import statemon
statemon.define("master_connection_error", int)
statemon.define("cluster_creation_error", int)

class ClusterException(Exception): pass
class ClusterInfoError(ClusterException): pass
class MasterMissingError(ClusterInfoError): pass
class ClusterConnectionError(ClusterException): pass
class ClusterCreationError(ClusterException): pass
class ExecutionError(ClusterException):pass
class MapReduceError(ExecutionError): pass

s3AddressRe = re.compile(r's3://([^/]+)/(\S+)')

def emr_iterator(conn, obj_type, cluster_id=None, **kwargs):
    '''Function that iterates through the responses to list_* functions
       including handing the paginatioin

    Inputs:
    conn - Connection object
    obj_type - String of the end of the list_<obj_type> function
               (e.g. 'clusters')
    cluster_id - The cluster id for any list function that's not list_clusters
    kwargs - Any specific keyworkd args for the list_* function

    Returns:
    The iterator through the objects
    '''
    obj_map = {
        'bootstrap_actions' : 'actions',
        'clusters' : 'clusters',
        'instance_groups': 'instancegroups',
        'instances' : 'instances',
        'steps': 'steps'}
    if not obj_type in obj_map:
        raise ValueError('Invalid object type: %s' % obj_type)

    list_func = getattr(conn, 'list_%s' % obj_type)
    if obj_type == 'clusters':
        get_page = lambda marker: list_func(marker=marker, **kwargs)
    else:
        get_page = lambda marker: list_func(cluster_id, marker=marker,
                                            **kwargs)

    cur_page = None
    while cur_page is None or 'marker' in cur_page.__dict__:
        if cur_page is None:
            # Get the first page of results
            cur_page = get_page(None)
        else:
            cur_page = get_page(cur_page.marker)

        for item in cur_page.__dict__[obj_map[obj_type]]:
            yield item

def EmrConnection(**kwargs):
    return boto.emr.connect_to_region(options.cluster_region, **kwargs)

def EC2Connection(**kwargs):
    return boto.ec2.connect_to_region(options.cluster_region, **kwargs)

class Cluster():
    # The possible instance and their multiplier of processing
    # power relative to the r3.xlarge. Bigger machines are
    # slightly better than an integer multiplier because of
    # network reduction.
    #
    # This table is (type, multiplier, on demand price)
    #instance_info = {
    #    'r3.2xlarge' : (2.1, 0.70),
    #    'r3.4xlarge' : (4.4, 1.40),
    #    'r3.8xlarge' : (9.6, 2.80),
    #    'hi1.4xlarge' : (2.5, 3.10),
    #    'm2.4xlarge' : (2.2, 0.98)
    #    }

    # We are basing it on a combination of memory and disk space
    instance_info = {
        'd2.2xlarge'  : (2.0, 1.38),
        'd2.4xlarge'  : (4.1, 2.76),
        'd2.8xlarge'  : (8.3, 5.52),
        'cc2.8xlarge' : (2.2, 2.0),
        'hi1.4xlarge' : (1.6, 3.1),
        'i2.4xlarge'  : (3.0, 3.41),
        'i2.8xlarge'  : (6.1, 6.82)
        }

    # Possible cluster roles
    ROLE_PRIMARY = 'primary'
    ROLE_BOOTING = 'booting'
    ROLE_TESTING = 'testing'
    
    '''Class representing the cluster'''
    def __init__(self, cluster_type, n_core_instances=None,
                 public_ip=None):
        '''
        cluster_type - Cluster type to connect to. Uses the cluster-type tag.
        n_core_instances - Number of r3.xlarge core instances. 
                           We will use the cheapest type of r3 instances 
                           that are equivalent to this number of r3.xlarge.
        public_ip - The public ip to assign to the cluster
        '''
        self.cluster_type = cluster_type
        self.public_ip = public_ip
        self.n_core_instances = n_core_instances
        self.cluster_id = None
        self.master_ip = None
        self.master_id = None

        self._lock = threading.RLock()

    def set_cluster_type(self, new_type):
        '''Sets the cluster type safely and reconnects as necessary.'''
        with self._lock:
            if self.cluster_type != new_type:
                self.cluster_id = None
                self.connect()

    def set_public_ip(self, new_ip):
        if new_ip is None or self.public_ip == new_ip:
            return

        if self.master_id is None:
            _log.error("Master id hasn't been set. Try connecting first")
            raise ValueError("No Master id set")
        
        with self._lock:
            _log.info('Grabbing elastic ip %s and assigning it to cluster %s' 
                      % (new_ip, self.cluster_id))
            conn = EC2Connection()

            elastic_addr = None
            for addr in conn.get_all_addresses():
                if addr.public_ip == new_ip:
                    elastic_addr = addr
                    break
            if elastic_addr is None:
                raise ClusterCreationError(
                    "Could not assign the elastic ip because the elastic"
                    " ip %s was not found" % new_ip)

            if elastic_addr.allocation_id is None:
                # It's a public ip
                success = conn.associate_address(instance_id=self.master_id,
                                                 public_ip=new_ip,
                                                 allow_reassociation=True)
            else:
                # It's a VPC ip
                success = conn.associate_address(
                    instance_id=self.master_id,
                    allocation_id=elastic_addr.allocation_id,
                    allow_reassociation=True)

            if not success:
                raise ClusterCreationError("Could not assign the elastic ip"
                                           "%s to instance %s" %
                                           (new_ip, self.master_id))
            self.public_ip = new_ip

    def connect(self):
        '''Connects to the cluster.

        If it's up, connect to it, otherwise create it
        '''
        with self._lock:
            if not self.is_alive():
                _log.warn("Could not find cluster %s. "
                          "Starting a new one instead"
                          % self.cluster_type)
                self._create()
            else:
                _log.info("Found cluster type %s with id %s" %
                          (self.cluster_type, self.cluster_id))
                self._set_requested_core_instances()

    def run_map_reduce_job(self, jar, main_class, input_path,
                           output_path, extra_ops, timeout=None, name='Raw Tracker Data Cleaning'):
        '''Runs a mapreduce job.

        Inputs:
        jar - Path to the jar to run. It should be a jar that packs in all the
              dependencies.
        main_class - Name of the main class in the jar of the job to run
        input_path - The input path of the data
        output_path - The output location for the data
        map_memory_mb - The memory needed for each map job. If the map is 
                        simple and doesn't have a lookup, this can be low,
                        which increases the number of maps each machine can
                        run.

        Returns:
        Returns once the job is done. If the job fails, an exception will be thrown.
        '''
        if timeout is not None:
            budget_time = datetime.datetime.now() + \
              datetime.timedelta(seconds=timeout)
        
        self.connect()
        stdout = self.send_job_to_cluster(jar, main_class, extra_ops,
                                          input_path, output_path)

        self.monitor_job_progress_hadoop(stdout, 
                                        budget_time,
                                        timeout,
                                        main_class,
                                        name='Raw Tracker Data Cleaning')

    def send_job_to_cluster(self, jar, main_class, extra_ops, input_path,
                            output_path):
        '''Sends a job to the cluster and returns a string of the stdout.

        Inputs:
        jar - Local path to the jar to run
        main_class - Main java class to execute
        extra_ops - Dictionary of extra parameters for the job
        input_path - Input path for the job to use. Probably hdfs or s3 path
        output_path - Output path for the job to use

        returns - String of the stdout for starting the job
        '''
        # First upload the jar to s3
        s3conn = S3Connection()
        bucket = s3conn.get_bucket(options.s3_jar_bucket)
        jar_key = bucket.new_key(os.path.basename(jar))
        _log.info('Uploading jar to s3://%s/%s' % (bucket.name, jar_key.name))
        jar_key.set_contents_from_filename(
            jar,
            headers={'Content-Type' : 'application/java-archive'},
            replace=True)

        _log.info('Wait for jar to be available in S3')
        found_key = bucket.get_key(jar_key.name)
        wait_count = 0
        while found_key is None or jar_key.md5 != found_key.etag.strip('"'):
            if wait_count > 120:
                _log.error('Timeout when waiting for the jar to show up in S3')
                raise IOError('Timeout when uploading jar to s3://%s/%s' %
                              (bucket.name, jar_key.name))
            time.sleep(5.0)
            found_key = bucket.get_key(jar_key.name)
            wait_count += 1

        # Now send the job to emr
        _log.info('Sending job to EMR')
        emrconn = EmrConnection()
        step_args = ' '.join(('-D %s=%s' % x 
                              for x in extra_ops.iteritems()))
        step_args = step_args.split()
        step_args.append(input_path)
        step_args.append(output_path)
        step = boto.emr.step.JarStep(main_class,
                                     's3://%s/%s' % (bucket.name,
                                                     jar_key.name),
                                     main_class,
                                     'CONTINUE',
                                     step_args)
        res = emrconn.add_jobflow_steps(self.cluster_id, [step])
        step_id = res.stepids[0].value

        self.monitor_job_progress_emr(step_id);

        # Get the stdout from the job being loaded up
        return self.get_emr_logfile(ssh_conn, step_id, 'stdout')

    def get_emr_logfile(self, ssh_conn, step_id, logtype='stdout', retry=True):
        '''Grabs the logfile from the master and returns it as a string'''
        ssh_conn = ClusterSSHConnection(self)
        try:
            log_fp = ssh_conn.open_remote_file(
                '/mnt/var/log/hadoop/steps/%s/%s' % (step_id, logtype),
                'r')
        except IOError:
            # The log might have been g-zipped so try getting it that way
            log_fp = gzip.GzipFile(
                '',
                'rb',
                fileobj=ssh_conn.open_remote_file(
                    '/mnt/var/log/hadoop/steps/%s/%s.gz' % (step_id, logtype),
                    'rb'))
        retval = ''.join(log_fp.readlines())
        if retval == '' and retry:
            # The data might not be in the file yet, wait a few
            # seconds and try again
            time.sleep(10.0)
            return self.get_emr_logfile(ssh_conn, step_id, logtype, False)
        return retval

    def is_alive(self):
        '''Returns true if the cluster is up and running.'''
        try:
            return self._check_cluster_state() in ['WAITING', 'RUNNING',
                                                   'STARTING',
                                                   'BOOTSTRAPPING']
        except ClusterInfoError:
            # If we couldn't get info about the cluster it doesn't exist
            # and so it is not alive
            return False

    def increment_core_size(self, amount=1):
        '''Increments the size of the core instance group.'''
        group = self.change_instance_group_size('CORE', incr_amount=amount)
        try:
            instance_count = group.requestedinstancecount
        except AttributeError:
            instance_count = group.num_instances
            
        self.n_core_instances = instance_count * \
          Cluster.instance_info[group.instancetype][0]

    def change_instance_group_size(self, group_type, incr_amount=None,
                                   new_size=None):
        '''Change a instance group size.

        Only one of incr_amount or new_size can be set

        Inputs:
        group_type - Type of group: 'MASTER', 'CORE' or 'TASK'
        incr_amount - Size to increate the group by. 
                      Can be negative for TASK group
        new_size - New size of the group.

        Outputs:
        InstanceGroup - Instance group status
        '''
        if group_type != 'TASK' and incr_amount < 1:
            raise ValueError('Cannot shrink an instance group of type %s' %
                             group_type)

        if ((incr_amount is not None and new_size is not None) or 
            (new_size is None and incr_amount is None)):
            raise ValueError('Exactly one of incr_amount or new_size must'
                             ' be set')

        with self._lock:
            self.connect()
            
            # First find the instance group
            conn = EmrConnection()
            found_group = None
            for group in emr_iterator(conn,'instance_groups',self.cluster_id):
                if group.instancegrouptype == group_type:
                    found_group = group
                    break

            if found_group is None:
                if group_type == 'TASK' and new_size is not None:
                    _log.info('Could not find the instance group to change, '
                              'creating it')
                    group = InstanceGroup(new_size, 'TASK', 'r3.2xlarge', 
                                          'SPOT', 'Task Instance Group', 0.77)
                    conn.add_instance_groups(self.cluster_id, [group])
                    return group
                        
                else:
                    raise ClusterInfoError('Could not find the %s instance '
                                           'group for cluster %s' 
                                           % (group_type, self.cluster_id))

            # Now increment the number of machines
            new_count = new_size
            if new_count is None:
                new_count = int(found_group.requestedinstancecount) + \
                  incr_amount
            if new_count != int(found_group.requestedinstancecount):
                _log.info('Changing the %s instance group size to %i' %
                          (group_type, new_count))
                conn.modify_instance_groups([found_group.id],
                                            [new_count])

        found_group.requestedinstancecount = new_count
        return found_group

    def query_resource_manager(self, query, tries=5):
        '''Query the resource manager for information from Hadoop.

        Inputs:
        query - The query to send. This will be a relative REST API endpoint

        Returns:
        A dictionary of the parsed json response
        '''
        query_url = 'http://{ip}:{port}{query}'.format(
            ip = self.master_ip,
            port = options.resource_manager_port,
            query = query)

        return self._query_hadoop_rest(query_url, tries)

    def query_history_manager(self, query, tries=5):
        '''Query the history manager for information from Hadoop.

        Inputs:
        query - The query to send. This will be a relative REST API endpoint

        Returns:
        A dictionary of the parsed json response
        '''
        query_url = 'http://{ip}:{port}{query}'.format(
            ip = self.master_ip,
            port = options.history_server_port,
            query = query)

        return self._query_hadoop_rest(query_url, tries)

    def _query_hadoop_rest(self, query_url, tries=5):
        cur_try = 0
        while cur_try < tries:
            cur_try += 1

            try:
                response = urllib2.urlopen(query_url)
                return json.load(response)
            except Exception as e:
                _log.error('Error querying %s manager (attempt %i): %s'
                           % (query_url, cur_try, e))
                if cur_try == tries:
                    raise
            time.sleep(30)

    def _check_cluster_state(self):
        '''Returns the state of the cluster.

        The state could be strings of 
        STARTING | BOOTSTRAPPING | RUNNING | WAITING | 
        TERMINATING | TERMINATED | TERMINATED_WITH_ERRORS
        '''
        if self.cluster_id is None:
            cluster_info = self.find_cluster()
            return cluster_info.status.state

        conn = EmrConnection()
        return conn.describe_cluster(self.cluster_id).status.state

    def find_cluster(self):
        '''Finds the cluster if it exists and fills the
        self.cluster_id, self.master_id and self.master_ip

        Returns the ClusterInfo object if the cluster was found.
        '''
        conn = EmrConnection()
        most_recent = None
        cluster_found = None
        for cluster in emr_iterator(conn, 'clusters',
                                    cluster_states=['STARTING',
                                                    'BOOTSTRAPPING',
                                                    'RUNNING',
                                                    'WAITING']):
            if cluster.name != options.cluster_name:
                # The cluster has to have the right name to be a possible match
                continue
            cluster_info = conn.describe_cluster(cluster.id)
            create_time = dateutil.parser.parse(
                cluster_info.status.timeline.creationdatetime)
            if (self._get_cluster_tag(cluster_info, 'cluster-type', '') == 
                self.cluster_type and
                self._get_cluster_tag(cluster_info, 'cluster-role', '') ==
                Cluster.ROLE_PRIMARY and (
                    most_recent is None or create_time > most_recent)):
                self.cluster_id = cluster.id
                most_recent = create_time
                cluster_found = cluster_info
            time.sleep(1) # Avoid AWS throttling
        
        if cluster_found is None:
            raise ClusterInfoError('Could not find a cluster of type %s'
                                   ' with name %s'
                                   % (self.cluster_type,
                                      options.cluster_name))
        self._find_master_info()
        
        return cluster_found

    def _get_cluster_tag(self, cluster_info, tag_name, default=None):
        '''Returns the cluster type from a Cluster response object.'''
        if cluster_info is None:
            _log.error('Cluster info is None')
            return ValueError('Cluster Info is None')

        if cluster_info.tags is not None:
            for tag in cluster_info.tags:
                if tag.key == tag_name:
                    return tag.value

        _log.warn('Could not determine tag %s for cluster named %s' %
                  (tag_name, cluster_info.name))
        if default is not None:
            return default
        raise KeyError('No tag %s' % tag_name)

    def _find_master_info(self):
        '''Find the ip address and id of the master node.'''
        conn = EmrConnection()
        ec2conn = EC2Connection()
        
        self.master_ip = None

        # Get the master instance group
        master_group = None
        for igroup in emr_iterator(conn, 'instance_groups', self.cluster_id):
            if igroup.instancegrouptype == 'MASTER':
                master_group = igroup
                break
        if master_group is None:
            raise MasterMissingError("Could not find master instance group")
                                     
        for instance in emr_iterator(conn, 'instances', self.cluster_id,
                                     instance_group_id=master_group.id):
            instance_info = ec2conn.get_only_instances([instance.ec2instanceid])
            if (instance_info and instance_info[0].state == 'running'):
                self.master_id = instance.ec2instanceid
                if options.use_public_ip and instance_info[0].ip_address:
                    self.master_ip = instance_info[0].ip_address
                else:
                    self.master_ip = instance_info[0].private_ip_address
                break

        if self.master_ip is None:
            raise MasterMissingError("Could not find the master ip")
        _log.info("Found master ip address %s" % self.master_ip)

    def _set_requested_core_instances(self):
        '''Sets self.n_core_instances to what is currently requested.'''
        found_group = self._get_instance_group_info('CORE')

        if found_group is not None:
            self.n_core_instances = int(found_group.requestedinstancecount) * \
              Cluster.instance_info[found_group.instancetype][0]

    def _get_instance_group_info(self, group_type):
        conn = EmrConnection()
        found_group = None
        for group in emr_iterator(conn, 'instance_groups', self.cluster_id):
            if group.instancegrouptype == group_type:
                found_group = group
                break

        return found_group

    def _create(self):
        '''Creates a new cluster.

        Blocks until the cluster is ready.
        '''
        #TODO(mdesnoyer): Parameterize this. For now, we just put the
        #settings here.

        bootstrap_actions = [
            BootstrapAction(
                'Fix Ride the Rocket',
                's3://support.elasticmapreduce/bootstrap-actions/ami/3.1.0/tcpPacketLoss.sh',
                []),
            BootstrapAction(
                'Install Ganglia',
                's3://elasticmapreduce/bootstrap-actions/install-ganglia',
                []),
            BootstrapAction(
                'Install Impala',
                's3://elasticmapreduce/libs/impala/setup-impala',
                ['--base-path', 's3://elasticmapreduce',
                 '--impala-version', '1.2.4']),
            BootstrapAction(
                'Configure Daemons',
                's3://elasticmapreduce/bootstrap-actions/configure-daemons',
                ['--client-opts=-Xmx14000m']),
            BootstrapAction(
                'Configure Hadoop',
                's3://elasticmapreduce/bootstrap-actions/configure-hadoop',
                ['--hdfs-key-value', 'io.file.buffer.size=65536',
                 '--mapred-key-value',
                 'mapreduce.job.user.classpath.first=true',
                 '--mapred-key-value', 'mapreduce.map.output.compress=true',
                 '--mapred-key-value',
                 'mapreduce.reduce.merge.inmem.threshold=10000',
                 '--mapred-key-value',
                 'mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec',
                 '--mapred-key-value',
                 'mapreduce.reduce.shuffle.parallelcopies=50',
                 '--mapred-key-value',
                 'mapreduce.task.io.sort.mb=512',
                 '--mapred-key-value',
                 'mapreduce.task.io.sort.factor=100',
                 '--yarn-key-value',
                 'yarn.nm.liveness-monitor.expiry-interval-ms=120000',
                 '--yarn-key-value',
                 'yarn.am.liveness-monitor.expiry-interval-ms=120000',
                 '--yarn-key-value',
                 'yarn.resourcemanager.container.liveness-monitor.interval-ms=120000',
                 '--yarn-key-value',
                 'yarn.log-aggregation-enable=true',
                 '--yarn-key-value',
                 'yarn.scheduler.maximum-allocation-mb=16000'])]
            
        steps = [
            boto.emr.step.InstallHiveStep('0.11.0.2')]

            
        subnet_id, instance_group = self._get_subnet_id_and_core_instance_group() 
        instance_groups = [
            InstanceGroup(1, 'MASTER', 'r3.xlarge', 'ON_DEMAND',
                          'Master Instance Group'),
            instance_group
            ]
        
        conn = EmrConnection()
        _log.info('Creating cluster %s' % options.cluster_name)
        try:
            self.cluster_id = conn.run_jobflow(
                options.cluster_name,
                log_uri='s3://neon-cluster-logs/',
                ec2_keyname=os.path.basename(options.ssh_key).split('.')[0],
                ami_version='3.1.4',
                job_flow_role='EMR_EC2_DefaultRole',
                service_role='EMR_DefaultRole',
                keep_alive=True,
                enable_debugging=True,
                steps=steps,
                bootstrap_actions=bootstrap_actions,
                instance_groups=instance_groups,
                visible_to_all_users=True,
                api_params = {'Instances.Ec2SubnetId' : 
                              subnet_id})
        except boto.exception.EmrResponseError as e:
            _log.error('Error creating the cluster: %s' % e)
            statemon.state.increment('cluster_creation_error')
            # We sleep because this most likely is caused by a bug in
            # the config and we don't want to piss AWS off by trying
            # over and over and over and over again
            time.sleep(30.0)
            raise

        conn.add_tags(self.cluster_id,
                      {'cluster-type' : self.cluster_type,
                       'cluster-role' : Cluster.ROLE_BOOTING})

        _log.info('Waiting until cluster %s is ready' % self.cluster_id)
        cur_cluster = conn.describe_cluster(self.cluster_id)
        while cur_cluster.status.state != 'WAITING':
            if cur_cluster.status.state in ['TERMINATING', 'TERMINATED',
                                            'TERMINATED_WITH_ERRORS',
                                            'FAILED']:
                msg = ('Cluster could not start because: %s',
                        cur_cluster.status.laststatechangereason)
                _log.error(msg)
                raise ClusterCreationError(msg)

            _log.info('Cluster is booting. State: %s' % 
                      cur_cluster.status.state)
            time.sleep(30.0)
            cur_cluster = conn.describe_cluster(self.cluster_id)

        _log.info('Making the new cluster primary')
        for cluster in emr_iterator(conn, 'clusters'):
            try:
                self._get_cluster_tag(cluster, 'cluster-role')
                conn.remove_tags(cluster.id, ['cluster-role'])
            except KeyError:
                pass
        conn.add_tags(self.cluster_id, {
            'cluster-role' : Cluster.ROLE_PRIMARY})
        self._find_master_info()
        cluster_ip = self.public_ip
        self.public_ip = None
        self.set_public_ip(cluster_ip)

    def _get_subnet_id_and_core_instance_group(self):   
        # Calculate the expected costs for each of the instance type options
        #avail_zone_to_subnet_id = { 'us-east-1c' : 'subnet-d3be7fa4',  
        #    'us-east-1d' : 'subnet-53fa1901' 
        #}
        avail_zone_to_subnet_id = { 'us-east-1c' : 'subnet-b0d884c7'}
 
        data = [(itype, math.ceil(self.n_core_instances / x[0]), 
                 x[0] * math.ceil(self.n_core_instances / x[0]), 
                 x[1], cur_price, avg_price, availability_zone)
                 for availability_zone in avail_zone_to_subnet_id.keys()
                 for itype, x in Cluster.instance_info.items()
                 for cur_price, avg_price in [self._get_spot_prices(itype, 
                   availability_zone)]]
        data = sorted(data, key=lambda x: (-x[2] / (np.mean(x[4:6]) * x[1]),
                                           -x[1]))
        chosen_type, count, cpu_units, on_demand_price, cur_spot_price, \
          avg_spot_price, availability_zone = data[0]

        _log.info('Choosing core instance type %s because its avg price was %f'
                  % (chosen_type, avg_spot_price))

        # If the best price is more than the on demand cost, just use on demand
        if (avg_spot_price > 0.80 * on_demand_price or 
            cur_spot_price > on_demand_price):
            _log.info('Spot pricing is too high, chosing on demand instance')
            market_type = 'ON_DEMAND'
        else:
            market_type = 'SPOT'

        subnet_id = avail_zone_to_subnet_id[availability_zone] 

        return subnet_id, InstanceGroup(int(count),
                             'CORE',
                             chosen_type,
                             market_type,
                             'Core instance group',
                             '%.3f' % (1.03 * on_demand_price))

    def _get_spot_prices(self, 
                         instance_type, 
                         availability_zone,
                         tdiff=datetime.timedelta(days=1)):
        '''Returns the (current, avg for the tdiff) for a given
        instance type.'''
        conn = EC2Connection()
        data = [(dateutil.parser.parse(x.timestamp), x.price) for x in 
                conn.get_spot_price_history(
                    start_time=(datetime.datetime.utcnow()-tdiff).isoformat(),
                    end_time=datetime.datetime.utcnow().isoformat(),
                    instance_type=instance_type,
                    product_description='Linux/UNIX (Amazon VPC)',
                    availability_zone=availability_zone)]
        timestamps, prices = zip(*(data[::-1]))

        timestamps = np.array(
            [(x-timestamps[0]).total_seconds() for x in timestamps])
        prices = np.array(prices)

        total_cost = np.dot((timestamps[1:] - timestamps[0:-1]) / 3600.,
                            prices[0:-1])
        avg_price = total_cost / (timestamps[-1] - timestamps[0]) * 3600.0
        cur_price = prices[-1]
        return cur_price, avg_price

    def checkpoint_hdfs_to_s3(self, jar_path, hdfs_path_to_copy, s3_path, timeout=None):
        #Does checkpoint of hdfs data from mapreduce output to S3.


        if timeout is not None:
            budget_time = datetime.datetime.now() + \
              datetime.timedelta(seconds=timeout)

        emrconn = boto.emr.EmrConnection()
        
        name_step = 'S3DistCp'

        step_arg = []
        step_arg.append('--src')
        step_arg.append(hdfs_path_to_copy)
        step_arg.append('--dest')
        step_arg.append(s3_path)

        _log.info("Copying data from %s to %s in cluster %s" % (hdfs_path_to_copy,s3_path,self.cluster_id))

        step = boto.emr.step.JarStep(name=name_step,
                                     jar=jar_path,
                                     step_args=step_arg,
                                     action_on_failure='CONTINUE')

        jobid = emrconn.add_jobflow_steps(self.cluster_id, [step])
        step_id = jobid.stepids[0].value

        self.monitor_job_progress_emr(step_id)
        
        # The tracking URL for S3DistCp is going to syslog, so grab it from there
        syslog = self.get_emr_logfile(ssh_conn, step_id, 'syslog')

        self.monitor_job_progress_hadoop(syslog, 
                                         budget_time,
                                         timeout,
                                         main_class='S3DistCp',
                                         name='S3DistCp')
    
    def monitor_job_progress_hadoop(self, stdout, budget_time, timeout, main_class, name):

        trackURLRe = re.compile(
            r"Tracking URL: https?://(\S+):[0-9]*/proxy/(\S+)/")
        jobidRe = re.compile(r"Job ID: (\S+)")
        url_parse = trackURLRe.search(stdout)
        if not url_parse:
            raise MapReduceError(
                "Could not find the tracking url. Stdout was: \n%s" % stdout)
        application_id = url_parse.group(2)
        host = url_parse.group(1)

        job_id_parse = jobidRe.search(stdout)
        if not job_id_parse:
            raise MapReduceError(
                "Could not find the job id. Stdout was: \n%s" % stdout)
        job_id = job_id_parse.group(1)

        _log.info('Running map reduce job %s. Tracking URL is %s' %
                  (job_id, url_parse.group(0)))

        # Sleep so that the job tracker has time to come up
        time.sleep(60)

        # Now poll the job status until it is done
        error_count = 0
        job_status = None
        while True:
            if timeout is not None and budget_time < datetime.datetime.now():
                raise MapReduceError("Map Reduce Job timed out.")
                
            try:
                if job_status != 'RUNNING' and job_status != 'FINISHED':
                    response = self.query_resource_manager(
                        '/ws/v1/cluster/apps?stats=RUNNING,ACCEPTED')
                    latest_app_time = None
                    for app in response['apps']['app']:
                        if (name in app['name'] and (
                            latest_app_time is None or 
                            latest_app_time < app['startedTime'])):
                            latest_app_time = app['startedTime']
                            job_status = app['state']
                    if job_status != 'RUNNING' and job_status != 'FINISHED':
                        time.sleep(60)
                        continue

                url = ("http://{host}:{port}/proxy/{app_id}/ws/v1/mapreduce/"
                       "jobs/{job_id}").format(
                           host=host, 
                           port=options.mapreduce_status_port, 
                           app_id=application_id, 
                           job_id=job_id)
                response = urllib2.urlopen(url)

                if url != response.geturl():
                    # The job is probably done, so we need to look at the
                    # job history server
                    data = self.query_history_manager(
                        '/ws/v1/history/mapreduce/jobs/%s' %
                        job_id)
                else:
                    data = json.load(response)

                data = data['job']

                # Send monitoring data
                for key, value in data.iteritems():
                    utils.monitor.send_data('batch_processor.%s' % key, value)

                if data['state'] == 'SUCCEEDED':
                    _log.info('Map reduce job %s complete. Results: %s' % 
                              (main_class, 
                               json.dumps(data, indent=4, sort_keys=True)))
                    return
                elif data['state'] in ['FAILED', 'KILLED', 'ERROR', 'KILL_WAIT']:
                    msg = ('Map reduce job %s failed: %s' %
                               (main_class,
                                json.dumps(data, indent=4, sort_keys=True)))
                    _log.error(msg)
                    raise MapReduceError(msg)

                error_count = 0

                time.sleep(60)
            except urllib2.URLError as e:
                _log.error("Error getting job information: %s" % e)
                statemon.state.increment('master_connection_error')
                error_count = error_count + 1
                if error_count > 5:
                    _log.error("Tried 5 times and couldn't get there so stop")
                    raise
                time.sleep(30)
            except socket.error as e:
                _log.error("Error getting job information: %s" % e)
                statemon.state.increment('master_connection_error')
                error_count = error_count + 1
                if error_count > 5:
                    _log.error("Tried 5 times and couldn't get there so stop")
                    raise
                time.sleep(30)

    def monitor_job_progress_emr(self, step_id):

        _log.info('EMR Job id is %s. Waiting for it to be sent to Hadoop' %
                  step_id)

        ssh_conn = ClusterSSHConnection(self)

        # Wait until it is "done". When it is "done" it has actually
        # only sucessfully loaded the job into the resource manager
        wait_count = 0
        while (emrconn.describe_step(self.cluster_id, step_id).status.state in
               ['PENDING', 'RUNNING']):
            if wait_count > 80:
                _log.error('Timeout when waiting for EMR to send the job %s '
                           'to Haddop' % step_id)
                _log.error('stderr was:\n %s' %
                       self.get_emr_logfile(ssh_conn, step_id, 'stderr'))
                _log.error('stdout was:\n %s' %
                       self.get_emr_logfile(ssh_conn, step_id, 'stdout'))
                _log.error('syslog was:\n %s' %
                       self.get_emr_logfile(ssh_conn, step_id, 'syslog'))
                raise MapReduceError('Timeout when waiting for EMR to send '
                                     'job %s to Hadoop' % step_id)
            time.sleep(15.0)
            wait_count += 1

        job_state = emrconn.describe_step(self.cluster_id,
                                          step_id).status.state
        if (job_state != 'COMPLETED'):
            _log.error('EMR job could not be added to Hadoop. It is state %s'
                       % job_state)

            # Get the logs from the cluster
            _log.error('stderr was:\n %s' %
                       self.get_emr_logfile(ssh_conn, step_id, 'stderr'))
            _log.error('stdout was:\n %s' %
                       self.get_emr_logfile(ssh_conn, step_id, 'stdout'))
            _log.error('syslog was:\n %s' %
                       self.get_emr_logfile(ssh_conn, step_id, 'syslog'))
            raise MapReduceError('Error loading job into Hadoop. '
                                 'See earlier logs for job logs')


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
        self.key_file.flush()

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.load_system_host_keys()

    def _connect(self):
        try:
            self.client.connect(self.cluster_info.master_ip,
                                username="hadoop",
                                key_filename = self.key_file.name)
        except socket.error as e:
            raise ClusterConnectionError("Error connecting to %s: %s" %
                                         (self.cluster_info.master_ip, e))
        except paramiko.SSHException as e:
            raise ClusterConnectionError("Error connecting to %s: %s" %
                                         (self.cluster_info.master_ip, e))

    def put_file(self, local_path, remote_path):
        '''Copies a file from the local path to the cluster.

        '''
        
        _log.info("Copying %s to %s" % (local_path,
                                        self.cluster_info.master_ip))
        self._connect()
        
        try:
            ftp_client = self.client.open_sftp()
            ftp_client.put(local_path, remote_path)

        finally:
            self.client.close()

    def open_remote_file(self, remote_path, mode='r', bufsize=-1):
        '''Returns a file like object for a remote file'''
        _log.info('Opening %s on %s' %
                  (remote_path, self.cluster_info.master_ip))
        self._connect()
        ftp_client = self.client.open_sftp()
        return ftp_client.file(remote_path, mode, bufsize)

    def execute_remote_command(self, cmd):
        '''Executes a command on the master node.

        Returns stdout of the process, or raises an Exception if the
        process failed.
        '''

        _log.info("Executing %s on cluster master at %s" %
                  (cmd, self.cluster_info.master_ip))
        self._connect()
            
        stdout_msg = []
        stderr_msg = []
        retcode = None
        try:
            stdin, stdout, stderr = self.client.exec_command(cmd, timeout=60.0)

            # Get the stdout and stderr data. Need to do this the long
            # way because if either is long, an ssh buffer fills up
            # and hangs forever waiting to be emptied.
            got_stdout = False
            got_stderr = False
            while not got_stderr or not got_stdout:
                stdout_line = stdout.readline()
                if stdout_line == '':
                    got_stdout = True
                else:
                    stdout_msg.append(stdout_line)

                stderr_line = stderr.readline()
                if stderr_line == '':
                    got_stderr = True
                else:
                    stderr_msg.append(stderr_line)
            retcode = stdout.channel.recv_exit_status()

        except socket.timeout:
            # TODO(mdesnoyer): Right now, running a yarn job will
            # hang, so a timeout occurs. However, the job actually
            # runs. Figure out why it hangs.
            _log.warn('Socket timeout when running command. '
                      'Assuming that the process succeeds for now: %s' % cmd)
            return ''.join(stdout_msg)
            
        finally:
            self.client.close()

        if retcode != 0:
            raise ExecutionError(
                "Error running command on the cluster: %s. Stderr was: \n%s" % 
                (cmd, ''.join(stderr_msg)))

        return ''.join(stdout_msg)