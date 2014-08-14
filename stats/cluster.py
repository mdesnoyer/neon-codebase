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

from boto.ec2.connection import EC2Connection
from boto.emr.connection import EmrConnection
from boto.emr.bootstrap_action import BootstrapAction
from boto.emr.instance_group import InstanceGroup
import boto.emr.step
from boto.s3.connection import S3Connection
import datetime
import math
import numpy as np
import paramiko
import threading

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("ssh_key", default="s3://neon-keys/emr-runner.pem",
       help="ssh key used to execute jobs on the master node")
define("resource_manager_port", default=9022,
       help="Port to query the resource manager on")

from utils import statemon
statemon.define("master_connection_error", int)

class ClusterException(Exception): pass
class ClusterInfoError(ClusterException): pass
class ClusterConnectionError(ClusterException): pass
class ClusterCreationError(ClusterException): pass

class Cluster():
    '''Class representing the cluster'''
    def __init__(self, cluster_name, n_core_instances):
        '''
        cluster_name - Name of the cluster.
        n_core_instances - Number of r3.xlarge core instances. 
                           We will use the cheapest type of r3 instances 
                           that are equivalent to this number of r3.xlarge.
        '''
        self.cluster_name = cluster_name
        self.n_core_instances = n_core_instances
        self.cluster_id = None
        self.master_ip = None
        self.master_id = None

        self._lock = threading.RLock()

    def connect(self):
        '''Connects to the cluster.

        If it's up, connect to it, otherwise create it
        '''
        with self._lock():
            if self.is_alive():
                _log.warn("Could not find cluster %s. "
                          "Starting a new one instead"
                          % self.cluster_name)
                self._create()
            else:
                _log.info("Found cluster name %s with id %s" %
                          (self.cluster_name, self.cluster_id))
                self._find_master_info()

    def run_map_reduce_job(self, jar, main_class, input_path,
                           output_path):
        '''Runs a mapreduce job.

        Inputs:
        jar - Path to the jar to run. It should be a jar that packs in all the
              dependencies.
        main_class - Name of the main class in the jar of the job to run
        input_path - The input path of the data
        output_path - The output location for the data

        Returns:
        Returns once the job is done. If the job fails, an exception will be thrown.
        '''
        self.connect()
        ssh_conn = ClusterSSHConnection(self)
        ssh_conn.copy_file(jar, '/home/hadoop/%s' % os.path.basename(jar))

        trackURLRe = re.compile(
            r"Tracking URL: https?://(\S+)/proxy/(\S+)/")
        jobidRe = re.compile(r"Job ID: (\S+)")
        stdout = ssh_conn.execute_remote_command(
            ('hadoop jar /home/hadoop/%s %s '
             '-D mapreduce.output.fileoutputformat.compress=true '
             '-D avro.output.codec=snappy %s %s') % 
             (os.path.basename(jar), main_class, input_path, output_path))
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
        time.sleep(30)

        # Now poll the job status until it is done
        error_count = 0
        while True:
            try:
                url = ("http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s" % 
                       (host, application_id, job_id))
                response = urllib2.urlopen(url)

                if url != response.geturl():
                    # The job is probably done, so we need to look at the
                    # job history server
                    history_url = ("http://%s/ws/v1/history/mapreduce/jobs/%s" %
                                   (urlparse.urlparse(response.geturl()).netloc,
                                    job_id))
                    response = urllib2.urlopen(history_url)

                data = json.load(response)['job']

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
                _log.exception("Error getting job information: %s" % e)
                statemon.state.increment('master_connection_error')
                error_count = error_count + 1
                if error_count > 5:
                    _log.error("Tried 5 times and couldn't get there so stop")
                    raise
                time.sleep(5)

    def is_alive(self):
        '''Returns true if the cluster is up and running.'''
        with self._lock():
            return self._check_cluster_state() in ['WAITING', 'RUNNING',
                                                   'STARTING',
                                                   'BOOTSTRAPPING']
    

    def _check_cluster_state(self):
        '''Returns the state of the cluster.

        The state could be strings of 
        STARTING | BOOTSTRAPPING | RUNNING | WAITING | 
        TERMINATING | TERMINATED | TERMINATED_WITH_ERRORS
        '''
        conn = EmrConnection()
        if self.cluster_id is None:
            most_recent = datetime.datetime.fromtimestamp(0)
            state = None
            for cluster in conn.list_clusters().clusters:
                create_time = datetime.datetime.strptime(
                    cluster.creationdatetime,
                    '%Y-%m-%dT%H:%M:%S.%fZ')
                if (cluster.name == self.cluster_name and
                    create_time > most_recent):
                    self.cluster_id = cluster.id
                    most_recent = create_time
                    state = cluster.status.state
            if self.cluster_id is None:
                raise ClusterInfoError(
                    "Could not get information about cluster %s " % 
                    self.cluster_name)
            return state

        return conn.describe_cluster(self.cluster_id).status.state

    def _find_master_info(self)
        '''Find the ip address and id of the master node.'''
        conn = EmrConnection()
        
        self.master_ip = None
        self.master_id = \
          conn.describe_jobflow(self.cluster_id).masterinstanceid
        for instance in conn.list_instances(self.cluster_id).instances:
            if instance.ec2instanceid == master_id:
                self.master_ip = instance.privateipaddress

        if self.master_ip is None:
            raise ClusterInfoError("Could not find the master ip")
        _log.info("Found master ip address %s" % self.master_ip)

    def _create(self):
        '''Creates a new cluster.

        Blocks until the cluster is ready.
        '''
        #TODO(mdesnoyer): Parameterize this. For now, we just put the
        #settings here.

        bootstrap_actions = [
            BootstrapAction(
                'Install HBase',
                's3://elasticmapreduce/bootstrap-actions/setup-hbase',
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
                'Configure Hadoop',
                's3://elasticmapreduce/bootstrap-actions/configure-hadoop',
                ['--site-key-value', 'io.file.buffer.size=65536',
                 '--mapred-key-value',
                 'mapreduce.job.user.classpath.first=true',
                 '--mapred-key-value', 'mapreduce.map.output.compress=true',
                 '--mapred-key-value',
                 'mapreduce.reduce.merge.inmem.threshold=10000',
                 '--mapred-key-value',
                 'mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec'])]
            
        steps = [
            boto.emr.step.InstallHiveStep('0.11.0.2'),
            boto.emr.step.JarStep(
                'Start HBase',
                '/home/hadoop/lib/hbase.jar',
                None,
                ['emr.hbase.backup.Main', '--start-master'])]

            
        instance_groups = [
            InstanceGroup(1, 'MASTER', 'm1.large', 'SPOT',
                          'Master Instance Group', 0.50),
            self._get_core_instance_group()
            ]
        
        conn = EmrConnection()
        _log.info('Creating cluster %s' % self.cluster_name)
        self.cluster_id = conn.run_jobflow(
            self.cluster_name,
            log_uri='s3n://neon-cluster-logs/',
            ec2_keyname='emr-runner',
            ami_version='3.1.0',
            jobflow_role='EMR_EC2_DefaultRole',
            service_role='EMR_DefaultRole',
            keep_alive=True,
            enable_debugging=True,
            steps=steps,
            bootstrap_actions=bootstrap_actions,
            instance_groups=instance_groups,
            visible_to_all_users=True,
            api_params = {'Instances.Ec2SubnetId' : 
                          'subnet-74c10003'})


        _log.info('Waiting until cluster %s is ready')
        cur_state = conn.describe_jobflow(self.cluster_id)
        while cur_state.state != 'WAITING':
            if cur_state.state in ['TERMINATING', 'TERMINATED',
                             'TERMINATED_WITH_ERRORS']:
                msg = ('Cluster could not start because: %s',
                           cur_state.laststatechangereason)
                _log.error(msg)
                raise ClusterCreationError(msg)

            _log.info('Cluster is booting. State: %s' % cur_state.state)
            time.sleep(30.0)
            cur_state = conn.describe_jobflow(self.cluster_id)
        self._find_master_info()                   

    def _get_core_instance_group(self):
        # The possible instance and their multiplier of processing
        # power relative to the r3.xlarge. Bigger machines are
        # slightly better than an integer multiplier because of
        # network reduction.
        instance_types = [
            ('r3.xlarge', 1., 0.35),
            ('r3.2xlarge', 2.1, 0.70),
            ('r3.4xlarge', 4.4, 1.40),
            ('r3.8xlarge', 9.6, 2.80),
            ('hi1.4xlarge', 2.5, 3.10),
            ('m2.4xlarge', 2.3, 0.98)
            ]

        # Calculate the expect costs for each of the instance type options
        data = [(itype, math.ceil(self.n_core_instances / mul), price,
                 self.get_avg_spot_price(itype))
                 for itype, mul, price in instance_types]
        data = sorted(data, key=lambda x: (x[3] / x[1], -x[1]))
        chosen_type, count, on_demand_price, avg_spot_price = data[0]

        _log.info('Choosing core instance type %s because its avg price was %f'
                  % (chosen_type, avg_spot_price))

        # If the best price is more than the on demand cost, just use on demand
        if avg_spot_price > 0.80 * on_demand_price:
            _log.info('Spot pricing is too high, chosing on demand instance')
            market_type = 'ON_DEMAND'
        else:
            market_type = 'SPOT'

        return InstanceGroup(count,
                             'CORE',
                             chosen_type,
                             market_type,
                             'Core instance group',
                             1.03 * on_demand_price)
                             

    def _get_avg_spot_price(self, instance_type, 
                            tdiff=datetime.timedelta(days=7)):
        '''Get the avg spot price for an instance over the last week.'''
        conn = EC2Connection()
        data = [(datetime.datetime.strptime(x.timestamp,
                                            '%Y-%m-%dT%H:%M:%S.%fZ'),
                x.price) for x in 
                conn.get_spot_price_history(
                    start_time=(datetime.utcnow() - tdiff).isoformat(),
                    end_time=datetime.utcnow().isoformat(),
                    instance_type=instance_type,
                    product_description='Linux/UNIX (Amazon VPC)',
                    availability_zone='us-east-1c')]
        timestamps, prices = zip(*(data[::-1]))

        timestamps = np.array(
            [(x-timestamps[0]).total_seconds() for x in timestamps])
        prices = np.array(prices)

        total_cost = np.multiply((timestamps[1:] - timestamps[0:-1]),
                                 prices[0:-1])
        return total_cost / (timestamps[-1] - timestamps[0])
            

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
        self.client.load_system_host_keys(options.master_host_key_file)

    def _connect(self):
        try:
            self.client.connect(self.cluster_info.master_ip,
                                username="hadoop",
                                key_filename = self.key_file.name)
        except socket.error as e:
            raise ClusterConnectionError("Error connecting to %s: %s" %
                                         (self.cluster_info.master_ip, e))     

    def copy_file(self, local_path, remote_path):
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
            stdin, stdout, stderr = self.client.exec_command(cmd)
            for line in stdout:
                stdout_msg.append(line)
            for line in stderr:
                stderr_msg.append(line)
            retcode = stdout.channel.recv_exit_status()
            
        finally:
            self.client.close()

        if retcode != 0:
            raise ExecutionError(
                "Error running command on the cluster: %s. Stderr was: \n%s" % 
                (cmd, '\n'.join(stderr_msg)))

        return '\n'.join(stdout_msg)
