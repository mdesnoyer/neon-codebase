#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.opsworks
import json
import urllib2
import logging
import time
import utils.neon
from utils import statemon
from utils.options import define, options

_log = logging.getLogger(__name__)

define("video_server", default="localhost", type=str,
       help="Video server api to get Queue Stats")
define("video_server_auth", default="secret_token", type=str,
       help="Secret token for talking with the video processing server")
define("aws_region", default="us-east-1", type=str,
       help="Region to look for the vclients")
define("stack_id", default="eb842d2e-b9e3-471e-b4eb-70aa7b40f2c6", type=str,
       help="The stack ID")
define("layer_id", default="599ee960-1122-49d7-ac74-e281fe71b54b", type=str,
       help="The instance layer ID")
define("ami_id", default="ami-dadfb6b2", type=str,
       help="AMI ID to be used to create the instance")
define("instance_type", default="t2.small", type=str,
       help="The instance type")
define("minimum_instances", default=8, type=int,
       help="Minimum number of instances to keep")
define("maximum_instances", default=20, type=int,
       help="Maximum number of instances to launch")
define("mb_per_vclient", default=500, type=int,
       help="Number of MB one client can process")
define("enable_batch_termination", default=0, type=int,
       help="When 0 only one instance is stopped per iteration." +
       "If 1 multiple instances can be stopped per interation")

# Monitoring
statemon.define('video_server_connection_failed', int)
statemon.define('boto_connection_failed', int)
statemon.define('boto_vclient_launch', int)
statemon.define('boto_vclient_terminate', int)

# Sleep Constants
NORMAL_SLEEP = 1
SCALE_UP_SLEEP = 5

# To indicate that the process should quit
SHUTDOWN = False

# Instances in these states are considered in the count of
# active video client instances
VALID_OPERATIONAL_STATUS = [
    'rebooting',
    'requested',
    'pending',
    'booting',
    'running_setup',
    'online'
]


def get_video_server_queue_info():
    '''
    Request the queue info from the video server.
    :returns: None or a dict containing the Q size (size) and
              Total number of bytes to be processed (bytes).
    '''
    response = None

    try:
        client_url = 'http://%s:8081/queuestats' % options.video_server
        headers = {'X-Neon-Auth': options.video_server_auth}

        request = urllib2.Request(client_url, None, headers)
        response = urllib2.urlopen(request, timeout=5)
    except:
        statemon.state.increment('video_server_connection_failed')
        return response

    try:
        data = json.loads(response.read())

        if data['size'] > 0 and data['bytes'] <= 0:
            _log.warn("Queue returning size > 0 with bytes <= 0")
        elif data['size'] <= 0 and data['bytes'] > 0:
            _log.warn("Queue returning size <= 0 with bytes > 0")

        return data
    except Exception, e:
        _log.error("Failed to read/convert data to json: %s" % e)
        return response


def get_vclients(status_list):
    '''
    Retrieve video clients from the video client layer filtering by Status.
    :param status_list: The instances status to filter for.
    :returns: List containing filtered instances.
    '''
    conn = boto.opsworks.connect_to_region(options.aws_region)

    if conn is None:
        statemon.state.increment('boto_connection_failed')
        return []

    instances_list = conn.describe_instances(
        layer_id=options.layer_id
    )['Instances']

    filter_list = [i for i in instances_list if i['Status'] in status_list]
    return filter_list


def get_num_operational_vclients():
    '''
    Retrieve the total number of active video clients.
    :returns: Total number.
    '''
    return len(get_vclients(VALID_OPERATIONAL_STATUS))


def get_number_vclient_to_change():
    '''
    Calculates the number of instances needed to be added or removed.
    :returns: Negative number for removal,
              Positive for addition or zero for no change.
    '''
    queue_info = get_video_server_queue_info()

    if queue_info is None:
        return 0

    mb_per_vclient = options.mb_per_vclient * 1048576
    if mb_per_vclient <= 0:
        mb_per_vclient = max(1, queue_info['bytes'])

    oper1 = min(queue_info['size'], queue_info['bytes'] / mb_per_vclient)
    oper2 = max(options.minimum_instances, oper1)
    should_have_num_vclients = min(options.maximum_instances, oper2)
    return should_have_num_vclients - get_num_operational_vclients()


def start_new_instances(instances_needed):
    '''
    Instantiates new video clients.
    :param instances_needed: Number of instances to create.
    :returns: Total number of newly created instances.
    '''
    conn = boto.opsworks.connect_to_region(options.aws_region)

    if conn is None:
        statemon.state.increment('boto_connection_failed')
        return 0

    # launch the number of instances needed
    num_instances_created = 0
    for x in range(instances_needed):
        InstanceId = conn.create_instance(
            stack_id=options.stack_id,
            layer_ids=[options.layer_id],
            ami_id=options.ami_id,
            instance_type=options.instance_type,
            os='Custom'
        )['InstanceId']

        if InstanceId is not None:
            conn.start_instance(InstanceId)
            num_instances_created += 1
            statemon.state.increment('boto_vclient_launch')
            # _log.info("Launch instance number %s with id %s", x, InstanceId)

    return num_instances_created


def terminate_instances(instances_needed):
    '''
    Stops and terminates video clients, waits on termination of the instances.
    :param instances_needed: Number of instances to terminate.
    :returns: Total number of instances terminated by this method.
    '''
    conn = boto.opsworks.connect_to_region(options.aws_region)
    if conn is None:
        statemon.state.increment('boto_connection_failed')
        return 0

    instancesValidList = get_vclients(VALID_OPERATIONAL_STATUS)

    # terminate the number of instances needed
    instanceIdToTerminateList = []
    for x in instancesValidList:
        conn.stop_instance(x['InstanceId'])
        instanceIdToTerminateList.append(x['InstanceId'])
        # _log.info("Stop instance number %s - %s",
        #          len(instanceIdToTerminateList),
        #          x['InstanceId'])

        if len(instanceIdToTerminateList) == instances_needed:
            break

    num_instances_terminated = 0
    while (num_instances_terminated < len(instanceIdToTerminateList)):
        stoppedInstanceList = get_vclients(['stopped'])
        for x in stoppedInstanceList:
            conn.delete_instance(x['InstanceId'])
            num_instances_terminated += 1
            statemon.state.increment('boto_vclient_terminate')
            # _log.info("Terminate instance %s", num_instances_terminated)

        # Protected for terminated instance outside of script
        # Instances terminate outside of script do not go towards the count
        terminatedList = get_vclients(['terminated'])
        ids = [i for i in terminatedList
               if i['InstanceId'] in instanceIdToTerminateList]
        instances_terminated = len(ids) == len(instanceIdToTerminateList)
        if instances_terminated:
            break

    return num_instances_terminated


def runloop():
    '''
    Constantly checks number of clients to add or remove and
    performs correct action.
    '''
    while(not SHUTDOWN):
        sleep_time = NORMAL_SLEEP

        number = get_number_vclient_to_change()
        if number > 0:
            sleep_time = SCALE_UP_SLEEP
            start_new_instances(number)
        elif number < 0:
            # Here we are stopping and terminating "number" of vclients,
            #  if batch termination is enabled.
            # If we want to terminate clients at slower rate
            #  (so that we do not lose out on startup times)
            # Set enable_batch_termination to False which
            #   will result in only one instance being stopped within the loop
            # The next time we go through the loop
            #   we will shut down another if we still need to.
            pos_number = abs(number) if options.enable_batch_termination else 1
            terminate_instances(pos_number)

        time.sleep(sleep_time)


def main():
    utils.neon.InitNeon()
    runloop()

if __name__ == "__main__":
    main()
