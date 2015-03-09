#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.opsworks
import boto.ec2
import json
import urllib2
import logging
import os
import random
import time
from utils import statemon

#Monitoring
statemon.define('video_server_connection_failed', int)
statemon.define('boto_connection_failed', int)
statemon.define('boto_vclient_launch', int)
statemon.define('boto_vclient_terminate', int)

from utils.options import define, options
define("aws_region", default="us-west-2", type=str,
       help="Region to look for the vclients")

AWS_REGION = 'us-west-2'
AWS_ACCESS_KEY_ID = 'AKIAIHEAXZIPN7HC5YBQ'
AWS_SECRET_ACCESS_KEY = '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'

#
video_server_ip = 'http://localhost'

# 
INITIAL_SLEEP       = 1
NORMAL_SLEEP        = 1
SCALE_UP_SLEEP      = 1
SCALE_DOWN_SLEEP    = 1

# to indicate that the process should quit
shutdown = False

# 
high_water_mark = 80 
low_water_mark = 10

#
minimum_vclients    = 8
maximum_vclients    = 20
SCALE_UP_FACTOR     = 0.2
SCALE_DOWN_FACTOR   = 0.1

# vclients states count
vclients_state_count = None


def get_video_server_queue_info():

    response = None

    try:
        response = urllib2.urlopen(video_server_ip)
    except URLError:
        statemon.state.increment('video_server_connection_failed')
        return -1
    except:
        statemon.state.increment('video_server_connection_failed')
        return -1
    
    try:
        data = json.loads(response)
        percentage = data['size']

        if percentage >= 0 and percentage <= 100:
            return percentage
        else: 
            return -1
    except:
        return -1


def is_scaling_in_progress(vclients):
      
    instances =  vclients['pending'] + vclients['shutting-down'] + vclients['stopping'] 

    if instances > 0: 
        return True;

    return False


def fetch_all_vclients_status(vclients):

    import pdb; pdb.set_trace()

    # open connection to AWS in our region
    conn = boto.ec2.connect_to_region(AWS_REGION,
                                      aws_access_key_id='AKIAIHEAXZIPN7HC5YBQ',
                                      aws_secret_access_key='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')

    if conn == None:
        statemon.state.increment('boto_connection_failed')
        return None

    # only inquire about video clients
    reservations = conn.get_all_instances(filters={"tag:opsworks:layer:vclient" : "Video Client"})
    
    if reservations == None:
        return None
 
    # extract instances
    vclient_instances = [i for r in reservations for i in r.instances]

    # count all vclients by their current state 
    for v in vclient_instances:
        if v.state == 'pending':
            vclients['pending'] += 1
        elif v.state == 'running':
            vclients['running'] += 1
        elif v.state == 'shutting-down':
            vclients['shutting-down'] += 1
        elif v.state == 'terminated':
            vclients['terminated'] += 1
        elif v.state == 'stopping':
            vclients['stopping'] += 1
        elif v.state == 'stopped':
            vclients['stopped'] += 1 

    print vclients
    return vclient_instances 


def fetch_all_vclients_status_opworks():
    
    import pdb; pdb.set_trace()
    
    conn = boto.opsworks.connect_to_region(options.aws_region)

    if(conn == None):
        return None

    conn.describe_instances( layer_id='Video Client')


def start_new_instances(instances_needed):
    print 'starting %d vclients' %instances_needed
    
    # open connection to AWS in our region
    conn = boto.ec2.connect_to_region(AWS_REGION, 
                                      aws_access_key_id=AWS_ACCESS_KEY_ID, 
                                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    if conn == None:
        statemon.state.increment('boto_connection_failed')
        return None

    # launch here 
    statemon.state.increment('boto_vclient_launch')


def terminate_instances(instances_needed, vclients):
    print 'terminating %d vclients' %instances_needed

     # open connection to AWS in our region
    conn = boto.ec2.connect_to_region(AWS_REGION,
                                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    if conn == None:
        statemon.state.increment('boto_connection_failed')
        return None

    # terminate the number of instances needed 
    statemon.state.increment('boto_vclient_terminate')


def handle_low_load(vclients_states_count, vclients):
    import pdb; pdb.set_trace()

    # minimum capacity already reached, no further action
    if vclients_states_count['running'] <= minimum_vclients:
        return

    # calculate how many vclients do we need to terminate, at least one
    instances_needed = max(1, int(vclients_states_count['running'] * SCALE_DOWN_FACTOR))

    # calculate the actual number of instances we can terminate before reaching
    # the minimum allowable 
    limit = vclients_states_count['running'] - minimum_vclients

    # pick the smallest number
    instances_needed = min(limit, instances_needed)

    # terminate a number of vclients from the list 
    # of running instances
    terminate_instances(instances_needed, vclients)


def handle_high_load(vclients):
    import pdb; pdb.set_trace()

    # alert, max capacity already reached
    if vclients['running'] >= maximum_vclients:
        return

    # calculate how many new vclients do we need
    if  vclients['running'] <= 0:
        # none running, therefore start the minimum
        instances_needed = minimum_vclients
    else:
        # increase the number of vclients by a factor, at least by one
        instances_needed = max(1, int(vclients['running'] * SCALE_UP_FACTOR))

        # calculate the max of instances we can actually launch
        # before we reach the maximum allowable
        limit = maximum_vclients - vclients['running']

        # pick the smaller number
        instances_needed = min(limit, instances_needed)

    start_new_instances(instances_needed)


###################################################################
# runloop
###################################################################
def runloop():

    # import pdb; pdb.set_trace()

    sleep_time = INITIAL_SLEEP 
    
    while(shutdown == False):

        # to hold the counts of all vclients and the state they're in
        vclients_states_count = {'pending':0,'running':0,'shutting-down':0,
                'terminated':0,'stopping':0,'stopped':0} 

        # periodic sleep
        time.sleep(sleep_time)
        sleep_time = NORMAL_SLEEP

        # get all video clients states (running, stopped, pending, etc))
        #vclients = None
        #vclients = fetch_all_vclients_status(vclients_states_count) 
 
        fetch_all_vclients_status_opworks()

        continue

        # error, try again later
        if vclients == None:
            continue

        # Here we check that no vclients are in the process of shutting down or 
        # starting up. This would be indicative that we migth have crashed and
        # restarted, or that some human intervention is going on.
        # As a safeguard, we do not take any action when vclient instances
        # are in transitory states.  
        if is_scaling_in_progress(vclients_states_count) == True:
            continue

        # get video server queue size
        queue_size = get_queue_size() 

        # error, try again later
        if(queue_size < 0):
            continue

        # scale up, the video server queue is growing
        if queue_size > high_water_mark:
            handle_high_load(vclients_states_count)
            sleep_time = SCALE_UP_SLEEP            

        # scale down, the video server queue is low
        elif queue_size < low_water_mark:
            handle_low_load(vclients_states_count, vclients)
            sleep_time = SCALE_DOWN_SLEEP


def main():
    # import pdb; pdb.set_trace()
    runloop()

if __name__ == "__main__":
        main()




