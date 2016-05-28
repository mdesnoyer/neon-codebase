'''
Utilities for interacting with an autoscaling group.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto
import concurrent.futures
import functools
import logging
import random
import threading
import tornado.ioloop
import tornado.gen
import utils.aws
from utils.options import define, options
import utils.sync

define('refresh_rate', default=120.0, type=float,
       help=('Rate (in seconds) at which to refresh autoscale info '
             'automatically'))

class NoValidHostsError(Exception): pass

class RefresherThread(threading.Thread):
    def __init__(self):
        self._lock = threading.RLock()
        self._timers = {} # name -> timer
        self.ioloop = tornado.ioloop.IOLoop()
        self.daemon = True

    def start(self):
        self.ioloop.make_current()
        self.ioloop.start()

    def add_group_to_monitor(self, group):
        '''Adds an AutoScaleGroup to monitor'''
        self.ioloop.add_callback(
            self._add_group_to_monitor_impl, group)

    def _add_group_to_monitor_impl(self, group):
        timer = utils.sync.PeriodicCoroutineTimer(group._refresh_data,
                                                  options.refresh_rate*1000,
                                                  self.ioloop)
        with self._lock:
            self._timers[group.name] = timer
        timer.start()

    def stop_monitoring_group(self, group_name):
        with self._lock:
            timer = self._timers[group_name]
            timer.stop()
            del self._timers[group_name]

    

class AutoScaleGroup(object):
    '''Class that watches an autoscaling group.'''

    def __init__(self, name):
        self.name = name # The autoscaling group name
        # List of dictionaries with instance info. will have
        # 'zone' and 'ip'
        self._instances = [] 

        self._lock = threading.RLock()

        self._executor = concurrent.futures.ThreadPoolExecutor(10)

    @tornado.gen.coroutine
    def _refresh_data(self):
        '''Refreshes the list of known ips.'''
        auto_conn = yield self._executor.submit(boto.connect_autoscale)
        ec2conn = yield self._executor.submit(boto.connect_ec2)
        groups = yield self._executor.submit(auto_conn.get_all_groups,
                                             names=[self.name])

        instance_info = []
        instance_ids = [x.instance_id for x in groups[0].instances if
                        x.lifecycle_state == 'InService']
        for i in range(0, len(instance_ids), 10):
            instances = yield self._executor.submit(ec2conn.get_only_instances,
                instance_ids=instance_ids[i:i+10])
            instance_info.extend([{'id': x.id,
                                   'ip': x.private_ip_address,
                                   'zone': x.placement} for x in instances])
            
        with self._lock:
            self._instances = instance_info
            

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_ip(self, force_refresh=False):
        '''Returns the ip address of a random instance in the autoscaling group.

        Inputs:
        force_refresh - Whether to check the instance group details before returning the ip.
        '''
        if force_refresh:
            yield self._refresh_data()

        with self._lock:
            # Pick an ip from this AZ if we can
            ips = [x['ip'] for x in self._instances 
                   if x['zone'] == utils.aws.get_current_az()]
            if len(ips) == 0:
                ips = [x['ip'] for x in self._instances]
            try:
                raise tornado.gen.Return(random.choice(ips))
            except IndexError:
                raise NoValidHostsError()
