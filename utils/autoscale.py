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
import boto.exception
import concurrent.futures
import functools
import itertools
import logging
import random
import threading
import tornado.ioloop
import tornado.gen
import utils.aws
import utils.obj
from utils.options import define, options
from utils import statemon
import utils.sync

define('refresh_rate', default=120.0, type=float,
       help=('Rate (in seconds) at which to refresh autoscale info '
             'automatically'))

statemon.define('ip_list_connection_lost', int)

_log = logging.getLogger(__name__)

class NoValidHostsError(Exception): pass

class RefresherThread(threading.Thread):
    __metaclass__ = utils.obj.Singleton
    
    def __init__(self):
        super(RefresherThread, self).__init__()
        self._lock = threading.RLock()
        self._timers = {} # name -> timer
        self.ioloop = tornado.ioloop.IOLoop()
        self.daemon = True

        self.start()

    def run(self):
        self.ioloop.make_current()
        self.ioloop.start()

    def add_group_to_monitor(self, group_name):
        '''Adds an AutoScaleGroup to monitor'''
        self.ioloop.add_callback(
            self._add_group_to_monitor_impl, group_name)

    def _add_group_to_monitor_impl(self, group_name):
        timer = utils.sync.PeriodicCoroutineTimer(
            lambda: self._refresh_data(group_name),
            options.refresh_rate*1000,
            self.ioloop)
        with self._lock:
            self._timers[group_name] = timer
        timer.start()

    @tornado.gen.coroutine
    def _refresh_data(self, group_name):
        group = AutoScaleGroup(group_name)
        yield group._refresh_data()

    def stop_monitoring_group(self, group_name):
        with self._lock:
            try:
                timer = self._timers[group_name]
                timer.stop()
                del self._timers[group_name]
            except KeyError:
                pass

    

class AutoScaleGroup(object):
    '''Class that watches an autoscaling group.'''
    __metaclass__ = utils.obj.KeyedSingleton

    def __init__(self, name):
        self.name = name # The autoscaling group name
        # List of dictionaries with instance info. will have
        # 'zone' and 'ip'
        self._instance_info = None 

        self._lock = threading.RLock()

        self._executor = concurrent.futures.ThreadPoolExecutor(10)

        # Start monitoring this group
        RefresherThread().add_group_to_monitor(name)

    def __del__(self):
        # Stop monitoring this group
        RefresherThread().stop_monitoring_group(self.name)

    @tornado.gen.coroutine
    def _refresh_data(self):
        '''Refreshes the list of known ips.'''
        try:
            auto_conn = yield self._executor.submit(boto.connect_autoscale)
            ec2conn = yield self._executor.submit(boto.connect_ec2)
            groups = yield self._executor.submit(auto_conn.get_all_groups,
                                                 names=[self.name])

            instance_info = []
            id_zone = [(x.instance_id, x.availability_zone)
                       for x in groups[0].instances if
                       x.lifecycle_state == 'InService']
            for i in range(0, len(id_zone), 10):
                chunk = id_zone[i:i+10]
                instances = yield self._executor.submit(
                    ec2conn.get_only_instances,
                    instance_ids=zip(*chunk)[0])
                instance_info.extend([{'id': y[0],
                                       'ip': x.private_ip_address,
                                       'zone': y[1]} 
                                       for x, y in zip(instances, chunk)])
            statemon.state.ip_list_connection_lost = 0
        except (boto.exception.BotoClientError, boto.exception.BotoServerError) as e:
            _log.error('Could not refresh autoscale data: %s' % e)
            statemon.state.ip_list_connection_lost = 1
            return
            
        with self._lock:
            self._instance_info = instance_info
            

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_ip(self, force_refresh=False):
        '''Returns the ip address of a random instance in the autoscaling group.

        Inputs:
        force_refresh - Whether to check the instance group details before returning the ip.
        only_cur_az - If true, only returns IPs from this AZ
        '''
        if force_refresh or self._instance_info is None:
            yield self._refresh_data()
        if self._instance_info is None:
            # There was a connection issue, but that's logged separately
            raise NoValidHostsError()

        ips = yield self._get_ip_list()
                    
        try:
            raise tornado.gen.Return(random.choice(ips))
        except IndexError:
            raise NoValidHostsError()

    @tornado.gen.coroutine
    def _get_ip_list(self, only_cur_az=False):
        '''Returns a list of ip addresses.

        Inputs:
        only_cur_az - If True only return IPs for this AZ
        '''
        metadata = utils.aws.InstanceMetadata()
        cur_az = yield self._executor.submit(metadata.get_current_az)

        with self._lock:
            # Pick an ip from this AZ if we can
            ips = [x['ip'] for x in self._instance_info
                   if x['zone'] == cur_az]
            if len(ips) == 0 and not only_cur_az:
                ips = [x['ip'] for x in self._instance_info]

        raise tornado.gen.Return(ips)


class MultipleAutoScaleGroups(object):
    '''Class that watches many autoscaling groups.'''
    def __init__(self, names):
        self.groups = [AutoScaleGroup(x) for x in names]

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_ip(self, force_refresh=False):
        if force_refresh or any([x._instance_info is None 
                                 for x in self.groups]):
            yield [x._refresh_data() for x in self.groups]

        ip_lists = yield [x._get_ip_list(True) for x in self.groups]
        ips = list(itertools.chain.from_iterable(ip_lists))
        if len(ips) == 0:
            ip_lists = yield [x._get_ip_list(False) for x in self.groups]
            ips = list(itertools.chain.from_iterable(ip_lists))

        try:
            raise tornado.gen.Return(random.choice(ips))
        except IndexError:
            raise NoValidHostsError()
