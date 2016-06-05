#!/usr/bin/env python
'''
Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto
from boto.ec2.instance import Instance as EC2Instance
import boto.ec2.autoscale
import logging
import test_utils.neontest
from mock import patch, MagicMock
import unittest
import utils.autoscale
import utils.neon

_log = logging.getLogger(__name__)

class TestAutoScaleGroup(test_utils.neontest.TestCase):
    def setUp(self):
        self.autoscale_mock = MagicMock()
        self.auto_conn_patcher = patch(
            'utils.autoscale.boto.connect_autoscale')
        self.auto_conn_patcher.start().return_value = self.autoscale_mock
        self.instance_info = boto.ec2.autoscale.Instance()
        self.instance_info.health_status = 'Healthy'
        self.instance_info.lifecycle_state = 'InService'
        self.instance_id = 'id1'
        group = boto.ec2.autoscale.AutoScalingGroup()
        group.instances = [self.instance_info]
        self.autoscale_mock.get_all_groups.return_value = [group]

        self.ec2_mock = MagicMock()
        self.ec2_conn_patcher = patch(
            'utils.autoscale.boto.connect_ec2')
        self.ec2_conn_patcher.start().return_value = self.ec2_mock
        self.ec2instance = EC2Instance()
        self.ec2instance.private_ip_address='10.0.1.1'
        self.ec2instance.id = 'id1'
        self.ec2instance._placement = 'us-east-1c'
                                     
        self.ec2_mock.get_only_instances.return_value = [self.ec2instance]

        self.metadata_patcher = patch('boto.utils.get_instance_metadata')
        self.metadata_mock = self.metadata_patcher.start()
        self.metadata_mock.side_effect = [{
            'placement' : {
                'availability-zone' : 'us-east-1c'
                }
            }]
            
            

    def tearDown(self):
        self.auto_conn_patcher.stop()
        self.ec2_conn_patcher.stop()
        self.metadata_patcher.stop()

        # Clear the singletons
        utils.autoscale.AutoScaleGroup._instances = {}

    def test_get_ip(self):
        group = utils.autoscale.AutoScaleGroup('group1')
        self.assertEquals(group.get_ip(), '10.0.1.1')
        self.ec2instance.private_ip_address = '10.0.1.2'
        self.assertEquals(group.get_ip(), '10.0.1.1')
        self.assertEquals(group.get_ip(force_refresh=True), '10.0.1.2')

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
