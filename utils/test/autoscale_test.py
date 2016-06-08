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
import boto.exception
import logging
import test_utils.neontest
from mock import patch, MagicMock
import tornado.testing
import unittest
import utils.autoscale
import utils.neon

_log = logging.getLogger(__name__)

class TestAutoScaleGroup(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.autoscale_mock = MagicMock()
        self.auto_conn_patcher = patch(
            'utils.autoscale.boto.connect_autoscale')
        self.auto_conn_patcher.start().return_value = self.autoscale_mock
        self.instance_info = boto.ec2.autoscale.Instance()
        self.instance_info.health_status = 'Healthy'
        self.instance_info.lifecycle_state = 'InService'
        self.instance_info.availability_zone = 'us-east-1c'
        self.instance_info.instance_id = 'id1'
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
                                     
        self.ec2_mock.get_only_instances.return_value = [self.ec2instance]

        self.metadata_patcher = patch('boto.utils.get_instance_metadata')
        self.metadata_mock = self.metadata_patcher.start()
        self.metadata_mock.side_effect = [{
            'placement' : {
                'availability-zone' : 'us-east-1c'
                }
            }]
        super(TestAutoScaleGroup, self).setUp()

    def tearDown(self):
        # Clear the singletons
        utils.autoscale.AutoScaleGroup._clear_singletons()
        utils.aws.InstanceMetadata._clear_singletons()
        
        self.auto_conn_patcher.stop()
        self.ec2_conn_patcher.stop()
        self.metadata_patcher.stop()
        super(TestAutoScaleGroup, self).tearDown()


    def test_get_ip(self):
        group = utils.autoscale.AutoScaleGroup('group1')
        self.assertEquals(group.get_ip(), '10.0.1.1')
        self.autoscale_mock.get_all_groups.assert_called_with(names=['group1'])
        self.ec2_mock.get_only_instances.assert_called_with(
            instance_ids=('id1',))
        
        self.ec2instance.private_ip_address = '10.0.1.2'
        self.assertEquals(group.get_ip(), '10.0.1.1')
        self.assertEquals(group.get_ip(force_refresh=True), '10.0.1.2')

    def test_only_get_instances_in_service(self):
        self.instance_info.lifecycle_state = 'Pending'

        with self.assertRaises(utils.autoscale.NoValidHostsError):
            utils.autoscale.AutoScaleGroup('group1').get_ip()

        for state in ['Terminating', 'Standby', 'Detached', 'Terminated']:
            self.instance_info.lifecycle_state = state

            with self.assertRaises(utils.autoscale.NoValidHostsError):
                utils.autoscale.AutoScaleGroup('group1').get_ip(True)

        self.instance_info.lifecycle_state = 'InService'
        with self.assertRaises(utils.autoscale.NoValidHostsError):
            utils.autoscale.AutoScaleGroup('group1').get_ip(False)

        self.assertEquals(
            utils.autoscale.AutoScaleGroup('group1').get_ip(True),
            '10.0.1.1')
        

    def test_different_az(self):
        instance2 = boto.ec2.autoscale.Instance()
        instance2.health_status = 'Healthy'
        instance2.lifecycle_state = 'InService'
        instance2.availability_zone = 'us-east-1d'
        instance2.instance_id = 'id_d'

        group = boto.ec2.autoscale.AutoScalingGroup()
        group.instances = [self.instance_info, instance2]
        self.autoscale_mock.get_all_groups.return_value = [group]

        ec2instance2 = EC2Instance()
        ec2instance2.private_ip_address='10.0.1.2'
        ec2instance2.id = 'id_d'
        self.ec2_mock.get_only_instances.return_value = [self.ec2instance,
                                                         ec2instance2]

        self.metadata_mock.side_effect = [{
            'placement' : {
                'availability-zone' : 'us-east-1d'
                }
            }]

        self.assertEquals(
            utils.autoscale.AutoScaleGroup('group1').get_ip(),
            '10.0.1.2')
        
        self.autoscale_mock.get_all_groups.assert_called_with(names=['group1'])
        self.ec2_mock.get_only_instances.assert_called_with(
            instance_ids=('id1', 'id_d'))

    @tornado.testing.gen_test
    def test_get_ip_list_only_cur_az(self):
        self.metadata_mock.side_effect = [{
            'placement' : {
                'availability-zone' : 'us-east-1d'
                }
            }]
        self.assertEquals(utils.autoscale.AutoScaleGroup('group1').get_ip(),
                          '10.0.1.1')

        ip_list = yield utils.autoscale.AutoScaleGroup('group1')._get_ip_list(
            True)
        self.assertEquals(ip_list, [])

    def test_connection_error(self):
        self.autoscale_mock.get_all_groups.side_effect = [
            boto.exception.BotoClientError('Fail')]
        with self.assertLogExists(logging.ERROR,
                                  'Could not refresh autoscale data'):
            with self.assertRaises(utils.autoscale.NoValidHostsError):
                utils.autoscale.AutoScaleGroup('group1').get_ip()

    def test_multiple_groups(self):
        self.assertEquals(
            utils.autoscale.MultipleAutoScaleGroups(['group1',
                                                     'group2']).get_ip(),
            '10.0.1.1')
        

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
