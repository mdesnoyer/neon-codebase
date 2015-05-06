#!/usr/bin/env python

'''
Unit test for Autoscaler
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import unittest
import utils.neon
import video_processor.autoscaler
import boto.opsworks.layer1
from mock import patch
from StringIO import StringIO
from utils.options import options

_log = logging.getLogger(__name__)

# Constants
VP_AUTOSCALER_MIN_INSTANCES = 'video_processor.autoscaler.minimum_instances'
VP_AUTOSCALER_MAX_INSTANCES = 'video_processor.autoscaler.maximum_instances'
VP_AUTOSCALER_MB_PER_VCLIENTS = 'video_processor.autoscaler.mb_per_vclient'
VP_AUTOSCALER_NORMAL_SLEEP = 'video_processor.autoscaler.normal_sleep'
VP_AUTOSCALER_SCALE_UP_SLEEP = 'video_processor.autoscaler.scale_up_sleep'
VP_AUTOSCALER_BATCH_END = 'video_processor.autoscaler.enable_batch_termination'


class MockOpsWorksConnection:
    def __init__(self):
        self.id = 0
        self.instances_dict = {"Instances": [
            {"Status": "shutting_down", "InstanceId": "%s" % self.inc_id()},
            {"Status": "online", "InstanceId": "%s" % self.inc_id()},
            {"Status": "pending", "InstanceId": "%s" % self.inc_id()}
        ]}

    def inc_id(self):
        self.id += 1
        return self.id

    def instances_count(self):
        return len(self.instances_dict["Instances"])

    def describe_instances(self, layer_id=None):
        return self.instances_dict

    def create_instance(self, stack_id=None, layer_ids=None,
                        ami_id=None, instance_type=None, os=None):
        val = {"Status": "requested", "InstanceId": "%s" % self.inc_id()}
        self.instances_dict["Instances"].append(val)
        return val

    def modify_instance(self, instance_id, new_status):
        self.instances_dict["Instances"] = [
            i for i in self.instances_dict["Instances"]
            if i['InstanceId'] != (instance_id)
        ]
        self.instances_dict["Instances"].append({
            "Status": new_status,
            "InstanceId": instance_id
        })

    def start_instance(self, instance_id):
        self.modify_instance(instance_id, "online")
        return None

    def stop_instance(self, instance_id):
        self.modify_instance(instance_id, "stopped")
        return None

    def delete_instance(self, instance_id):
        self.modify_instance(instance_id, "terminated")
        return None


class TestAutoScaler(unittest.TestCase):
    def setUp(self):
        super(TestAutoScaler, self).setUp()

        # Default values
        options._set(VP_AUTOSCALER_MIN_INSTANCES, 3)
        options._set(VP_AUTOSCALER_MAX_INSTANCES, 20)
        options._set(VP_AUTOSCALER_MB_PER_VCLIENTS, 10)
        options._set(VP_AUTOSCALER_NORMAL_SLEEP, 1)
        options._set(VP_AUTOSCALER_SCALE_UP_SLEEP, 1)
        options._set(VP_AUTOSCALER_BATCH_END, 0)

    def tearDown(self):
        super(TestAutoScaler, self).tearDown()

    def get_string_io_formatted(self, qsize, qbytes):
        return StringIO('{"size": %d, "bytes": %d}' % (qsize, qbytes))

    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_video_server_queue_info(self, mock_urlopen):
        mock_urlopen.return_value = self.get_string_io_formatted(10, 20)

        resp = video_processor.autoscaler.get_video_server_queue_info()
        self.assertEqual(resp['size'], 10)
        self.assertEqual(resp['bytes'], 20)

    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_get_vclients(self, mock_region, mock_desc_instances):
        instances_dict = {"Instances": [
            {"Status": "shutting_down", "InstanceId": "0"},
            {"Status": "online", "InstanceId": "1"},
            {"Status": "pending", "InstanceId": "2"},
            {"Status": "stopped", "InstanceId": "3"},
            {"Status": "stopped", "InstanceId": "4"}
        ]}

        mock_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_desc_instances.return_value = instances_dict

        resp = video_processor.autoscaler.get_vclients(['shutting_down'])
        self.assertEqual(len(resp), 1)

        resp = video_processor.autoscaler.get_vclients(['stopped'])
        self.assertEqual(len(resp), 2)

        resp = video_processor.autoscaler.get_vclients(['online', 'pending'])
        self.assertEqual(len(resp), 2)

        resp = video_processor.autoscaler.get_vclients([
            'shutting_down',
            'online',
            'pending',
            'stopped'
        ])
        self.assertEqual(len(resp), len(instances_dict['Instances']))

    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_get_num_operational_vclients(self, mock_region,
                                          mock_desc_instances):
        instances_dict = {"Instances": [
            {"Status": "shutting_down", "InstanceId": "0"},
            {"Status": "online", "InstanceId": "1"},
            {"Status": "pending", "InstanceId": "2"}
        ]}

        mock_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_desc_instances.return_value = instances_dict

        resp = video_processor.autoscaler.get_num_operational_vclients()
        self.assertEqual(resp, 2)

        instances_dict['Instances'].append({
            "Status": "requested",
            "InstanceId": "%s" % len(instances_dict['Instances'])
        })
        resp = video_processor.autoscaler.get_num_operational_vclients()
        self.assertEqual(resp, 3)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_maximum_vclients_is_not_exceeded(self, mock_urlopen,
                                              mock_num_oper_vclients):
        qsize = options.get(VP_AUTOSCALER_MIN_INSTANCES) + 100
        qbytes = options.get(VP_AUTOSCALER_MB_PER_VCLIENTS) * qsize * 1048576

        mock_num_oper_vclients.return_value = 0
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 20)

        mock_num_oper_vclients.return_value = 5
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 15)

        mock_num_oper_vclients.return_value = 20
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_minimum_vclients_are_maintained(self, mock_urlopen,
                                             mock_num_oper_vclients):
        qsize = options.get(VP_AUTOSCALER_MIN_INSTANCES) - 1
        qbytes = options.get(VP_AUTOSCALER_MB_PER_VCLIENTS) * qsize * 1048576

        mock_num_oper_vclients.return_value = 0
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 3)

        mock_num_oper_vclients.return_value = 3
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_num_vclients_to_change_for_scaling_up(self, mock_urlopen,
                                                   mock_num_oper_vclients):
        qsize = 5
        qbytes = options.get(VP_AUTOSCALER_MB_PER_VCLIENTS) * qsize * 1048576

        mock_num_oper_vclients.return_value = 0
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 5)

        mock_num_oper_vclients.return_value = 3
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 2)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_num_vclients_to_change_for_scaling_down(self, mock_urlopen,
                                                     mock_num_oper_vclients):
        qsize = 5
        qbytes = options.get(VP_AUTOSCALER_MB_PER_VCLIENTS) * qsize * 1048576

        mock_num_oper_vclients.return_value = 7
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -2)

        mock_num_oper_vclients.return_value = 22
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -17)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_num_vclients_to_change_for_no_scaling(self, mock_urlopen,
                                                   mock_num_oper_vclients):
        qsize = 8
        qbytes = options.get(VP_AUTOSCALER_MB_PER_VCLIENTS) * qsize * 1048576

        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_num_vclients_to_change_according_to_bytes(self, mock_urlopen,
                                                       mock_num_oper_vclients):
        mb_per_vclients = options.get(VP_AUTOSCALER_MB_PER_VCLIENTS)

        # Bytes > Queue size
        qsize = 8
        qbytes = mb_per_vclients * (qsize * 1048576 * 2.5)
        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

        qsize = 5
        qbytes = mb_per_vclients * (qsize * 1048576 * 2.5)
        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -3)

        # Bytes < Queue size
        qsize = 8
        qbytes = mb_per_vclients * (qsize * 1048576 / 2)
        mock_num_oper_vclients.return_value = 4
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

        qsize = 8
        qbytes = mb_per_vclients * (qsize * 1048576 / 2)
        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -4)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_num_vclients_to_change_w_negative_bytes(self, mock_urlopen,
                                                     mock_num_oper_vclients):
        mb_per_vclients = options.get(VP_AUTOSCALER_MB_PER_VCLIENTS)

        qsize = 10
        qbytes = mb_per_vclients * (1048576 * -1)
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

        qsize = 0
        qbytes = mb_per_vclients * (1048576 * -1)
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

        options._set(VP_AUTOSCALER_MB_PER_VCLIENTS, 0)
        qsize = 3
        qbytes = mb_per_vclients * (1048576 * 20)
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

        options._set(VP_AUTOSCALER_MB_PER_VCLIENTS, 0)
        qsize = 3
        qbytes = 0
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = self.get_string_io_formatted(qsize, qbytes)
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

    @patch('boto.opsworks.layer1.OpsWorksConnection.start_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.create_instance')
    @patch('boto.opsworks.connect_to_region')
    def test_start_new_instances(self, mock_region, mock_create_instance,
                                 mock_start_instance):
        num_instances_to_create = 5
        instances_created = {
            "InstanceId": "5f9adeaa-c94c-42c6-aeef-28a5376002cd"
        }

        mock_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_create_instance.return_value = instances_created
        mock_start_instance.return_value = None

        resp = video_processor.autoscaler.start_new_instances(
            num_instances_to_create
        )
        self.assertEqual(resp, num_instances_to_create)

    @patch('boto.opsworks.layer1.OpsWorksConnection.delete_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.stop_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_terminate_instances(self, mock_region, mock_describe_instances,
                                 mock_stop_instance, mock_delete_instance):
        num_instances_to_terminate = 3
        instances_dict = {"Instances": [
            {"Status": "online", "InstanceId": "0"},
            {"Status": "online", "InstanceId": "1"},
            {"Status": "online", "InstanceId": "2"},
            {"Status": "terminated", "InstanceId": "3"}
        ]}
        instances_stopped_dict = {"Instances": [
            {"Status": "stopped", "InstanceId": "0"}
        ]}

        mock_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_describe_instances.side_effect = [
            instances_dict,
            instances_stopped_dict,
            instances_dict,
            instances_stopped_dict,
            instances_dict,
            instances_stopped_dict,
            instances_dict
        ]
        mock_stop_instance.return_value = None
        mock_delete_instance.return_value = None

        resp = video_processor.autoscaler.terminate_instances(
            num_instances_to_terminate
        )
        self.assertEqual(resp, num_instances_to_terminate)

    @patch('boto.opsworks.layer1.OpsWorksConnection.delete_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.stop_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_terminate_instances_with_killed_outside(self,
                                                     mock_region,
                                                     mock_describe_instances,
                                                     mock_stop_instance,
                                                     mock_delete_instance):
        num_instances_to_terminate = 3
        instances_dict = {"Instances": [
            {"Status": "online", "InstanceId": "0"},
            {"Status": "online", "InstanceId": "1"},
            {"Status": "online", "InstanceId": "2"},
            {"Status": "terminated", "InstanceId": "3"}
        ]}
        instances_stopped_dict = {"Instances": [
            {"Status": "stopped", "InstanceId": "0"}
        ]}
        instances_terminated_dict = {"Instances": [
            {"Status": "terminated", "InstanceId": "0"},
            {"Status": "terminated", "InstanceId": "1"},
            {"Status": "terminated", "InstanceId": "2"}
        ]}

        mock_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_describe_instances.side_effect = [
            instances_dict,
            instances_stopped_dict,
            instances_terminated_dict
        ]
        mock_stop_instance.return_value = None
        mock_delete_instance.return_value = None

        # terminate_instances returns the number of instances
        #   it has tried to terminate.
        # That number does not include instances terminated
        #   from outside of the script.
        resp = video_processor.autoscaler.terminate_instances(
            num_instances_to_terminate
        )
        self.assertEqual(resp, 1)

    @patch('video_processor.autoscaler.is_shutdown')
    @patch('video_processor.autoscaler.get_video_server_queue_info')
    @patch('boto.opsworks.connect_to_region')
    def test_run_loop(self, mock_region, mock_queue_info, mock_is_shutdown):
        mock_opsworks_conn = MockOpsWorksConnection()
        mock_region.return_value = mock_opsworks_conn
        func = video_processor.autoscaler.get_num_operational_vclients

        # First iteration
        mod = 3
        mock_is_shutdown.side_effect = [False, True]
        mock_queue_info.return_value = {
            "size": func() + mod, "bytes": (func() + mod) * 10485760 + 1
        }
        ic = mock_opsworks_conn.instances_count()
        video_processor.autoscaler.run_loop()
        self.assertEqual(mock_opsworks_conn.instances_count(), ic + mod)

        # Second iteration
        mod = 10
        mock_is_shutdown.side_effect = [False, True]
        mock_queue_info.return_value = {
            "size": func() + mod, "bytes": (func() + mod) * 10485760 + 1
        }
        ic = mock_opsworks_conn.instances_count()
        video_processor.autoscaler.run_loop()
        self.assertEqual(mock_opsworks_conn.instances_count(), ic + mod)

        # Third iteration
        mod = options.get(VP_AUTOSCALER_MAX_INSTANCES)
        mock_is_shutdown.side_effect = [False, True]
        mock_queue_info.return_value = {
            "size": func() + mod, "bytes": (func() + mod) * 10485760 + 1
        }
        video_processor.autoscaler.run_loop()
        self.assertEqual(
            video_processor.autoscaler.get_num_operational_vclients(),
            mod
        )

        # Fourth iteration (with enable_batch_termination = False)
        options._set(VP_AUTOSCALER_BATCH_END, 0)
        mod = -10
        mock_is_shutdown.side_effect = [False, True]
        mock_queue_info.return_value = {
            "size": func() + mod, "bytes": (func() + mod) * 10485760 + 1
        }
        vc = video_processor.autoscaler.get_num_operational_vclients()
        video_processor.autoscaler.run_loop()
        vc1 = video_processor.autoscaler.get_num_operational_vclients()
        self.assertEqual((vc - 1), vc1)

        # Fifth iteration (with enable_batch_termination = False)
        options._set(VP_AUTOSCALER_BATCH_END, 0)
        mod = -10
        mock_is_shutdown.side_effect = [False, False, False, True]
        mock_queue_info.return_value = {
            "size": func() + mod, "bytes": (func() + mod) * 10485760 + 1
        }
        vc = video_processor.autoscaler.get_num_operational_vclients()
        video_processor.autoscaler.run_loop()
        vc1 = video_processor.autoscaler.get_num_operational_vclients()
        self.assertEqual((vc - 3), vc1)

        # Sixth iteration (with enable_batch_termination = True)
        options._set(VP_AUTOSCALER_BATCH_END, 1)
        mod = video_processor.autoscaler.get_num_operational_vclients() * -1
        mock_is_shutdown.side_effect = [False, True]
        mock_queue_info.return_value = {
            "size": func() + mod, "bytes": (func() + mod) * 10485760 + 1
        }
        video_processor.autoscaler.run_loop()
        self.assertEqual(
            video_processor.autoscaler.get_num_operational_vclients(),
            options.get(VP_AUTOSCALER_MIN_INSTANCES)
        )

        # Seventh iteration
        options._set(VP_AUTOSCALER_BATCH_END, 0)
        mock_is_shutdown.side_effect = [False, False, False, False, True]
        mock_queue_info.side_effect = [
            {"size": 8, "bytes": 8 * 10485760 + 1},   # 8
            {"size": 5, "bytes": 5 * 10485760 + 1},   # 7
            {"size": 2, "bytes": 2 * 10485760 + 1},   # 6
            {"size": 11, "bytes": 11 * 10485760 + 1}  # 11
        ]
        video_processor.autoscaler.run_loop()
        self.assertEqual(
            video_processor.autoscaler.get_num_operational_vclients(),
            11
        )

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
