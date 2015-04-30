#!/usr/bin/env python

'''
Unit test for Autoscaler 
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] <> __base_path__:
    sys.path.insert(0, __base_path__)

from mock import patch, MagicMock
import logging
import unittest
import utils.neon
import video_processor.autoscaler
import boto.opsworks.layer1
from StringIO import StringIO

_log = logging.getLogger(__name__)

# Default values
MAXIMUM_VCLIENTS = 20
MINIMUM_VCLIENTS = 3
BYTES_PER_VCLIENT = 10*1024*1024 # 10MB

class TestAutoScaler(unittest.TestCase):
    def setUp(self):
        super(TestAutoScaler, self).setUp()

    def tearDown(self):
        super(TestAutoScaler, self).tearDown()

    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_video_server_queue_info(self, mock_urlopen):
        mock_urlopen.return_value = StringIO('{"size": 10, "bytes": 20}')

        resp = video_processor.autoscaler.get_video_server_queue_info()
        self.assertEqual(resp['size'], 10)
        self.assertEqual(resp['bytes'], 20)

    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_get_vclients(self, mock_connect_to_region, mock_describe_instances):
        instancesDict = { "Instances": [ 
            {"Status":"shutting_down", "InstanceId":"0"},
            {"Status":"online", "InstanceId":"1"},
            {"Status":"pending", "InstanceId":"2"},
            {"Status":"stopped", "InstanceId":"3"}, 
            {"Status":"stopped", "InstanceId":"4"}
        ] }

        mock_connect_to_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_describe_instances.return_value = instancesDict

        resp = video_processor.autoscaler.get_vclients(['shutting_down'])
        self.assertEqual(len(resp), 1)

        resp = video_processor.autoscaler.get_vclients(['stopped'])
        self.assertEqual(len(resp), 2)

        resp = video_processor.autoscaler.get_vclients(['online', 'pending'])
        self.assertEqual(len(resp), 2)

        resp = video_processor.autoscaler.get_vclients(['shutting_down', 'online', 'pending', 'stopped'])
        self.assertEqual(len(resp), len(instancesDict['Instances']))

    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_get_num_operational_vclients(self, mock_connect_to_region, mock_describe_instances):
        instancesDict = { "Instances": [ 
            {"Status":"shutting_down", "InstanceId":"0"},
            {"Status":"online", "InstanceId":"1"},
            {"Status":"pending", "InstanceId":"2"}
        ] }

        mock_connect_to_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_describe_instances.return_value = instancesDict

        resp = video_processor.autoscaler.get_num_operational_vclients()
        self.assertEqual(resp, 2)

        instancesDict['Instances'].append( {"Status":"requested", "InstanceId":"%s" %len(instancesDict['Instances'])} )
        resp = video_processor.autoscaler.get_num_operational_vclients()
        self.assertEqual(resp, 3)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_maximum_vclients_is_not_exceeded(self, mock_urlopen, mock_num_oper_vclients):
        video_processor.autoscaler.MAXIMUM_VCLIENTS = MAXIMUM_VCLIENTS
        video_processor.autoscaler.MINIMUM_VCLIENTS = MINIMUM_VCLIENTS
        video_processor.autoscaler.BYTES_PER_VCLIENT = BYTES_PER_VCLIENT

        qsize = video_processor.autoscaler.MAXIMUM_VCLIENTS + 100
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * qsize

        mock_num_oper_vclients.return_value = 0
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 20) 

        mock_num_oper_vclients.return_value = 5
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 15)

        mock_num_oper_vclients.return_value = 20
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_minimum_vclients_are_maintained(self, mock_urlopen, mock_num_oper_vclients):
        video_processor.autoscaler.MAXIMUM_VCLIENTS = MAXIMUM_VCLIENTS
        video_processor.autoscaler.MINIMUM_VCLIENTS = MINIMUM_VCLIENTS
        video_processor.autoscaler.BYTES_PER_VCLIENT = BYTES_PER_VCLIENT

        qsize = video_processor.autoscaler.MINIMUM_VCLIENTS - 1
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * qsize

        mock_num_oper_vclients.return_value = 0
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 3)

        mock_num_oper_vclients.return_value = 3
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_get_number_vclients_to_change_for_scaling_up(self, mock_urlopen, mock_num_oper_vclients):
        video_processor.autoscaler.MAXIMUM_VCLIENTS = MAXIMUM_VCLIENTS
        video_processor.autoscaler.MINIMUM_VCLIENTS = MINIMUM_VCLIENTS
        video_processor.autoscaler.BYTES_PER_VCLIENT = BYTES_PER_VCLIENT

        qsize = 5
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * qsize

        mock_num_oper_vclients.return_value = 0
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 5)

        mock_num_oper_vclients.return_value = 3
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 2)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_get_number_vclients_to_change_for_scaling_down(self, mock_urlopen, mock_num_oper_vclients):
        video_processor.autoscaler.MAXIMUM_VCLIENTS = MAXIMUM_VCLIENTS
        video_processor.autoscaler.MINIMUM_VCLIENTS = MINIMUM_VCLIENTS
        video_processor.autoscaler.BYTES_PER_VCLIENT = BYTES_PER_VCLIENT

        qsize = 5
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * qsize

        mock_num_oper_vclients.return_value = 7
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -2)

        mock_num_oper_vclients.return_value = 22
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -17)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_get_number_vclients_to_change_for_no_scaling(self, mock_urlopen, mock_num_oper_vclients):
        video_processor.autoscaler.MAXIMUM_VCLIENTS = MAXIMUM_VCLIENTS
        video_processor.autoscaler.MINIMUM_VCLIENTS = MINIMUM_VCLIENTS
        video_processor.autoscaler.BYTES_PER_VCLIENT = BYTES_PER_VCLIENT

        qsize = 8
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * qsize

        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_get_number_vclients_to_change_according_to_bytes_to_process(self, mock_urlopen, mock_num_oper_vclients):
        video_processor.autoscaler.MAXIMUM_VCLIENTS = MAXIMUM_VCLIENTS
        video_processor.autoscaler.MINIMUM_VCLIENTS = MINIMUM_VCLIENTS
        video_processor.autoscaler.BYTES_PER_VCLIENT = BYTES_PER_VCLIENT

        # Bytes > Queue size
        qsize = 8
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * (qsize*2.5)
        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

        qsize = 5
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * (qsize*2.5)
        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -3)

        # Bytes < Queue size
        qsize = 8
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * (qsize/2)
        mock_num_oper_vclients.return_value = 4
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, 0)

        qsize = 8
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * (qsize/2)
        mock_num_oper_vclients.return_value = 8
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -4)

    @patch('video_processor.autoscaler.get_num_operational_vclients')
    @patch('video_processor.autoscaler.urllib2.urlopen')
    def test_get_number_vclients_to_change_with_negative_bytes(self, mock_urlopen, mock_num_oper_vclients):
        video_processor.autoscaler.MAXIMUM_VCLIENTS = MAXIMUM_VCLIENTS
        video_processor.autoscaler.MINIMUM_VCLIENTS = MINIMUM_VCLIENTS
        video_processor.autoscaler.BYTES_PER_VCLIENT = BYTES_PER_VCLIENT

        qsize = 10
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * -1
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

        qsize = 0
        qbytes = video_processor.autoscaler.BYTES_PER_VCLIENT * 1
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

        video_processor.autoscaler.BYTES_PER_VCLIENT = 0
        qsize = 3
        qbytes = BYTES_PER_VCLIENT*20
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

        video_processor.autoscaler.BYTES_PER_VCLIENT = 0
        qsize = 3
        qbytes = 0
        mock_num_oper_vclients.return_value = 10
        mock_urlopen.return_value = StringIO('{"size": %d, "bytes": %d}' %(qsize, qbytes))
        resp = video_processor.autoscaler.get_number_vclient_to_change()
        self.assertEqual(resp, -7)

    @patch('boto.opsworks.layer1.OpsWorksConnection.create_instance')
    @patch('boto.opsworks.connect_to_region')
    def test_start_new_instances(self, mock_connect_to_region, mock_create_instance):
        num_instances_to_create = 5
        instancesCreated = { "InstanceId": "5f9adeaa-c94c-42c6-aeef-28a5376002cd" }

        mock_connect_to_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_create_instance.return_value = instancesCreated

        resp = video_processor.autoscaler.start_new_instances(num_instances_to_create)
        self.assertEqual(resp, num_instances_to_create)

    @patch('boto.opsworks.layer1.OpsWorksConnection.delete_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.stop_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_terminate_instances(self, mock_connect_to_region, mock_describe_instances, mock_stop_instance, mock_delete_instance):
        num_instances_to_terminate = 3
        instancesDict = { "Instances": [ 
            {"Status":"online", "InstanceId":"0"},
            {"Status":"online", "InstanceId":"1"},
            {"Status":"online", "InstanceId":"2"},
            {"Status":"terminated", "InstanceId":"3"}
        ] }
        instancesStoppedDict = { "Instances": [ 
            {"Status":"stopped", "InstanceId":"0"}
        ] }

        mock_connect_to_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_describe_instances.side_effect = [instancesDict, instancesStoppedDict, instancesDict, instancesStoppedDict, instancesDict, instancesStoppedDict, instancesDict]
        mock_stop_instance.return_value = None
        mock_delete_instance.return_value = None

        resp = video_processor.autoscaler.terminate_instances(num_instances_to_terminate)
        self.assertEqual(resp, num_instances_to_terminate)

    @patch('boto.opsworks.layer1.OpsWorksConnection.delete_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.stop_instance')
    @patch('boto.opsworks.layer1.OpsWorksConnection.describe_instances')
    @patch('boto.opsworks.connect_to_region')
    def test_terminate_instances_exits_if_instances_are_killed_outside(self, mock_connect_to_region, mock_describe_instances, mock_stop_instance, mock_delete_instance):
        num_instances_to_terminate = 3
        instancesDict = { "Instances": [ 
            {"Status":"online", "InstanceId":"0"},
            {"Status":"online", "InstanceId":"1"},
            {"Status":"online", "InstanceId":"2"},
            {"Status":"terminated", "InstanceId":"3"}
        ] }
        instancesStoppedDict = { "Instances": [ 
            {"Status":"stopped", "InstanceId":"0"} 
        ] }
        instancesTerminatedDict = { "Instances": [ 
            {"Status":"terminated", "InstanceId":"0"},
            {"Status":"terminated", "InstanceId":"1"},
            {"Status":"terminated", "InstanceId":"2"}
        ] }

        mock_connect_to_region.return_value = boto.opsworks.layer1.OpsWorksConnection()
        mock_describe_instances.side_effect = [instancesDict, instancesStoppedDict, instancesTerminatedDict]
        mock_stop_instance.return_value = None
        mock_delete_instance.return_value = None

        # terminate_instances returns the number of instances it has tried to terminate.
        # That number does not include instances terminated from outside of the script.
        resp = video_processor.autoscaler.terminate_instances(num_instances_to_terminate)
        self.assertEqual(resp, 1)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()