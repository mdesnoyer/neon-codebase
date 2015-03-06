#!/usr/bin/env python

'''
Unit test for Autoscaler 
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> __base_path__:
        sys.path.insert(0, __base_path__)

import logging
import json
from mock import patch, MagicMock
import os
import subprocess
import test_utils.neontest
import time
import unittest
import utils.neon
from utils.options import define, options
import video_processor.autoscaler

_log = logging.getLogger(__name__)

class TestAutoScaler(unittest.TestCase):
    def setUp(self):
        super(TestAutoScaler, self).setUp()
    
    def tearDown(self):
        super(TestAutoScaler, self).tearDown()

    @patch('video_processor.autoscaler.urllib2')
    def test_get_qsize(self, mock_urlopen):
        mock_urlopen.urlopen.return_value = '{"size": 10, "bytes": 20}'
        resp = video_processor.autoscaler.get_queue_size()
        self.assertEqual(resp, 10)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
