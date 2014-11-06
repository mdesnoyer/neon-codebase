#!/usr/bin/env python
'''
Unittests for the utils.boto module

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
from mock import patch, MagicMock
import test_utils.mock_boto_s3 as boto_mock
import test_utils.neontest
import tornado.testing
from utils.boto import run_async
import utils.neon

_log = logging.getLogger(__name__)

class TestAsyncCalls(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestAsyncCalls, self).setUp()
        self.conn = boto_mock.MockConnection()
        
        self.async_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')

        self.mock_client = self.async_patcher.start()
        self.mock_responses = MagicMock()                              

        self.mock_client().fetch.side_effect = \
          lambda x, callback: self.io_loop.add_callback(callback,
                                                        self.mock_responses(x))


    def tearDown(self):
        self.async_patcher.stop()
        super(TestAsyncCalls, self).tearDown()

    @tornado.testing.gen_test
    def test_async_s3_calls(self):
        bucket = yield run_async(self.conn.create_bucket, 'my-bucket')

        yield [run_async(bucket.new_key('key1').set_contents_from_string,
                         'my key1'),
               run_async(bucket.new_key('key2').set_contents_from_string,
                         'my key2')]

        key1, key2, key3 = yield [run_async(bucket.get_key, 'key1'),
                                  run_async(bucket.get_key, 'key2'),
                                  run_async(bucket.get_key, 'key3')]
        self.assertIsNone(key3)
        self.assertIsNotNone(key1)
        self.assertIsNotNone(key2)
        self.assertEquals(key1.get_contents_as_string(), 'my key1')
        self.assertEquals(key1.get_contents_as_string(), 'my key2')

        
    
if __name__ == '__main__':
    utils.neon.InitNeon()
    test_utils.neontest.main()
