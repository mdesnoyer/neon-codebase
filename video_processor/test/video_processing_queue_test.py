#!/usr/bin/env python

'''
SQS unit test

#TODO:
    Identify all the errors cases and inject them to be tested
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)

from boto.sqs.message import Message
from boto.s3.connection import S3Connection
import logging
from mock import MagicMock, patch
import time
import test_utils
import test_utils.neontest
from test_utils import sqsmock
import tornado
from tornado import ioloop
from tornado.testing import AsyncTestCase
import unittest
import utils.neon
import video_processor.video_processing_queue

_log = logging.getLogger(__name__)

class TestVideoProcessingQueue(test_utils.neontest.AsyncTestCase):
    '''Used to test the SQS queue'''

    def setUp(self):
        super(TestVideoProcessingQueue, self).setUp()
        
        self.video_queue_region = 'us-east-1'
        self.num_queues = 3

        self.mock_sqs = sqsmock.SQSConnectionMock()

        self.sqs_patcher = patch('video_processor.video_processing_queue.boto.sqs.' \
                                 'connect_to_region')

        self.sqs_patcher.start().return_value = self.mock_sqs

        self.q = video_processor.video_processing_queue.VideoProcessingQueue()

    def tearDown(self):
        self.sqs_patcher.stop()
        super(TestVideoProcessingQueue, self).tearDown()
     
    @tornado.testing.gen_test
    def test_full_class(self):

        for i in range(1, 11):
            priority = i % self.num_queues

            message_body = yield self.q.write_message(priority, str(i),
                                                   i*12.3)
            self.assertTrue(message_body)

        size = yield self.q.size()
        self.assertEquals(size, 10)

        count = 10
        while count > 0:
            mes = yield self.q.read_message()

            if mes != None:
                # Look for the message attributes
                self.assertLess(int(
                    mes.message_attributes['priority']['string_value']),
                    self.num_queues)
                self.assertGreater(float(
                    mes.message_attributes['duration']['string_value']),
                    0.0)
                
                deleted = yield self.q.delete_message(mes)
                self.assertTrue(deleted)
                count = count - 1

        size = yield self.q.size()
        self.assertEquals(size, 0)

    @tornado.testing.gen_test
    def test_invalid_priority(self):

        with self.assertRaises(ValueError) as cm:
            message_body = yield self.q.write_message(5, 'Test')

    @tornado.testing.gen_test
    def test_read_from_empty_server(self):
        
        message = yield self.q.read_message()
        self.assertIsNone(message)

    @tornado.testing.gen_test
    def test_invalid_message_body(self):
        
        test_tuple = (0, 'test')
        test_dict = [0,3,2]

        with self.assertRaises(ValueError) as cm:
            yield self.q.write_message(0, 13)

        with self.assertRaises(ValueError) as cm:
            yield self.q.write_message(0, test_tuple)

        with self.assertRaises(ValueError) as cm:
            yield self.q.write_message(0, test_dict)

    @tornado.testing.gen_test
    def test_no_duration(self):        
        message = yield self.q.write_message(0, "test", None)

        self.assertEqual(message, "test")

        rmes = yield self.q.read_message()

        self.assertIsNone(self.q.get_duration(rmes))

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
    tornado.ioloop.IOLoop.current().start()
