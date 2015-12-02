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

        '''self.mock_sqs = sqsmock.SQSConnectionMock()

        self.sqs_patcher = patch('video_processor.video_processing_queue.boto.sqs.' \
                                 'connect_to_region')

        self.mock_sqs_future = self._future_wrap_mock(
                                         self.sqs_patcher.start(),
                                         require_async_kw=True)
       
        self.mock_sqs_future.return_value = self.mock_sqs'''

    def tearDown(self):
        #self.sqs_patcher.stop()
        super(TestVideoProcessingQueue, self).tearDown()
     
    @tornado.testing.gen_test(timeout=10)
    def test_full_class(self):
        sqs = video_processor.video_processing_queue.VideoProcessingQueue()

        yield sqs.connect_to_server(self.video_queue_region)

        for i in range(1, 11):
            priority = i % self.num_queues

            message_body = yield sqs.write_message(priority, str(i))
            self.assertTrue(message_body)

        count = 10
        while count > 0:
            mes = yield sqs.read_message()

            if mes != None:
                deleted = yield sqs.delete_message(mes)
                self.assertTrue(deleted)
                count = count - 1

    @tornado.testing.gen_test
    def test_invalid_priority(self):
        sqs = video_processor.video_processing_queue.VideoProcessingQueue()

        yield sqs.connect_to_server(self.video_queue_region)

        with self.assertRaises(ValueError) as cm:
            message_body = yield sqs.write_message(5, 'Test')

    @tornado.testing.gen_test
    def test_read_from_empty_server(self):
        sqs = video_processor.video_processing_queue.VideoProcessingQueue()

        yield sqs.connect_to_server(self.video_queue_region)
        
        message = yield sqs.read_message()
        self.assertIsNone(message)

    @tornado.testing.gen_test
    def test_delete_message_twice(self):
        sqs = video_processor.video_processing_queue.VideoProcessingQueue()

        yield sqs.connect_to_server(self.video_queue_region)

        message = Message()
        yield sqs.write_message(0, 'test')
        mes = None
        while not mes:
            mes = yield sqs.read_message()

        deleted = yield sqs.delete_message(mes)
        self.assertTrue(deleted)
        deleted = yield sqs.delete_message(mes)
        self.assertFalse(deleted)

    @tornado.testing.gen_test
    def test_invalid_message_body(self):
        sqs = video_processor.video_processing_queue.VideoProcessingQueue()

        yield sqs.connect_to_server(self.video_queue_region)
        
        test_tuple = (0, 'test')
        test_dict = [0,3,2]

        with self.assertRaises(ValueError) as cm:
            yield sqs.write_message(0, 13)

        with self.assertRaises(ValueError) as cm:
            yield sqs.write_message(0, test_tuple)

        with self.assertRaises(ValueError) as cm:
            yield sqs.write_message(0, test_dict)

    @tornado.testing.gen_test
    def test_no_duration(self):
        sqs = video_processor.video_processing_queue.VideoProcessingQueue()

        yield sqs.connect_to_server(self.video_queue_region)
        
        message = yield sqs.write_message(0, "test", None)

        self.assertEqual(message, "test")

    @tornado.testing.gen_test
    def test_connect_to_bad_region(self):
        sqs = video_processor.video_processing_queue.VideoProcessingQueue()

        with self.assertRaises(AttributeError) as cm:
            yield sqs.connect_to_server("fake_region")

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
    tornado.ioloop.IOLoop.current().start()
