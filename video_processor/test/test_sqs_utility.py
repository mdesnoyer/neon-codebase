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
import video_processor.sqs_utilities

_log = logging.getLogger(__name__)

# ======== Parameters  =======================#
from utils.options import define, options
define('region', default='us-east-1', help='Region of the SQS queue to connect to')
define('aws_key', default='AKIAIG2UEH2FF6WSRXDA', help='Key to connect to AWS')
define('secret_key', default='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', 
       help='Secret key to connect to AWS')
define('num_queues', default=3, help='Number of queues in the SQS server')

class TestVideoProcessingQueue(test_utils.neontest.AsyncTestCase):
    '''Used to test the SQS queue'''

    def setUp(self):
        super(TestVideoProcessingQueue, self).setUp()
        
        self.mock_sqs = sqsmock.SQSConnectionMock()
        self.mock_queue = sqsmock.SQSQueueMock()

        self.sqs_patcher = patch('video_processor.sqs_utilities.boto.sqs.' \
                                 'connect_to_region')

        self.mock_sqs_future = self._future_wrap_mock(
                                         self.sqs_patcher.start(),
                                         require_async_kw=True)
       
        self.mock_sqs_future.return_value = self.mock_sqs

        self.read_patcher = patch('video_processor.sqs_utilities.boto.sqs.' \
                                    'queue.Queue.read')

        self.mock_read_future = self._future_wrap_mock(
            self.read_patcher.start(),
            require_async_kw=True)

        self.mock_read_future.side_effect = self.mock_queue.read


        self.write_patcher = patch('video_processor.sqs_utilities.boto.sqs.' \
                                    'queue.Queue.write')

        self.mock_write_future = self._future_wrap_mock(
            self.write_patcher.start(),
            require_async_kw=True)

        self.mock_write_future.side_effect = self.mock_queue.write

        self.delete_patcher = patch('video_processor.sqs_utilities.boto.sqs.' \
                                    'queue.Queue.delete_message')

        self.mock_delete_future = self._future_wrap_mock(
            self.delete_patcher.start(),
            require_async_kw=True)

        self.mock_delete_future.side_effect = self.mock_queue.delete_message

    def tearDown(self):
        self.sqs_patcher.stop()
        self.read_patcher.stop()
        self.write_patcher.stop()
        self.delete_patcher.stop()
        super(TestVideoProcessingQueue, self).tearDown()
     
    @tornado.testing.gen_test(timeout=5)
    def test_full_class(self):
        _log.info("Beginning full sqs test")
        serv = video_processor.sqs_utilities
        self.sqs = serv.VideoProcessingQueue()

        yield self.sqs.connect_to_server(options.region,
                                         options.aws_key,
                                         options.secret_key)

        for i in range(1, 101):
            priority = i%options.num_queues

            message_body = yield self.sqs.write_message(priority, str(i))
            _log.info("message written")
            self.assertTrue(message_body)

        count = 100
        while count > 0:
            mes = yield self.sqs.read_message()
            _log.info("message read")

            if mes != None:
                deleted = yield self.sqs.delete_message(mes)
                _log.info("message deleted")
                self.assertTrue(deleted)
                count = count - 1

    @tornado.testing.gen_test(timeout=5)
    def test_invalid_priority(self):
        _log.info("Writing garbage")
        serv = video_processor.sqs_utilities
        self.sqs = serv.VideoProcessingQueue()

        yield self.sqs.connect_to_server(options.region,
                                         options.aws_key,
                                         options.secret_key)

        message_body = yield self.sqs.write_message(3, 'Test')

    @tornado.testing.gen_test(timeout=5)
    def test_read_from_empty_server(self):
        _log.info("Reading empty server")
        serv = video_processor.sqs_utilities
        self.sqs = serv.VideoProcessingQueue()

        yield self.sqs.connect_to_server(options.region,
                                         options.aws_key,
                                         options.secret_key)
        
        message = yield self.sqs.read_message()
        self.assertIsNone(message)

    @tornado.testing.gen_test(timeout=5)
    def test_delete_from_empty_server(self):
        serv = video_processor.sqs_utilities
        _log.info("Reading empty server")
        self.sqs = serv.VideoProcessingQueue()

        yield self.sqs.connect_to_server(options.region,
                                         options.aws_key,
                                         options.secret_key)

        message = Message()
        deleted = yield self.sqs.delete_message(message)
        self.assertFalse(message)


if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
    tornado.ioloop.IOLoop.current().start()
