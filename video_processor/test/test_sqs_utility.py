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
import logging
from mock import MagicMock, patch
import time
import test_utils
import test_utils.neontest
from test_utils import sqsmock
import tornado
#from tornado import ioloop
import unittest
import utils.neon
import video_processor

_log = logging.getLogger(__name__)

# ======== Parameters  =======================#
from utils.options import define, options
define('region', default='us-east-1', help='Region of the SQS queue to connect to')
define('aws_key', default='AKIAIG2UEH2FF6WSRXDA', help='Key to connect to AWS')
define('secret_key', default='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', 
       help='Secret key to connect to AWS')

class TestVideoProcessingQueue(test_utils.neontest.TestCase):
    '''Used to test the SQS queue'''

    def setUp(self):
        super(TestVideoProcessingQueue, self).setUp()

        self.mock_sqs = sqsmock.SQSConnectionMock()
        self.mock_queue = sqsmock.SQSQueueMock('Priority_0', True)

        self.sqs_patcher = patch('video_processor.sqs_utilities.boto.sqs.' \
                                 'connect_to_region')
        #self.sqs_patcher.start().return_value = self.mock_sqs

        self.mock_sqs_future = self._future_wrap_mock(self.sqs_patcher.start(),
                                                      require_async_kw=True)
       
        self.mock_sqs_future.return_value = self.mock_sqs

        self.queue_patcher = patch('video_processor.sqs_utilities.' \
                                    'VideoProcessingQueue._create_queue')
        #self.queue_patcher.start().return_value = self.mock_queue

        self.mock_queue_future = self._future_wrap_mock(
            self.queue_patcher.start(),
            require_async_kw=True)

        self.mock_queue_future.return_value = self.mock_queue

        serv = video_processor.sqs_utilities
        self.sqs = serv.VideoProcessingQueue(options.region,
                                             options.aws_key,
                                             options.secret_key)
        self.m = None
        self.num_queues = 3

    def tearDown(self):
        self.sqs_patcher.stop()
        self.queue_patcher.stop()
        super(TestVideoProcessingQueue, self).tearDown()
     
    @tornado.gen.coroutine
    def test_full_class(self):
        _log.info("Beginning sqs test")
        for i in range(1, 101):
            self.m = Message()
            self.m.set_body(str(i))
            message = yield self.sqs.write_message(i%self.num_queues, self.m)
            _log.info("message written")
            self.assertTrue(mes)

        count = 100
        start_time = time.time() 
        while count > 0:
            mes = yield self.sqs.read_message()
            _log.info("message read")
            # See if we timeout
            self.assertLess(time.time() - start_time, 5, 'Timed out')

            if mes != None:
                mbody = mes.get_body()
                deleted = yield self.sqs.delete_message(mes)
                _log.info("message deleted")
                self.assertTrue(deleted)
                count = count - 1

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
    io_loop = tornado.ioloop.IOLoop.instance()
    io_loop.start()
