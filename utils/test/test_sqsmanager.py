#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.exception
import logging
import Queue
import socket
from StringIO import StringIO
import test_utils.neontest
import threading
import time
import tornado
import tornado.testing
import unittest
import utils.sqsmanager
from utils.sqsmanager import CustomerCallbackMessage

from mock import patch
from mock import MagicMock
from test_utils import sqsmock

class TestSQSCallbackManager(test_utils.neontest.AsyncTestCase):

    def setUp(self):
        super(TestSQSCallbackManager, self).setUp()
        request = tornado.httpclient.HTTPRequest("http://neon-lab.com")
        self.valid_response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=StringIO(''))
        self.invalid_response = tornado.httpclient.HTTPResponse(request, 404,
                            buffer=StringIO(''))

        # Mock out the http call
        self.http_patcher = patch('utils.sqsmanager.utils.http')
        self.mock_http = self.http_patcher.start()
        self.mock_http_response = MagicMock()
        self.mock_http_response.return_value = self.valid_response
        self.mock_http.send_request.side_effect = \
          lambda x, callback: callback(self.mock_http_response())

        # Mock out sqs
        self.sqsmock = sqsmock.SQSConnectionMock()
        self.sqs_patcher = patch('utils.sqsmanager.boto.connect_sqs')
        self.sqs_patcher.start().return_value = self.sqsmock

        self.manager = utils.sqsmanager.CustomerCallbackManager()

    def tearDown(self):
        self.sqs_patcher.stop()
        self.http_patcher.stop()
        super(TestSQSCallbackManager, self).tearDown()        
    
    @tornado.testing.gen_test
    def test_add_callback_response(self):
        self.assertTrue(
            self.manager.add_callback_response('v1', 'http://callback',
                                               '{"vid": 1}'))
        self.assertTrue(
            self.manager.add_callback_response('v2', 'http://callback2',
                                               '{"vid": 2}'))

        self.assertEqual(self.manager.sq.count(), 2)
        
        msgs = yield self.manager.get_callback_messages()
        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs, [
            CustomerCallbackMessage('v1', 'http://callback', '{"vid": 1}'),
            CustomerCallbackMessage('v2', 'http://callback2', '{"vid": 2}')])

    @tornado.testing.gen_test
    def test_add_invalid_char_message(self):
        self.assertTrue(
            self.manager.add_callback_response('v1', 'http://callback',
                                               u'\xDAAA' + 'pop'))

        self.assertEqual(self.manager.sq.count(), 1)

        msgs = yield self.manager.get_callback_messages()
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs, [
            CustomerCallbackMessage('v1', 'http://callback',
                                    u'\xDAAA' + 'pop')])

    @tornado.testing.gen_test
    def test_error_adding_callback_response(self):
        self.manager.sq.write = MagicMock()
        self.manager.sq.write.side_effect = [
            boto.exception.SQSError(404, 'because')]

        with self.assertLogExists(logging.ERROR,
                                  'failed to write to SQS'):
            self.assertFalse(
                self.manager.add_callback_response('v1', 'http://callback',
                                                   '{"vid": 1}'))

    @tornado.testing.gen_test
    def test_sqs_connection_error_adding_callback(self):
        self.manager.sq.write = MagicMock()
        self.manager.sq.write.side_effect = [
            boto.exception.BotoServerError(500, 'because')]

        with self.assertLogExists(logging.ERROR,
                                  'failed to write to SQS'):
            self.assertFalse(
                self.manager.add_callback_response('v1', 'http://callback',
                                                   '{"vid": 1}'))
        

    @tornado.testing.gen_test
    def test_send_callback_response(self):
        self.assertTrue(
            self.manager.add_callback_response('v1', 'http://callback',
                                               '{"vid": 1}'))
        # populate callback messages locally
        yield self.manager.get_callback_messages()
        self.assertEqual(self.manager.sq.count(), 1)

        yield self.manager.send_callback_response('v1')

        self.assertEqual(self.manager.sq.count(), 0)
        self.assertEqual(self.mock_http.send_request.call_count, 1)
        request, kwargs = self.mock_http.send_request.call_args
        self.assertEqual(request[0].url, 'http://callback')
        self.assertEqual(request[0].body, '{"vid": 1}')
        self.assertEqual(len(self.manager.callback_messages), 0)

    @tornado.testing.gen_test
    def test_cannot_decode_message_in_sqs(self):
        self.manager.sq.get_messages = MagicMock()
        self.manager.sq.get_messages.side_effect = [
            boto.exception.SQSDecodeError(404, 'dummy')]

        self.assertTrue(
            self.manager.add_callback_response('v1', 'http://callback',
                                               '{"vid": 1}'))

        with self.assertLogExists(logging.ERROR,
                                  'Unable to decode sqs message'):
            yield self.manager.get_callback_messages()

    @tornado.testing.gen_test
    def test_bad_message_in_sqs(self):
        msg = boto.sqs.message.Message()
        msg.set_body('Some non-json message')
        self.manager.sq.write(msg)
        self.assertEquals(self.manager.sq.count(), 1)

        with self.assertLogExists(logging.ERROR,
                                  'Bad message in SQS. removing'):
            self.manager.schedule_all_callbacks(['v1', 'v2'])

        self.assertEquals(self.manager.sq.count(), 0)
        self.assertEquals(self.mock_http.send_request.call_count, 0)

    @tornado.testing.gen_test
    def test_schedule_all_callbacks(self):
        self.manager.add_callback_response('v1', 'http://callback1',
                                           '{"vid": 1}') 
        self.manager.add_callback_response('v2', 'http://callback2',
                                           '{"vid": 2}') 
        self.manager.schedule_all_callbacks(set(['v1', 'v2', 'v3']))
        self.assertEqual(self.mock_http.send_request.call_count, 2)
        requests = self.mock_http.send_request.call_args_list
        self.assertEqual(requests[0][0][0].url, 'http://callback1')
        self.assertEqual(requests[0][0][0].body, '{"vid": 1}')
        self.assertEqual(requests[1][0][0].url, 'http://callback2')
        self.assertEqual(requests[1][0][0].body, '{"vid": 2}')
        self.assertEqual(len(self.manager.messages), 0)
        self.assertEqual(self.manager.sq.count(), 0)
        msgs = yield self.manager.get_callback_messages()
        self.assertEqual(len(msgs), 0)

    def test_error_on_callback(self):
        self.mock_http_response.side_effect = [
            self.invalid_response
            ]
        self.manager.add_callback_response('v1', 'http://callback1',
                                           '{"vid": 1}') 
        
        with self.assertLogExists(logging.ERROR,
                                  'Error sending callback for key v1'):
            self.manager.schedule_all_callbacks(['v1'])

        self.assertEqual(self.manager.sq.count(), 1)

    def _test_send_callback_response2(self):
        '''
        Test SQS directly, helpful to debug the mock library
        Keeping this around for future debugging

        '''
        vid = 'v1'
        cb = 'http://neon-lab.com'
        resp = '{"vid": 1}'
        manager = utils.sqsmanager.CustomerCallbackManager()
        #clear the Q
        manager.sq.clear()
        manager.add_callback_response(vid, cb, resp) 
        manager.get_callback_messages() # populate callback messages locally
        manager.send_callback_response(vid)
        while manager.n_callbacks > 0:
            time.sleep(0.1)
        self.assertEqual(manager.sq.count(), 0)
        self.assertEqual(len(manager.callback_messages), 0)
        self.assertEqual(len(manager.get_callback_messages()), 0)
        self.assertEqual(len(manager.messages), 0)

if __name__ == '__main__':
    unittest.main()
