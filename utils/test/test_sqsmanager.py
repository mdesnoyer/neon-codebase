#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import threading
import time
import Queue
from StringIO import StringIO
import tornado
import unittest
import utils.sqsmanager

from mock import patch
from mock import MagicMock
from test_utils import sqsmock

class TestSQSCallbackManager(unittest.TestCase):

    def setUp(self):
        
        request = tornado.httpclient.HTTPRequest("http://neon-lab.com")
        self.mock_response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=StringIO(''))
        # Reduce the delay so that time thread executes almost immediately
        utils.sqsmanager.CustomerCallbackManager.delay = 0

    def tearDown(self):
        pass
    
    @patch('utils.sqsmanager.boto.connect_sqs')
    def test_add_callback_response(self, mock_conn):
        mock_conn.return_value = sqsmock.SQSConnectionMock()
        vid = 'v1'
        cb = 'http://callback'
        resp = '{"vid": 1}'
        manager = utils.sqsmanager.CustomerCallbackManager()
        manager.add_callback_response(vid, cb, resp) 
        msgs = manager.get_callback_messages()
        self.assertEqual(len(msgs), 1)

    @patch('utils.http')
    @patch('utils.sqsmanager.boto.connect_sqs')
    def test_send_callback_response(self, mock_conn, mock_http):
        mock_http.send_request.return_value = self.mock_response
        mock_conn.return_value = sqsmock.SQSConnectionMock()
        vid = 'v1'
        cb = 'http://callback'
        resp = '{"vid": 1}'
        manager = utils.sqsmanager.CustomerCallbackManager()
        manager.add_callback_response(vid, cb, resp) 
        manager.get_callback_messages() # populate callback messages locally
        manager.send_callback_response(vid)
        while manager.n_callbacks > 0:
            time.sleep(0.1)
         
        self.assertEqual(len(mock_http._mock_mock_calls), 1)
        self.assertEqual(manager.sq.count(), 0)
        self.assertEqual(len(manager.callback_messages), 0)

    @patch('utils.http')
    @patch('utils.sqsmanager.boto.connect_sqs')
    def test_schedule_all_callbacks(self, mock_conn, mock_http):
        mock_http.send_request.return_value = self.mock_response
        mock_conn.return_value = sqsmock.SQSConnectionMock()
        cb = 'http://callback'
        resp = '{"vid": 1}'
        manager = utils.sqsmanager.CustomerCallbackManager()
        manager.add_callback_response('v1', cb, resp) 
        manager.add_callback_response('v2', cb, resp) 
        manager.schedule_all_callbacks(['v1', 'v2', 'v3'])
        while manager.n_callbacks > 0:
            # May be change to a wait with timeout, what if this loops forever ? 
            time.sleep(0.1)
        self.assertEqual(len(mock_http._mock_mock_calls), 2)
        self.assertEqual(len(manager.messages), 0)
        self.assertEqual(manager.sq.count(), 0) 
        self.assertEqual(len(manager.get_callback_messages()), 0)

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
