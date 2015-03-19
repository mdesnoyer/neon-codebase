#!/usr/bin/env python
'''
Unittests for the http module

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')))

import logging
from mock import patch, MagicMock
import Queue
import random
import socket
from StringIO import StringIO
import test_utils.neontest
from tornado.concurrent import Future
import tornado.gen
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import unittest
import utils.http
import utils.neon

_log = logging.getLogger(__name__)

def create_valid_ack():
    request = HTTPRequest('http://www.neon.com')
    response = HTTPResponse(request, 200,
                            buffer=StringIO('{"error":nil}'))
    return request, response

class TestSyncSendRequest(test_utils.neontest.TestCase):
    def setUp(self):
        self.sync_patcher = \
          patch('utils.http.tornado.httpclient.HTTPClient')

        self.mock_client = self.sync_patcher.start()
        logging.getLogger('utils.http').reset_sample_counters()

    def tearDown(self):
        self.sync_patcher.stop()

    def test_valid_sync_request(self):
        request, response = create_valid_ack()
        self.mock_client().fetch.side_effect = [response]

        self.assertEqual(utils.http.send_request(request), response)

    def test_json_error_field(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING, 'key=http_response_error'):
            found_response = utils.http.send_request(request, 3)

        self.assertEqual(found_response, valid_response)

    def test_json_error_out(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            invalid_response
            ]

        with self.assertLogExists(logging.WARNING, 'key=http_response_error'):
            found_response = utils.http.send_request(request, 3)

        self.assertRegexpMatches(found_response.error.message, '.*600')

    def test_connection_errors(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(500) 
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'key=http_connection_error msg=.*500'):
            found_response = utils.http.send_request(request, 3)

        self.assertEqual(found_response, valid_response)

    def test_socket_errors(self):
        request, valid_response = create_valid_ack()
        self.mock_client().fetch.side_effect = [
            socket.error(),
            socket.error(),
            valid_response
            ]

        with self.assertLogExists(logging.ERROR,
                                  'socket resolution error'):
            found_response = utils.http.send_request(request, 3)

        self.assertEqual(found_response, valid_response)

    def test_no_logging(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(500) 
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogNotExists(logging.WARNING,
                                  'key=http_connection_error msg=.*500'):
            found_response = utils.http.send_request(request, 3,
                                                     do_logging=False)

        self.assertEqual(found_response, valid_response)

    def test_not_raised_connection_errors(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'key=http_connection_error msg=.*500'):
            found_response = utils.http.send_request(request, 3)

        self.assertEqual(found_response, valid_response)

    def test_too_many_retries(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]
        
        found_response = utils.http.send_request(request, 2)

        self.assertEqual(found_response.error.code, 500)
        self.assertRegexpMatches(str(found_response.error),
                                 'Internal Server Error')

class TestAsyncSendRequest(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestAsyncSendRequest, self).setUp()
        self.async_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')

        self.mock_client = self.async_patcher.start()
        self.mock_responses = MagicMock()                              

        self.mock_client().fetch.side_effect = \
          lambda x, callback: self.io_loop.add_callback(callback,
                                                        self.mock_responses(x))


    def tearDown(self):
        self.async_patcher.stop()
        super(TestAsyncSendRequest, self).tearDown()

    def test_valid_async_request(self):
        request, response = create_valid_ack()
        self.mock_responses.side_effect = [response]

        utils.http.send_request(request, callback=self.stop)
        
        found_response = self.wait()

        self.assertEqual(found_response, response)

    @tornado.testing.gen_test
    def test_yielding(self):
        request, response = create_valid_ack()
        self.mock_responses.side_effect = [response]

        found_response = yield tornado.gen.Task(utils.http.send_request,
                                                request)

        self.assertEqual(found_response, response)

    def test_async_json_error_field(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_responses.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING, 'key=http_response_error'):
            utils.http.send_request(request, 3, callback=self.stop)
            found_response = self.wait()
            self.assertEqual(found_response, valid_response)

    @unittest.skip('Cannot mock out exceptions properly in async mode. '
                   'Should switch to a pure tornado solution instead of '
                   'handling the callbacks manually')
    @tornado.testing.gen_test
    def test_async_socket_errors(self):
        request, valid_response = create_valid_ack()
        self.mock_responses.side_effect = [
            socket.error(),
            socket.error(),
            valid_response
            ]

        with self.assertLogExists(logging.ERROR,
                                  'socket resolution error'):
            found_response = yield tornado.gen.Task(
                utils.http.send_request,
                HTTPRequest('http://sdjfaoei.com'),
                3)
            self.assertEqual(found_response, valid_response)

    def test_too_many_retries(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_responses.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]
        
        utils.http.send_request(request, 2, callback=self.stop)
        found_response = self.wait()

        self.assertEqual(found_response.error.code, 500)
        self.assertRegexpMatches(str(found_response.error),
                                 'Internal Server Error')

class TestRequestPool(test_utils.neontest.TestCase):
    def setUp(self):
        self.pool = utils.http.RequestPool(5, 3)
        self.patcher = \
          patch('utils.http.tornado.httpclient.HTTPClient')
        self.mock_client = self.patcher.start()

        self.response_q = Queue.Queue()
        logging.getLogger('utils.http').reset_sample_counters()
        
    def tearDown(self):
        self.pool.stop()
        self.patcher.stop()

    def test_single_blocking_request(self):
        request, response = create_valid_ack()
        self.mock_client().fetch.side_effect = [response]

        got_response = self.pool.send_request(request)

        self.assertEqual(got_response, response)

    def test_single_async_request(self):
        request, response = create_valid_ack()
        self.mock_client().fetch.side_effect = [response]

        self.pool.send_request(request, lambda x: self.response_q.put(x))

        self.pool.join()

        self.assertEqual(self.response_q.get_nowait(), response)

    def test_json_error_field(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING, 'key=http_response_error'):
            self.pool.send_request(request, lambda x: self.response_q.put(x))
            self.pool.join()

        self.assertEqual(self.response_q.get_nowait(), valid_response)

    def test_too_many_retries(self):
        request = HTTPRequest('http://www.neon.com', body='Lalalalal')
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            invalid_response,
            ]

        with self.assertLogExists(logging.ERROR, 'key=http_too_many_errors'):
            self.pool.send_request(request, lambda x: self.response_q.put(x))
            self.pool.join()

        found_response = self.response_q.get_nowait()
        self.assertEqual(found_response.error.code, 500)
        self.assertEqual(found_response, invalid_response)
        self.assertTrue(self.response_q.empty())

    def test_no_logging(self):
        request = HTTPRequest('http://www.neon.com', body='Lalalalal')
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            invalid_response,
            ]

        with self.assertLogNotExists(logging.ERROR,'key=http_too_many_errors'):
            self.pool.send_request(request, lambda x: self.response_q.put(x),
                                   do_logging=False)
            self.pool.join()

        found_response = self.response_q.get_nowait()
        self.assertEqual(found_response.error.code, 500)
        self.assertEqual(found_response, invalid_response)
        self.assertTrue(self.response_q.empty())

    def test_599_error(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(599)
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'key=http_connection_error msg=.*599'):
            self.pool.send_request(request, lambda x: self.response_q.put(x))
            self.pool.join()

        self.assertEqual(self.response_q.get_nowait(), valid_response)

    def test_500_error_not_raised(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'key=http_connection_error msg=.*500'):
            self.pool.send_request(request,
                                   lambda x: self.response_q.put(x))
            self.pool.join()

        self.assertEqual(self.response_q.get_nowait(), valid_response)

    def test_no_json_in_body(self):
        request = HTTPRequest('http://www.neon.com')
        response = HTTPResponse(request, 200,
                                buffer=StringIO('hello'))
        self.mock_client().fetch.side_effect = [response]

        got_response = self.pool.send_request(request)

        self.assertEqual(got_response, response)

    def test_different_json_in_body(self):
        request = HTTPRequest('http://www.neon.com')
        response = HTTPResponse(request, 200,
                                buffer=StringIO('{"hello": 3}'))
        self.mock_client().fetch.side_effect = [response]

        got_response = self.pool.send_request(request)

        self.assertEqual(got_response, response)

    def test_lots_of_requests(self):
        random.seed(1659843)
        self.pool = utils.http.RequestPool(5, 16)
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(599)

        n_queries = 200
        n_errors = 15
        responses = [valid_response for i in range(n_queries)]
        responses.extend([invalid_response for i in range(n_errors)])
        random.shuffle(responses)
        
        self.mock_client().fetch.side_effect = responses

        for i in range(n_queries):
            self.pool.send_request(request, lambda x: self.response_q.put(x))

        self.pool.join()

        n_responses = 0
        while not self.response_q.empty():
            self.assertEqual(self.response_q.get_nowait(), valid_response) 
            n_responses += 1

        self.assertEqual(n_responses, n_queries)

class TestTornadoAsyncRequestPool(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestTornadoAsyncRequestPool, self).setUp()
        self.pool = utils.http.RequestPool(5, 3)
        self.patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.mock_client = self.patcher.start()

        self.mock_responses = MagicMock()

        def _response(x, callback=None):
            if callback:
                self.io_loop.add_callback(callback,
                                          self.mock_responses(x))
            else:
                return self.mock_responses(x)

        self.mock_client().fetch.side_effect = _response
        
    def tearDown(self):
        self.pool.stop()
        self.patcher.stop()
        super(TestTornadoAsyncRequestPool, self).tearDown()

    @tornado.testing.gen_test
    def test_yielding(self):
        request, response = create_valid_ack()
        self.mock_responses.side_effect = [response]

        found_response = yield tornado.gen.Task(self.pool.send_request,
                                                request)

        self.assertEqual(found_response, response)
        

        
   
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
