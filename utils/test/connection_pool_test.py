#!/usr/bin/env python
'''
Unittests for the connection pool module

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
from StringIO import StringIO
import test_utils.neontest
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import unittest
import utils.connection_pool

class TestCommandLineParsing(test_utils.neontest.TestCase):
    def setUp(self):
        self.pool = utils.connection_pool.HttpConnectionPool(5, 3)
        self.patcher = \
          patch('utils.connection_pool.tornado.httpclient.HTTPClient')
        self.mock_client = self.patcher.start()

        self.response_q = Queue.Queue()
        
    def tearDown(self):
        self.pool.stop()
        self.patcher.stop()

    def create_valid_ack(self):
        request = HTTPRequest('http://www.neon.com')
        response = HTTPResponse(request, 200,
                                buffer=StringIO('{"error":nil}'))
        return request, response

    def test_single_blocking_request(self):
        request, response = self.create_valid_ack()
        self.mock_client().fetch.side_effect = [response]

        got_response = self.pool.send_request(request)

        self.assertEqual(got_response, response)

    def test_single_async_request(self):
        request, response = self.create_valid_ack()
        self.mock_client().fetch.side_effect = [response]

        self.pool.send_request(request, lambda x: self.response_q.put(x))

        self.pool.join()

        self.assertEqual(self.response_q.get_nowait(), response)

    def test_json_error_field(self):
        request, valid_response = self.create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.warning, 'key=http_response_error'):
            self.pool.send_request(request, lambda x: self.response_q.put(x))
            self.pool.join()

        self.assertEqual(self.response_q.get_nowait(), valid_response)

    def test_too_many_retries(self):
        request = HTTPRequest('http://www.neon.com')
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            invalid_response,
            ]

        with self.assertLogExists(logging.error, 'key=http_too_many_errors'):
            self.pool.send_request(request, lambda x: self.response_q.put(x))
            self.pool.join()

        found_response = self.response_q.get_nowait()
        self.assertEqual(found_response.error.code, 503)
        self.assertEqual(found_response, invalid_response)
        self.assertTrue(self.response_q.empty())

    def test_599_error(self):
        request, valid_response = self.create_valid_ack()
        invalid_response = HTTPError(599)
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.warning,
                                  'key=http_connection_error msg=.*599'):
            self.pool.send_request(request, lambda x: self.response_q.put(x))
            self.pool.join()

        self.assertEqual(self.response_q.get_nowait(), valid_response)

    def test_500_error_not_raised(self):
        request, valid_response = self.create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client().fetch.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.warning,
                                  'key=http_connection_error msg=.*500'):
            self.pool.send_request(request, lambda x: self.response_q.put(x))
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
        request, valid_response = self.create_valid_ack()
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

        
   
if __name__ == '__main__':
    unittest.main()
