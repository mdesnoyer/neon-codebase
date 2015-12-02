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

from concurrent.futures import Future, ThreadPoolExecutor
import logging
from mock import patch, MagicMock
import Queue
import multiprocessing
import random
import socket
from StringIO import StringIO
import test_utils.neontest
import threading
import time
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

class TestSendRequest(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestSendRequest, self).setUp()
        self.sync_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')

        self.mock_client = self._future_wrap_mock(
            self.sync_patcher.start()().fetch)
        logging.getLogger('utils.http').reset_sample_counters()

    def tearDown(self):
        self.sync_patcher.stop()
        super(TestSendRequest, self).tearDown()

    def test_valid_sync_request(self):
        request, response = create_valid_ack()
        self.mock_client.side_effect = [response]

        self.assertEqual(utils.http.send_request(request), response)

    def test_invalid_url(self):
        with self.assertLogExists(logging.ERROR, 'Invalid url to request'):
            response = utils.http.send_request(
                HTTPRequest('file:///some/local/file'))

        self.assertEquals(response.code, 400)
        self.assertIsNotNone(response.error)

    def test_json_error_field(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Error sending request to.* 600'):
            found_response = utils.http.send_request(request, 3,
                                                     base_delay=0.02)

        self.assertEqual(found_response, valid_response)

    def test_json_error_out(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            invalid_response
            ]

        with self.assertLogExists(logging.WARNING, 'Error sending.*600'):
            found_response = utils.http.send_request(request, 3,
                                                     base_delay=0.02)

        self.assertRegexpMatches(found_response.error.message, '.*600')

    def test_connection_errors(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(500) 
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Error sending request to .*500'):
            found_response = utils.http.send_request(request, 3,
                                                     base_delay=0.02)

        self.assertEqual(found_response, valid_response)

    def test_no_retry_codes(self):
        request, valid_response = create_valid_ack()
        ok_error = HTTPError(405)
        self.mock_client.side_effect = [HTTPError(404),
                                        ok_error]

        found_response = utils.http.send_request(request, 3,
                                                 no_retry_codes=[405])
        self.assertEqual(found_response.error, ok_error)
        self.assertEqual(self.mock_client.call_count, 2)

    def test_socket_errors(self):
        request, valid_response = create_valid_ack()
        self.mock_client.side_effect = [
            socket.error(),
            socket.error(),
            valid_response
            ]

        with self.assertLogExists(logging.ERROR,
                                  'Socket resolution error'):
            found_response = utils.http.send_request(request, 3,
                                                     base_delay=0.02)

        self.assertEqual(found_response, valid_response)

    def test_no_logging(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(500) 
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogNotExists(logging.WARNING,
                                     'Error sending request to .*500'):
            found_response = utils.http.send_request(request, 3,
                                                     do_logging=False,
                                                     base_delay=0.02)

        self.assertEqual(found_response, valid_response)

    def test_not_raised_connection_errors(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Error sending request to .*500'):
            found_response = utils.http.send_request(request, 3,
                                                     base_delay=0.02)

        self.assertEqual(found_response, valid_response)

    def test_too_many_retries(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]
        
        found_response = utils.http.send_request(request, 2,
                                                 base_delay=0.02)

        self.assertEqual(found_response.error.code, 500)
        self.assertRegexpMatches(str(found_response.error),
                                 'Internal Server Error')

    @tornado.testing.gen_test
    def test_valid_async_request(self):
        request, response = create_valid_ack()
        self.mock_client.side_effect = [response]
        
        found_response = yield utils.http.send_request(request, async=True)

        self.assertEqual(found_response, response)

    @tornado.testing.gen_test
    def test_yielding_task(self):
        request, response = create_valid_ack()
        self.mock_client.side_effect = [response]

        found_response = yield tornado.gen.Task(utils.http.send_request,
                                                request)

        self.assertEqual(found_response, response)

    @tornado.testing.gen_test
    def test_async_json_error_field(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING, 'Error sending.*600'):
            found_response = yield utils.http.send_request(request, 3,
                                                           base_delay=0.02,
                                                           async=True)
            self.assertEqual(found_response, valid_response)

    @tornado.testing.gen_test
    def test_async_socket_errors(self):
        request, valid_response = create_valid_ack()
        self.mock_client.side_effect = [
            socket.error(),
            socket.error(),
            valid_response
            ]

        with self.assertLogExists(logging.ERROR,
                                  'Socket resolution error'):
            found_response = yield utils.http.send_request(
                HTTPRequest('http://sdjfaoei.com'),
                3,
                base_delay=0.02,
                async=True)
            self.assertEqual(found_response, valid_response)

    @tornado.testing.gen_test
    def test_too_many_retries(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]
        
        
        found_response = yield utils.http.send_request(request, 2,
                                                       base_delay=0.02,
                                                       async=True)

        self.assertEqual(found_response.error.code, 500)
        self.assertRegexpMatches(str(found_response.error),
                                 'Internal Server Error')

class TestRequestPool(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestRequestPool, self).setUp()
        self.pool = utils.http.RequestPool(5)
        self.patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.mock_client = self._future_wrap_mock(
            self.patcher.start()().fetch)
        logging.getLogger('utils.http').reset_sample_counters()
        
    def tearDown(self):
        self.patcher.stop()
        super(TestRequestPool, self).tearDown()

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _send_request(self, *args, **kwargs):
        kwargs['async'] = True
        val = yield self.pool.send_request(*args, **kwargs)
        raise tornado.gen.Return(val)

    def test_single_blocking_request(self):
        request, response = create_valid_ack()
        self.mock_client.side_effect = [response]

        got_response = self._send_request(request)

        self.assertEqual(got_response, response)

    @tornado.testing.gen_test
    def test_json_error_field(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING, 'Error sending.*600'):
            response = yield self._send_request(request,
                                                base_delay=0.02,
                                                async=True)

        self.assertEqual(response, valid_response)

    @tornado.testing.gen_test
    def test_too_many_retries(self):
        request = HTTPRequest('http://www.neon.com', body='Lalalalal')
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            invalid_response,
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Too many errors connecting'):
            found_response = yield self._send_request(request,
                                                          base_delay=0.02,
                                                          ntries=3,
                                                          async=True)

        self.assertEqual(found_response.error.code, 500)
        self.assertEqual(found_response, invalid_response)

    @tornado.testing.gen_test
    def test_no_logging(self):
        request = HTTPRequest('http://www.neon.com', body='Lalalalal')
        invalid_response = HTTPResponse(request, 200,
                                        buffer=StringIO('{"error":600}'))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            invalid_response,
            ]

        with self.assertLogNotExists(logging.WARNING, 'Too many errors'):
            found_response = yield self._send_request(request,
                                                          base_delay=0.02,
                                                          ntries=3,
                                                          async=True,
                                                          do_logging=False)

        self.assertEqual(found_response.error.code, 500)
        self.assertEqual(found_response, invalid_response)

    @tornado.testing.gen_test
    def test_599_error(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(599)
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Error sending request.*599'):
            found_response = yield self._send_request(
                request, base_delay=0.02, async=True)

        self.assertEqual(found_response, valid_response)

    @tornado.testing.gen_test
    def test_500_error_not_raised(self):
        request, valid_response = create_valid_ack()
        invalid_response = HTTPResponse(request, 500,
                                        error=HTTPError(500))
        self.mock_client.side_effect = [
            invalid_response,
            invalid_response,
            valid_response
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Error sending request.*500'):
            found_response = yield self._send_request(request, 
                                                          base_delay=0.05,
                                                          async=True)

        self.assertEqual(found_response, valid_response)

    @tornado.testing.gen_test
    def test_no_json_in_body(self):
        request = HTTPRequest('http://www.neon.com')
        response = HTTPResponse(request, 200,
                                buffer=StringIO('hello'))
        self.mock_client.side_effect = [response]

        got_response = yield self._send_request(request, 
                                                    base_delay=0.05,
                                                    async=True)

        self.assertEqual(got_response, response)

    @tornado.testing.gen_test
    def test_different_json_in_body(self):
        request = HTTPRequest('http://www.neon.com')
        response = HTTPResponse(request, 200,
                                buffer=StringIO('{"hello": 3}'))
        self.mock_client.side_effect = [response]

        got_response = yield self._send_request(request, 
                                                    base_delay=0.05,
                                                    async=True)

        self.assertEqual(got_response, response)

    @tornado.testing.gen_test(timeout=10)
    def test_lots_of_requests(self):
        random.seed(1659843)
        request, valid_response = create_valid_ack()
        invalid_response = HTTPError(599)

        n_queries = 200
        n_errors = 15
        responses = [valid_response for i in range(n_queries)]
        responses.extend([invalid_response for i in range(n_errors)])
        random.shuffle(responses)
        
        self.mock_client.side_effect = responses

        responses = yield [self._send_request(request, 
                                              base_delay=0.05,
                                              ntries=12,
                                              async=True) 
                           for x in range(n_queries)]

        self.assertEqual(len(responses), n_queries)

    @tornado.testing.gen_test
    def test_yielding(self):
        request, response = create_valid_ack()
        self.mock_client.side_effect = [response]

        found_response = yield self._send_request(request, async=True)

        self.assertEqual(found_response, response)

class TestRequestPoolInThread(TestRequestPool):
    def setUp(self):
        super(TestRequestPoolInThread, self).setUp()
        self.pool = utils.http.RequestPool(5, thread_safe=True)
        self.n_sent = 0
        
    def tearDown(self):
        super(TestRequestPoolInThread, self).tearDown()

    def _thread_submit(self, *args, **kwargs):
        self.n_sent += 1
        val = self.pool.send_request(*args, **kwargs)
        self.n_sent -= 1
        return val

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _send_request(self, *args, **kwargs):
        pool = ThreadPoolExecutor(1)
        val = yield pool.submit(self._thread_submit, *args, **kwargs)
        raise tornado.gen.Return(val)
        

class TestRequestPoolInSubprocess(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestRequestPoolInSubprocess, self).setUp()
        self.pool = utils.http.RequestPool(1, True)
        self.patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.mock_client = self.patcher.start()().fetch

        self.response_q = multiprocessing.Queue()
        logging.getLogger('utils.http').reset_sample_counters()
        
    def tearDown(self):
        self.patcher.stop()
        super(TestRequestPoolInSubprocess, self).tearDown()
        
    @tornado.testing.gen_test
    def test_limit_resources_across_procs(self):
        request, response = create_valid_ack()
        response_future = Future()
        response_future.set_result(response)
        # This will set the side effect for the subprocess because
        # this object is copied on subprocess creation
        self.mock_client.side_effect = [response_future]

        # Setup a sempahore to stop the other processing from sending
        # its request until we are ready
        start_func = multiprocessing.Semaphore(1)
        start_func.acquire()

        def _send_request(start_func):
            with start_func:
                response = self.pool.send_request(request)
                self.response_q.put(response)

        proc = multiprocessing.Process(target=_send_request,
                                       args=(start_func,))
        proc.daemon = True
        proc.start()

        # Now set the side effect for the master process
        response_future = Future()
        self.mock_client.side_effect = [response_future]

        # Now, we grab the resource in the pool because we won't set
        # the future's return value (or yield it)
        pool_future = self.pool.send_request(request, async=True)

        # Let the sub process go and make sure it doesn't grab the resource
        start_func.release()

        time.sleep(0.5)
        self.assertTrue(proc.is_alive())
        with self.assertRaises(Queue.Empty):
            self.response_q.get_nowait()

        # Finish the request in the master process
        response_future.set_result(response)
        yield pool_future

        # Now make sure that the subprocess did it's thing correctly
        proc.join(5.0)
        self.assertFalse(proc.is_alive())

        found_response = self.response_q.get(timeout=5.0)
        self.assertEqual(found_response.body, response.body)
        self.assertEqual(found_response.code, response.code)
        self.assertIsNone(found_response.error)
        self.assertEqual(found_response.effective_url, response.effective_url)

    @tornado.testing.gen_test
    def test_potential_deadlock_no_multiproc(self):
        self.pool = utils.http.RequestPool(1, False)
        request, response = create_valid_ack()

        response_futures = [Future(), Future(), Future()]
        response_futures[1].set_result(response)
        response_futures[2].set_result(response)
        self.mock_client.side_effect = response_futures

        # Send the first request, which keeps the http request pending
        pool_future1 = self.pool.send_request(request, async=True)
        self.assertFalse(pool_future1.done())

        # Send the second request, which will return if it is actually
        # able to get the lock. It should be in the semaphore acquire.
        pool_future2 = self.pool.send_request(request, async=True)
        self.assertFalse(pool_future1.done())
        self.assertFalse(pool_future2.done())

        # Now send a third request that should also be able to get to
        # the semaphore and not deadlock here.
        pool_future3 = self.pool.send_request(request, async=True)
        self.assertFalse(pool_future1.done())
        self.assertFalse(pool_future2.done())
        self.assertFalse(pool_future3.done())

        # Now we are going to release the first call, which should let
        # everything through.
        response_futures[0].set_result(response)
        yield [pool_future1, pool_future2, pool_future3]

        self.assertEquals(self.mock_client.call_count, 3)
        
   
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
