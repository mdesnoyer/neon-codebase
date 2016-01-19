#!/usr/bin/env python
'''
Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
  sys.path.insert(0, __base_path__)

import logging
import multiprocessing
import os
import time
import tornado.concurrent
import tornado.gen
import tornado.ioloop
import tornado.testing
import test_utils.neontest
import utils.http
import utils.sync
import unittest

class TestOptionalSyncDecorator(test_utils.neontest.AsyncTestCase):

    def setUp(self):
        super(TestOptionalSyncDecorator, self).setUp()

        self.fast_callback_count = 0
        self.slow_callback_count = 0

    def tearDown(self):
        super(TestOptionalSyncDecorator, self).tearDown()

    @tornado.concurrent.return_future
    def _fast_callback_function(self, callback):
        self.fast_callback_count += 1
        callback(self.fast_callback_count)

    @tornado.concurrent.return_future
    def _slow_callback_function(self, callback):
        self.slow_callback_count += 1
        tornado.ioloop.IOLoop.current().add_callback(callback,
                                                     self.slow_callback_count)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _some_tornado_async_function(self, func):
        val = yield func()
        raise tornado.gen.Return(val)

    def test_tornado_wait_async(self):
        self._some_tornado_async_function(self._slow_callback_function,
                                          callback=self.stop)
        self._some_tornado_async_function(self._fast_callback_function,
                                          callback=self.stop)
        self._some_tornado_async_function(self._slow_callback_function,
                                          callback=self.stop)
        self.wait()

        self.assertEqual(self.fast_callback_count, 1)
        self.assertEqual(self.slow_callback_count, 2)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def test_tornado_task_async_function(self):
        val = yield tornado.gen.Task(self._some_tornado_async_function,
                                     self._slow_callback_function)
        self.assertEqual(val, 1)
        val = yield tornado.gen.Task(self._some_tornado_async_function,
                                     self._fast_callback_function)
        self.assertEqual(val, 1)
        val = yield tornado.gen.Task(self._some_tornado_async_function,
                                     self._slow_callback_function)
        self.assertEqual(val, 2)
        

        self.assertEqual(self.fast_callback_count, 1)
        self.assertEqual(self.slow_callback_count, 2)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def test_tornado_async_function_keyword(self):
        val = yield self._some_tornado_async_function(
            self._slow_callback_function, async=True)
        self.assertEqual(val, 1)
        val = yield self._some_tornado_async_function(
            self._fast_callback_function, async=True)
        self.assertEqual(val, 1)
        val = yield self._some_tornado_async_function(
            self._slow_callback_function, async=True)
        self.assertEqual(val, 2)
        

        self.assertEqual(self.fast_callback_count, 1)
        self.assertEqual(self.slow_callback_count, 2)

    def test_sync_function_call(self):
        self.assertEqual(
            self._some_tornado_async_function(self._slow_callback_function), 1)
        self.assertEqual(
            self._some_tornado_async_function(self._fast_callback_function), 1)
        self.assertEqual(
            self._some_tornado_async_function(self._slow_callback_function), 2)

        self.assertEqual(self.fast_callback_count, 1)
        self.assertEqual(self.slow_callback_count, 2)

    def test_exception_in_function_call(self):
        cur_io_loop = tornado.ioloop.IOLoop.current()

        def exception_raise():
            raise Exception()

        with self.assertRaises(Exception):
            self._some_tornado_async_function(exception_raise)

        self.assertEqual(tornado.ioloop.IOLoop.current(), cur_io_loop)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _call_google(self):
        response = yield tornado.gen.Task(
            utils.http.send_request,
            tornado.httpclient.HTTPRequest('http://localhost'),
            1)
        raise tornado.gen.Return(response)

    def _call_google_alot(self, start_gate, wait_gate, end_gate):
        wait_gate.set()
        start_gate.wait()
        for i in range(10):
            self._call_google()
        wait_gate.set()
        end_gate.wait()

    def test_file_descriptors_leaking(self):
        start_gate = multiprocessing.Event()
        wait_gate = multiprocessing.Event()
        end_gate = multiprocessing.Event()
        proc = multiprocessing.Process(target=self._call_google_alot,
                                       args=(start_gate, wait_gate, end_gate))
        proc.daemon = True
        proc.start()
        try:
            wait_gate.wait()
            wait_gate.clear()
            start_fd_count = len(os.listdir("/proc/%s/fd" % proc.pid))

            start_gate.set()
            wait_gate.wait()
            self.assertEqual(start_fd_count,
                             len(os.listdir("/proc/%s/fd" % proc.pid)))
            end_gate.set()

        except Exception:
            # Release the process so that it can finish
            start_gate.set()
            end_gate.set()
            raise

class TestPeriodicCoroutineTimer(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestPeriodicCoroutineTimer, self).setUp()
        self.timer = None

    def tearDown(self):
        if self.timer:
            self.timer.stop()
        super(TestPeriodicCoroutineTimer, self).tearDown()

    @tornado.testing.gen_test
    def test_wait_for_running_func(self):

        state = [None]

        @tornado.gen.coroutine
        def _wait_a_bit():
            state[0] = 'start'
            yield tornado.gen.sleep(0.1)
            state[0] = 'done'

            raise tornado.gen.Return('Finished')
        
        self.timer = utils.sync.PeriodicCoroutineTimer(_wait_a_bit, 0.0)
        self.timer.start()
        
        # Make sure we get the timer started
        yield tornado.gen.moment
        self.assertEquals(state[0], 'start')

        # Now wait for it to be done
        funcval = yield self.timer.wait_for_running_func()
        self.assertEquals(funcval, 'Finished')
        
    @tornado.testing.gen_test
    def test_dont_call_too_often(self):

        calls = []

        @tornado.gen.coroutine
        def _func():
            calls.append(True)
            yield tornado.gen.sleep(0.1)

        self.timer = utils.sync.PeriodicCoroutineTimer(_func, 0.04)
        self.timer.start()

        yield tornado.gen.sleep(0.09)
        self.timer.stop()

        yield self.timer.wait_for_running_func()

        self.assertEquals(len(calls), 1)
        

if __name__ == '__main__':
    unittest.main()
