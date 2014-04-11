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

import tornado.concurrent
import tornado.gen
import tornado.ioloop
import test_utils.neontest
import utils.sync
import unittest

class TestOptionalSyncDecorator(test_utils.neontest.AsyncTestCase):
    ''' Services Test '''

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

if __name__ == '__main__':
    unittest.main()
