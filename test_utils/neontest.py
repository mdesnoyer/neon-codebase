'''
An extention of the builtin unittest module.

Allows some more complicated assert options.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import concurrent.futures
from contextlib import contextmanager
import logging
from mock import MagicMock
import re
import time
import tornado.testing
import unittest

class TestCase(unittest.TestCase):
    '''Use this instead of the unittest one to get the extra functionality.'''

    @contextmanager
    def assertLogExists(self, level, regexp):
        '''Asserts that a log message was written at a given level.

        This can be used either in a with statement e.g:
        with self.assertLogExists(logging.INFO, 'Hi'):
          do_stuff()
          _log.info('Hi')

        or as a decorator. e.g.:
        @assertLogExists(logging.INFO, 'hi')
        def test_did_log(self):
          _log.info('Hi')
          
        '''
        handler = LogCaptureHandler()
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            yield

        finally:
            logger.removeHandler(handler)
            
        reg = re.compile(regexp)
        matching_logs = handler.get_matching_logs(reg, level)

        if len(matching_logs) == 0:
            self.fail(
                'Msg: %s was not logged. The log was: %s' % 
                (regexp,
                 '\n'.join(['%s: %s' % (x.levelname, x.getMessage())
                            for x in handler.logs])))

    @contextmanager
    def assertLogNotExists(self, level, regexp):
        '''Asserts that a log message was not written at a given level.

        This can be used either in a with statement e.g:
        with self.assertLogNotExists(logging.INFO, 'Hi'):
          do_stuff()
          _log.info('Hi')

        or as a decorator. e.g.:
        @assertLogNotExists(logging.INFO, 'hi')
        def test_did_log(self):
          _log.error('Hi')
          
        '''
        handler = LogCaptureHandler()
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            yield

        finally:
            logger.removeHandler(handler)

        reg = re.compile(regexp)
        matching_logs = handler.get_matching_logs(reg, level)

        if len(matching_logs) > 0:
            self.fail(
                'Msg: %s was logged and it should not have. '
                'The log was: %s' % 
                (regexp,
                '\n'.join(['%s: %s' % (x.levelname, x.getMessage())
                            for x in handler.logs])))

    def assertWaitForEquals(self, func, expected, timeout=5.0):
        '''Waits for the result of a function to equal val.'''
        start_time = time.time()
        found = None
        while (time.time() - start_time) < timeout:
            try:
                found = func()
                if found == expected:
                    return
            except Exception as e:
                found = '%s: %s' % (e.__class__.__name__, e)
            time.sleep(0.05)
        self.fail('Timed out waiting for %s to equal %s. '
                  'Its value was %s' % (func, expected, found))

    def _callback_wrap_mock(self, outer_mock):
        '''Sets up a mock that mocks out a call that acts on a callback.

        So, if you are trying to mock out a function that looks like:
        def my_callback_func(x, callback=None):
           pass

        You would mock it by:
        patcher = patch('my_callback_func')
        mock = self._callback_wrap_mock(patcher.start())

        And you can treat the mock as if it is a normal, synchronous function.
        e.g. mock.side_effect = [response]

        Input: outer_mock - Mock of the function that does a callback
        Returns: 
        mock that can be used to set the actual function return value/exception
        '''
        inner_mock = MagicMock()
        def _do_callback(*args, **kwargs):
            callback = kwargs.get('callback', None)
            if 'callback' in kwargs:
                del kwargs['callback']
                
            response = inner_mock(*args, **kwargs)
            if callback:
                callback(response)
            else:
                return response

        outer_mock.side_effect = _do_callback
        return inner_mock

class LogCaptureHandler(logging.Handler):
    '''A class that just collects all the logs.'''
    def __init__(self):
        super(LogCaptureHandler, self).__init__()
        self.logs = []

    def emit(self, record):
        self.logs.append(record)

    def get_matching_logs(self, regexp, level):
        '''Returns all the logs that match the regexp at a given level.'''
        return [x for x in self.logs if 
                x.levelno == level and regexp.search(x.getMessage())]

class AsyncTestCase(tornado.testing.AsyncTestCase, TestCase):
    '''A test case that has access to Neon functions and can do tornado async calls.'''
    def setUp(self):
        tornado.testing.AsyncTestCase.setUp(self)

    def tearDown(self):
        tornado.testing.AsyncTestCase.tearDown(self)

    def _future_wrap_mock(self, outer_mock):
        '''Sets up a mock that mocks out a call that returns a future.

        For example, if a function that returns a future is patched with
        func_patch, then do:
        
        self.func_mock = self._future_wrap_mock(func_patcher.start())

        and the following will do what you expect
        self.func_mock.side_effect = [67, Exception('oops')]

        Input:
        outer_mock - Mock of the function that needs a future
        Returns: 
        mock that can be used to set the actual function return value/exception
        '''
        inner_mock = MagicMock()
        def _build_future(*args, **kwargs):
            future = concurrent.futures.Future()
            try:
                future.set_result(inner_mock(*args, **kwargs))
            except Exception as e:
                future.set_exception(e)
            return future
        outer_mock.side_effect = _build_future
        return inner_mock

class AsyncHTTPTestCase(tornado.testing.AsyncHTTPTestCase, TestCase):
    '''A test case that has access to Neon functions and can 
    test a tornado async http server calls.

    '''
    def setUp(self):
        tornado.testing.AsyncHTTPTestCase.setUp(self)

    def tearDown(self):
        tornado.testing.AsyncHTTPTestCase.tearDown(self)

def main():
    unittest.main()
