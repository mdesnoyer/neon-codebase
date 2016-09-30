#!/usr/bin/env python
'''
Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import __builtin__
import cloghandler
import copy
from cStringIO import StringIO
import fake_filesystem
import json
import logging
from mock import patch, MagicMock
import re
from StringIO import StringIO
import test_utils.neontest
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import unittest
import urllib2
import utils.logs
from utils.options import define, options
import utils.neon

_log = logging.getLogger(__name__)

class TestLogMessageInSystem(test_utils.neontest.TestCase):
    def test_logdebug(self):
        with self.assertLogExists(logging.DEBUG, 'I got a.* log'):
            _log.debug('I got a DEBUG log')
                        
    def test_loginfo(self):
        with self.assertLogExists(logging.INFO, 'I got a.* log'):
            _log.info('I got an INFO log')

    def test_logwarn(self):
        with self.assertLogExists(logging.WARNING, 'I got a.* log'):
            _log.warn('I got a WARNING log')

    def test_logerror(self):
        with self.assertLogExists(logging.ERROR, 'I got a.* log'):
            _log.error('I got an ERROR log')

    def test_logfatal(self):
        with self.assertLogExists(logging.FATAL, 'I got a.* log'):
            _log.fatal('I got a FATAL log')

class TestSampledLogging(test_utils.neontest.TestCase):
    def test_logdebug(self):
        with self.assertLogExists(logging.DEBUG, 'I got a.* log. count 5'):
            for i in range(8):
                _log.debug_n('I got a DEBUG log. count %i' % i, 5)

    def test_loginfo(self):
        with self.assertLogExists(logging.INFO, 'I got a.* log. count 5'):
            for i in range(8):
                _log.info_n('I got a INFO log. count %i' % i, 5)

    def test_logwarn(self):
        with self.assertLogExists(logging.WARNING, 'I got a.* log. count 5'):
            for i in range(8):
                _log.warning_n('I got a WARNING log. count %i' % i, 5)

    def test_logerror(self):
        with self.assertLogExists(logging.ERROR, 'I got a.* log. count 5'):
            for i in range(8):
                _log.error_n('I got a ERROR log. count %i' % i, 5)

    def test_logfatal(self):
        with self.assertLogExists(logging.CRITICAL, 'I got a.* log. count 5'):
            for i in range(8):
                _log.fatal_n('I got a CRITICAL log. count %i' % i, 5)

    def test_log_exception(self):
        with self.assertLogExists(logging.ERROR, 'I got a.* log. count 5'):
            for i in range(8):
                try:
                    raise Exception
                except:
                    _log.exception_n('I got an exception log. count %i' % i, 5)

class TestAccessLogger(test_utils.neontest.TestCase):
    def setUp(self):
        self.access_logger = logging.getLogger('tornado.access')
        self.orig_handlers = copy.copy(self.access_logger.handlers)
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.fake_open = fake_filesystem.FakeFileOpen(self.filesystem)
        cloghandler.open = self.fake_open

        self.logger_configured = utils.logs._added_configured_logger
        utils.logs._added_configured_logger = False

    def tearDown(self):
        # Delete any handlers in the tornado access logger
        handlers_to_delete = [x for x in self.access_logger.handlers if
                              x not in self.orig_handlers]
        for handler in handlers_to_delete:
            self.access_logger.removeHandler(handler)

        logging.open = __builtin__.open
        cloghandler.open = __builtin__.open
        utils.logs._added_configured_logger = self.logger_configured

    def test_access_log_no_propagate(self):
        utils.logs.AddConfiguredLogger()
        with self.assertLogNotExists(logging.INFO, 'This is an access log'):
            logger = logging.getLogger('tornado.access')
            logger.info('This is an access log')

    def test_access_log_to_file(self):
        with options._set_bounded('utils.logs.access_log_file',
                                  '/access.log'):
            utils.logs.AddConfiguredLogger()

        with self.assertLogNotExists(logging.INFO, 'This is an access log'):
            logger = logging.getLogger('tornado.access')
            logger.info('This is an access log')

        # Check the file contents
        self.assertRegexpMatches(self.fake_open('/access.log').read(),
                                 '.*This is an access log.*')

class TestLogglyHandler(test_utils.neontest.TestCase):
    def setUp(self):
        self.url_patcher = patch('utils.http.RequestPool')
        self.url_mock = self._future_wrap_mock(
            self.url_patcher.start()().send_request)

        self.url_mock.side_effect = \
          lambda x, **kwargs: HTTPResponse(x, 200)

        
        self.handler = utils.logs.LogglyHandler('mytag')
        self.logger = logging.getLogger()
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.logger.removeHandler(self.handler)
        del self.handler
        self.url_patcher.stop()

    def test_loginfo(self):
        with self.assertLogExists(logging.INFO, 'I got a.* log'):
            _log.info('I got an %s log', 'INFO')

        self.assertWaitForEquals(lambda: self.url_mock.call_count, 1)
        request = self.url_mock.call_args[0][0]
        self.assertRegexpMatches(request.url, 'https://.*/tag/mytag')
        self.assertDictContainsSubset(
            {'Content-type' : 'application/x-www-form-urlencoded'},
            request.headers)
        raw_data = re.compile('PLAINTEXT=(.*)').match(request.body).group(1)
        record = json.loads(urllib2.unquote(raw_data))

        self.assertEqual(record['message'], 'I got an INFO log')
        self.assertEqual(record['levelname'], 'INFO')

    @patch('sys.stderr', new_callable=StringIO)
    def test_bad_connection(self, mock_stderr):
        self.url_mock.side_effect = \
          lambda x, **kw: HTTPResponse(x, 404, error=HTTPError(404))

        with self.assertLogExists(logging.ERROR, 'I got a.* log'):
            _log.error('I got an %s log', 'ERROR')

        self.assertWaitForEquals(
            lambda: re.compile('HTTPError: HTTP 404').search(
                mock_stderr.getvalue()) is not None, True)

class TestFlumeHandler(test_utils.neontest.TestCase):
    def setUp(self):
        self.url_patcher = patch('utils.http.RequestPool')
        self.url_mock = self._future_wrap_mock(
            self.url_patcher.start()().send_request)

        self.url_mock.side_effect = \
          lambda x, **kw: HTTPResponse(x, 200)

        
        self.handler = utils.logs.FlumeHandler('http://localhost:6366')
        self.logger = logging.getLogger()
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.logger.removeHandler(self.handler)
        del self.handler
        self.url_patcher.stop()

    def test_loginfo(self):
        with self.assertLogExists(logging.INFO, 'I got a.* log'):
            _log.info('I got an %s log', 'INFO')

        self.assertWaitForEquals(lambda: self.url_mock.call_count, 1)
        request = self.url_mock.call_args[0][0]
        self.assertRegexpMatches(request.url, 'http://localhost:6366')
        self.assertDictContainsSubset(
            {'Content-type' : 'application/json'},
            request.headers)
        flume_data = json.loads(request.body)[0]
        headers = flume_data['headers']

        self.assertRegexpMatches(flume_data['body'], 'I got an INFO log')
        self.assertGreater(headers['timestamp'], 1402444041000)
        self.assertEqual(headers['level'], 'INFO')

    @patch('sys.stderr', new_callable=StringIO)
    def test_bad_connection(self, mock_stderr):
        self.url_mock.side_effect = \
          lambda x, **kw: HTTPResponse(x, 404, error=HTTPError(404))

        with self.assertLogExists(logging.ERROR, 'I got a.* log'):
            _log.error('I got an %s log', 'ERROR')

        self.assertWaitForEquals(
            lambda: re.compile('HTTPError: HTTP 404').search(
                mock_stderr.getvalue()) is not None, True)


if __name__ == '__main__':
    utils.neon.InitNeon()

    unittest.main()
