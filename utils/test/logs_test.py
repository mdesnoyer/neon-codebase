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

from cStringIO import StringIO
import fake_filesystem
import json
import logging
from mock import patch
from mock import MagicMock
import re
from StringIO import StringIO
import test_utils.neontest
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import unittest
import urllib2
import utils.logs
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

class TestLogglyHandler(test_utils.neontest.TestCase):
    def setUp(self):
        self.url_patcher = patch('utils.http.send_request')
        self.url_mock = self.url_patcher.start()

        self.url_mock.side_effect = \
          lambda x, callback, do_logging: callback(HTTPResponse(x, 200))

        
        self.handler = utils.logs.LogglyHandler('mytag')
        self.logger = logging.getLogger()
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.url_patcher.stop()
        self.logger.removeHandler(self.handler)

    def test_loginfo(self):
        with self.assertLogExists(logging.INFO, 'I got a.* log'):
            _log.info('I got an %s log', 'INFO')

        self.assertEqual(self.url_mock.call_count, 1)
        request = self.url_mock.call_args[0][0]
        self.assertRegexpMatches(request.url, 'https://.*/tag/mytag')
        self.assertDictContainsSubset(
            {'Content-type' : 'application/x-www-form-urlencoded'},
            request.headers)
        raw_data = re.compile('PLAINTEXT=(.*)').match(request.body).group(1)
        record = json.loads(urllib2.unquote(raw_data))

        self.assertEqual(record['msg'], 'I got an INFO log')
        self.assertEqual(record['levelno'], logging.INFO)

    @patch('sys.stderr', new_callable=StringIO)
    def test_bad_connection(self, mock_stderr):
        self.url_mock.side_effect = HTTPError(404)

        with self.assertLogExists(logging.ERROR, 'I got a.* log'):
            _log.error('I got an %s log', 'ERROR')

        self.assertRegexpMatches(mock_stderr.getvalue(),
                                 'HTTPError: HTTP 404')

class TestFlumeHandler(test_utils.neontest.TestCase):
    def setUp(self):
        self.url_patcher = patch('utils.http.send_request')
        self.url_mock = self.url_patcher.start()

        self.url_mock.side_effect = \
          lambda x, callback, do_logging: callback(HTTPResponse(x, 200))

        
        self.handler = utils.logs.FlumeHandler('http://localhost:6366')
        self.logger = logging.getLogger()
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.url_patcher.stop()
        self.logger.removeHandler(self.handler)

    def test_loginfo(self):
        with self.assertLogExists(logging.INFO, 'I got a.* log'):
            _log.info('I got an %s log', 'INFO')

        self.assertEqual(self.url_mock.call_count, 1)
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
        lambda x, callback, do_logging: callback(
            HTTPResponse(x, 404, error=HTTPError(404)))

        with self.assertLogExists(logging.ERROR, 'I got a.* log'):
            _log.error('I got an %s log', 'ERROR')

        self.assertRegexpMatches(mock_stderr.getvalue(),
                                 'HTTPError: HTTP 404')


if __name__ == '__main__':
    utils.neon.InitNeon()

    unittest.main()
