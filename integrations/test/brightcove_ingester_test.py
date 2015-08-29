#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
from cmsdb import neondata
from cmsdb.neondata import ThumbnailMetadata, ThumbnailType, VideoMetadata
import concurrent.futures
import integrations.brightcove_ingester
from cStringIO import StringIO
import json
import logging
from mock import patch, MagicMock
import multiprocessing
import test_utils.redis
import test_utils.neontest
import tornado.gen
import tornado.testing
import unittest
from utils.imageutils import PILImageUtils
from utils.options import define, options
import utils.neon


class SmokeTesting(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        # Mock brightcove integration
        self.int_mocker = patch(
            'integrations.brightcove_ingester.integrations.brightcove.'
            'BrightcoveIntegration')
        self.int_mock = self.int_mocker.start()
        self.process_mock = self._future_wrap_mock(
            self.int_mock().process_publisher_stream)

        # Build a platform
        def _set_platform(x):
            x.account_id = 'a1'
        neondata.BrightcovePlatform.modify('acct1', 'i1', 
                                           _set_platform,
                                           create_missing=True)

        self.old_poll_cycle = options.get(
            'integrations.brightcove_ingester.poll_period')
        options._set('integrations.brightcove_ingester.poll_period', 0)
        

        super(SmokeTesting, self).setUp()

    def tearDown(self):
        options._set('integrations.brightcove_ingester.poll_period',
                     self.old_poll_cycle)
        self.int_mocker.stop()
        self.redis.stop()

        super(SmokeTesting, self).tearDown()

    @tornado.gen.coroutine
    def _run_one_loop(self):
        # Run the cycle. Clearing the flag after getting back the
        # future will cause the loop to stop after the first iteration.
        run_flag = multiprocessing.Event()
        run_flag.set()
        main_future = integrations.brightcove_ingester.main(run_flag)
        run_flag.clear()
        yield main_future

    @tornado.testing.gen_test
    def test_correct_platform(self):
        yield self._run_one_loop()

        # Make sure we processed the publisher stream
        self.assertEquals(self.process_mock.call_count, 1)
        cargs, kwargs = self.int_mock.call_args
        self.assertEquals(cargs[0], 'a1')
        self.assertEquals(cargs[1].neon_api_key, 'acct1')
        self.assertEquals(cargs[1].integration_id, 'i1')

    @tornado.testing.gen_test
    def test_platform_disabled(self):
        def _set_platform(x):
            x.enabled = False
        neondata.BrightcovePlatform.modify('acct1', 'i1', 
                                           _set_platform)

        with self.assertLogNotExists(logging.DEBUG,
                                     'Processing Brightcove platform'):
                yield self._run_one_loop()

        self.assertEquals(self.process_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_lookup_accountid(self):
        def _set_platform(x):
            x.account_id = None
        neondata.BrightcovePlatform.modify('acct1', 'i1', 
                                           _set_platform)
        neondata.NeonUserAccount('a1', 'acct1').save()

        yield self._run_one_loop()

        # Make sure we processed the publisher stream
        self.assertEquals(self.process_mock.call_count, 1)
        cargs, kwargs = self.int_mock.call_args
        self.assertEquals(cargs[0], 'a1')
        self.assertEquals(cargs[1].neon_api_key, 'acct1')
        self.assertEquals(cargs[1].integration_id, 'i1')

    @tornado.testing.gen_test
    def test_unxepected_error(self):
        self.process_mock.side_effect = [Exception('Huh?!?')]

        with self.assertLogExists(logging.ERROR, 
                                  ('Unexpected exception when processing '
                                   'publisher stream')):
            yield self._run_one_loop()

    @tornado.testing.gen_test
    def test_known_error(self):
        self.process_mock.side_effect = [
            integrations.ovp.OVPError('Oops'),
            integrations.ovp.CMSAPIError('Oops CMSAPI')
            ]

        with self.assertLogNotExists(logging.ERROR, 
                                     ('Unexpected exception when processing '
                                      'publisher stream')):
            yield self._run_one_loop()

        self.assertEquals(self.process_mock.call_count, 1)

        with self.assertLogNotExists(logging.ERROR, 
                                     ('Unexpected exception when processing '
                                      'publisher stream')):
            yield self._run_one_loop()

        self.assertEquals(self.process_mock.call_count, 2)

        
            
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
