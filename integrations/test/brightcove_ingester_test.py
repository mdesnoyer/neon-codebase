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
from utils import statemon


class SmokeTesting(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        statemon.state._reset_values()

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
        options._set('integrations.brightcove_ingester.poll_period', 0.1)
        

        super(SmokeTesting, self).setUp()

        self.manager = integrations.brightcove_ingester.Manager()

    def tearDown(self):
        self.manager.stop()
        options._set('integrations.brightcove_ingester.poll_period',
                     self.old_poll_cycle)
        self.int_mocker.stop()
        self.redis.stop()

        super(SmokeTesting, self).tearDown()

    @tornado.testing.gen_test
    def test_correct_platform(self):
        self.manager.start()

        # Make sure we processed the publisher stream
        yield self.assertWaitForEquals(lambda: self.process_mock.call_count,
                                       1,
                                       async=True)
        cargs, kwargs = self.int_mock.call_args
        self.assertEquals(cargs[0], 'a1')
        self.assertEquals(cargs[1].neon_api_key, 'acct1')
        self.assertEquals(cargs[1].integration_id, 'i1')

    @tornado.testing.gen_test
    def test_two_platforms(self):
        def _set_platform(x):
            x.account_id = 'a2'
        neondata.BrightcovePlatform.modify('acct2', 'i2', 
                                           _set_platform,
                                           create_missing=True)

        self.int_mock.reset_mock()
        self.manager.start()

        yield self.assertWaitForEquals(lambda: self.int_mock.call_count,
                                       2, async=True)

        calls = dict([x[0] for x in self.int_mock.call_args_list])

        self.assertItemsEqual(calls.keys(), ['a1', 'a2'])
        self.assertEquals(calls['a1'].integration_id, 'i1')
        self.assertEquals(calls['a2'].integration_id, 'i2')

    @tornado.testing.gen_test
    def test_platform_disabled(self):
        def _set_platform(x):
            x.enabled = False
        neondata.BrightcovePlatform.modify('acct1', 'i1', 
                                           _set_platform)

        with self.assertLogNotExists(logging.INFO, 'Turning on integration'):
            yield self.manager.check_integration_list()

        self.assertEquals(len(self.manager._timers), 0)

    @tornado.testing.gen_test
    def test_turn_off_platform(self):
        with self.assertLogExists(logging.INFO, 'Turning on integration'):
            yield self.manager.check_integration_list()

        self.assertEquals(len(self.manager._timers), 1)
        timer = self.manager._timers.values()[0]
        self.assertTrue(timer.is_running())

        def _set_platform(x):
            x.enabled = False
        neondata.BrightcovePlatform.modify('acct1', 'i1', 
                                           _set_platform)

        with self.assertLogExists(logging.INFO, 'Turning off integration'):
            yield self.manager.check_integration_list()

        self.assertEquals(len(self.manager._timers), 0)
        self.assertFalse(timer.is_running())

    @tornado.testing.gen_test
    def test_lookup_accountid(self):
        def _set_platform(x):
            x.account_id = None
        neondata.BrightcovePlatform.modify('acct1', 'i1', 
                                           _set_platform)
        neondata.NeonUserAccount('a1', 'acct1').save()

        yield integrations.brightcove_ingester.process_one_account(
            'acct1', 'i1')

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
            yield integrations.brightcove_ingester.process_one_account(
                'acct1', 'i1')

    @tornado.testing.gen_test
    def test_slow_update(self):
        with self.assertLogExists(logging.WARNING, 
                                  ('Finished processing.*Time was')):
            yield integrations.brightcove_ingester.process_one_account(
                'acct1', 'i1', slow_limit=0.0)

        self.assertEquals(self.process_mock.call_count, 1)
        cargs, kwargs = self.int_mock.call_args
        self.assertEquals(cargs[0], 'a1')
        self.assertEquals(cargs[1].neon_api_key, 'acct1')
        self.assertEquals(cargs[1].integration_id, 'i1')

        self.assertEquals(
            statemon.state.get('integrations.brightcove_ingester.slow_update'),
            1)

    @tornado.testing.gen_test
    def test_platform_missing(self):
        with self.assertLogExists(logging.ERROR, 'Could not find platform'):
            yield integrations.brightcove_ingester.process_one_account(
                'acct1', 'i10')

        self.assertEquals(
            statemon.state.get('integrations.brightcove_ingester.platform_missing'),
            1)

        self.assertEquals(self.process_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_known_error(self):
        self.process_mock.side_effect = [
            integrations.ovp.OVPError('Oops'),
            integrations.ovp.CMSAPIError('Oops CMSAPI')
            ]

        with self.assertLogNotExists(logging.ERROR, 
                                     ('Unexpected exception when processing '
                                      'publisher stream')):
            yield integrations.brightcove_ingester.process_one_account(
                'acct1', 'i1')

        self.assertEquals(self.process_mock.call_count, 1)

        with self.assertLogNotExists(logging.ERROR, 
                                     ('Unexpected exception when processing '
                                      'publisher stream')):
            yield integrations.brightcove_ingester.process_one_account(
                'acct1', 'i1')

        self.assertEquals(self.process_mock.call_count, 2)

        
            
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
