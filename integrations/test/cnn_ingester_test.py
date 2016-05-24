#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cnn_api
from cmsdb import neondata
import integrations.cnn
import logging
from mock import patch, MagicMock
import test_utils.neontest
import test_utils.postgresql
import tornado.gen
import tornado.httpclient
import tornado.testing
import unittest
import utils.neon
from utils.options import define, options
from utils import statemon

class SmokeTesting(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        statemon.state._reset_values()

        # Mock cnn integration
        self.int_mocker = patch(
            'integrations.ingester.integrations.cnn.'
            'CNNIntegration')
        self.int_mock = self.int_mocker.start()
        self.process_mock = self._future_wrap_mock(
            self.int_mock().process_publisher_stream)

        # Build a platform
        user_id = '234234234dasfds'
        self.user = neondata.NeonUserAccount(user_id,name='testingaccount')
        self.user.save()
        self.integration = neondata.CNNIntegration(self.user.neon_api_key,  
                                                   last_process_date='2015-10-29T23:59:59Z', 
                                                   api_key_ref='c2vfn5fb8gubhrmd67x7bmv9')
        self.integration.save()
        self.integration_id = self.integration.integration_id 

        self.old_poll_cycle = options.get(
            'integrations.ingester.poll_period')
        options._set('integrations.ingester.poll_period', 0.1)
        options._set('integrations.ingester.service_name', 'cnn')

        super(test_utils.neontest.AsyncTestCase, self).setUp()

        self.manager = integrations.ingester.Manager()

    def tearDown(self):
        self.manager.stop()
        options._set('integrations.ingester.poll_period',
                     self.old_poll_cycle)
        self.int_mocker.stop()
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        cls.postgresql.stop()

    @tornado.testing.gen_test
    def test_correct_platform(self):
        self.manager.start()

        # Make sure we processed the publisher stream
        yield self.assertWaitForEquals(lambda: self.process_mock.call_count,
                                       1,
                                       async=True)
        cargs, kwargs = self.int_mock.call_args
        self.assertEquals(cargs[0], self.integration.account_id)
        self.assertEquals(cargs[1].account_id, self.user.neon_api_key)
        self.assertEquals(cargs[1].integration_id, self.integration_id)

    @tornado.testing.gen_test
    def test_two_platforms(self):
        integration_two = neondata.CNNIntegration('acct2',  
                                                  last_process_date='2015-10-29T23:59:59Z', 
                                                  api_key_ref='c2vfn5fb8gubhrmd67x7bmv9')
        integration_two.save()

        self.int_mock.reset_mock()
        self.manager.start()

        yield self.assertWaitForEquals(lambda: self.int_mock.call_count,
                                       2, async=True)

        calls = dict([x[0] for x in self.int_mock.call_args_list])

        self.assertItemsEqual(calls.keys(), [self.user.neon_api_key, 'acct2'])
        self.assertEquals(calls[self.user.neon_api_key].integration_id, self.integration.integration_id)
        self.assertEquals(calls['acct2'].integration_id, integration_two.integration_id)

    @tornado.testing.gen_test
    def test_platform_disabled(self):
        def _set_platform(x):
            x.enabled = False
        neondata.CNNIntegration.modify(self.integration.integration_id, _set_platform)

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
        neondata.CNNIntegration.modify(self.integration.integration_id, _set_platform)

        with self.assertLogExists(logging.INFO, 'Turning off integration'):
            yield self.manager.check_integration_list()

        self.assertEquals(len(self.manager._timers), 0)
        self.assertFalse(timer.is_running())

    @tornado.testing.gen_test
    def test_unexpected_error(self):
        self.process_mock.side_effect = [Exception('Huh?!?')]

        with self.assertLogExists(logging.ERROR, 
                                  ('Unexpected exception when processing '
                                   'publisher stream')):
            yield integrations.ingester.process_one_account(
                'acct1', self.integration.integration_id)

    @tornado.testing.gen_test
    def test_slow_update(self):
        with self.assertLogExists(logging.WARNING, 
                                  ('Finished processing.*Time was')):
            yield integrations.ingester.process_one_account(
                self.user.neon_api_key, self.integration.integration_id, slow_limit=0.0)

        self.assertEquals(self.process_mock.call_count, 1)
        cargs, kwargs = self.int_mock.call_args
        self.assertEquals(cargs[0], self.user.neon_api_key)
        self.assertEquals(cargs[1].account_id, self.user.neon_api_key)
        self.assertEquals(cargs[1].integration_id, self.integration.integration_id)

        self.assertEquals(
            statemon.state.get('integrations.ingester.slow_update'),
            1)

    @tornado.testing.gen_test
    def test_platform_missing(self):
        with self.assertLogExists(logging.ERROR, 'Could not find platform'):
            yield integrations.ingester.process_one_account(
                self.user.neon_api_key, 'i10')

        self.assertEquals(
            statemon.state.get('integrations.ingester.platform_missing'),
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
            yield integrations.ingester.process_one_account(
                self.user.neon_api_key, self.integration.integration_id)

        self.assertEquals(self.process_mock.call_count, 1)

        with self.assertLogNotExists(logging.ERROR, 
                                     ('Unexpected exception when processing '
                                      'publisher stream')):
            yield integrations.ingester.process_one_account(
                self.user.neon_api_key, self.integration.integration_id)

        self.assertEquals(self.process_mock.call_count, 2)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
