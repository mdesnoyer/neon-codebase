#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from datetime import datetime
import mock
from mock import MagicMock, patch
from mrjob.protocol import *
import MySQLdb
import os
import redis.exceptions
from StringIO import StringIO
import sqlite3
from supportServices import neondata
import stats.db
import tempfile
from test_utils import neontest
import test_utils.mr
import unittest
import urllib2
import utils.neon
from utils.options import define, options

import stats.tracker_monitoring_mr as tm

### Helper functions
def encode(entries, protocol=PickleProtocol):
    '''Encodes a list of key value pairs into the mrjob protocol.'''
    encoder = protocol()
    return '\n'.join([encoder.write(key, value) for key, value in entries])

def sec2str(sec):
    return datetime.utcfromtimestamp(sec).isoformat(' ')

def sec2time(sec):
    return datetime.utcfromtimestamp(sec)

class TestDataParsing(neontest.TestCase):
    def setUp(self):
        self.mr = tm.TrackerMonitoring(['-r', 'inline', '--no-conf', '-'])

    def test_valid_click(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"sts":19800, "a":"click", "page":"here.com", "tai":"kp",'
             '"ttype":"flashonly", "img":"http://monkey.com"}\n'
             '{"sts":19801, "a":"click", "page":"www.there.com/pole",'
             '"tai":"45d", "ttype":"flashonly", "img":"panda.jpg"}\n'
             '{"sts":19802, "a":"click", "page":"http://co.com/lop",'
             '"tai":"98e", "ttype":"flashonly", "img":"tiger.png"}\n'
             '{"sts":19803, "a":"click", "page":"https://here.com","tai":"kp",'
             '"ttype":"flashonly", "img":"tiger.png"}\n'
             ),
            protocol=RawProtocol)
        self.assertItemsEqual(
            results,
            [(('click', 'here.com', 'kp'), 19800),
             (('click', 'www.there.com/pole', '45d'), 19801),
             (('click', 'co.com/lop', '98e'), 19802),
             (('click', 'here.com', 'kp'), 19803)])

    def test_valid_load(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
          ('{"sts":19800,"a":"load","page":"here.com/now","ttype":"flashonly",'
           '"tai":"sdf3", "imgs":["http://monkey.com","poprocks.jpg"]}'),
            protocol=RawProtocol)
                                            
        self.assertItemsEqual(
            results,
            [(('load', 'here.com/now', 'sdf3'), 19800),
             ])

    def test_valid_unicode(self):
        
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"sts":19800, "a":"click", "page":"here.com/\xc3\xa9te",'
             '"tai":"kp","ttype":"flashonly", "img":"http://monkey.com"}\n'
             ),
            protocol=RawProtocol)
        self.assertItemsEqual(
            results,
            [(('click', u'here.com/\xe9te', 'kp'), 19800),])

    def test_invalid_json(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"sts":19800, "a":"click" "img":"http://monkey.com"}\n'
            '{"sts":1900, "a":"click", "img":"http://monkey.com"\n'
            '{"sts":1900, "a":"load", "imgs":["now.com"}\n'
            '{"sts":"fge", "ttype":"flashonly","a":"load","page":"here.com",'
            '"imgs":["now.com"],"tai":"adsfe"}\n'),
            step=0, protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['TrackerMonitoringErrors']['JSONParseErrors'], 4)

    def test_fields_missing(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"a":"click","ttype":"flashonly", "page":"here.com", '
             '"tai":"697"}\n'
             '{"sts":19800, "ttype":"flashonly", "page":"here.com", '
             '"tai":"697"}\n'
             '{"sts":19800, "ttype":"flashonly", "a":"click", "tai":"697"}\n'
             '{"sts":19800, "ttype":"flashonly", "a":"click", '
             '"page":"here.com"}\n'),
            protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['TrackerMonitoringErrors']['JSONFieldMissing'], 4)

    def test_bad_action(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"sts":19800,"ttype":"flashonly", "a":"clicks", '
             '"img":"http://monkey.com"}\n'
            '{"sts":1900,"ttype":"flashonly", "a":"wow",'
            '"img":"http://monkey.com"}\n'
            '{"sts":1900,"ttype":"flashonly", "a":"loading",'
            '"imgs":["now.com"]}\n'),
            step=0, protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['TrackerMonitoringErrors']['InvalidData'], 3)

    def test_html5_player(self):
        '''We need to ignore entries for the html5 player.

        It can't track loads properly.
        '''
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"sts":19800, "a":"click", '
             '"ttype":"html5", "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"load", '
             '"ttype":"html5", "imgs":["http://panda.com"]}\n'),
            protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
           counters['TrackerMonitoringErrors']['HTML5_bc_click'], 1)
        self.assertEqual(
           counters['TrackerMonitoringErrors']['HTML5_bc_load'], 1)

class TestIDMapping(neontest.TestCase):
    '''Tests for mapping thumbnail urls to ids.'''
    def setUp(self):
        self.mr = tm.TrackerMonitoring(['-r', 'inline', '--no-conf', '-'])

    def tearDown(self):
        pass

    @patch('stats.tracker_monitoring_mr.neondata.TrackerAccountIDMapper.get_neon_account_id')
    def test_valid_mapping(self, mock_mapper):
        mock_mapper.return_value = ("54s9dfewgvw9e8g9",
                                    neondata.TrackerAccountIDMapper.PRODUCTION)
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'first.com', 'tid'), 3)]),
            step=1)

        self.assertEqual(mock_mapper.call_count, 1)
        cargs, kwargs = mock_mapper.call_args
        self.assertEqual(cargs[0], 'tid')
        self.assertItemsEqual(results[0],
                              (('click', 'first.com','54s9dfewgvw9e8g9',False),
                               3))

    @patch('stats.tracker_monitoring_mr.neondata.TrackerAccountIDMapper.get_neon_account_id')
    def test_valid_staging_mapping(self, mock_mapper):
        mock_mapper.return_value = ("54s9dfewgvw9e8g9",
                                    neondata.TrackerAccountIDMapper.STAGING)
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'first.com', 'tid'), 3)]),
            step=1)

        self.assertEqual(mock_mapper.call_count, 1)
        cargs, kwargs = mock_mapper.call_args
        self.assertEqual(cargs[0], 'tid')
        self.assertItemsEqual(results[0],
                              (('click', 'first.com','54s9dfewgvw9e8g9',True),
                               3))

    @patch('stats.tracker_monitoring_mr.neondata.TrackerAccountIDMapper.get_neon_account_id')
    def test_no_mapping_available(self, mock_mapper):
        mock_mapper.side_effect = [None]
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tid'), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock_mapper.call_count, 1)
        self.assertEqual(
            counters['TrackerMonitoringErrors']['InvalidTAI'], 1)

    @patch('stats.tracker_monitoring_mr.neondata.TrackerAccountIDMapper.get_neon_account_id')
    def test_redis_error(self, mock_mapper):
        mock_mapper.side_effect = redis.exceptions.RedisError
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tid'), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock_mapper.call_count, 1)
        self.assertEqual(
            counters['TrackerMonitoringErrors']['RedisErrors'], 1)
        

class TestDatabaseWriting(neontest.TestCase):
    '''Tests database writing step.'''
    def setUp(self):
        self.mr = tm.TrackerMonitoring(['-r', 'inline', '--no-conf', '-'])
        
        self.dbconnect = MySQLdb.connect
        dbmock = MagicMock()
        def connect2db(*args, **kwargs):
            return sqlite3.connect('file::memory:?cache=shared')
        dbmock.side_effect = connect2db
        MySQLdb.connect = dbmock
        self.ramdb = connect2db()

        self.urlopen_patcher = patch(
            'stats.tracker_monitoring_mr.urllib2.urlopen')
        self.mock_urlopen = self.urlopen_patcher.start()

        self.account_patch = patch(
            'stats.tracker_monitoring_mr.neondata.'
            'NeonUserAccount.get_account')
        self.get_account_mock = self.account_patch.start()
        self.account_mock = MagicMock()
        self.get_account_mock.return_value = self.account_mock
        self.account_mock.get_platforms.return_value = [
            MagicMock() for x in range(2)]
        
        self.api_key_patch = patch(
            'stats.tracker_monitoring_mr.neondata.'
            'NeonApiKey.get_api_key')
        self.api_key_patcher = self.api_key_patch.start()
        self.get_api_key = MagicMock()
        self.get_api_key().return_value = "neon_api_key" 

    def tearDown(self):
        self.urlopen_patcher.stop()
        self.account_patch.stop()
        MySQLdb.connect = self.dbconnect
        try:
            cursor = self.ramdb.cursor()
            cursor.execute('drop table pages_seen')
            self.ramdb.commit()
        except Exception as e:
            pass
        self.ramdb.close()
        os.remove('file::memory:?cache=shared')

    def test_table_creation(self):
        results, counters = test_utils.mr.run_single_step(self.mr, '', step=2)
        cursor = self.ramdb.cursor()
        cursor.execute('select * from %s' % stats.db.get_pages_seen_table())
        self.assertEqual(len(cursor.fetchall()), 0)
        self.assertEqual(len(cursor.description), 6)
        self.assertEqual([x[0] for x in cursor.description],
                         ['id', 'neon_acct_id', 'page', 'is_testing',
                          'last_load', 'last_click'])

    def test_new_click_entry(self):
        self.mock_urlopen().getcode.return_value = 200
        self.mock_urlopen.reset_mock()
        
        garb, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('click', 'www.go.com/now', 'na4576', False), 15600)]),
            step=2)
        
        # Make sure that there were no errors
        self.assertEqual(counters, {})

        # Make sure that the entry in the database was updated
        cursor = self.ramdb.cursor()
        cursor.execute('select neon_acct_id, page, is_testing, last_click '
                       'from pages_seen')
        self.assertEqual(cursor.fetchall(),
                         [('na4576', 'www.go.com/now', False, sec2str(15600))])

        # Now ensure that an analytics received message was received
        self.assertEqual(self.mock_urlopen.call_count, 1)
        request = self.mock_urlopen.call_args[0][0]
        self.assertEqual(
            request.get_full_url(),
            'http://api.neon-lab.com/accounts/na4576/analytics_received')
        self.assertEqual(request.headers['X-neon-api-key'],
                         neondata.NeonApiKey.get_api_key('na4576'))

        # Make sure that the abtesting was turned on
        self.get_account_mock.assert_called_once_with(
            neondata.NeonApiKey.get_api_key('na4576'))
        for platform in self.account_mock.get_platforms():
            self.assertTrue(platform.abtest)
            platform.save.assert_called_once_with()

    def test_many_entries_for_same_account(self):
        self.mock_urlopen().getcode.return_value = 200
        self.mock_urlopen.reset_mock()

        garb, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('click', 'www.go.com/now', 'na4576', False), 15600),
                    (('load', 'www.go.com/now', 'na4576', False), 15500),
                    (('load', 'www.go.com/later', 'na4576', False), 15700),
                    (('load', 'www.go.com/around', 'na4576', True), 14800),]),
            step=2)

        # Make sure that there were no errors
        self.assertEqual(counters, {})

        # Make sure that only one analytics received message was sent
        self.assertEqual(self.mock_urlopen.call_count, 1)

        # Make sure that the ab testing was turned on only once
        self.get_account_mock.assert_called_once_with(
            neondata.NeonApiKey.get_api_key('na4576'))

        # Check the database
        cursor = self.ramdb.cursor()
        cursor.execute('select neon_acct_id, page, is_testing, last_click, '
                       'last_load from pages_seen')
        self.assertItemsEqual(
            cursor.fetchall(),
            [('na4576','www.go.com/now',False,sec2str(15600),sec2str(15500)),
             ('na4576', 'www.go.com/later', False, None, sec2str(15700)),
             ('na4576', 'www.go.com/around', True, None, sec2str(14800))])

    def test_only_staging_entries(self):
        self.mock_urlopen().getcode.return_value = 200
        self.mock_urlopen.reset_mock()

        garb, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('click', 'www.go.com/now', 'na4576', True), 15600),
                    (('load', 'www.go.com/now', 'na4576', True), 15500)]),
            step=2)

        # Make sure that there were no errors
        self.assertEqual(counters, {})

        # Make sure that no analytics received message was sent
        self.assertEqual(self.mock_urlopen.call_count, 0)

        # Make sure that the ab testing was not turned on
        self.assertEqual(self.get_account_mock.call_count, 0)

        # Check the database
        cursor = self.ramdb.cursor()
        cursor.execute('select neon_acct_id, page, is_testing, last_click, '
                       'last_load from pages_seen')
        self.assertItemsEqual(
            cursor.fetchall(),
            [('na4576','www.go.com/now',True,sec2str(15600),sec2str(15500))])

    def test_update_entry(self):
        self.mock_urlopen().getcode.return_value = 200

        # Record some data first
        test_utils.mr.run_single_step(
            self.mr,
            encode([(('load', 'www.go.com/now', 'na4576', False), 15500)]),
            step=2)

        self.mock_urlopen.reset_mock()
        self.get_account_mock.reset_mock()

        # Now send data that will update the entry
        garb, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('click', 'www.go.com/now', 'na4576', False), 16600),
                    (('load', 'www.go.com/now', 'na4576', False), 16500)]),
            step=2)

        # Make sure that there were no errors
        self.assertEqual(counters, {})

        # Make sure that there was no analytics received message sent
        self.assertEqual(self.mock_urlopen.call_count, 0)

        # Make sure that the ab testing wasn't changed
        self.assertEqual(self.get_account_mock.call_count, 0)
        
        # Make sure that the entry in the database was updated
        cursor = self.ramdb.cursor()
        cursor.execute('select neon_acct_id, page, is_testing, last_click, '
                       'last_load from pages_seen')
        self.assertEqual(
            cursor.fetchall(),
            [('na4576', 'www.go.com/now', False, sec2str(16600),
              sec2str(16500))])

    def test_multiple_accounts(self):
        self.mock_urlopen().getcode.return_value = 200
        self.mock_urlopen.reset_mock()

        garb, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('click', 'www.go.com/now', 'na4576', False), 15600),
                    (('load', 'www.go.com/now', 'na4576', False), 15500),
                    (('load', 'www.go.com/now', '4576na', False), 15700),]),
            step=2)

        # Make sure that there were no errors
        self.assertEqual(counters, {})

        # Make sure that two analytics received message was sent
        self.assertEqual(self.mock_urlopen.call_count, 2)

        # Make sure that abtesting was updated for both accounts
        self.get_account_mock.assert_has_calls(
            [mock.call(neondata.NeonApiKey.get_api_key('na4576')),
             mock.call(neondata.NeonApiKey.get_api_key('4576na'))], True)
        
        # Check the database
        cursor = self.ramdb.cursor()
        cursor.execute('select neon_acct_id, page, last_click, last_load, '
                       'is_testing from pages_seen')
        self.assertItemsEqual(
            cursor.fetchall(),
            [('na4576', 'www.go.com/now', sec2str(15600),sec2str(15500),False),
             ('4576na', 'www.go.com/now', None, sec2str(15700), False)])

    def test_update_account_lookup_error(self):
        self.mock_urlopen().getcode.return_value = 200
        self.get_account_mock.side_effect = redis.exceptions.RedisError

        results, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('load', 'www.go.com/now', 'na4576', False), 15500)]),
            step=2)

        self.assertEqual(counters['TrackerMonitoringErrors']['RedisError'],
                         1)

    def test_get_platforms_error(self):
        self.mock_urlopen().getcode.return_value = 200
        self.account_mock.get_platforms.side_effect = \
          redis.exceptions.RedisError

        results, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('load', 'www.go.com/now', 'na4576', False), 15500)]),
            step=2)

        self.assertEqual(counters['TrackerMonitoringErrors']['RedisError'],
                         1)

    def test_notify_analytics_error(self):
        self.mock_urlopen.side_effect = urllib2.URLError('Oops')

        results, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('click', 'www.go.com/now', 'na4576', False), 15600),
                    (('load', 'www.go.com/now', 'na4576', False), 15500),
                    (('load', 'www.go.com/now', '4576na', False), 15700),]),
            step=2)

        self.assertEqual(results, [])
        self.assertEqual(self.mock_urlopen.call_count, 2)
        self.assertEqual(
            counters['TrackerMonitoringErrors']['NoNotificationSent'], 2)

    def test_notify_analytics_error_code(self):
        self.mock_urlopen().getcode.return_value = 302
        self.mock_urlopen.reset_mock()

        results, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('click', 'www.go.com/now', 'na4576', False), 15600),
                    (('load', 'www.go.com/now', 'na4576', False), 15500),
                    (('load', 'www.go.com/now', '4576na', False), 15700),]),
            step=2)

        self.assertEqual(results, [])
        self.assertEqual(self.mock_urlopen.call_count, 2)
        self.assertEqual(
            counters['TrackerMonitoringErrors']['NoNotificationSent'], 2)

    def test_connection_error(self):
        MySQLdb.connect = MagicMock(
            side_effect=[MySQLdb.Error('yikes')])
        self.assertRaises(MySQLdb.Error, test_utils.mr.run_single_step,
           self.mr, '', 'mapper', 2)


class TestEndToEnd(neontest.TestCase):
    '''Tests database writing step.'''
    def setUp(self):
        self.maxDiff = None
        self.mr = tm.TrackerMonitoring(['-r', 'inline', '--no-conf', '-'])

        self.idmapper_patch = patch(
            'stats.tracker_monitoring_mr.neondata.'
            'TrackerAccountIDMapper.get_neon_account_id')
        self.idmapper_mock = self.idmapper_patch.start()

        self.urlopen_patcher = patch(
            'stats.tracker_monitoring_mr.urllib2.urlopen')
        self.mock_urlopen = self.urlopen_patcher.start()

        self.account_patch = patch(
            'stats.tracker_monitoring_mr.neondata.'
            'NeonUserAccount.get_account')
        self.get_account_mock = self.account_patch.start()
        self.account_mock = MagicMock()
        self.get_account_mock.return_value = self.account_mock
        self.account_mock.get_platforms.side_effect = [[
            MagicMock() for x in range(2)] for x in range(2)]

        # For some reason, the in memory database isn't shared, so use
        # a temporary file instead. It worked in the other test case....
        self.tempfile = tempfile.NamedTemporaryFile()

        # Replace the database with an in memory one.
        self.dbconnect = MySQLdb.connect
        dbmock = MagicMock()
        def connect2db(*args, **kwargs):
            return sqlite3.connect(self.tempfile.name)
        dbmock.side_effect = connect2db
        MySQLdb.connect = dbmock
        self.ramdb = connect2db()
        
        self.api_key_patch = patch(
            'stats.tracker_monitoring_mr.neondata.'
            'NeonApiKey.get_api_key')
        self.api_key_patcher = self.api_key_patch.start()
        self.get_api_key = MagicMock()
        self.get_api_key().return_value = "neon_api_key" 

    def tearDown(self):
        self.idmapper_patch.stop()
        self.urlopen_patcher.stop()
        self.account_patch.stop()
        self.api_key_patch.stop()

        MySQLdb.connect = self.dbconnect
        try:
            self.ramdb.execute('drop table pages_seen')
        except Exception as e:
            pass
        self.ramdb.close()
        self.tempfile.close()

    def test_bunch_of_data(self):
        # Setup the input data
        input_data = (
            '{"sts":19800, "a":"click", "page":"here.com", "tai":"lok", '
            '"ttype":"flashonly", "img":"http://monkey.com"}\n'
            
            '{"sts":19795, "a":"load", "page":"there.com",'
            '"tai":"pole", "ttype":"flashonly",'
            '"imgs":["http://monkey.com","http://panda.com","pumpkin.wow"]}\n'
            
            '{"sts":19805, "a":"click", "page":"here.com/now", "tai":"lok",'
             '"ttype":"flashonly", "img":"http://panda.com"}\n'
             
            '{"sts":19801, "a":"load", "page":"http://here.com", '
            '"ttype":"flashonly", "tai":"lok","imgs":["http://monkey.com",'
            '"pumpkin.jpg"]}\n'
            
            '{"sts":19810, "a":"click", "page":"here.com","tai":"lok",'
             '"ttype":"flashonly", "img":"http://panda.com"}\n'
             
            '{"sts":19815, "a":"click", "page":"there.com/where","tai":"pole",'
             '"ttype":"flashonly", "img":"pumpkin.jpg"}')
        stdin = StringIO(input_data)
        self.mr.sandbox(stdin=stdin)

        # Mock out the responses for tracker ids to neon account ids
        account_id_map = {
            "lok": ("49a8efg1ea98",neondata.TrackerAccountIDMapper.PRODUCTION),
            "pole": ("2348598ewsfrwe",neondata.TrackerAccountIDMapper.STAGING)
        }
        self.idmapper_mock.side_effect = \
          lambda tai: account_id_map[tai]

        self.mock_urlopen().getcode.return_value = 200
        self.mock_urlopen.reset_mock()

        # Run the map reduce job
        runner = self.mr.make_runner()
        runner.run()
        
        self.assertGreater(self.idmapper_mock.call_count, 0)
        self.assertGreater(MySQLdb.connect.call_count, 0)

        # Make sure notification was only sent for the one account
        self.assertEqual(self.mock_urlopen.call_count, 1)

        # Verify that the A/B tests were turned on for only one account
        self.get_account_mock.assert_has_calls(
            [mock.call(neondata.NeonApiKey.get_api_key('49a8efg1ea98'))], True)

        # Finally, check the database to make sure it says what we want
        cursor = self.ramdb.cursor()
        cursor.execute('select neon_acct_id, page, last_click, last_load, '
                       'is_testing from pages_seen')
        self.assertItemsEqual(
            cursor.fetchall(),
            [('49a8efg1ea98', 'here.com',sec2str(19810),sec2str(19801),False),
             ('49a8efg1ea98', 'here.com/now', sec2str(19805), None, False),
             ('2348598ewsfrwe', 'there.com', None, sec2str(19795), True),
             ('2348598ewsfrwe', 'there.com/where', sec2str(19815),None,True)])

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
