#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from datetime import datetime
from mock import MagicMock, patch
from mrjob.protocol import *
import MySQLdb
import os
import redis.exceptions
from StringIO import StringIO
import sqlite3
from supportServices import neondata
import tempfile
import test_utils.mr
import unittest
import utils.neon
from utils.options import define, options

from stats.hourly_event_stats_mr import *

### Helper functions
def encode(entries, protocol=PickleProtocol):
    '''Encodes a list of key value pairs into the mrjob protocol.'''
    encoder = protocol()
    return '\n'.join([encoder.write(key, value) for key, value in entries])

def hr2str(hr):
    return datetime.utcfromtimestamp(hr*3600).isoformat(' ')

def sec2str(sec):
    return datetime.utcfromtimestamp(sec).isoformat(' ')

class TestDataParsing(unittest.TestCase):
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])

    def test_valid_click(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"sts":19800, "a":"click", "tai":"t1", "page":"here.com",'
             '"ttype":"flashonly", "id":"id3", "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"click", "tai":"t1", "page":"here.com",'
             '"ttype":"flashonly", "id":"id4", "img":"http://panda.com"}\n'),
            protocol=RawProtocol)
        self.assertItemsEqual(results,
                              [(('click', 'id3', 'http://monkey.com', 't1'),
                                (5, 1)),
                              (('click', 'id4', 'http://panda.com', 't1'),
                               (5, 1)),
                              ('latest', 19800),
                              ('latest', 19800)])

    def test_valid_load(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
          ('{"sts":19800, "a":"load", "page":"here.com", "ttype":"flashonly",'
           '"imgs":["http://monkey.com","poprocks.jpg","pumpkin.wow"],'
           '"tai":"t1", "id":"id3"}'),
            protocol=RawProtocol)
                                            
        self.assertItemsEqual(results,
                              [(('load', 'id3', 'http://monkey.com', 't1'),
                                (5, 1)),
                               (('load', 'id3', 'poprocks.jpg', 't1'),
                                (5, 1)),
                               (('load', 'id3', 'pumpkin.wow', 't1'),
                                (5, 1)),
                               ('latest', 19800)])

    def test_invalid_json(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"sts":19800, "a":"click" "img":"http://monkey.com"}\n'
            '{"sts":1900, "a":"click", "img":"http://monkey.com"\n'
            '{"sts":1900, "a":"load", "imgs":["now.com"}\n'),
            step=0, protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['HourlyEventStatsErrors']['JSONParseErrors'], 3)

    def test_fields_missing(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            ('{"a":"click", "tai":"t1", "ttype":"flashonly", "id":"id3",'
             '"img":"http://monkey.com"}\n'
             '{"sts":19800,"tai":"t1","id":"id3","a":"click",'
             '"img":"http://monkey.com"}\n'
             '{"sts":19800, "tai":"t1", "ttype":"flashonly", "id":"id3",'
             '"img":"http://monkey.com"}\n'
             '{"sts":19800, "tai":"t1", "id":"id3", "ttype":"flashonly", '
             '"a":"click"}\n'
             '{"sts":19800, "tai":"t1", "id":"id3", "ttype":"flashonly", '
             '"a":"click", "imgs":"http://monkey.com"}\n'
             '{"ttype":"flashonly", "a":"load", "tai":"t1", "id":"id3",'
             '"imgs":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":1900, "a":"load", "id":"id3", "tai":"t1", '
             '"imgs":["now.com"]}\n'
             '{"sts":19800, "ttype":"flashonly", "tai":"t1", "id":"id3",'
             '"imgs":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":19800, "tai":"t1", "id":"id3", "ttype":"flashonly", '
             '"a":"load"}\n'
             '{"sts":19800, "tai":"t1", "id":"id3", "ttype":"flashonly", '
             '"a":"load", "img":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":19800, "tai":"t1", "ttype":"flashonly", "a":"load", '
             '"id":"id3", "imgs":"a.com"}\n'
             '{"sts":19800, "a":"click", "page":"here.com", "id":"id3",'
             '"ttype":"flashonly", "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"click", "page":"here.com", '
             '"ttype":"flashonly", "tai":"t1", "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"load", "id":"id3", "page":"here.com",'
             '"ttype":"flashonly",'
             '"imgs":["http://monkey.com","poprocks.jpg","pumpkin.wow"]}\n'),
            protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['HourlyEventStatsErrors']['JSONFieldMissing'], 14)

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
           counters['HourlyEventStatsErrors']['HTML5_bc_click'], 1)
        self.assertEqual(
           counters['HourlyEventStatsErrors']['HTML5_bc_load'], 1)

class TestFilterDuplicateEvents(unittest.TestCase):
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])

    def test_latest_time_update(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([('latest', 3),
                    ('latest', 8),
                    ('latest', 1),]),
            step_type='reducer')
        self.assertItemsEqual(results, [('latest', 8)])
        self.assertEqual(counters, {})

    def test_duplicate_clicks(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'id2', 'here.jpg', 'tai1'), (5, 1)),
                    (('click', 'id2', 'here.jpg', 'tai1'), (6, 1)),
                    (('click', 'id2', 'here.jpg', 'tai1'), (5, 1)),
                    (('click', 'id2', 'there.jpg', 'tai1'), (5, 1)),
                    (('click', 'id3', 'here.jpg', 'tai1'), (5, 1)),
                    (('click', 'id2', 'here.jpg', 'test1'), (5, 1)),
                    ]),
            step_type='reducer')
        self.assertItemsEqual(results, [
            (('click', 'here.jpg', 'tai1', 5), 1),
            (('click', 'there.jpg', 'tai1', 5), 1),
            (('click', 'here.jpg', 'tai1', 5), 1), # This one has id3
            (('click', 'here.jpg', 'test1', 5), 1),
            ])
        self.assertEqual(counters, {})

    def test_duplicate_loads(self):
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('load', 'id2', 'here.jpg', 'tai1'), (5, 1)),
                    (('load', 'id2', 'here.jpg', 'tai1'), (6, 1)),
                    (('load', 'id2', 'here.jpg', 'tai1'), (5, 1)),
                    (('load', 'id2', 'there.jpg', 'tai1'), (5, 1)),
                    (('load', 'id3', 'here.jpg', 'tai1'), (5, 1)),
                    (('load', 'id2', 'here.jpg', 'test1'), (5, 1)),
                    ]),
            step_type='reducer')
        self.assertItemsEqual(results, [
            (('load', 'here.jpg', 'tai1', 5), 1),
            (('load', 'there.jpg', 'tai1', 5), 1),
            (('load', 'here.jpg', 'tai1', 5), 1), # This one has id3
            (('load', 'here.jpg', 'test1', 5), 1),
            ])
        self.assertEqual(counters, {})
        

class TestIDMapping(unittest.TestCase):
    '''Tests for mapping thumbnail urls to ids.'''
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])
        self.url_patcher = patch('stats.hourly_event_stats_mr.'
                                 'neondata.ThumbnailURLMapper'
                                 '.get_id')
        self.mock_mapper = self.url_patcher.start()

        self.tai_patcher = patch('stats.hourly_event_stats_mr.'
                                 'neondata.TrackerAccountIDMapper.'
                                 'get_neon_account_id')
        self.account_mapper = self.tai_patcher.start()

    def tearDown(self):
        self.url_patcher.stop()
        self.tai_patcher.stop()

    def test_valid_mapping(self):
        self.mock_mapper.return_value = "54s9dfewgvw9e8g9"
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.PRODUCTION)
        
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tai1', 94), 3)]),
            step=2)

        self.assertEqual(self.mock_mapper.call_count, 1)
        self.mock_mapper.assert_called_with('http://first.jpg')
        self.assertEqual(results[0], (('click', '54s9dfewgvw9e8g9', 94), 3))

    def test_mapping_too_short(self):
        self.mock_mapper.side_effect = "54s9"
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.PRODUCTION)
        
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tai1', 94), 3)]),
            step=2)

        self.assertEqual(results, [])
        self.assertEqual(self.mock_mapper.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['ThumbIDTooShort'], 1)

    def test_no_thumb_mapping(self):
        self.mock_mapper.side_effect = [None]
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.PRODUCTION)
        
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tai1', 94), 3)]),
            step=2)

        self.assertEqual(results, [])
        self.assertEqual(self.mock_mapper.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['UnknownThumbnailURL'], 1)

    def test_tid_end_of_bc_url(self):
        self.mock_mapper.side_effect = [None, None]
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.PRODUCTION)

        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://brightcove.vo.llnwd.net/d21/unsecured/media/1079349493/201404/3085/1079349493_3457310978001_neontnsdf434g-3442191308001-51c85b5b15c087c75a1406df90a9a752.jpg', 'tai1', 94), 3),
                    (('load', 'https://brightcove.vo.llnwd.net/d21/unsecured/media/1079349493/201404/3085/1079349493_3457310978001_neontnsdf434g-3442191308001-51c85b5b15c087c75a1406df90a9a752.jpg', 'tai1', 87), 1)]),
            step=2)

        self.assertItemsEqual(
            results, 
            [(('click', 'sdf434g_3442191308001_51c85b5b15c087c75a1406df90a9a752', 94), 3),
             (('load', 'sdf434g_3442191308001_51c85b5b15c087c75a1406df90a9a752', 87), 1)])

    @patch('stats.hourly_event_stats_mr.neondata.ThumbnailMetadata.get_many')
    @patch('stats.hourly_event_stats_mr.neondata.VideoMetadata.get')
    def test_old_bcove_url_format(self, video_mock, thumb_mock):
        self.mock_mapper.side_effect = [None, None]
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.PRODUCTION)
        video_mock.return_value = \
            neondata.VideoMetadata('acct1_3449463059001',
                                   ['3449463059001_t1', '3449463059001_t2'])
        thumb_mock.return_value = [
            neondata.ThumbnailMetadata('3449463059001_t1',
                                       'acct1_3449463059001',
                                       ttype='brightcove'),
            neondata.ThumbnailMetadata('3449463059001_t2',
                                       'acct1_3449463059001',
                                       ttype='neon', chosen=True)]

        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://brightcove.vo.llnwd.net/d21/unsecured/media/1079349493/201404/2085/1079349493_3457310977001_neonthumbnail-3449463059001.jpg', 'tai1', 94), 3),
                    (('load', 'http://brightcove.vo.llnwd.net/d21/unsecured/media/1105443290001/201404/3122/1105443290001_3449709878001_neonthumbnailbc-3449463059001.jpg', 'tai1', 87), 1)]),
            step=2)

        video_mock.assert_any_call('acct1_3449463059001')
        self.assertEqual(video_mock.call_count, 2)
        thumb_mock.assert_any_call(['3449463059001_t1', '3449463059001_t2'])
        self.assertEqual(thumb_mock.call_count, 2)

        self.assertItemsEqual(
            results, 
            [(('click', '3449463059001_t2', 94), 3),
             (('load', '3449463059001_t1', 87), 1)])

    @patch('stats.hourly_event_stats_mr.neondata.VideoMetadata.get')
    def test_old_bcove_url_format_unknown_vid(self, video_mock):
        self.mock_mapper.side_effect = [None, None]
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.PRODUCTION)
        video_mock.side_effect = [None, None]

        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://brightcove.vo.llnwd.net/d21/unsecured/media/1079349493/201404/2085/1079349493_3457310977001_neonthumbnail-3449463059001.jpg', 'tai1', 94), 3),
                    (('load', 'http://brightcove.vo.llnwd.net/d21/unsecured/media/1105443290001/201404/3122/1105443290001_3449709878001_neonthumbnailbc-3449463059001.jpg', 'tai1', 87), 1)]),
            step=2)

        self.assertEqual(results, [])
        self.assertEqual(self.mock_mapper.call_count, 2)
        self.assertEqual(video_mock.call_count, 2)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['UnknownThumbnailURL'], 2)
        

    def test_thumb_url_redis_error(self):
        self.mock_mapper.side_effect = redis.exceptions.RedisError
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.PRODUCTION)
        
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tai1', 94), 3)]),
            step=2)

        self.assertEqual(results, [])
        self.assertEqual(self.mock_mapper.call_count, 1)
        self.assertEqual(counters['HourlyEventStatsErrors']['RedisErrors'], 1)

    def test_filter_staging_tracker_id(self):
        self.mock_mapper.return_value = "54s9dfewgvw9e8g9"
        self.account_mapper.return_value = (
            "acct1", neondata.TrackerAccountIDMapper.STAGING)
        
        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tai1', 94), 3)]),
            step=2)

        self.assertEqual(results, [])
        self.assertEqual(counters, {})
        self.assertEqual(self.account_mapper.call_count, 1)
        self.account_mapper.assert_called_with('tai1')

    def test_unknown_tracker_id(self):
        self.mock_mapper.return_value = "54s9dfewgvw9e8g9"
        self.account_mapper.side_effect = [None]

        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tai1', 94), 3)]),
            step=2)
        
        self.assertEqual(results, [])
        self.account_mapper.assert_called_with('tai1')
        self.assertEqual(
            counters['HourlyEventStatsErrors']['UnknownTrackerId'], 1)

    def test_tracker_id_redis_error(self):
        self.mock_mapper.return_value = "54s9dfewgvw9e8g9"
        self.account_mapper.side_effect = redis.exceptions.RedisError

        results, counters = test_utils.mr.run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 'tai1', 94), 3)]),
            step=2)
        
        self.assertEqual(results, [])
        self.account_mapper.assert_called_with('tai1')
        self.assertEqual(counters['HourlyEventStatsErrors']['RedisErrors'], 1) 

class TestDatabaseWriting(unittest.TestCase):
    '''Tests database writing step.'''
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])

        self.dbfile = tempfile.NamedTemporaryFile()

        self.db_patcher = patch('stats.hourly_event_stats_mr.sqldb.connect')
        self.dbmock = self.db_patcher.start()
        def connect2db(*args, **kwargs):
            return sqlite3.connect(self.dbfile.name)
        self.dbmock.side_effect = connect2db
        self.ramdb = connect2db()
    
    def tearDown(self):
        self.db_patcher.stop()
        self.ramdb.close()
        self.dbfile.close()

    def test_table_creation(self):
        results, counters = test_utils.mr.run_single_step(self.mr, '', step=3,
                                            step_type='reducer')
        cursor = self.ramdb.cursor()
        cursor.execute('select * from hourly_events')
        self.assertEqual(len(cursor.fetchall()), 0)
        self.assertEqual(len(cursor.description), 4)
        self.assertEqual([x[0] for x in cursor.description],
                         ['thumbnail_id', 'hour', 'loads', 'clicks'])

    def test_new_data(self):
        garb, counters = test_utils.mr.run_single_step(
            self.mr,
            encode([(('imgA', 56),(5, 'click')),
                    (('imgA', 56),(55, 'load')),
                    (('imgB', 56),(9, 'click')),
                    (('imgA', 54),(12, 'load')),
                    ('latest', 201600)]),
            step=3,
            step_type='reducer')
        cursor = self.ramdb.cursor()
        cursor.execute('select thumbnail_id, hour, loads, clicks '
                       'from hourly_events')
        results = {}
        for data in cursor.fetchall():
            results[(data[0], data[1])] = (data[2], data[3])

        self.assertEqual(len(results.items()), 3)
        self.assertEqual(results[('imgA', hr2str(56))], (55, 5))
        self.assertEqual(results[('imgA', hr2str(54))], (12, 0))
        self.assertEqual(results[('imgB', hr2str(56))], (0, 9))

        cursor.execute('select logtime from last_update where '
                       'tablename = "hourly_events"')
        self.assertEqual(cursor.fetchone()[0], hr2str(56))

    def test_replace_data(self):
        '''The default option replaces instead of increments.'''
        test_utils.mr.run_single_step(self.mr,
                        encode([(('imgA', 56),(5, 'click')),
                                (('imgA', 56),(55, 'load')),
                                (('imgB', 56),(9, 'click')),
                                (('imgA', 54),(12, 'load')),
                                ('latest', 201600)]),
                                step=3,
                                step_type='reducer')
        test_utils.mr.run_single_step(self.mr,
                        encode([(('imgA', 56),(10, 'click')),
                                (('imgA', 56),(75, 'load')),
                                (('imgB', 56),(9, 'click')),
                                (('imgA', 54),(12, 'load')),
                                ('latest', 201605)]),
                                step=3,
                                step_type='reducer')
        cursor = self.ramdb.cursor()
        cursor.execute('select thumbnail_id, hour, loads, clicks from '
                       'hourly_events')
        results = {}
        for data in cursor.fetchall():
            results[(data[0], data[1])] = (data[2], data[3])

        self.assertEqual(len(results.items()), 3)
        self.assertEqual(results[('imgA', hr2str(56))], (75, 10))
        self.assertEqual(results[('imgA', hr2str(54))], (12, 0))
        self.assertEqual(results[('imgB', hr2str(56))], (0, 9))

        cursor.execute('select logtime from last_update where '
                       'tablename = "hourly_events"')
        self.assertEqual(cursor.fetchone()[0], sec2str(201605))

    def test_increment_data(self):
        '''Test when the counts are incremented.'''
        self.mr.options.increment_stats = 1
            
        test_utils.mr.run_single_step(self.mr,
                        encode([(('imgA', 56),(5, 'click')),
                                (('imgA', 56),(55, 'load')),
                                (('imgB', 56),(9, 'click')),
                                (('imgA', 54),(12, 'load'))]),
                                step=3,
                                step_type='reducer')
        test_utils.mr.run_single_step(self.mr,
                        encode([(('imgA', 56),(2, 'click')),
                                (('imgA', 56),(10, 'load')),
                                (('imgA', 59),(16, 'load'))]),
                                step=3,
                                step_type='reducer')
        cursor = self.ramdb.cursor()
        cursor.execute('select thumbnail_id, hour, loads, clicks from '
                       'hourly_events')
        results = {}
        for data in cursor.fetchall():
            results[(data[0], data[1])] = (data[2], data[3])

        self.assertEqual(len(results.items()), 4)
        self.assertEqual(results[('imgA', hr2str(56))], (65, 7))
        self.assertEqual(results[('imgA', hr2str(54))], (12, 0))
        self.assertEqual(results[('imgB', hr2str(56))], (0, 9))
        self.assertEqual(results[('imgA', hr2str(59))], (16, 0))

    def test_connection_error(self):
        self.dbmock.side_effect = [
            stats.hourly_event_stats_mr.sqldb.Error('yikes')]
        self.assertRaises(stats.hourly_event_stats_mr.sqldb.Error,
                          test_utils.mr.run_single_step,
                          self.mr, '', 'reducer', 3)


class TestEndToEnd(unittest.TestCase):
    '''Tests database writing step.'''
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])

        self.url_patcher = patch('stats.hourly_event_stats_mr.'
                                 'neondata.ThumbnailURLMapper'
                                 '.get_id')
        self.mock_urlmapper = self.url_patcher.start()
        self.tai_patcher = patch('stats.hourly_event_stats_mr.'
                                 'neondata.TrackerAccountIDMapper.'
                                 'get_neon_account_id')
        self.account_mapper = self.tai_patcher.start()
        
        self.dbfile = tempfile.NamedTemporaryFile()
        self.db_patcher = patch('stats.hourly_event_stats_mr.sqldb.connect')
        dbmock = self.db_patcher.start()
        def connect2db(*args, **kwargs):
            return sqlite3.connect(self.dbfile.name)
        dbmock.side_effect = connect2db
        self.ramdb = connect2db()
        

    def tearDown(self):
        self.url_patcher.stop()
        self.tai_patcher.stop()
        self.db_patcher.stop()
        self.ramdb.close()
        self.dbfile.close()

    def test_bunch_of_data(self):
        # Setup the input data
        input_data = (
            '{"sts":19800, "a":"click", "page":"here.com", "tai":"tai_prod",'
            '"ttype":"flashonly", "id":"id1", "img":"http://monkey.com"}\n'
            '{"sts":19795, "a":"load", "id":"id1", "ttype":"imagetracker", '
            '"tai":"tai_prod",'
            '"imgs":["http://monkey.com","http://panda.com","pumpkin.wow"]}\n'
            '{"sts":19810, "a":"click", "page":"here.com", "tai":"tai_prod",'
            '"ttype":"flashonly", "id":"id1", "img":"http://monkey.com"}\n'
            '{"sts":19798, "a":"load", "id":"id1", "ttype":"flashonly", '
            '"tai":"tai_prod",'
            '"imgs":["http://panda.com"]}\n'
            '{"sts":19805, "a":"click", "page":"here.com", "tai":"tai_prod",'
             '"ttype":"flashonly", "id":"id1", "img":"http://panda.com"}\n'
            '{"sts":19800, "a":"load", "id":"id2", "page":"here.com", '
            '"tai":"tai_prod", "ttype":"flashonly", '
            '"imgs":["http://monkey.com","pumpkin.jpg"]}\n'
            '{"sts":19806, "a":"load", "id":"id3", "ttype":"flashonly", '
            '"tai":"tai_prod",'
            '"imgs":["http://monkey.com","http://panda.com","pumpkin.wow"]}\n'
            '{"sts":19808, "a":"load", "id":"id4", "ttype":"flashonly", '
            '"tai":"tai_stage",'
            '"imgs":["http://monkey.com","http://panda.com","pumpkin.wow"]}\n'
            '{"sts":19810, "a":"click", "page":"here.com", "tai":"tai_prod",'
             '"ttype":"flashonly", "id":"id3", "img":"http://panda.com"}\n'
            '{"sts":19810, "a":"click", "page":"here.com", "tai":"tai_prod",'
             '"ttype":"flashonly", "id":"id2", "img":"pumpkin.jpg"}\n'
            '{"sts":19820, "a":"click", "page":"here.com", "tai":"tai_stage",'
            '"ttype":"flashonly", "id":"id4", "img":"http://monkey.com"}\n')
        stdin = StringIO(input_data)
        self.mr.sandbox(stdin=stdin)

        # Mock out the responses for converting urls to thumbnail ids
        tid_map = {
            "http://monkey.com": "49a8efg1ea98",
            "http://panda.com": "2348598ewsfrwe",
            "pumpkin.wow": "68367sgdhs",
            "pumpkin.jpg": "faefr42345dsfg"
        }
        self.mock_urlmapper.side_effect = lambda url: tid_map[url]

        # Mock out the responses for getting the account id from tracker id
        tai_map = {
            "tai_prod": ("account1",
                         neondata.TrackerAccountIDMapper.PRODUCTION),
            "tai_stage": ("account1",
                          neondata.TrackerAccountIDMapper.STAGING),
            }
        self.account_mapper.side_effect = lambda tai: tai_map[tai]
        

        # Run the map reduce job
        runner = self.mr.make_runner()
        runner.run()
        
        self.assertGreater(self.mock_urlmapper.call_count, 0)
        self.assertGreater(self.account_mapper.call_count, 0)
        self.assertGreater(MySQLdb.connect.call_count, 0)

        # Finally, check the database to make sure it says what we want
        cursor = self.ramdb.cursor()
        cursor.execute('select thumbnail_id, hour, loads, clicks from '
                       'hourly_events')
        results = {}
        for data in cursor.fetchall():
            results[(data[0], data[1])] = (data[2], data[3])

        self.assertEqual(len(results.items()), 4)
        self.assertEqual(results[('49a8efg1ea98', hr2str(5))], (3, 1))
        self.assertEqual(results[('2348598ewsfrwe', hr2str(5))], (2, 2))
        self.assertEqual(results[('68367sgdhs', hr2str(5))], (2, 0))
        self.assertEqual(results[('faefr42345dsfg', hr2str(5))], (1, 1))

        cursor.execute('select logtime from last_update where '
                       'tablename = "hourly_events"')
        self.assertEqual(cursor.fetchone()[0], sec2str(19820))

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
