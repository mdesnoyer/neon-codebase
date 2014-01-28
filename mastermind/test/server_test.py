#!/usr/bin/env python
'''
Unittests for portions of the mastermind server.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
import mastermind.server

from datetime import datetime
import logging
import mastermind.core
from mock import MagicMock, patch
import mock
import MySQLdb
import redis
import sqlite3
import stats.db
from StringIO import StringIO
from supportServices import neondata
import test_utils.neontest
import test_utils.redis
import tornado.web
import unittest
import utils.neon

class TestTornadoHandlers(unittest.TestCase):
    def setUp(self):
        self.mastermind = MagicMock()
        self.ab_manager = MagicMock()
        self.request = MagicMock()
        
        self.handler = mastermind.server.ApplyDelta(
            tornado.web.Application([]),
            self.request,
            mastermind=self.mastermind,
            ab_manager=self.ab_manager)
        self.handler.finish = MagicMock()

        self.server_log = mastermind.server._log
        mastermind.server._log = MagicMock()

    def tearDown(self):
        mastermind.server._log = self.server_log 

    def test_apply_delta_post_valid_data(self):
        self.mastermind.incorporate_delta_stats.side_effect = \
          ["new directive", None]
        self.request.body = StringIO(
            '{"d": [1569, "videoA", "thumbA", 2, 1]}\n'
            '{"d": [1575, "videoA", "thumbB", 5, 0]}')

        self.handler.post()

        self.assertEqual(self.mastermind.incorporate_delta_stats.call_count, 2)
        cargs, kwargs = self.mastermind.incorporate_delta_stats.call_args
        self.assertEqual(cargs, (1575, "videoA", "thumbB", 5, 0))

        self.assertEqual(self.ab_manager.send.call_count, 1)
        cargs, kwargs = self.ab_manager.send.call_args
        self.assertEqual(cargs, ("new directive",))

    def test_apply_delta_post_invalid_data(self):
        self.request.body = StringIO(
            '[1569, "videoA", "thumbA", 2, 1]\n'
            '{"e": [1575, "videoA", "thumbB", 5, 0]}')

        self.handler.post()

        # Nothing changes, but something should be logged for each error
        self.assertEqual(len(mastermind.server._log.mock_calls), 2)
        
        self.assertEqual(self.mastermind.incorporate_delta_stats.call_count, 0)
        self.assertEqual(self.ab_manager.send.call_count, 0)

class TestGetDirectives(unittest.TestCase):
    def setUp(self):
        self.mastermind = MagicMock()
        self.ab_manager = MagicMock()
        self.request = MagicMock()
        
        self.handler = mastermind.server.GetDirectives(
            tornado.web.Application([]),
            self.request,
            mastermind=self.mastermind,
            ab_manager=self.ab_manager)
        self.handler.finish = MagicMock()
        self.handler.flush = MagicMock()

        self.server_log = mastermind.server._log
        mastermind.server._log = MagicMock()

        self.output_stream = StringIO()
        self.handler.write = lambda x: self.output_stream.write(x)

    def tearDown(self):
        mastermind.server._log = self.server_log

    def set_arguments(self, **kwargs):
        self.args = kwargs
        self.handler.get_argument = MagicMock(
            side_effect=lambda x, y: self.get_argument(x, y))

    def get_argument(self, name, default=None):
        try:
            return self.args[name]
        except KeyError:
            return default

    def test_get_directives_single_vid(self):
        self.set_arguments(vid='videoA')
        directive = ('videoA', [('thumb1', 0.2), ('thumb2', 0.8)])
        self.mastermind.get_directives.side_effect = [[directive]]

        self.handler.get()

        # Make sure the change was pushed to the controllers
        self.assertEqual(self.ab_manager.send.call_count, 1)
        cargs, kwargs = self.ab_manager.send.call_args
        self.assertEqual(cargs, (directive,))

        self.assertEqual(
            self.output_stream.getvalue(),
            '{"d": ["videoA", [["thumb1", 0.2], ["thumb2", 0.8]]]}')

    def test_get_directives_no_push(self):
        self.set_arguments(vid='videoA', push=False)
        directive = ('videoA', [('thumb1', 0.2), ('thumb2', 0.8)])
        self.mastermind.get_directives.side_effect = [[directive]]

        self.handler.get()

        # Make sure the change was not pushed to the controllers
        self.assertEqual(self.ab_manager.send.call_count, 0)

        self.assertEqual(
            self.output_stream.getvalue(),
            '{"d": ["videoA", [["thumb1", 0.2], ["thumb2", 0.8]]]}')

    def test_get_all_directives(self):
        self.set_arguments()
        directives = [
            ('videoA', [('thumb1', 0.2), ('thumb2', 0.8)]),
            ('videoB', [('b1', 1.0), ('b2', 0.0)]),
            ('videoC', [('c1', 0.4), ('c2', 0.6)])]
        self.mastermind.get_directives.side_effect = [directives]

        self.handler.get()

        self.assertEqual(self.ab_manager.send.call_count, 3)

        self.assertEqual(
            self.output_stream.getvalue(),
            '{"d": ["videoA", [["thumb1", 0.2], ["thumb2", 0.8]]]}\n'
            '{"d": ["videoB", [["b1", 1.0], ["b2", 0.0]]]}\n'
            '{"d": ["videoC", [["c1", 0.4], ["c2", 0.6]]]}')

@patch('mastermind.server.neondata')
class TestVideoDBWatcher(test_utils.neontest.TestCase):
    def setUp(self):
        self.mastermind = mastermind.core.Mastermind()
        self.ab_manager = MagicMock()
        self.watcher = mastermind.server.VideoDBWatcher(self.mastermind,
                                                        self.ab_manager)
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

    def tearDown(self):
        self.redis.stop()

    def test_good_db_data(self, datamock):
        # Define platforms in the database

        #create neon user account 
        nuser = neondata.NeonUserAccount('a1')
        api_key = nuser.neon_api_key

        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', api_key, 
                                        abtest=False)
        bcPlatform.add_video(0, 'job11')
        bcPlatform.add_video(10, 'job12')

        testPlatform = neondata.BrightcovePlatform('a2', 'i2', api_key, 
                                        abtest=True)
        testPlatform.add_video(1, 'job21')
        testPlatform.add_video(2, 'job22')

        apiPlatform = neondata.NeonPlatform('a3', api_key, abtest=True)
        apiPlatform.add_video(4, 'job31')

        noVidPlatform = neondata.BrightcovePlatform('a4', 'i4', api_key, 
                                        abtest=True)
        
        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform, testPlatform, apiPlatform, noVidPlatform]

        # Define the video meta data
        vid_meta = {
            '0': neondata.VideoMetadata(0,['t01','t02','t03'],'','','','','',''),
            '10': neondata.VideoMetadata(0,[],'','','','','',''),
            '1': neondata.VideoMetadata(0,['t11'],'','','','','',''),
            '2': neondata.VideoMetadata(0,['t21','t22'],'','','','','',''),
            '4': neondata.VideoMetadata(0,['t41', 't42'],'','','','','','')
            }
        datamock.VideoMetadata.get.side_effect = lambda vid: vid_meta[vid]

        # Define the thumbnail meta data
        TMD = neondata.ThumbnailMetaData
        tid_meta = {
            't01': TMD('t01', 0,0,0,0,'brightcove',0,0,True,False,0),
            't02': TMD('t02', 0,0,0,0,'neon',0,0,True,True,0),
            't03': TMD('t03', 0,0,0,0,'neon',1,0,True,False,0),
            't11': TMD('t11', 0,0,0,0,'brightcove',0,0,True,False,0),
            't21': TMD('t21', 0,0,0,0,'brightcove',0,0,True,False,0),
            't22': TMD('t22', 0,0,0,0,'neon',0,0,True,True,0),
            't41': TMD('t41', 0,0,0,0,'neon',0,0,True,False,0),
            't42': TMD('t42', 0,0,0,0,'neon',1,0,True,False,0),
            }
        datamock.ThumbnailIDMapper.get_thumb_metadata.side_effect = \
          lambda tid: tid_meta[tid]

        # Process the data
        self.watcher._process_db_data()

        # The order of the thumbnails in the directive doesn't matter,
        # but there isn't an easy way to do recursive
        # assertItemsEqual, so I'm hard coding the order.
        expected = [
            mock.call(('0', [('t03', 0.0), ('t02', 1.0), ('t01', 0.0)])),
            mock.call(('10', [])),
            mock.call(('1', [('t11', 1.0)])),
            mock.call(('2', [('t21', 0.15), ('t22', 0.85)])),
            mock.call(('4', [('t42', 0.0), ('t41', 1.0)]))]
        self.assertEqual(self.ab_manager.send.call_count, 5)
        self.maxDiff = 700
        self.assertItemsEqual(expected, self.ab_manager.send.call_args_list)

        self.assertTrue(self.watcher.is_loaded.is_set())

    def test_connection_error(self, datamock):
        datamock.AbstractPlatform.get_all_instances.side_effect = \
          redis.ConnectionError

        with self.assertRaises(redis.ConnectionError):
            self.watcher._process_db_data()

    def test_video_metadata_missing(self, datamock):
        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', 'api_key', 
                abtest=True)
        bcPlatform.add_video(0, 'job11')
        bcPlatform.add_video(10, 'job12')
        
        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform]
        datamock.VideoMetadata.get.return_value = None

        with self.assertLogExists(logging.ERROR,
                                  'Could not find information about video 10'):
            with self.assertLogExists(logging.ERROR,
                                      'Could not find information about '
                                      'video 0'):
                self.watcher._process_db_data()
        
        self.assertTrue(self.watcher.is_loaded.is_set())

    def test_thumb_metadata_missing(self, datamock):
        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', 'api_key',  
                abtest=True)
        bcPlatform.add_video(0, 'job11')
        bcPlatform.add_video(10, 'job12')
        
        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform]

        vid_meta = {
            '0': neondata.VideoMetadata(0,['t01','t02','t03'],'','','','','',''),
            '10': neondata.VideoMetadata(0,[],'','','','','','')
            }
        datamock.VideoMetadata.get.side_effect = lambda vid: vid_meta[vid]

        TMD = neondata.ThumbnailMetaData
        tid_meta = {
            't01': TMD('t01', 0,0,0,0,'brightcove',0,0,True,False,0),
            't02': TMD('t02', 0,0,0,0,'neon',0,0,True,False,0),
            't03': None,
            }
        datamock.ThumbnailIDMapper.get_thumb_metadata.side_effect = \
          lambda tid: tid_meta[tid]

        with self.assertLogExists(logging.ERROR,
                                  'Could not find metadata for thumb t03'):
            self.watcher._process_db_data()

        # Make sure that there is a directive sent about the other
        # video in the account.
        self.assertEqual(self.ab_manager.send.call_count, 1)
        self.ab_manager.send.assert_called_with(('10', []))

        # Make sure that the processing gets flagged as done
        self.assertTrue(self.watcher.is_loaded.is_set())

class TestStatsDBWatcher(unittest.TestCase):
    def setUp(self):
        self.mastermind = MagicMock()
        self.ab_manager = MagicMock()
        self.watcher = mastermind.server.StatsDBWatcher(self.mastermind,
                                                        self.ab_manager)

        self.tid_mapper = MagicMock()
        self.mod_get_video_id = neondata.ThumbnailIDMapper.get_video_id
        neondata.ThumbnailIDMapper.get_video_id = self.tid_mapper

        self.server_log = mastermind.server._log
        mastermind.server._log = MagicMock()

        self.dbconnect = MySQLdb.connect
        dbmock = MagicMock()
        def connect2db(*args, **kwargs):
            return sqlite3.connect('file::memory:?cache=shared')
        dbmock.side_effect = connect2db
        MySQLdb.connect = dbmock
        self.ramdb = connect2db()

        cursor = self.ramdb.cursor()
        stats.db.create_tables(cursor)
        self.ramdb.commit()

    def tearDown(self):
        neondata.ThumbnailIDMapper.get_video_id = self.mod_get_video_id
        mastermind.server._log = self.server_log
        MySQLdb.connect = self.dbconnect
        try:
            cursor = self.ramdb.cursor()
            cursor.execute('drop table hourly_events')
            cursor.execute('drop table last_update')
            self.ramdb.commit()
        except Exception as e:
            pass
        self.ramdb.close()
        f = 'file::memory:?cache=shared'
        if os.path.exists(f):
            os.remove(f)

    def test_working_db(self):
        # Always say that the thumbnail id is part of the same video
        self.tid_mapper.side_effect = lambda x: 'videoA'

        # Mastermind will first return a list of directives and then no change
        self.mastermind.update_stats_info.side_effect = [
            ["directive1", "directive2"],
            []]

        # Initialize the database
        cursor = self.ramdb.cursor()
        cursor.executemany('''REPLACE INTO hourly_events 
        (thumbnail_id, hour, loads, clicks) VALUES (?,?,?,?)''',
        [('thumbA', datetime(1980, 3, 20, 6), 300, 10),
         ('thumbB', datetime(1980, 3, 20, 6), 10, 0),
         ('thumbB', datetime(1980, 3, 20, 5), 600, 30)])
        cursor.execute('''REPLACE INTO last_update
        (tablename, logtime) VALUES ('hourly_events', '1980-3-20 6:16:00')''')
        self.ramdb.commit()

        self.watcher._process_db_data()

        # Check that mastermind was updated properly
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        cargs, kwargs = self.mastermind.update_stats_info.call_args
        self.assertEqual(cargs[0],
                         (datetime(1980, 3, 20, 6, 16) -
                          datetime(1970,1, 1)).total_seconds())
        self.assertItemsEqual(cargs[1], [('videoA', 'thumbA', 300, 10),
                                         ('videoA', 'thumbB', 610, 30)])

        # Check that data was sent because it's the first update
        self.assertEqual(self.ab_manager.send.call_count, 2)
        self.ab_manager.send.assert_has_calls([
            mock.call('directive1'), mock.call('directive2')])

        # If we check again, the database hasn't changed, so nothing
        # should be called.
        self.ab_manager.send.reset_mock()
        self.mastermind.update_stats_info.reset_mock()

        self.watcher._process_db_data()
        self.assertEqual(self.mastermind.update_stats_info.call_count, 0)
        self.assertEqual(self.ab_manager.send.call_count, 0)

        # Now change the database, but it won't cause a significant change.
        cursor.executemany('''REPLACE INTO hourly_events 
        (thumbnail_id, hour, loads, clicks) VALUES (?,?,?,?)''',
        [('thumbA', datetime(1980, 3, 20, 6), 300, 10),
         ('thumbB', datetime(1980, 3, 20, 6), 110, 3),
         ('thumbB', datetime(1980, 3, 20, 5), 600, 30)])
        cursor.execute('''REPLACE INTO last_update
        (tablename, logtime) VALUES ('hourly_events', '1980-3-20 6:27:00')''')
        self.ramdb.commit()

        self.watcher._process_db_data()

        # Check that the stats were updated
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        cargs, kwargs = self.mastermind.update_stats_info.call_args
        self.assertEqual(cargs[0],
                         (datetime(1980, 3, 20, 6, 27) -
                          datetime(1970,1, 1)).total_seconds())
        self.assertItemsEqual(cargs[1], [('videoA', 'thumbA', 300, 10),
                                         ('videoA', 'thumbB', 710, 33)])

        # Make sure no update was pushed
        self.assertEqual(self.ab_manager.send.call_count, 0)

    def test_stat_db_connection_error(self):
        MySQLdb.connect = MagicMock(
            side_effect=MySQLdb.Error('some error'))

        self.watcher._process_db_data()

        self.assertEqual(len(mastermind.server._log.exception.mock_calls), 1)

    def test_stats_db_no_update_time(self):
        cursor = self.ramdb.cursor()
        cursor.executemany('''REPLACE INTO hourly_events 
        (thumbnail_id, hour, loads, clicks) VALUES (?,?,?,?)''',
        [('thumbA', datetime(1980, 3, 20, 6), 300, 10),
         ('thumbB', datetime(1980, 3, 20, 6), 10, 0),
         ('thumbB', datetime(1980, 3, 20, 5), 600, 30)])
        self.ramdb.commit()

        self.watcher._process_db_data()

        # Make sure than an error was logged
        self.assertEqual(len(mastermind.server._log.error.mock_calls), 1)
        

        
if __name__ == '__main__':
    utils.neon.InitNeonTest()
    unittest.main()
        
        
