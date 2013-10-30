#!/usr/bin/env python
'''
Unittests for portions of the mastermind server.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
import mastermind.server

from datetime import datetime
import mastermind.core
from mock import MagicMock
import mock
import mysql.connector
import sqlite3
from StringIO import StringIO
from supportServices import neondata
import tornado.web
import unittest

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

class TestVideoDBWatcher(unittest.TestCase):
    def setUp(self):
        self.mastermind = MagicMock()
        self.ab_manager = MagicMock()
        self.watcher = mastermind.server.VideoDBWatcher(self.mastermind,
                                                        self.ab_manager)

        # TODO(mdesnoyer) Write this test once the interface to the
        # video db is better defined
    def test_good_db_data(self):
        pass

class TestStatsDBWatcher(unittest.TestCase):
    def setUp(self):
        self.mastermind = MagicMock()
        self.ab_manager = MagicMock()
        self.watcher = mastermind.server.StatsDBWatcher(self.mastermind,
                                                        self.ab_manager)

        self.tid_mapper = MagicMock()
        self.mod_get_id = neondata.ThumbnailIDMapper.get_id
        neondata.ThumbnailIDMapper.get_id = self.tid_mapper

        self.server_log = mastermind.server._log
        mastermind.server._log = MagicMock()

        self.dbconnect = mysql.connector.connect
        dbmock = MagicMock()
        def connect2db(*args, **kwargs):
            return sqlite3.connect('file::memory:?cache=shared')
        dbmock.side_effect = connect2db
        mysql.connector.connect = dbmock
        self.ramdb = connect2db()

        cursor = self.ramdb.cursor()
        cursor.execute('''CREATE TABLE hourly_events (
                       thumbnail_id VARCHAR(32) NOT NULL,
                       hour DATETIME NOT NULL,
                       loads INT NOT NULL DEFAULT 0,
                       clicks INT NOT NULL DEFAULT 0,
                       UNIQUE (thumbnail_id, hour))''')
        cursor.execute('''CREATE TABLE last_update (
                       tablename VARCHAR(256) NOT NULL UNIQUE,
                       logtime DATETIME)''')
        self.ramdb.commit()

    def tearDown(self):
        neondata.ThumbnailIDMapper.get_id = self.mod_get_id
        mastermind.server._log = self.server_log
        mysql.connector.connect = self.dbconnect
        try:
            cursor = self.ramdb.cursor()
            cursor.execute('drop table hourly_events')
            cursor.execute('drop table last_update')
            self.ramdb.commit()
        except Exception as e:
            pass
        self.ramdb.close()
        os.remove('file::memory:?cache=shared')

    def test_working_db(self):
        # Always say that the thumbnail id is part of the same video
        self.tid_mapper.side_effect = lambda x: neondata.ThumbnailIDMapper(
            '', 'videoA', None)

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
        mysql.connector.connect = MagicMock(
            side_effect=mysql.connector.Error('some error'))

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
    unittest.main()
        
        
