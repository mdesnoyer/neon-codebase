#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from datetime import datetime
from mock import MagicMock
from mrjob.protocol import *
import MySQLdb
import os
from StringIO import StringIO
import sqlite3
import tempfile
import unittest
import urllib2
import utils.neon
from utils.options import define, options

from hourly_event_stats_mr import *

### Helper functions
def encode(entries, protocol=JSONProtocol):
    '''Encodes a list of key value pairs into the mrjob protocol.'''
    encoder = protocol()
    return '\n'.join([encoder.write(key, value) for key, value in entries])

def run_single_step(mr, input_str, step_type='mapper', step=0,
                    protocol=JSONProtocol):
    '''Runs a single step and returns the results.

    Inputs:
    mr - Map reduce job
    input_str - stdin input string to process
    step - Step to run
    step_type - 'mapper' or 'reducer'
    protocol - Protocole that the input data was encoded as
    
    Outputs: ([(key, value)], counters)
    '''
    results = []
    counters = {}

    stdin = StringIO(input_str)
    mr.sandbox(stdin=stdin)
    if step_type == 'mapper':
        if step == 0:
            mr.INPUT_PROTOCOL = protocol
        else:
            mr.INTERNAL_PROTOCOL = protocol
        mr.run_mapper(step)
        return (mr.parse_output(mr.INTERNAL_PROTOCOL()),
                mr.parse_counters())
    elif step_type == 'reducer':
        mr.INTERNAL_PROTOCOL = protocol
        mr.run_reducer(step)
        if step == len(mr.steps()) - 1:
            return (mr.parse_output(mr.OUTPUT_PROTOCOL()),
                    mr.parse_counters())
        else:
            return (mr.parse_output(mr.INTERNAL_PROTOCOL()),
                    mr.parse_counters())

def hr2str(hr):
    return datetime.utcfromtimestamp(hr*3600).isoformat(' ')

def sec2str(sec):
    return datetime.utcfromtimestamp(sec).isoformat(' ')

class TestDataParsing(unittest.TestCase):
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])

    def test_valid_click(self):
        results, counters = run_single_step(self.mr,
            ('{"sts":19800, "a":"click", '
             '"ttype":"flashonly", "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"click", '
             '"ttype":"flashonly", "img":"http://panda.com"}\n'),
            protocol=RawProtocol)
        self.assertItemsEqual(results,
                              [(('click', 'http://monkey.com', 5), 1),
                              (('click', 'http://panda.com', 5), 1),
                              ('latest', 19800),
                              ('latest', 19800)])

    def test_valid_load(self):
        results, counters = run_single_step(self.mr,
          ('{"sts":19800, "a":"load", "ttype":"flashonly",'
           '"imgs":["http://monkey.com","poprocks.jpg","pumpkin.wow"]}'),
            protocol=RawProtocol)
                                            
        self.assertItemsEqual(results,
                              [(('load', 'http://monkey.com', 5), 1),
                               (('load', 'poprocks.jpg', 5), 1),
                               (('load', 'pumpkin.wow', 5), 1),
                               ('latest', 19800)])

    def test_invalid_json(self):
        results, counters = run_single_step(self.mr,
            ('{"sts":19800, "a":"click" "img":"http://monkey.com"}\n'
            '{"sts":1900, "a":"click", "img":"http://monkey.com"\n'
            '{"sts":1900, "a":"load", "imgs":["now.com"}\n'),
            step=0, protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['HourlyEventStatsErrors']['JSONParseErrors'], 3)

    def test_fields_missing(self):
        results, counters = run_single_step(self.mr,
            ('{"a":"click", "ttype":"flashonly", "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"click", "img":"http://monkey.com"}\n'
             '{"sts":19800, "ttype":"flashonly", "img":"http://monkey.com"}\n'
             '{"sts":19800, "ttype":"flashonly", "a":"click"}\n'
             '{"sts":19800, "ttype":"flashonly", "a":"click",'
             '"imgs":"http://monkey.com"}\n'
             '{"ttype":"flashonly", "a":"load", '
             '"imgs":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":1900, "a":"load", "imgs":["now.com"]}\n'
             '{"sts":19800, "ttype":"flashonly",'
             '"imgs":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":19800, "ttype":"flashonly", "a":"load"}\n'
             '{"sts":19800, "ttype":"flashonly", "a":"load", '
             '"img":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":19800,"ttype":"flashonly","a":"load","imgs":"a.com"}\n'),
            protocol=RawProtocol)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['HourlyEventStatsErrors']['JSONFieldMissing'], 11)

    def test_html5_player(self):
        '''We need to ignore entries for the html5 player.

        It can't track loads properly.
        '''
        results, counters = run_single_step(self.mr,
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

class TestIDMapping(unittest.TestCase):
    '''Tests for mapping thumbnail urls to ids.'''
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])
        self.urlopen = urllib2.urlopen

    def tearDown(self):
        urllib2.urlopen = self.urlopen

    def test_valid_mapping(self):
        mock = MagicMock(side_effect=[StringIO('{"tid":"54s9dfewgvw9e8g9"}')])
        urllib2.urlopen = mock
        results, counters = run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 94), 3)]),
            step=1)

        self.assertEqual(mock.call_count, 1)
        cargs, kwargs = mock.call_args
        self.assertEqual(cargs[1], 'url=http%3A%2F%2Ffirst.jpg')
        self.assertEqual(results[0], (['click', '54s9dfewgvw9e8g9', 94], 3))

    def test_mapping_too_short(self):
        mock = MagicMock(side_effect=[StringIO('{"tid":"54s9"}')])
        urllib2.urlopen = mock
        results, counters = run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 94), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['TIDParseError'], 1)

    def test_no_thumb_mapping(self):
        mock = MagicMock(side_effect=[StringIO('{"tid":null}')])
        urllib2.urlopen = mock
        results, counters = run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 94), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['UnknownThumbnailURL'], 1)

    def test_no_tid_field(self):
        mock = MagicMock(side_effect=[StringIO('{"fid":"feswfefs9"}')])
        urllib2.urlopen = mock
        results, counters = run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 94), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['TIDFieldMissing'], 1)

    def test_invalid_json_response(self):
        mock = MagicMock(side_effect=[StringIO('feswfefs9')])
        urllib2.urlopen = mock
        results, counters = run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 94), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['TIDParseError'], 1)

    def test_io_error(self):
        mock = MagicMock(side_effect=IOError)
        urllib2.urlopen = mock
        results, counters = run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 94), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['TIDParseError'], 1)

    def test_url_error(self):
        mock = MagicMock(side_effect=[urllib2.URLError("because")])
        urllib2.urlopen = mock
        results, counters = run_single_step(self.mr,
            encode([(('click', 'http://first.jpg', 94), 3)]),
            step=1)

        self.assertEqual(results, [])
        self.assertEqual(mock.call_count, 1)
        self.assertEqual(
            counters['HourlyEventStatsErrors']['VideoDBConnectionError'], 1)

    def test_latest_time(self):
        results, counters = run_single_step(self.mr,
            encode([('latest', 19800)]),
            step=1)
        self.assertEqual(results[0], ('latest', 19800))
        

class TestDatabaseWriting(unittest.TestCase):
    '''Tests database writing step.'''
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])
        
        self.dbconnect = MySQLdb.connect
        dbmock = MagicMock()
        def connect2db(*args, **kwargs):
            return sqlite3.connect('file::memory:?cache=shared')
        dbmock.side_effect = connect2db
        MySQLdb.connect = dbmock
        self.ramdb = connect2db()

    def tearDown(self):
        MySQLdb.connect = self.dbconnect
        try:
            cursor = self.ramdb.cursor()
            cursor.execute('drop table hourly_events')
            cursor.execute('drop table last_update')
            self.ramdb.commit()
        except Exception as e:
            pass
        self.ramdb.close()
        os.remove('file::memory:?cache=shared')

    def test_table_creation(self):
        results, counters = run_single_step(self.mr, '', step=2,
                                            step_type='reducer')
        cursor = self.ramdb.cursor()
        cursor.execute('select * from hourly_events')
        self.assertEqual(len(cursor.fetchall()), 0)
        self.assertEqual(len(cursor.description), 4)
        self.assertEqual([x[0] for x in cursor.description],
                         ['thumbnail_id', 'hour', 'loads', 'clicks'])

    def test_new_data(self):
        garb, counters = run_single_step(
            self.mr,
            encode([(('imgA', 56),(5, 'click')),
                    (('imgA', 56),(55, 'load')),
                    (('imgB', 56),(9, 'click')),
                    (('imgA', 54),(12, 'load')),
                    ('latest', 201600)]),
            step=2,
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
        run_single_step(self.mr,
                        encode([(('imgA', 56),(5, 'click')),
                                (('imgA', 56),(55, 'load')),
                                (('imgB', 56),(9, 'click')),
                                (('imgA', 54),(12, 'load')),
                                ('latest', 201600)]),
                                step=2,
                                step_type='reducer')
        run_single_step(self.mr,
                        encode([(('imgA', 56),(10, 'click')),
                                (('imgA', 56),(75, 'load')),
                                (('imgB', 56),(9, 'click')),
                                (('imgA', 54),(12, 'load')),
                                ('latest', 201605)]),
                                step=2,
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
        with options._set_bounded(
                'stats.hourly_event_stats_mr.increment_stats', 1):
            
            run_single_step(self.mr,
                            encode([(('imgA', 56),(5, 'click')),
                                    (('imgA', 56),(55, 'load')),
                                    (('imgB', 56),(9, 'click')),
                                    (('imgA', 54),(12, 'load'))]),
                                    step=2,
                                    step_type='reducer')
            run_single_step(self.mr,
                            encode([(('imgA', 56),(2, 'click')),
                                    (('imgA', 56),(10, 'load')),
                                    (('imgA', 59),(16, 'load'))]),
                                    step=2,
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
        MySQLdb.connect = MagicMock(
            side_effect=[MySQLdb.Error('yikes')])
        self.assertRaises(MySQLdb.Error, run_single_step,
           self.mr, '', 'reducer', 2)


class TestEndToEnd(unittest.TestCase):
    '''Tests database writing step.'''
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])
        self.urlopen = urllib2.urlopen
        self.dbconnect = MySQLdb.connect

        # For some reason, the in memory database isn't shared, so use
        # a temporary file instead. It worked in the other test case....
        self.tempfile = tempfile.NamedTemporaryFile()

        # Replace the database with an in memory one.
        dbmock = MagicMock()
        def connect2db(*args, **kwargs):
            return sqlite3.connect(self.tempfile.name)
            #return sqlite3.connect('file::memory:?cache=shared')
        dbmock.side_effect = connect2db
        MySQLdb.connect = dbmock
        self.ramdb = connect2db()
        

    def tearDown(self):
        urllib2.urlopen = self.urlopen
        MySQLdb.connect = self.dbconnect
        try:
            self.ramdb.execute('drop table hourly_events')
        except Exception as e:
            pass
        self.ramdb.close()

    def test_bunch_of_data(self):
        # Setup the input data
        input_data = (
            '{"sts":19800, "a":"click", '
            '"ttype":"flashonly", "img":"http://monkey.com"}\n'
            '{"sts":19795, "a":"load", "ttype":"flashonly",'
            '"imgs":["http://monkey.com","http://panda.com","pumpkin.wow"]}\n'
            '{"sts":19805, "a":"click", '
             '"ttype":"flashonly", "img":"http://panda.com"}\n'
            '{"sts":19800, "a":"load", "ttype":"flashonly",'
            '"imgs":["http://monkey.com","pumpkin.jpg"]}\n'
            '{"sts":19810, "a":"click", '
             '"ttype":"flashonly", "img":"http://panda.com"}\n'
            '{"sts":19810, "a":"click", '
             '"ttype":"flashonly", "img":"pumpkin.jpg"}')
        stdin = StringIO(input_data)
        self.mr.sandbox(stdin=stdin)

        # Mock out the responses for converting urls to thumbnail ids
        tid_list = [
            ("http://monkey.com", "49a8efg1ea98"),
            ("http://panda.com", "2348598ewsfrwe"),
            ("pumpkin.wow", "68367sgdhs"),
            ("pumpkin.jpg", "faefr42345dsfg")
        ]
        tid_map = dict([(urllib.urlencode({'url':x[0]}),
                         json.dumps({'tid':x[1]})) 
                        for x in tid_list])
        tid_mock = MagicMock()
        tid_mock.side_effect = lambda x, url, y: StringIO(tid_map[url])
        urllib2.urlopen = tid_mock

        # Run the map reduce job
        runner = self.mr.make_runner()
        runner.run()
        
        self.assertGreater(tid_mock.call_count, 0)
        self.assertGreater(MySQLdb.connect.call_count, 0)

        # Finally, check the database to make sure it says what we want
        cursor = self.ramdb.cursor()
        cursor.execute('select thumbnail_id, hour, loads, clicks from '
                       'hourly_events')
        results = {}
        for data in cursor.fetchall():
            results[(data[0], data[1])] = (data[2], data[3])

        self.assertEqual(len(results.items()), 4)
        self.assertEqual(results[('49a8efg1ea98', hr2str(5))], (2, 1))
        self.assertEqual(results[('2348598ewsfrwe', hr2str(5))], (1, 2))
        self.assertEqual(results[('68367sgdhs', hr2str(5))], (1, 0))
        self.assertEqual(results[('faefr42345dsfg', hr2str(5))], (1, 1))

        cursor.execute('select logtime from last_update where '
                       'tablename = "hourly_events"')
        self.assertEqual(cursor.fetchone()[0], sec2str(19810))

if __name__ == '__main__':
    utils.neon.InitNeonTest()
    unittest.main()
