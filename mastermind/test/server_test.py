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

import boto.exception
import datetime
import dateutil.parser
import fake_filesystem
import fake_tempfile
import impala.error
import json
import logging
import mastermind.core
from mock import MagicMock, patch
import mock
import redis
import sqlite3
import stats.db
from StringIO import StringIO
import socket
from supportServices import neondata
import test_utils.mock_boto_s3
import test_utils.neontest
import tornado.web
import unittest
import utils.neon

STAGING = neondata.TrackerAccountIDMapper.STAGING
PROD = neondata.TrackerAccountIDMapper.PRODUCTION

@patch('mastermind.server.neondata')
class TestVideoDBWatcher(test_utils.neontest.TestCase):
    def setUp(self):
        self.mastermind = mastermind.core.Mastermind()
        self.directive_publisher = mastermind.server.DirectivePublisher(
            self.mastermind)
        self.watcher = mastermind.server.VideoDBWatcher(
            self.mastermind,
            self.directive_publisher)

    def test_good_db_data(self, datamock):
        # Define platforms in the database

        api_key = "neonapikey"

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
            api_key + '_0': neondata.VideoMetadata(api_key + '_0',
                                                   ['t01','t02','t03']),
            api_key + '_10': neondata.VideoMetadata(api_key + '_10', []),
            api_key + '_1': neondata.VideoMetadata(api_key + '_1', ['t11']),
            api_key + '_2': neondata.VideoMetadata(api_key + '_2',
                                                   ['t21','t22']),
            api_key + '_4': neondata.VideoMetadata(api_key + '_4',
                                                   ['t41', 't42'])
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]

        # Define the thumbnail meta data
        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            't01': TMD('t01',api_key+'_0',ttype='brightcove'),
            't02': TMD('t02',api_key+'_0',ttype='neon', rank=0, chosen=True),
            't03': TMD('t03',api_key+'_0',ttype='neon', rank=1),
            't11': TMD('t11',api_key+'_1',ttype='brightcove'),
            't21': TMD('t21',api_key+'_2',ttype='centerframe'),
            't22': TMD('t22',api_key+'_2',ttype='neon', chosen=True),
            't41': TMD('t41',api_key+'_4',ttype='neon', rank=0),
            't42': TMD('t42',api_key+'_4',ttype='neon', rank=1),
            }
        datamock.ThumbnailMetadata.get_many.side_effect = \
                lambda tids: [tid_meta[tid] for tid in tids]

        # Define the serving strategy
        datamock.ExperimentStrategy.get.return_value = \
          neondata.ExperimentStrategy(api_key)

        # Process the data
        self.watcher._process_db_data()

        # Check the resulting directives
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(len(directives), 4)
        self.assertEquals(directives[(api_key, api_key+'_0')],
                          {'t01': 0.0, 't02': 1.0, 't03': 0.0})
        self.assertEquals(directives[(api_key, api_key+'_1')], {'t11': 1.0})
        self.assertEquals(directives[(api_key, api_key+'_2')],
                          {'t21': 0.01, 't22': 0.99})
        self.assertGreater(directives[(api_key, api_key+'_4')]['t41'], 0.0)
        self.assertGreater(directives[(api_key, api_key+'_4')]['t42'], 0.0)

        self.assertTrue(self.watcher.is_loaded.is_set())

    def test_serving_url_update(self, datamock):
        serving_urls = {
            't01' : { (640, 480): 't01_640.jpg',
                      (120, 90): 't01_120.jpg'},
            't02' : { (800, 600): 't02_800.jpg',
                      (120, 90): 't02_120.jpg'}}
        
        datamock.ThumbnailServingURLs.get_all.return_value = [
            neondata.ThumbnailServingURLs(k, v) for k, v in
            serving_urls.iteritems()
            ]

        # Process the data
        self.watcher._process_db_data()

        # Make sure that the serving urls were sent to the directive pusher
        self.assertEqual(serving_urls, self.directive_publisher.serving_urls)

    def test_tracker_id_update(self, datamock):
        datamock.TrackerAccountIDMapper.get_all.return_value = [
            neondata.TrackerAccountIDMapper('tai1', 'acct1', STAGING),
            neondata.TrackerAccountIDMapper('tai11', 'acct2', PROD),
            neondata.TrackerAccountIDMapper('tai2', 'acct1', PROD)
            ]
        
        # Process the data
        self.watcher._process_db_data()

        # Make sure we have the tracker account id mapping
        self.assertEqual(self.directive_publisher.tracker_id_map,
                         {'tai1': 'acct1',
                          'tai2': 'acct1',
                          'tai11': 'acct2'})
            

    def test_connection_error(self, datamock):
        datamock.ThumbnailServingURLs.get_all.side_effect = \
          redis.ConnectionError

        with self.assertRaises(redis.ConnectionError):
            self.watcher._process_db_data()

    def test_video_metadata_missing(self, datamock):
        api_key = 'apikey'
        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', api_key, 
                abtest=True)
        bcPlatform.add_video('0', 'job11')
        bcPlatform.add_video('10', 'job12')
        
        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform]
        datamock.VideoMetadata.get_many.return_value = [None, None] 

        with self.assertLogExists(
                logging.ERROR,
                'Could not find information about video apikey_0'):
            with self.assertLogExists(logging.ERROR,
                                      'Could not find information about '
                                      'video apikey_10'):
                self.watcher._process_db_data()
        
        self.assertTrue(self.watcher.is_loaded.is_set())

    def test_thumb_metadata_missing(self, datamock):
        api_key = 'apikey'
        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', api_key,  
                abtest=True)
        bcPlatform.add_video('0', 'job11')
        bcPlatform.add_video('1', 'job12')
        
        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform]

        vid_meta = {
            api_key + '_0': neondata.VideoMetadata(api_key + '_0',
                                                   ['t01','t02','t03']),
            api_key + '_1': neondata.VideoMetadata(api_key + '_1', ['t11']),
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]

        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            't01': TMD('t01',api_key+'_0',ttype='brightcove'),
            't02': TMD('t02',api_key+'_0',ttype='neon', rank=0, chosen=True),
            't03': None,
            't11': TMD('t11',api_key+'_1',ttype='brightcove'),
            }

        datamock.ThumbnailMetadata.get_many.side_effect = \
                lambda tids: [tid_meta[tid] for tid in tids]
        with self.assertLogExists(logging.ERROR,
                                  'Could not find metadata for thumb t03'):
            self.watcher._process_db_data()

        # Make sure that there is a directive about the other
        # video in the account.
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(directives[(api_key, api_key+'_1')],
                          {'t11': 1.0})
        self.assertEquals(len(directives), 1)

        # Make sure that the processing gets flagged as done
        self.assertTrue(self.watcher.is_loaded.is_set())

@patch('mastermind.server.neondata')
class TestStatsDBWatcher(test_utils.neontest.TestCase):
    def setUp(self):
        self.mastermind = MagicMock()
        self.watcher = mastermind.server.StatsDBWatcher(self.mastermind)

        def connect2db(*args, **kwargs):
            return sqlite3.connect('file::memory:?cache=shared')
        self.ramdb = connect2db()

        cursor = self.ramdb.cursor()
        # Create the necessary tables (these are subsets of the real tables)
        cursor.execute('''CREATE TABLE IF NOT EXISTS videoplays (
                       serverTime DOUBLE,
                       mnth INT,
                       yr INT)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS EventSequences (
                       thumbnail_id varchar(128),
                       imloadclienttime DOUBLE,
                       imvisclienttime DOUBLE,
                       imclickclienttime DOUBLE,
                       adplayclienttime DOUBLE,
                       videoplayclienttime DOUBLE,
                       mnth INT,
                       yr INT,
                       tai varchar(64))''')
        self.ramdb.commit()

        #patch impala connect
        self.sqlite_connect_patcher = \
          patch('mastermind.server.impala.dbapi.connect')
        self.sqllite_mock = self.sqlite_connect_patcher.start()
        self.sqllite_mock.side_effect = \
          lambda host=None, port=None: self.ramdb 

    def tearDown(self):
        self.sqlite_connect_patcher.stop()
        try:
            cursor = self.ramdb.cursor()
            cursor.execute('drop table videoplays')
            cursor.execute('drop table eventsequences')
            self.ramdb.commit()
        except Exception as e:
            pass
        self.ramdb.close()
        f = 'file::memory:?cache=shared'
        if os.path.exists(f):
            os.remove(f)

    def _get_all_stat_updates(self):
        retval = []
        for call in self.mastermind.update_stats_info.call_args_list:
            cargs, kwargs = call
            retval.extend([x for x in cargs[0]])
        return retval

    def test_working_db(self, datamock):
        # Mock out the calls to the video database
        datamock.TrackerAccountIDMapper.get_all.return_value = [
            neondata.TrackerAccountIDMapper('tai1', 'acct1', STAGING),
            neondata.TrackerAccountIDMapper('tai11', 'acct2', PROD),
            neondata.TrackerAccountIDMapper('tai2', 'acct1', PROD)
            ]
        datamock.TrackerAccountIDMapper.PRODUCTION = PROD
        datamock.TrackerAccountIDMapper.STAGING = STAGING
        datamock.MetricType = neondata.MetricType
        datamock.ExperimentStrategy.get.side_effect = \
          lambda x: neondata.ExperimentStrategy(x)

        tid_map = {'tid11' : 'vid11', 'tid12' : 'vid12', 'tid21' : 'vid21'}
        datamock.ThumbnailMetadata.get_video_id.side_effect = \
          lambda tid: tid_map[tid]

        # Add entries to the database
        cursor = self.ramdb.cursor()
        cursor.execute('''REPLACE INTO VideoPlays
        (serverTime, mnth, yr) values (1405372146.32, 6, 2033)''')
        cursor.executemany('''REPLACE INTO EventSequences
        (thumbnail_id, imvisclienttime, imclickclienttime, mnth, yr, tai)
        VALUES (?,?,?,?,?,?)''', [
            ('tid11', 1405372146, None, 6, 2033, 'tai2'),
            ('tid11', 1405372146, None, 6, 2033, 'tai2'),
            ('tid11', 1405372146, None, 6, 2033, 'tai2'),
            ('tid11', 1405372146, 1405372146, 6, 2033, 'tai2'),
            ('tid12', 1405372146, 1405372146, 6, 2033, 'tai2'),
            ('tid21', 1405372146, None, 6, 2033, 'tai11'),
            ('tid21', None, 1405372146, 6, 2033, 'tai11')])
        self.ramdb.commit()

        # Check the stats db
        with self.assertLogExists(logging.INFO, 'Polling the stats database'):
            with self.assertLogExists(logging.INFO, 'Found a newer'):
                self.watcher._process_db_data()

        # Make sure the data got to mastermind
        self.assertEqual(self.mastermind.update_stats_info.call_count, 2)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', 4, 1),
                               ('vid12', 'tid12', 1, 1),
                               ('vid21', 'tid21', 1, 0)])
        self.assertTrue(self.watcher.is_loaded)

        # If we call it again, the db hasn't been updated so nothing
        # should happen
        self.mastermind.update_stats_info.reset_mock()
        with self.assertLogNotExists(logging.INFO, 'Found a newer'):
            self.watcher._process_db_data()

        self.assertEqual(self.mastermind.update_stats_info.call_count, 0)
        self.assertTrue(self.watcher.is_loaded)

    def test_stat_db_connection_error(self, datamock):
        self.sqllite_mock.side_effect=impala.error.Error('some error')

        with self.assertLogExists(logging.ERROR, 'Error connecting to stats'):
            self.watcher._process_db_data()

    def test_stats_db_no_videoplay_data(self, datamock):
        cursor = self.ramdb.cursor()
        cursor.executemany('''REPLACE INTO EventSequences
        (thumbnail_id, imvisclienttime, imclickclienttime, mnth, yr, tai)
        VALUES (?,?,?,?,?,?)''', [
            ('tid11', 1405372146, None, 6, 2033, 'tai2'),
            ('tid11', 1405372146, None, 6, 2033, 'tai2'),
            ('tid11', 1405372146, None, 6, 2033, 'tai2'),
            ('tid11', 1405372146, 1405372146, 6, 2033, 'tai2'),
            ('tid12', 1405372146, 1405372146, 6, 2033, 'tai2'),
            ('tid21', 1405372146, None, 6, 2033, 'tai11'),
            ('tid21', None, 1405372146, 6, 2033, 'tai11')])
        self.ramdb.commit()

        with self.assertLogExists(logging.ERROR, 
                                  ('Cannot determine when the database was '
                                   'last updated')):
            self.watcher._process_db_data()

        # Make sure the job is flagged as ready
        self.assertTrue(self.watcher.is_loaded)

    def test_stats_db_count_plays(self, datamock):
        # Mock out the calls to the video database
        datamock.TrackerAccountIDMapper.get_all.return_value = [
            neondata.TrackerAccountIDMapper('tai1', 'acct1', STAGING),
            neondata.TrackerAccountIDMapper('tai11', 'acct2', PROD),
            neondata.TrackerAccountIDMapper('tai2', 'acct1', PROD)
            ]
        datamock.TrackerAccountIDMapper.PRODUCTION = PROD
        datamock.TrackerAccountIDMapper.STAGING = STAGING
        datamock.MetricType.PLAYS = neondata.MetricType.PLAYS
        datamock.MetricType = neondata.MetricType
        datamock.ExperimentStrategy.get.side_effect = \
          lambda x: neondata.ExperimentStrategy(x,
              impression_type=neondata.MetricType.LOADS,
              conversion_type=neondata.MetricType.PLAYS)
        
        tid_map = {'tid11' : 'vid11', 'tid12' : 'vid12', 'tid21' : 'vid21'}
        datamock.ThumbnailMetadata.get_video_id.side_effect = \
          lambda tid: tid_map[tid]

        # Add entries to the database
        cursor = self.ramdb.cursor()
        cursor.execute('''REPLACE INTO VideoPlays
        (serverTime, mnth, yr) values (1405372146.32, 6, 2033)''')
        cursor.executemany('''REPLACE INTO EventSequences
        (thumbnail_id, imloadclienttime, imclickclienttime, adplayclienttime,
        videoplayclienttime, mnth, yr, tai)
        VALUES (?,?,?,?,?,?,?,?)''', [
            ('tid11', 1405372146, None, None, None, 1, 2033, 'tai2'),
            ('tid11', 1405372146, 1405372146, None, None, 1, 2033, 'tai2'),
            ('tid11', 1405372146, 1405372146, 1405372146, None, 1, 2033,
             'tai2'),
            ('tid11', 1405372146, 1405372146, None, 1405372146, 1, 2033,
             'tai2'),
            ('tid12', 1405372146, 1405372146, 1405372146, 1405372146, 1, 2033,
             'tai2'),
            ('tid21', 1405372146, None, 1405372146, None, 1, 2033, 'tai11'),
            ('tid21', 1405372146, None, 1405372146, 1405372146, 1, 2033,
             'tai11'),
            ('tid21', 1405372146, None, None, 1405372146, 1, 2033, 'tai11'),
            ('tid21', None, 1405372146, 1405372146, 1405372146, 1, 2033,
             'tai11')])
        self.ramdb.commit()

        # Check the stats db
        with self.assertLogExists(logging.INFO, 'Polling the stats database'):
            with self.assertLogExists(logging.INFO, 'Found a newer'):
                self.watcher._process_db_data()

        # Make sure the data got to mastermind
        self.assertEqual(self.mastermind.update_stats_info.call_count, 2)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', 4, 2),
                               ('vid12', 'tid12', 1, 1),
                               ('vid21', 'tid21', 3, 0)])
        self.assertTrue(self.watcher.is_loaded)

class TestDirectivePublisher(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestDirectivePublisher, self).setUp()
        self.mastermind = mastermind.core.Mastermind()
        self.publisher = mastermind.server.DirectivePublisher(
            self.mastermind)

        # Mock out the connection to S3
        self.s3_patcher = patch('mastermind.server.S3Connection')
        self.s3conn = test_utils.mock_boto_s3.MockConnection()
        self.s3_patcher.start().return_value = self.s3conn
        self.s3conn.create_bucket('neon-image-serving-directives')

        # Insert a fake filesystem
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.real_tempfile = mastermind.server.tempfile
        mastermind.server.tempfile = fake_tempfile.FakeTempfileModule(
            self.filesystem)

    def tearDown(self):
        mastermind.server.tempfile = self.real_tempfile
        self.s3_patcher.stop()
        super(TestDirectivePublisher, self).tearDown()

    def test_s3_connection_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = socket.gaierror('Unknown name')

        with self.assertLogExists(logging.ERROR, 'Error connecting to S3'):
            self.publisher._publish_directives()

    def test_s3_bucket_missing(self):
        self.s3conn.delete_bucket('neon-image-serving-directives')

        with self.assertLogExists(logging.ERROR, 'Could not get bucket'):
            self.publisher._publish_directives()

    def test_s3_bucket_permission_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = boto.exception.S3PermissionsError('Ooops')

        with self.assertLogExists(logging.ERROR, 'Could not get bucket'):
            self.publisher._publish_directives()

    def test_basic_directive(self):
        self.mastermind.serving_directive = {
            'acct1_vid1': (('acct1', 'acct1_vid1'), 
                           [('tid11', 0.1),
                            ('tid12', 0.2),
                            ('tid13', 0.8)]),
            'acct1_vid2': (('acct1', 'acct1_vid2'), 
                           [('tid21', 0.0),
                            ('tid22', 1.0)])}
        self.publisher.update_tracker_id_map({
            'tai1' : 'acct1',
            'tai1s' : 'acct1',
            'tai2p' : 'acct2'})
        self.publisher.update_serving_urls({
            'tid11' : { (640, 480): 't11_640.jpg',
                        (120, 90): 't11_120.jpg' },
            'tid12' : { (800, 600): 't12_800.jpg',
                        (120, 90): 't12_120.jpg'},
            'tid13' : { (120, 90): 't13_120.jpg'},
            'tid21' : { (1920, 720): 't21_1920.jpg',
                        (120, 90): 't21_120.jpg'},
            'tid22' : { (500, 500): 't22_500.jpg',
                        (120, 90): 't22_120.jpg'},
                        })

        self.publisher._publish_directives()

        # Make sure that there are two directive files, one is the
        # REST endpoint and the second is a timestamped one.
        bucket = self.s3conn.get_bucket('neon-image-serving-directives')
        keys = [x for x in bucket.get_all_keys()]
        key_names = [x.name for x in keys]
        self.assertEquals(len(key_names), 2)
        self.assertEquals(keys[0].size,keys[1].size)
        self.assertIn('mastermind', key_names)
        key_names.remove('mastermind')
        self.assertRegexpMatches(key_names[0], 'mastermind\.[0-9]+')
        
        # Now check the data format in the file
        lines = bucket.get_key('mastermind').get_contents_as_string().split('\n')
        # Make sure the expiry is valid
        self.assertRegexpMatches(lines[0], 'expiry=.+')
        self.assertGreater(dateutil.parser.parse(lines[0].split['='][1]),
                           datetime.datetime.utcnow() +
                           datetime.timedelta(seconds=300))

        # Split the data lines into tracker id maps and directives
        tracker_ids = {}
        directives = {}
        for line in lines[1:]:
            data = json.loads(line)
            if data['type'] == 'pub':
                tracker_ids[data['pid']] = data['aid']
            elif data['type'] == 'dir':
                directives[(data['aid'], data['vid'])] = data
            else:
                self.fail('Invalid data type: %s' % data['type'])

        # Validate the tracker mappings
        self.assertEqual(tracker_ids, {'tai1': 'acct1',
                                       'tai1s': 'acct1',
                                       'tai2p': 'acct2'})

        # Validate the actual directives
        

        
        
if __name__ == '__main__':
    utils.neon.InitNeonTest()
    unittest.main()
        
        
