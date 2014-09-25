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
import datetime as date
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
import test_utils.redis
import time
import tornado.web
import unittest
import utils.neon
from utils.options import options
import utils.ps

STAGING = neondata.TrackerAccountIDMapper.STAGING
PROD = neondata.TrackerAccountIDMapper.PRODUCTION

@patch('mastermind.server.neondata')
class TestVideoDBWatcher(test_utils.neontest.TestCase):
    def setUp(self):
        # Mock out the redis connection so that it doesn't throw an error
        self.redis_patcher = patch(
            'supportServices.neondata.blockingRedis.StrictRedis')
        self.redis_patcher.start()
        
        self.mastermind = mastermind.core.Mastermind()
        self.directive_publisher = mastermind.server.DirectivePublisher(
            self.mastermind)
        self.watcher = mastermind.server.VideoDBWatcher(
            self.mastermind,
            self.directive_publisher)
        logging.getLogger('mastermind.server').reset_sample_counters()

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.redis_patcher.stop()

    def test_good_db_data(self, datamock):
        # Define platforms in the database

        api_key = "neonapikey"

        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', api_key, 
                                                 abtest=False)
        bcPlatform.add_video(0, 'job11')
        job11 = neondata.NeonApiRequest('job11', api_key, 0, 't', 't', 'r', 'h')

        bcPlatform.add_video(10, 'job12')
        job12 = neondata.NeonApiRequest('job12', api_key, 10, 't', 't', 'r', 'h')
        bcPlatform.get_processed_internal_video_ids = MagicMock()
        bcPlatform.get_processed_internal_video_ids.return_value = [api_key +
                '_0', api_key + '_10'] 

        testPlatform = neondata.BrightcovePlatform('a2', 'i2', api_key, 
                                                   abtest=True)
        testPlatform.add_video(1, 'job21')
        job21 = neondata.NeonApiRequest('job21', api_key, 1, 't', 't', 'r', 'h')

        testPlatform.add_video(2, 'job22')
        job22 = neondata.NeonApiRequest('job22', api_key, 2, 't', 't', 'r', 'h')
        testPlatform.get_processed_internal_video_ids = MagicMock()
        testPlatform.get_processed_internal_video_ids.return_value = [api_key
        + '_1', api_key + '_2'] 

        apiPlatform = neondata.NeonPlatform('a3', api_key, abtest=True)
        apiPlatform.add_video(4, 'job31')
        job31 = neondata.NeonApiRequest('job31', api_key, 4, 't', 't', 'r', 'h')
        apiPlatform.get_processed_internal_video_ids = MagicMock()
        apiPlatform.get_processed_internal_video_ids.return_value = [api_key +
                '_4'] 

        noVidPlatform = neondata.BrightcovePlatform('a4', 'i4', api_key, 
                                                    abtest=True)
        noVidPlatform.get_processed_internal_video_ids = MagicMock()
        noVidPlatform.get_processed_internal_video_ids.return_value = [] 
        
        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform, testPlatform, apiPlatform, noVidPlatform]

        # Define the video meta data
        vid_meta = {
            api_key + '_0': neondata.VideoMetadata(
                api_key + '_0',
                [api_key+'_0_t01',api_key+'_0_t02',api_key+'_0_t03']),
            api_key + '_10': neondata.VideoMetadata(api_key + '_10', []),
            api_key + '_1': neondata.VideoMetadata(api_key + '_1',
                                                   [api_key+'_1_t11']),
            api_key + '_2': neondata.VideoMetadata(
                api_key + '_2',
                [api_key+'_2_t21', api_key+'_2_t22']),
            api_key + '_4': neondata.VideoMetadata(
                api_key + '_4',
                [api_key+'_4_t41', api_key+'_4_t42'])
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]

        # Define the thumbnail meta data
        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            api_key+'_0_t01': TMD(api_key+'_0_t01',api_key+'_0',
                                  ttype='brightcove'),
            api_key+'_0_t02': TMD(api_key+'_0_t02',api_key+'_0',ttype='neon', 
                                  rank=0, chosen=True),
            api_key+'_0_t03': TMD(api_key+'_0_t03',api_key+'_0',ttype='neon', 
                                  rank=1),
            api_key+'_1_t11': TMD(api_key+'_1_t11',api_key+'_1',
                                  ttype='brightcove'),
            api_key+'_2_t21': TMD(api_key+'_2_t21',api_key+'_2',
                                  ttype='centerframe'),
            api_key+'_2_t22': TMD(api_key+'_2_t22',api_key+'_2',ttype='neon', 
                                  chosen=True),
            api_key+'_4_t41': TMD(api_key+'_4_t41',api_key+'_4',ttype='neon', 
                                  rank=0),
            api_key+'_4_t42': TMD(api_key+'_4_t42',api_key+'_4',ttype='neon', 
                                  rank=1),
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
                          {api_key+'_0_t01': 0.0, api_key+'_0_t02': 1.0,
                           api_key+'_0_t03': 0.0})
        self.assertEquals(directives[(api_key, api_key+'_1')],
                          {api_key+'_1_t11': 1.0})
        self.assertEquals(directives[(api_key, api_key+'_2')],
                          {api_key+'_2_t21': 0.01, api_key+'_2_t22': 0.99})
        self.assertGreater(
            directives[(api_key, api_key+'_4')][api_key+'_4_t41'], 0.0)
        self.assertGreater(
            directives[(api_key, api_key+'_4')][api_key+'_4_t42'], 0.0)

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

    def test_default_size_update(self, datamock):
        datamock.NeonUserAccount.get_all_accounts.return_value = [
            neondata.NeonUserAccount('a1', 'acct1', default_size=(160, 90)),
            neondata.NeonUserAccount('a2', 'acct2'),
            neondata.NeonUserAccount('a3', 'acct3', default_size=(640, 480))]

        # Process the data
        self.watcher._process_db_data()

        # Check the data
        self.assertEqual(self.directive_publisher.default_sizes['acct1'],
                         (160, 90))
        self.assertEqual(self.directive_publisher.default_sizes['acct2'],
                         (160, 90))
        self.assertEqual(self.directive_publisher.default_sizes['acct3'],
                         (640, 480))

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
        job11 = neondata.NeonApiRequest('job11', api_key, 0, 't', 't', 'r', 'h')
        job12 = neondata.NeonApiRequest('job12', api_key, 10, 't', 't', 'r', 'h')
        bcPlatform.get_processed_internal_video_ids = MagicMock()
        bcPlatform.get_processed_internal_video_ids.return_value = [api_key +
                '_0', api_key + '_10'] 
        
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
        job11 = neondata.NeonApiRequest('job11', api_key, 0, 't', 't', 'r', 'h')
        job12 = neondata.NeonApiRequest('job12', api_key, 1, 't', 't', 'r', 'h')
        
        bcPlatform.get_processed_internal_video_ids = MagicMock()
        bcPlatform.get_processed_internal_video_ids.return_value = [api_key +
                '_0', api_key + '_1'] 
        
        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform]

        vid_meta = {
            api_key + '_0': neondata.VideoMetadata(
                api_key+ '_0',
                [api_key+'_0_t01',api_key+'_0_t02',api_key+'_0_t03']),
            api_key + '_1': neondata.VideoMetadata(api_key + '_1',
                                                   [api_key+'_1_t11']),
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]

        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            api_key+'_0_t01': TMD(api_key+'_0_t01',api_key+'_0',ttype='brightcove'),
            api_key+'_0_t02': TMD(api_key+'_0_t02',api_key+'_0',ttype='neon', rank=0, chosen=True),
            api_key+'_0_t03': None,
            api_key+'_1_t11': TMD(api_key+'_1_t11',api_key+'_1',ttype='brightcove'),
            }

        datamock.ThumbnailMetadata.get_many.side_effect = \
                lambda tids: [tid_meta[tid] for tid in tids]
        with self.assertLogExists(logging.ERROR,
                                  'Could not find metadata for thumb .+t03'):
            self.watcher._process_db_data()

        # Make sure that there is a directive about the other
        # video in the account.
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(directives[(api_key, api_key+'_1')],
                          {api_key+'_1_t11': 1.0})
        self.assertEquals(len(directives), 1)

        # Make sure that the processing gets flagged as done
        self.assertTrue(self.watcher.is_loaded.is_set())

    def test_serving_disabled(self, datamock):
        api_key = "neonapikey"

        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', api_key, 
                                                 abtest=True)
        bcPlatform.add_video(0, 'job11')
        job11 = neondata.NeonApiRequest('job11', api_key, 0, 
                                        't', 't', 'r', 'h')
        bcPlatform.get_processed_internal_video_ids = MagicMock()
        bcPlatform.get_processed_internal_video_ids.return_value = [api_key +
                '_0', api_key + '_1'] 

        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform]

        vid_meta = {
            api_key + '_0': neondata.VideoMetadata(api_key + '_0',
                                                   [api_key+'_0_t01',
                                                    api_key+'_0_t02']),
            api_key + '_1': neondata.VideoMetadata(api_key + '_1', 
                                                   [api_key+'_1_t11']),
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]

        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            api_key+'_0_t01': TMD(api_key+'_0_t01',api_key+'_0',
                                  ttype='brightcove'),
            api_key+'_0_t02': TMD(api_key+'_0_t02',api_key+'_0',ttype='neon', 
                                  rank=0, chosen=True),
            api_key+'_1_t11': TMD(api_key+'_1_t11',api_key+'_1',
                                  ttype='brightcove'),
            }

        datamock.ThumbnailMetadata.get_many.side_effect = \
                lambda tids: [tid_meta[tid] for tid in tids]

        self.watcher._process_db_data()

        # Make sure that there is a directive for both videos
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(len(directives), 2)
        self.assertEquals(directives[(api_key, api_key+'_0')],
                          {api_key+'_0_t01': 0.0, api_key+'_0_t02':1.0})
        self.assertEquals(directives[(api_key, api_key+'_1')],
                          {api_key+'_1_t11': 1.0})

        # Now disable one of the videos
        vid_meta[api_key+'_0'].serving_enabled = False
        self.watcher._process_db_data()

        # Make sure that only one directive is left
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(len(directives), 1)
        self.assertEquals(directives[(api_key, api_key+'_1')],
                          {api_key+'_1_t11': 1.0})

        # Finally, disable the account and make sure that there are no
        # directives
        bcPlatform.serving_enabled = False
        self.watcher._process_db_data()
        self.assertEquals(len([x for x in self.mastermind.get_directives()]),
                          0)
        

class SQLWrapper(object):
    def __init__(self, test_case):
        self.conn = sqlite3.connect('file::memory:?cache=shared')
        self.test_case = test_case

    def cursor(self):
        return SQLWrapper.CursorWrapper(self.conn, self.test_case)

    def __getattr__(self, attr):
            return getattr(self.conn, attr)

    class CursorWrapper(object):
        def __init__(self, conn, test_case):
            self.cursor = conn.cursor()
            self.test_case = test_case

        def execute(self, *args, **kwargs):
            self.test_case.assertNotIn('\n', args[0])
            return self.cursor.execute(*args, **kwargs)

        def executemany(self, *args, **kwargs):
            self.test_case.assertNotIn('\n', args[0])
            return self.cursor.executemany(*args, **kwargs)

        def __getattr__(self, attr):
            return getattr(self.cursor, attr)

        def __iter__(self):
            return self.cursor.__iter__()
                

@patch('mastermind.server.neondata')
class TestStatsDBWatcher(test_utils.neontest.TestCase):    
    def setUp(self):
        self.mastermind = MagicMock()
        self.watcher = mastermind.server.StatsDBWatcher(self.mastermind)

        def connect2db(*args, **kwargs):
            return SQLWrapper(self)
        self.ramdb = connect2db()

        cursor = self.ramdb.cursor()
        # Create the necessary tables (these are subsets of the real tables)
        cursor.execute('CREATE TABLE IF NOT EXISTS videoplays ('
                       'serverTime DOUBLE, '
                       'mnth INT, '
                       'yr INT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS EventSequences ('
                       'thumbnail_id varchar(128),'
                       'imloadclienttime DOUBLE,'
                       'imvisclienttime DOUBLE,'
                       'imclickclienttime DOUBLE,'
                       'adplayclienttime DOUBLE,'
                       'videoplayclienttime DOUBLE,'
                       'mnth INT,'
                       'yr INT,'
                       'tai varchar(64))')
        self.ramdb.commit()

        #patch impala connect
        self.sqlite_connect_patcher = \
          patch('mastermind.server.impala.dbapi.connect')
        self.sqllite_mock = self.sqlite_connect_patcher.start()
        self.sqllite_mock.side_effect = \
          lambda host=None, port=None: self.ramdb 

        # Patch the cluster lookup
        self.cluster_patcher = patch('mastermind.server.stats.cluster.Cluster')
        self.cluster_mock = self.cluster_patcher.start()

    def tearDown(self):
        self.cluster_patcher.stop()
        neondata.DBConnection.clear_singleton_instance()
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
        cursor.execute('REPLACE INTO VideoPlays '
        '(serverTime, mnth, yr) values (1405372146.32, 6, 2033)')
        cursor.executemany('REPLACE INTO EventSequences '
        '(thumbnail_id, imvisclienttime, imclickclienttime, mnth, yr, tai) '
        'VALUES (?,?,?,?,?,?)', [
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
        cursor.executemany('REPLACE INTO EventSequences '
        '(thumbnail_id, imvisclienttime, imclickclienttime, mnth, yr, tai) '
        'VALUES (?,?,?,?,?,?)', [
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

    def test_stats_db_no_strategy_found(self, datamock):
        datamock.TrackerAccountIDMapper.get_all.return_value = [
            neondata.TrackerAccountIDMapper('tai11', 'acct2', PROD),
            ]
        datamock.TrackerAccountIDMapper.PRODUCTION = PROD
        datamock.TrackerAccountIDMapper.STAGING = STAGING
        datamock.MetricType = neondata.MetricType
        datamock.ExperimentStrategy.get.side_effect = [None]

        cursor = self.ramdb.cursor()
        cursor.execute('REPLACE INTO VideoPlays '
        '(serverTime, mnth, yr) values (1405372146.32, 6, 2033)')
        self.ramdb.commit()

        with self.assertLogExists(logging.ERROR, 
                                  'Could not find experimental strategy for'):
            self.watcher._process_db_data()

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
        cursor.execute('REPLACE INTO VideoPlays '
        '(serverTime, mnth, yr) values (1405372146.32, 6, 2033)')
        cursor.executemany('REPLACE INTO EventSequences '
        '(thumbnail_id, imloadclienttime, imclickclienttime, adplayclienttime, '
        'videoplayclienttime, mnth, yr, tai) '
        'VALUES (?,?,?,?,?,?,?,?)', [
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

    def test_cannot_find_cluster(self, datamock):
        self.cluster_mock().find_cluster.side_effect = [
            stats.cluster.ClusterInfoError()
            ]

        with self.assertRaises(stats.cluster.ClusterInfoError):
            with self.assertLogExists(logging.ERROR,
                                      'Could not find the cluster'):
                self.watcher._process_db_data()

class TestDirectivePublisher(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestDirectivePublisher, self).setUp()
        # Mock out the connection to S3
        self.s3_patcher = patch('mastermind.server.S3Connection')
        self.s3conn = test_utils.mock_boto_s3.MockConnection()
        self.s3_patcher.start().return_value = self.s3conn
        self.s3conn.create_bucket('neon-image-serving-directives-test')

        # Insert a fake filesystem
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.real_tempfile = mastermind.server.tempfile
        mastermind.server.tempfile = fake_tempfile.FakeTempfileModule(
            self.filesystem)

        self.mastermind = mastermind.core.Mastermind()
        self.publisher = mastermind.server.DirectivePublisher(
            self.mastermind)

    def tearDown(self):
        neondata.DBConnection.clear_singleton_instance()
        mastermind.server.tempfile = self.real_tempfile
        self.s3_patcher.stop()
        del self.mastermind
        super(TestDirectivePublisher, self).tearDown()

    def _parse_directive_file(self, file_data):
        '''Returns expiry, {tracker_id -> account_id},
        {(account_id, video_id) -> json_directive}'''
        lines = file_data.split('\n')
        
        # Make sure the expiry is valid
        self.assertRegexpMatches(lines[0], 'expiry=.+')
        expiry = dateutil.parser.parse(lines[0].split('=')[1])

        # Split the data lines into tracker id maps and directives
        tracker_ids = {}
        directives = {}
        found_end = False
        for line in lines[1:]:
            if len(line.strip()) == 0:
                # It's an empty line
                continue
            if line == 'end':
                found_end = True
                break
            data = json.loads(line)
            if data['type'] == 'pub':
                tracker_ids[data['pid']] = data['aid']
            elif data['type'] == 'dir':
                directives[(data['aid'], data['vid'])] = data
            else:
                self.fail('Invalid data type: %s' % data['type'])

        self.assertTrue(found_end)

        return expiry, tracker_ids, directives

    def test_s3_connection_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = socket.gaierror('Unknown name')

        with self.assertLogExists(logging.ERROR, 'Error connecting to S3'):
            self.publisher._publish_directives()

    def test_s3_bucket_missing(self):
        self.s3conn.delete_bucket('neon-image-serving-directives-test')

        with self.assertLogExists(logging.ERROR, 'Could not get bucket'):
            self.publisher._publish_directives()

    def test_s3_bucket_permission_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = boto.exception.S3PermissionsError('Ooops')

        with self.assertLogExists(logging.ERROR, 'Could not get bucket'):
            self.publisher._publish_directives()

    def test_basic_directive(self):
        # We will fill the data structures in the mastermind core
        # directly because it's too complicated to insert them using
        # the update* functions and mocking out all the db calls.
        self.mastermind.serving_directive = {
            'acct1_vid1': (('acct1', 'acct1_vid1'), 
                           [('tid11', 0.1),
                            ('tid12', 0.2),
                            ('tid13', 0.8)]),
            'acct1_vid2': (('acct1', 'acct1_vid2'), 
                           [('tid21', 0.0),
                            ('tid22', 1.0)])}
        self.mastermind.video_info = self.mastermind.serving_directive
        
        self.publisher.update_tracker_id_map({
            'tai1' : 'acct1',
            'tai1s' : 'acct1',
            'tai2p' : 'acct2'})
        self.publisher.update_serving_urls({
            'acct1_vid1_tid11' : { (640, 480): 't11_640.jpg',
                                   (160, 90): 't11_160.jpg' },
            'acct1_vid1_tid12' : { (800, 600): 't12_800.jpg',
                                   (160, 90): 't12_160.jpg'},
            'acct1_vid1_tid13' : { (160, 90): 't13_160.jpg'},
            'acct1_vid2_tid21' : { (1920, 720): 't21_1920.jpg',
                                   (160, 90): 't21_160.jpg'},
            'acct1_vid2_tid22' : { (500, 500): 't22_500.jpg',
                                   (160, 90): 't22_160.jpg'},
                                   })

        self.publisher._publish_directives()

        # Make sure that there are two directive files, one is the
        # REST endpoint and the second is a timestamped one.
        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        keys = [x for x in bucket.get_all_keys()]
        key_names = [x.name for x in keys]
        self.assertEquals(len(key_names), 2)
        self.assertEquals(keys[0].size,keys[1].size)
        self.assertIn('mastermind', key_names)
        key_names.remove('mastermind')
        self.assertRegexpMatches(key_names[0], '[0-9]+\.mastermind')
        
        # Now check the data format in the file
        expiry, tracker_ids, directives = self._parse_directive_file(
            bucket.get_key('mastermind').get_contents_as_string())
        
        # Make sure the expiry is valid
        self.assertGreater(expiry,
                           date.datetime.now(dateutil.tz.tzutc()) +
                           date.timedelta(seconds=300))

        # Validate the tracker mappings
        self.assertEqual(tracker_ids, {'tai1': 'acct1',
                                       'tai1s': 'acct1',
                                       'tai2p': 'acct2'})

        # Validate the actual directives
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid1',
            'fractions' : [
                { 'pct' : 0.1,
                  'tid' : 'acct1_vid1_tid11',
                  'default_url' : 't11_160.jpg',
                  'imgs' : [
                      { 'h': 480, 'w': 640, 'url': 't11_640.jpg' },
                      { 'h': 90, 'w': 160, 'url': 't11_160.jpg' }]
                },
                { 'pct' : 0.2,
                  'tid' : 'acct1_vid1_tid12',
                  'default_url' : 't12_160.jpg',
                  'imgs' : [
                      { 'h': 600, 'w': 800, 'url': 't12_800.jpg' },
                      { 'h': 90, 'w': 160, 'url': 't12_160.jpg' }]
                },
                { 'pct' : 0.8,
                  'tid' : 'acct1_vid1_tid13',
                  'default_url' : 't13_160.jpg',
                  'imgs' : [
                      { 'h': 90, 'w': 160, 'url': 't13_160.jpg' }]
                }]
            },
            directives[('acct1', 'acct1_vid1')])
        self.assertLessEqual(
            dateutil.parser.parse(directives[('acct1', 'acct1_vid1')]['sla']),
            date.datetime.now(dateutil.tz.tzutc()))
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid2',
            'fractions' : [
                { 'pct' : 0.0,
                  'tid' : 'acct1_vid2_tid21',
                  'default_url' : 't21_160.jpg',
                  'imgs' : [
                      { 'h': 720, 'w': 1920, 'url': 't21_1920.jpg' },
                      { 'h': 90, 'w': 160, 'url': 't21_160.jpg' }]
                },
                { 'pct' : 1.0,
                  'tid' : 'acct1_vid2_tid22',
                  'default_url' : 't22_160.jpg',
                  'imgs' : [
                      { 'h': 500, 'w': 500, 'url': 't22_500.jpg' },
                      { 'h': 90, 'w': 160, 'url': 't22_160.jpg' }]
                }]
            },
            directives[('acct1', 'acct1_vid2')])
        self.assertLessEqual(
            dateutil.parser.parse(directives[('acct1', 'acct1_vid2')]['sla']),
            date.datetime.now(dateutil.tz.tzutc()))

    def test_different_default_urls(self):
        self.mastermind.serving_directive = {
            'acct1_vid1': (('acct1', 'acct1_vid1'), 
                           [('tid11', 1.0)]),
            'acct2_vid2': (('acct2', 'acct2_vid2'), 
                           [('tid21', 1.0),
                            ('tid22', 0.0)])}
        self.mastermind.video_info = self.mastermind.serving_directive
        
        self.publisher.update_tracker_id_map({'tai1' : 'acct1',
                                              'tai2' : 'acct2'})
        self.publisher.update_default_sizes({
            'acct1' : (640, 480),
            'acct2' : None
            })
        self.publisher.update_serving_urls({
            'acct1_vid1_tid11' : { (640, 480): 't11_640.jpg',
                                   (160, 90): 't11_160.jpg' },
            'acct2_vid2_tid21' : { (800, 600): 't21_800.jpg',
                                   (160, 90): 't21_160.jpg'},
            'acct2_vid2_tid22' : { (800, 600): 't22_800.jpg',
                                   (240, 180): 't22_240.jpg',
                                   (120, 68): 't22_120.jpg'}})

        self.publisher._publish_directives()

        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        expiry, tracker_ids, directives = self._parse_directive_file(
            bucket.get_key('mastermind').get_contents_as_string())

        # Extract a dictionary of the default urls for each thumb
        defaults = {}
        for key, directive in directives.iteritems():
            for thumb in directive['fractions']:
                defaults[thumb['tid']] = thumb['default_url']

        # Validate the sizes
        self.assertEqual(defaults,
                         {'acct1_vid1_tid11': 't11_640.jpg', 
                          'acct2_vid2_tid21': 't21_160.jpg',
                          'acct2_vid2_tid22': 't22_120.jpg'})

    def test_serving_url_missing(self):
        self.mastermind.serving_directive = {
            'acct1_vid1': (('acct1', 'acct1_vid1'), 
                           [('tid11', 1.0)]),
            'acct1_vid2': (('acct1', 'acct1_vid2'), 
                           [('tid21', 1.0),
                            ('tid22', 0.0)])}
        self.mastermind.video_info = self.mastermind.serving_directive
        self.publisher.update_tracker_id_map({'tai1' : 'acct1',
                                              'tai2' : 'acct2'})

        self.publisher.update_serving_urls({
            'acct1_vid2_tid21' : { (800, 600): 't21_800.jpg',
                                   (160, 90): 't21_160.jpg'}})

        with self.assertLogExists(logging.ERROR, 
                                  ('Could not find all serving URLs for '
                                   'video: acct1_vid1')):
            with self.assertLogNotExists(logging.ERROR,
                                         ('Could not find all serving URLs for'
                                          ' video: acct1_vid2')):
                self.publisher._publish_directives()

        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        expiry, tracker_ids, directives = self._parse_directive_file(
            bucket.get_key('mastermind').get_contents_as_string())

        # Make sure that there is a directive for vid2 because tid22
        # is 0% serving, so we don't need to know the serving urls.
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid2',
            'fractions' : [
                { 'pct' : 1.0,
                  'tid' : 'acct1_vid2_tid21',
                  'default_url' : 't21_160.jpg',
                  'imgs' : [
                      { 'h': 600, 'w': 800, 'url': 't21_800.jpg' },
                      { 'h': 90, 'w': 160, 'url': 't21_160.jpg' }]
                }]
            },
            directives[('acct1', 'acct1_vid2')])

class SmokeTesting(test_utils.neontest.TestCase):

    def setUp(self):
        super(SmokeTesting, self).setUp()
        # Open up a temoprary redis server
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        # Mock out the connection to S3
        self.s3_patcher = patch('mastermind.server.S3Connection')
        self.s3conn = test_utils.mock_boto_s3.MockConnection()
        self.s3_patcher.start().return_value = self.s3conn
        self.s3conn.create_bucket('neon-image-serving-directives-test')

        # Insert a fake filesystem
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.real_tempfile = mastermind.server.tempfile
        mastermind.server.tempfile = fake_tempfile.FakeTempfileModule(
            self.filesystem)

        # Use an in-memory sqlite for the impala server
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
        self.sqllite_mock.side_effect = connect2db

        # Patch the cluster lookup
        self.cluster_patcher = patch('mastermind.server.stats.cluster.Cluster')
        self.cluster_patcher.start()

        self.activity_watcher = utils.ps.ActivityWatcher()
        self.mastermind = mastermind.core.Mastermind()
        self.directive_publisher = mastermind.server.DirectivePublisher(
            self.mastermind, activity_watcher=self.activity_watcher)
        self.video_watcher = mastermind.server.VideoDBWatcher(
            self.mastermind,
            self.directive_publisher,
            self.activity_watcher)
        self.stats_watcher = mastermind.server.StatsDBWatcher(
            self.mastermind, self.activity_watcher)

    def tearDown(self):
        neondata.DBConnection.clear_singleton_instance()
        mastermind.server.tempfile = self.real_tempfile
        self.cluster_patcher.stop()
        self.s3_patcher.stop()
        self.sqlite_connect_patcher.stop()
        self.redis.stop()
        self.directive_publisher.stop()
        self.video_watcher.stop()
        self.stats_watcher.stop()
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
        del self.mastermind
        super(SmokeTesting, self).tearDown()

    def test_integration(self):
        # This is purely a smoke test to see if anything breaks when
        # it's all hooked together.

        # Setup api request and update the state to processed
        job = neondata.NeonApiRequest('job1', 'key1', 'vid1', 't', 't', 'r', 'h')
        job.state = neondata.RequestState.FINISHED 
        job.save()
        
        # Create a video with a couple of thumbs in the database
        vid = neondata.VideoMetadata('key1_vid1', request_id='job1',
                                     tids=['key1_vid1_t1', 'key1_vid1_t2'])
        vid.save()
        platform = neondata.BrightcovePlatform('acct1', 'i1', 'key1',
                                               abtest=True)
        platform.add_video('vid1', vid.job_id)
        platform.save()
        acct = neondata.NeonUserAccount('acct1', 'key1')
        acct.add_platform(platform)
        acct.save()
        thumbs =  [neondata.ThumbnailMetadata('key1_vid1_t1', 'key1_vid1',
                                              ttype='centerframe'),
                   neondata.ThumbnailMetadata('key1_vid1_t2', 'key1_vid1',
                                              ttype='neon')]
        neondata.ThumbnailMetadata.save_all(thumbs)
        neondata.ThumbnailServingURLs.save_all([
            neondata.ThumbnailServingURLs('key1_vid1_t1',
                                          {(160, 90) : 't1.jpg'}),
            neondata.ThumbnailServingURLs('key1_vid1_t2',
                                          {(160, 90) : 't2.jpg'})])
        neondata.TrackerAccountIDMapper(
            'tai1', 'key1', neondata.TrackerAccountIDMapper.PRODUCTION).save()
        neondata.ExperimentStrategy('key1').save()
        
        # Add a couple of entries to the stats db
        cursor = self.ramdb.cursor()
        cursor.execute('''REPLACE INTO VideoPlays
        (serverTime, mnth, yr) values (1405372146.32, 6, 2033)''')
        cursor.executemany('''REPLACE INTO EventSequences
        (thumbnail_id, imvisclienttime, imclickclienttime, mnth, yr, tai)
        VALUES (?,?,?,?,?,?)''', [
            ('key1_vid1_t1', 1405372146, None, 6, 2033, 'tai1'),
            ('key1_vid1_t1', 1405372146, 1405372146, 6, 2033, 'tai1'),
            ('key1_vid1_t2', 1405372146, None, 6, 2033, 'tai1'),
            ('key1_vid1_t2', None, 1405372146, 6, 2033, 'tai1')])
        self.ramdb.commit()

        # Now start all the threads
        self.video_watcher.start()
        self.video_watcher.wait_until_loaded()
        self.stats_watcher.start()
        self.stats_watcher.wait_until_loaded()
        self.directive_publisher.start()

        time.sleep(1) # Make sure that the directive publisher gets busy
        self.activity_watcher.wait_for_idle()

        # See if there is anything in S3 (which there should be)
        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        lines = bucket.get_key('mastermind').get_contents_as_string().split('\n')
        self.assertEqual(len(lines), 4)
        
if __name__ == '__main__':
    utils.neon.InitNeonTest()
    unittest.main()
