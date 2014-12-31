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
import happybase
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
import struct
import socket
from supportServices import neondata
import test_utils.mock_boto_s3
import test_utils.neontest
import test_utils.redis
import thrift.Thrift
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
        job11 = neondata.NeonApiRequest('job11', api_key, 0)

        bcPlatform.add_video(10, 'job12')
        job12 = neondata.NeonApiRequest('job12', api_key, 10)
        bcPlatform.get_processed_internal_video_ids = MagicMock()
        bcPlatform.get_processed_internal_video_ids.return_value = [api_key +
                '_0', api_key + '_10'] 

        testPlatform = neondata.BrightcovePlatform('a2', 'i2', api_key, 
                                                   abtest=True)
        testPlatform.add_video(1, 'job21')
        job21 = neondata.NeonApiRequest('job21', api_key, 1)

        testPlatform.add_video(2, 'job22')
        job22 = neondata.NeonApiRequest('job22', api_key, 2)
        testPlatform.get_processed_internal_video_ids = MagicMock()
        testPlatform.get_processed_internal_video_ids.return_value = [api_key
        + '_1', api_key + '_2'] 

        apiPlatform = neondata.NeonPlatform('a3', api_key, abtest=True)
        apiPlatform.add_video(4, 'job31')
        job31 = neondata.NeonApiRequest('job31', api_key, 4)
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
                                  ttype='random'),
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
        api_key = "neonapikey"

        bcPlatform = neondata.BrightcovePlatform('a1', 'i1', api_key, 
                                                 abtest=True)
        bcPlatform.add_video(0, 'job11')
        job11 = neondata.NeonApiRequest('job11', api_key, 0, 
                                        't', 't', 'r', 'h')
        bcPlatform.get_processed_internal_video_ids = MagicMock()
        bcPlatform.get_processed_internal_video_ids.return_value = [api_key +
                '_0'] 

        datamock.AbstractPlatform.get_all_instances.return_value = \
          [bcPlatform]
        vid_meta = {
            api_key + '_0': neondata.VideoMetadata(api_key + '_0',
                                                   [api_key+'_0_t01',
                                                    api_key+'_0_t02']),
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]
        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            api_key+'_0_t01': TMD(api_key+'_0_t01',api_key+'_0',
                                  ttype='brightcove'),
            api_key+'_0_t02': TMD(api_key+'_0_t02',api_key+'_0',ttype='neon', 
                                  rank=0, chosen=True),
            }

        datamock.ThumbnailMetadata.get_many.side_effect = \
                lambda tids: [tid_meta[tid] for tid in tids]
        
        serving_urls = {
            api_key+'_0_t01' : { (640, 480): 't01_640.jpg',
                      (120, 90): 't01_120.jpg'},
            api_key+'_0_t02' : { (800, 600): 't02_800.jpg',
                      (120, 90): 't02_120.jpg'}}
        datamock.ThumbnailServingURLs.get_all.return_value = [
            neondata.ThumbnailServingURLs(k, v) for k, v in
            serving_urls.iteritems()
            ]

        # Process the data
        self.watcher._process_db_data()

        # Make sure that the serving urls were sent to the directive pusher
        self.assertEqual(dict([(k, mastermind.server.pack_obj(v)) 
                               for k, v in serving_urls.iteritems()]),
            self.directive_publisher.serving_urls)

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
        job11 = neondata.NeonApiRequest('job11', api_key, 0)
        job12 = neondata.NeonApiRequest('job12', api_key, 10)
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
        job11 = neondata.NeonApiRequest('job11', api_key, 0)
        job12 = neondata.NeonApiRequest('job12', api_key, 1)
        
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
            api_key+'_0_t01': TMD(api_key+'_0_t01',api_key+'_0',
                                  ttype='brightcove'),
            api_key+'_0_t02': TMD(api_key+'_0_t02',api_key+'_0',ttype='neon', 
                                  rank=0, chosen=True),
            api_key+'_0_t03': None,
            api_key+'_1_t11': TMD(api_key+'_1_t11',api_key+'_1',
                                  ttype='brightcove'),
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

class MockHBaseCountTable(object):
    def __init__(self):
        self.data = {} # key => {colname=> cell integer}

    def scan(self, row_start=None, row_stop=None, columns=None):
        '''Function that simulates scanning a table.'''
        rows = sorted(self.data.items(), key=lambda x: x[0])
        for key, cols in rows:
            if row_start is not None and key < row_start:
                continue
            if row_stop is not None and key >= row_stop:
                break

            filtered_cols = {}
            for col, data in cols.iteritems():
                if columns is None or \
                  any([col.startswith(x) for x in columns]):
                    filtered_cols[col] = data
            yield key, filtered_cols

    def insert_count(self, key, col, value):
        '''Insert an entry into the table

        Inputs:
        key - Row key
        col - Full row name with family
        value - Integer value
        '''
        cols = self.data.setdefault(key, {})
        cols[col] = struct.pack('>q', value)

    def insert_row(self, key, col_map):
        '''Insert a row into the mock table.
        
        col_map - column -> integer count dictionary
        '''
        for col, value in col_map.iteritems():
            self.insert_count(key, col, value)

    def insert_event_row(self, key, il=None, iv=None, ic=None, vp=None):
        col_map = {}
        if il is not None:
            col_map['evts:il'] = il
        if iv is not None:
            col_map['evts:iv'] = iv
        if ic is not None:
            col_map['evts:ic'] = ic
        if vp is not None:
            col_map['evts:vp'] = vp
        self.insert_row(key, col_map)
        

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
                       'thumbnail_id varchar(128), '
                       'imloadclienttime DOUBLE, '
                       'imvisclienttime DOUBLE, '
                       'imclickclienttime DOUBLE, '
                       'adplayclienttime DOUBLE, '
                       'videoplayclienttime DOUBLE, '
                       'serverTime DOUBLE, '
                       'mnth INT, '
                       'yr INT, '
                       'tai varchar(64))')
        self.ramdb.commit()

        # Patch neondata and fill with some basic entries
        self.neondata_patcher = patch('mastermind.server.neondata')
        self.datamock = self.neondata_patcher.start()
        self.datamock.TrackerAccountIDMapper.get_all.return_value = [
            neondata.TrackerAccountIDMapper('tai1', 'acct1', PROD)]
        self.datamock.TrackerAccountIDMapper.PRODUCTION = PROD
        self.datamock.TrackerAccountIDMapper.STAGING = STAGING
        self.datamock.MetricType = neondata.MetricType
        self.datamock.ExperimentStrategy.get.side_effect = \
          lambda x: neondata.ExperimentStrategy(x)
        tid_map = {'tid11' : 'vid11', 'tid12' : 'vid12', 'tid21' : 'vid21'}
        self.datamock.ThumbnailMetadata.get_video_id.side_effect = \
          lambda tid: tid_map[tid]

        #patch impala connect
        self.sqlite_connect_patcher = \
          patch('mastermind.server.impala.dbapi.connect')
        self.sqllite_mock = self.sqlite_connect_patcher.start()
        self.sqllite_mock.side_effect = \
          lambda host=None, port=None: self.ramdb 

        #patch hbase connection
        self.hbase_patcher = patch('mastermind.server.happybase.Connection')
        self.hbase_conn = self.hbase_patcher.start()
        self.timethumb_table = MockHBaseCountTable()
        self.thumbtime_table = MockHBaseCountTable()
        def _pick_table(name):
            if name == 'TIMESTAMP_THUMBNAIL_EVENT_COUNTS':
                return self.timethumb_table
            elif name == 'THUMBNAIL_TIMESTAMP_EVENT_COUNTS':
                return self.thumbtime_table
            raise thrift.Thrift.TException('Unknown table')
        self.hbase_conn().table.side_effect = _pick_table

        # Patch the cluster lookup
        self.cluster_patcher = patch('mastermind.server.stats.cluster.Cluster')
        self.cluster_mock = self.cluster_patcher.start()

    def tearDown(self):
        self.neondata_patcher.stop()
        self.cluster_patcher.stop()
        self.hbase_patcher.stop()
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

    def _add_hbase_entry(self, timestamp, thumb_id, il=None, iv=None, ic=None,
                         vp=None):
        tstamp = date.datetime.utcfromtimestamp(timestamp).strftime(
            '%Y-%m-%dT%H')
        self.timethumb_table.insert_event_row(
            '%s_%s' % (tstamp, thumb_id),
            il, iv, ic, vp)
        self.thumbtime_table.insert_event_row(
            '%s_%s' % (thumb_id, tstamp),
            il, iv, ic, vp)

    def test_working_db(self):
        # Mock out the calls to the video database
        self.datamock.TrackerAccountIDMapper.get_all.return_value = [
            neondata.TrackerAccountIDMapper('tai1', 'acct1', STAGING),
            neondata.TrackerAccountIDMapper('tai11', 'acct2', PROD),
            neondata.TrackerAccountIDMapper('tai2', 'acct1', PROD)
            ]

        # Add entries to the batch database. The most recent entries
        # are in the current hour, whereas the older entries are an
        # hour back.
        cursor = self.ramdb.cursor()
        cursor.execute('REPLACE INTO VideoPlays '
                       '(serverTime, mnth, yr) values '
                       '(1405375746.324, 6, 2033)')
        cursor.executemany('REPLACE INTO EventSequences '
        '(thumbnail_id, imvisclienttime, imclickclienttime, servertime, mnth, '
        'yr, tai) '
        'VALUES (?,?,?,?,?,?,?)', [
            ('tid11', 1405372146, None, 1405372146, 6, 2033, 'tai2'),
            ('tid11', 1405372146, None, 1405372146, 6, 2033, 'tai2'),
            ('tid11', 1405375626, None, 1405375626, 6, 2033, 'tai2'),
            ('tid11', 1405372146, 1405372146, 1405372146, 6, 2033, 'tai2'),
            ('tid12', 1405372146, 1405372146, 1405372146, 6, 2033, 'tai2'),
            ('tid21', 1405372146, None, 1405372146, 6, 2033, 'tai11'),
            ('tid21', None, 1405372146, 1405372146, 6, 2033, 'tai11')])
        self.ramdb.commit()

        # Add entries to the hbase incremental database
        self._add_hbase_entry(1405375626, 'tid11', iv=1, ic=0)
        self._add_hbase_entry(1405372146, 'tid11', iv=3, ic=1)
        self._add_hbase_entry(1405372146, 'tid12', iv=1, ic=1)
        self._add_hbase_entry(1405372146, 'tid21', iv=1, ic=1)

        # Check the stats db
        with self.assertLogExists(logging.INFO, 'Polling the stats database'):
            with self.assertLogExists(logging.INFO, 'Found a newer'):
                self.watcher._process_db_data()

        # Make sure the data got to mastermind
        self.assertEqual(self.mastermind.update_stats_info.call_count, 2)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', 3, 1, 1, 0),
                               ('vid12', 'tid12', 1, 0, 1, 0),
                               ('vid21', 'tid21', 1, 0, 0, 0)])
        self.assertTrue(self.watcher.is_loaded)

        # If we call it again, the db hasn't been updated so nothing
        # should happen and we should just get the incremental update
        # for the latest hour.
        self.mastermind.update_stats_info.reset_mock()
        with self.assertLogNotExists(logging.INFO, 'Found a newer'):
            self.watcher._process_db_data()

        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', None, 1, None, 0)])
        self.assertTrue(self.watcher.is_loaded)

    def test_stat_db_connection_error(self):
        self.sqllite_mock.side_effect=impala.error.Error('some error')

        with self.assertLogExists(logging.ERROR, 'Error connecting to stats'):
            self.watcher._process_db_data()

    def test_stats_db_no_videoplay_data(self):
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

        # Make sure that there was no stats update
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(), [])

        # Make sure the job is flagged as ready
        self.assertTrue(self.watcher.is_loaded)

    def test_stats_db_batch_count_plays(self):
        # Mock out the calls to the video database
        self.datamock.TrackerAccountIDMapper.get_all.return_value = [
            neondata.TrackerAccountIDMapper('tai1', 'acct1', STAGING),
            neondata.TrackerAccountIDMapper('tai11', 'acct2', PROD),
            neondata.TrackerAccountIDMapper('tai2', 'acct1', PROD)
            ]
        self.datamock.ExperimentStrategy.get.side_effect = \
          lambda x: neondata.ExperimentStrategy(x,
              impression_type=neondata.MetricType.LOADS,
              conversion_type=neondata.MetricType.PLAYS)

        # Add entries to the database
        cursor = self.ramdb.cursor()
        cursor.execute('REPLACE INTO VideoPlays '
        '(serverTime, mnth, yr) values (1405375746.32, 6, 2033)')
        cursor.executemany('REPLACE INTO EventSequences '
        '(thumbnail_id, imloadclienttime, imclickclienttime, adplayclienttime,'
        'videoplayclienttime, servertime, mnth, yr, tai) '
        'VALUES (?,?,?,?,?,?,?,?,?)', [
            ('tid11', 1405372146, None, None, None, 1405372146, 1, 2033,
             'tai2'),
            ('tid11', 1405372146, 1405372146, None, None, 1405372146, 1, 2033,
             'tai2'),
            ('tid11', 1405372146, 1405372146, 1405372146, None, 1405372146, 1,
             2033, 'tai2'),
            ('tid11', 1405372146, 1405372146, None, 1405372146, 1405372146, 1,
             2033, 'tai2'),
            ('tid12', 1405372146, 1405372146, 1405372146, 1405372146,
             1405372146, 1, 2033, 'tai2'),
            ('tid21', 1405372146, None, 1405372146, None, 1405372146, 1, 2033,
             'tai11'),
            ('tid21', 1405372146, None, 1405372146, 1405372146, 1405372146, 1,
             2033, 'tai11'),
            ('tid21', 1405372146, None, None, 1405372146, 1405372146, 1, 2033,
             'tai11'),
            ('tid21', None, 1405372146, 1405372146, 1405372146, 1405372146, 1,
             2033, 'tai11')])
        self.ramdb.commit()

        # Check the stats db
        with self.assertLogExists(logging.INFO, 'Polling the stats database'):
            with self.assertLogExists(logging.INFO, 'Found a newer'):
                self.watcher._process_db_data()

        # Make sure the data got to mastermind
        self.assertEqual(self.mastermind.update_stats_info.call_count, 2)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', 4, 0, 2, 0),
                               ('vid12', 'tid12', 1, 0, 1, 0),
                               ('vid21', 'tid21', 3, 0, 0, 0)])
        self.assertTrue(self.watcher.is_loaded)

    def test_hbase_down_impala_up(self):
        # hbase connection attempt fails
        self.hbase_conn.side_effect = [
            thrift.transport.TTransport.TTransportException(
                None, 'Could not connect')]

        # Add entries to the batch database.
        cursor = self.ramdb.cursor()
        cursor.execute('REPLACE INTO VideoPlays '
                       '(serverTime, mnth, yr) values '
                       '(1405375746.324, 6, 2033)')
        cursor.executemany('REPLACE INTO EventSequences '
        '(thumbnail_id, imvisclienttime, imclickclienttime, servertime, mnth, '
        'yr, tai) '
        'VALUES (?,?,?,?,?,?,?)', [
            ('tid11', 1405372146, None, 1405372146, 6, 2033, 'tai1'),
            ('tid11', 1405372146, 1405372146, 1405372146, 6, 2033, 'tai1')])
        self.ramdb.commit()

        # Check the stats db
        with self.assertLogExists(logging.INFO, 'Polling the stats database'):
            with self.assertLogExists(logging.INFO, 'Found a newer'):
                with self.assertLogExists(logging.ERROR,
                                          'Error connecting to incremental '
                                          'stats'):
                    self.watcher._process_db_data()

        # Make sure the data got to mastermind. Since we got an update
        # to the base, we want to set the incremental amount to 0 to
        # be conservative.
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', 2, 0, 1, 0)])
        self.assertTrue(self.watcher.is_loaded)

    def test_missing_table_in_hbase(self):
        self.hbase_conn().table.side_effect = [
            happybase.hbase.ttypes.IOError('oops table missing')]
        happybase.hbase.ttypes.IOError

        # Force there to be no impala update
        cursor = self.ramdb.cursor()
        self.watcher.last_update = date.datetime.utcnow()
        cursor.execute('REPLACE INTO VideoPlays '
                       '(serverTime, mnth, yr) values '
                       '(1405375746.324, 6, 2033)')
        self.ramdb.commit()

        # Check the stats db
        with self.assertLogExists(logging.INFO, 'Polling the stats database'):
            with self.assertLogExists(logging.ERROR,
                                      'Error connecting to incremental '
                                      'stats'):
                self.watcher._process_db_data()

        # Make sure the data got to mastermind
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(), [])
        self.assertTrue(self.watcher.is_loaded)

    def test_missing_cols_in_hbase_db(self):
        # Skip filling the batch db
        self.watcher.last_update = date.datetime.utcnow()
        cursor = self.ramdb.cursor()
        cursor.execute('REPLACE INTO VideoPlays '
                       '(serverTime, mnth, yr) values '
                       '(1405375746.324, 6, 2033)')
        self.ramdb.commit()

        # There is only an impression column in the database
        self._add_hbase_entry(1405375626, 'tid11', iv=2)
        self._add_hbase_entry(1405379226, 'tid11', ic=1)

        self.watcher._process_db_data()

        # The conversion increment should be 0 in this case
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', None, 2, None, 1)])
        self.assertTrue(self.watcher.is_loaded)

    def test_bad_entry_in_hbase(self):

        # Skip filling the batch db
        self.watcher.last_update = date.datetime.utcnow()
        cursor = self.ramdb.cursor()
        cursor.execute('REPLACE INTO VideoPlays '
                       '(serverTime, mnth, yr) values '
                       '(1405375746.324, 6, 2033)')
        self.ramdb.commit()

        # The data is malformed in the db. It must be 8 bytes
        self.timethumb_table.data['2014-07-14T22_tid11'] = { 
            'evts:iv' : '\x00\x08'}

        with self.assertLogExists(logging.WARNING, 'Invalid value found'):
            self.watcher._process_db_data()

        # The conversion increment should be 0 in this case
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(), [])
        self.assertTrue(self.watcher.is_loaded)

    def test_cannot_find_cluster_on_boot(self):
        self.cluster_mock().find_cluster.side_effect = [
            stats.cluster.ClusterInfoError()
            ]
        self._add_hbase_entry(1405375746, 'tid11', iv=2, ic=1)
        
        with self.assertLogExists(logging.ERROR,
                                  'Could not find the cluster'):
            self.watcher._process_db_data()

        # Hbase should still be used
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', None, 2, None, 1)])

    def test_cannot_find_cluster_or_hbase_on_boot(self):
        self.cluster_mock().find_cluster.side_effect = [
            stats.cluster.ClusterInfoError()
            ]
        self.hbase_conn.side_effect = [
            thrift.transport.TTransport.TTransportException(
                None, 'Could not connect')]

        with self.assertLogExists(logging.ERROR,
                                  'Could not find the cluster'):
            with self.assertLogExists(logging.ERROR,
                                      'Error connecting to incremental '
                                      'stats'):
                self.watcher._process_db_data()

        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(), [])

    def test_impala_goes_down_after_boot(self):

        # Add entries to the batch database.
        cursor = self.ramdb.cursor()
        cursor.execute('REPLACE INTO VideoPlays '
                       '(serverTime, mnth, yr) values '
                       '(1405375746.324, 6, 2033)')
        cursor.executemany('REPLACE INTO EventSequences '
        '(thumbnail_id, imvisclienttime, imclickclienttime, servertime, mnth, '
        'yr, tai) '
        'VALUES (?,?,?,?,?,?,?)', [
            ('tid11', 1405372146, None, 1405372146, 6, 2033, 'tai1'),
            ('tid11', 1405372146, 1405372146, 1405372146, 6, 2033, 'tai1')])
        self.ramdb.commit()

        self._add_hbase_entry(1405372146, 'tid11', iv=4, ic=2)
        self._add_hbase_entry(1405379226, 'tid11', iv=3, ic=1)

        # Do the boot process, which reads from impala
        with self.assertLogExists(logging.INFO, 'Polling the stats database'):
            with self.assertLogExists(logging.INFO, 'Found a newer'):
                self.watcher._process_db_data()

        # Make sure the data got to mastermind
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', 2, 3, 1, 1)])
        self.assertTrue(self.watcher.is_loaded)

        # Now impala goes down and we should still get the incremental
        # update from hbase.
        self.mastermind.update_stats_info.reset_mock()
        self.cluster_mock().find_cluster.side_effect = [
            stats.cluster.ClusterInfoError()
            ]
        self._add_hbase_entry(1405465626, 'tid11', iv=10, ic=3)
        with self.assertLogExists(logging.ERROR, 'Could not find the cluster'):
            self.watcher._process_db_data()
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(),
                              [('vid11', 'tid11', None, 13, None, 4)])

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
        self.s3conn.create_bucket('neon-image-serving-directives-test')

        # Insert a fake filesystem
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.real_tempfile = mastermind.server.tempfile
        mastermind.server.tempfile = fake_tempfile.FakeTempfileModule(
            self.filesystem)

        # Mock out neondata
        self.neondata_patcher = patch('mastermind.server.neondata')
        self.datamock = self.neondata_patcher.start()
        self.datamock.RequestState = neondata.RequestState

        self.mastermind = mastermind.core.Mastermind()
        self.publisher = mastermind.server.DirectivePublisher(
            self.mastermind)
        logging.getLogger('mastermind.server').reset_sample_counters()

    def tearDown(self):
        self.neondata_patcher.stop()
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
        self.publisher.update_serving_urls(
            {
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
        self.publisher.update_serving_urls(
            {
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
            'acct1_vid2_tid21' : 
                { (800, 600): 't21_800.jpg',
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

    def test_update_request_state_to_serving(self):
        '''
        Test the update_request_state logic
        '''
        api_key = "apikey"
        i_vids = [];
        requests = {}
        def add_video(i, state=neondata.RequestState.SUBMIT):    
            jid = 'job%s' % i
            vid = 'vid%s' % i 
            i_vid = "%s_%s" % (api_key, vid)
            nar = neondata.NeonApiRequest(jid, api_key, vid)
            nar.state = state
            nar.save = MagicMock()
            requests[i_vid] = nar
            i_vids.append(i_vid)
            self.publisher._add_video_id_to_serving_map(i_vid)

        # Add videos
        for i in range(5):
            add_video(i)
        add_video(11, state=neondata.RequestState.ACTIVE)
        self.datamock.VideoMetadata.get_video_requests.side_effect = \
          lambda vids: [requests.get(vid, None) for vid in vids]

        # Check initial state in the map
        for i_vid in i_vids:
            self.assertFalse(self.publisher.video_id_serving_map[i_vid])

        self.publisher._update_request_state_to_serving()
        
        def validate():
            for i_vid in i_vids:
                self.assertTrue(self.publisher.video_id_serving_map[i_vid])

            for req in requests.values():
                self.assertEqual(req.save.call_count, 1)
                if req.job_id == 'job11':
                    self.assertEqual(req.state,
                                     neondata.RequestState.SERVING_AND_ACTIVE)
                else:
                    self.assertEqual(req.state, neondata.RequestState.SERVING)
        
        validate()

        # Second iteration of directive publisher with no video change
        self.publisher._update_request_state_to_serving()
        validate()

        # Add a video
        add_video('6')
        self.publisher._update_request_state_to_serving()
        validate()

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
                       serverTime DOUBLE, 
                       mnth INT,
                       yr INT,
                       tai varchar(64))''')
        self.ramdb.commit()

        #patch impala connect
        self.sqlite_connect_patcher = \
          patch('mastermind.server.impala.dbapi.connect')
        self.sqllite_mock = self.sqlite_connect_patcher.start()
        self.sqllite_mock.side_effect = connect2db

        #patch hbase connection
        self.hbase_patcher = patch('mastermind.server.happybase.Connection')
        self.hbase_conn = self.hbase_patcher.start()
        self.timethumb_table = MockHBaseCountTable()
        self.thumbtime_table = MockHBaseCountTable()
        def _pick_table(name):
            if name == 'TIMESTAMP_THUMBNAIL_EVENT_COUNTS':
                return self.timethumb_table
            elif name == 'THUMBNAIL_TIMESTAMP_EVENT_COUNTS':
                return self.thumbtime_table
            raise thrift.Thrift.TException('Unknown table')
        self.hbase_conn().table.side_effect = _pick_table

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
            activity_watcher=self.activity_watcher)
        self.stats_watcher = mastermind.server.StatsDBWatcher(
            self.mastermind, activity_watcher=self.activity_watcher)

    def tearDown(self):
        neondata.DBConnection.clear_singleton_instance()
        mastermind.server.tempfile = self.real_tempfile
        self.hbase_patcher.stop()
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
        self.directive_publisher.join(2)
        self.video_watcher.join(2)
        self.stats_watcher.join(2)
        super(SmokeTesting, self).tearDown()

    def _add_hbase_entry(self, timestamp, thumb_id, il=None, iv=None, ic=None,
                         vp=None):
        tstamp = date.datetime.utcfromtimestamp(timestamp).strftime(
            '%Y-%m-%dT%H')
        self.timethumb_table.insert_event_row(
            '%s_%s' % (tstamp, thumb_id),
            il, iv, ic, vp)
        self.thumbtime_table.insert_event_row(
            '%s_%s' % (thumb_id, tstamp),
            il, iv, ic, vp)

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
        (thumbnail_id, imvisclienttime, imclickclienttime, servertime, mnth, 
        yr, tai)
        VALUES (?,?,?,?,?,?,?)''', [
            ('key1_vid1_t1', 1405372146, 1405372146, None, 6, 2033, 'tai1'),
            ('key1_vid1_t1', 1405372146, 1405372146, 1405372146, 6, 2033,
             'tai1'),
            ('key1_vid1_t2', 1405372146, 1405372146, None, 6, 2033, 'tai1'),
            ('key1_vid1_t2', None, 1405372146, 1405372146, 6, 2033, 'tai1')])
        self.ramdb.commit()

        # Add entries to the hbase incremental database
        self._add_hbase_entry(1405375626, 'key1_vid1_t1', iv=1, ic=0)
        self._add_hbase_entry(1405372146, 'key1_vid1_t1', iv=3, ic=1)
        self._add_hbase_entry(1405372146, 'key1_vid1_t2', iv=1, ic=1)

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
    
        # Check if the serving state of the video has changed
        self.assertTrue(
                self.directive_publisher.video_id_serving_map['key1_vid1'])

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
