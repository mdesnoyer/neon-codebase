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
from cmsdb import neondata
import datetime as date
import dateutil.parser
import fake_filesystem
import fake_tempfile
import happybase
import impala.error
import json
import gzip
import logging
import mastermind.core
from mock import MagicMock, patch
import mock
import re
import sqlite3
import stats.db
from StringIO import StringIO
import struct
import socket
import test_utils.mock_boto_s3
import test_utils.neontest
import test_utils.postgresql 
import thrift.Thrift
import time
import tornado.web
import unittest
import utils.neon
from utils.options import options
import utils.ps
from utils import statemon

STAGING = neondata.TrackerAccountIDMapper.STAGING
PROD = neondata.TrackerAccountIDMapper.PRODUCTION

class ServerPostgresTest(test_utils.neontest.TestCase):
    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(ServerPostgresTest, self).tearDown()

    @classmethod
    def setUpClass(cls):
        super(ServerPostgresTest, cls).tearDownClass() 
        cls.max_io_loop_size = options.get(
            'cmsdb.neondata.max_io_loop_dict_size')
        options._set('cmsdb.neondata.max_io_loop_dict_size', 10)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        cls.postgresql.stop()
        options._set('cmsdb.neondata.max_io_loop_dict_size', 
            cls.max_io_loop_size)
        super(ServerPostgresTest, cls).tearDownClass()
 
class ServerAsyncPostgresTest(test_utils.neontest.AsyncTestCase):
    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(ServerAsyncPostgresTest, self).tearDown()

    @classmethod
    def setUpClass(cls):
        super(ServerAsyncPostgresTest, cls).tearDownClass() 
        cls.max_io_loop_size = options.get(
            'cmsdb.neondata.max_io_loop_dict_size')
        options._set('cmsdb.neondata.max_io_loop_dict_size', 10)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        cls.postgresql.stop()
        options._set('cmsdb.neondata.max_io_loop_dict_size', 
            cls.max_io_loop_size)
        super(ServerAsyncPostgresTest, cls).tearDownClass() 

@patch('mastermind.server.neondata')
class TestVideoDBWatcher(ServerPostgresTest):
    def setUp(self):
        self.callback_patcher = patch('cmsdb.neondata.utils.http')
        self.callback_patcher.start()
        
        self.mastermind = mastermind.core.Mastermind()
        self.directive_publisher = mastermind.server.DirectivePublisher(
            self.mastermind)
        self.watcher = mastermind.server.VideoDBWatcher(
            self.mastermind,
            self.directive_publisher)
        logging.getLogger('mastermind.server').reset_sample_counters()
        acct = neondata.NeonUserAccount('acct1', 
            'apikey', 
            serving_enabled=True)
        acct.save()

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.callback_patcher.stop()

    @patch('cmsdb.neondata.NeonUserAccount.get_internal_video_ids')
    def test_good_db_data(self, get_ivids_mock, datamock):
        datamock.InternalVideoID = neondata.InternalVideoID

        self.directive_publisher.last_published_videos.add('apikey1' + '_0')
        # set up 4 accounts with videos, make sure it processes 
        # them all correctly
 
        acct1 = neondata.NeonUserAccount('acct1', 
            'apikey1', 
            serving_enabled=True)
        job11 = neondata.NeonApiRequest('job11', 'apikey1', 0)
        job11.state = neondata.RequestState.FINISHED

        # a job in submit state, without any thumbnails
        job12 = neondata.NeonApiRequest('job12', 'apikey1', 10)
        job12.state = neondata.RequestState.SUBMIT

        acct2 = neondata.NeonUserAccount('acct2', 
            'apikey2', 
            serving_enabled=True)
        job21 = neondata.NeonApiRequest('job21', 'apikey2', 1)
        job21.state = neondata.RequestState.FINISHED

        job22 = neondata.NeonApiRequest('job22', 'apikey2', 2)
        job22.state = neondata.RequestState.FINISHED

        acct3 = neondata.NeonUserAccount('acct3', 
           'apikey3', 
           serving_enabled=True)
        job31 = neondata.NeonApiRequest('job31', 'apikey3', 4)
        job31.state = neondata.RequestState.CUSTOMER_ERROR

        acct4 = neondata.NeonUserAccount('acct4', 
            'apikey4', 
            serving_enabled=True)

        datamock.NeonUserAccount.iterate_all.return_value = [acct1,acct2,acct3,acct4] 
        get_ivids_mock.side_effect = [
                                       ['apikey1_0','apikey1_10'], 
                                       ['apikey2_1','apikey2_2'], 
                                       ['apikey3_4'], 
                                       [] 
                                     ] 

        # Define the video meta data
        vid_meta = {
            'apikey1' + '_0': neondata.VideoMetadata(
                'apikey1' + '_0',
                ['apikey1'+'_0_t01','apikey1'+'_0_t02','apikey1'+'_0_t03'],
                i_id='i1'),
            'apikey1' + '_10': neondata.VideoMetadata('apikey1' + '_10', [],
                                                    i_id='i1'),
            'apikey2' + '_1': neondata.VideoMetadata('apikey2' + '_1',
                                                   ['apikey2'+'_1_t11'],
                                                   i_id='i2'),
            'apikey2' + '_2': neondata.VideoMetadata(
                'apikey2' + '_2',
                ['apikey2'+'_2_t21', 'apikey2'+'_2_t22'],
                i_id='i2'),
            'apikey3' + '_4': neondata.VideoMetadata(
                'apikey3' + '_4',
                ['apikey3'+'_4_t41', 'apikey3'+'_4_t42'],
                i_id='0')
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]

        # Define the thumbnail meta data
        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            'apikey1'+'_0_t01': TMD('apikey1'+'_0_t01','apikey1'+'_0',
                                  ttype='brightcove'),
            'apikey1'+'_0_t02': TMD('apikey1'+'_0_t02','apikey1'+'_0',ttype='neon', 
                                  rank=0, chosen=True),
            'apikey1'+'_0_t03': TMD('apikey1'+'_0_t03','apikey1'+'_0',ttype='neon', 
                                  rank=1),
            'apikey2'+'_1_t11': TMD('apikey2'+'_1_t11','apikey2'+'_1',
                                  ttype='brightcove'),
            'apikey2'+'_2_t21': TMD('apikey2'+'_2_t21','apikey2'+'_2',
                                  ttype='random'),
            'apikey2'+'_2_t22': TMD('apikey2'+'_2_t22','apikey2'+'_2',ttype='neon', 
                                  chosen=True),
            'apikey3'+'_4_t41': TMD('apikey3'+'_4_t41','apikey3'+'_4',ttype='neon', 
                                  rank=0),
            'apikey3'+'_4_t42': TMD('apikey3'+'_4_t42','apikey3'+'_4',ttype='neon', 
                                  rank=1),
            }
        datamock.ThumbnailMetadata.get_many.side_effect = \
                lambda tids: [tid_meta[tid] for tid in tids]

        # Define the serving strategy
        datamock.ExperimentStrategy.get.side_effect = \
          [neondata.ExperimentStrategy('apikey1', exp_frac=0.0),
           neondata.ExperimentStrategy('apikey2', 
               exp_frac=0.01, holdback_frac=0.01),
           neondata.ExperimentStrategy('apikey3',
               exp_frac=0.01, holdback_frac=0.01),
           neondata.ExperimentStrategy('apikey4',
               exp_frac=0.01, holdback_frac=0.01)]

        # Process the data
        self.watcher._process_db_data(True)

        # Check the resulting directives
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(len(directives), 4)
        self.assertEquals(directives[('apikey1', 'apikey1'+'_0')],
                          {'apikey1'+'_0_t01': 0.0, 'apikey1'+'_0_t02': 1.0,
                           'apikey1'+'_0_t03': 0.0})
        self.assertEquals(directives[('apikey2', 'apikey2'+'_1')],
                          {'apikey2'+'_1_t11': 1.0})
        self.assertEquals(directives[('apikey2', 'apikey2'+'_2')],
                          {'apikey2'+'_2_t21': 0.01, 'apikey2'+'_2_t22': 0.99})
        self.assertGreater(
            directives[('apikey3', 'apikey3'+'_4')]['apikey3'+'_4_t41'], 0.0)
        self.assertGreater(
            directives[('apikey3', 'apikey3'+'_4')]['apikey3'+'_4_t42'], 0.0)
        # video in submit state without thumbnails shouldn't be in
        # the directive file
        self.assertFalse(directives.has_key(('apikey1', 'apikey1'+'_10')))

        self.assertTrue(self.watcher.is_loaded.is_set())
        self.assertNotIn('apikey1' + '_0', 
                         self.directive_publisher.last_published_videos)

    def test_serving_url_update(self, datamock):
        datamock.InternalVideoID = neondata.InternalVideoID
        api_key = "neonapikey"

        job11 = neondata.NeonApiRequest('job11', api_key, 0, 
                                        't', 't', 'r', 'h')

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
        
        serving_urls = [
            neondata.ThumbnailServingURLs(
                api_key+'_0_t01',
                base_url='http://one.com', 
                sizes = [(640, 480), (120,90)]),
            neondata.ThumbnailServingURLs(
                api_key+'_0_t02',
                base_url='http://two.com', 
                sizes = [(800, 600), (120,90)])]
        datamock.ThumbnailServingURLs.get_many.return_value = serving_urls

        # Process the data
        self.watcher._process_db_data(True)

        # Make sure that the serving urls were sent to the directive pusher
        self.assertEqual(dict([(x.get_id(),
                                mastermind.server.pack_obj(x.__dict__)) 
                               for x in serving_urls]),
            self.directive_publisher.serving_urls)

    def test_tracker_id_update(self, datamock):
        datamock.TrackerAccountIDMapper.iterate_all.return_value = [
            neondata.TrackerAccountIDMapper('tai1', 'acct1', STAGING),
            neondata.TrackerAccountIDMapper('tai11', 'acct2', PROD),
            neondata.TrackerAccountIDMapper('tai2', 'acct1', PROD)
            ]
        
        # Process the data
        self.watcher._process_db_data(True)

        # Make sure we have the tracker account id mapping
        self.assertEqual(self.directive_publisher.tracker_id_map,
                         {'tai1': 'acct1',
                          'tai2': 'acct1',
                          'tai11': 'acct2'})

    @patch('cmsdb.neondata.NeonUserAccount.get_internal_video_ids')
    def test_account_default_thumb_update(self, get_ivids_mock, datamock):
        datamock.InternalVideoID = neondata.InternalVideoID
        a1 = neondata.NeonUserAccount('a1', 'acct1', serving_enabled=True)
        a1.default_thumbnail_id = 'a1_%s_tdef' % neondata.InternalVideoID.NOVIDEO
        a2 = neondata.NeonUserAccount('a2', 'acct2', serving_enabled=True)
        datamock.NeonUserAccount.iterate_all.return_value = [
            a1, a2]
        get_ivids_mock.return_value = []

        # Process the data
        self.watcher._process_db_data(True)

        # Check the data
        self.assertEqual(self.directive_publisher.default_thumbs['acct1'],
                         'a1_%s_tdef' % neondata.InternalVideoID.NOVIDEO)
        self.assertNotIn('acct2', self.directive_publisher.default_thumbs)

        # Check if we remove the default thumb, it is removed from the map
        a1.default_thumbnail_id = None
        self.watcher._process_db_data(True)
        self.assertNotIn('acct1', self.directive_publisher.default_thumbs)

    @patch('cmsdb.neondata.NeonUserAccount.get_internal_video_ids')
    def test_default_size_update(self, get_ivids_mock, datamock):
        datamock.NeonUserAccount.iterate_all.return_value = [
            neondata.NeonUserAccount('a1', 'acct1', default_size=(160, 90)),
            neondata.NeonUserAccount('a2', 'acct2'),
            neondata.NeonUserAccount('a3', 'acct3', default_size=(640, 480))]
        get_ivids_mock.return_value = [] 
        # Process the data
        self.watcher._process_db_data(True)

        # Check the data
        self.assertEqual(self.directive_publisher.default_sizes['acct1'],
                         (160, 90))
        self.assertEqual(self.directive_publisher.default_sizes['acct2'],
                         (160, 90))
        self.assertEqual(self.directive_publisher.default_sizes['acct3'],
                         (640, 480))

    @patch('cmsdb.neondata.NeonUserAccount.get_internal_video_ids')
    def test_video_metadata_missing(self, get_internal_vids_mock, datamock):
        datamock.InternalVideoID = neondata.InternalVideoID
        api_key = 'apikey'
        acct = neondata.NeonUserAccount('acct1', api_key, serving_enabled=True)
        datamock.NeonUserAccount.iterate_all.return_value = [acct] 
        job11 = neondata.NeonApiRequest('job11', api_key, 0)
        job12 = neondata.NeonApiRequest('job12', api_key, 10)
        
        get_internal_vids_mock.return_value = ['apikey_0', 'apikey_10'] 
        datamock.VideoMetadata.get_many.return_value = [None, None] 

        with self.assertLogExists(
                logging.ERROR,
                'Could not find information about video apikey_0'):
            with self.assertLogExists(logging.ERROR,
                                      'Could not find information about '
                                      'video apikey_10'):
                self.watcher._process_db_data(True) 
        
        self.assertTrue(self.watcher.is_loaded.is_set())

    @patch('cmsdb.neondata.NeonUserAccount.get_internal_video_ids')
    def test_thumb_metadata_missing(self, get_ivids_mock, datamock):
        datamock.InternalVideoID = neondata.InternalVideoID
        api_key = 'apikey'
        job11 = neondata.NeonApiRequest('job11', api_key, 0)
        job12 = neondata.NeonApiRequest('job12', api_key, 1)
        acct = neondata.NeonUserAccount('acct1', api_key, serving_enabled=True)
        datamock.NeonUserAccount.iterate_all.return_value = [acct] 
        
        vid_meta = {
            api_key + '_0': neondata.VideoMetadata(
                api_key+ '_0',
                [api_key+'_0_t01',api_key+'_0_t02',api_key+'_0_t03'],
                i_id='i1'),
            api_key + '_1': neondata.VideoMetadata(api_key + '_1',
                                                   [api_key+'_1_t11'],
                                                   i_id='i1'),
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]
        get_ivids_mock.return_value = [api_key+'_0', api_key+'_1'] 

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
            self.watcher._process_db_data(True)

        # Make sure that there is a directive about the other
        # video in the account.
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(directives[(api_key, api_key+'_1')],
                          {api_key+'_1_t11': 1.0})
        self.assertEquals(len(directives), 1)

        # Make sure that the processing gets flagged as done
        self.assertTrue(self.watcher.is_loaded.is_set())

    @patch('cmsdb.neondata.NeonUserAccount.get_internal_video_ids')
    def test_serving_disabled(self, get_ivids_mock, datamock):
        datamock.InternalVideoID = neondata.InternalVideoID
        api_key = "neonapikey"

        job11 = neondata.NeonApiRequest('job11', api_key, 0, 
                                        't', 't', 'r', 'h')
        job12 = neondata.NeonApiRequest('job11', api_key, '1')
        acct = neondata.NeonUserAccount('acct1', api_key, serving_enabled=True)
        datamock.NeonUserAccount.iterate_all.return_value = [acct] 

        vid_meta = {
            api_key + '_0': neondata.VideoMetadata(api_key + '_0',
                                                   [api_key+'_0_t01',
                                                    api_key+'_0_t02'],
                                                    i_id='i1'),
            api_key + '_1': neondata.VideoMetadata(api_key + '_1', 
                                                   [api_key+'_1_t11'],
                                                   i_id='i1'),
            }
        datamock.VideoMetadata.get_many.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]
        get_ivids_mock.return_value = [api_key+'_0', api_key+'_1'] 

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

        self.watcher._process_db_data(False)

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
        self.watcher._process_db_data(True)

        # Make sure that only one directive is left
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(len(directives), 1)
        self.assertEquals(directives[(api_key, api_key+'_1')],
                          {api_key+'_1_t11': 1.0})

        # Finally, disable the account and make sure that there are no
        # directives
        acct.serving_enabled = False 
        self.watcher._process_db_data(True)
        self.assertEquals(len([x for x in self.mastermind.get_directives()]),
                          0)

    @patch('cmsdb.neondata.NeonUserAccount.get_videos_and_statuses')
    @patch('cmsdb.neondata.NeonUserAccount.get_internal_video_ids')
    def test_initialize_serving_directives(self, 
         get_ivids_mock, 
         iter_all_vids_mock, 
         datamock):

        datamock.InternalVideoID = neondata.InternalVideoID
        acct1 = neondata.NeonUserAccount('a1', 'a1', serving_enabled=True)
        acct2 = neondata.NeonUserAccount('a2', 'a2', serving_enabled=True)

        datamock.NeonUserAccount.iterate_all.return_value = [acct1,acct2] 

        # Define the video meta data
        vid_meta = {
            'a1_vid1': neondata.VideoMetadata(
                'a1_vid1',
                ['a1_vid1_t01'],
                i_id='i1'),
            'a2_vid1': neondata.VideoMetadata(
                'a2_vid1',
                ['a2_vid1_t01',
                 'a2_vid1_t02'],
                i_id='i2'),
            'a2_vid2': neondata.VideoMetadata(
                'a2_vid2',
                ['a2_vid2_t01'],
                i_id='i2', serving_enabled=False)
        }
        datamock.VideoMetadata.get.side_effect = \
          lambda vid: vid_meta[vid]

        # Define the video status
        vid_status = {
            'a1_vid1': neondata.VideoStatus('a1_vid1',
                                            neondata.ExperimentState.COMPLETE),
            'a2_vid1': neondata.VideoStatus('a2_vid1',
                                            neondata.ExperimentState.COMPLETE),
            'a2_vid2': neondata.VideoStatus('a2_vid2',
                                            neondata.ExperimentState.COMPLETE),
        }
        datamock.VideoStatus.get.side_effect = \
          lambda vid, **kw: vid_status[vid]

        # Define the thumbnail status
        tid_status = {
            'a1_vid1_t01' : neondata.ThumbnailStatus('a1_vid1_t01', '0.3'),
            'a2_vid1_t01' : neondata.ThumbnailStatus('a2_vid1_t01', '0.3'),
            'a2_vid1_t02' : neondata.ThumbnailStatus('a2_vid1_t02', '0.7')
            }
        datamock.ThumbnailStatus.get_many.side_effect = \
          lambda tids, **kw: [tid_status[x] for x in tids]

        # Do the initialization
        get_ivids_mock.side_effect = [ ['a1_vid1'],   
            ['a2_vid1','a2_vid2'] 
        ] 
        def _bigger_object():  
            obj = {} 
            obj['a2_vid1'] = { 'serving_enabled' : True, 
                               'video_status_obj' : vid_status['a2_vid1'], 
                               'thumbnail_status_list' : [ tid_status['a2_vid1_t01'], 
                                  tid_status['a2_vid1_t02']       
                               ] 
                             }
            obj['a2_vid2'] = { 'serving_enabled' : True, 
                               'video_status_obj' : vid_status['a2_vid2'], 
                               'thumbnail_status_list' : [] 
                             }
            return obj 
              
        iter_all_vids_mock.side_effect = [ 
              { 'a1_vid1' : 
                { 'serving_enabled' : True, 
                  'video_status_obj' : vid_status['a1_vid1'], 
                  'thumbnail_status_list' : [ tid_status['a1_vid1_t01'] ]
                } 
              }, 
              _bigger_object() 
            ] 
        self.watcher._initialize_serving_directives()

        # Make sure one video was updated
        directives = dict((x[0], dict(x[1]))
                          for x in self.mastermind.get_directives())
        self.assertEquals(len(directives), 1)
        self.assertEquals(directives[('a2', 'a2_vid1')],
                          {'a2_vid1_t01' : 0.3,
                           'a2_vid1_t02' : 0.7})
        self.assertEquals(self.mastermind.experiment_state['a2_vid1'],
                          neondata.ExperimentState.COMPLETE)

class TestVideoDBPushUpdatesPG(test_utils.neontest.AsyncTestCase): 
    def setUp(self):
        # Run a process cycle and then turn on the subscriptions
        self.callback_patcher = patch('cmsdb.neondata.utils.http')
        self.callback_patcher.start()
        self.acct = neondata.NeonUserAccount('acct1', 
                       'key1', 
                       serving_enabled=True)
        self.acct.save()

        # Setup api request
        self.job = neondata.NeonApiRequest('job1', 'key1', 'vid1')
        self.job.save()

        # Create a video with a couple of thumbs in the database
        self.vid = neondata.VideoMetadata('key1_vid1', request_id='job1',
                                          tids=['key1_vid1_t1', 'key1_vid1_t2'],
                                          i_id='i1')
        self.vid.save()
        self.thumbs =  [
            neondata.ThumbnailMetadata('key1_vid1_t1', 'key1_vid1',
                                       ttype='random'),
            neondata.ThumbnailMetadata('key1_vid1_t2', 'key1_vid1',
                                       ttype='neon')]
        neondata.ThumbnailMetadata.save_all(self.thumbs)
        neondata.ThumbnailServingURLs.save_all([
            neondata.ThumbnailServingURLs('key1_vid1_t1',
                                          {(160, 90) : 't1.jpg'}),
            neondata.ThumbnailServingURLs('key1_vid1_t2',
                                          {(160, 90) : 't2.jpg'})])
        neondata.TrackerAccountIDMapper(
            'tai1', 'key1', neondata.TrackerAccountIDMapper.PRODUCTION).save()
        neondata.ExperimentStrategy('key1', 
            exp_frac=0.01, 
            holdback_frac=0.01).save()
        self.mastermind = mastermind.core.Mastermind()
        self.directive_publisher = mastermind.server.DirectivePublisher(
            self.mastermind)
        self.watcher = mastermind.server.VideoDBWatcher(
            self.mastermind,
            self.directive_publisher)
        self.watcher._process_db_data(False)
        if not self.watcher._change_subscriber.is_alive():
            self.watcher._change_subscriber.start()
        super(TestVideoDBPushUpdatesPG, self).setUp()
     
    def tearDown(self):
        self.watcher.__del__()
        self.postgresql.clear_all_tables()
        self.callback_patcher.stop()
        super(TestVideoDBPushUpdatesPG, self).tearDown()

    @classmethod
    def setUpClass(cls):
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)
        super(TestVideoDBPushUpdatesPG, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()
        super(TestVideoDBPushUpdatesPG, cls).tearDownClass()

    def parse_directives(self):
        return dict((x[0], dict(x[1]))
                    for x in self.mastermind.get_directives())
    
    @tornado.testing.gen_test 
    def test_add_default_thumb_to_account(self):
        tornado.ioloop.IOLoop.current().add_callback(lambda: 
            self.watcher._change_subscriber.subscribe_to_db_changes())
        while not self.watcher._change_subscriber._is_subscribed: 
            yield tornado.gen.sleep(0.1)
        nvd = neondata.InternalVideoID.NOVIDEO
        default_acct_thumb = neondata.ThumbnailMetadata('key1_%s_t0' % nvd,
                                                        'key1_%s' % nvd,
                                                        ttype='default',
                                                        rank=0)
        yield default_acct_thumb.save(async=True)
        t_urls = neondata.ThumbnailServingURLs('key1_%s_t0' % nvd,
                                               {(160, 90) : 't_default.jpg'})
        yield t_urls.save(async=True)
        self.acct.default_thumbnail_id = 'key1_%s_t0' % nvd
        self.acct.default_size = (640, 480)
        yield self.acct.save(async=True)
        yield self.assertWaitForEquals(
            lambda: self.directive_publisher.default_thumbs['key1'],
            'key1_%s_t0' % nvd, async=True)

        self.assertEquals(self.directive_publisher.default_sizes['key1'],
                          [640,480])

        yield self.assertWaitForEquals(
            lambda: self.directive_publisher.get_serving_urls(
                'key1_%s_t0' % nvd).get_serving_url(160, 90),
            't_default.jpg', async=True)
        self.acct.default_thumbnail_id = None
        yield self.acct.save(async=True)

        yield self.assertWaitForEquals(
            lambda: 'key1' in self.directive_publisher.default_thumbs,
            False, async=True)

    @tornado.testing.gen_test 
    def test_turn_off_serving(self):
        tornado.ioloop.IOLoop.current().add_callback(lambda: 
            self.watcher._change_subscriber.subscribe_to_db_changes())
        while not self.watcher._change_subscriber._is_subscribed: 
            yield tornado.gen.sleep(0.1)

        self.acct.serving_enabled = False 
        yield self.acct.save(async=True)  
        yield self.assertWaitForEquals(
            lambda: 'key1' in self.watcher._account_subscribers, 
            False, async=True)
        yield self.assertWaitForEquals(
            lambda: len([x for x in self.mastermind.get_directives()]),
            0, async=True)

    @tornado.testing.gen_test(timeout=20.0) 
    def test_add_new_video(self):
        tornado.ioloop.IOLoop.current().add_callback(lambda: 
            self.watcher._change_subscriber.subscribe_to_db_changes())
        while not self.watcher._change_subscriber._is_subscribed: 
            yield tornado.gen.sleep(0.1)
        thumb = neondata.ThumbnailMetadata('key1_vid2_t1', 'key1_vid2',
                                           ttype='random')
        yield thumb.save(async=True)
        vid = neondata.VideoMetadata('key1_vid2', request_id='job2',
                                     tids=['key1_vid2_t1'],
                                     i_id='i1')
        yield vid.save(async=True)
        yield self.assertWaitForEquals(lambda: 
            len([x for x in self.mastermind.get_directives()]), 
              2, 
            async=True, timeout=10.0)
        yield self.assertWaitForEquals(lambda: 
            self.parse_directives()[('key1', 'key1_vid2')],
                {'key1_vid2_t1': 1.0}, 
            async=True)

    @tornado.testing.gen_test 
    def test_change_experiment_strategy(self):
        tornado.ioloop.IOLoop.current().add_callback(lambda: 
            self.watcher._change_subscriber.subscribe_to_db_changes())
        while not self.watcher._change_subscriber._is_subscribed: 
            yield tornado.gen.sleep(0.1)

        exp = yield neondata.ExperimentStrategy('key1', exp_frac=0.1).save(async=True)
        def _parse_directives():
            return dict((x[0], dict(x[1]))
                        for x in self.mastermind.get_directives())
        yield self.assertWaitForEquals(
            self.parse_directives,
            {('key1', 'key1_vid1') : { 'key1_vid1_t1': 0.9,
                                       'key1_vid1_t2': 0.1}}, async=True)

    @tornado.testing.gen_test 
    def test_add_tracker_id(self):
        tornado.ioloop.IOLoop.current().add_callback(lambda: 
            self.watcher._change_subscriber.subscribe_to_db_changes())
        while not self.watcher._change_subscriber._is_subscribed: 
            yield tornado.gen.sleep(0.1)
        yield neondata.TrackerAccountIDMapper(
                'tai2', 'key1', neondata.TrackerAccountIDMapper.STAGING).save(async=True)

        yield self.assertWaitForEquals(
                lambda : self.directive_publisher.tracker_id_map['tai2'], 
                'key1', 
                async=True)

    @tornado.testing.gen_test 
    def test_change_serving_urls(self):
        tornado.ioloop.IOLoop.current().add_callback(lambda: 
            self.watcher._change_subscriber.subscribe_to_db_changes())
        while not self.watcher._change_subscriber._is_subscribed: 
            yield tornado.gen.sleep(0.1)

        yield neondata.ThumbnailServingURLs('key1_vid1_t1',
                                            {(160, 90) : 't1.jpg',
                                            (640, 480) : '640.jpg'}).save(async=True)
        yield self.assertWaitForEquals(
                 lambda: self.directive_publisher.get_serving_urls(
                     'key1_vid1_t1').get_serving_url(640,480),
                     '640.jpg', async=True)

        # Test deleting the serving url
        yield neondata.ThumbnailServingURLs.delete('key1_vid1_t1', async=True)
        yield self.assertWaitForEquals(
                  lambda: 'key1_vid1_t1' in self.directive_publisher.serving_urls,
                  False,
                  async=True)

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
        cursor.execute('CREATE TABLE IF NOT EXISTS table_build_times ('
                       'done_time timestamp)')
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
        self.datamock.TrackerAccountIDMapper.iterate_all.return_value = [
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
        self.sqlite_connect_patcher.stop()

        try:
            cursor = self.ramdb.cursor()
            cursor.execute('drop table videoplays')
            cursor.execute('drop table eventsequences')
            cursor.execute('drop table if exists table_build_times')
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
        self.datamock.TrackerAccountIDMapper.iterate_all.return_value = [
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
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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
                                  ('Cannot find any videoplay events')):
            self.watcher._process_db_data()

        # Make sure that there was no stats update
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(), [])

        # Make sure the job is flagged as ready
        self.assertTrue(self.watcher.is_loaded)

    def test_stats_db_batch_count_plays(self):
        # Mock out the calls to the video database
        self.datamock.TrackerAccountIDMapper.iterate_all.return_value = [
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
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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
        self.watcher.last_table_build = date.datetime.utcnow()
        cursor.execute('REPLACE INTO VideoPlays '
                       '(serverTime, mnth, yr) values '
                       '(1405375746.324, 6, 2033)')
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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
        self.watcher.last_table_build = date.datetime.utcnow()
        cursor = self.ramdb.cursor()
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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
        self.watcher.last_table_build = date.datetime.utcnow()
        cursor = self.ramdb.cursor()
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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

    def test_missing_table_build_times(self):
        # Mock out the database call because if the table isn't there,
        # a impala.error.RPCError RPC status error:
        # TExecuteStatementResp: TStatus(errorCode=None,
        # errorMessage='AnalysisException: Table does not exist:
        # default.my_funk_time', sqlState='HY000', infoMessages=None,
        # statusCode=3 is thrown.
        mock_conn = MagicMock()
        mock_conn.cursor().execute.side_effect = [
            impala.error.RPCError('missing table')]
        self.sqllite_mock.side_effect = [mock_conn]

        with self.assertLogExists(logging.ERROR, 'Probably a table is not'):
            self.watcher._process_db_data()

        # Make sure that there was no stats update
        self.assertEqual(self.mastermind.update_stats_info.call_count, 1)
        self.assertItemsEqual(self._get_all_stat_updates(), [])
        
        self.assertTrue(self.watcher.is_loaded)

class TestDirectivePublisher(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestDirectivePublisher, self).setUp()

        # Mock out the callback sending
        self.callback_patcher = patch('cmsdb.neondata.utils.http')
        self.callback_patcher.start()

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
        self.real_os = mastermind.server.os
        mastermind.server.os = fake_filesystem.FakeOsModule(self.filesystem)

        # Mock out neondata
        self.neondata_patcher = patch('mastermind.server.neondata')
        self.datamock = self.neondata_patcher.start()
        self.datamock.RequestState = neondata.RequestState
        self.datamock.ThumbnailServingURLs = neondata.ThumbnailServingURLs
        self.datamock.CallbackState = neondata.CallbackState

        self.old_serving_update_delay = options.get(
            'mastermind.server.serving_update_delay')
        options._set('mastermind.server.serving_update_delay', 0)

        self.mastermind = mastermind.core.Mastermind()
        self.publisher = mastermind.server.DirectivePublisher(
            self.mastermind)
        logging.getLogger('mastermind.server').reset_sample_counters()

    def tearDown(self):
        self.neondata_patcher.stop()
        self.callback_patcher.stop()
        mastermind.server.tempfile = self.real_tempfile
        mastermind.server.os = self.real_os
        self.s3_patcher.stop()
        options._set('mastermind.server.serving_update_delay',
                     self.old_serving_update_delay)
        del self.mastermind
        super(TestDirectivePublisher, self).tearDown()

    def _parse_directive_file(self, file_data):
        '''Returns expiry, {tracker_id -> account_id},
        {(account_id, video_id) -> json_directive}'''
        gz = gzip.GzipFile(fileobj=StringIO(file_data), mode='rb')
        lines = gz.read().split('\n')
        
        # Make sure the expiry is valid
        self.assertRegexpMatches(lines[0], 'expiry=.+')
        expiry = dateutil.parser.parse(lines[0].split('=')[1])

        # Split the data lines into tracker id maps, default thumb
        # maps and directives
        tracker_ids = {}
        default_thumbs = {}
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
            elif data['type'] == 'default_thumb':
                default_thumbs[data['aid']] = data
            else:
                self.fail('Invalid data type: %s' % data['type'])

        self.assertTrue(found_end)

        return expiry, tracker_ids, default_thumbs, directives

    @tornado.testing.gen_test
    def test_s3_connection_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = socket.gaierror('Unknown name')

        with self.assertLogExists(logging.ERROR, 'Error connecting to S3'):
            yield self.publisher._publish_directives()

    @tornado.testing.gen_test
    def test_s3_bucket_missing(self):
        self.s3conn.delete_bucket('neon-image-serving-directives-test')

        with self.assertLogExists(logging.ERROR, 'Could not get bucket'):
            yield self.publisher._publish_directives()

    @tornado.testing.gen_test
    def test_s3_bucket_permission_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = boto.exception.S3PermissionsError('Ooops')

        with self.assertLogExists(logging.ERROR, 'Could not get bucket'):
            yield self.publisher._publish_directives()

    @tornado.testing.gen_test
    def test_basic_directive(self):
        # We will fill the data structures in the mastermind core
        # directly because it's too complicated to insert them using
        # the update* functions and mocking out all the db calls.
        vid_meta = {
            'acct1_vid1': neondata.VideoMetadata('acct1_vid1',
                                            tids=['acct1_vid1_tid11', 
                                                  'acct1_vid1_tid12', 
                                                  'acct1_vid1_tid13'], 
                                            request_id='job1'), 
            'acct1_vid2': neondata.VideoMetadata('acct1_vid2', 
                                            tids=['acct1_vid2_t21', 
                                             'acct1_vid2_t22'],
                                            request_id='job2')
            }
        get_many_mocker = patch('mastermind.server.neondata.VideoMetadata.get_many')
        get_many_mock = self._future_wrap_mock(get_many_mocker.start()) 
        get_many_mock.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]
        job_info = { 
            ('job1','acct1'): neondata.NeonApiRequest('job1', 'acct1', 'acct1_vid1'), 
            ('job2','acct1'): neondata.NeonApiRequest('job2', 'acct1', 'acct1_vid2') 
        }
        job_many_mocker = patch('mastermind.server.neondata.NeonApiRequest.get_many') 
        job_many_mock = self._future_wrap_mock(job_many_mocker.start()) 
        job_many_mock.side_effect = \
                        lambda jobs: [job_info[job] for job in jobs]
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
        self.publisher.update_default_thumbs({
            'acct1' : 'acct1_vid1_tid11'}
            )
        self.publisher.add_serving_urls(
            'acct1_vid1_tid11',
            neondata.ThumbnailServingURLs('acct1_vid1_tid11',
                                          base_url = 'http://first_tids.com',
                                          sizes=[(640, 480), (160,90)]))
        self.publisher.add_serving_urls(
            'acct1_vid1_tid12',
            neondata.ThumbnailServingURLs(
                'acct1_vid1_tid12',
                size_map = { (800, 600): 't12_800.jpg',
                             (160, 90): 't12_160.jpg'}))
        self.publisher.add_serving_urls(
            'acct1_vid1_tid13',
            neondata.ThumbnailServingURLs('acct1_vid1_tid13',
                                          base_url = 'http://third_tids.com',
                                          sizes=[(160, 90)]))
        self.publisher.add_serving_urls(
            'acct1_vid2_tid21',
            neondata.ThumbnailServingURLs(
                'acct1_vid2_tid21',
                base_url = 'http://two_one.com',
                sizes=[(1920,720), (160, 90)],
                size_map = {(320,240): 't21_320.jpg'}))
        self.publisher.add_serving_urls(
            'acct1_vid2_tid22',
            neondata.ThumbnailServingURLs('acct1_vid2_tid22',
                                          base_url = 'http://two_two.com',
                                          sizes=[(500,500), (160, 90)]))

        yield self.publisher._publish_directives()

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
       
        # check that mastermind file has a gzip header 
        mastermind_file = bucket.get_key('mastermind')
        self.assertEqual(mastermind_file.content_type, 'application/x-gzip')

        # Now check the data format in the file
        expiry, tracker_ids, default_thumbs, directives = \
          self._parse_directive_file(
            mastermind_file.get_contents_as_string())
        
        # Make sure the expiry is valid
        self.assertGreater(expiry,
                           date.datetime.now(dateutil.tz.tzutc()) +
                           date.timedelta(seconds=300))

        # Validate the tracker mappings
        self.assertEqual(tracker_ids, {'tai1': 'acct1',
                                       'tai1s': 'acct1',
                                       'tai2p': 'acct2'})

        # Validate the default thumb
        self.assertEqual(default_thumbs, {
            'acct1' : {
                'type' : 'default_thumb',
                'send_query_string' : False, 
                'aid' : 'acct1',
                'base_url' : 'http://first_tids.com',
                'default_size' : {'h': 90, 'w': 160},
                'img_sizes' : [{'h': 480, 'w': 640}, {'h': 90, 'w': 160}]
            }})

        # Validate the actual directives
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid1',
            'send_query_string' : False, 
            'fractions' : [
                { 'pct' : 0.1,
                  'tid' : 'acct1_vid1_tid11',
                  'base_url' : 'http://first_tids.com',
                  'default_size' : {'h': 90, 'w': 160},
                  'img_sizes' : [{'h': 480, 'w': 640}, {'h': 90, 'w': 160}]
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
                  'base_url' : 'http://third_tids.com',
                  'default_size' : {'h': 90, 'w': 160},
                  'img_sizes' : [{'h': 90, 'w': 160}]
                }]
            },
            directives[('acct1', 'acct1_vid1')])
        self.assertLessEqual(
            dateutil.parser.parse(directives[('acct1', 'acct1_vid1')]['sla']),
            date.datetime.now(dateutil.tz.tzutc()))
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid2',
            'send_query_string' : False, 
            'fractions' : [
                { 'pct' : 0.0,
                  'tid' : 'acct1_vid2_tid21',
                  'default_url' : 'http://two_one.com/neontnacct1_vid2_tid21_w160_h90.jpg',
                  'imgs' : [
                      { 'h': 240, 'w': 320, 'url': 't21_320.jpg' },
                      { 'h': 720, 'w': 1920, 'url': 'http://two_one.com/neontnacct1_vid2_tid21_w1920_h720.jpg' },
                      { 'h': 90, 'w': 160, 'url': 'http://two_one.com/neontnacct1_vid2_tid21_w160_h90.jpg' },
                      ]
                },
                { 'pct' : 1.0,
                  'tid' : 'acct1_vid2_tid22',
                  'base_url' : 'http://two_two.com',
                  'default_size' : {'h': 90, 'w': 160},
                  'img_sizes' : [{'h': 500, 'w': 500}, {'h': 90, 'w': 160}
                ]}]
            },
            directives[('acct1', 'acct1_vid2')])
        self.assertLessEqual(
            dateutil.parser.parse(directives[('acct1', 'acct1_vid2')]['sla']),
            date.datetime.now(dateutil.tz.tzutc()))
        job_many_mocker.stop()
        get_many_mocker.stop()

    @tornado.testing.gen_test
    def test_basic_directive_send_query_string(self):
        # I'm copy pasting from above to test one extra 
        # field, TODO fix this so that's not really necessary 
        vid_meta = {
            'acct1_vid1': neondata.VideoMetadata('acct1_vid1',
                                            tids=['acct1_vid1_tid11', 
                                                  'acct1_vid1_tid12', 
                                                  'acct1_vid1_tid13'], 
                                            request_id='job1'), 
            'acct1_vid2': neondata.VideoMetadata('acct1_vid2', 
                                            tids=['acct1_vid2_t21', 
                                             'acct1_vid2_t22'],
                                            request_id='job2')
            }
        get_many_mocker = patch('mastermind.server.neondata.VideoMetadata.get_many')
        get_many_mock = self._future_wrap_mock(get_many_mocker.start()) 
        get_many_mock.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]
        job_info = { 
            ('job1','acct1'): neondata.NeonApiRequest('job1', 'acct1', 'acct1_vid1'), 
            ('job2','acct1'): neondata.NeonApiRequest('job2', 'acct1', 'acct1_vid2') 
        }
        job_many_mocker = patch('mastermind.server.neondata.NeonApiRequest.get_many') 
        job_many_mock = self._future_wrap_mock(job_many_mocker.start()) 
        job_many_mock.side_effect = \
                        lambda jobs: [job_info[job] for job in jobs]
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
        self.publisher.update_default_thumbs({
            'acct1' : 'acct1_vid1_tid11'}
            )
        self.publisher.add_serving_urls(
            'acct1_vid1_tid11',
            neondata.ThumbnailServingURLs('acct1_vid1_tid11',
                                          base_url = 'http://first_tids.com',
                                          send_query_string = True, 
                                          sizes=[(640, 480), (160,90)]))
        self.publisher.add_serving_urls(
            'acct1_vid1_tid12',
            neondata.ThumbnailServingURLs(
                'acct1_vid1_tid12',
                size_map = { (800, 600): 't12_800.jpg',
                             (160, 90): 't12_160.jpg'}))
        self.publisher.add_serving_urls(
            'acct1_vid1_tid13',
            neondata.ThumbnailServingURLs('acct1_vid1_tid13',
                                          base_url = 'http://third_tids.com',
                                          send_query_string = True, 
                                          sizes=[(160, 90)]))
        self.publisher.add_serving_urls(
            'acct1_vid2_tid21',
            neondata.ThumbnailServingURLs(
                'acct1_vid2_tid21',
                base_url = 'http://two_one.com',
                send_query_string = True, 
                sizes=[(1920,720), (160, 90)],
                size_map = {(320,240): 't21_320.jpg'}))
        self.publisher.add_serving_urls(
            'acct1_vid2_tid22',
            neondata.ThumbnailServingURLs('acct1_vid2_tid22',
                                          base_url = 'http://two_two.com',
                                          send_query_string = True, 
                                          sizes=[(500,500), (160, 90)]))

        yield self.publisher._publish_directives()

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
       
        # check that mastermind file has a gzip header 
        mastermind_file = bucket.get_key('mastermind')
        self.assertEqual(mastermind_file.content_type, 'application/x-gzip')

        # Now check the data format in the file
        expiry, tracker_ids, default_thumbs, directives = \
          self._parse_directive_file(
            mastermind_file.get_contents_as_string())
        
        # Make sure the expiry is valid
        self.assertGreater(expiry,
                           date.datetime.now(dateutil.tz.tzutc()) +
                           date.timedelta(seconds=300))

        # Validate the tracker mappings
        self.assertEqual(tracker_ids, {'tai1': 'acct1',
                                       'tai1s': 'acct1',
                                       'tai2p': 'acct2'})

        # Validate the default thumb
        self.assertEqual(default_thumbs, {
            'acct1' : {
                'type' : 'default_thumb',
                'send_query_string' : True, 
                'aid' : 'acct1',
                'base_url' : 'http://first_tids.com',
                'default_size' : {'h': 90, 'w': 160},
                'img_sizes' : [{'h': 480, 'w': 640}, {'h': 90, 'w': 160}]
            }})

        # Validate the actual directives
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid1',
            'send_query_string' : True, 
            'fractions' : [
                { 'pct' : 0.1,
                  'tid' : 'acct1_vid1_tid11',
                  'base_url' : 'http://first_tids.com',
                  'default_size' : {'h': 90, 'w': 160},
                  'img_sizes' : [{'h': 480, 'w': 640}, {'h': 90, 'w': 160}]
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
                  'base_url' : 'http://third_tids.com',
                  'default_size' : {'h': 90, 'w': 160},
                  'img_sizes' : [{'h': 90, 'w': 160}]
                }]
            },
            directives[('acct1', 'acct1_vid1')])
        self.assertLessEqual(
            dateutil.parser.parse(directives[('acct1', 'acct1_vid1')]['sla']),
            date.datetime.now(dateutil.tz.tzutc()))
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid2',
            'send_query_string' : True, 
            'fractions' : [
                { 'pct' : 0.0,
                  'tid' : 'acct1_vid2_tid21',
                  'default_url' : 'http://two_one.com/neontnacct1_vid2_tid21_w160_h90.jpg',
                  'imgs' : [
                      { 'h': 240, 'w': 320, 'url': 't21_320.jpg' },
                      { 'h': 720, 'w': 1920, 'url': 'http://two_one.com/neontnacct1_vid2_tid21_w1920_h720.jpg' },
                      { 'h': 90, 'w': 160, 'url': 'http://two_one.com/neontnacct1_vid2_tid21_w160_h90.jpg' },
                      ]
                },
                { 'pct' : 1.0,
                  'tid' : 'acct1_vid2_tid22',
                  'base_url' : 'http://two_two.com',
                  'default_size' : {'h': 90, 'w': 160},
                  'img_sizes' : [{'h': 500, 'w': 500}, {'h': 90, 'w': 160}
                ]}]
            },
            directives[('acct1', 'acct1_vid2')])
        self.assertLessEqual(
            dateutil.parser.parse(directives[('acct1', 'acct1_vid2')]['sla']),
            date.datetime.now(dateutil.tz.tzutc()))
        job_many_mocker.stop()
        get_many_mocker.stop()
    
    @tornado.testing.gen_test
    def test_different_default_urls(self):
        api_key = 'acct1'
        vid_meta = {
            'acct1_vid1': neondata.VideoMetadata('acct1_vid1',
                                            tids=['acct1_vid1_t11'], 
                                            request_id='job1'), 
            'acct2_vid2': neondata.VideoMetadata('acct2_vid2', 
                                            tids=['acct2_vid2_t21', 
                                             'acct2_vid2_t22'],
                                            request_id='job2')
            }
        get_many_mocker = patch('mastermind.server.neondata.VideoMetadata.get_many')
        get_many_mock = self._future_wrap_mock(get_many_mocker.start()) 
        get_many_mock.side_effect = \
                        lambda vids: [vid_meta[vid] for vid in vids]
        job_info = { 
            ('job1','acct1'): neondata.NeonApiRequest('job1', 'acct1', 'acct1_vid1'), 
            ('job2','acct2'): neondata.NeonApiRequest('job2', 'acct2', 'acct2_vid2') 
        }
        job_many_mocker = patch('mastermind.server.neondata.NeonApiRequest.get_many') 
        job_many_mock = self._future_wrap_mock(job_many_mocker.start()) 
        job_many_mock.side_effect = \
                        lambda jobs: [job_info[job] for job in jobs]
        
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
        self.publisher.update_default_thumbs({
            'acct1' : 'acct1_vid1_tid11'}
            )
        self.publisher.add_serving_urls(
            'acct1_vid1_tid11',
            neondata.ThumbnailServingURLs('acct1_vid1_tid11',
                                          base_url = 'http://first_tids.com',
                                          sizes=[(640, 480), (160,90)]))
        self.publisher.add_serving_urls(
            'acct2_vid2_tid21',
            neondata.ThumbnailServingURLs('acct2_vid2_tid21',
                                          base_url = 'http://two_one.com',
                                          sizes=[(800,600), (160, 90)]))
        self.publisher.add_serving_urls(
            'acct2_vid2_tid22',
            neondata.ThumbnailServingURLs('acct2_vid2_tid22',
                                          base_url = 'http://two_two.com',
                                          sizes=[(800,600), (240,180),
                                                 (160, 90)]))

        yield self.publisher._publish_directives()

        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        expiry, tracker_ids, default_thumbs, directives = \
          self._parse_directive_file(
            bucket.get_key('mastermind').get_contents_as_string())

        # Validate the default thumb
        self.assertEqual(default_thumbs, {
            'acct1' : {
                'type' : 'default_thumb',
                'aid' : 'acct1',
                'send_query_string' : False, 
                'base_url' : 'http://first_tids.com',
                'default_size' : {'h': 480, 'w': 640},
                'img_sizes' : [{'h': 480, 'w': 640}, {'h': 90, 'w': 160}]
                }
            })

        # Extract a dictionary of the default urls for each thumb
        defaults = {}
        for key, directive in directives.iteritems():
            for thumb in directive['fractions']:
                defaults[thumb['tid']] = thumb['default_size']

        # Validate the sizes
        self.assertEqual(defaults,
                         {'acct1_vid1_tid11': {'h': 480, 'w': 640}, 
                          'acct2_vid2_tid21': {'h': 90, 'w': 160},
                          'acct2_vid2_tid22': {'h': 90, 'w': 160}})
        job_many_mocker.stop()
        get_many_mocker.stop()

    @tornado.testing.gen_test
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
        self.publisher.update_default_thumbs({
            'acct1' : 'acct1_vid1_tid11'}
            )

        self.publisher.add_serving_urls(
            'acct1_vid2_tid21',
            neondata.ThumbnailServingURLs('acct1_vid2_tid21',
                                          base_url = 'http://two_one.com',
                                          sizes=[(800,600), (160, 90)]))

        with self.assertLogExists(logging.ERROR,
                                  'Could not find serving url for thumb '
                                  'acct1_vid1_tid11, which is .*'
                                  'account acct1'):
            with self.assertLogExists(logging.ERROR, 
                                      ('Could not find all serving URLs for '
                                       'video: acct1_vid1')):
                with self.assertLogNotExists(logging.ERROR,
                                             ('Could not find all serving '
                                              'URLs for video: acct1_vid2')):
                    yield self.publisher._publish_directives()

        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        expiry, tracker_ids, default_thumbs, directives = \
          self._parse_directive_file(
            bucket.get_key('mastermind').get_contents_as_string())

        # Make sure that no default thumbs are listed
        self.assertEquals(default_thumbs, {})

        # Make sure that there is a directive for vid2 because tid22
        # is 0% serving, so we don't need to know the serving urls.
        self.assertDictContainsSubset({
            'aid' : 'acct1',
            'vid' : 'acct1_vid2',
            'fractions' : [
                { 'pct' : 1.0,
                  'tid' : 'acct1_vid2_tid21',
                  'base_url' : 'http://two_one.com',
                  'default_size' : {'h': 90, 'w': 160},
                  'img_sizes' : [{'h': 600, 'w': 800}, {'h': 90, 'w': 160}]
                }]
            },
            directives[('acct1', 'acct1_vid2')])

    @tornado.testing.gen_test
    def test_serving_url_list_empty(self):
        self.mastermind.serving_directive = {
            'acct1_vid2': (('acct1', 'acct1_vid2'), 
                           [('tid21', 1.0),
                            ('tid22', 0.0)])}
        self.mastermind.video_info = self.mastermind.serving_directive
        self.publisher.update_tracker_id_map({'tai1' : 'acct1',
                                              'tai2' : 'acct2'})

        self.publisher.add_serving_urls(
            'acct1_vid2_tid21',
            neondata.ThumbnailServingURLs('acct1_vid2_tid21'))

        with self.assertLogExists(logging.ERROR, 
                                  ('Could not find all serving URLs for '
                                   'video: acct1_vid2')):
            with self.assertLogExists(logging.WARNING,
                                      ('No valid sizes to serve for thumb '
                                       'acct1_vid2_tid21')):
                yield self.publisher._publish_directives()

        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        expiry, tracker_ids, default_thumbs, directives = \
          self._parse_directive_file(
            bucket.get_key('mastermind').get_contents_as_string())

    @tornado.testing.gen_test
    def test_get_send_query_string_false(self):
        self.publisher.add_serving_urls(
            'acct1_vid2_tid21',
            neondata.ThumbnailServingURLs('acct1_vid2_tid21'))
        self.assertEquals(False,
           self.publisher._get_send_query_string('acct1_vid2_tid21'))

    @tornado.testing.gen_test
    def test_get_send_query_string_true(self):
        self.publisher.add_serving_urls(
            'acct1_vid2_tid21',
            neondata.ThumbnailServingURLs(
                'acct1_vid2_tid21', 
                send_query_string=True))
        self.assertEquals(True,
           self.publisher._get_send_query_string('acct1_vid2_tid21'))
         

class TestPublisherStatusUpdatesInDB(ServerAsyncPostgresTest):
    '''Tests for updates to the database when directives are published.'''
    def setUp(self):
        super(TestPublisherStatusUpdatesInDB, self).setUp()

        statemon.state._reset_values()

        # Mock out http connections
        self.http_patcher = patch('cmsdb.neondata.utils.http.send_request')
        self.http_mock = self._future_wrap_mock(self.http_patcher.start(),
                                                require_async_kw=True)
        self.callback_mock = MagicMock()
        self.isp_mock = MagicMock()
        def _handle_http(x, **kw):
            if '/v1/video?' in x.url:
                return self.isp_mock(x, **kw)
            else:
                return self.callback_mock(x, **kw)
        self.http_mock.side_effect = _handle_http

        self.callback_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, 200)

        self.isp_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, 200, effective_url='http://somewhere.com/neontnvid.jpg')

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
        self.real_os = mastermind.server.os
        mastermind.server.os = fake_filesystem.FakeOsModule(self.filesystem)

        # Start a database

        # setup neonuser account with apikey = 'acct1'
        self.acc = neondata.NeonUserAccount('myacctid', 
            'acct1', 
            serving_enabled=True)
        self.acc.save()

        # Initialize the data in the database that we actually need
        neondata.VideoMetadata(
            'acct1_vid1',
            tids=['acct1_vid1_tid11', 'acct1_vid1_tid12'],
            request_id='job1',
            i_id='int1').save()

        request = neondata.BrightcoveApiRequest(
            'job1', 'acct1', 'vid1',
            http_callback='http://callback.com')
        request.state = neondata.RequestState.FINISHED
        request.response = { 'video_id' : 'vid1' }
        request.save()

        self.old_serving_update_delay = options.get(
            'mastermind.server.serving_update_delay')
        options._set('mastermind.server.serving_update_delay', 0)

        # Create the publisher
        self.mastermind = mastermind.core.Mastermind()
        self.publisher = mastermind.server.DirectivePublisher(
            self.mastermind)

        # Set the state of the publisher and the mastermind core
        self.mastermind.serving_directive = {
            'acct1_vid1': (('acct1', 'acct1_vid1'), 
                           [('tid11', 0.1),
                            ('tid12', 0.9)])}
        self.mastermind.video_info = self.mastermind.serving_directive
        self.publisher.update_tracker_id_map({
            'tai1' : 'acct1'})
        self.publisher.add_serving_urls(
            'acct1_vid1_tid11',
            neondata.ThumbnailServingURLs('acct1_vid1_tid11',
                                          base_url = 'http://first_tids.com',
                                          sizes=[(640, 480), (160,90)]))
        self.publisher.add_serving_urls(
            'acct1_vid1_tid12',
            neondata.ThumbnailServingURLs(
                'acct1_vid1_tid12',
                size_map = { (800, 600): 't12_800.jpg',
                             (160, 90): 't12_160.jpg'}))

        logging.getLogger('mastermind.server').reset_sample_counters()

    def tearDown(self):
        self.http_patcher.stop()
        mastermind.server.tempfile = self.real_tempfile
        mastermind.server.os = self.real_os
        self.s3_patcher.stop()
        options._set('mastermind.server.serving_update_delay',
                     self.old_serving_update_delay)
        self._wait_for_db_updates()
        super(TestPublisherStatusUpdatesInDB, self).tearDown()

    def _wait_for_db_updates(self):
        self.io_loop.add_future(self.publisher.wait_for_pending_modifies(5),
                                lambda x: self.io_loop.stop())
        self.io_loop.start()

    @tornado.testing.gen_test
    def test_update_request_state_add_and_remove_video(self):
        self.assertEquals(neondata.NeonApiRequest.get('job1', 'acct1').state,
                          neondata.RequestState.FINISHED)
        
        self.assertEquals(neondata.VideoMetadata.get('acct1_vid1').serving_url,
                          None)
        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)
        # Make sure that vid1 was changed in the database to serving
        # because it was just added.
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        yield self.assertWaitForEquals(
            lambda: request.state == \
            neondata.RequestState.SERVING,
            True, async=True)

        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.SUCESS,
            True, async=True)

        self.assertEquals(self.callback_mock.call_count, 1)
        cb_request = self.callback_mock.call_args[0][0]
        self.assertDictContainsSubset(
            {'video_id' : 'vid1',
             'job_id' : 'job1',
             'error' : None,
             'processing_state' : neondata.RequestState.SERVING
             },
            json.loads(cb_request.body))
        self.assertEquals(cb_request.url, 'http://callback.com')
        

        # Make sure that the serving URL was added to the video
        serving_url = neondata.VideoMetadata.get('acct1_vid1').serving_url
        s3httpRe = re.compile(
                'http://i[0-9].neon-images.com/v1/client/%s/neonvid_([a-zA-Z0-9\-\._/]+)'\
                 % self.acc.tracker_account_id)
        self.assertRegexpMatches(serving_url, s3httpRe)

        self.assertIn('acct1_vid1', self.publisher.last_published_videos)

        # Now remove the video and make sure it goes back to state finished
        self.mastermind.remove_video_info('acct1_vid1')
        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)
        yield self.assertWaitForEquals(lambda: 
            neondata.NeonApiRequest.get('job1', 'acct1').state == \
            neondata.RequestState.FINISHED, True, async=True)

        # Make sure the serving url is gone
        self.assertIsNone(neondata.VideoMetadata.get('acct1_vid1').serving_url)

        self.assertNotIn('acct1_vid1', self.publisher.last_published_videos)

    @tornado.testing.gen_test
    def test_request_state_when_no_serving_urls(self):
        self.publisher.serving_urls = {}

        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)

        # The video shouldn't serve because there are not valid serving urls
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        yield self.assertWaitForEquals(lambda: 
            request.state == \
            neondata.RequestState.FINISHED, True, async=True)

        # Make sure the callback wasn't sent
        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.NOT_SENT,
            True, async=True)
        self.assertFalse(self.callback_mock.called)

        self.assertIsNone(neondata.VideoMetadata.get('acct1_vid1').serving_url)

        self.assertNotIn('acct1_vid1', self.publisher.last_published_videos)

    @tornado.testing.gen_test
    def test_no_update_when_only_default_thumb(self):
        # TODO: Add ThumbnailMetadata change to this test when we
        # check for the thumb state instead of just the number of
        # thumbs

        # Only make one thumb valid
        self.mastermind.serving_directive = {
            'acct1_vid1': (('acct1', 'acct1_vid1'), 
                           [('tid11', 1.0)])}
        self.mastermind.video_info = self.mastermind.serving_directive

        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)

        # The video shouldn't be in a serving state because there is only
        # the default thumb
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        yield self.assertWaitForEquals(lambda: 
            request.state == \
            neondata.RequestState.FINISHED, True, async=True)

        # Make sure the callback wasn't sent
        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.NOT_SENT,
            True, async=True)
        self.assertFalse(self.callback_mock.called)

        self.assertIsNone(neondata.VideoMetadata.get('acct1_vid1').serving_url)

        self.assertNotIn('acct1_vid1', self.publisher.last_published_videos)

    @tornado.testing.gen_test
    def test_no_callback_if_already_sent(self):
        def _mod_request(x):
            x.callback_state = neondata.CallbackState.SUCESS
        neondata.NeonApiRequest.modify('job1', 'acct1', _mod_request)

        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)

        # Make sure that vid1 was changed in the database to serving
        # because it was just added.
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        yield self.assertWaitForEquals(lambda: 
            request.state == \
            neondata.RequestState.SERVING, True, async=True)

        # Make sure the callback was not sent again
        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.SUCESS,
            True, async=True)
        self.assertFalse(self.callback_mock.called)
     
    @tornado.testing.gen_test
    def test_callback_error(self):
        self.callback_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, 400)

        with self.assertLogExists(logging.WARNING,
                                  'Error when sending callback'):
            yield self.publisher._publish_directives()
            yield tornado.gen.sleep(0.1)

        # Make sure that vid1 was changed in the database to serving
        # because it was just added.
        request = yield neondata.NeonApiRequest.get('job1', 'acct1', async=True)
        yield self.assertWaitForEquals(
            lambda: request.state == \
            neondata.RequestState.SERVING,
            True, async=True)

        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.ERROR,
            True, async=True)

        self.assertIn('acct1_vid1', self.publisher.last_published_videos)

    @tornado.testing.gen_test
    def test_unexpected_callback_error(self):
        self.callback_mock.side_effect = \
          [Exception('Some bad exception')]

        with self.assertLogExists(logging.WARNING,
                                  'Unexpected error when sending'):
            yield self.publisher._publish_directives()
            yield tornado.gen.sleep(0.1)

        # Make sure that vid1 was changed in the database to serving
        # because it was just added.
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        yield self.assertWaitForEquals(
            lambda: request.state == \
            neondata.RequestState.SERVING,
            True, async=True)

        # Make sure the callback was flagged as error
        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.NOT_SENT,
            True, async=True)

        self.assertIn('acct1_vid1', self.publisher.last_published_videos)

    @tornado.testing.gen_test
    def test_callback_not_blocking(self):
        cb_wait = tornado.locks.Event()
        cb_done = tornado.locks.Event()
        @tornado.gen.coroutine
        def wait_for_event(x, **kwargs):
            yield cb_wait.wait()
            cb_done.set()
            raise tornado.gen.Return(tornado.httpclient.HTTPResponse(x, 200))
        self.callback_mock.side_effect = wait_for_event

        yield self.publisher._publish_directives()
        
        cb_wait.set()
        yield cb_done.wait()

        self.assertTrue(self.callback_mock.called)
        
    
    @tornado.testing.gen_test
    def test_isp_timeout(self):
        self.isp_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, 400)
              
        with options._set_bounded('mastermind.server.isp_wait_timeout', -1.0):
            with self.assertLogExists(logging.ERROR, 'Timed out waiting for'):
                yield self.publisher._publish_directives()
                yield tornado.gen.sleep(0.1)

        self.assertEquals(statemon.state.get(
            'mastermind.server.timeout_waiting_for_isp'), 1)

        # Check the state of the database to make sure it was not updated
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        yield self.assertWaitForEquals(
            lambda: request.state == \
            neondata.RequestState.FINISHED,
            True, async=True)
        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.NOT_SENT,
            True, async=True)
        self.assertFalse(self.callback_mock.called)
        self.assertIsNone(neondata.VideoMetadata.get('acct1_vid1').serving_url)
        
        self.assertNotIn('acct1_vid1', self.publisher.last_published_videos)

    @tornado.testing.gen_test
    def test_already_serving(self):
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        request.state = neondata.RequestState.SERVING
        request.save()

        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)

        # Don't need to send callback
        self.assertFalse(self.callback_mock.called)

        # Video is added to the list
        yield self.assertWaitForEquals( 
            lambda: 'acct1_vid1' in \
            self.publisher.last_published_videos, 
            True, async=True)

    @tornado.testing.gen_test
    def test_error_then_success(self):
        request = neondata.NeonApiRequest.get('job1', 'acct1')
        request.state = neondata.RequestState.INT_ERROR
        request.save()

        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)

        # Don't need to send callback
        self.assertFalse(self.callback_mock.called)

        # Video is added to the list of known ones
        yield self.assertWaitForEquals( 
            lambda: 'acct1_vid1' in \
            self.publisher.last_published_videos, 
            True, async=True)

        # Now change the video and finish it
        request.state = neondata.RequestState.FINISHED
        request.save()
        self.publisher.set_video_updated('acct1_vid1')

        yield self.publisher._publish_directives()
        yield tornado.gen.sleep(0.1)

        # Check the state and callback
        request = neondata.NeonApiRequest.get('job1', 'acct1')

        #self.assertTrue(self.callback_mock.called)
        yield self.assertWaitForEquals( 
            lambda: self.callback_mock.called == \
            True, True, async=True) 
        yield self.assertWaitForEquals(
            lambda: request.state == \
            neondata.RequestState.SERVING,
            True, async=True)
        yield self.assertWaitForEquals(
            lambda: request.callback_state == \
            neondata.CallbackState.SUCESS,
            True, async=True)
        self.assertIsNotNone(neondata.VideoMetadata.get(
            'acct1_vid1').serving_url)

class SmokeTesting(ServerAsyncPostgresTest):
    def setUp(self):
        super(SmokeTesting, self).setUp()
        # Mock out the connection to S3
        self.s3_patcher = patch('mastermind.server.S3Connection')
        self.s3conn = test_utils.mock_boto_s3.MockConnection()
        self.s3_patcher.start().return_value = self.s3conn
        self.s3conn.create_bucket('neon-image-serving-directives-test')

        # Mock out the http connections
        self.http_patcher = patch('cmsdb.neondata.utils.http.send_request')
        self.http_mock = self._future_wrap_mock(self.http_patcher.start(),
                                                require_async_kw=True)
        self.callback_mock = MagicMock()
        self.isp_mock = MagicMock()
        def _handle_http(x, **kw):
            if '/v1/video?' in x.url:
                return self.isp_mock(x, **kw)
            else:
                return self.callback_mock(x, **kw)
        self.http_mock.side_effect = _handle_http

        self.callback_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, 200)

        self.isp_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, 200, effective_url='http://somewhere.com/neontnvid.jpg')

        # Insert a fake filesystem
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.real_tempfile = mastermind.server.tempfile
        mastermind.server.tempfile = fake_tempfile.FakeTempfileModule(
            self.filesystem)
        self.real_os = mastermind.server.os
        mastermind.server.os = fake_filesystem.FakeOsModule(self.filesystem)

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
        cursor.execute('CREATE TABLE IF NOT EXISTS table_build_times ('
                       'done_time timestamp)')
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

        self.old_serving_update_delay = options.get(
            'mastermind.server.serving_update_delay')
        options._set('mastermind.server.serving_update_delay', 0)

        self.mastermind = mastermind.core.Mastermind()
        self.old_publishing_period = options.get(
            'mastermind.server.publishing_period') 
        options._set('mastermind.server.publishing_period', 0.001)
        self.directive_publisher = mastermind.server.DirectivePublisher(
            self.mastermind, activity_watcher=self.activity_watcher)
        self.video_watcher = mastermind.server.VideoDBWatcher(
            self.mastermind,
            self.directive_publisher,
            activity_watcher=self.activity_watcher)
        self.stats_watcher = mastermind.server.StatsDBWatcher(
            self.mastermind, activity_watcher=self.activity_watcher)

    def tearDown(self):
        mastermind.server.tempfile = self.real_tempfile
        mastermind.server.os = self.real_os
        self.hbase_patcher.stop()
        self.cluster_patcher.stop()
        self.http_patcher.stop()
        self.s3_patcher.stop()
        self.sqlite_connect_patcher.stop()
        options._set('mastermind.server.serving_update_delay',
                     self.old_serving_update_delay)
        options._set('mastermind.server.publishing_period',
                     self.old_publishing_period)
        self.directive_publisher.stop()
        self.video_watcher.stop()
        self.stats_watcher.stop()
        try:
            cursor = self.ramdb.cursor()
            cursor.execute('drop table videoplays')
            cursor.execute('drop table table_build_times')
            cursor.execute('drop table eventsequences')
            self.ramdb.commit()
        except Exception as e:
            pass
        self.ramdb.close()
        f = 'file::memory:?cache=shared'
        if os.path.exists(f):
            os.remove(f)
        del self.mastermind
        self.video_watcher.join(2)
        self.video_watcher.__del__()
        self.directive_publisher.join(2)
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

    @tornado.testing.gen_test 
    def test_integration(self):
        # This is purely a smoke test to see if anything breaks when
        # it's all hooked together.

        # Create the account with a default thumbnail
        nvd = neondata.InternalVideoID.NOVIDEO
        default_acct_thumb = neondata.ThumbnailMetadata('key1_%s_t0' % nvd,
                                                        'key1_%s' % nvd,
                                                        ttype='default',
                                                        rank=0)
        default_acct_thumb.save()
        neondata.ThumbnailServingURLs('key1_%s_t0' % nvd,
                                      {(160, 90) : 't_default.jpg'}).save()
        acct = neondata.NeonUserAccount('acct1', 'key1', serving_enabled=True)
        acct.default_thumbnail_id = default_acct_thumb.key
        acct.save()

        # Setup api request and update the state to processed
        job = neondata.NeonApiRequest('job1', 'key1', 'vid1',
                                      http_callback='http://some_callback.com')
        job.state = neondata.RequestState.FINISHED
        job.save()
        
        # Create a video with a couple of thumbs in the database
        vid = neondata.VideoMetadata('key1_vid1', request_id='job1',
                                     tids=['key1_vid1_t1', 'key1_vid1_t2'],
                                     i_id='i1')
        vid.save()
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
        cursor.execute('INSERT INTO table_build_times (done_time) values '
                       "('2014-12-03 10:14:15')")
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

        # Start all the threads
        #import pdb; pdb.set_trace()
        self.video_watcher.start()
        #import pdb; pdb.set_trace()
        self.video_watcher.wait_until_loaded(10.0)
        #tornado.ioloop.IOLoop.current().add_callback(lambda: 
        #    self.video_watcher._change_subscriber.subscribe_to_db_changes())
        while not self.video_watcher._change_subscriber._is_subscribed: 
            yield tornado.gen.sleep(0.1)
        self.stats_watcher.start()
        self.stats_watcher.wait_until_loaded(5.0)
        #import pdb; pdb.set_trace()
        self.directive_publisher.start()
        time.sleep(1) # Make sure that the directive publisher gets busy
        self.activity_watcher.wait_for_idle()

        yield self.directive_publisher.wait_for_pending_modifies(5.0)
        # See if there is anything in S3 (which there should be)
        bucket = self.s3conn.get_bucket('neon-image-serving-directives-test')
        data = bucket.get_key('mastermind').get_contents_as_string()
        gz = gzip.GzipFile(fileobj=StringIO(data), mode='rb')
        lines = gz.read().split('\n')
        self.assertEqual(len(lines), 5)

        # check the DB to ensure it has changed
        req = neondata.VideoMetadata.get_video_request('key1_vid1')
        self.assertEqual(req.state, neondata.RequestState.SERVING)
        self.assertEqual(req.callback_state, neondata.CallbackState.SUCESS)

        # Make sure a callback was sent
        self.assertEqual(self.callback_mock.call_count, 1)

        # Trigger new videos with different states via a push
        # (finished, customer_error)
        for i in [2, 3]:
            job = neondata.NeonApiRequest('job%d'%i, 'key1', 'vid%d'%i)
            if i == 2:
                job.state = neondata.RequestState.FINISHED 
            if i == 3:
                job.state = neondata.RequestState.CUSTOMER_ERROR
            job.save()
    
            # Create a video with a couple of thumbs in the database
            vid = neondata.VideoMetadata('key1_vid%d'%i,
                                    request_id='job%d'%i,
                                    tids=['key1_vid%d_t%d' % (i, j)
                                          for j in range(2)],
                                    i_id='i1')
            yield vid.save(async=True)
            thumbs =  [neondata.ThumbnailMetadata(
                'key1_vid%d_t%d'% (i,j),
                'key1_vid%d' % i,
                ttype='neon',
                rank=j) for j in range(2)]
            yield neondata.ThumbnailMetadata.save_all(thumbs, async=True)
            yield neondata.ThumbnailServingURLs.save_all([
                neondata.ThumbnailServingURLs(
                    'key1_vid%d_t%d' % (i,j),
                    {(160, 90) : 't21.jpg'})
                    for j in range(2)], async=True)

        yield self.assertWaitForEquals(
            lambda: 'key1_vid2' in \
            self.directive_publisher.last_published_videos,
            True, async=True)

        # Check state for the customer_error video and ensure its in the 
        # list of last published videos
        yield self.assertWaitForEquals(
            lambda: 'key1_vid3' in \
            self.directive_publisher.last_published_videos,
            True, async=True)
        self.assertEquals(
            neondata.NeonApiRequest.get('job3', 'key1').state,
            neondata.RequestState.CUSTOMER_ERROR)

        lb_current_time = date.datetime.utcnow() - date.timedelta(hours=12) 
        self.assertTrue(
            dateutil.parser.parse(
                self.video_watcher._account_last_updated_time['key1']) > \
                    lb_current_time)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
