#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)

import copy
from concurrent.futures import Future
import contextlib
import datetime
import dateutil.parser
import logging
import json
import momoko 
import multiprocessing
from mock import patch, MagicMock, ANY
import os
import psycopg2
import re
import redis
import random
import socket
import string
import subprocess
import test_utils.neontest
import test_utils.redis
import time
import threading
import test_utils.postgresql
import tornado.gen
import tornado.ioloop
import tornado.testing
import tornado.httpclient
from tornado.httpclient import HTTPResponse, HTTPRequest
import tornado.ioloop
import utils.neon
from utils.options import options
from cvutils.imageutils import PILImageUtils
import unittest
import uuid
import test_utils.mock_boto_s3 as boto_mock
import test_utils.redis 
from StringIO import StringIO
from cmsdb import neondata
from cmsdb.neondata import NeonPlatform, BrightcovePlatform, \
        YoutubePlatform, NeonUserAccount, DBConnection, NeonApiKey, \
        AbstractPlatform, VideoMetadata, ThumbnailID, ThumbnailURLMapper,\
        ThumbnailMetadata, InternalVideoID, OoyalaPlatform, \
        TrackerAccountIDMapper, ThumbnailServingURLs, ExperimentStrategy, \
        ExperimentState, NeonApiRequest, CDNHostingMetadata,\
        S3CDNHostingMetadata, CloudinaryCDNHostingMetadata, \
        NeonCDNHostingMetadata, CDNHostingMetadataList, ThumbnailType, \
        User
from cvutils import smartcrop
import numpy as np

_log = logging.getLogger(__name__)

class TestNeondataRedisListSpecific(test_utils.neontest.AsyncTestCase): 
    def setUp(self):
        super(TestNeondataRedisListSpecific, self).setUp()
        self.maxDiff = 5000
        logging.getLogger('cmsdb.neondata').reset_sample_counters()

    def tearDown(self):
        conn = neondata.DBConnection.get(VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(ThumbnailMetadata)
        conn.clear_db()
        super(TestNeondataRedisListSpecific, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()

    @tornado.testing.gen_test
    def test_creating_list_of_videos_async(self):
      yield tornado.gen.Task(VideoMetadata('a1_v1').save)
      yield tornado.gen.Task(VideoMetadata.save_all, [
        VideoMetadata('a1_v2'),
        VideoMetadata('a2_v1'),
        VideoMetadata('a2_v2'),
        VideoMetadata('a2_v3')])
      yield tornado.gen.Task(
        VideoMetadata.modify, 'a2_v4', lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(VideoMetadata)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a1_v1')._set_keyname()),
        ['a1_v1', 'a1_v2'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a2_v1')._set_keyname()),
        ['a2_v1', 'a2_v2', 'a2_v3', 'a2_v4'])
      # Now delete some
      yield tornado.gen.Task(VideoMetadata.delete_many,
                             ['a1_v2', 'a2_v2'])

      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a1_v1')._set_keyname()),
        ['a1_v1'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a2_v1')._set_keyname()),
        ['a2_v1', 'a2_v3', 'a2_v4'])

    def test_creating_list_of_videos_sync(self):
      VideoMetadata('a1_v1').save()
      VideoMetadata.save_all([
        VideoMetadata('a1_v2'),
        VideoMetadata('a2_v1'),
        VideoMetadata('a2_v2'),
        VideoMetadata('a2_v3')])
      VideoMetadata.modify('a2_v4', lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(VideoMetadata)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a1_v1')._set_keyname()),
        ['a1_v1', 'a1_v2'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a2_v1')._set_keyname()),
        ['a2_v1', 'a2_v2', 'a2_v3', 'a2_v4'])

      # Now delete some
      VideoMetadata.delete_many(['a1_v2', 'a2_v2'])

      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a1_v1')._set_keyname()),
        ['a1_v1'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        VideoMetadata('a2_v1')._set_keyname()),
        ['a2_v1', 'a2_v3', 'a2_v4'])

    def test_creating_list_of_thumbs_sync(self):
      ThumbnailMetadata('a1_v1_t1').save()
      ThumbnailMetadata.save_all([
        ThumbnailMetadata('a1_v1_t2'),
        ThumbnailMetadata('a1_v2_t1'),
        ThumbnailMetadata('a1_v2_t2'),
        ThumbnailMetadata('a1_v2_t3')])
      ThumbnailMetadata.modify('a1_v2_t4', lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(ThumbnailMetadata)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v1_t1')._set_keyname()),
        ['a1_v1_t1', 'a1_v1_t2'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v2_t1')._set_keyname()),
        ['a1_v2_t1', 'a1_v2_t2', 'a1_v2_t3', 'a1_v2_t4'])

      # Now delete some
      ThumbnailMetadata.delete_many(['a1_v1_t2', 'a1_v2_t2'])

      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v1_t1')._set_keyname()),
        ['a1_v1_t1'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v2_t1')._set_keyname()),
        ['a1_v2_t1', 'a1_v2_t3', 'a1_v2_t4'])

    @tornado.testing.gen_test
    def test_creating_list_of_thumbs_async(self):
      yield tornado.gen.Task(ThumbnailMetadata('a1_v1_t1').save)
      yield tornado.gen.Task(ThumbnailMetadata.save_all, [
        ThumbnailMetadata('a1_v1_t2'),
        ThumbnailMetadata('a1_v2_t1'),
        ThumbnailMetadata('a1_v2_t2'),
        ThumbnailMetadata('a1_v2_t3')])
      yield tornado.gen.Task(
        ThumbnailMetadata.modify, 'a1_v2_t4', lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(ThumbnailMetadata)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v1_t1')._set_keyname()),
        ['a1_v1_t1', 'a1_v1_t2'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v2_t1')._set_keyname()),
        ['a1_v2_t1', 'a1_v2_t2', 'a1_v2_t3', 'a1_v2_t4'])

      # Now delete some
      yield tornado.gen.Task(ThumbnailMetadata.delete_many,
                             ['a1_v1_t2', 'a1_v2_t2'])

      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v1_t1')._set_keyname()),
        ['a1_v1_t1'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        ThumbnailMetadata('a1_v2_t1')._set_keyname()),
        ['a1_v2_t1', 'a1_v2_t3', 'a1_v2_t4'])

    @tornado.testing.gen_test
    def test_create_list_of_requests_sync(self):
      NeonApiRequest('j1', 'a1').save()
      NeonApiRequest.save_all([
        NeonApiRequest('j1', 'a2'),
        NeonApiRequest('j2', 'a2'),
        NeonApiRequest('j3', 'a2'),
        NeonApiRequest('j2', 'a1')])
      NeonApiRequest.modify('j4', 'a2', lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(NeonApiRequest)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a1')._set_keyname()),
        ['request_a1_j1', 'request_a1_j2'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a2')._set_keyname()),
        ['request_a2_j1', 'request_a2_j2', 'request_a2_j3', 'request_a2_j4'])

      # Now delete some
      NeonApiRequest.delete_many([('j2', 'a1'), ('j2', 'a2')])

      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a1')._set_keyname()),
        ['request_a1_j1'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a2')._set_keyname()),
        ['request_a2_j1', 'request_a2_j3', 'request_a2_j4'])

    @tornado.testing.gen_test
    def test_create_list_of_requests_async(self):
      yield tornado.gen.Task(NeonApiRequest('j1', 'a1').save)
      yield tornado.gen.Task(NeonApiRequest.save_all, [
        NeonApiRequest('j1', 'a2'),
        NeonApiRequest('j2', 'a2'),
        NeonApiRequest('j3', 'a2'),
        NeonApiRequest('j2', 'a1')])
      yield tornado.gen.Task(NeonApiRequest.modify,
                             'j4', 'a2', lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(NeonApiRequest)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a1')._set_keyname()),
        ['request_a1_j1', 'request_a1_j2'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a2')._set_keyname()),
        ['request_a2_j1', 'request_a2_j2', 'request_a2_j3', 'request_a2_j4'])

      # Now delete some
      yield tornado.gen.Task(NeonApiRequest.delete_many,
                             [('j2', 'a1'), ('j2', 'a2')])

      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a1')._set_keyname()),
        ['request_a1_j1'])
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonApiRequest('j1', 'a2')._set_keyname()),
        ['request_a2_j1', 'request_a2_j3', 'request_a2_j4'])

    @tornado.testing.gen_test
    def test_create_list_of_namespaced_objects_sync(self):
      objs = [NeonUserAccount('a%i' % i) for i in range(4)]
      objs[0].save()
      NeonUserAccount.save_all(objs[1:3])
      NeonUserAccount.modify(objs[3].neon_api_key,
                             lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(NeonUserAccount)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonUserAccount('a1')._set_keyname()),
        [x.key for x in objs])

      # Now delete some
      NeonUserAccount.delete_many([x.neon_api_key for x in objs[1:3]])
      
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonUserAccount('a1')._set_keyname()),
        [objs[0].key, objs[3].key])

    @tornado.testing.gen_test
    def test_create_list_of_namespaced_objects_async(self):
      objs = [NeonUserAccount('a%i' % i) for i in range(4)]
      yield tornado.gen.Task(objs[0].save)
      yield tornado.gen.Task(NeonUserAccount.save_all, objs[1:3])
      yield tornado.gen.Task(NeonUserAccount.modify, 
                             objs[3].neon_api_key,
                             lambda x: x, create_missing=True)

      conn = neondata.DBConnection.get(NeonUserAccount)
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonUserAccount('a1')._set_keyname()),
        [x.key for x in objs])

      # Now delete some
      yield tornado.gen.Task(NeonUserAccount.delete_many,
                             [x.neon_api_key for x in objs[1:3]])
      
      self.assertItemsEqual(conn.blocking_conn.smembers(
        NeonUserAccount('a1')._set_keyname()),
        [objs[0].key, objs[3].key])

class TestNeondataDataSpecific(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestNeondataDataSpecific, self).setUp()
        self.maxDiff = 5000
        logging.getLogger('cmsdb.neondata').reset_sample_counters()

    def tearDown(self):
        conn = neondata.DBConnection.get(VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(ThumbnailMetadata)
        conn.clear_db()
        super(TestNeondataDataSpecific, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()

    def test_neon_api_key(self):
        ''' test api key generation '''
        #Test key is random on multiple generations
        a_id = "testaccount"
        api_key_1 = NeonApiKey.generate(a_id)
        api_key_2 = NeonApiKey.generate("12")
        self.assertNotEqual(api_key_1, api_key_2)

        #create neon account and verify its api key
        a_id = 'test_account1'
        na = NeonUserAccount(a_id)
        na.save()

        api_key_from_db = NeonApiKey.get_api_key(a_id)
        self.assertEqual(na.neon_api_key, api_key_from_db)

    def test_get_all_accounts(self):
        ''' test get all neonuser accounts '''

        a_id_prefix = 'test_account_'
        a_ids = []
        for i in range(10):
            a_id = a_id_prefix + str(i)
            a_ids.append(a_id)
            na = NeonUserAccount(a_id)
            na.save()

        nu_accounts = NeonUserAccount.get_all_accounts()
        nu_a_ids = [nu.account_id for nu in nu_accounts]
        self.assertItemsEqual(a_ids, nu_a_ids)

    @tornado.testing.gen_test
    def test_get_all_accounts_async(self):
        a_id_prefix = 'test_account_'
        a_ids = []
        for i in range(10):
            a_id = a_id_prefix + str(i)
            a_ids.append(a_id)
            na = NeonUserAccount(a_id)
            na.save()

        nu_accounts = yield NeonUserAccount.get_all(async=True)
        nu_a_ids = [nu.account_id for nu in nu_accounts]
        self.assertItemsEqual(a_ids, nu_a_ids)
        
    def test_iterate_all_accounts(self):
        a_id_prefix = 'test_account_'
        a_ids = []
        for i in range(4):
            a_id = a_id_prefix + str(i)
            a_ids.append(a_id)
            na = NeonUserAccount(a_id)
            na.save()

        nu_accounts = list(NeonUserAccount.iterate_all(max_request_size=2))
        nu_a_ids = [nu.account_id for nu in nu_accounts]
        self.assertItemsEqual(a_ids, nu_a_ids)

    def test_get_all_requests(self):
        na1 = NeonUserAccount('a1', 'acct1')
        na1.save()
        na3 = NeonUserAccount('a3', 'acct3')
        na3.save()
        requests1 = [NeonApiRequest('job1', 'acct1'),
                     neondata.BrightcoveApiRequest('jobbc', 'acct1')]

        requests3 = [neondata.OoyalaApiRequest('joboo', 'acct3')]
        NeonApiRequest.save_all(requests1)
        NeonApiRequest.save_all(requests3)

        requests_found = NeonApiRequest.get_many(na1.get_all_job_keys())
        self.assertEqual(sorted(requests_found), sorted(requests1))

        requests_found = NeonApiRequest.get_many(na3.get_all_job_keys())
        self.assertEqual(requests_found, requests3)

    @tornado.testing.gen_test
    def test_get_all_requests_async(self):
        na1 = NeonUserAccount('a1', 'acct1')
        na1.save()
        na3 = NeonUserAccount('a3', 'acct3')
        na3.save()
        requests1 = [NeonApiRequest('job1', 'acct1'),
                     neondata.BrightcoveApiRequest('jobbc', 'acct1')]

        requests3 = [neondata.OoyalaApiRequest('joboo', 'acct3')]
        NeonApiRequest.save_all(requests1)
        NeonApiRequest.save_all(requests3)

        keys = yield na1.get_all_job_keys(async=True)
        requests_found = yield tornado.gen.Task(NeonApiRequest.get_many, keys)
        self.assertEqual(sorted(requests_found), sorted(requests1))

        keys = yield na3.get_all_job_keys(async=True)
        requests_found = yield tornado.gen.Task(NeonApiRequest.get_many, keys)
        self.assertEqual(requests_found, requests3)

    def test_iterate_all_jobs_sync(self):
        na = NeonUserAccount('a1', 'acct1')
        na.save()
        requests1 = [NeonApiRequest('job1', 'acct1'),
                     neondata.BrightcoveApiRequest('jobbc', 'acct1')]
        NeonApiRequest.save_all(requests1)
        req = NeonApiRequest.get_many([('acct1', 'job1')])
        found_jobs = list(na.iterate_all_jobs(max_request_size=1))
        self.assertEqual(sorted(found_jobs), sorted(requests1))

    @tornado.testing.gen_test
    def test_iterate_all_videos_async(self):
        na = NeonUserAccount('a1', 'key1')
        na.save()

        for i in range(5):
            yield tornado.gen.Task(VideoMetadata('key1_v%d' % i).save)

        found_videos = []
        iterator = yield na.iterate_all_videos(async=True, max_request_size=2)
        while True:
            item = yield iterator.next(async=True)
            if isinstance(item, StopIteration):
                break
            found_videos.append(item)

        self.assertItemsEqual([x.key for x in found_videos],
                              ['key1_v%d' % i for i in range(5)])


    def test_iterate_all_videos_sync(self):
        na = NeonUserAccount('a1', 'key1')
        na.save()

        for i in range(5):
            VideoMetadata('key1_v%d' % i).save()
        
        found_videos = list(na.iterate_all_videos(max_request_size=2))
        self.assertItemsEqual([x.key for x in found_videos],
                              ['key1_v%d' % i for i in range(5)])

    def test_get_all_trackerids(self):
        ''' test get all the tracker ids '''
        expected = []
        for i in range(10):
            tai = 'tracker_%i' % i
            acct_id = 'account_%i' % i
            expected.append((tai, acct_id))
            TrackerAccountIDMapper(tai, acct_id,
                                   TrackerAccountIDMapper.PRODUCTION).save()

        found_maps = TrackerAccountIDMapper.get_all()
        self.assertItemsEqual(expected,
                              [(x.get_tai(), x.value) for x in found_maps])
        self.assertEqual(
                TrackerAccountIDMapper.get_neon_account_id('tracker_3'),
                ('account_3', TrackerAccountIDMapper.PRODUCTION))

    def test_get_all_platforms_sync(self):
        NeonPlatform.modify('acct1', '0', lambda x: x,
                            create_missing=True)
        BrightcovePlatform.modify('acct2', 'ibc', lambda x: x,
                                  create_missing=True)
        OoyalaPlatform.modify('acct3', 'ioo', lambda x: x,
                              create_missing=True)
        YoutubePlatform.modify('acct4', 'iyt', lambda x: x,
                               create_missing=True)

        objs = AbstractPlatform.get_all()

        self.assertEquals(len(objs), 4)

    @tornado.testing.gen_test
    def test_get_all_platforms_async(self):
        NeonPlatform.modify('acct1', '0', lambda x: x,
                            create_missing=True)
        BrightcovePlatform.modify('acct2', 'ibc', lambda x: x,
                            create_missing=True)
        OoyalaPlatform.modify('acct3', 'ioo', lambda x: x,
                            create_missing=True)
        YoutubePlatform.modify('acct4', 'iyt', lambda x: x,
                            create_missing=True)

        objs = yield AbstractPlatform.get_all(async=True)

        self.assertEquals(len(objs), 4)

    def test_default_bcplatform_settings(self):
        ''' brightcove defaults ''' 

        na = NeonUserAccount('acct1')
        bp = BrightcovePlatform(na.neon_api_key, 'iid', 'aid')

        self.assertFalse(bp.abtest)
        self.assertFalse(bp.auto_update)
        self.assertTrue(bp.serving_enabled)
        self.assertNotEqual(bp.neon_api_key, '')
        self.assertEqual(bp.key, 'brightcoveplatform_%s_iid' % bp.neon_api_key)

        # Make sure that save and regenerating creates the same object
        def _set_acct(x):
            x.account_id = 'aid'
        bp = BrightcovePlatform.modify(na.neon_api_key, 'iid', _set_acct,
                                       create_missing=True)

        bp2 = BrightcovePlatform.get(na.neon_api_key, 'iid')
        self.assertEqual(bp.__dict__, bp2.__dict__)

    def test_bcplatform_with_callback(self):

        na = NeonUserAccount('acct1')
        bp = BrightcovePlatform('aid', 'iid', na.neon_api_key,
                                callback_url='http://www.callback.com')

        self.assertFalse(bp.abtest)
        self.assertFalse(bp.auto_update)
        self.assertTrue(bp.serving_enabled)
        self.assertNotEqual(bp.neon_api_key, '')
        self.assertEqual(bp.key, 'brightcoveplatform_%s_iid' % bp.neon_api_key)
        self.assertEqual(bp.callback_url, 'http://www.callback.com')

    def test_neon_user_account(self):
        ''' nuser account '''

        na = NeonUserAccount('acct1')
        def _set_acct(x):
            x.account_id = 'acct1'
        bp = BrightcovePlatform.modify(na.neon_api_key, 'bp1',
                                       _set_acct, create_missing=True)
        np = NeonPlatform.modify(na.neon_api_key, '0',
                                 _set_acct, create_missing=True)
        na.add_platform(bp)
        na.add_platform(np)
        na.save()

        # Retrieve the account
        NeonUserAccount.get(na.neon_api_key,
                            callback=self.stop)
        account = self.wait()
        self.assertIsNotNone(account)
        self.assertEqual(account.account_id, 'acct1')
        self.assertEqual(account.tracker_account_id, na.tracker_account_id)

        # Get the platforms
        account.get_platforms(callback=self.stop)
        platforms = self.wait()
        self.assertItemsEqual([x.__dict__ for x in platforms],
                              [x.__dict__ for x in [bp, np]])

        # Try retrieving the platforms synchronously
        platforms = account.get_platforms()
        self.assertItemsEqual([x.__dict__ for x in platforms],
                              [x.__dict__ for x in [bp, np]])
    
    def test_neon_user_account_save(self):
        na = NeonUserAccount('acct1')
        na.save()

        # fetch the api key and verify its the same as this account
        api_key = NeonApiKey.get_api_key('acct1')
        self.assertEqual(na.neon_api_key, api_key)

        # create account again
        na1 = NeonUserAccount('acct1')  

        # ensure that the old api key is still in tact
        api_key = NeonApiKey.get_api_key('acct1')
        self.assertEqual(na.neon_api_key, api_key)

    def test_delete_all_video_related_data(self):
        # create all video related objects
        
        na = NeonUserAccount('ta1')
        bp = BrightcovePlatform.modify(na.neon_api_key, 'bp1', lambda x: x,
                                       create_missing=True)
        np = NeonPlatform.modify(na.neon_api_key, '0', lambda x: x,
                                 create_missing=True)
        na.add_platform(bp)
        na.add_platform(np)
        na.save()

        req = NeonApiRequest('job1', na.neon_api_key, 'vid1', 't', 't', 'r', 'h')
        i_vid = InternalVideoID.generate(na.neon_api_key, 'vid1')
        tid = i_vid + "_t1"
        thumb = ThumbnailMetadata(tid, i_vid, ['t1.jpg'], None, None, None,
                              None, None, None)
        vid = VideoMetadata(i_vid, [thumb.key],
                           'job1', 'v0.mp4', 0, 0, None, '0')
        req.save()
        ThumbnailMetadata.save_all([thumb])
        VideoMetadata.save_all([vid])
        def modify_me(x): 
            x.add_video('dummyv', 'dummyjob') 
            x.add_video('vid1', 'job1') 
        #NeonPlatform.modify(na.neon_api_key, '0', modify_me) 
        #                    lambda x: x.add_video('dummyv', 'dummyjob'))
        np = NeonPlatform.modify(na.neon_api_key, '0', modify_me) 
#                                 lambda x: x.add_video('vid1', 'job1'))
        np = NeonPlatform.get(na.neon_api_key, '0') 
        np.delete_all_video_related_data('vid1', really_delete_keys=True)
        
        # check the keys have been deleted
        self.assertIsNone(NeonApiRequest.get('job1', na.neon_api_key))
        self.assertIsNone(ThumbnailMetadata.get(thumb.key))
        self.assertIsNone(VideoMetadata.get(i_vid))

        # verify account
        np = NeonPlatform.get(na.neon_api_key, '0')
        self.assertListEqual(np.get_videos(), [u'dummyv'])
        
        #TODO: Add more failure test cases

    def test_delete_video_data(self):
        api_key = 'key'

        NeonApiRequest('job1', api_key, 'vid1').save()
        i_vid = InternalVideoID.generate(api_key, 'vid1')
        tid = i_vid + "_t1"
        ThumbnailMetadata(tid, i_vid).save()
        VideoMetadata(i_vid, [tid],'job1').save()
        neondata.VideoStatus(i_vid, 'complete').save()
        neondata.ThumbnailStatus(tid, 0.2).save()
        ThumbnailServingURLs(tid, sizes=[(640,480)]).save()

        VideoMetadata.delete_related_data(i_vid)

        self.assertIsNone(VideoMetadata.get(i_vid))
        self.assertEquals(neondata.VideoStatus.get(i_vid).experiment_state,
                          'unknown')
        self.assertIsNone(NeonApiRequest.get('job1', api_key))
        self.assertIsNone(ThumbnailMetadata.get(tid))
        self.assertIsNone(ThumbnailServingURLs.get(tid))
        self.assertIsNone(neondata.ThumbnailStatus.get(tid).serving_frac)
        
    def test_ThumbnailServingURLs(self):
        input1 = ThumbnailServingURLs('acct1_vid1_tid1')
        input1.add_serving_url(
            'http://neon.com/neontnacct1_vid1_tid1_w800_h600.jpg', 800, 600)
        input1.add_serving_url(
            'http://neon.com/neontnacct1_vid1_tid1_w100_h50.jpg', 100, 50)
        
        input1.save()
        output1 = ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertEqual(output1.get_thumbnail_id(), input1.get_thumbnail_id())
        self.assertEqual(output1.get_serving_url(800, 600),
                         'http://neon.com/neontnacct1_vid1_tid1_w800_h600.jpg')
        with self.assertRaises(KeyError):
            output1.get_serving_url(640, 480)
        self.assertItemsEqual(
            list(input1),
            [((800, 600), 'http://neon.com/neontnacct1_vid1_tid1_w800_h600.jpg'),
             ((100, 50), 'http://neon.com/neontnacct1_vid1_tid1_w100_h50.jpg')])
        self.assertItemsEqual(list(input1), list(output1))

        input1.add_serving_url('http://neon.com/neontnacct1_vid1_tid1_w640_h480.jpg',
                               640, 480) 
        input2 = ThumbnailServingURLs(
            'acct1_vid1_tid2',
            {(640, 480) : 'http://neon.com/neontnacct1_vid1_tid2_w640_h480.jpg'})
        ThumbnailServingURLs.save_all([input1, input2])
        output1, output2 = ThumbnailServingURLs.get_many(['acct1_vid1_tid1',
                                                          'acct1_vid1_tid2'])
        self.assertEqual(output1.get_thumbnail_id(),
                         input1.get_thumbnail_id())
        self.assertEqual(output2.get_thumbnail_id(),
                         input2.get_thumbnail_id())
        self.assertEqual(output1.get_serving_url(640, 480),
                         'http://neon.com/neontnacct1_vid1_tid1_w640_h480.jpg')
        self.assertEqual(output1.get_serving_url(800, 600),
                         'http://neon.com/neontnacct1_vid1_tid1_w800_h600.jpg')
        self.assertEqual(output2.get_serving_url(640, 480),
                         'http://neon.com/neontnacct1_vid1_tid2_w640_h480.jpg')

    def test_thumbnail_servingurl_base_url(self):
        input1 = ThumbnailServingURLs('acct1_vid1_tid1',
                                      base_url='http://neon-images.com/y6w')
        url800 = \
          'http://neon-images.com/y6w/neontnacct1_vid1_tid1_w800_h600.jpg'
        input1.add_serving_url(url800, 800, 600)
        url160 = 'http://neon-images.com/ppw/neontnacct1_vid1_tid1_w160_h90.jpg'
        with self.assertLogExists(logging.WARNING, 'url.*does not conform'):
            input1.add_serving_url(url160, 160, 90)
        self.assertEqual(input1.get_serving_url(800, 600), url800)
        self.assertEqual(input1.get_serving_url(160, 90), url160)
        input1.save()

        output1 = ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertEqual(input1.get_serving_url(800, 600),
                         output1.get_serving_url(800, 600))
        self.assertEqual(input1.get_serving_url(160, 90),
                         output1.get_serving_url(160, 90))
        

    def test_backwards_compatible_thumb_serving_urls_diff_base(self):
        json_str = "{\"_type\": \"ThumbnailServingURLs\", \"_data\": {\"size_map\": [[[210, 118], \"http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w210_h118.jpg\"], [[160, 90], \"http://n3.neon-images.com/EaE/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w160_h90.jpg\"], [[1280, 720], \"http://n3.neon-images.com/ZZc/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w1280_h720.jpg\"]], \"key\": \"thumbnailservingurls_b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8\"}}"

        obj = ThumbnailServingURLs._create('thumbnailservingurls_b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8', json.loads(json_str))

        # It's different base urls so we keep the object in the old format
        self.assertEquals(len(obj.size_map), 3)
        self.assertEquals(obj.get_serving_url_count(), 3)
        self.assertIsNone(obj.base_url)
        self.assertEquals(len(obj.sizes), 0)
        self.assertEquals(obj.get_serving_url(160, 90),
                          'http://n3.neon-images.com/EaE/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w160_h90.jpg')

    def test_backwards_compatible_thumb_serving_urls_same_base(self):
        json_str = "{\"_type\": \"ThumbnailServingURLs\", \"_data\": {\"size_map\": [[[210, 118], \"http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w210_h118.jpg\"], [[160, 90], \"http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w160_h90.jpg\"], [[1280, 720], \"http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w1280_h720.jpg\"]], \"key\": \"thumbnailservingurls_b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8\"}}"

        obj = ThumbnailServingURLs._create('thumbnailservingurls_b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8', json.loads(json_str))

        # It's the same base url so store in the new format
        self.assertEquals(len(obj.size_map), 0)
        self.assertEquals(obj.get_serving_url_count(), 3)
        self.assertEquals(obj.base_url, 'http://n3.neon-images.com/fF7')
        self.assertEquals(obj.sizes, set([(210,118), (160,90), (1280,720)]))
        self.assertEquals(obj.get_serving_url(160, 90),
                          'http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w160_h90.jpg')
        self.assertEquals(obj.get_thumbnail_id(),
                          'b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8')
    def test_hosting_metadata(self):
        '''
        Test saving and retrieving CDNHostingMetadataList object
        '''
        cloud = CloudinaryCDNHostingMetadata()
        self.assertFalse(cloud.resize)
        self.assertFalse(cloud.update_serving_urls)
        neon_cdn = NeonCDNHostingMetadata(None, 
                                          'my-bucket',
                                          ['mycdn.neon-lab.com'],
                                          rendition_sizes=[[360, 240]])
        self.assertTrue(neon_cdn.resize)
        self.assertTrue(neon_cdn.update_serving_urls)
        self.assertEqual(neon_cdn.folder_prefix, None)
        s3_cdn = S3CDNHostingMetadata(None,
                                      'access-key',
                                      'secret-key',
                                      'customer-bucket',
                                      ['cdn.cdn.com'],
                                      'folder/',
                                      True,
                                      True)
        cdns = CDNHostingMetadataList(
            CDNHostingMetadataList.create_key('api-key', 'integration0'),
            [cloud, neon_cdn, s3_cdn])
        cdns.save()

        new_cdns = CDNHostingMetadataList.get(
            CDNHostingMetadataList.create_key('api-key', 'integration0'))

        self.assertEqual(cdns, new_cdns)

    def test_hosting_metadata_bad_classname_in_json(self):
        neon_cdn = NeonCDNHostingMetadata(None,
                                          'my-bucket',
                                          ['mycdn.neon-lab.com'])
        cdns = CDNHostingMetadataList('api-key_integration0',
                                      [neon_cdn])
        good_json = cdns.to_json()
        bad_json = re.sub('NeonCDNHostingMetadata', 'UnknownHostingMetadata',
                          good_json)

        with self.assertLogExists(logging.ERROR,
                                  'Unknown class .* UnknownHostingMetadata'):
            cdn_list = CDNHostingMetadataList._create(
                'api-key_integration0',
                json.loads(bad_json))
            self.assertIsNone(cdn_list)

    def test_hosting_metadata_list_bad_key(self):
        with self.assertRaises(ValueError):
            CDNHostingMetadataList('integration0')

        with self.assertRaises(ValueError):
            CDNHostingMetadataList('acct_1_integration0')

    def test_old_akamai_hosting_objects(self):
        folder_in_cdn_prefixes = neondata.AkamaiCDNHostingMetadata._create(
            None,
            {'akamai_key':'Bt9lrfGL9hGBs7APtg10AfCUti5FeDrJ65dZIFVTxuvd33H74C',
             'akamai_name': 'neonuser',
             'update_serving_urls': True,
             'baseurl': '/17200/neon/prod',
             'rendition_sizes': [[120, 67]],
             'host': 'http://usatoday-nsu.akamaihd.net',
             'key': None,
             'cdn_prefixes': ['www.gannett-cdn.com/neon/prod'],
             'resize': True})
        self.assertEquals(folder_in_cdn_prefixes.cpcode, '17200')
        self.assertEquals(folder_in_cdn_prefixes.folder_prefix, 'neon/prod')
        self.assertEquals(folder_in_cdn_prefixes.cdn_prefixes,
                          ['http://www.gannett-cdn.com'])

        folder_not_in_cdn_prefixes = neondata.AkamaiCDNHostingMetadata._create(
            None,
            {'akamai_key': 'Igi93b3mTZ1Cq30N0dY0G39NjZq92nIGtUclw23hm2Iy7RF2Nj',
             'akamai_name': 'neon',
             'update_serving_urls': True,
             'baseurl': '/386270/neon/prod',
             'rendition_sizes': [[50, 50]],
             'host': 'http://tvaiharmony-nsu.akamaihd.net',
             'key': None,
             'cdn_prefixes': ['static.neon.groupetva.ca'],
             'resize': True})

        self.assertEquals(folder_not_in_cdn_prefixes.cpcode, '386270')
        self.assertEquals(folder_not_in_cdn_prefixes.folder_prefix,
                          'neon/prod')
        self.assertEquals(folder_not_in_cdn_prefixes.cdn_prefixes,
                          ['http://static.neon.groupetva.ca'])

    def test_old_s3_hosting_list(self):
        old_str = "{\"_type\": \"CDNHostingMetadataList\", \"_data\": {\"_id\": \"9xmw08l4ln1rk8uhv3txwbg1_0\", \"cdns\": [{\"_type\": \"S3CDNHostingMetadata\", \"_data\": {\"access_key\": \"AKIAJZOPH5BBEXRQFCKA\", \"folder_prefix\": \"thumbs/neon/\", \"update_serving_urls\": true, \"bucket_name\": \"o.assets.ign.com\", \"key\": null, \"cdn_prefixes\": [\"assets.ign.com\", \"assets2.ignimgs.com\"], \"secret_key\": \"sdErEhAMR1XARhQ8qjKH4P4ZTjCf1WiU6+lKV4aL\", \"do_salt\": false, \"resize\": true}}], \"key\": \"cdnhostingmetadatalist_9xmw08l4ln1rk8uhv3txwbg1_0\"}}"

        obj_dict = json.loads(old_str)
        cdn_list = neondata.CDNHostingMetadataList._create(
            obj_dict['_data']['key'],
            obj_dict)
        self.assertEquals(cdn_list.cdns[0].cdn_prefixes,
                          ["http://assets.ign.com",
                           "http://assets2.ignimgs.com"])
        self.assertEquals(cdn_list.cdns[0].bucket_name,
                          'o.assets.ign.com')
        self.assertEquals(cdn_list.cdns[0].folder_prefix,
                          'thumbs/neon/')
        

    def test_internal_video_id(self):
        '''
        Generate bunch of random video ids, covert them to internal VID
        and try to get external VID out of them.
        '''
        random.seed(time.time())
        def id_generator(size=32, 
                chars=string.ascii_lowercase + string.digits + "_"):
            
            return ''.join(random.choice(chars) for x in range(size))
        
        external_vids = ['5ieGdqMjoiVJJjw7YIZk5fMBvIE86Z1c', 
                'xhdG5nMjoiKNbeAz0UrQo2_YVPcZRng8', '12451561361', 
                'R4YjBnMjrzRQRcDLf34bXbRH4qR6CEF1', 'vid_3' ]

        for i in range(100):
            external_vids.append(id_generator())

        i_vids = [InternalVideoID.generate("apikey", vid) for vid in external_vids]

        result = [InternalVideoID.to_external(i_vid) for i_vid in i_vids]

        self.assertEqual(result, external_vids)

    def test_get_winner_tid(self):
        '''
        Test the get_winner_tid logic for a given video id

        '''
        vid = InternalVideoID.generate('acct1', 'vid1')
        video_meta = VideoMetadata(vid, ['acct1_vid1_t1', 'acct1_vid1_t2'],
                                    'reqid0','v0.mp4')
        video_meta.save()
        neondata.VideoStatus(vid, winner_tid='acct1_vid1_t2').save()
        self.assertEquals(video_meta.get_winner_tid(), 'acct1_vid1_t2')

    def test_video_status_history(self):
        vid = InternalVideoID.generate('acct1', 'vid1')
        neondata.VideoStatus(vid).save()
        video_status = neondata.VideoStatus.get(vid)
        self.assertEquals(video_status.experiment_state, 
                          neondata.ExperimentState.UNKNOWN)
        self.assertEquals(video_status.state_history, [])
        def _update(status):
            status.set_experiment_state(neondata.ExperimentState.COMPLETE)
        neondata.VideoStatus.modify(vid, _update)
        video_status = neondata.VideoStatus.get(vid)
        self.assertEquals(video_status.experiment_state, 
                          neondata.ExperimentState.COMPLETE)
        self.assertEquals(video_status.state_history[0][1],
                          neondata.ExperimentState.COMPLETE)
        new_time = dateutil.parser.parse(
            video_status.state_history[0][0])
        self.assertLess(
            (datetime.datetime.utcnow() - new_time).total_seconds(),
            600)

    def test_video_metadata_methods(self):
        '''
        Currently only Tests the video_requests methods 
        '''
        api_key = "TEST"
        job_ids = []
        i_vids = []
        for i in range(10):
            jid = 'job%s' % i
            vid = 'vid%s' % i 
            i_vid = "%s_%s" % (api_key, vid)
            nar = NeonApiRequest(jid, api_key, vid, 't', 't', 'r', 'h')
            vm = VideoMetadata(i_vid, [], jid, 'v0.mp4')
            nar.save()
            vm.save()
            i_vids.append(i_vid)

        reqs = VideoMetadata.get_video_requests(i_vids)
        for jid, req in zip(job_ids, reqs):
            self.assertEqual(jid, req.job_id)
       
        # Non existent video
        i_vids = ["dummy_vid"]
        reqs = VideoMetadata.get_video_requests(i_vids)
        self.assertEqual(reqs, [None])

    # TODO(Sunil): move the test to VideoMetadata specific tests 
    def test_neon_serving_url(self):
        '''
        Test Serving URL generation and management
        '''
        
        na = NeonUserAccount('acct1')
        na.save()
        np = NeonPlatform.modify(na.neon_api_key, '0', lambda x: x,
                                 create_missing=True)
       
        vid = InternalVideoID.generate(na.neon_api_key, 'vid1')
        vm = VideoMetadata(vid, [], 'reqid0', 'v0.mp4', 0, 0, None, 0, None, True)
        vm.save()
        
        # check staging URL first, since it isn't saved in the DB
        staging_url = vm.get_serving_url(staging=True)
        serving_format = "neon-images.com/v1/client/%s/neonvid_%s.jpg"
        expected_url = serving_format % (na.staging_tracker_account_id, 'vid1')
        self.assertTrue(expected_url in staging_url)
        
        s_url = vm.get_serving_url()
        serving_format = "neon-images.com/v1/client/%s/neonvid_%s.jpg"
        expected_url = serving_format % (na.tracker_account_id, 'vid1')
        
        # ignore http://i{} and check if there is a substring match
        self.assertTrue(expected_url in s_url)

        # Check serving URL is in the VM object
        vm = VideoMetadata.get(vid) 
        self.assertTrue(expected_url in vm.serving_url)
        
        # check staging URL
        staging_url = vm.get_serving_url(staging=True)
        expected_url = serving_format % (na.staging_tracker_account_id, 'vid1')
        self.assertTrue(expected_url in staging_url)


    @patch('cmsdb.neondata.VideoMetadata.get')
    def test_save_default_thumbnail(self, get_video_mock): 
        get_video_mock.side_effect = lambda x, callback: callback(None)

        # Start with no default thumbnail specified
        api_request = NeonApiRequest('job1', 'acct1', 'vid1', 'title',
                                     'video.mp4', 'neon', None,
                                     default_thumbnail=None)
        api_request.save_default_thumbnail()
        self.assertEquals(get_video_mock.call_count, 0)

        # Add the video data to the database
        video = VideoMetadata('acct1_vid1')
        add_thumb_mock = MagicMock()
        video.download_and_add_thumbnail = add_thumb_mock
        add_future = Future()
        add_future.set_result(MagicMock())
        add_thumb_mock.return_value = add_future
        get_video_mock.side_effect = lambda x, callback: callback(video)
        # Use the default_thumbnail attribute
        api_request.default_thumbnail = 'new_thumbnail'
        api_request.save_default_thumbnail()
        thumbmeta, url_seen, cdn = add_thumb_mock.call_args[0]
        self.assertEquals(url_seen, 'new_thumbnail')
        add_thumb_mock.reset_mock()

        # Add a different default to the database, so rank should be lower
        thumb = ThumbnailMetadata('acct1_vid1_thumb1', 
                                  urls=['other_default.jpg'], 
                                  rank=0, ttype=ThumbnailType.DEFAULT)
        thumb.save()
        video.thumbnail_ids.append(thumb.key)
        api_request.save_default_thumbnail()
        self.assertEquals(add_thumb_mock.call_count, 1)
        thumbmeta, url_seen, cdn = add_thumb_mock.call_args[0]
        self.assertEquals(url_seen, 'new_thumbnail')
        self.assertEquals(thumbmeta.rank, -1)
        self.assertEquals(thumbmeta.type, ThumbnailType.DEFAULT)
        add_thumb_mock.reset_mock()

        # Try to add the same default again and it shouldn't be
        # added. Need to add it to the database manually because we
        # mocked out the call that does that.
        thumb = ThumbnailMetadata('acct1_vid1_thumb2', 
                                  urls=['new_thumbnail'], 
                                  rank=-1, ttype=ThumbnailType.DEFAULT)
        thumb.save()
        video.thumbnail_ids.append(thumb.key)
        api_request.save_default_thumbnail()
        self.assertEquals(add_thumb_mock.call_count, 0)

    def test_api_request(self):
        # Make sure that the Api Requests are saved and read from the
        # database consistently.
        bc_request = neondata.BrightcoveApiRequest(
            'bc_job', 'api_key', 'vid0', 'title',
            'url', 'rtoken', 'wtoken', 'pid',
            'callback_url', 'i_id',
            'default_thumbnail')
        bc_request.save()

        bc_found = NeonApiRequest.get('bc_job', 'api_key')
        self.assertEquals(bc_found.key, 'request_api_key_bc_job')
        self.assertEquals(bc_request, bc_found)
        self.assertIsInstance(bc_found, neondata.BrightcoveApiRequest)

        oo_request = neondata.OoyalaApiRequest(
            'oo_job', 'api_key', 'i_id', 'vid0', 'title',
            'url', 'oo_api_key', 'oo_secret_key',
            'callback_url', 'default_thumbnail')
        oo_request.save()

        self.assertEquals(oo_request, NeonApiRequest.get('oo_job', 'api_key'))

        yt_request = neondata.YoutubeApiRequest(
            'yt_job', 'api_key', 'vid0', 'title',
            'url', 'access_token', 'request_token', 'expiry',
            'callback_url', 'default_thumbnail')
        yt_request.save()

        self.assertEquals(yt_request, NeonApiRequest.get('yt_job', 'api_key'))

        n_request = NeonApiRequest(
            'n_job', 'api_key', 'vid0', 'title',
            'url', 'neon', 'callback_url', 'default_thumbnail')
        n_request.save()

        self.assertEquals(n_request, NeonApiRequest.get('n_job', 'api_key'))

    def test_neon_api_request_publish_date(self):
        json_str="{\"api_method\": \"topn\", \"video_url\": \"http://brightcove.vo.llnwd.net/pd16/media/136368194/201407/1283/136368194_3671520771001_linkasia2014071109-lg.mp4\", \"model_version\": \"20130924\", \"job_id\": \"e38ef7abba4c9102b26feb90bc5df3a8\", \"state\": \"serving\", \"api_param\": 1, \"api_key\": \"dhfaagb0z0h6n685ntysas00\", \"publisher_id\": \"136368194\", \"integration_type\": \"neon\", \"autosync\": false, \"request_type\": \"brightcove\", \"key\": \"request_dhfaagb0z0h6n685ntysas00_e38ef7abba4c9102b26feb90bc5df3a8\", \"submit_time\": \"1405130164.16\", \"integration_id\": \"35\", \"read_token\": \"rgkAluxK9pAC26XCRusctnSfWwzrujq9cTRdmrNpWU4.\", \"video_id\": \"3671481626001\", \"previous_thumbnail\": \"https://host-thumbnails.s3.amazonaws.com/dhfaagb0z0h6n685ntysas00/e38ef7abba4c9102b26feb90bc5df3a8/brightcove.jpeg\", \"publish_date\": \"2014-07-12T01:36:36Z\", \"callback_url\": \"http://localhost:8081/testcallback\", \"write_token\": \"v4OZjhHCkoFOqlNFJZLBA-KcbnNUhtQjseDXO9Y4dyA.\", \"video_title\": \"How Was China's Xi Jinping Welcomed in South Korea?\"}"
        
        obj = NeonApiRequest._create('request_dhfaagb0z0h6n685ntysas00_e38ef7abba4c9102b26feb90bc5df3a8', json.loads(json_str))
        self.assertEquals(obj.publish_date, '2014-07-12T01:36:36Z')

        
    def test_neon_api_request_backwards_compatibility(self):
        json_str="{\"api_method\": \"topn\", \"video_url\": \"http://brightcove.vo.llnwd.net/pd16/media/136368194/201407/1283/136368194_3671520771001_linkasia2014071109-lg.mp4\", \"model_version\": \"20130924\", \"job_id\": \"e38ef7abba4c9102b26feb90bc5df3a8\", \"state\": \"serving\", \"api_param\": 1, \"api_key\": \"dhfaagb0z0h6n685ntysas00\", \"publisher_id\": \"136368194\", \"integration_type\": \"neon\", \"autosync\": false, \"request_type\": \"brightcove\", \"key\": \"request_dhfaagb0z0h6n685ntysas00_e38ef7abba4c9102b26feb90bc5df3a8\", \"submit_time\": \"1405130164.16\", \"response\": {\"job_id\": \"e38ef7abba4c9102b26feb90bc5df3a8\", \"timestamp\": \"1405130266.52\", \"video_id\": \"3671481626001\", \"error\": null, \"data\": [7139.97], \"thumbnails\": [\"https://host-thumbnails.s3.amazonaws.com/dhfaagb0z0h6n685ntysas00/e38ef7abba4c9102b26feb90bc5df3a8/neon0.jpeg\"]}, \"integration_id\": \"35\", \"read_token\": \"rgkAluxK9pAC26XCRusctnSfWwzrujq9cTRdmrNpWU4.\", \"video_id\": \"3671481626001\", \"previous_thumbnail\": \"https://host-thumbnails.s3.amazonaws.com/dhfaagb0z0h6n685ntysas00/e38ef7abba4c9102b26feb90bc5df3a8/brightcove.jpeg\", \"publish_date\": 1405128996278, \"callback_url\": \"http://localhost:8081/testcallback\", \"write_token\": \"v4OZjhHCkoFOqlNFJZLBA-KcbnNUhtQjseDXO9Y4dyA.\", \"video_title\": \"How Was China's Xi Jinping Welcomed in South Korea?\"}"

        obj = NeonApiRequest._create('request_dhfaagb0z0h6n685ntysas00_e38ef7abba4c9102b26feb90bc5df3a8', json.loads(json_str))

        self.assertIsInstance(obj, neondata.BrightcoveApiRequest)
        self.assertEquals(obj.read_token,
                          'rgkAluxK9pAC26XCRusctnSfWwzrujq9cTRdmrNpWU4.')
        self.assertEquals(
            obj.get_id(),
            ('e38ef7abba4c9102b26feb90bc5df3a8', 'dhfaagb0z0h6n685ntysas00'))
        self.assertEquals(obj.publish_date, '2014-07-12T01:36:36.278000')

    def test_neonplatform_account_backwards_compatibility(self):
        json_str = "{\"integration_id\": \"0\", \"account_id\": \"161\", \"videos\": {}, \"abtest\": false, \"neon_api_key\": \"hzxts57y7ywcl9onl811b0p4\", \"key\": \"neonplatform_hzxts57y7ywcl9onl811b0p4_0\"}"
        obj = NeonPlatform._create('neonplatform_hzxts57y7ywcl9onl811b0p4_0', json.loads(json_str))
        self.assertEqual(obj.account_id, '161')
        self.assertEqual(obj.neon_api_key, 'hzxts57y7ywcl9onl811b0p4')
        
        # save the object and ensure that its "savable", then verify
        # contents again
        def _set_params(x):
            x.account_id = obj.account_id
            x.abtest = True
        NeonPlatform.modify(obj.neon_api_key, obj.integration_id,
                            _set_params, create_missing=True)
        
        obj = NeonPlatform.get(obj.neon_api_key, '0')
        self.assertEqual(obj.account_id, '161')
        self.assertEqual(obj.neon_api_key, 'hzxts57y7ywcl9onl811b0p4')
        self.assertTrue(obj.abtest)

    def test_brightcoveplatform_account_backwards_compatibility(self):
        json_str = "{\"integration_id\": \"39\", \"account_id\": \"145\", \"videos\": {\"3579581800001\": \"ebc8d852519d582d4819fc94c29d55df\", \"2007541443001\": \"791ab73928e9f4ea83d8de164f9c93f8\", \"3696935732001\": \"812ae49b93338b15665305b09b3964f1\", \"2007730256001\": \"37462869b0a3d1a735dfedd85556635d\", \"3582347795001\": \"38fa298d176a1466de4b06abc404d81a\", \"2007730265001\": \"7f6889e3703ab754dc37cf7348c7d29d\", \"2007541445001\": \"d75de4027945a48af3209c2d46691881\", \"3582347813001\": \"c5f004b162ac72b1d1f38e1a5e27b5fd\", \"3621471894001\": \"c86e6e9637681a61531b792341a7578b\", \"3621469853001\": \"095039d7ef3bbb1bad2ab8015a15d565\", \"3582347871001\": \"3c9ea0d2ea3b4119faa8788afe6f6c28\", \"3919711209001\": \"c57484a1c5082e59d1421dbf655dd13c\", \"2007610405001\": \"e50eff970fea8186d81131c9549f54c4\", \"2007610412001\": \"92b8e2bfb3fdf2e6be47fd4f35039426\", \"2007610408001\": \"bf979a72676ead55dd2332ef34467f9a\", \"3621469871001\": \"18fbd51ca4f4d45258bcee7924dc30d0\", \"2007730258001\": \"28878f0ad25408e2c5f2041d2efa81a8\", \"3903548524001\": \"bbc0c1b62a3c1828204ccdf9b926e857\"}, \"read_token\": \"tEBLhTQ18FsIacTpRnO7fjCyExLaVpcLWxhEsFkhIG6xxJfcJVicKQ..\", \"write_token\": \"1oupJkYVgMK1nOFZAIVo7uM3BB093eaBCwKFBB4qb3QNeAKbneikEw..\", \"abtest\": false, \"enabled\": true, \"video_still_width\": 480, \"last_process_date\": 1417544805, \"neon_api_key\": \"wsoruplnzkbilzj3apr05kvz\", \"rendition_frame_width\": null, \"linked_youtube_account\": false, \"key\": \"brightcoveplatform_wsoruplnzkbilzj3apr05kvz_39\", \"serving_enabled\": true, \"auto_update\": false, \"publisher_id\": \"1948681880001\", \"serving_controller\": \"imageplatform\", \"account_created\": 1398870554.476695}" 
        
        obj = BrightcovePlatform._create('brightcoveplatform_wsoruplnzkbilzj3apr05kvz_39', json.loads(json_str)) 
        self.assertEqual(obj.account_id, '145')
        self.assertEqual(obj.neon_api_key, 'wsoruplnzkbilzj3apr05kvz')
        self.assertEqual(obj.publisher_id, '1948681880001')
        
        def _set_params(x):
            x.account_id = obj.account_id
            x.publisher_id = obj.publisher_id
            x.abtest = True
        BrightcovePlatform.modify(obj.neon_api_key, obj.integration_id,
                                  _set_params, create_missing=True)
        
        obj = BrightcovePlatform.get('wsoruplnzkbilzj3apr05kvz', '39')
        self.assertEqual(obj.account_id, '145')
        self.assertEqual(obj.neon_api_key, 'wsoruplnzkbilzj3apr05kvz')
        self.assertEqual(obj.publisher_id, '1948681880001')
        self.assertTrue(obj.abtest)

    def test_ooyalaplatform_account_backwards_compatibility(self):
        json_str = "{\"integration_id\": \"1\", \"account_id\": \"136\", \"videos\": {}, \"auto_update\": false, \"api_secret\": \"uwTrMevYq54eani8ViRn6Ar5-rwmmmvKwq1HDtCn\", \"ooyala_api_key\": \"s0Y3YxOp0XTCL2hFlfFS1S2MRmaY.nxNs0\", \"abtest\": false, \"partner_code\": \"s0Y3YxOp0XTCL2hFlfFS1S2MRmaY\", \"neon_api_key\": \"qo4vtvhu2cqgdi30k63bahzh\", \"key\": \"ooyalaplatform_qo4vtvhu2cqgdi30k63bahzh_1\"}"

        obj = OoyalaPlatform._create('ooyalaplatform_qo4vtvhu2cqgdi30k63bahzh_1', json.loads(json_str)) 
        self.assertEqual(obj.account_id, '136')
        self.assertEqual(obj.neon_api_key, 'qo4vtvhu2cqgdi30k63bahzh')
        self.assertEqual(obj.partner_code, 's0Y3YxOp0XTCL2hFlfFS1S2MRmaY')
        self.assertEqual(obj.api_secret,
                         'uwTrMevYq54eani8ViRn6Ar5-rwmmmvKwq1HDtCn')

        def _set_params(x):
            x.account_id = obj.account_id
            x.partner_code = obj.partner_code
            x.abtest = True
            x.api_secret = obj.api_secret
        OoyalaPlatform.modify(obj.neon_api_key, obj.integration_id,
                              _set_params, create_missing=True)
        
        obj = OoyalaPlatform.get('qo4vtvhu2cqgdi30k63bahzh', '1')
        self.assertEqual(obj.account_id, '136')
        self.assertEqual(obj.neon_api_key, 'qo4vtvhu2cqgdi30k63bahzh')
        self.assertEqual(obj.partner_code, 's0Y3YxOp0XTCL2hFlfFS1S2MRmaY')
        self.assertEqual(obj.api_secret, 
                         'uwTrMevYq54eani8ViRn6Ar5-rwmmmvKwq1HDtCn')
        self.assertTrue(obj.abtest)

    def test_neonplatform_get_many(self):
        '''
        Test get many function for neonplatform
        '''
        nps = []
        for i in range(10):
            np = NeonPlatform.modify('acct%s' % i, '0', lambda x: x,
                                     create_missing=True)
            nps.append(np)

        keys = [('acct%s' % i, '0') for i in range(10)]
        r_nps = NeonPlatform.get_many(keys)
        self.assertListEqual(nps, r_nps)

    def test_defaulted_get_experiment_strategy(self):
        strategy = ExperimentStrategy('in_db',
                                      max_neon_thumbs=7,
                                      only_exp_if_chosen=True)
        strategy.save()

        with self.assertLogNotExists(logging.WARN, 'No ExperimentStrategy'):
            self.assertEquals(strategy, ExperimentStrategy.get('in_db'))

        with self.assertLogExists(logging.WARN, 'No ExperimentStrategy'):
            es_non_get = ExperimentStrategy('not_in_db')
            es_get = ExperimentStrategy.get('not_in_db')
            for (key, value), (key_two, value_two) in zip(es_non_get.__dict__.iteritems(), 
                                                          es_get.__dict__.iteritems()):
                if key is 'created': 
                    self.assertLess(value, value_two) 
                elif key is 'updated':  
                    self.assertLess(value, value_two) 
                else: 
                    self.assertEquals(value, value_two) 

        with self.assertLogNotExists(logging.WARN, 'No ExperimentStrategy'):
            es_non_get = ExperimentStrategy('not_in_db')
            es_get = ExperimentStrategy.get('not_in_db', log_missing=False)
            for (key, value), (key_two, value_two) in zip(es_non_get.__dict__.iteritems(), 
                                                          es_get.__dict__.iteritems()):
                if key is 'created': 
                    self.assertLess(value, value_two) 
                elif key is 'updated':  
                    self.assertLess(value, value_two) 
                else: 
                    self.assertEquals(value, value_two)
 
    @tornado.testing.gen_test
    def test_send_invalid_callback(self):
      request = NeonApiRequest('j1', 'key1', http_callback='null')
      request.save()

      with self.assertLogExists(logging.ERROR, 'Invalid callback url '):
        yield request.send_callback(async=True)

      # Make sure the state is correct now
      self.assertEquals(NeonApiRequest.get('j1', 'key1').callback_state,
                        neondata.CallbackState.ERROR)

    @patch('cmsdb.neondata.utils.http')
    @tornado.testing.gen_test
    def test_callback_with_experiment_state(self, http_mock):
      fetch_mock = self._future_wrap_mock(http_mock.send_request,
                                          require_async_kw=True)
      fetch_mock.side_effect = lambda x, **kw: HTTPResponse(x, 200)
      request = NeonApiRequest('j1', 'key1', 'vid1',
                               http_callback='http://some.where')
      request.state = neondata.RequestState.SERVING
      request.response['framenos'] = [34, 61]
      request.response['serving_url'] = 'http://some_serving_url.com'
      request.save()
      neondata.VideoStatus('key1_vid1', neondata.ExperimentState.COMPLETE,
                           winner_tid='key1_vid1_t2').save()

      yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.SUCESS)
      expected_response = {
        'job_id' : 'j1',
         'video_id' : 'vid1',
         'error': None,
         'framenos' : [34, 61],
         'serving_url' : 'http://some_serving_url.com',
         'processing_state' : neondata.ExternalRequestState.SERVING,
         'experiment_state' : neondata.ExperimentState.COMPLETE,
         'winner_thumbnail' : 'key1_vid1_t2'}
      
      self.assertDictContainsSubset(expected_response, found_request.response)

      # Check the callback
      self.assertTrue(fetch_mock.called)
      cargs, kwargs = fetch_mock.call_args
      found_request = cargs[0]
      response_dict = json.loads(found_request.body)
      self.assertDictContainsSubset(expected_response, response_dict)

    @patch('cmsdb.neondata.utils.http')
    @tornado.testing.gen_test
    def test_callback_with_error_state(self, http_mock):
      fetch_mock = self._future_wrap_mock(http_mock.send_request,
                                          require_async_kw=True)
      fetch_mock.side_effect = lambda x, **kw: HTTPResponse(x, 200)
      request = NeonApiRequest('j1', 'key1', 'vid1',
                               http_callback='http://some.where')
      request.state = neondata.RequestState.CUSTOMER_ERROR
      request.response['framenos'] = []
      request.response['serving_url'] = None
      request.response['error'] = 'some customer error'
      request.save()

      yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.SUCESS)
      expected_response = {
        'job_id' : 'j1',
         'video_id' : 'vid1',
         'error': 'some customer error',
         'framenos' : [],
         'serving_url' : None,
         'processing_state' : neondata.ExternalRequestState.FAILED,
         'experiment_state' : neondata.ExperimentState.UNKNOWN,
         'winner_thumbnail' : None}
      
      self.assertDictContainsSubset(expected_response, found_request.response)

      # Check the callback
      self.assertTrue(fetch_mock.called)
      cargs, kwargs = fetch_mock.call_args
      found_request = cargs[0]
      response_dict = json.loads(found_request.body)
      self.assertDictContainsSubset(expected_response, response_dict)

    def test_request_state_conversion(self):
      for state_name, val in neondata.RequestState.__dict__.items():
        if state_name.startswith('__'):
          # It's not a state name
          continue
        # The only state that should map to unknown is the unknown state
        if val == neondata.RequestState.UNKNOWN:
          self.assertEquals(
          neondata.ExternalRequestState.from_internal_state(val), 
          neondata.ExternalRequestState.UNKNOWN)
        else:
          self.assertNotEquals(
            neondata.ExternalRequestState.from_internal_state(val), 
            neondata.ExternalRequestState.UNKNOWN,
            'Internal state %s does not map to an external one' % state_name)

    @patch('cmsdb.neondata.utils.http')
    @tornado.testing.gen_test
    def test_image_in_isp(self, http_mock):
      fetch_mock = self._future_wrap_mock(http_mock.send_request,
                                          require_async_kw=True)

      acct = NeonUserAccount('a1', 'acct1')
      acct.default_thumbnail_id = 'acct1_default_thumb'
      acct.save()
      video = VideoMetadata('acct1_v1')

      # Check when isp returns a 204 because it doesn't have the video
      fetch_mock.side_effect = lambda x: HTTPResponse(x, code=204)
      with self.assertLogExists(logging.DEBUG, 'Image not available in '):
        is_avail = yield video.image_available_in_isp(async=True)
        self.assertFalse(is_avail)
      cargs, kwargs = fetch_mock.call_args
      found_request = cargs[0]
      self.assertTrue(found_request.follow_redirects)

      # Check when the image is there
      fetch_mock.side_effect = lambda x: HTTPResponse(x, code=200)
      is_avail = yield video.image_available_in_isp(async=True)
      self.assertTrue(is_avail)

      # Check when there was an http error that was not raised
      fetch_mock.side_effect = lambda x: HTTPResponse(x, code=500)
      with self.assertLogExists(logging.ERROR, 'Unexpected response looking'):
        is_avail = yield video.image_available_in_isp(async=True)
        self.assertFalse(is_avail)

      # Check on a raised http error
      fetch_mock.side_effect = [tornado.httpclient.HTTPError(400, 'Bad error')]
      with self.assertLogExists(logging.ERROR, 'Unexpected response looking'):
        is_avail = yield video.image_available_in_isp(async=True)
        self.assertFalse(is_avail)

    @tornado.testing.gen_test
    def test_account_missing_when_checking_isp(self):
      video = VideoMetadata('acct1_v1')
      with self.assertLogExists(logging.ERROR,
                                'Cannot find the neon user account'):
        with self.assertRaises(neondata.DBStateError):
          yield video.image_available_in_isp()

    @tornado.testing.gen_test
    def test_delete_videos_async(self):
      # Delete some video objects
      vids = [VideoMetadata('acct1_v1'),
              VideoMetadata('acct1_v2'),
              VideoMetadata('acct1_v3')]
      yield tornado.gen.Task(VideoMetadata.save_all, vids)
      found_objs = yield tornado.gen.Task(VideoMetadata.get_many,
                                          ['acct1_v1', 'acct1_v2', 'acct1_v3'])
      self.assertEquals(found_objs, vids)
      yield tornado.gen.Task(VideoMetadata.delete, 'acct1_v1')
      self.assertIsNone(VideoMetadata.get('acct1_v1'))
      self.assertIsNotNone(VideoMetadata.get('acct1_v2'))
      self.assertIsNotNone(VideoMetadata.get('acct1_v3'))
      n_deleted = yield tornado.gen.Task(VideoMetadata.delete_many,
                                         ['acct1_v2', 'acct1_v3'])
      self.assertIsNone(VideoMetadata.get('acct1_v1'))
      self.assertIsNone(VideoMetadata.get('acct1_v2'))
      self.assertIsNone(VideoMetadata.get('acct1_v3'))

    @tornado.testing.gen_test
    def test_delete_videos_sync(self):
      # Delete some video objects
      vids = [VideoMetadata('acct1_v1'),
              VideoMetadata('acct1_v2'),
              VideoMetadata('acct1_v3')]
      VideoMetadata.save_all(vids)
      found_objs = VideoMetadata.get_many(['acct1_v1', 'acct1_v2', 'acct1_v3'])
      self.assertEquals(found_objs, vids)
      self.assertTrue(VideoMetadata.delete('acct1_v1'))
      self.assertIsNone(VideoMetadata.get('acct1_v1'))
      self.assertIsNotNone(VideoMetadata.get('acct1_v2'))
      self.assertIsNotNone(VideoMetadata.get('acct1_v3'))
      self.assertTrue(VideoMetadata.delete_many(['acct1_v2', 'acct1_v3']))
      self.assertIsNone(VideoMetadata.get('acct1_v1'))
      self.assertIsNone(VideoMetadata.get('acct1_v2'))
      self.assertIsNone(VideoMetadata.get('acct1_v3'))
      
    @tornado.testing.gen_test
    def test_delete_platforms_async(self):
      # Do some platform objects
      np = NeonPlatform.modify('acct1', '0', lambda x: x,
                               create_missing=True)
      bp = BrightcovePlatform.modify('acct2', 'ibc', lambda x: x,
                                     create_missing=True)
      op = OoyalaPlatform.modify('acct3', 'ioo', lambda x: x,
                                 create_missing=True)
      yp = YoutubePlatform.modify('acct4', 'iyt', lambda x: x,
                                  create_missing=True)
      plats = yield AbstractPlatform.get_all(async=True)
      self.assertEquals(len(plats), 4)
      deleted_counts = yield [
        tornado.gen.Task(NeonPlatform.delete, np.neon_api_key,
                         np.integration_id),
        tornado.gen.Task(BrightcovePlatform.delete_many,
                         [(bp.neon_api_key, bp.integration_id)]),
        tornado.gen.Task(OoyalaPlatform.delete, op.neon_api_key,
                         op.integration_id),
        tornado.gen.Task(YoutubePlatform.delete, yp.neon_api_key,
                         yp.integration_id)]
      self.assertEquals(deleted_counts, [1,1,1,1])
      self.assertIsNone(NeonPlatform.get('acct1', '0'))
      self.assertIsNone(BrightcovePlatform.get('acct2', 'ibc'))
      self.assertIsNone(OoyalaPlatform.get('acct3', 'ioo'))
      self.assertIsNone(YoutubePlatform.get('acct4', 'iyt'))

    @tornado.testing.gen_test
    def test_delete_requests_async(self):
      nreq = NeonApiRequest('job1', 'acct1')
      breq = neondata.BrightcoveApiRequest('job2', 'acct1')
      oreq = neondata.OoyalaApiRequest('job3', 'acct1')
      yreq = neondata.YoutubeApiRequest('job4', 'acct1')
      NeonApiRequest.save_all([nreq, breq, oreq, yreq])
      self.assertIsNotNone(NeonApiRequest.get('job1', 'acct1'))
      self.assertIsNotNone(NeonApiRequest.get('job2', 'acct1'))
      self.assertIsNotNone(NeonApiRequest.get('job3', 'acct1'))
      self.assertIsNotNone(NeonApiRequest.get('job4', 'acct1'))
      deleted_bool = yield [
        tornado.gen.Task(NeonApiRequest.delete, 'job1', 'acct1'),
        tornado.gen.Task(NeonApiRequest.delete_many,
                         [('job2', 'acct1'), ('job3', 'acct1'),
                          ('job4', 'acct1'), ('job5', 'acct1')])]
      self.assertEquals(deleted_bool, [True, True])
      self.assertIsNone(NeonApiRequest.get('job1', 'acct1'))
      self.assertIsNone(NeonApiRequest.get('job2', 'acct1'))
      self.assertIsNone(NeonApiRequest.get('job3', 'acct1'))
      self.assertIsNone(NeonApiRequest.get('job4', 'acct1'))
      self.assertIsNone(NeonApiRequest.get('job5', 'acct1'))

class TestPGNeondataDataSpecific(TestNeondataDataSpecific):
    def setUp(self): 
        self.maxDiff = 5000
        logging.getLogger('cmsdb.neondata').reset_sample_counters()
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self):
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()

    def test_default_bcplatform_settings(self):
        ''' override from base due to saving 
            as abstractplatform now ''' 
        na = NeonUserAccount('acct1')
        bp = BrightcovePlatform(na.neon_api_key, 'iid', 'aid')

        self.assertFalse(bp.abtest)
        self.assertFalse(bp.auto_update)
        self.assertTrue(bp.serving_enabled)
        self.assertNotEqual(bp.neon_api_key, '')
        self.assertEqual(bp.key, 'abstractplatform_%s_iid' % bp.neon_api_key)

        # Make sure that save and regenerating creates the same object
        def _set_acct(x):
            x.account_id = 'aid'
        bp = BrightcovePlatform.modify(na.neon_api_key, 'iid', _set_acct,
                                       create_missing=True)

        bp2 = BrightcovePlatform.get(na.neon_api_key, 'iid')
        self.assertEqual(bp.__dict__, bp2.__dict__)

    def test_bcplatform_with_callback(self):
        ''' override from base due to saving 
            as abstractplatform now ''' 
        na = NeonUserAccount('acct1')
        bp = BrightcovePlatform('aid', 'iid', na.neon_api_key,
                                callback_url='http://www.callback.com')

        self.assertFalse(bp.abtest)
        self.assertFalse(bp.auto_update)
        self.assertTrue(bp.serving_enabled)
        self.assertNotEqual(bp.neon_api_key, '')
        self.assertEqual(bp.key, 'abstractplatform_%s_iid' % bp.neon_api_key)
        self.assertEqual(bp.callback_url, 'http://www.callback.com')

class TestNeondata(test_utils.neontest.AsyncTestCase):
    '''
    Neondata class tester
    '''
    def setUp(self):
        super(TestNeondata, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.maxDiff = 5000
        logging.getLogger('cmsdb.neondata').reset_sample_counters()

    def tearDown(self):
        neondata.PubSubConnection.clear_singleton_instance()
        self.redis.stop()
        super(TestNeondata, self).tearDown()

    #@unittest.skip('TODO(Sunil): add this test')
    #def test_add_platform_with_bad_account_id(self):
    #    pass
    
    def test_dbconn_singleton(self):
        bp = BrightcovePlatform('2','3', 'test')
        self.bp_conn = DBConnection.get(bp)

        bp2 = BrightcovePlatform('12','13','test')
        self.bp_conn2 = DBConnection.get(bp2)


        vm = VideoMetadata('test1', None, None, None,
                None, None, None, None)
        self.vm_conn = DBConnection.get(vm)

        vm2 = VideoMetadata('test2', None, None, None, 
                None, None, None, None)
        self.vm_conn2 = DBConnection.get(vm2)
        
        self.assertEqual(self.bp_conn, self.bp_conn2)
        self.assertEqual(self.vm_conn, self.vm_conn2)

        self.assertNotEqual(self.bp_conn, self.vm_conn)

    #TODO: Test Async DB Connection
    
    def test_db_connection(self):
        ''' 
        DB Connection test
        '''
        ap = AbstractPlatform('a1', 'i1')
        db = DBConnection.get(ap)
        key = "fookey"
        val = "fooval"
        self.assertTrue(db.blocking_conn.set(key, val))
        self.assertEqual(db.blocking_conn.get(key), val)
        self.assertTrue(db.blocking_conn.delete(key))

    @tornado.testing.gen_test
    def test_async_fetch_keys_from_db(self):
        accts = []
        for i in range(10):
            new_acct = NeonUserAccount(str(i))
            new_acct.save()
            accts.append(new_acct)

        conn = neondata.DBConnection.get(NeonUserAccount)
        keys = yield tornado.gen.Task(conn.fetch_keys_from_db,
                                      'neonuseraccount_*', 2)

        self.assertEquals(len(keys), 10)
        self.assertItemsEqual(keys, [x.key for x in accts])

    def test_sync_fetch_keys_from_db(self):
        accts = []
        for i in range(10):
            new_acct = NeonUserAccount(str(i))
            new_acct.save()
            accts.append(new_acct)

        conn = neondata.DBConnection.get(NeonUserAccount)
        keys = conn.fetch_keys_from_db('neonuseraccount_*', 2)

        self.assertEquals(len(keys), 10)
        self.assertItemsEqual(keys, [x.key for x in accts])

    def test_concurrent_requests(self):
        ''' Make concurrent requests to the db 
            verify that singleton instance doesnt cause race condition
        '''
        def db_operation(key):
            ''' db op '''
            resultQ.put(db.blocking_conn.get(key))

        ap = AbstractPlatform('a1', 'i1')
        db = DBConnection.get(ap)
        key = "fookey"
        val = "fooval"*1000
        nkeys = 100
        for i in range(nkeys):
            db.blocking_conn.set(key+"%s"%i, val+"%s"%i)
       
        resultQ =  multiprocessing.Queue()
        threads = []
        for n in range(nkeys):
            thread = threading.Thread(target=db_operation, args=(key+"%s"%n,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        results = []
        for i in range(nkeys):
            results.append(resultQ.get())  

        #Make sure that each of the thread retrieved a key
        #and did not have an exception
        self.assertTrue(None not in results)

    def test_too_many_open_connections_sync(self):
        self.maxDiff = 10000

        def _get_open_connection_list():
            fd_list = subprocess.check_output(
                ['lsof', '-a', '-d0-65535', 
                 '-p', str(os.getpid())]).split('\n')
            fd_list = [x.split() for x in fd_list if x]
            return [x for x in fd_list if 
                    x[7] == 'TCP' and 'localhost:%d' % self.redis.port in x[8]]
                
        
        # When making calls on various objects, there should only be
        # one connection per object type to the redis server. We're
        # just going to make sure that that is the case

        get_func = lambda x: x.get('key')
        
        obj_types = [
            (VideoMetadata('key'), get_func),
            (ThumbnailMetadata('key'), get_func),
            (TrackerAccountIDMapper('key'), get_func),
            (ThumbnailServingURLs('key'), get_func),
            (ExperimentStrategy('key'), get_func),
            (NeonUserAccount('key', 'api'),
             lambda x: x.get('key')),
            (NeonPlatform('api', '0', 'a'),
             lambda x: x.get('api', '0')),
            (BrightcovePlatform('api', 'i1', 'a'),
             lambda x: x.get('api', 'i1')),
            (OoyalaPlatform('api', 'i2', 'a', 'b', 'c', 'd', 'e'),
             lambda x: x.get('api', 'i2'))
             ]

        for obj, read_func in obj_types:
            # Start by saving the object
            if isinstance(obj, AbstractPlatform):
                obj.modify('api', obj.integration_id, lambda x: x,
                           create_missing=True)
            else:
                obj.save()

            # Now find the number of open file descriptors
            start_fd_list = _get_open_connection_list()

            def nop(x): pass

            # Run the func a bunch of times
            for i in range(10):
                read_func(obj)

            # Check the file descriptors
            end_fd_list = _get_open_connection_list()
            if len(start_fd_list) != len(end_fd_list):
                # This will give us more information if it fails
                self.assertEqual(start_fd_list, end_fd_list)

    class ChangeTrap:
        '''Helper class to test subscribing to changes.'''
        def __init__(self):
            self.args = []
            self._event = threading.Event()

        def _handler(self, *args):
            '''Handler function to give to the subscribe_to_changes'''
            self.args.append(args)
            self._event.set()

        def reset(self):
            self.args = []
                
        def subscribe(self, cls, pattern='*'):
            self.reset()
            cls.subscribe_to_changes(self._handler, pattern)

        def unsubscribe(self, cls, pattern='*'):
            cls.unsubscribe_from_changes(pattern)

        def wait(self, timeout=1.0):
            '''Wait for an event (or timeout) and return the mock.'''
            self._event.wait(timeout)
            self._event.clear()
            return self.args
            
    def test_subscribe_to_changes(self):
        trap = TestNeondata.ChangeTrap()

        # Try a pattern
        trap.subscribe(VideoMetadata, 'acct1_*')
        video_meta = VideoMetadata('acct1_vid1', request_id='req1')
        video_meta.save()
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertEquals(events[0], ('acct1_vid1', video_meta, 'set'))


        # Try subscribing to a specific key
        trap.subscribe(ThumbnailMetadata, 'acct1_vid2_t3')
        thumb_meta = ThumbnailMetadata('acct1_vid2_t3', width=654)
        thumb_meta.save()
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertEquals(events[0], ('acct1_vid2_t3', thumb_meta, 'set'))

        # Try doing a namespaced object
        trap.subscribe(NeonUserAccount, 'acct1')
        acct_meta = NeonUserAccount('a1', 'acct1', default_size=[45,98])
        acct_meta.save()
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertEquals(events[0], ('acct1', acct_meta, 'set'))

    def test_subscribe_changes_to_video_and_thumbs(self):
        vid_trap = TestNeondata.ChangeTrap()
        thumb_trap = TestNeondata.ChangeTrap()

        vid_trap.subscribe(VideoMetadata, 'acct1_*')
        thumb_trap.subscribe(ThumbnailMetadata, 'acct1_vid1_*')
        vid_meta = VideoMetadata('acct1_vid1', request_id='req1',
                                 tids=['acct1_vid1_t1'])
        vid_meta.save()
        thumb_meta = ThumbnailMetadata('acct1_vid1_t1', width=654)
        thumb_meta.save()

        vid_events = vid_trap.wait()
        thumb_events = thumb_trap.wait()

        self.assertEquals(len(vid_events), 1)
        self.assertEquals(vid_events[0], ('acct1_vid1', vid_meta, 'set'))
        self.assertEquals(len(thumb_events), 1)
        self.assertEquals(thumb_events[0],
                          ('acct1_vid1_t1', thumb_meta, 'set'))

    def test_subscribe_twice(self):
        vid_trap = TestNeondata.ChangeTrap()
        vid_trap.subscribe(VideoMetadata, 'acct1_*')

        vid_meta = VideoMetadata('acct1_vid1', request_id='req1',
                                 tids=['acct1_vid1_t1'])
        vid_meta.save()
        vid_events = vid_trap.wait()

        self.assertEquals(len(vid_events), 1)
        self.assertEquals(vid_events[0], ('acct1_vid1', vid_meta, 'set'))

        # Subscribe a second time with the same thing
        vid_trap.reset()
        vid_trap.subscribe(VideoMetadata, 'acct1_*')

        vid_meta = VideoMetadata('acct1_vid2', request_id='req2',
                                 tids=['acct1_vid2_t1'])
        vid_meta.save()
        vid_events = vid_trap.wait()

        self.assertEquals(len(vid_events), 1)
        self.assertEquals(vid_events[0], ('acct1_vid2', vid_meta, 'set'))
        

    def test_subscribe_to_changes_with_modifies(self):
        trap = TestNeondata.ChangeTrap()
        
        trap.subscribe(VideoMetadata, 'acct1_*')
        video_meta = VideoMetadata('acct1_vid1', request_id='req1')
        video_meta.save()
        events = trap.wait()
        self.assertEquals(len(events), 1)

        # Now do a modify that changes the object. Should see a new event
        def _set_experiment_state(x):
            x.experiment_state = ExperimentState.RUNNING

        VideoMetadata.modify('acct1_vid1', _set_experiment_state)
        events = trap.wait()
        self.assertEquals(len(events), 2)
        self.assertEquals(events[-1][1].experiment_state,
                          ExperimentState.RUNNING)

        # Modify again and we should not see a change event
        VideoMetadata.modify('acct1_vid1', _set_experiment_state)
        events = trap.wait(0.2)
        self.assertEquals(len(events), 2)

        # Do a modify that creates a new, default
        VideoMetadata.modify('acct1_vid3', lambda x: x)
        new_vid = VideoMetadata.modify('acct1_vid2', lambda x: x,
                                       create_missing=True)
        events = trap.wait()
        self.assertEquals(len(events), 3)
        self.assertEquals(events[-1], ('acct1_vid2', new_vid, 'set'))

    def test_subscribe_api_request_changes(self):
        #import pdb; pdb.set_trace()
        bc_trap = TestNeondata.ChangeTrap()
        generic_trap = TestNeondata.ChangeTrap()

        bc_trap.subscribe(neondata.BrightcoveApiRequest, '*')
        generic_trap.subscribe(NeonApiRequest)

        # Test a Brightcove request
        neondata.BrightcoveApiRequest('jobbc', 'acct1').save()
        bc_events = bc_trap.wait()
        all_events = generic_trap.wait()
        self.assertEquals(len(bc_events), 1)
        self.assertEquals(len(all_events), 1)
        self.assertEquals(bc_events[0][1].job_id, 'jobbc')
        self.assertEquals(all_events[0][1].job_id, 'jobbc')

        # Now try a Neon request. the BC one shouldn't be listed
        NeonApiRequest('jobneon', 'acct1').save()
        all_events = generic_trap.wait()
        bc_events = bc_trap.wait(0.1)
        self.assertEquals(len(bc_events), 1)
        self.assertEquals(len(all_events), 2)
        self.assertEquals(all_events[1][1].job_id, 'jobneon')


    def test_subscribe_platform_changes(self):
        trap = TestNeondata.ChangeTrap()
        trap.subscribe(AbstractPlatform)

        def _set_acct(x):
            x.account_id = 'a1'

        BrightcovePlatform.modify('acct1','bc', _set_acct, create_missing=True)
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertIsInstance(events[0][1], BrightcovePlatform)

        NeonPlatform.modify('acct1', '0', _set_acct, create_missing=True)
        events = trap.wait()
        self.assertEquals(len(events), 2)
        self.assertIsInstance(events[-1][1], NeonPlatform)

        YoutubePlatform.modify('acct1', 'yt', _set_acct, create_missing=True)
        events = trap.wait()
        self.assertEquals(len(events), 3)
        self.assertIsInstance(events[-1][1], YoutubePlatform)

        OoyalaPlatform.modify('acct1', 'oo', _set_acct, create_missing=True)
        events = trap.wait()
        self.assertEquals(len(events), 4)
        self.assertIsInstance(events[-1][1], OoyalaPlatform)

    def test_unsubscribe_changes(self):
        plat_trap = TestNeondata.ChangeTrap()
        plat_trap.subscribe(AbstractPlatform)
        request_trap = TestNeondata.ChangeTrap()
        request_trap.subscribe(NeonApiRequest)
        video_trap = TestNeondata.ChangeTrap()
        video_trap.subscribe(VideoMetadata, 'acct1_*')

        # Make sure all the subscriptions are working
        def _set_acct(x):
            x.account_id = 'a1'
        BrightcovePlatform.modify('acct1','bc', _set_acct, create_missing=True)
        plat_trap.wait()
        NeonPlatform.modify('acct1', '0', _set_acct, create_missing=True)
        plat_trap.wait()
        YoutubePlatform.modify('acct1', 'yt', _set_acct, create_missing=True)
        plat_trap.wait()
        OoyalaPlatform.modify('acct1', 'oo', _set_acct, create_missing=True)
        plat_trap.wait()
        self.assertEquals(len(plat_trap.args), 4)
        plat_trap.reset()
        
        neondata.BrightcoveApiRequest('jobbc', 'acct1').save()
        request_trap.wait()
        NeonApiRequest('jobneon', 'acct1').save()
        request_trap.wait()
        self.assertEquals(len(request_trap.args), 2)
        request_trap.reset()

        VideoMetadata('acct1_vid1', request_id='req1').save()
        video_trap.wait()
        self.assertEquals(len(video_trap.args), 1)
        video_trap.reset()

        # Now unsubscribe and make changes
        plat_trap.unsubscribe(AbstractPlatform)
        request_trap.unsubscribe(NeonApiRequest)
        video_trap.unsubscribe(VideoMetadata, 'acct1_*')
        
        BrightcovePlatform.modify('acct2','bc', _set_acct, create_missing=True)
        NeonPlatform.modify('acct2', '0', _set_acct, create_missing=True)
        YoutubePlatform.modify('acct2', 'yt', _set_acct, create_missing=True)
        OoyalaPlatform.modify('acct2', 'oo', _set_acct, create_missing=True)
        neondata.BrightcoveApiRequest('jobbc', 'acct2').save()
        NeonApiRequest('jobneon', 'acct2').save()
        VideoMetadata('acct1_vid2', request_id='req1').save()

        # Now resubscribe and see the changes
        plat_trap.subscribe(AbstractPlatform)
        request_trap.subscribe(NeonApiRequest)
        video_trap.subscribe(VideoMetadata, 'acct1_*')
        
        BrightcovePlatform.modify('acct3','bc', _set_acct, create_missing=True)
        plat_trap.wait()
        NeonPlatform.modify('acct3', '0', _set_acct, create_missing=True)
        plat_trap.wait()
        YoutubePlatform.modify('acct3', 'yt', _set_acct, create_missing=True)
        plat_trap.wait()
        OoyalaPlatform.modify('acct3', 'oo', _set_acct, create_missing=True)
        plat_trap.wait()
        self.assertEquals(len(plat_trap.args), 4)
        
        neondata.BrightcoveApiRequest('jobbc', 'acct3').save()
        request_trap.wait()
        NeonApiRequest('jobneon', 'acct3').save()
        request_trap.wait()
        self.assertEquals(len(request_trap.args), 2)

        VideoMetadata('acct1_vid3', request_id='req1').save()
        video_trap.wait()
        self.assertEquals(len(video_trap.args), 1)

    def test_subscribe_changes_db_address_change(self):
        trap = TestNeondata.ChangeTrap()
        
        # Try initial change
        trap.subscribe(VideoMetadata, 'acct1_*')
        trap.subscribe(VideoMetadata, 'acct2_*')
        video_meta = VideoMetadata('acct1_vid1', request_id='req1')
        video_meta.save()
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertEquals(events[0], ('acct1_vid1', video_meta, 'set'))
        trap.reset()
        video_meta2 = VideoMetadata('acct2_vid1', request_id='req2')
        video_meta2.save()
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertEquals(events[0], ('acct2_vid1', video_meta2, 'set'))
        trap.reset()

        # Now unsubscribe from account 2
        trap.unsubscribe(VideoMetadata, 'acct2_*')

        # Now start a new server, which will change the connection address
        temp_redis = test_utils.redis.RedisServer()
        temp_redis.start(clear_singleton=False)
        self.assertNotEquals(temp_redis.port, self.redis.port)
        try:
            self.assertWaitForEquals(
            lambda: neondata.PubSubConnection.get(VideoMetadata)._address,
                ('0.0.0.0', temp_redis.port))
            
            # Now change the video and make sure we get the event for
            # account 1, but not 2
            video_meta.serving_enabled=False
            video_meta2.save()
            video_meta.serving_enabled=False
            video_meta.save()
            events = trap.wait()
            self.assertEquals(len(events), 1)
            self.assertEquals(events[0], ('acct1_vid1', video_meta, 'set'))

        finally:
            temp_redis.stop(clear_singleton=False)

        self.assertWaitForEquals(
            lambda: neondata.PubSubConnection.get(VideoMetadata)._address,
                ('0.0.0.0', self.redis.port))

        # Now we're connected back to the first server, so make sure
        # the state didn't get that change. We'll lose some state
        # change, but oh well
        self.assertTrue(VideoMetadata.get('acct1_vid1').serving_enabled)

    @tornado.testing.gen_test
    def test_async_change_db_connection(self):
        video_meta = VideoMetadata('acct1_vid1', request_id='req1')
        yield tornado.gen.Task(video_meta.save)

        # force a connection loss and restart. This will change the
        # port we are talking to.
        self.redis.stop(clear_singleton=False)
        self.redis = test_utils.redis.RedisServer()
        self.redis.start(clear_singleton=False)

        # We won't get continuity in the database, but that's ok for this test
        yield tornado.gen.Task(video_meta.save)
        found_val = yield tornado.gen.Task(VideoMetadata.get, 'acct1_vid1')
        self.assertEquals(found_val, video_meta)

    def test_sync_change_db_connection(self):
        video_meta = VideoMetadata('acct1_vid1', request_id='req1')
        video_meta.save()

        # force a connection loss and restart. This will change the
        # port we are talking to.
        self.redis.stop(clear_singleton=False)
        self.redis = test_utils.redis.RedisServer()
        self.redis.start(clear_singleton=False)

        # We won't get continuity in the database, but that's ok for this test
        video_meta.save()
        found_val = VideoMetadata.get('acct1_vid1')
        self.assertEquals(found_val, video_meta)

    @tornado.testing.gen_test
    def test_async_talk_to_different_db(self):
        video_meta = VideoMetadata('acct1_vid1', request_id='req1')
        yield tornado.gen.Task(video_meta.save)
        self.assertEquals(VideoMetadata.get('acct1_vid1'), video_meta)

        # Start a new database, this should cause neondata to talk to it
        temp_redis = test_utils.redis.RedisServer()
        temp_redis.start(clear_singleton=False)
        try:
            found_val = yield tornado.gen.Task(VideoMetadata.get, 'acct1_vid1')
            self.assertIsNone(found_val)

        finally:
          temp_redis.stop(clear_singleton=False)

        # Now try getting data from the original database again
        found_val = yield tornado.gen.Task(VideoMetadata.get, 'acct1_vid1')
        self.assertEquals(found_val, video_meta)

    def test_sync_talk_to_different_db(self):
        video_meta = VideoMetadata('acct1_vid1', request_id='req1')
        video_meta.save()
        self.assertEquals(VideoMetadata.get('acct1_vid1'), video_meta)

        # Start a new database, this should cause neondata to talk to it
        temp_redis = test_utils.redis.RedisServer()
        temp_redis.start(clear_singleton=False)
        try:
            self.assertIsNone(VideoMetadata.get('acct1_vid1'))

        finally:
          temp_redis.stop(clear_singleton=False)

        # Now try getting data from the original database again
        self.assertEquals(VideoMetadata.get('acct1_vid1'), video_meta)

class TestDbConnectionHandling(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestDbConnectionHandling, self).setUp()
        logging.getLogger('cmsdb.neondata').reset_sample_counters()
        self.connection_patcher = patch('cmsdb.neondata.blockingRedis.StrictRedis')

        # For the sake of this test, we will only mock the get() function
        self.mock_redis = self.connection_patcher.start()
        self.mock_responses = MagicMock()
        self.mock_redis().get.side_effect = self._mocked_get_func

        self.valid_obj = TrackerAccountIDMapper("tai1", "api_key")

        # Speed up the retry delays to make the test faster
        self.old_delay = options.get('cmsdb.neondata.baseRedisRetryWait')
        options._set('cmsdb.neondata.baseRedisRetryWait', 0.01)
        self.old_retries = options.get('cmsdb.neondata.maxRedisRetries')
        options._set('cmsdb.neondata.maxRedisRetries', 5)

    def tearDown(self):
        self.connection_patcher.stop()
        DBConnection.clear_singleton_instance()
        neondata.PubSubConnection.clear_singleton_instance()
        options._set('cmsdb.neondata.baseRedisRetryWait',
                     self.old_delay)
        options._set('cmsdb.neondata.maxRedisRetries', self.old_retries)
        super(TestDbConnectionHandling, self).tearDown()

    def _mocked_get_func(self, key, callback=None):
        if callback:
            self.io_loop.add_callback(callback, self.mock_responses(key))
        else:
            return self.mock_responses(key)

    def test_subscribe_connection_error(self):
        self.mock_redis().pubsub().psubscribe.side_effect = (
            [redis.exceptions.ConnectionError()] * 5 +
            [socket.gaierror()] * 5)
        
        with self.assertLogExists(logging.ERROR, 'Error subscribing'):
            with self.assertRaises(neondata.DBConnectionError):
                NeonUserAccount.subscribe_to_changes(lambda x,y,z: x)

        with self.assertLogExists(logging.ERROR, 'Socket error subscribing'):
            with self.assertRaises(neondata.DBConnectionError):
                NeonUserAccount.subscribe_to_changes(lambda x,y,z: x)

    
    def test_unsubscribe_connection_error(self):
        self.mock_redis().pubsub().punsubscribe.side_effect = (
            [redis.exceptions.ConnectionError()] * 5 +
            [socket.gaierror()] * 5)

        with self.assertLogExists(logging.ERROR, 'Error unsubscribing'):
            with self.assertRaises(neondata.DBConnectionError):
                NeonUserAccount.unsubscribe_from_changes('*')

        with self.assertLogExists(logging.ERROR, 'Socket error unsubscribing'):
            with self.assertRaises(neondata.DBConnectionError):
                NeonUserAccount.unsubscribe_from_changes('*')
        

    def test_async_good_connection(self):
        self.mock_responses.side_effect = [self.valid_obj.to_json()]
        
        TrackerAccountIDMapper.get("tai1", callback=self.stop)
        found_obj = self.wait()
        self.assertEqual(self.valid_obj.__dict__, found_obj.__dict__)

    def test_sync_good_connection(self):
        self.mock_responses.side_effect = [self.valid_obj.to_json()]
        
        found_obj = TrackerAccountIDMapper.get("tai1")

        self.assertEqual(self.valid_obj.__dict__, found_obj.__dict__)

    @tornado.testing.gen_test
    def test_async_some_errors(self):
        self.mock_responses.side_effect = [
            redis.ConnectionError("Connection Error"),
            redis.BusyLoadingError("Loading Error"),
            socket.timeout("Socket Timeout"),
            socket.error("Socket Error"),
            self.valid_obj.to_json()
            ]

        with self.assertLogExists(logging.ERROR, 'Connection Error'):
            with self.assertLogExists(logging.WARN, 'Redis is busy'):
                with self.assertLogExists(logging.ERROR, 'Socket Timeout'):
                    with self.assertLogExists(logging.ERROR, 'Socket Error'):
                        found_obj = yield tornado.gen.Task(
                            TrackerAccountIDMapper.get,
                            'tai1')

        self.assertEqual(self.valid_obj.__dict__, found_obj.__dict__)

    def test_sync_some_errors(self):
        self.mock_responses.side_effect = [
            redis.ConnectionError("Connection Error"),
            redis.BusyLoadingError("Loading Error"),
            socket.timeout("Socket Timeout"),
            socket.error("Socket Error"),
            self.valid_obj.to_json()
            ]

        with self.assertLogExists(logging.ERROR, 'Connection Error'):
            with self.assertLogExists(logging.WARN, 'Redis is busy'):
                with self.assertLogExists(logging.ERROR, 'Socket Timeout'):
                    with self.assertLogExists(logging.ERROR, 'Socket Error'):
                        found_obj = TrackerAccountIDMapper.get("tai1")

        self.assertEqual(self.valid_obj.__dict__, found_obj.__dict__)

    def test_async_too_many_errors(self):
        self.mock_responses.side_effect = [
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            ]

        TrackerAccountIDMapper.get("tai1", callback=self.stop)
        
        with self.assertRaises(redis.ConnectionError):
            self.wait()

    def test_sync_too_many_errors(self):
        self.mock_responses.side_effect = [
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            redis.ConnectionError("Connection Error"),
            ]
        
        with self.assertRaises(redis.ConnectionError):
            TrackerAccountIDMapper.get("tai1")

    # TODO(mdesnoyer): Add test for the modify if there is a failure
    # on one of the underlying commands.

class TestThumbnailHelperClass(test_utils.neontest.AsyncTestCase):
    '''
    Thumbnail ID Mapper and other thumbnail helper class tests 
    '''
    def setUp(self):
        super(TestThumbnailHelperClass, self).setUp()
        self.image = PILImageUtils.create_random_image(360, 480)

    def tearDown(self):
        conn = neondata.DBConnection.get(VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(ThumbnailMetadata)
        conn.clear_db()
        super(TestThumbnailHelperClass, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()
        super(TestThumbnailHelperClass, cls).tearDownClass()

    def test_thumbnail_mapper(self):
        ''' Thumbnail mappings '''

        url = "http://thumbnail.jpg"
        vid = "v123"
        image = PILImageUtils.create_random_image(360, 480)
        tid = ThumbnailID.generate(image, vid)
    
        tdata = ThumbnailMetadata(tid, vid, [], 0, 480, 360,
                        "ttype", 0, 1, 0)
        tdata.save()
        tmap = ThumbnailURLMapper(url, tid)
        tmap.save()
        
        res_tid = ThumbnailURLMapper.get_id(url)
        self.assertEqual(tid, res_tid)

    def test_thumbnail_get_data(self):

        vid = InternalVideoID.generate('api1', 'vid1')
        tid = ThumbnailID.generate(self.image, vid)
        tdata = ThumbnailMetadata(tid, vid, ['one.jpg', 'two.jpg'],
                                  None, self.image.size[1], self.image.size[0],
                                  'brightcove', 1.0, '1.2')
        tdata.save()
        self.assertEqual(tdata.get_account_id(), 'api1')
        self.assertEqual(ThumbnailMetadata.get_video_id(tid), vid)
        self.assertEqual(tdata.rank, 0)
        self.assertEqual(tdata.urls, ['one.jpg', 'two.jpg'])
    
    @tornado.testing.gen_test
    def test_atomic_modify(self):
        vid = InternalVideoID.generate('api1', 'vid1')
        tid = ThumbnailID.generate(self.image, vid)
        tdata = ThumbnailMetadata(tid, vid, ['one.jpg', 'two.jpg'],
                                  None, self.image.size[1], self.image.size[0],
                                  'brightcove', 1.0, '1.2')
        tdata.save()

        thumb = ThumbnailMetadata.modify(tid,
                                         lambda x: x.urls.append('url3.jpg'))
        self.assertItemsEqual(thumb.urls, ['one.jpg', 'two.jpg', 'url3.jpg'])
        self.assertItemsEqual(ThumbnailMetadata.get(tid).urls,
                              ['one.jpg', 'two.jpg', 'url3.jpg'])

        # Now try asynchronously
        def setphash(thumb): thumb.phash = 'hash'
        def setrank(thumb): thumb.rank = 6

        yield ThumbnailMetadata.modify(tid, setphash, async=True)
        yield ThumbnailMetadata.modify(tid, setrank, async=True)
        
        thumb = ThumbnailMetadata.get(tid)
        self.assertEqual(thumb.phash, 'hash')
        self.assertEqual(thumb.rank, 6)
        self.assertItemsEqual(thumb.urls,
                              ['one.jpg', 'two.jpg', 'url3.jpg'])

    def test_atomic_modify_many(self):
        vid1 = InternalVideoID.generate('api1', 'vid1')
        tid1 = ThumbnailID.generate(self.image, vid1)
        tdata1 = ThumbnailMetadata(tid1, vid1, ['one.jpg', 'two.jpg'],
                                   None, self.image.size[1],
                                   self.image.size[0],
                                   'brightcove', 1.0, '1.2')
        vid2 = InternalVideoID.generate('api1', 'vid2')
        tid2 = ThumbnailID.generate(self.image, vid2)
        tdata2 = ThumbnailMetadata(tid2, vid2, ['1.jpg', '2.jpg'],
                                   None, self.image.size[1],
                                   self.image.size[0],
                                   'brightcove', 1.0, '1.2')
        self.assertFalse(tdata2.chosen)
        self.assertFalse(tdata1.chosen)
        self.assertTrue(tdata2.enabled)
        self.assertTrue(tdata1.enabled)

        ThumbnailMetadata.save_all([tdata1, tdata2])

        def _change_thumb_data(d):
            d[tid1].chosen = True
            d[tid2].enabled = False

        updated = ThumbnailMetadata.modify_many([tid1, tid2],
                                                _change_thumb_data)
        self.assertTrue(updated[tid1].chosen)
        self.assertFalse(updated[tid2].chosen)
        self.assertTrue(updated[tid1].enabled)
        self.assertFalse(updated[tid2].enabled)
        self.assertTrue(ThumbnailMetadata.get(tid1).chosen)
        self.assertFalse(ThumbnailMetadata.get(tid2).chosen)
        self.assertTrue(ThumbnailMetadata.get(tid1).enabled)
        self.assertFalse(ThumbnailMetadata.get(tid2).enabled)

    def test_create_or_modify(self):
        vid1 = InternalVideoID.generate('api1', 'vid1')
        tid1 = ThumbnailID.generate(self.image, vid1)
        self.assertIsNone(ThumbnailMetadata.get(tid1))

        ThumbnailMetadata.modify(tid1,
                                 lambda x: x.urls.append('http://image.jpg'),
                                 create_missing=True)

        val = ThumbnailMetadata.get(tid1)
        self.assertIsNotNone(val)
        self.assertEqual(val.urls, ['http://image.jpg'])

    def test_create_infinite_model_score(self):
        vid1 = InternalVideoID.generate('api1', 'vid1')
        tid1 = ThumbnailID.generate(self.image, vid1)
        tdata1 = ThumbnailMetadata(tid1, vid1, ['one.jpg', 'two.jpg'],
                                   None, 
                                   self.image.size[1],
                                   self.image.size[0],
                                   'brightcove', model_score=float('-inf'))

        tdata1.save() 
        val = ThumbnailMetadata.get(tid1)
        self.assertIsNotNone(val)
        self.assertEqual(val.urls, ['one.jpg', 'two.jpg'])

    @tornado.testing.gen_test
    def test_create_or_modify_async(self):
        vid1 = InternalVideoID.generate('api1', 'vid1')
        tid1 = ThumbnailID.generate(self.image, vid1)
        self.assertIsNone(ThumbnailMetadata.get(tid1))

        yield tornado.gen.Task(
            ThumbnailMetadata.modify,
            tid1,
            lambda x: x.urls.append('http://image.jpg'),
            create_missing=True)

        val = ThumbnailMetadata.get(tid1)
        self.assertIsNotNone(val)
        self.assertEqual(val.urls, ['http://image.jpg'])

    def test_create_or_modify_many(self):
        vid1 = InternalVideoID.generate('api1', 'vid1')
        tid1 = ThumbnailID.generate(self.image, vid1)
        tdata1 = ThumbnailMetadata(tid1, vid1, ['one.jpg', 'two.jpg'],
                                   None, self.image.size[1],
                                   self.image.size[0],
                                   'brightcove', 1.0, '1.2')
        tdata1.save()

        vid2 = InternalVideoID.generate('api1', 'vid2')
        tid2 = ThumbnailID.generate(self.image, vid2)
        self.assertIsNone(ThumbnailMetadata.get(tid2))

        def _add_thumb(d):
            for thumb in d.itervalues():
                thumb.urls.append('%s.jpg' % thumb.key)

        ThumbnailMetadata.modify_many(
            [tid1, tid2],
            _add_thumb,
            create_missing=True)

        self.assertEqual(ThumbnailMetadata.get(tid1).urls,
                         ['one.jpg', 'two.jpg', '%s.jpg' % tid1])
        self.assertEqual(ThumbnailMetadata.get(tid2).urls,
                         ['%s.jpg' % tid2])

    @tornado.testing.gen_test
    def test_create_or_modify_many_async(self):
        vid1 = InternalVideoID.generate('api1', 'vid1')
        tid1 = ThumbnailID.generate(self.image, vid1)
        tdata1 = ThumbnailMetadata(tid1, vid1, ['one.jpg', 'two.jpg'],
                                   None, self.image.size[1],
                                   self.image.size[0],
                                   'brightcove', 1.0, '1.2')
        tdata1.save()

        vid2 = InternalVideoID.generate('api1', 'vid2')
        tid2 = ThumbnailID.generate(self.image, vid2)
        self.assertIsNone(ThumbnailMetadata.get(tid2))

        def _add_thumb(d):
            for thumb in d.itervalues():
                thumb.urls.append('%s.jpg' % thumb.key)

        yield tornado.gen.Task(
            ThumbnailMetadata.modify_many,
            [tid1, tid2],
            _add_thumb,
            create_missing=True)

        self.assertEqual(ThumbnailMetadata.get(tid1).urls,
                         ['one.jpg', 'two.jpg', '%s.jpg' % tid1])
        self.assertEqual(ThumbnailMetadata.get(tid2).urls,
                         ['%s.jpg' % tid2])
        
class TestAddingImageData(test_utils.neontest.AsyncTestCase):
    '''
    Test cases that add image data to thumbnails (and do uploads) 
    '''
    def setUp(self):
        # Mock out s3
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('hosting-bucket')
        self.bucket = self.s3conn.get_bucket('hosting-bucket')

        # Mock out cloudinary
        self.cloudinary_patcher = patch('cmsdb.cdnhosting.CloudinaryHosting')
        self.cloudinary_mock = self.cloudinary_patcher.start()
        future = Future()
        future.set_result(None)
        self.cloudinary_mock().hoster_type = "cloudinary"
        self.cloudinary_mock().upload.side_effect = [future]

        # Mock out the cdn url check
        self.cdn_check_patcher = patch('cmsdb.cdnhosting.utils.http')
        self.mock_cdn_url = self._future_wrap_mock(
            self.cdn_check_patcher.start().send_request)
        self.mock_cdn_url.side_effect = lambda x, **kw: HTTPResponse(x, 200)

        random.seed(1654984)

        self.image = PILImageUtils.create_random_image(360, 480)
        super(TestAddingImageData, self).setUp()

    def tearDown(self):
        self.s3_patcher.stop()
        self.cloudinary_patcher.stop()
        self.cdn_check_patcher.stop()
        conn = neondata.DBConnection.get(VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(ThumbnailMetadata)
        conn.clear_db()
        super(TestAddingImageData, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()
        super(TestAddingImageData, cls).tearDownClass()

    @tornado.testing.gen_test
    def test_lookup_cdn_info(self):

        # Create the necessary buckets so that we can write to them
        self.s3conn.create_bucket('n3.neon-images.com')
        self.s3conn.create_bucket('customer-bucket')
        self.s3conn.create_bucket('host-thumbnails')
        
        # Setup the CDN information in the database
        VideoMetadata(InternalVideoID.generate('acct1', 'vid1'),
                      i_id='i6').save()
        neon_cdnhosting_metadata = NeonCDNHostingMetadata(do_salt=False)
        neon_cdnhosting_metadata.crop_with_saliency = False
        neon_cdnhosting_metadata.crop_with_face_detection = False
        neon_cdnhosting_metadata.crop_with_text_detection = False
        s3_cdnhosting_metadata = \
            S3CDNHostingMetadata(bucket_name='customer-bucket',
                                 do_salt=False)
        s3_cdnhosting_metadata.crop_with_saliency = False
        s3_cdnhosting_metadata.crop_with_face_detection = False
        s3_cdnhosting_metadata.crop_with_text_detection = False

        cdn_list = CDNHostingMetadataList(
            CDNHostingMetadataList.create_key('acct1', 'i6'), 
            [neon_cdnhosting_metadata,
             s3_cdnhosting_metadata])
            # [ NeonCDNHostingMetadata(do_salt=False),
            #   S3CDNHostingMetadata(bucket_name='customer-bucket',
            #                        do_salt=False) ])
        cdn_list.save()

        thumb_info = ThumbnailMetadata(None, 'acct1_vid1',
                                       ttype=ThumbnailType.NEON, rank=3)
        yield thumb_info.add_image_data(self.image, async=True)

        # Check that the thumb_info was updated
        self.assertIsNotNone(thumb_info.key)
        self.assertEqual(thumb_info.width, 480)
        self.assertEqual(thumb_info.height, 360)
        self.assertIsNotNone(thumb_info.created_time)
        self.assertIsNotNone(thumb_info.phash)
        self.assertEqual(thumb_info.type, ThumbnailType.NEON)
        self.assertEqual(thumb_info.rank, 3)
        self.assertEqual(thumb_info.urls,
                         ['http://s3.amazonaws.com/host-thumbnails/%s.jpg' %
                          re.sub('_', '/', thumb_info.key)])

        # Make sure that the image was uploaded to s3 properly
        primary_hosting_key = re.sub('_', '/', thumb_info.key)+'.jpg'
        self.assertIsNotNone(self.s3conn.get_bucket('host-thumbnails').
                             get_key(primary_hosting_key))
        self.assertIsNotNone(self.s3conn.get_bucket('customer-bucket').
                             get_key('neontn%s_w480_h360.jpg'%thumb_info.key))
        # Make sure that some different size is found on the Neon CDN
        self.assertIsNotNone(self.s3conn.get_bucket('n3.neon-images.com').
                             get_key('neontn%s_w160_h120.jpg'%thumb_info.key))

        #NOTE: Redirects have been disabled temporarily
        # Check the redirect object
        #redirect = self.s3conn.get_bucket('host-thumbnails').get_key(
        #    'acct1/vid1/neon3.jpg')
        #self.assertIsNotNone(redirect)
        #self.assertEqual(redirect.redirect_destination,
        #                 '/' + primary_hosting_key)

    @tornado.testing.gen_test
    def test_add_thumbnail_to_video_and_save(self):
        '''
        Testing adding a thumbnail to the video object after it has been
        hosted in 2 places - Primary Neon copy and then to a specified customer
        hosting bucket 
        '''
        self.s3conn.create_bucket('customer-bucket')
        self.s3conn.create_bucket('host-thumbnails')

        cdn_metadata = S3CDNHostingMetadata(bucket_name='customer-bucket',
                                            do_salt=False) 

        video_info = VideoMetadata('acct1_vid1')
        thumb_info = ThumbnailMetadata(None,
                                       ttype=ThumbnailType.CUSTOMUPLOAD,
                                       rank=-1,
                                       frameno=35)

        yield video_info.add_thumbnail(thumb_info, self.image, [cdn_metadata],
                                       save_objects=True, async=True)
        primary_hosting_key = re.sub('_', '/', thumb_info.key)+'.jpg'
        
        self.assertEqual(thumb_info.video_id, video_info.key)
        self.assertGreater(len(thumb_info.urls), 0) # verify url insertion
        self.assertEqual(thumb_info.urls[0],
                'http://s3.amazonaws.com/host-thumbnails/%s' %\
                primary_hosting_key)

        self.assertIsNotNone(thumb_info.key)
        self.assertEqual(video_info.thumbnail_ids, [thumb_info.key])

        # Check that the images are in S3
        self.assertIsNotNone(self.s3conn.get_bucket('host-thumbnails').
                             get_key(primary_hosting_key))
        self.assertIsNotNone(self.s3conn.get_bucket('customer-bucket').
                             get_key('neontn%s_w480_h360.jpg'%thumb_info.key))
        #NOTE: Redirects have been disabled temporarily
        #redirect = self.s3conn.get_bucket('host-thumbnails').get_key(
        #    'acct1/vid1/customupload-1.jpg')
        #self.assertIsNotNone(redirect)
        #self.assertEqual(redirect.redirect_destination,
        #                 '/' + primary_hosting_key)

        # Check the database
        self.assertEqual(VideoMetadata.get('acct1_vid1').thumbnail_ids,
                         [thumb_info.key])
        self.assertEqual(ThumbnailMetadata.get(thumb_info.key).video_id,
                         'acct1_vid1')
    
    @tornado.testing.gen_test
    def test_add_thumbnail_to_video_and_save_with_cloudinary_hosting(self):
        '''
        Testing adding a thumbnail to the video object after it has been
        hosted in 2 places - Primary Neon copy and then to cloudinary 
        '''
        
        self.s3conn.create_bucket('host-thumbnails')
        cdn_metadata = CloudinaryCDNHostingMetadata()

        video_info = VideoMetadata('acct1_vid1')
        thumb_info = ThumbnailMetadata(None,
                                       ttype=ThumbnailType.CUSTOMUPLOAD,
                                       rank=-1,
                                       frameno=35)

        yield video_info.add_thumbnail(thumb_info, self.image, [cdn_metadata],
                                       save_objects=True, async=True)

        self.assertEqual(thumb_info.video_id, video_info.key)
        self.assertIsNotNone(thumb_info.key)
        self.assertEqual(video_info.thumbnail_ids, [thumb_info.key])

        # Check that the images are in S3
        primary_hosting_key = re.sub('_', '/', thumb_info.key)+'.jpg'
        self.assertIsNotNone(self.s3conn.get_bucket('host-thumbnails').
                             get_key(primary_hosting_key))
        # Check cloudinary
        cargs, kwargs = self.cloudinary_mock().upload.call_args
        self.assertEqual(cargs[1], thumb_info.key)
        self.assertEqual(cargs[2], thumb_info.urls[0])

    @tornado.testing.gen_test
    def test_add_thumbnail_to_video_and_save_new_video(self):
        self.s3conn.create_bucket('host-thumbnails')

        video_info = VideoMetadata('acct1_vid1', video_url='my.mp4')
        video_info.save()
        thumb_info = ThumbnailMetadata(None,
                                       ttype=ThumbnailType.CUSTOMUPLOAD,
                                       rank=-1,
                                       frameno=35)

        yield video_info.add_thumbnail(thumb_info, self.image, [],
                                       save_objects=True, async=True)

        self.assertEqual(thumb_info.video_id, video_info.key)
        self.assertIsNotNone(thumb_info.key)
        self.assertEqual(video_info.thumbnail_ids, [thumb_info.key])

        # Check the database
        self.assertEqual(VideoMetadata.get('acct1_vid1').thumbnail_ids,
                         [thumb_info.key])
        self.assertEqual(ThumbnailMetadata.get(thumb_info.key).video_id,
                         'acct1_vid1')

    @tornado.testing.gen_test
    def test_add_thumbnail_to_video_without_saving(self):
        self.s3conn.create_bucket('customer-bucket')
        self.s3conn.create_bucket('host-thumbnails')

        cdn_metadata = S3CDNHostingMetadata(bucket_name='customer-bucket',
                                            do_salt=False) 

        video_info = VideoMetadata('acct1_vid1')
        thumb_info = ThumbnailMetadata(None,
                                       ttype=ThumbnailType.CUSTOMUPLOAD,
                                       rank=-1,
                                       frameno=35)

        yield video_info.add_thumbnail(thumb_info, self.image, [cdn_metadata],
                                       save_objects=False, async=True)

        self.assertEqual(thumb_info.video_id, video_info.key)
        self.assertIsNotNone(thumb_info.key)
        self.assertEqual(video_info.thumbnail_ids, [thumb_info.key])

        # Check that the images are in S3
        primary_hosting_key = re.sub('_', '/', thumb_info.key)+'.jpg'
        self.assertIsNotNone(self.s3conn.get_bucket('host-thumbnails').
                             get_key(primary_hosting_key))
        self.assertIsNotNone(self.s3conn.get_bucket('customer-bucket').
                             get_key('neontn%s_w480_h360.jpg'%thumb_info.key))
        #NOTE: Redirects have been disabled temporarily
        #redirect = self.s3conn.get_bucket('host-thumbnails').get_key(
        #    'acct1/vid1/customupload-1.jpg')
        #self.assertIsNotNone(redirect)
        #self.assertEqual(redirect.redirect_destination,
        #                 '/' + primary_hosting_key)

        # Check the database is empty
        self.assertIsNone(VideoMetadata.get('acct1_vid1'))
        self.assertIsNone(ThumbnailMetadata.get(thumb_info.key))

    @tornado.testing.gen_test
    def test_download_and_add_thumbnail(self):
        self.s3conn.create_bucket('host-thumbnails')

        video_info = VideoMetadata('acct1_vid1')
        thumb_info = ThumbnailMetadata(None,
                                       ttype=ThumbnailType.CUSTOMUPLOAD,
                                       rank=-1,
                                       frameno=35)

        with patch('cmsdb.neondata.cvutils.imageutils.PILImageUtils') \
          as pil_mock:
            image_future = Future()
            image_future.set_result(self.image)
            pil_mock.download_image.return_value = image_future

            yield video_info.download_and_add_thumbnail(
                thumb_info, "http://my_image.jpg", cdn_metadata=[], async=True,
                save_objects=True)

            # Check that the image was downloaded
            pil_mock.download_imageassert_called_with("http://my_image.jpg",
                                                      async=True)

        self.assertEqual(thumb_info.video_id, video_info.key)
        self.assertIsNotNone(thumb_info.key)
        self.assertEqual(video_info.thumbnail_ids, [thumb_info.key])
        
        # Check that the images are in S3
        primary_hosting_key = re.sub('_', '/', thumb_info.key)+'.jpg'
        self.assertIsNotNone(self.s3conn.get_bucket('host-thumbnails').
                             get_key(primary_hosting_key))

        # Check that the database was updated
        self.assertEqual(VideoMetadata.get('acct1_vid1').thumbnail_ids,
                         [thumb_info.key])
        self.assertEqual(ThumbnailMetadata.get(thumb_info.key).video_id,
                         'acct1_vid1')

    @tornado.testing.gen_test
    def test_add_account_default_thumb(self):
        _log.info('here**')
        self.s3conn.create_bucket('host-thumbnails')
        self.s3conn.create_bucket('n3.neon-images.com')
        account = NeonUserAccount('a1')

        self.smartcrop_patcher = patch('cvutils.smartcrop.SmartCrop')
        self.mock_crop_and_resize = self.smartcrop_patcher.start()
        self.mock_responses = MagicMock()
        mock_image = PILImageUtils.create_random_image(540, 640)
        self.mock_crop_and_resize().crop_and_resize.side_effect = \
            lambda x, *kw: np.array(PILImageUtils.create_random_image(540, 640))

        yield account.add_default_thumbnail(self.image, async=True)
        self.assertGreater(self.mock_crop_and_resize.call_count, 0)
        self.smartcrop_patcher.stop()

        # Make sure that the thumbnail id is put in
        self.assertIsNotNone(account.default_thumbnail_id)
        self.assertEquals(
            account.default_thumbnail_id,
            NeonUserAccount.get(account.neon_api_key).default_thumbnail_id)

        # Make sure that the thubmnail info is in the database
        tmeta = ThumbnailMetadata.get(account.default_thumbnail_id)
        self.assertIsNotNone(tmeta)
        self.assertEquals(tmeta.type, ThumbnailType.DEFAULT)
        self.assertEquals(tmeta.rank, 0)
        self.assertGreater(len(tmeta.urls), 0)
        self.assertEquals(tmeta.width, 480)
        self.assertEquals(tmeta.height, 360)
        self.assertIsNotNone(tmeta.phash)

        # Make sure the image is hosted in s3
        primary_hosting_key = re.sub('_', '/', tmeta.key)+'.jpg'
        self.assertIsNotNone(self.s3conn.get_bucket('host-thumbnails').
                             get_key(primary_hosting_key))

        # If we try to add another image as the default, we should
        # throw an error
        new_image = PILImageUtils.create_random_image(540, 640)


        with self.assertRaises(ValueError):
            yield account.add_default_thumbnail(new_image, async=True)

        # Now force the new image to be added
        yield account.add_default_thumbnail(new_image, replace=True,
                                            async=True)

        # Check that the new thumb is in the account
        self.assertIsNotNone(account.default_thumbnail_id)
        self.assertNotEquals(account.default_thumbnail_id, tmeta.key)
        self.assertEquals(
            account.default_thumbnail_id,
            NeonUserAccount.get(account.neon_api_key).default_thumbnail_id)

        # Check the data in the new thumb
        tmeta2 = ThumbnailMetadata.get(account.default_thumbnail_id)
        self.assertIsNotNone(tmeta2)
        self.assertEquals(tmeta2.type, ThumbnailType.DEFAULT)
        self.assertEquals(tmeta2.rank, -1)
        self.assertGreater(len(tmeta2.urls), 0)
        self.assertEquals(tmeta2.width, 640)
        self.assertEquals(tmeta2.height, 540)
        self.assertIsNotNone(tmeta2.phash)

class TestPGThumbnailHelperClass(TestThumbnailHelperClass):
    def setUp(self): 
        self.image = PILImageUtils.create_random_image(360, 480)
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        file_str = os.path.join(__base_path__, '/cmsdb/test/cmsdb.sql')
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()

    def test_thumbnail_mapper(self):
        # we are not carrying over the thumbnail url mapper 
        self.assertEquals(1,1) 

class TestPGAddingImageData(TestAddingImageData): 
    def setUp(self):
        # Mock out s3
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('hosting-bucket')
        self.bucket = self.s3conn.get_bucket('hosting-bucket')

        # Mock out cloudinary
        self.cloudinary_patcher = patch('cmsdb.cdnhosting.CloudinaryHosting')
        self.cloudinary_mock = self.cloudinary_patcher.start()
        future = Future()
        future.set_result(None)
        self.cloudinary_mock().hoster_type = "cloudinary"
        self.cloudinary_mock().upload.side_effect = [future]

        # Mock out the cdn url check
        self.cdn_check_patcher = patch('cmsdb.cdnhosting.utils.http')
        self.mock_cdn_url = self._future_wrap_mock(
            self.cdn_check_patcher.start().send_request)
        self.mock_cdn_url.side_effect = lambda x, **kw: HTTPResponse(x, 200)

        random.seed(1654984)

        self.image = PILImageUtils.create_random_image(360, 480)
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self):
        self.s3_patcher.stop()
        self.cloudinary_patcher.stop()
        self.cdn_check_patcher.stop()
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        file_str = os.path.join(__base_path__, '/cmsdb/test/cmsdb.sql')
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()
        super(TestPGAddingImageData, cls).tearDownClass()

class TestPostgresDBConnections(test_utils.neontest.AsyncTestCase):
    def setUp(self): 
        super(TestPostgresDBConnections, self).setUp()
        # do this in setup because its tough to shutdown, and restart 
        # from tests otherwise, this should be the only place where 
        # this is done, as the operation is slow. 
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        self.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    def tearDown(self): 
        super(TestPostgresDBConnections, self).tearDown()
        options._set('cmsdb.neondata.wants_postgres', 0)
        self.postgresql.stop()
    
    @tornado.testing.gen_test(timeout=20.0) 
    def test_retry_connection_fails(self): 
        exception_mocker = patch('momoko.Connection.connect')
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = psycopg2.OperationalError('blah blah')

        pg1 = neondata.PostgresDB()
        with self.assertRaises(Exception):
            yield pg1.get_connection()
        exception_mocker.stop()

    @tornado.testing.gen_test(timeout=20.0) 
    def test_retry_connection_fails_then_success(self): 
        exception_mocker = patch('momoko.Connection.connect')
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = psycopg2.OperationalError('blah blah')

        pg1 = neondata.PostgresDB()
        with self.assertRaises(Exception):
            yield pg1.get_connection()
        exception_mocker.stop()
        conn = yield pg1.get_connection() 
        self.assertTrue("dbname=test" in conn.dsn)

    @tornado.testing.gen_test(timeout=20.0) 
    def test_database_restarting(self):
        pg = neondata.PostgresDB()
        conn = yield pg.get_connection()  
        self.assertTrue("dbname=test" in conn.dsn) 
        self.postgresql.stop() 
        file_str = os.path.join(__base_path__, '/cmsdb/test/cmsdb.sql')
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        postgresql_two = test_utils.postgresql.Postgresql(dump_file=dump_file, 
                  dbname='test2')
        conn = yield pg.get_connection()
        self.assertTrue("dbname=test2" in conn.dsn)
        self.postgresql.setup() 
        self.postgresql.start()
        conn = yield pg.get_connection()
        self.assertTrue("dbname=test" in conn.dsn)
 
    @tornado.testing.gen_test(timeout=20.0) 
    def test_max_io_loop_size(self):
        pg = neondata.PostgresDB()
        old_io_loop_size = options.get('cmsdb.neondata.max_io_loop_dict_size')
        options._set('cmsdb.neondata.max_io_loop_dict_size', 2)
        i1 = tornado.ioloop.IOLoop()
        i1.running = False 
        i2 = tornado.ioloop.IOLoop()
        i2.running = False 
        i3 = tornado.ioloop.IOLoop()
        i3.running = False
        item = {} 
        item['pool'] = None
        item2 = {} 
        item2['pool'] = momoko.Pool('test')  
        pg.io_loop_dict[i1] = True
        pg.io_loop_dict[i2] = item 
        pg.io_loop_dict[i3] = item2
        self.assertEquals(len(pg.io_loop_dict), 3)
        conn = yield pg.get_connection() 
        self.assertEquals(len(pg.io_loop_dict), 1)
        options._set('cmsdb.neondata.max_io_loop_dict_size', old_io_loop_size)

class TestPostgresDB(test_utils.neontest.AsyncTestCase):
    def setUp(self): 
        super(TestPostgresDB, self).setUp()
    def tearDown(self): 
        neondata.PostgresDB.instance = None
        super(TestPostgresDB, self).tearDown()
    
    @classmethod
    def setUpClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 1)
        file_str = os.path.join(__base_path__, '/cmsdb/test/cmsdb.sql')
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()

    @tornado.testing.gen_test 
    def test_singletoness(self): 
        pg1 = neondata.PostgresDB() 
        pg2 = neondata.PostgresDB() 

        self.assertEquals(id(pg1), id(pg2))

    @tornado.testing.gen_test
    def test_pool_connections(self):
        pg = neondata.PostgresDB()
        conn = yield pg.get_connection()
        conn = yield pg.get_connection()
        conn = yield pg.get_connection()
        conn = yield pg.get_connection()
        # pool size should be 1 all from same ioloop
        self.assertEquals(1, len(pg.io_loop_dict))

    @tornado.testing.gen_test
    def test_pool_momoko_connections(self):
        pg = neondata.PostgresDB()
        conn1 = yield pg.get_connection()
        conn2 = yield pg.get_connection()
        conn3 = yield pg.get_connection()
        conn4 = yield pg.get_connection()
        pool = pg.io_loop_dict[tornado.ioloop.IOLoop.current()]['pool']
        self.assertEquals(len(pool.conns.free), 0)
        # first conn won't be in the pool should be 3 
        self.assertEquals(len(pool.conns.busy), 3)
        pg.return_connection(conn2) 
        self.assertEquals(len(pool.conns.busy), 2) 
        self.assertEquals(len(pool.conns.free), 1)
        pg.return_connection(conn3) 
        self.assertEquals(len(pool.conns.busy), 1) 
        self.assertEquals(len(pool.conns.free), 2)
        pg.return_connection(conn4) 
        self.assertEquals(len(pool.conns.busy), 0) 
        self.assertEquals(len(pool.conns.free), 3)

    @tornado.testing.gen_test(timeout=4.0)
    def test_pool_momoko_going_dead(self):
        pg = neondata.PostgresDB()
        conn1 = yield pg.get_connection()
        conn2 = yield pg.get_connection()
        pool = pg.io_loop_dict[tornado.ioloop.IOLoop.current()]['pool']
        pool.conns.dead.add(conn2)
        self.assertEquals(len(pool.conns.dead), 1) 
        conn3 = yield pg.get_connection()
        # this would previously hang, and die out because momoko would 
        # not return a connection
        conn3 = yield pg.get_connection()
        
        self.assertEquals(len(pool.conns.dead), 0) 

class TestPostgresPubSub(test_utils.neontest.AsyncTestCase):
    def setUp(self): 
        super(TestPostgresPubSub, self).setUp()
    def tearDown(self): 
        neondata.PostgresPubSub.instance = None
        super(TestPostgresPubSub, self).tearDown()
    
    @classmethod
    def setUpClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 1)
        file_str = os.path.join(__base_path__, '/cmsdb/test/cmsdb.sql')
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()

    @tornado.testing.gen_test()
    def test_listen_and_notify(self): 
        cb = MagicMock()     
        pubsub = neondata.PostgresPubSub()
        pubsub.listen('neonuseraccount', cb)
        so = neondata.NeonUserAccount(uuid.uuid1().hex)
        rv = yield so.save(async=True)
        yield tornado.gen.sleep(0.01) 
        cb.assert_called_with(ANY)
 
    @tornado.testing.gen_test()
    def test_subscribe_to_changes(self):
        cb = MagicMock()     
        yield neondata.NeonUserAccount.subscribe_to_changes(cb, async=True)
        so = neondata.NeonUserAccount(uuid.uuid1().hex)
        rv = yield so.save(async=True)
        yield tornado.gen.sleep(0.01) 
        cb.assert_called_with(so.get_id(), ANY, ANY)
 
    @tornado.testing.gen_test()
    def test_unsubscribe_from_changes(self):
        cb = MagicMock()     
        yield neondata.NeonUserAccount.subscribe_to_changes(cb, async=True)
        so = neondata.NeonUserAccount(uuid.uuid1().hex)
        rv = yield so.save(async=True)

        yield neondata.NeonUserAccount.unsubscribe_from_changes('*', async=True)
        so2 = neondata.NeonUserAccount(uuid.uuid1().hex)
        rv = yield so2.save(async=True)

        yield tornado.gen.sleep(0.01) 
        cb.assert_called_once_with(so.get_id(), ANY, ANY)
 
    @tornado.testing.gen_test()
    def test_multiple_functions_listening(self):
        cb = MagicMock()     
        yield neondata.NeonUserAccount.subscribe_to_changes(cb, async=True)

        cb2 = MagicMock() 
        yield neondata.NeonUserAccount.subscribe_to_changes(cb2, async=True)

        so = neondata.NeonUserAccount(uuid.uuid1().hex)
        rv = yield so.save(async=True)

        yield tornado.gen.sleep(0.01)
        cb.assert_called_once_with(so.get_id(), ANY, ANY)
        cb2.assert_called_once_with(so.get_id(), ANY, ANY)

    @tornado.testing.gen_test()
    def test_payload_too_long_save(self):
        self.returned_key = None 
        def cb(key, data, op):
            self.returned_key = key 
        yield neondata.ThumbnailStatus.subscribe_to_changes(cb, async=True)
        tn = neondata.ThumbnailStatus('123')
        for i in range(1,500): 
            tn.serving_history.append([time.time(), '0.21'])  
        yield tn.save(async=True)
        yield self.assertWaitForEquals(lambda: '123' in self.returned_key, 
            True, 
            async=True)
 
    @tornado.testing.gen_test()
    def test_payload_too_long_delete(self):
        self.returned_key = None
        self.returned_op = None  
        def cb(key, data, op):
            self.returned_key = key 
            self.returned_op = op 
        yield neondata.ThumbnailStatus.subscribe_to_changes(cb, async=True)
        tn = neondata.ThumbnailStatus('123')
        for i in range(1,500): 
            tn.serving_history.append([time.time(), '0.21'])  
        yield tn.save(async=True)
        yield self.assertWaitForEquals(lambda: '123' in self.returned_key and  
            'INSERT' in self.returned_op, 
            True, 
            async=True) 
        yield tn.delete(tn.key, async=True) 
        yield self.assertWaitForEquals(lambda: '123' in self.returned_key and 
            'DELETE' in self.returned_op, 
            True, 
            async=True) 

class TestPGPlatformAndIntegration(test_utils.neontest.AsyncTestCase):
    def setUp(self): 
        super(TestPGPlatformAndIntegration, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(TestPGPlatformAndIntegration, self).tearDown()
    
    @classmethod
    def setUpClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 1)
        file_str = os.path.join(__base_path__, '/cmsdb/test/cmsdb.sql')
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()

    @tornado.testing.gen_test 
    def test_modify_brightcove_platform(self):
        def _initialize_bc_plat(x):
            x.account_id = '123'
            x.publisher_id = '123'
            x.read_token = 'abc'
            x.write_token = 'def'
            x.last_process_date = time.time()
        bc = yield tornado.gen.Task(
              neondata.BrightcovePlatform.modify,
              '45', '82',
              _initialize_bc_plat, create_missing=True)
 
    @tornado.testing.gen_test 
    def test_modify_brightcove_integration(self):
        def _initialize_bc_int(x):
            x.account_id = '123'
            x.publisher_id = '123'
            x.read_token = 'abc'
            x.write_token = 'def'
            x.last_process_date = time.time()
        bc = yield tornado.gen.Task(
              neondata.BrightcoveIntegration.modify,
              '45', _initialize_bc_int, create_missing=True)

class BasePGNormalObject(object):
    @classmethod 
    def _get_object_type(cls):
        raise NotImplementedError()

    @classmethod 
    def _make_keys(cls, obj): 
        obj_dict = obj.__dict__ 
        key_values = []
        for tup in cls.keys: 
            if tup[0] == 'dynamic': 
                key_values.append(obj_dict[tup[1]]) 
            else: 
                key_values.append(tup[1])
        return key_values  
            
    @tornado.gen.coroutine 
    def _get_get_function(cls, obj):
        new_keys = cls._make_keys(obj)  
        if len(new_keys) == 2:
            rv = yield obj.get(new_keys[0], 
                               new_keys[1], 
                               async=True) 
        elif len(cls.keys) == 1: 
            rv = yield obj.get(new_keys[0], 
                               async=True)
        else: 
            rv = yield obj.get(obj.key, 
                               async=True) 
        raise tornado.gen.Return(rv) 
 
    @tornado.gen.coroutine 
    def _get_delete_function(cls, obj):
        new_keys = cls._make_keys(obj)  
        if len(new_keys) == 2:
            rv = yield obj.delete(new_keys[0], 
                                  new_keys[1], 
                                  async=True) 
        elif len(cls.keys) == 1: 
            rv = yield obj.delete(new_keys[0], 
                                  async=True)
        else: 
            rv = yield obj.delete(obj.key, 
                                  async=True) 
        raise tornado.gen.Return(rv) 

    @tornado.gen.coroutine 
    def _run_delete_many_function(cls, objs): 
        key_set = [] 
        for obj in objs:
            keys = cls._make_keys(obj)
            if len(keys) == 2:  
                key_set.append(cls._make_keys(obj))
            else: 
               key_set.append(keys[0])
        rv = yield obj.delete_many(key_set, async=True)
        raise tornado.gen.Return(rv) 
 
    @tornado.gen.coroutine 
    def _run_get_many_function(cls, objs): 
        key_set = [] 
        for obj in objs:
            keys = cls._make_keys(obj)
            if len(keys) == 2:  
                key_set.append(cls._make_keys(obj))
            else: 
               key_set.append(keys[0])
        rv = yield obj.get_many(key_set, async=True)
        raise tornado.gen.Return(rv)
 
    @tornado.gen.coroutine 
    def _run_modify_many_function(cls, objs, mocker): 
        key_set = [] 
        for obj in objs:
            keys = cls._make_keys(obj)
            if len(keys) == 2:  
                key_set.append(cls._make_keys(obj))
            else: 
               key_set.append(keys[0])
        rv = yield obj.modify_many(key_set, mocker, async=True)
        raise tornado.gen.Return(rv) 
 
    @tornado.gen.coroutine 
    def _run_modify_function(cls, obj, mocker):
        new_keys = cls._make_keys(obj)  
        if len(new_keys) == 2:
            rv = yield obj.modify(new_keys[0], 
                                  new_keys[1],
                                  mocker,  
                                  async=True) 
        elif len(cls.keys) == 1: 
            rv = yield obj.modify(new_keys[0], 
                                  mocker, 
                                  async=True)
        else: 
            rv = yield obj.modify(obj.key,
                                  mocker,  
                                  async=True) 
        raise tornado.gen.Return(rv) 

    @tornado.testing.gen_test
    def test_save_object(self):
        obj_id = uuid.uuid1().hex 
        obj_type = self._get_object_type()
        obj = obj_type(obj_id) 
        rv = yield obj.save(async=True)
        get_obj = yield self._get_get_function(obj) 
        self.assertTrue(rv)
        self.assertEquals(get_obj.key, obj.key) 

    @tornado.testing.gen_test 
    def test_save_duplicate_objects(self):     
        unique_id = uuid.uuid1().hex 
        so1 = self._get_object_type()(unique_id, 'test1') 
        so2 = self._get_object_type()(unique_id, 'test1') 
        yield so1.save(async=True) 
        yield so2.save(async=True)  
        get_obj_one = yield self._get_get_function(so1) 
        get_obj_two = yield self._get_get_function(so2) 
        self.assertEquals(get_obj_one.key,get_obj_two.key)
    
    @tornado.testing.gen_test 
    def test_save_all_objects(self):    
        key1 = uuid.uuid1().hex
        key2 = uuid.uuid1().hex 
        so1 = self._get_object_type()(key1, 'test1')
        so2 = self._get_object_type()(key2, 'test2')
        self._get_object_type().save_all([so1, so2])
        get_obj_one = yield self._get_get_function(so1) 
        get_obj_two = yield self._get_get_function(so2) 
        self.assertEquals(get_obj_one.key,so1.key)
        self.assertEquals(get_obj_two.key,so2.key)
     
    @tornado.testing.gen_test 
    def test_get_many_objects(self):     
        so1 = self._get_object_type()(uuid.uuid1().hex, 'test')
        so2 = self._get_object_type()(uuid.uuid1().hex, 'test2')
        yield self._get_object_type().save_all([so1, so2], async=True)
        results = yield self._run_get_many_function([so1,so2])
        self.assertEquals(len(results), 2)

    @tornado.testing.gen_test 
    def test_get_many_with_key_like_objects(self):    
        key1 = uuid.uuid1().hex 
        #key2 = uuid.uuid1().hex
        so1 = self._get_object_type()(key1, 'testabcdef')
        so2 = self._get_object_type()(key1, 'testfedcba')
        yield self._get_object_type().save_all([so1, so2], async=True)
        results = yield so1.get_many_with_key_like(so1.key, async=True) 
        self.assertEquals(len(results), 1)
        results = yield so1.get_many_with_key_like(so2.key, async=True) 
        self.assertEquals(len(results), 1)

    @tornado.testing.gen_test 
    def test_delete_object(self):  
        so1 = self._get_object_type()(uuid.uuid1().hex) 
        yield self._get_delete_function(so1) 
        get1 = yield self._get_get_function(so1)
        self.assertEquals(None, get1)

    @tornado.testing.gen_test 
    def test_delete_many_objects(self): 
        so1 = self._get_object_type()(uuid.uuid1().hex, 'test1')
        so2 = self._get_object_type()(uuid.uuid1().hex, 'test2')
        yield so1.save(async=True)
        yield so2.save(async=True)
        yield self._run_delete_many_function([so1, so2]) 
        get1 = yield self._get_get_function(so1)
        self.assertEquals(None, get1)
        get2 = yield self._get_get_function(so2)
        self.assertEquals(None, get2)

    @tornado.testing.gen_test
    def test_delete_many_objects_key_dne(self): 
        so1 = self._get_object_type()(uuid.uuid1().hex, 'test1')
        so2 = self._get_object_type()(uuid.uuid1().hex, 'test2')
        so3 = self._get_object_type()('doesnotexist')
        yield so1.save(async=True)
        yield so2.save(async=True)
        yield self._run_delete_many_function([so1,so2,so3]) 
        get1 = yield self._get_get_function(so1)
        self.assertEquals(None, get1)
        get2 = yield self._get_get_function(so2)
        self.assertEquals(None, get2)

    @tornado.testing.gen_test
    def test_modify_object(self):
        modify_me = MagicMock()  
        so = self._get_object_type()(uuid.uuid1().hex, 'test1')
        yield so.save(async=True)
        yield self._run_modify_function(so, modify_me) 
        self.assertEquals(modify_me.call_args[0][0].key, so.key)
        self.assertEquals(modify_me.call_count, 1)

    @tornado.testing.gen_test 
    def test_modify_many_objects(self):     
        modify_me = MagicMock()  
        so1 = self._get_object_type()(uuid.uuid1().hex, 'test1')
        so2 = self._get_object_type()(uuid.uuid1().hex, 'test2')
        yield so1.save(async=True)
        yield so2.save(async=True)
        yield self._run_modify_many_function([so1,so2], modify_me) 
        self.assertEquals(1,1)
        keys = []
        for iter_item in modify_me.call_args[0][0].iteritems(): 
            keys.append(iter_item[1].key) 
        self.assertTrue(so2.key in keys) 
        self.assertTrue(so1.key in keys) 
        self.assertEquals(modify_me.call_count, 1)

class TestPGVideoMetadata(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        BasePGNormalObject.keys = [('dynamic', 'key')] 
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @classmethod 
    def _get_object_type(cls): 
        return VideoMetadata

    @tornado.testing.gen_test 
    def test_base_search_videos(self):
        # this function is tested more thoroughly 
        # in the api tests, this is here as a sanity 
        # check. not going to double up the tests at this point
        request = NeonApiRequest('r1', 'acct1')
        request.video_title="pie ala mode" 
        yield request.save(async=True) 
        video_info = VideoMetadata('acct1_vid1', request_id='r1')
        yield video_info.save(async=True)

        results = yield neondata.VideoMetadata.search_videos()
        self.assertEquals(len(results['videos']), 1)

class TestPGVerification(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        BasePGNormalObject.keys = [('dynamic', 'key')] 
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @classmethod 
    def _get_object_type(cls): 
        return neondata.Verification

class TestPGAccountLimits(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        BasePGNormalObject.keys = [('dynamic', 'key')] 
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @classmethod 
    def _get_object_type(cls): 
        return neondata.AccountLimits

class TestPGNeonRequest(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        BasePGNormalObject.keys = [('dynamic', 'key'), ('static', 'a1')]
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @classmethod 
    def _get_object_type(cls): 
        return NeonApiRequest
    
    @tornado.testing.gen_test 
    def test_save_request_with_special_character(self): 
        request_id = uuid.uuid1().hex 
        request = NeonApiRequest(request_id, 'a1')
        request.video_title="pie a'la mode" 
        yield request.save(async=True) 
        out_of_db = yield request.get(request.job_id, 'a1', async=True) 
        self.assertEquals(out_of_db.key, request.key) 

class TestPGUser(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        BasePGNormalObject.keys = [('dynamic', 'key')] 
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @classmethod
    def _get_object_type(cls): 
        return User

    @tornado.testing.gen_test 
    def test_get_associated_account_ids_single(self):
        new_user = User(username='test_user') 
        yield new_user.save(async=True)
        new_account = NeonUserAccount('test_account')      
        new_account.users.append('test_user') 
        yield new_account.save(async=True)
        
        a_ids = yield new_user.get_associated_account_ids(async=True)
        self.assertEquals(1, len(a_ids)) 
        a_id = a_ids[0] 
        self.assertEquals(a_id, new_account.neon_api_key)
 
    @tornado.testing.gen_test 
    def test_get_associated_account_ids_multiple(self):
        new_user = User(username='test_user') 
        yield new_user.save(async=True)
        new_account_one = NeonUserAccount('test_account1')      
        new_account_one.users.append('test_user') 
        yield new_account_one.save(async=True)

        new_account_two = NeonUserAccount('test_account2')      
        new_account_two.users.append('test_user') 
        yield new_account_two.save(async=True)
        
        a_ids = yield new_user.get_associated_account_ids(async=True)
        self.assertEquals(2, len(a_ids))
        self.assertItemsEqual([new_account_one.neon_api_key, 
                                new_account_two.neon_api_key], 
                              a_ids)  

    @tornado.testing.gen_test 
    def test_get_associated_account_ids_empty(self):
        new_user = User(username='test_user') 
        yield new_user.save(async=True)
        a_ids = yield new_user.get_associated_account_ids(async=True)
        self.assertEquals(0, len(a_ids)) 
    
class TestPGNeonUserAccount(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        BasePGNormalObject.keys = [('dynamic', 'key')] 
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @classmethod
    def _get_object_type(cls): 
        return NeonUserAccount
        
    @tornado.testing.gen_test 
    def test_get_api_key_successfully(self):
        so = neondata.NeonUserAccount(uuid.uuid1().hex)
        yield so.save(async=True)
        so2 = neondata.NeonUserAccount(so.account_id)
        self.assertEquals(so.neon_api_key, so2.neon_api_key)
 
    @tornado.testing.gen_test 
    def test_get_videos_and_statuses(self):
        api_key = 'key'
        i_vid = InternalVideoID.generate(api_key, 'vid1')
        tid = i_vid + "_t1"
        ThumbnailMetadata(tid, i_vid).save()
        VideoMetadata(i_vid, [tid],'job1').save()
        neondata.VideoStatus(i_vid, 'complete').save()
        neondata.ThumbnailStatus(tid, 0.2).save()

        so = neondata.NeonUserAccount('key', api_key='key')
        yield so.save(async=True)
        yield so.get_videos_and_statuses(async=True)

    @tornado.testing.gen_test 
    def test_mm_neon_user_account(self):
        def _m_me(a): 
            for obj in a.itervalues(): 
                if obj is not None: 
                    obj.neon_api_key = 'asdfaafds'
        so = neondata.NeonUserAccount(uuid.uuid1().hex)
        yield so.save(async=True)
        neondata.NeonUserAccount.modify_many([so.key], _m_me) 
        get_me = yield so.get(so.key, async=True)
        self.assertEquals('asdfaafds', get_me.neon_api_key)
    
    @tornado.testing.gen_test
    def test_base_get_integrations(self): 
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True) 
        bi = neondata.BrightcoveIntegration('kevinacct')
        yield bi.save(async=True)
        oi = neondata.OoyalaIntegration('kevinacct')
        yield oi.save(async=True)
        bi = neondata.BrightcoveIntegration('kevinacct')
        yield bi.save(async=True)
        integrations = yield so.get_integrations(async=True)

        self.assertEquals(len(integrations), 3)  
        # test order by 
        self.assertEquals(type(integrations[2]), neondata.OoyalaIntegration) 
        self.assertEquals(integrations[2].account_id, 'kevinacct')
 
    @tornado.testing.gen_test
    def test_empty_get_integrations(self): 
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True) 
        integrations = yield so.get_integrations(async=True)
        self.assertEquals(len(integrations), 0)  


if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
