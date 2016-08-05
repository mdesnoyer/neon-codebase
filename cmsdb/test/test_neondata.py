#!/usr/bin/env python
# encoding: utf-8

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
import random
import socket
import string
import subprocess
import test_utils.neontest
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
from StringIO import StringIO
from cmsdb import neondata
from cmsdb.neondata import (
    AbstractPlatform,
    AkamaiCDNHostingMetadata,
    BrightcoveApiRequest,
    BrightcovePlayer,
    BrightcovePlatform,
    CDNHostingMetadata,
    CDNHostingMetadataList,
    CloudinaryCDNHostingMetadata,
    ExperimentState,
    ExperimentStrategy,
    InternalVideoID,
    NeonApiKey,
    NeonApiRequest,
    NeonCDNHostingMetadata,
    NeonPlatform,
    NeonUserAccount,
    OoyalaApiRequest,
    OoyalaPlatform,
    S3CDNHostingMetadata,
    Tag,
    TagThumbnail,
    ThumbnailID,
    ThumbnailMetadata,
    ThumbnailServingURLs,
    ThumbnailStatus,
    ThumbnailType,
    ThumbnailURLMapper,
    TrackerAccountIDMapper,
    User,
    VideoMetadata,
    VideoStatus,
    YoutubeApiRequest,
    YoutubePlatform )
from cvutils import smartcrop
import numpy as np

_log = logging.getLogger(__name__)

# Supress momoko debug.
logging.getLogger('momoko').setLevel(logging.INFO)


class NeonDbTestCase(test_utils.neontest.AsyncTestCase):

    def setUp(self):
        logging.getLogger('cmsdb.neondata').reset_sample_counters()
        super(NeonDbTestCase, self).setUp()

    def tearDown(self):
        self.postgresql.clear_all_tables()
        super(NeonDbTestCase, self).setUp()

    @classmethod
    def setUpClass(cls):
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()

    @classmethod
    def _get_object_type(cls):
        return cls.__name__


class TestNeondataDataSpecific(NeonDbTestCase):
    def setUp(self):
        self.maxDiff = 5000
        super(TestNeondataDataSpecific, self).setUp()

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
                     BrightcoveApiRequest('jobbc', 'acct1')]

        requests3 = [OoyalaApiRequest('joboo', 'acct3')]
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
                     BrightcoveApiRequest('jobbc', 'acct1')]

        requests3 = [OoyalaApiRequest('joboo', 'acct3')]
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
                     BrightcoveApiRequest('jobbc', 'acct1')]
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
        VideoStatus(i_vid, 'complete').save()
        ThumbnailStatus(tid, 0.2).save()
        ThumbnailServingURLs(tid, sizes=[(640,480)]).save()

        VideoMetadata.delete_related_data(i_vid)

        self.assertIsNone(VideoMetadata.get(i_vid))
        self.assertEquals(VideoStatus.get(i_vid).experiment_state,
                          'unknown')
        self.assertIsNone(NeonApiRequest.get('job1', api_key))
        self.assertIsNone(ThumbnailMetadata.get(tid))
        self.assertIsNone(ThumbnailServingURLs.get(tid))
        self.assertIsNone(ThumbnailStatus.get(tid).serving_frac)

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
        folder_in_cdn_prefixes = AkamaiCDNHostingMetadata._create(
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

        folder_not_in_cdn_prefixes = AkamaiCDNHostingMetadata._create(
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
        cdn_list = CDNHostingMetadataList._create(
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
        VideoStatus(vid, winner_tid='acct1_vid1_t2').save()
        self.assertEquals(video_meta.get_winner_tid(), 'acct1_vid1_t2')

    def test_video_status_history(self):
        vid = InternalVideoID.generate('acct1', 'vid1')
        VideoStatus(vid).save()
        video_status = VideoStatus.get(vid)
        self.assertEquals(video_status.experiment_state,
                          ExperimentState.UNKNOWN)
        self.assertEquals(video_status.state_history, [])
        def _update(status):
            status.set_experiment_state(ExperimentState.COMPLETE)
        VideoStatus.modify(vid, _update)
        video_status = VideoStatus.get(vid)
        self.assertEquals(video_status.experiment_state,
                          ExperimentState.COMPLETE)
        self.assertEquals(video_status.state_history[0][1],
                          ExperimentState.COMPLETE)
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
        bc_request = BrightcoveApiRequest(
            'bc_job', 'api_key', 'vid0', 'title',
            'url', 'rtoken', 'wtoken', 'pid',
            'callback_url', 'i_id',
            'default_thumbnail')
        bc_request.save()

        bc_found = NeonApiRequest.get('bc_job', 'api_key')
        self.assertEquals(bc_found.key, 'request_api_key_bc_job')
        self.assertEquals(bc_request, bc_found)
        self.assertIsInstance(bc_found, BrightcoveApiRequest)

        oo_request = OoyalaApiRequest(
            'oo_job', 'api_key', 'i_id', 'vid0', 'title',
            'url', 'oo_api_key', 'oo_secret_key',
            'callback_url', 'default_thumbnail')
        oo_request.save()

        self.assertEquals(oo_request, NeonApiRequest.get('oo_job', 'api_key'))

        yt_request = YoutubeApiRequest(
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

        self.assertIsInstance(obj, BrightcoveApiRequest)
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
      breq = BrightcoveApiRequest('job2', 'acct1')
      oreq = OoyalaApiRequest('job3', 'acct1')
      yreq = YoutubeApiRequest('job4', 'acct1')
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


class TestThumbnailHelperClass(NeonDbTestCase):
    '''Thumbnail ID Mapper and other thumbnail helper class tests'''
    def setUp(self):
        self.image = PILImageUtils.create_random_image(360, 480)
        super(TestThumbnailHelperClass, self).setUp()

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


class TestAddingImageData(NeonDbTestCase):
    '''Test cases that add image data to thumbnails and do uploads'''
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
        super(TestAddingImageData, self).tearDown()

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

        video_info = VideoMetadata('acct1_vid1', tag_id='tag_id')
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


class TestPostgresDBConnections(NeonDbTestCase):
    '''Test Postgres connections.'''
    def setUp(self):
        super(TestPostgresDBConnections, self).setUp()
        # do this in setup because its tough to shutdown, and restart
        # from tests otherwise, this should be the only place where
        # this is done, as the operation is slow.
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        self.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    def tearDown(self):
        super(TestPostgresDBConnections, self).tearDown()
        self.postgresql.stop()

    @tornado.testing.gen_test
    def test_retry_connection_fails(self):
        ps = 'momoko.Connection.connect'
        exception_mocker = patch('momoko.Connection.connect')
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = psycopg2.OperationalError('blah blah')
        pg1 = neondata.PostgresDB()
        with options._set_bounded('cmsdb.neondata.max_connection_retries', 1):
            with self.assertRaises(Exception):
                yield pg1.get_connection()
        exception_mocker.stop()

    @tornado.testing.gen_test
    def test_retry_connection_fails_then_success(self):
        exception_mocker = patch('momoko.Connection.connect')
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = psycopg2.OperationalError('blah blah')

        pg1 = neondata.PostgresDB()
        with options._set_bounded('cmsdb.neondata.max_connection_retries', 1): 
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


class TestPostgresDB(NeonDbTestCase):
    def tearDown(self):
        neondata.PostgresDB.instance = None
        super(TestPostgresDB, self).tearDown()

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

    @tornado.testing.gen_test
    def test_pool_momoko_starving(self):
        with options._set_bounded('cmsdb.neondata.max_pool_size', 3),\
         options._set_bounded('cmsdb.neondata.max_connection_retries', 1):
            pg = neondata.PostgresDB()
            # fill up the pool, with a flurry of connections
            yield pg.get_connection()
            yield pg.get_connection()
            rt1 = yield pg.get_connection()
            rt2 = yield pg.get_connection()
            with self.assertRaises(Exception):
                with self.assertLogExists(logging.ERROR, 'Retrying PG'):
                    cwt = options.get('cmsdb.neondata.connection_wait_time')
                    with options._set_bounded(
                        'cmsdb.neondata.connection_wait_time', 0.1):
                        yield pg.get_connection()

        # now return the connections and make sure we can get more
        pg.return_connection(rt1)
        pg.return_connection(rt2)
        new_conn = yield pg.get_connection()
        self.assertEquals(type(new_conn), momoko.connection.Connection)

class TestPostgresPubSub(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestPostgresPubSub, self).setUp()

    def tearDown(self):
        neondata.PostgresPubSub.instance = None
        super(TestPostgresPubSub, self).tearDown()

    @classmethod
    def setUpClass(cls):
        file_str = os.path.join(__base_path__, '/cmsdb/test/cmsdb.sql')
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls):
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


class TestPlatformAndIntegration(NeonDbTestCase):
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


class TestTagThumbnail(NeonDbTestCase):
    @tornado.testing.gen_test
    def test_get_has_save_delete(self):
        # Both order of keys.
        given1 = {'tag_id': '100', 'thumbnail_id': '3sdf3rwf', 'async': True}
        given2 = {'thumbnail_id': '3sdf3rwf', 'tag_id': '101', 'async': True}

        # Start with nothing.
        has_result = TagThumbnail.has(tag_id='100', thumbnail_id='3sdf3rwf')
        self.assertFalse(has_result)
        get_result = TagThumbnail.get(tag_id='100')
        self.assertFalse(get_result)

        # Add one row and check.
        save_result = yield TagThumbnail.save(**given1)
        self.assertTrue(save_result)

        get_result = TagThumbnail.get(tag_id=given1['tag_id'])
        self.assertEqual([given1['thumbnail_id']], get_result)
        get_result = TagThumbnail.get(thumbnail_id=given1['thumbnail_id'])
        self.assertEqual([given1['tag_id']], get_result)
        get_result = TagThumbnail.get(tag_id=given2['tag_id'])
        self.assertFalse(get_result)

        has_result = yield TagThumbnail.has(**given1)
        self.assertTrue(has_result)
        has_result = yield TagThumbnail.has(**given2)
        self.assertFalse(has_result)

        # Saving again, 0 row count change.
        save_result = yield TagThumbnail.save(**given1)
        self.assertFalse(save_result)
        get_result = TagThumbnail.get(tag_id=given1['tag_id'])
        self.assertEqual([given1['thumbnail_id']], get_result)
        get_result = TagThumbnail.get(tag_id=given2['tag_id'])
        self.assertFalse(get_result)

        # Saving new tag: one row.
        save_result = yield TagThumbnail.save(**given2)
        self.assertTrue(save_result)
        has_result = yield TagThumbnail.has(**given1)
        self.assertTrue(has_result)
        has_result = yield TagThumbnail.has(**given2)
        self.assertTrue(has_result)
        get_result = TagThumbnail.get(thumbnail_id='3sdf3rwf')
        self.assertEqual({'100', '101'}, set(get_result))

        # Delete and check.
        del_result = yield TagThumbnail.delete(**given1)
        self.assertEqual(1, del_result)
        del_result = yield TagThumbnail.delete(**given1)
        self.assertEqual(0, del_result)
        has_result = yield TagThumbnail.has(**given1)
        self.assertFalse(has_result)
        has_result = yield TagThumbnail.has(**given2)
        self.assertTrue(has_result)
        get_result = TagThumbnail.get(thumbnail_id='3sdf3rwf')
        self.assertEqual(['101'], get_result)

    @tornado.testing.gen_test
    def test_has_save_delete_many(self):
        given1 = {'tag_id': [100, 101], 'thumbnail_id': ['3sdf3rwf', '23reff'], 'async': True}
        given2 = {'tag_id': [102], 'thumbnail_id': ['4fj', '3fd'], 'async': True}
        pairs1 = {
            ('100', '3sdf3rwf'),
            ('100', '23reff'),
            ('101', '3sdf3rwf'),
            ('101', '23reff')
        }
        pairs2 = {
            ('102', '4fj'),
            ('102', '3fd')
        }

        # Start with nothing.
        has_result = yield TagThumbnail.has_many(**given1)
        (self.assertFalse(has_result[pair]) for pair in pairs1)

        # Add twos row and check.
        save_result = yield TagThumbnail.save_many(**given1)
        self.assertEqual(4, save_result)
        has_result = yield TagThumbnail.has_many(**given1)
        [self.assertTrue(has_result[pair]) for pair in pairs1]
        [self.assertFalse(has_result[pair]) for pair in pairs2]
        get_result = yield TagThumbnail.get_many(tag_id=['101', '109'], async=True)
        self.assertEqual(set(given1['thumbnail_id']), set(get_result['101']))
        get_result = yield TagThumbnail.get_many(thumbnail_id=['23reff'], async=True)
        self.assertEqual({'100', '101'}, set(get_result['23reff']))
        get_result = yield TagThumbnail.get_many(tag_id=['102'], async=True)
        self.assertEqual(set(), set(get_result['102']))

        # Saving again, 0 row count change.
        save_result = yield TagThumbnail.save_many(**given1)
        self.assertEqual(0, save_result)

        # Saving new tags: two rows.
        save_result = yield TagThumbnail.save_many(**given2)
        self.assertEqual(2, save_result)
        has_result = yield TagThumbnail.has_many(**given1)
        [self.assertTrue(has_result[pair]) for pair in pairs1]
        has_result = yield TagThumbnail.has_many(**given2)
        [self.assertTrue(has_result[pair]) for pair in pairs2]

        # Delete and check.
        delete_args = {'tag_id':100, 'thumbnail_id': ['23reff', '4fj'], 'async': True}
        pairs = {
            ('100', '3sdf3rwf'),
            ('101', '3sdf3rwf'),
            ('101', '23reff'),
            ('102', '4fj'),
            ('102', '3fd')
        }
        del_result = yield TagThumbnail.delete_many(**delete_args)
        self.assertEqual(1, del_result)
        del_result = yield TagThumbnail.delete_many(**delete_args)
        self.assertEqual(0, del_result)
        given = {
            'tag_id': [100, 101, 102],
            'thumbnail_id': ['3sdf3rwf', '23reff', '4fj', '3fd'],
            'async': True}
        has_result = yield TagThumbnail.has_many(**given)
        [self.assertTrue(has_result[pair]) for pair in pairs]
        self.assertFalse(has_result[(100, '23reff')])

    @tornado.testing.gen_test
    def test_empty_get(self):
        result = yield TagThumbnail.get(tag_id=None, async=True)
        self.assertEqual([], result)

    @tornado.testing.gen_test
    def test_empty_get_many(self):
        get_result = yield TagThumbnail.get_many(tag_id=[], async=True)
        self.assertEqual({}, get_result)

    @tornado.testing.gen_test
    def test_bad_call(self):

        bad_input1 = {'bad_id': '100', 'thumbnail_id': '3sdf3rwf', 'async': True}
        with self.assertRaises(KeyError):
            yield TagThumbnail.save(**bad_input1)
        bad_input2 = {
            'thumbnail_id': '3sdf3rwf',
            'tag_id': '100',
            'bad_id': '15',
            'async': True}
        with self.assertRaises(KeyError):
            yield TagThumbnail.save(**bad_input2)
        bad_input3 = {'thumbnail_id': None, 'tag_id': '100', 'async': True}
        with self.assertRaises(ValueError):
            yield TagThumbnail.save(**bad_input3)
        bad_input4 = {'thumbnail_id': '123', 'tag_id': 'asdf', 'async': True}
        with self.assertRaises(TypeError):
            yield TagThumbnail.get(**bad_input4)
        with self.assertRaises(ValueError):
            yield TagThumbnail.get(tag_id=100, async=True)


class BasePGNormalObject(object):

    keys = [('dynamic', 'key')]

    @classmethod
    def _create_key(cls):
        return uuid.uuid1().hex

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
        obj_id = self._create_key() 
        obj_type = self._get_object_type()
        obj = obj_type(obj_id)
        rv = yield obj.save(async=True)
        get_obj = yield self._get_get_function(obj)
        self.assertTrue(rv)
        self.assertEquals(get_obj.key, obj.key) 

    @tornado.testing.gen_test 
    def test_save_duplicate_objects(self):     
        unique_id = self._create_key() 
        so1 = self._get_object_type()(unique_id, 'test1') 
        so2 = self._get_object_type()(unique_id, 'test1') 
        yield so1.save(async=True) 
        yield so2.save(async=True)  
        get_obj_one = yield self._get_get_function(so1) 
        get_obj_two = yield self._get_get_function(so2) 
        self.assertEquals(get_obj_one.key,get_obj_two.key)
    
    @tornado.testing.gen_test 
    def test_save_all_objects(self):    
        key1 = self._create_key()
        key2 = self._create_key() 
        so1 = self._get_object_type()(key1, 'test1')
        so2 = self._get_object_type()(key2, 'test2')
        self._get_object_type().save_all([so1, so2])
        get_obj_one = yield self._get_get_function(so1)
        get_obj_two = yield self._get_get_function(so2)
        self.assertEquals(get_obj_one.key,so1.key)
        self.assertEquals(get_obj_two.key,so2.key)
     
    @tornado.testing.gen_test 
    def test_get_many_objects(self):     
        so1 = self._get_object_type()(self._create_key(), 'test')
        so2 = self._get_object_type()(self._create_key(), 'test2')
        yield self._get_object_type().save_all([so1, so2], async=True)
        results = yield self._run_get_many_function([so1,so2])
        self.assertEquals(len(results), 2)

    @tornado.testing.gen_test 
    def test_get_many_with_key_like_objects(self):    
        key1 = self._create_key() 
        key2 = self._create_key()
        so1 = self._get_object_type()(key1, 'testabcdef')
        so2 = self._get_object_type()(key2, 'testfedcba')
        yield self._get_object_type().save_all([so1, so2], async=True)
        results = yield so1.get_many_with_key_like(so1.key, async=True)
        self.assertEquals(len(results), 1)
        results = yield so1.get_many_with_key_like(so2.key, async=True)
        self.assertEquals(len(results), 1)

    @tornado.testing.gen_test 
    def test_delete_object(self):  
        so1 = self._get_object_type()(self._create_key()) 
        yield self._get_delete_function(so1) 
        get1 = yield self._get_get_function(so1)
        self.assertEquals(None, get1)

    @tornado.testing.gen_test 
    def test_delete_many_objects(self): 
        so1 = self._get_object_type()(self._create_key(), 'test1')
        so2 = self._get_object_type()(self._create_key(), 'test2')
        yield so1.save(async=True)
        yield so2.save(async=True)
        yield self._run_delete_many_function([so1, so2])
        get1 = yield self._get_get_function(so1)
        self.assertEquals(None, get1)
        get2 = yield self._get_get_function(so2)
        self.assertEquals(None, get2)

    @tornado.testing.gen_test
    def test_delete_many_objects_key_dne(self): 
        so1 = self._get_object_type()(self._create_key(), 'test1')
        so2 = self._get_object_type()(self._create_key(), 'test2')
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
        so = self._get_object_type()(self._create_key(), 'test1')
        yield so.save(async=True)
        yield self._run_modify_function(so, modify_me)
        self.assertEquals(modify_me.call_args[0][0].key, so.key)
        self.assertEquals(modify_me.call_count, 1)

    @tornado.testing.gen_test 
    def test_modify_many_objects(self):     
        modify_me = MagicMock()  
        so1 = self._get_object_type()(self._create_key(), 'test1')
        so2 = self._get_object_type()(self._create_key(), 'test2')
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

class TestThumbnailMetadata(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        BasePGNormalObject.keys = [('dynamic', 'key')] 
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        cls.postgresql.stop()
    
    @classmethod
    def _get_object_type(cls):
        return neondata.ThumbnailMetadata

    @tornado.testing.gen_test 
    def test_modify_many_objects_values(self):     
        def modify_me(t):
            for x in t.itervalues():
                if x is not None:
                    x.features = np.array([1.0,2.0,3.0,4.0])
        so1 = self._get_object_type()(uuid.uuid1().hex, 'test1')
        so2 = self._get_object_type()(uuid.uuid1().hex, 'test2')
        yield so1.save(async=True)
        yield so2.save(async=True)
        rv = yield ThumbnailMetadata.modify_many(
            [so1.key, so2.key], modify_me, async=True)
        for x in rv.itervalues(): 
            self.assertEquals(x.features[0], 1.0) 
            self.assertEquals(x.features[1], 2.0) 
            self.assertEquals(x.features[2], 3.0) 
            self.assertEquals(x.features[3], 4.0) 
        so1 = yield ThumbnailMetadata.get(so1.key, async=True)
        self.assertEquals(so1.features[0], 1.0) 

class TestVideoMetadata(NeonDbTestCase, BasePGNormalObject):

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

        video_ids = yield neondata.VideoMetadata.search_for_keys(async=True)
        self.assertEqual(1, len(video_ids))
        videos = yield neondata.VideoMetadata.search_for_objects(async=True)
        self.assertEqual(1, len(videos))

    @tornado.testing.gen_test
    def test_video_results_list(self):
        orig = neondata.VideoMetadata(
            'a1_vid1',
            job_results = [
                neondata.VideoJobThumbnailList(thumbnail_ids=['tid1', 'tid2']),
                neondata.VideoJobThumbnailList('50+', 'M',
                                               ['tid3', 'tid4'],
                                               ['bad1'],
                                               'model_vers')])
        yield orig.save(async=True)

        found = yield neondata.VideoMetadata.get('a1_vid1', async=True)
        self.assertItemsEqual(found.job_results[0].thumbnail_ids,
                              ['tid1', 'tid2'])
        self.assertIsNone(found.job_results[0].age)
        self.assertIsNone(found.job_results[0].gender)
        self.assertIsNone(found.job_results[0].model_version)
        self.assertEquals(found.job_results[0].bad_thumbnail_ids, [])
        self.assertItemsEqual(found.job_results[1].thumbnail_ids,
                              ['tid3', 'tid4'])
        self.assertEquals(found.job_results[1].age, '50+')
        self.assertEquals(found.job_results[1].gender, 'M')
        self.assertEquals(found.job_results[1].model_version, 'model_vers')
        self.assertItemsEqual(found.job_results[1].bad_thumbnail_ids, ['bad1'])

        self.assertEquals(orig, found)


class TestAccountLimits(NeonDbTestCase, BasePGNormalObject):
    @classmethod
    def _get_object_type(cls):
        return neondata.AccountLimits

class TestBillingPlans(NeonDbTestCase, BasePGNormalObject):
    @classmethod
    def _get_object_type(cls):
        return neondata.BillingPlans

class TestNeonRequest(NeonDbTestCase, BasePGNormalObject):
    def setUp(self):
        BasePGNormalObject.keys = [('dynamic', 'key'), ('static', 'a1')]

        # Create an account and request with a callback
        NeonUserAccount('acct1', 'key1').save()
        NeonApiRequest('j1', 'key1', 'vid1',
                       http_callback='http://some.where').save()

        self.http_mocker = patch('cmsdb.neondata.utils.http')
        self.http_mock = self._future_wrap_mock(
            self.http_mocker.start().send_request, require_async_kw=True)
        self.http_mock.side_effect = lambda x, **kw: HTTPResponse(x, 200)
        super(NeonDbTestCase, self).setUp()

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

    @tornado.testing.gen_test
    def test_callback_with_experiment_state(self):
      def _mod_request(x):
        x.state = neondata.RequestState.SERVING
        x.http_callback='http://some.where'
        x.response['framenos'] = [34, 61]
        x.response['serving_url'] = 'http://some_serving_url.com'
      request = NeonApiRequest.modify('j1', 'key1', _mod_request)
      neondata.VideoStatus('key1_vid1', neondata.ExperimentState.COMPLETE,
                           winner_tid='key1_vid1_t2').save()

      yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.WINNER_SENT)
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
      self.assertTrue(self.http_mock.called)
      cargs, kwargs = self.http_mock.call_args
      cb_request = cargs[0]
      response_dict = json.loads(cb_request.body)
      self.assertDictContainsSubset(expected_response, response_dict)

      # Do it again and it should not send a call
      yield found_request.send_callback(async=True)
      self.assertEquals(self.http_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_callback_on_serving(self):
      def _mod_request(x):
        x.state = neondata.RequestState.SERVING
        x.http_callback='http://some.where'
        x.response['framenos'] = [34, 61]
        x.response['serving_url'] = 'http://some_serving_url.com'
      request = NeonApiRequest.modify('j1', 'key1', _mod_request)

      yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.SERVING_SENT)
      expected_response = {
        'job_id' : 'j1',
         'video_id' : 'vid1',
         'error': None,
         'framenos' : [34, 61],
         'serving_url' : 'http://some_serving_url.com',
         'processing_state' : neondata.ExternalRequestState.SERVING,
         'experiment_state' : neondata.ExperimentState.UNKNOWN,
         'winner_thumbnail' : None}

      self.assertDictContainsSubset(expected_response, found_request.response)

      # Check the callback
      self.assertTrue(self.http_mock.called)
      cargs, kwargs = self.http_mock.call_args
      cb_request = cargs[0]
      response_dict = json.loads(cb_request.body)
      self.assertDictContainsSubset(expected_response, response_dict)

      # Do it again and it should not send a call
      yield found_request.send_callback(async=True)
      self.assertEquals(self.http_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_callback_on_finished(self):
      def _mod_request(x):
        x.state = neondata.RequestState.FINISHED
        x.http_callback='http://some.where'
        x.response['framenos'] = [34, 61]
        x.response['serving_url'] = 'http://some_serving_url.com'
      request = NeonApiRequest.modify('j1', 'key1', _mod_request)

      yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.PROCESSED_SENT)
      expected_response = {
        'job_id' : 'j1',
         'video_id' : 'vid1',
         'error': None,
         'framenos' : [34, 61],
         'serving_url' : 'http://some_serving_url.com',
         'processing_state' : neondata.ExternalRequestState.PROCESSED,
         'experiment_state' : neondata.ExperimentState.UNKNOWN,
         'winner_thumbnail' : None}

      self.assertDictContainsSubset(expected_response, found_request.response)

      # Check the callback
      self.assertTrue(self.http_mock.called)
      cargs, kwargs = self.http_mock.call_args
      cb_request = cargs[0]
      response_dict = json.loads(cb_request.body)
      self.assertDictContainsSubset(expected_response, response_dict)

      # Do it again and it should not send a call
      yield found_request.send_callback(async=True)
      self.assertEquals(self.http_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_callback_with_error_state(self):
      def _mod_request(x):
        x.state = neondata.RequestState.CUSTOMER_ERROR
        x.http_callback='http://some.where'
        x.response['framenos'] = []
        x.response['serving_url'] = None
        x.response['error'] = 'some customer error'
      request = NeonApiRequest.modify('j1', 'key1', _mod_request)

      yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.FAILED_SENT)
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
      self.assertTrue(self.http_mock.called)
      cargs, kwargs = self.http_mock.call_args
      cb_request = cargs[0]
      response_dict = json.loads(cb_request.body)
      self.assertDictContainsSubset(expected_response, response_dict)

      # Do it again and it should not send a call
      yield found_request.send_callback(async=True)
      self.assertEquals(self.http_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_callback_ignore_type(self):
      def _mod_request(x):
        x.state = neondata.RequestState.FINISHED
        x.http_callback='http://some.where'
        x.response['framenos'] = [34, 61]
        x.response['serving_url'] = 'http://some_serving_url.com'
      request = NeonApiRequest.modify('j1', 'key1', _mod_request)
      NeonUserAccount('acct1', 'key1',
                      callback_states_ignored=[
                        neondata.CallbackState.PROCESSED_SENT]).save()

      yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.PROCESSED_SENT)
      self.assertFalse(self.http_mock.called)

    @tornado.testing.gen_test
    def test_send_callback_failure(self):
      def _mod_request(x):
        x.state = neondata.RequestState.FINISHED
        x.http_callback='http://some.where'
        x.response['framenos'] = [34, 61]
        x.response['serving_url'] = 'http://some_serving_url.com'
      request = NeonApiRequest.modify('j1', 'key1', _mod_request)

      self.http_mock.side_effect = lambda x, **kw: HTTPResponse(x, code=500)

      with self.assertLogExists(logging.WARNING,
                                'Error when sending callback'):
        yield request.send_callback(async=True)

      found_request = NeonApiRequest.get('j1', 'key1')
      self.assertEquals(found_request.callback_state,
                        neondata.CallbackState.ERROR)

    @tornado.testing.gen_test
    def test_send_invalid_callback(self):
      request = NeonApiRequest('j1', 'key1', http_callback='null')
      request.save()

      with self.assertLogExists(logging.ERROR, 'Invalid callback url '):
        yield request.send_callback(async=True)

      # Make sure the state is correct now
      self.assertEquals(NeonApiRequest.get('j1', 'key1').callback_state,
                        neondata.CallbackState.ERROR)
      self.assertFalse(self.http_mock.called)

class TestUser(test_utils.neontest.AsyncTestCase, BasePGNormalObject):
    def setUp(self): 
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()


class TestUser(NeonDbTestCase, BasePGNormalObject):
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


class TestNeonUserAccount(NeonDbTestCase, BasePGNormalObject):
    @classmethod
    def setUpClass(cls):
        super(TestNeonUserAccount, cls).setUpClass()
        BasePGNormalObject.keys = [('dynamic', 'key')]

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

    @tornado.testing.gen_test
    def test_get_internal_video_ids(self):
        api_key = 'key'
        i_vid = InternalVideoID.generate(api_key, 'vid1')
        tid = i_vid + "_t1"
        yield ThumbnailMetadata(tid, i_vid).save(async=True)
        yield VideoMetadata(i_vid, [tid],'job1').save(async=True)
        so = neondata.NeonUserAccount('key', api_key='key')
        yield so.save(async=True)
        video_ids = yield so.get_internal_video_ids(async=True)
        self.assertEquals(len(video_ids), 1)
        self.assertEquals(video_ids[0], 'key_vid1')

    @tornado.testing.gen_test
    def test_get_internal_video_ids_multiple(self):
        api_key = 'key'
        i_vid = InternalVideoID.generate(api_key, 'vid1')
        i_vid_two = InternalVideoID.generate(api_key, 'vid2')
        tid = i_vid + "_t1"
        yield ThumbnailMetadata(tid, i_vid).save(async=True)
        yield VideoMetadata(i_vid, [tid],'job1').save(async=True)
        yield VideoMetadata(i_vid_two, [tid],'job2').save(async=True)
        so = neondata.NeonUserAccount('key', api_key='key')
        yield so.save(async=True)
        video_ids = yield so.get_internal_video_ids(async=True)
        self.assertEquals(len(video_ids), 2)
        self.assertEquals(video_ids[0], 'key_vid1')
        self.assertEquals(video_ids[1], 'key_vid2')

    @tornado.testing.gen_test
    def test_get_internal_video_ids_since_date(self):
        api_key = 'key'
        i_vid = InternalVideoID.generate(api_key, 'vid1')
        i_vid_two = InternalVideoID.generate(api_key, 'vid2')
        tid = i_vid + "_t1"
        yield ThumbnailMetadata(tid, i_vid).save(async=True)
        yield VideoMetadata(i_vid, [tid],'job1').save(async=True)
        video_one = yield VideoMetadata.get(i_vid, async=True)
        yield VideoMetadata(i_vid_two, [tid],'job2').save(async=True)
        video = yield VideoMetadata.get(i_vid_two, async=True)
        so = NeonUserAccount('key', api_key='key')
        yield so.save(async=True)
        video_ids = yield so.get_internal_video_ids(
            async=True,
            since=video_one.created)
        self.assertEquals(len(video_ids), 1)
        self.assertEquals(video_ids[0], 'key_vid2')


class TestBrightcovePlayer(NeonDbTestCase, BasePGNormalObject):
    @classmethod
    def _get_object_type(cls):
        return BrightcovePlayer


class TestTag(NeonDbTestCase, BasePGNormalObject):

    @classmethod
    def setUpClass(cls):
        super(TestTag, cls).setUpClass()
        cls.account_id = 'acct0'

    @classmethod
    def _get_object_type(cls):
        return Tag

    @tornado.testing.gen_test
    def test_search_no_args(self):
        tag = Tag()
        yield tag.save(async=True)
        keys = yield Tag.search_for_keys(async=True)
        self.assertEqual([tag.get_id()], keys)
        tags = yield Tag.search_for_objects(async=True)
        self.assertEqual([tag.get_id()], [t.get_id() for t in tags])

    @tornado.testing.gen_test
    def test_search_acct(self):
        tag = Tag()
        yield tag.save(async=True)
        keys = yield Tag.search_for_keys(account_id=self.account_id, async=True)
        self.assertFalse(keys)
        acct_tag = Tag(account_id=self.account_id)
        yield acct_tag.save(async=True)
        keys = yield Tag.search_for_keys(account_id=self.account_id, async=True)
        self.assertEqual([acct_tag.get_id()], keys)

    @tornado.testing.gen_test
    def test_search_acct_and_name(self):
        Tag(account_id=self.account_id).save()
        Tag(account_id='someone else').save()
        Tag(account_id=self.account_id, name='ABC').save()
        Tag(account_id='someone else', name='ABC').save()
        Tag(account_id=self.account_id, name='BCD').save()
        Tag(account_id='someone else', name='BCD').save()
        result = yield Tag.search_for_keys(query='A', async=True)
        self.assertEqual(2, len(result))
        result = yield Tag.search_for_objects(query='A', account_id=self.account_id, async=True)
        self.assertEqual(1, len(result))
        result = yield Tag.search_for_objects(query='BC', account_id=self.account_id, async=True)
        self.assertEqual(2, len(result))
        result = yield Tag.search_for_objects(name='BCD', account_id=self.account_id, async=True)
        self.assertEqual(1, len(result))

    @tornado.testing.gen_test
    def test_since(self):
        tags = [Tag() for _ in range(20)]
        [tag.save() for tag in tags]
        cut = random.randint(0, 19)
        cut_tag = yield Tag.get(tags[cut].key, async=True)
        since = dateutil.parser.parse(cut_tag.created).strftime('%s.%f')
        before, after = tags[:cut], tags[cut + 1:]
        result = yield Tag.search_for_objects(since=since, async=True)
        # Expect only tags in after in result.
        self.assertEqual(len(after), len(result))

    @tornado.testing.gen_test
    def test_until(self):
        tags = [Tag() for _ in range(20)]
        [tag.save() for tag in tags]
        cut = random.randint(1, 19)
        cut_tag = yield Tag.get(tags[cut].key, async=True)
        until = dateutil.parser.parse(cut_tag.created).strftime('%s.%f')
        before, after = tags[:cut], tags[cut + 1:]
        result = yield Tag.search_for_objects(until=until, async=True)
        # Expect only tags in unti in result.
        before_keys = [i.get_id() for i in before]
        result_keys = [i.get_id() for i in result]
        self.assertEqual(set(before_keys), set(result_keys))

    @tornado.testing.gen_test
    def test_limit_and_offset(self):
        [Tag().save() for _ in range(20)]
        offset = random.randint(0, 18)
        limit = random.randint(1, 19 - offset)
        result = yield Tag.search_for_objects(limit=limit, async=True)
        self.assertEqual(limit, len(result))
        result = yield Tag.search_for_objects(offset=offset, async=True)
        self.assertEqual(20 - offset, len(result))
        result = yield Tag.search_for_keys(limit=limit, offset=offset, async=True)
        self.assertEqual(limit, len(result))

    @tornado.testing.gen_test
    def test_tag_type(self):
        [Tag(tag_type=neondata.TagType.COLLECTION).save() for _ in range(3)]
        [Tag(tag_type=neondata.TagType.VIDEO).save() for _ in range(5)]

        # Search for all.
        result = yield Tag.search_for_keys(async=True)
        self.assertEqual(3 + 5, len(result))
        result = yield Tag.search_for_objects(async=True)
        self.assertEqual(3 + 5, len(result))

        # Search for GALLERY.
        result = yield Tag.search_for_keys(tag_type=neondata.TagType.COLLECTION, async=True)
        self.assertEqual(3, len(result))
        result = yield Tag.search_for_objects(tag_type=neondata.TagType.COLLECTION, async=True)
        self.assertEqual(3, len(result))

        # Search for VIDEO.
        result = yield Tag.search_for_keys(tag_type=neondata.TagType.VIDEO, async=True)
        self.assertEqual(5, len(result))
        result = yield Tag.search_for_objects(tag_type=neondata.TagType.VIDEO, async=True)
        self.assertEqual(5, len(result))

        # Search for something unknown.
        result = yield Tag.search_for_keys(tag_type='UNKNOWN', async=True)
        self.assertEqual(0, len(result))
        result = yield Tag.search_for_objects(tag_type='UNKNOWN', async=True)
        self.assertEqual(0, len(result))

    @tornado.testing.gen_test
    def test_bad_arg(self):
        with self.assertRaises(KeyError):
            yield Tag.search_for_keys(title='bad argument')


class TestFeature(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(test_utils.neontest.AsyncTestCase, self).setUp()

    def tearDown(self):
        self.postgresql.clear_all_tables()
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()

    @tornado.testing.gen_test 
    def test_save_and_get(self): 
        key = neondata.Feature.create_key('kfmodel', 1) 
        yield neondata.Feature(key).save(async=True)
        feature = yield neondata.Feature.get(key, async=True)
        self.assertEquals(feature.index, 1) 
        self.assertEquals(feature.name, None) 
        self.assertEquals(feature.model_name, 'kfmodel')
 
    @tornado.testing.gen_test 
    def test_modify(self): 
        def _modify(f): 
            f.name = 'newname'         
        key = neondata.Feature.create_key('kfmodel', 1) 
        yield neondata.Feature(key).save(async=True)
        feature = yield neondata.Feature.modify(key, _modify, async=True)
        self.assertEquals(feature.index, 1) 
        self.assertEquals(feature.name, 'newname') 
        self.assertEquals(feature.model_name, 'kfmodel')

    @tornado.testing.gen_test 
    def test_delete(self): 
        key = neondata.Feature.create_key('kfmodel', 1) 
        yield neondata.Feature(key, name='oldname').save(async=True)
        yield neondata.Feature.delete(key, async=True)
        feature = yield neondata.Feature.get(key, async=True)
        self.assertEquals(feature.name, None)
 
    @tornado.testing.gen_test 
    def test_get_by_model_name(self): 
        key = neondata.Feature.create_key('kfmodel', 1) 
        yield neondata.Feature(key).save(async=True)
        key = neondata.Feature.create_key('kfmodel', 2) 
        yield neondata.Feature(key).save(async=True)

        fs = yield neondata.Feature.get_by_model_name('kfmodel', async=True)
        f1 = fs[0]
        f2 = fs[1] 
        self.assertEquals(f1.index, 1)  
        self.assertEquals(f2.index, 2)  


class TestStoredObject(test_utils.neontest.AsyncTestCase):


    def test_to_json_utf8(self):

        class SomeClass(neondata.StoredObject):
            def __init__(self, key, utf_property):
                self.utf_property = utf_property
                super(SomeClass, self).__init__(key)

        key = 'key'
        given = u'Lucía'
        obj = SomeClass(key, given)
        result = json.loads(obj.to_json())
        self.assertEqual(given, result['_data']['utf_property'])


if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
