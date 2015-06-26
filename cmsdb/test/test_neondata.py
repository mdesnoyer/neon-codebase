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
import logging
import json
import multiprocessing
from mock import patch, MagicMock
import os
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
from tornado.httpclient import HTTPResponse, HTTPRequest
import tornado.ioloop
import utils.neon
from utils.options import options
from utils.imageutils import PILImageUtils
import unittest
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
        NeonCDNHostingMetadata, CDNHostingMetadataList, ThumbnailType

_log = logging.getLogger(__name__)

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
        #print len(nu_accounts), len(a_ids)
        self.assertItemsEqual(a_ids, nu_a_ids)

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
        NeonPlatform('acct1', api_key='acct1').save()
        BrightcovePlatform('acct2', 'i_bc', 'acct2').save()
        OoyalaPlatform('acct3', 'i_oo', 'acct3').save()
        YoutubePlatform('acct4', 'i_yt', 'acct4').save()

        objs = AbstractPlatform.get_all()

        self.assertEquals(len(objs), 4)

    @tornado.testing.gen_test
    def test_get_all_platforms_async(self):
        NeonPlatform('acct1', api_key='acct1').save()
        BrightcovePlatform('acct2', 'i_bc', 'acct2').save()
        OoyalaPlatform('acct3', 'i_oo', 'acct3').save()
        YoutubePlatform('acct4', 'i_yt', 'acct4').save()

        objs = yield tornado.gen.Task(AbstractPlatform.get_all)

        self.assertEquals(len(objs), 4)

    def test_default_bcplatform_settings(self):
        ''' brightcove defaults ''' 

        na = NeonUserAccount('acct1')
        na.save()
        bp = BrightcovePlatform('aid', 'iid', na.neon_api_key)

        self.assertFalse(bp.abtest)
        self.assertFalse(bp.auto_update)
        self.assertTrue(bp.serving_enabled)
        self.assertNotEqual(bp.neon_api_key, '')
        self.assertEqual(bp.key, 'brightcoveplatform_%s_iid' % bp.neon_api_key)

        # Make sure that save and regenerating creates the same object
        bp.save()

        bp2 = BrightcovePlatform.get(bp.neon_api_key, 'iid')
        self.assertEqual(bp.__dict__, bp2.__dict__)

    def test_neon_user_account(self):
        ''' nuser account '''

        na = NeonUserAccount('acct1')
        bp = BrightcovePlatform('acct1', 'bp1', na.neon_api_key)
        bp.save()
        np = NeonPlatform('acct1', '0', na.neon_api_key)
        np.save()
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

    @unittest.skip('TODO(Sunil): add this test')
    def test_add_platform_with_bad_account_id(self):
        pass
    
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
        ap = AbstractPlatform('')
        db = DBConnection.get(ap)
        key = "fookey"
        val = "fooval"
        self.assertTrue(db.blocking_conn.set(key, val))
        self.assertEqual(db.blocking_conn.get(key), val)
        self.assertTrue(db.blocking_conn.delete(key))

    def test_concurrent_requests(self):
        ''' Make concurrent requests to the db 
            verify that singleton instance doesnt cause race condition
        '''
        def db_operation(key):
            ''' db op '''
            resultQ.put(db.blocking_conn.get(key))

        ap = AbstractPlatform('')
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

    def test_delete_all_video_related_data(self):
        # create all video related objects
        
        na = NeonUserAccount('ta1')
        bp = BrightcovePlatform('ta1', 'bp1', na.neon_api_key)
        bp.save()
        np = NeonPlatform('ta1', '0', na.neon_api_key)
        np.save()
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
        np.add_video('dummyv', 'dummyjob')
        np.add_video('vid1', 'job1')
        np.save()
        NeonPlatform.delete_all_video_related_data(np, 'vid1',
                really_delete_keys=True)
        
        # check the keys have been deleted
        self.assertIsNone(NeonApiRequest.get('job1', na.neon_api_key))
        self.assertIsNone(ThumbnailMetadata.get(thumb.key))
        self.assertIsNone(VideoMetadata.get(i_vid))

        # verify account
        np = NeonPlatform.get(na.neon_api_key, '0')
        self.assertListEqual(np.get_videos(), [u'dummyv'])
        
        #TODO: Add more failure test cases


    def test_iterate_all_thumbnails(self):
        #NOTE: doesn't work on MAC

        # Build up a database structure
        na = NeonUserAccount('acct1')
        bp = BrightcovePlatform('acct1', 'bp1', na.neon_api_key)
        op = OoyalaPlatform('acct1', 'op1', na.neon_api_key, 'p_code', 'o_key',
                            'o_secret')
        na.add_platform(bp)
        na.add_platform(op)
        na.save()

        vids = [
            InternalVideoID.generate(na.neon_api_key, 'v0'),
            InternalVideoID.generate(na.neon_api_key, 'v1'),
            InternalVideoID.generate(na.neon_api_key, 'v2')
            ]
            
        thumbs = [
            ThumbnailMetadata('t1', vids[0], ['t1.jpg'], None, None, None,
                              None, None, None),
            ThumbnailMetadata('t2', vids[0], ['t2.jpg'], None, None, None,
                              None, None, None),
            ThumbnailMetadata('t3', vids[1], ['t3.jpg'], None, None, None,
                              None, None, None),
            ThumbnailMetadata('t4', vids[2], ['t4.jpg'], None, None, None,
                              None, None, None),
            ThumbnailMetadata('t5', vids[2], ['t5.jpg'], None, None, None,
                              None, None, None)]
        ThumbnailMetadata.save_all(thumbs)

        v0 = VideoMetadata(vids[0], [thumbs[0].key, thumbs[1].key],
                           'r0', 'v0.mp4', 0, 0, None, bp.integration_id)
        v0.save()
        v1 = VideoMetadata(vids[1], [thumbs[2].key],
                           'r1', 'v1.mp4', 0, 0, None, bp.integration_id)
        v1.save()
        v2 = VideoMetadata(vids[2], [thumbs[3].key, thumbs[4].key],
                           'r2', 'v2.mp4', 0, 0, None, op.integration_id)
        v2.save()


        bp.add_video('v0', 'r0')
        bp.add_video('v1', 'r1')
        bp.save()
        op.add_video('v2', 'r2')
        op.save()

        # Now make sure that the iteration goes through all the thumbnails
        found_thumb_ids = [x.key for x in  
                           ThumbnailMetadata.iterate_all_thumbnails()]
        self.assertItemsEqual(found_thumb_ids, [x.key for x in thumbs])

    def test_ThumbnailServingURLs(self):
        input1 = ThumbnailServingURLs('acct1_vid1_tid1')
        input1.add_serving_url('http://that_800_600.jpg', 800, 600)
        input1.add_serving_url('http://this_100_50.jpg', 100, 50)
        
        input1.save()
        output1 = ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertEqual(output1.get_thumbnail_id(), input1.get_thumbnail_id())
        self.assertEqual(output1.get_serving_url(800, 600),
                         'http://that_800_600.jpg')
        with self.assertRaises(KeyError):
            _log.info('Output1 %s' % output1)
            output1.get_serving_url(640, 480)

        input1.add_serving_url('http://that_640_480.jpg', 640, 480) 
        input2 = ThumbnailServingURLs('tid2', {(640, 480) : 'http://this.jpg'})
        ThumbnailServingURLs.save_all([input1, input2])
        output1, output2 = ThumbnailServingURLs.get_many(['acct1_vid1_tid1',
                                                          'tid2'])
        self.assertEqual(output1.get_thumbnail_id(),
                         input1.get_thumbnail_id())
        self.assertEqual(output2.get_thumbnail_id(),
                         input2.get_thumbnail_id())
        self.assertEqual(output1.get_serving_url(640, 480),
                         'http://that_640_480.jpg')
        self.assertEqual(output1.get_serving_url(800, 600),
                         'http://that_800_600.jpg')
        self.assertEqual(output2.get_serving_url(640, 480),
                         'http://this.jpg')

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
        self.assertIsNone(obj.base_url)
        self.assertEquals(len(obj.sizes), 0)
        self.assertEquals(obj.get_serving_url(160, 90),
                          'http://n3.neon-images.com/EaE/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w160_h90.jpg')

    def test_backwards_compatible_thumb_serving_urls_same_base(self):
        json_str = "{\"_type\": \"ThumbnailServingURLs\", \"_data\": {\"size_map\": [[[210, 118], \"http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w210_h118.jpg\"], [[160, 90], \"http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w160_h90.jpg\"], [[1280, 720], \"http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w1280_h720.jpg\"]], \"key\": \"thumbnailservingurls_b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8\"}}"

        obj = ThumbnailServingURLs._create('thumbnailservingurls_b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8', json.loads(json_str))

        # It's the same base url so store in the new format
        self.assertEquals(len(obj.size_map), 0)
        self.assertEquals(obj.base_url, 'http://n3.neon-images.com/fF7')
        self.assertEquals(obj.sizes, set([(210,118), (160,90), (1280,720)]))
        self.assertEquals(obj.get_serving_url(160, 90),
                          'http://n3.neon-images.com/fF7/neontnb6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8_w160_h90.jpg')
        self.assertEquals(obj.get_thumbnail_id(),
                          'b6rpyj7bkp2wfn0s4mdt5xc8_caf4beb275ce81ec61347ae57d91dcc8_a7eaead18140903cd4c21d43113f38b8')
        

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
            (NeonPlatform('key', '0', 'api'),
             lambda x: x.get('api', '0')),
            (BrightcovePlatform('a', 'i', 'api'),
             lambda x: x.get('api', 'i')),
            (OoyalaPlatform('a', 'i', 'api', 'b', 'c', 'd', 'e'),
             lambda x: x.get('api', 'i'))]

        for obj, read_func in obj_types:
            # Start by saving the object
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
    
    def test_processed_internal_video_ids(self):
        na = NeonUserAccount('accttest')
        na.save()
        bp = BrightcovePlatform('aid', 'iid', na.neon_api_key)
        
        vids = bp.get_processed_internal_video_ids()
        self.assertEqual(len(vids), 0)

        for i in range(10):
            bp.add_video('vid%s' % i, 'job%s' % i)
            r = NeonApiRequest('job%s' % i, na.neon_api_key, 'vid%s' % i, 't', 't', 'r', 'h')
            r.state = "active"
            r.save()
        bp.save()

        vids = bp.get_processed_internal_video_ids()
        self.assertEqual(len(vids), 10)

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
        self.assertEqual(neon_cdn.folder_prefix, '')
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
            self.assertItemsEqual(cdn_list, [])

    def test_hosting_metadata_list_bad_key(self):
        with self.assertRaises(ValueError):
            CDNHostingMetadataList('integration0')

        with self.assertRaises(ValueError):
            CDNHostingMetadataList('acct_1_integration0')

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
        np = NeonPlatform('acct1', '0', na.neon_api_key)
        np.save()
       
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

        
        # Add an old url and there is no video data in the database yet
        api_request.previous_thumbnail = 'old_thumbnail'
        with self.assertLogExists(logging.ERROR, 
                                 'VideoMetadata for job .* is missing'):
            with self.assertRaises(neondata.DBStateError):
                api_request.save_default_thumbnail()

        # Add the video data to the database
        video = VideoMetadata('acct1_vid1')
        add_thumb_mock = MagicMock()
        video.download_and_add_thumbnail = add_thumb_mock
        add_future = Future()
        add_future.set_result(MagicMock())
        add_thumb_mock.return_value = add_future
        get_video_mock.side_effect = lambda x, callback: callback(video)
        
        api_request.save_default_thumbnail()
        self.assertEquals(add_thumb_mock.call_count, 1)
        thumbmeta, url_seen, cdn = add_thumb_mock.call_args[0]
        self.assertEquals(url_seen, 'old_thumbnail')
        self.assertEquals(thumbmeta.rank, 0)
        self.assertEquals(thumbmeta.type, ThumbnailType.DEFAULT)
        add_thumb_mock.reset_mock()

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

        
    def test_neon_api_request_backwards_compatibility(self):
        json_str="{\"api_method\": \"topn\", \"video_url\": \"http://brightcove.vo.llnwd.net/pd16/media/136368194/201407/1283/136368194_3671520771001_linkasia2014071109-lg.mp4\", \"model_version\": \"20130924\", \"job_id\": \"e38ef7abba4c9102b26feb90bc5df3a8\", \"state\": \"serving\", \"api_param\": 1, \"api_key\": \"dhfaagb0z0h6n685ntysas00\", \"publisher_id\": \"136368194\", \"integration_type\": \"neon\", \"autosync\": false, \"request_type\": \"brightcove\", \"key\": \"request_dhfaagb0z0h6n685ntysas00_e38ef7abba4c9102b26feb90bc5df3a8\", \"submit_time\": \"1405130164.16\", \"response\": {\"job_id\": \"e38ef7abba4c9102b26feb90bc5df3a8\", \"timestamp\": \"1405130266.52\", \"video_id\": \"3671481626001\", \"error\": null, \"data\": [7139.97], \"thumbnails\": [\"https://host-thumbnails.s3.amazonaws.com/dhfaagb0z0h6n685ntysas00/e38ef7abba4c9102b26feb90bc5df3a8/neon0.jpeg\"]}, \"integration_id\": \"35\", \"read_token\": \"rgkAluxK9pAC26XCRusctnSfWwzrujq9cTRdmrNpWU4.\", \"video_id\": \"3671481626001\", \"previous_thumbnail\": \"https://host-thumbnails.s3.amazonaws.com/dhfaagb0z0h6n685ntysas00/e38ef7abba4c9102b26feb90bc5df3a8/brightcove.jpeg\", \"publish_date\": 1405128996278, \"callback_url\": \"http://localhost:8081/testcallback\", \"write_token\": \"v4OZjhHCkoFOqlNFJZLBA-KcbnNUhtQjseDXO9Y4dyA.\", \"video_title\": \"How Was China's Xi Jinping Welcomed in South Korea?\"}"

        obj = NeonApiRequest._create('request_dhfaagb0z0h6n685ntysas00_e38ef7abba4c9102b26feb90bc5df3a8', json.loads(json_str))

        self.assertIsInstance(obj, neondata.BrightcoveApiRequest)
        self.assertEquals(obj.read_token,
                          'rgkAluxK9pAC26XCRusctnSfWwzrujq9cTRdmrNpWU4.')
        self.assertEquals(
            obj.get_id(),
            ('e38ef7abba4c9102b26feb90bc5df3a8', 'dhfaagb0z0h6n685ntysas00'))

    def test_neonplatform_account_backwards_compatibility(self):
        json_str = "{\"integration_id\": \"0\", \"account_id\": \"161\", \"videos\": {}, \"abtest\": false, \"neon_api_key\": \"hzxts57y7ywcl9onl811b0p4\", \"key\": \"neonplatform_hzxts57y7ywcl9onl811b0p4_0\"}"
        obj = NeonPlatform._create('neonplatform_hzxts57y7ywcl9onl811b0p4_0', json.loads(json_str))
        self.assertEqual(obj.account_id, '161')
        self.assertEqual(obj.neon_api_key, 'hzxts57y7ywcl9onl811b0p4')
        
        # save the object and ensure that its "savable", then verify contents again
        obj.abtest = True
        obj.save() 
        
        obj = NeonPlatform.get(obj.neon_api_key, '0')
        self.assertEqual(obj.account_id, '161')
        self.assertEqual(obj.neon_api_key, 'hzxts57y7ywcl9onl811b0p4')
        self.assertTrue(obj.abtest)

    def test_brightcoveplatform_account_backwards_compatibility(self):
        json_str = "{\"integration_id\": \"39\", \"account_id\": \"145\", \"videos\": {\"3579581800001\": \"ebc8d852519d582d4819fc94c29d55df\", \"2007541443001\": \"791ab73928e9f4ea83d8de164f9c93f8\", \"3696935732001\": \"812ae49b93338b15665305b09b3964f1\", \"2007730256001\": \"37462869b0a3d1a735dfedd85556635d\", \"3582347795001\": \"38fa298d176a1466de4b06abc404d81a\", \"2007730265001\": \"7f6889e3703ab754dc37cf7348c7d29d\", \"2007541445001\": \"d75de4027945a48af3209c2d46691881\", \"3582347813001\": \"c5f004b162ac72b1d1f38e1a5e27b5fd\", \"3621471894001\": \"c86e6e9637681a61531b792341a7578b\", \"3621469853001\": \"095039d7ef3bbb1bad2ab8015a15d565\", \"3582347871001\": \"3c9ea0d2ea3b4119faa8788afe6f6c28\", \"3919711209001\": \"c57484a1c5082e59d1421dbf655dd13c\", \"2007610405001\": \"e50eff970fea8186d81131c9549f54c4\", \"2007610412001\": \"92b8e2bfb3fdf2e6be47fd4f35039426\", \"2007610408001\": \"bf979a72676ead55dd2332ef34467f9a\", \"3621469871001\": \"18fbd51ca4f4d45258bcee7924dc30d0\", \"2007730258001\": \"28878f0ad25408e2c5f2041d2efa81a8\", \"3903548524001\": \"bbc0c1b62a3c1828204ccdf9b926e857\"}, \"read_token\": \"tEBLhTQ18FsIacTpRnO7fjCyExLaVpcLWxhEsFkhIG6xxJfcJVicKQ..\", \"write_token\": \"1oupJkYVgMK1nOFZAIVo7uM3BB093eaBCwKFBB4qb3QNeAKbneikEw..\", \"abtest\": false, \"enabled\": true, \"video_still_width\": 480, \"last_process_date\": 1417544805, \"neon_api_key\": \"wsoruplnzkbilzj3apr05kvz\", \"rendition_frame_width\": null, \"linked_youtube_account\": false, \"key\": \"brightcoveplatform_wsoruplnzkbilzj3apr05kvz_39\", \"serving_enabled\": true, \"auto_update\": false, \"publisher_id\": \"1948681880001\", \"serving_controller\": \"imageplatform\", \"account_created\": 1398870554.476695}" 
        
        obj = BrightcovePlatform._create('brightcoveplatform_wsoruplnzkbilzj3apr05kvz_39', json.loads(json_str)) 
        self.assertEqual(obj.account_id, '145')
        self.assertEqual(obj.neon_api_key, 'wsoruplnzkbilzj3apr05kvz')
        obj.abtest = True
        obj.save() 
        
        obj = BrightcovePlatform.get('wsoruplnzkbilzj3apr05kvz', '39')
        self.assertEqual(obj.account_id, '145')
        self.assertEqual(obj.neon_api_key, 'wsoruplnzkbilzj3apr05kvz')
        self.assertTrue(obj.abtest)

    def test_ooyalaplatform_account_backwards_compatibility(self):
        json_str = "{\"integration_id\": \"1\", \"account_id\": \"136\", \"videos\": {}, \"auto_update\": false, \"api_secret\": \"uwTrMevYq54eani8ViRn6Ar5-rwmmmvKwq1HDtCn\", \"ooyala_api_key\": \"s0Y3YxOp0XTCL2hFlfFS1S2MRmaY.nxNs0\", \"abtest\": false, \"partner_code\": \"s0Y3YxOp0XTCL2hFlfFS1S2MRmaY\", \"neon_api_key\": \"qo4vtvhu2cqgdi30k63bahzh\", \"key\": \"ooyalaplatform_qo4vtvhu2cqgdi30k63bahzh_1\"}"

        obj = OoyalaPlatform._create('ooyalaplatform_qo4vtvhu2cqgdi30k63bahzh_1', json.loads(json_str)) 
        self.assertEqual(obj.account_id, '136')
        self.assertEqual(obj.neon_api_key, 'qo4vtvhu2cqgdi30k63bahzh')
        self.assertEqual(obj.partner_code, 's0Y3YxOp0XTCL2hFlfFS1S2MRmaY')
        obj.abtest = True
        obj.save() 
        
        obj = OoyalaPlatform.get('qo4vtvhu2cqgdi30k63bahzh', '1')
        self.assertEqual(obj.account_id, '136')
        self.assertEqual(obj.neon_api_key, 'qo4vtvhu2cqgdi30k63bahzh')
        self.assertEqual(obj.partner_code, 's0Y3YxOp0XTCL2hFlfFS1S2MRmaY')
        self.assertTrue(obj.abtest)

    def test_neonplatform_get_many(self):
        '''
        Test get many function for neonplatform
        '''
        nps = []
        for i in range(10):
            np = NeonPlatform('acct_%s' % i, '0', "APIKEY%s" % i)
            np.save()
            nps.append(np)

        keys = ['neonplatform_APIKEY%s_0' % i for i in range(10)]
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
            self.assertEquals(ExperimentStrategy('not_in_db'),
                              ExperimentStrategy.get('not_in_db'))

        with self.assertLogNotExists(logging.WARN, 'No ExperimentStrategy'):
            self.assertEquals(ExperimentStrategy('not_in_db'),
                              ExperimentStrategy.get('not_in_db',
                                                     log_missing=False))

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

        neondata.BrightcovePlatform('a1', 'bc', 'acct1').save()
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertIsInstance(events[0][1], BrightcovePlatform)

        neondata.NeonPlatform('a1', 'neon', 'acct1').save()
        events = trap.wait()
        self.assertEquals(len(events), 2)
        self.assertIsInstance(events[-1][1], NeonPlatform)

        neondata.YoutubePlatform('a1', 'yt', 'acct1').save()
        events = trap.wait()
        self.assertEquals(len(events), 3)
        self.assertIsInstance(events[-1][1], YoutubePlatform)

        neondata.OoyalaPlatform('a1', 'oo', 'acct1').save()
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
        neondata.BrightcovePlatform('a1', 'bc', 'acct1').save()
        plat_trap.wait()
        neondata.NeonPlatform('a1', 'neon', 'acct1').save()
        plat_trap.wait()
        neondata.YoutubePlatform('a1', 'yt', 'acct1').save()
        plat_trap.wait()
        neondata.OoyalaPlatform('a1', 'oo', 'acct1').save()
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

        neondata.BrightcovePlatform('a1', 'bc', 'acct2').save()
        neondata.NeonPlatform('a1', 'neon', 'acct2').save()
        neondata.YoutubePlatform('a1', 'yt', 'acct2').save()
        neondata.OoyalaPlatform('a1', 'oo', 'acct2').save()
        neondata.BrightcoveApiRequest('jobbc', 'acct2').save()
        NeonApiRequest('jobneon', 'acct2').save()
        VideoMetadata('acct1_vid2', request_id='req1').save()

        # Now resubscribe and see the changes
        plat_trap.subscribe(AbstractPlatform)
        request_trap.subscribe(NeonApiRequest)
        video_trap.subscribe(VideoMetadata, 'acct1_*')

        neondata.BrightcovePlatform('a1', 'bc', 'acct3').save()
        plat_trap.wait()
        neondata.NeonPlatform('a1', 'neon', 'acct3').save()
        plat_trap.wait()
        neondata.YoutubePlatform('a1', 'yt', 'acct3').save()
        plat_trap.wait()
        neondata.OoyalaPlatform('a1', 'oo', 'acct3').save()
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

    def test_subscribe_changes_after_db_connection_loss(self):
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

        # Now force a connection loss by stopping the server
        self.redis.stop(clear_singleton=False)
        self.assertWaitForEquals(
            lambda: neondata.PubSubConnection.get(VideoMetadata).connected,
            False)

        # Start a new server
        self.redis = test_utils.redis.RedisServer()
        self.redis.start(clear_singleton=False)
        self.assertWaitForEquals(
            lambda: neondata.PubSubConnection.get(VideoMetadata).connected,
            True)

        # Now change the video and make sure we get the event for
        # account 1, but not 2
        video_meta.serving_enabled=False
        video_meta2.save()
        video_meta.serving_enabled=False
        video_meta.save()
        events = trap.wait()
        self.assertEquals(len(events), 1)
        self.assertEquals(events[0], ('acct1_vid1', video_meta, 'set'))

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
        self.connection_patcher = patch('cmsdb.neondata.blockingRedis.StrictRedis')

        # For the sake of this test, we will only mock the get() function
        self.mock_redis = self.connection_patcher.start()
        self.mock_responses = MagicMock()
        self.mock_redis().get.side_effect = self._mocked_get_func

        self.valid_obj = TrackerAccountIDMapper("tai1", "api_key")

        # Speed up the retry delays to make the test faster
        self.old_delay = options.get('cmsdb.neondata.baseRedisRetryWait')
        options._set('cmsdb.neondata.baseRedisRetryWait', 0.01)

    def tearDown(self):
        self.connection_patcher.stop()
        DBConnection.clear_singleton_instance()
        options._set('cmsdb.neondata.baseRedisRetryWait',
                     self.old_delay)
        super(TestDbConnectionHandling, self).tearDown()

    def _mocked_get_func(self, key, callback=None):
        if callback:
            self.io_loop.add_callback(callback, self.mock_responses(key))
        else:
            return self.mock_responses(key)

    def test_subscribe_connection_error(self):
        self.mock_redis().pubsub().psubscribe.side_effect = [
            redis.exceptions.ConnectionError(),
            socket.gaierror()]
        
        with self.assertLogExists(logging.ERROR, 'Error subscribing'):
            with self.assertRaises(neondata.DBConnectionError):
                NeonUserAccount.subscribe_to_changes(lambda x,y,z: x)

        with self.assertLogExists(logging.ERROR, 'Socket error subscribing'):
            with self.assertRaises(neondata.DBConnectionError):
                NeonUserAccount.subscribe_to_changes(lambda x,y,z: x)

    def test_async_good_connection(self):
        self.mock_responses.side_effect = [self.valid_obj.to_json()]
        
        TrackerAccountIDMapper.get("tai1", callback=self.stop)
        found_obj = self.wait()

        self.assertEqual(self.valid_obj.__dict__, found_obj.__dict__)

    def test_sync_good_connection(self):
        self.mock_responses.side_effect = [self.valid_obj.to_json()]
        
        found_obj = TrackerAccountIDMapper.get("tai1")

        self.assertEqual(self.valid_obj.__dict__, found_obj.__dict__)

    def test_async_some_errors(self):
        self.mock_responses.side_effect = [
            redis.ConnectionError("Connection Error"),
            redis.BusyLoadingError("Loading Error"),
            socket.timeout("Socket Timeout"),
            socket.error("Socket Error"),
            self.valid_obj.to_json()
            ]

        TrackerAccountIDMapper.get("tai1", callback=self.stop)

        with self.assertLogExists(logging.ERROR, 'Connection Error'):
            with self.assertLogExists(logging.WARN, 'Redis is busy'):
                with self.assertLogExists(logging.ERROR, 'Socket Timeout'):
                    with self.assertLogExists(logging.ERROR, 'Socket Error'):
                        found_obj = self.wait()

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
                        found_obj =  TrackerAccountIDMapper.get("tai1")

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
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.image = PILImageUtils.create_random_image(360, 480)

    def tearDown(self):
        self.redis.stop()
        super(TestThumbnailHelperClass, self).tearDown()

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

        counters = [1, 2]
        def wrapped_callback(param):
            counters.pop()
            self.stop()

        ThumbnailMetadata.modify(tid, setphash, callback=wrapped_callback)
        ThumbnailMetadata.modify(tid, setrank, callback=wrapped_callback)
        
        # if len(counters) is not 0, call self.wait() to service the other
        # callbacks  
        while len(counters) > 0:
            self.wait()

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
        

    def test_read_thumbnail_old_format(self):
        # Make sure that we're backwards compatible
        thumb = ThumbnailMetadata._create('2630b61d2db8c85e9491efa7a1dd48d0_2876590502001_9e1d6017cab9aa970fca5321de268e15', json.loads("{\"video_id\": \"2630b61d2db8c85e9491efa7a1dd48d0_2876590502001\", \"thumbnail_metadata\": {\"chosen\": false, \"thumbnail_id\": \"2630b61d2db8c85e9491efa7a1dd48d0_2876590502001_9e1d6017cab9aa970fca5321de268e15\", \"model_score\": 4.1698684091322846, \"enabled\": true, \"rank\": 5, \"height\": 232, \"width\": 416, \"model_version\": \"20130924\", \"urls\": [\"https://host-thumbnails.s3.amazonaws.com/2630b61d2db8c85e9491efa7a1dd48d0/208720d8a4aef7ee5f565507833e2ccb/neon4.jpeg\"], \"created_time\": \"2013-12-02 09:55:19\", \"type\": \"neon\", \"refid\": null}, \"key\": \"2630b61d2db8c85e9491efa7a1dd48d0_2876590502001_9e1d6017cab9aa970fca5321de268e15\"}"))

        self.assertEqual(thumb.key, '2630b61d2db8c85e9491efa7a1dd48d0_2876590502001_9e1d6017cab9aa970fca5321de268e15')
        self.assertEqual(thumb.video_id, '2630b61d2db8c85e9491efa7a1dd48d0_2876590502001')
        self.assertEqual(thumb.chosen, False)
        self.assertAlmostEqual(thumb.model_score, 4.1698684091322846)
        self.assertEqual(thumb.enabled, True)
        self.assertEqual(thumb.rank, 5)
        self.assertEqual(thumb.height, 232)
        self.assertEqual(thumb.width, 416)
        self.assertEqual(thumb.model_version, '20130924')
        self.assertEqual(thumb.type, 'neon')
        self.assertEqual(thumb.created_time, '2013-12-02 09:55:19')
        self.assertIsNone(thumb.refid)
        self.assertEqual(thumb.urls, ["https://host-thumbnails.s3.amazonaws.com/2630b61d2db8c85e9491efa7a1dd48d0/208720d8a4aef7ee5f565507833e2ccb/neon4.jpeg"])
        
        with self.assertRaises(AttributeError):
            thumb.thumbnail_id

class TestAddingImageData(test_utils.neontest.AsyncTestCase):
    '''
    Test cases that add image data to thumbnails (and do uploads) 
    '''
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

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

        random.seed(1654984)

        self.image = PILImageUtils.create_random_image(360, 480)
        super(TestAddingImageData, self).setUp()

    def tearDown(self):
        self.s3_patcher.stop()
        self.cloudinary_patcher.stop()
        self.redis.stop()
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
        cdn_list = CDNHostingMetadataList(
            CDNHostingMetadataList.create_key('acct1', 'i6'), 
            [ NeonCDNHostingMetadata(do_salt=False),
              S3CDNHostingMetadata(bucket_name='customer-bucket',
                                   do_salt=False) ])
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

        with patch('cmsdb.neondata.utils.imageutils.PILImageUtils') \
          as pil_mock:
            image_future = Future()
            image_future.set_result(self.image)
            pil_mock.download_image.return_value = image_future

            yield video_info.download_and_add_thumbnail(
                thumb_info, "http://my_image.jpg", [], async=True,
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
        self.s3conn.create_bucket('host-thumbnails')
        self.s3conn.create_bucket('n3.neon-images.com')
        account = NeonUserAccount('a1')

        yield account.add_default_thumbnail(self.image, async=True)

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
    

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
