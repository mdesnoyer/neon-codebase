#!/usr/bin/env python

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
        sys.path.insert(0,base_path)

import bcove_responses
import copy
from concurrent.futures import Future
import logging
_log = logging.getLogger(__name__)
import json
import multiprocessing
from mock import patch, MagicMock
import os
import re
import redis
import random
import socket
import string
import test_utils.neontest
import test_utils.redis
import time
import threading
from tornado.httpclient import HTTPResponse, HTTPRequest
import tornado.ioloop
from utils.options import options
from utils.imageutils import PILImageUtils
import unittest
import test_utils.mock_boto_s3 as boto_mock
import test_utils.redis 
from StringIO import StringIO
from supportServices import neondata
from supportServices.neondata import NeonPlatform, BrightcovePlatform, \
        YoutubePlatform, NeonUserAccount, DBConnection, NeonApiKey, \
        AbstractPlatform, VideoMetadata, ThumbnailID, ThumbnailURLMapper,\
        ThumbnailMetadata, InternalVideoID, OoyalaPlatform, \
        TrackerAccountIDMapper, ThumbnailServingURLs, ExperimentStrategy, \
        ExperimentState, NeonApiRequest, CDNHostingMetadata,\
        S3CDNHostingMetadata, CloudinaryCDNHostingMetadata, \
        NeonCDNHostingMetadata, CDNHostingMetadataList, ThumbnailType

class TestNeondata(test_utils.neontest.AsyncTestCase):
    '''
    Neondata class tester
    '''
    def setUp(self):
        super(TestNeondata, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.maxDiff = 5000

    def tearDown(self):
        self.redis.stop()
        super(TestNeondata, self).tearDown()

    def test_neon_api_key(self):
        ''' test api key generation '''
        #Test key is random on multiple generations
        a_id = "testaccount"
        api_key_1 = NeonApiKey.generate(a_id)
        api_key_2 = NeonApiKey.generate(a_id)
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

        bp2 = BrightcovePlatform.get_account(bp.neon_api_key, 'iid')
        self.assertEqual(bp.__dict__, bp2.__dict__)

    def test_neon_user_account(self):
        ''' nuser account '''

        na = NeonUserAccount('acct1')
        bp = BrightcovePlatform('acct1', 'bp1', na.neon_api_key)
        bp.save()
        np = NeonPlatform('acct1', na.neon_api_key)
        np.save()
        yp = YoutubePlatform('acct1', 'yp1', na.neon_api_key)
        yp.save()
        na.add_platform(bp)
        na.add_platform(np)
        na.add_platform(yp)
        na.save()

        # Retrieve the account
        NeonUserAccount.get_account(na.neon_api_key,
                                    callback=self.stop)
        account = self.wait()
        self.assertIsNotNone(account)
        self.assertEqual(account.account_id, 'acct1')
        self.assertEqual(account.tracker_account_id, na.tracker_account_id)

        # Get the platforms
        account.get_platforms(callback=self.stop)
        platforms = self.wait()
        self.assertItemsEqual([x.__dict__ for x in platforms],
                              [x.__dict__ for x in [bp, np, yp]])

        # Try retrieving the platforms synchronously
        platforms = account.get_platforms()
        self.assertItemsEqual([x.__dict__ for x in platforms],
                              [x.__dict__ for x in [bp, np, yp]])

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

    @unittest.skip("DBconn check disabled")
    def test_db_connection_error(self):
        ''' #Verify that database connection is re-established 
        after config change '''
        ap = AbstractPlatform()
        db = DBConnection.get(ap)
        key = "fookey"
        val = "fooval"
        self.assertTrue(db.blocking_conn.set(key, val))
        self.redis.stop()
        
        #try fetching the key after db has been stopped
        try :
            db.blocking_conn.get(key)
        except Exception,e:
            print e
            #assert exception is ConnectionError 

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        
        #Trigger a change in the options, so that the watchdog thread 
        #can update the connection
        options._set("supportServices.neondata.dbPort", self.redis.port)
        check_interval = options.get("supportServices.neondata.watchdogInterval")
        time.sleep(check_interval + 0.5)
        
        #try any db operation
        self.assertTrue(db.blocking_conn.set(key, val))

    #TODO: Test Async DB Connection
    
    def test_db_connection(self):
        ''' 
        DB Connection test
        '''
        ap = AbstractPlatform()
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

        ap = AbstractPlatform()
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

    def test_too_many_open_connections_sync(self):
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
             lambda x: x.get_account('api')),
            (NeonPlatform('key', 'api'),
             lambda x: x.get_account('api')),
            (BrightcovePlatform('a', 'i', 'api'),
             lambda x: x.get_account('api', 'i')),
            (OoyalaPlatform('a', 'i', 'api', 'b', 'c', 'd', 'e'),
             lambda x: x.get_account('api', 'i')),
            (YoutubePlatform('a', 'i', 'api'),
             lambda x: x.get_account('api', 'i')),]

        for obj, read_func in obj_types:
            # Start by saving the object
            obj.save()

            # Now find the number of open file descriptors
            start_fd_count = len(os.listdir("/proc/%s/fd" % os.getpid()))

            def nop(x): pass

            # Run the func a bunch of times
            for i in range(10):
                read_func(obj)

            # Check the file descriptors
            end_fd_count = len(os.listdir("/proc/%s/fd" % os.getpid()))
            self.assertEqual(start_fd_count, end_fd_count)
    
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
                                          ['mycdn.neon-lab.com'])
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
        acc_id = 'acct1'
        na = NeonUserAccount(acc_id)
        na.save()
        vid = InternalVideoID.generate(na.neon_api_key, 'vid1')
        
        #Set experiment strategy
        es = ExperimentStrategy(na.neon_api_key)
        es.chosen_thumb_overrides = True
        es.override_when_done = True
        es.save()
        
        #Save thumbnails 
        thumbs = [
            ThumbnailMetadata('t1', vid, ['t1.jpg'], None, None, None,
                              None, None, None, serving_frac=0.8),
            ThumbnailMetadata('t2', vid, ['t2.jpg'], None, None, None,
                              None, None, None, serving_frac=0.15),
            ThumbnailMetadata('t3', vid, ['t3.jpg'], None, None, None,
                              None, None, None, serving_frac=0.01),
            ThumbnailMetadata('t4', vid, ['t4.jpg'], None, None, None,
                              None, None, None, serving_frac=0.04),
            ThumbnailMetadata('t5', vid, ['t5.jpg'], None, None, None,
                              None, None, None, serving_frac=0.0)
            ]
        ThumbnailMetadata.save_all(thumbs)
        
        #Save VideoMetadata
        tids = [thumb.key for thumb in thumbs]
        v0 = VideoMetadata(vid, tids, 'reqid0', 'v0.mp4', 0, 0, None, 0, None,
                            True, ExperimentState.COMPLETE)
        v0.save()
        
        #Set up Serving URLs (2 per tid)
        for thumb in thumbs:
            inp = ThumbnailServingURLs('%s_%s' % (vid, thumb.key))
            inp.add_serving_url('http://servingurl_800_600.jpg', 800, 600) 
            inp.add_serving_url('http://servingurl_120_90.jpg', 120, 90) 
            inp.save()

        winner_tid = v0.get_winner_tid()     
        self.assertEqual(winner_tid, 't1')

        # Case 2: chosen_thumb_overrides is False (holdback state)
        es.chosen_thumb_overrides = False
        es.override_when_done = False
        es.exp_frac = 0.2
        es.save()
        winner_tid = v0.get_winner_tid()     
        self.assertEqual(winner_tid, 't1')
    
        # Case 3: Experiement is in experimental state
        es.exp_frac = es.holdback_frac = 0.8
        es.save()
        winner_tid = v0.get_winner_tid()     
        self.assertEqual(winner_tid, 't1')

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
        np = NeonPlatform('acct1', na.neon_api_key)
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


    @patch('supportServices.neondata.VideoMetadata.get')
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
            'dhfaagb0z0h6n685ntysas00_e38ef7abba4c9102b26feb90bc5df3a8')

    def test_defaulted_get(self):
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



class TestDbConnectionHandling(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestDbConnectionHandling, self).setUp()
        self.connection_patcher = patch('supportServices.neondata.blockingRedis.StrictRedis')

        # For the sake of this test, we will only mock the get() function
        mock_redis = self.connection_patcher.start()
        self.mock_responses = MagicMock()
        mock_redis().get.side_effect = self._mocked_get_func

        self.valid_obj = TrackerAccountIDMapper("tai1", "api_key")

        # Speed up the retry delays to make the test faster
        self.old_delay = options.get('supportServices.neondata.baseRedisRetryWait')
        options._set('supportServices.neondata.baseRedisRetryWait', 0.01)

    def tearDown(self):
        self.connection_patcher.stop()
        DBConnection.clear_singleton_instance()
        options._set('supportServices.neondata.baseRedisRetryWait',
                     self.old_delay)
        super(TestDbConnectionHandling, self).tearDown()

    def _mocked_get_func(self, key, callback=None):
        if callback:
            self.io_loop.add_callback(callback, self.mock_responses(key))
        else:
            return self.mock_responses(key)

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
            with self.assertLogExists(logging.ERROR, 'Loading Error'):
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
            with self.assertLogExists(logging.ERROR, 'Loading Error'):
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
        self.s3_patcher = patch('api.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('hosting-bucket')
        self.bucket = self.s3conn.get_bucket('hosting-bucket')

        # Mock out cloudinary
        self.cloudinary_patcher = patch('api.cdnhosting.CloudinaryHosting')
        self.cloudinary_mock = self.cloudinary_patcher.start()
        future = Future()
        future.set_result(None)
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
                         ['https://s3.amazonaws.com/host-thumbnails/%s.jpg' %
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

        # Check the redirect object
        redirect = self.s3conn.get_bucket('host-thumbnails').get_key(
            'acct1/vid1/neon3.jpg')
        self.assertIsNotNone(redirect)
        self.assertEqual(redirect.redirect_destination,
                         '/' + primary_hosting_key)

        # Check cloudinary
        self.cloudinary_mock().upload.assert_called_with(thumb_info.urls[0],
                                                         thumb_info.key,
                                                         async=True)

    @tornado.testing.gen_test
    def test_add_thumbnail_to_video_and_save(self):
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

        self.assertEqual(thumb_info.video_id, video_info.key)
        self.assertIsNotNone(thumb_info.key)
        self.assertEqual(video_info.thumbnail_ids, [thumb_info.key])

        # Check that the images are in S3
        primary_hosting_key = re.sub('_', '/', thumb_info.key)+'.jpg'
        self.assertIsNotNone(self.s3conn.get_bucket('host-thumbnails').
                             get_key(primary_hosting_key))
        self.assertIsNotNone(self.s3conn.get_bucket('customer-bucket').
                             get_key('neontn%s_w480_h360.jpg'%thumb_info.key))
        redirect = self.s3conn.get_bucket('host-thumbnails').get_key(
            'acct1/vid1/customupload-1.jpg')
        self.assertIsNotNone(redirect)
        self.assertEqual(redirect.redirect_destination,
                         '/' + primary_hosting_key)

        # Check the database
        self.assertEqual(VideoMetadata.get('acct1_vid1').thumbnail_ids,
                         [thumb_info.key])
        self.assertEqual(ThumbnailMetadata.get(thumb_info.key).video_id,
                         'acct1_vid1')

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
        redirect = self.s3conn.get_bucket('host-thumbnails').get_key(
            'acct1/vid1/customupload-1.jpg')
        self.assertIsNotNone(redirect)
        self.assertEqual(redirect.redirect_destination,
                         '/' + primary_hosting_key)

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

        with patch('supportServices.neondata.utils.imageutils.PILImageUtils') \
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
    

if __name__ == '__main__':
    unittest.main()
