#!/usr/bin/env python

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
        sys.path.insert(0,base_path)

import bcove_responses
import logging
_log = logging.getLogger(__name__)
import multiprocessing
from mock import patch, MagicMock
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
import test_utils.redis 
from StringIO import StringIO
from supportServices.neondata import NeonPlatform, BrightcovePlatform, \
        YoutubePlatform, NeonUserAccount, DBConnection, NeonApiKey, \
        AbstractPlatform, VideoMetadata, ThumbnailID, ThumbnailURLMapper,\
        ThumbnailMetadata, InternalVideoID, OoyalaPlatform, \
        TrackerAccountIDMapper, ThumbnailServingURLs, ExperimentStrategy, \
        ExperimentState, NeonApiRequest

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

    def test_add_platform_with_bad_account_id(self):
        pass
    
    def test_dbconn_singleton(self):
        bp = BrightcovePlatform('2','3', 'test')
        self.bp_conn = DBConnection(bp)

        bp2 = BrightcovePlatform('12','13','test')
        self.bp_conn2 = DBConnection(bp2)


        vm = VideoMetadata('test1', None, None, None,
                None, None, None, None)
        self.vm_conn = DBConnection(vm)

        vm2 = VideoMetadata('test2', None, None, None, 
                None, None, None, None)
        self.vm_conn2 = DBConnection(vm2)
        
        self.assertEqual(self.bp_conn, self.bp_conn2)
        self.assertEqual(self.vm_conn, self.vm_conn2)

        self.assertNotEqual(self.bp_conn, self.vm_conn)

    @unittest.skip("DBconn check disabled")
    def test_db_connection_error(self):
        ''' #Verify that database connection is re-established 
        after config change '''
        ap = AbstractPlatform()
        db = DBConnection(ap)
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
        db = DBConnection(ap)
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
        db = DBConnection(ap)
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
        print found_thumb_ids
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
        ThumbnailMetadata.modify(tid, setphash, callback=self.stop)
        ThumbnailMetadata.modify(tid, setrank, callback=self.stop)
        self.wait() #wait() runs the IOLoop until self.stop() is called
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
        

    def test_read_thumbnail_old_format(self):
        # Make sure that we're backwards compatible
        thumb = ThumbnailMetadata._create('2630b61d2db8c85e9491efa7a1dd48d0_2876590502001_9e1d6017cab9aa970fca5321de268e15', "{\"video_id\": \"2630b61d2db8c85e9491efa7a1dd48d0_2876590502001\", \"thumbnail_metadata\": {\"chosen\": false, \"thumbnail_id\": \"2630b61d2db8c85e9491efa7a1dd48d0_2876590502001_9e1d6017cab9aa970fca5321de268e15\", \"model_score\": 4.1698684091322846, \"enabled\": true, \"rank\": 5, \"height\": 232, \"width\": 416, \"model_version\": \"20130924\", \"urls\": [\"https://host-thumbnails.s3.amazonaws.com/2630b61d2db8c85e9491efa7a1dd48d0/208720d8a4aef7ee5f565507833e2ccb/neon4.jpeg\"], \"created_time\": \"2013-12-02 09:55:19\", \"type\": \"neon\", \"refid\": null}, \"key\": \"2630b61d2db8c85e9491efa7a1dd48d0_2876590502001_9e1d6017cab9aa970fca5321de268e15\"}")

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


if __name__ == '__main__':
    unittest.main()
