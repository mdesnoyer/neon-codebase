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
from mock import patch
import random
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
        ThumbnailMetadata, InternalVideoID, OoyalaPlatform

class TestNeondata(test_utils.neontest.AsyncTestCase):
    '''
    Neondata class tester
    '''
    def setUp(self):
        super(TestNeondata, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

    def tearDown(self):
        self.redis.stop()
        super(TestNeondata, self).tearDown()

    # TODO: It should be possible to run this with an IOLoop for each
    # test, but it's not running. Need to figure out why.
    def get_new_ioloop(self):
        ''' new ioloop '''
        return tornado.ioloop.IOLoop.instance()

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
        #self.assertItemsEqual(a_ids, nu_a_ids)

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
                'R4YjBnMjrzRQRcDLf34bXbRH4qR6CEF1' ]

        for i in range(100):
            external_vids.append(id_generator())

        i_vids = [InternalVideoID.generate("apikey", vid) for vid in external_vids]

        result = [InternalVideoID.to_external(i_vid) for i_vid in i_vids]

        self.assertEqual(result, external_vids)


if __name__ == '__main__':
    
    test_classes_to_run = [TestNeondata, TestThumbnailHelperClass]
    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    alltests = unittest.TestSuite(suites_list)

    runner = unittest.TextTestRunner()
    results = runner.run(alltests)
