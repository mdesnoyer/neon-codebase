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
import test_utils.neontest
import test_utils.redis
import time
import threading
from tornado.httpclient import HTTPResponse, HTTPRequest
import tornado.ioloop
from utils.options import options
from utils.imageutils import ImageUtils
import unittest
import test_utils.redis 
from StringIO import StringIO
from supportServices.neondata import NeonPlatform, BrightcovePlatform, \
        YoutubePlatform, NeonUserAccount, DBConnection, NeonApiKey, \
        AbstractPlatform, VideoMetadata, ThumbnailID, ThumbnailURLMapper,\
        ImageMD5Mapper, ThumbnailMetaData, ThumbnailIDMapper

class TestNeondata(test_utils.neontest.AsyncTestCase):
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
        self.assertItemsEqual(a_ids, nu_a_ids)

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


class TestBrightcovePlatform(unittest.TestCase):
    '''
    Brightcove platform specific tests
    '''
    def setUp(self):
        self.cp_sync_patcher = \
          patch('utils.http.tornado.httpclient.HTTPClient')
        self.cp_async_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.cp_mock_client = self.cp_sync_patcher.start()
        self.cp_mock_async_client = self.cp_async_patcher.start()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

    def tearDown(self):
        self.cp_sync_patcher.stop()
        self.cp_async_patcher.stop()
        self.redis.stop()
        super(TestBrightcovePlatform, self).tearDown()

    def test_check_feed(self):
        ''' check brightcove feed '''

        def _side_effect(*args, **kwargs):
            ''' mock side effect '''

            request = args[0]
            if "find_modified_videos" in request.url:
                return bcove_find_modified_videos_response 
            if "find_all_videos" in request.url:
                return bcove_response
            elif "submitvideo" in request.url:
                return neon_api_response

        bcove_request = HTTPRequest(
            'http://api.brightcove.com/services/library?'
            'get_item_count=true&command=find_all_videos&page_size=5&sort_by='
            'publish_date&token=rtoken&page_number=0&\
             output=json&media_delivery=http') 
        bcove_response = HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_all_videos_response))
    
        neon_api_response = HTTPResponse(bcove_request, 200,
                buffer=StringIO('{"job_id":"j123"}'))

        bcove_find_modified_videos_response = \
                HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_modified_videos_response))

        a_id = 'test' 
        i_id = 'i123'
        nvideos = 6
        na = NeonUserAccount('acct1')
        na.save()
        bp = BrightcovePlatform(a_id, i_id, na.neon_api_key, 'p1', 'rt', 'wt', 
                last_process_date=21492000000)
        bp.account_created = 21492000
        bp.save()
        self.cp_mock_client().fetch.side_effect = _side_effect 
        
        bp.check_feed_and_create_api_requests()

        u_bp = BrightcovePlatform.create(bp.get())
        self.assertEqual(len(u_bp.get_videos()), nvideos)

class TestThumbnailHelperClass(unittest.TestCase):
    '''
    Thumbnail ID Mapper and other thumbnail helper class tests 
    '''
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

    def tearDown(self):
        self.redis.stop()

    def test_thumbnail_mapper(self):
        ''' Thumbnail mappings '''

        url = "http://thumbnail.jpg"
        vid = "v123"
        image = ImageUtils.create_random_image(360, 480)
        tid = ThumbnailID.generate(image, vid)
        im_md5 = ImageMD5Mapper(vid, image, tid) 
        im_md5.save()

        res_tid = ImageMD5Mapper.get_tid(vid, im_md5)
        #self.assertEqual(tid, res_tid)   
    
        tdata = ThumbnailMetaData(tid, [], 0, 480, 360,
                        "ttype", 0, 1, 0)
        idmapper = ThumbnailIDMapper(tid, vid, tdata.to_dict())
        ThumbnailIDMapper.save_all([idmapper])
        tmap = ThumbnailURLMapper(url, tid)
        tmap.save()
        
        res_tid = ThumbnailURLMapper.get_id(url)
        self.assertEqual(tid, res_tid)


if __name__ == '__main__':
    
    #test_classes_to_run = [TestNeondata, TestBrightcovePlatform]
    test_classes_to_run = [TestThumbnailHelperClass]
    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    alltests = unittest.TestSuite(suites_list)

    runner = unittest.TextTestRunner()
    results = runner.run(alltests)
