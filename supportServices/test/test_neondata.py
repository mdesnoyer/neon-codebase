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
from mock import patch, MagicMock
import time
import test_utils.redis
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from utils.options import define, options
import unittest

from supportServices.neondata import *
from test_utils.redis import * 

#TODO: Test db connection stuff and more.....

class TestNeondata(unittest.TestCase):
    def setUp(self):
        self.redis = RedisServer()
        self.redis.start()

    def tearDown(self):
        self.redis.stop()

    '''
    def test_dbconn_singleton(self):
        bp = BrightcovePlatform('2','3',4)
        self.bp_conn = DBConnection(bp)

        bp2 = BrightcovePlatform('12','13',4)
        self.bp_conn2 = DBConnection(bp2)


        vm = VideoMetadata('test1',None,None,None,None,None,None,None)
        self.vm_conn = DBConnection(vm)

        vm2 = VideoMetadata('test2',None,None,None,None,None,None,None)
        self.vm_conn2 = DBConnection(vm2)
        
        self.assertEqual(self.bp_conn,self.bp_conn2)
        self.assertEqual(self.vm_conn,self.vm_conn2)

        self.assertNotEqual(self.bp_conn,self.vm_conn)
    '''

    '''
    #Verify that database connection is re-established after config change
    def test_db_connection_error(self):
        ap = AbstractPlatform()
        db = DBConnection(ap)
        key = "fookey"
        val = "fooval"
        self.assertTrue(db.blocking_conn.set(key,val))
        self.redis.stop()
        
        #try fetching the key after db has been stopped
        try :
            db.blocking_conn.get(key)
        except Exception,e:
            print e
            #assert exception is ConnectionError 

        self.redis = RedisServer()
        self.redis.start()
        
        #Trigger a change in the options, so that the watchdog thread 
        #can update the connection
        options._set("supportServices.neondata.dbPort",self.redis.port)
        check_interval = options.get("supportServices.neondata.watchdogInterval")
        time.sleep(check_interval + 0.5)
        
        #try any db operation
        self.assertTrue(db.blocking_conn.set(key,val))

    #TODO: Test Async DB Connection
    '''
    
    def test_db_connection(self):
        ap = AbstractPlatform()
        db = DBConnection(ap)
        key = "fookey"
        val = "fooval"
        self.assertTrue(db.blocking_conn.set(key,val))
        self.assertEqual(db.blocking_conn.get(key),val)
        self.assertTrue(db.blocking_conn.delete(key))


class TestBrightcovePlatform(unittest.TestCase):
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

    #TODO
    def test_check_feed(self):
        def _side_effect(*args,**kwargs):
            request = args[0]
            if "find_modified_videos" in request.url:
                return bcove_find_modified_videos_response 
            if "find_all_videos" in request.url:
                return bcove_response
            elif "submitvideo" in request.url:
                return neon_api_response

        bcove_request = HTTPRequest('http://api.brightcove.com/services/library?'
            'get_item_count=true&command=find_all_videos&page_size=5&sort_by='
            'publish_date&token=rtoken&page_number=0&output=json&media_delivery=http') #build the string
        bcove_response = HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_all_videos_response))
    
        neon_api_response = HTTPResponse(bcove_request, 200,
                buffer=StringIO('{"job_id":"j123"}'))

        bcove_find_modified_videos_response = HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_modified_videos_response))

        a_id = 'test' ; i_id = 'i123' ;
        nvideos = 6
        bp = BrightcovePlatform(a_id,i_id,'p1','rt','wt',last_process_date=21492000000)
        bp.account_created = 21492000
        bp.save()
        self.cp_mock_client().fetch.side_effect = _side_effect 
        
        bp.check_feed_and_create_api_requests()

        u_bp = BrightcovePlatform.create(bp.get())
        self.assertEqual(len(u_bp.get_videos()),nvideos)

if __name__ == '__main__':
    
    test_classes_to_run = [TestNeondata,TestBrightcovePlatform]
    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    alltests = unittest.TestSuite(suites_list)

    runner = unittest.TextTestRunner()
    results = runner.run(alltests)
