#!/usr/bin/env python
'''
Unit test for Video Server
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
        sys.path.insert(0,base_path)

import os
import subprocess
import re
import unittest
import urllib
import random
import test_utils.redis
import tornado.gen
import tornado.ioloop
import tornado.web
import tornado.httpclient
import json
from mock import patch
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError

from api import server,properties
from supportServices import neondata
from utils.options import define, options
import logging
_log = logging.getLogger(__name__)

random.seed(1324)
class TestVideoServer(AsyncHTTPTestCase):
    ''' Video Server test'''

    def setUp(self):
        super(TestVideoServer, self).setUp()
        #create un-mocked httpclients for use of testcases later
        self.real_httpclient = tornado.httpclient.HTTPClient()
        self.real_asynchttpclient = tornado.httpclient.AsyncHTTPClient(self.io_loop)
        self.sync_patcher = patch('tornado.httpclient.HTTPClient')
        self.async_patcher = patch('tornado.httpclient.AsyncHTTPClient')
        self.mock_client = self.sync_patcher.start()
        self.mock_async_client = self.async_patcher.start()

        #self.nplatform_patcher = patch('api.server.NeonPlatform')
        #self.mock_nplatform_patcher = self.nplatform_patcher.start()
        #self.mock_nplatform_patcher.get_account.side_effect = self._db_side_effect

        self.base_uri = '/api/v1/submitvideo/topn'
        self.neon_api_url = self.get_url(self.base_uri)
  
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

        #create test account
        self.na = neondata.NeonPlatform("testaccountneonapi")
        self.api_key = self.na.neon_api_key
        self.na.save()
    
    #def _db_side_effect(*args, **kwargs):
    #
    #    key = args[0]
    #    cb  = args[1]
    #    print key,cb
    #    return "something"

    def get_app(self):
        return server.application
    
    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()
    
    def tearDown(self):
        self.sync_patcher.stop()
        self.async_patcher.stop()
        #self.mock_nplatform_patcher.stop()
        self.redis.stop()

    def make_api_request(self,vals,url=None):
        if not url:
            url = self.neon_api_url

        body = json.dumps(vals)
        self.real_asynchttpclient.fetch(url, 
                callback=self.stop, method="POST", body=body)
        response = self.wait()
        return response
    
    def add_request(self,video_id="vid123"):

        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": video_id , "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title"}
        resp = self.make_api_request(vals)
        return resp

    def test_neon_api_request(self):
        resp = self.add_request("neonapi_vid123") 
        self.assertEqual(resp.code,201)

    def test_duplicate_request(self):
        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title" }
        resp = self.make_api_request(vals)
        resp = self.make_api_request(vals)
        self.assertEqual(resp.code,409)

    def test_brightcove_request(self):
        #create brightcove platform account
        i_id = "i125"
        bp = neondata.BrightcovePlatform("testaccountneonapi",i_id)
        bp.save()

        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title",
                    "autosync" : False,
                    "topn" : 1,
                    "integration_id" : i_id,
                    "publisher_id" : "pubid",
                    "read_token": "rtoken",
                    "write_token": "wtoken",
                    "previous_thumbnail": "http://prev_thumb"
                    }
        url = self.get_url('/api/v1/submitvideo/brightcove')
        resp = self.make_api_request(vals,url)
        self.assertEqual(resp.code,201)

    def test_empty_request(self):
        self.real_asynchttpclient.fetch(self.neon_api_url, 
                callback=self.stop, method="POST", body='')
        resp = self.wait()
        self.assertEqual(resp.code,400)

    def test_dequeue_handler(self):
            
        resp = self.add_request()
        self.assertEqual(resp.code,201)
        h = {'X-Neon-Auth' : properties.NEON_AUTH} 
        for i in range(10): #dequeue a bunch 
            self.real_asynchttpclient.fetch(self.get_url('/dequeue'), 
                callback=self.stop, method="GET", headers=h)
            resp = self.wait()
            self.assertEqual(resp.code,200)
        
        self.assertEqual(resp.body,'{}')
        
    def test_requeue_handler(self):
        self.add_request()
        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title"}
        jdata = json.dumps(vals)
        self.real_asynchttpclient.fetch(self.get_url('/requeue'),
                callback=self.stop, method="POST", body=jdata)
        resp = self.wait()
        self.assertEqual(resp.code, 200)

    def test_job_status_handler(self):
        resp = self.add_request()
        job_id = json.loads(resp.body)['job_id']
        self.real_asynchttpclient.fetch(
                self.get_url('/api/v1/jobstatus?api_key=%s&job_id=%s'
                    %(self.api_key, job_id)),
                callback=self.stop, method="GET")
        resp = self.wait()
        self.assertEqual(resp.code, 200)
        
        #wrong job_id
        job_id = 'dummyjobid'
        self.real_asynchttpclient.fetch(
                self.get_url('/api/v1/jobstatus?api_key=%s&job_id=%s'
                    %(self.api_key, job_id)),
                callback=self.stop, method="GET")
        resp = self.wait()
        self.assertEqual(resp.code, 400)

    def test_get_results_handler(self):
        pass
        #TODO: Get results from DB
        #resp = self.add_request()
        #self.real_asynchttpclient.fetch(
        #        self.get_url('/api/v1/jobstatus?api_key=%s&job_id=%s'
        #            %(self.api_key,job_id)),
        #        callback=self.stop, method="GET")
        #resp = self.wait()
        #self.assertEqual(resp.code,200)

if __name__ == '__main__':
    unittest.main()
