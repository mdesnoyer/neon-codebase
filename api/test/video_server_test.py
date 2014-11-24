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

from api import properties
import api.server
import logging
import json
from mock import patch
import os
import re
import random
import subprocess
from supportServices import neondata
import test_utils.redis
import tornado.gen
import tornado.web
import tornado.httpclient
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import unittest
import urllib

from utils.options import define, options

_log = logging.getLogger(__name__)

class TestVideoServer(AsyncHTTPTestCase):
    ''' Video Server test'''

    @classmethod
    def setUpClass(cls):
        super(TestVideoServer, cls).setUpClass()
    
    def setUp(self):
        super(TestVideoServer, self).setUp()

        self.base_uri = '/api/v1/submitvideo/topn'
        self.neon_api_url = self.get_url(self.base_uri)
  
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
        random.seed(1324)

        #create test account
        a_id = "testaccountneonapi"
        self.nuser = neondata.NeonUserAccount(a_id)
        self.nuser.save()
        self.api_key = self.nuser.neon_api_key
        self.na = neondata.NeonPlatform(a_id, self.api_key)
        self.na.save()

    def get_app(self):
        return api.server.application
    
    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()
    
    def tearDown(self):
        self.redis.stop()
        super(TestVideoServer, self).tearDown()

    def make_api_request(self, vals, url=None):
        if not url:
            url = self.neon_api_url

        body = json.dumps(vals)
        request = tornado.httpclient.HTTPRequest(
                                url=url, 
                                method="POST",
                                body=body)
        self.http_client.fetch(request, self.stop)
        response = self.wait()
        return response
    
    def add_request(self, video_id="vid123"):

        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": video_id , "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title"}
        resp = self.make_api_request(vals)
        return resp

    def test_neon_api_request(self):
        resp = self.add_request("neonapi_vid123") 
        self.assertEqual(resp.code, 201)

    def test_duplicate_request(self):
        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title" }
        resp = self.make_api_request(vals)
        resp = self.make_api_request(vals)
        self.assertEqual(resp.code, 409)

    def test_brightcove_request(self):
        ''' create brightcove platform account '''

        i_id = "i125"
        bp = neondata.BrightcovePlatform("testaccountneonapi", i_id,
               self.api_key)
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
                    "default_thumbnail": "http://prev_thumb"
                    }
        url = self.get_url('/api/v1/submitvideo/brightcove')
        resp = self.make_api_request(vals, url)
        self.assertEqual(resp.code, 201)

    def test_empty_request(self):
        ''' test empty request '''
        self.http_client.fetch(self.neon_api_url, 
                callback=self.stop, method="POST", body='')
        resp = self.wait()
        self.assertEqual(resp.code, 400)

    def test_dequeue_handler(self):
        ''' Dequeue handler of server '''

        resp = self.add_request()
        self.assertEqual(resp.code, 201)
        h = {'X-Neon-Auth' : 'secret_token'} 
        for i in range(10): #dequeue a bunch 
            self.http_client.fetch(self.get_url('/dequeue'), 
                callback=self.stop, method="GET", headers=h)
            resp = self.wait()
            self.assertEqual(resp.code, 200)
        
        self.assertEqual(resp.body,'{}')
        
    def test_requeue_handler(self):
        ''' requeue handler '''
        self.add_request()
        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title"}
        jdata = json.dumps(vals)
        self.http_client.fetch(self.get_url('/requeue'),
                callback=self.stop, method="POST", body=jdata)
        resp = self.wait()
        self.assertEqual(resp.code, 200)

    def test_job_status_handler(self):
        resp = self.add_request()
        job_id = json.loads(resp.body)['job_id']
        self.http_client.fetch(
                self.get_url('/api/v1/jobstatus?api_key=%s&job_id=%s'
                    %(self.api_key, job_id)),
                callback=self.stop, method="GET")
        resp = self.wait()
        self.assertEqual(resp.code, 200)
        
        #wrong job_id
        job_id = 'dummyjobid'
        self.http_client.fetch(
                self.get_url('/api/v1/jobstatus?api_key=%s&job_id=%s'
                    %(self.api_key, job_id)),
                callback=self.stop, method="GET")
        resp = self.wait()
        self.assertEqual(resp.code, 400)

    def test_request_without_callback_url(self):
        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "vid1" , "topn":2, 
                    "video_title": "test_title"}
        resp = self.make_api_request(vals)
        self.assertEqual(resp.code, 201)
        
if __name__ == '__main__':
    unittest.main()
