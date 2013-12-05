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
from StringIO import StringIO
from mock import patch, MagicMock
from tornado.concurrent import Future
from tornado.testing import AsyncHTTPTestCase,AsyncTestCase,AsyncHTTPClient
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError

from api import server
from supportServices import neondata
import utils
from utils.options import define, options
import logging
_log = logging.getLogger(__name__)

random.seed(1324)
class TestVideoServer(AsyncHTTPTestCase):
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
    
    
    def _db_side_effect(*args,**kwargs):
        key = args[0]
        cb  = args[1]
        print key,cb
        return "something"

    def get_app(self):
        return server.application
    
    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()
    
    def cleanup_db(self,prefix):
        import redis
        client = redis.StrictRedis()
        keys = client.keys("*%s*"%prefix)
        for key in keys:
            client.delete(key)

    def tearDown(self):
        self.sync_patcher.stop()
        self.async_patcher.stop()
        #self.mock_nplatform_patcher.stop()
        self.redis.stop()

    def make_neon_api_request(self,vals):
        body = json.dumps(vals)
        self.real_asynchttpclient.fetch(self.neon_api_url, 
                callback=self.stop, method="POST", body=body)
        response = self.wait()
        return response

    def test_neon_api_request(self):
        #Create fake account
        na = neondata.NeonPlatform("testaccountneonapi")
        api_key = na.neon_api_key
        self.cleanup_db(api_key)
        na.save()
        vals = {"api_key": api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title"}

        resp = self.make_neon_api_request(vals)
        self.cleanup_db(api_key)
        self.assertEqual(resp.code,201)

    def test_duplicate_request(self):
        na = neondata.NeonPlatform("testaccountneonapi")
        api_key = na.neon_api_key
        na.save()
        vals = {"api_key": api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title" }
        resp = self.make_neon_api_request(vals)
        resp = self.make_neon_api_request(vals)
        self.cleanup_db(api_key)
        self.assertEqual(resp.code,409)

    def test_brightcove_request(self):
        #create brightcove platform account
        na = neondata.NeonPlatform("testaccountneonapi")
        api_key = na.neon_api_key
        na.save()
        i_id = "i125"
        bp = neondata.BrightcovePlatform("testaccountneonapi",i_id)
        bp.save()

        vals = {"api_key": api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "testid123", "topn":2, 
                    "callback_url": "http://callback_push_url", 
                    "video_title": "test_title",
                    "autosync" : False,
                    "topn" : 1,
                    "integration_id" : i_id,
                    "publisher_id" : "pubid",
                    "read_token": "rtoken",
                    "write_token": "wtoken"
                    }
        resp = self.make_neon_api_request(vals)
        self.cleanup_db(api_key)
        self.assertEqual(resp.code,201)

    def test_empty_request(self):
        self.real_asynchttpclient.fetch(self.neon_api_url, 
                callback=self.stop, method="POST", body='')
        resp = self.wait()
        self.assertEqual(resp.code,400)

if __name__ == '__main__':
    unittest.main()
