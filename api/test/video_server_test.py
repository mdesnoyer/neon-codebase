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

from api import server 
import api.server
import logging
import json
from mock import patch, MagicMock
import os
import re
import random
import subprocess
from supportServices import neondata
from StringIO import StringIO
import test_utils.redis
import test_utils.neontest
import time
import threading
import tornado.gen
import tornado.web
import tornado.httpclient
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import unittest
import urllib

from utils.options import define, options

_log = logging.getLogger(__name__)
NEON_AUTH = "secret_key"

class TestSimpleThreadSafeDictQ(test_utils.neontest.TestCase):
    '''
    # NOTE: according to the Queue documentation, the items inserted arent'
    immediately availale, hence after puts there is a small time delay
    introduced
    '''
    def setUp(self):
        super(TestSimpleThreadSafeDictQ, self).setUp()
        self.sq = server.SimpleThreadSafeDictQ(int)

    def tearDown(self):
        super(TestSimpleThreadSafeDictQ, self).tearDown()

    def test_put(self):
        n = 100
        for i in range(n):
            ret = self.sq.put(i, i)
     
        # assert Q is not empty
        self.assertFalse(self.sq.is_empty())

        # verify all the puts
        for i in range(n):
            item = self.sq.get()
            self.assertEqual(item, i)
       
        # assert all items have been pop'ed
        self.assertTrue(self.sq.is_empty())

    def test_qsize(self):

        self.assertEqual(self.sq.size(), 0)

        n = 100
        for i in range(n):
            self.sq.put(i, i)
        
        self.assertEqual(self.sq.size(), n)

        for i in range(n/2):
            self.sq.get()
        

        self.assertEqual(self.sq.size(), n/2)

    def test_empty(self):
        self.assertTrue(self.sq.is_empty())

    def test_peek(self):
        n = 100
        for i in range(n):
            self.sq.put(i, i)
       
        # test peek'ing using the key
        for i in range(n):
            val = self.sq.peek(i)
            self.assertEqual(val, i)

    def test_q_with_multiple_threads(self):

        # start 100 threads to insert items
        n = 100
        threads = []
        for i in range(n):
            t = threading.Thread(target=self.sq.put, args=(i, i,))
            threads.append(t)
        
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # Check all the puts    
        items = [self.sq.get() for i in range(n)]
        self.assertSetEqual(set(items), set([i for i in range(n)]))

class TestFairWeightedQ(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestFairWeightedQ, self).setUp()
        self.fwq = server.FairWeightedRequestQueue(nqueues=2)
        self.fwq._schedule_metadata_thread = MagicMock()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
        
        #set up 2 test accounts with diff priorities
        self.nuser1 = neondata.NeonUserAccount("acc1")
        self.nuser1.save()
        self.nuser2 = neondata.NeonUserAccount("acc2")
        self.nuser2.set_processing_priority(0)
        self.nuser2.save()

    def tearDown(self):
        super(TestFairWeightedQ, self).tearDown()
        self.redis.stop()

    def test_basic_queue_operations(self):
        # pop from an empty Q 
        item = self.fwq.get()
        self.assertIsNone(item)

        # Insert 10 items with single priority and verify that
        # it works as a queue

        n = 10
        for i in range(n):
            req = neondata.NeonApiRequest('job%s' % i, self.nuser1.neon_api_key,
                    'vid%s' % i, 't', 't', 'r', 'h')
            self.fwq.put(req)
        

        for i in range(n):
            item = self.fwq.get()
            req = neondata.NeonApiRequest.create(item)
            self.assertEqual(req.video_id, 'vid%s' % i)

    def test_fairweightedness_queue(self):
        distribution = [self.fwq._get_priority_qindex() for i in range(100)]
        ratio = distribution.count(0)/float(distribution.count(1))
        # TODO (Sunil) define exact boundaries for this ratio
        # for now check that ratio is > 1, i.e its a priority Q :)
        self.assertGreater(ratio, 1.0)

    def test_items_with_different_priorities(self):
        # Insert items with different priorties
        p1n = 4 
        for i in range(p1n):
            i = "p1%s" % i
            req = neondata.NeonApiRequest('job%s' % i, self.nuser1.neon_api_key,
                    'vid%s' % i, 't', 't', 'r', 'h')
            self.fwq.put(req)
        
        p0n = 10
        for i in range(p0n):
            i = "p0%s" % i
            req = neondata.NeonApiRequest('job%s' % i, self.nuser2.neon_api_key,
                    'vid%s' % i, 't', 't', 'r', 'h')
            self.fwq.put(req)
        
       
        # verify popping every element inserted
        for i in range(p1n+p0n):
            item = self.fwq.get()
            self.assertIsNotNone(item)

    @patch('utils.http')
    def test_adding_metadata(self, mock_http):
        vsize = 1024
        i = 0
        req = neondata.NeonApiRequest('job%s' % i, self.nuser1.neon_api_key,
                    'vid%s' % i, 't', 't', 'r', 'h')
        self.fwq.put(req)
        request = tornado.httpclient.HTTPRequest("http://xyz")
        response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO(''), headers={'Content-Length': vsize})
        mock_http.send_request.side_effect = \
                lambda x, callback: callback(response)
     
        # TODO (Sunil): Refactor when you use tornado 4.0
        response = self.fwq._add_metadata(1, req.key, callback=self.wait)
        self.assertTrue(mock_http.send_request.called)
        # check request in Q, should be in the Q with priority=1
        # kinda hacky, but we need to get the raw RequestData object
        item = self.fwq.pqs[1].get()
        self.assertEqual(item.get_video_size(), vsize)
    
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

        self.http_patcher = patch('utils.http')
        self.mock_http = self.http_patcher.start()
        vsize = 1024
        request = tornado.httpclient.HTTPRequest("http://xyz")
        response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO(''), headers={'Content-Length': vsize})
        self.mock_http.send_request.side_effect = \
                lambda x, callback: callback(response)

    def get_app(self):
        return api.server.application
    
    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()
    
    def tearDown(self):
        self.redis.stop()
        self.http_patcher.stop()
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
        resp = self.add_request("neonapivid123")
        self.assertEqual(resp.code, 201)
        
        # verify request saved in DB
        job_id = json.loads(resp.body)['job_id']
        req = neondata.NeonApiRequest.get_request(self.api_key, job_id)
        api_request = neondata.NeonApiRequest.create(req)
        self.assertEqual(api_request.video_id, "neonapivid123")

    def test_neon_api_request_invalid_id(self):
        resp = self.add_request("neonap_-ivid123") 
        self.assertEqual(resp.code, 400)

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
                    "previous_thumbnail": "http://prev_thumb"
                    }
        url = self.get_url('/api/v1/submitvideo/brightcove')
        resp = self.make_api_request(vals, url)
        self.assertEqual(resp.code, 201)
        
        job_id = json.loads(resp.body)['job_id']
        req = neondata.NeonApiRequest.get_request(self.api_key, job_id)
        api_request = neondata.NeonApiRequest.create(req)
        self.assertEqual(api_request.video_id, "testid123")

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
        h = {'X-Neon-Auth' : NEON_AUTH} 
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

    def test_request_without_callback_url(self):
        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "vid1" , "topn":2, 
                    "video_title": "test_title"}
        resp = self.make_api_request(vals)
        self.assertEqual(resp.code, 201)
        
class VideoServerSmokeTest(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(VideoServerSmokeTest, self).setUp()
        self.fwq = server.FairWeightedRequestQueue(nqueues=2)
        self.nuser1 = neondata.NeonUserAccount("acc1")
        self.nuser1.save()
        self.nuser2 = neondata.NeonUserAccount("acc2")
        self.nuser2.set_processing_priority(0)
        self.nuser2.save()

    def tearDown(self):
        super(VideoServerSmokeTest, self).tearDown()

    @patch('utils.http')
    def test_system(self, mock_http):

        # producer insert requests
        # thread to DQ requests
        # verify DQ'ed item format to match with client
        # make sure you can fetch counters?

        vsize = 1024
        request = tornado.httpclient.HTTPRequest("http://xyz")
        response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO(''), headers={'Content-Length': vsize})
        mock_http.send_request.side_effect = \
                lambda x, callback: callback(response)

        items_put = []
        items_get = []
        n = 10
        for i in range(n):
            api_key = self.nuser1.neon_api_key
            if i % 2 == 0:
                api_key = self.nuser2.neon_api_key

            req = neondata.NeonApiRequest('job%s' % i, api_key, 
                    'vid%s' % i, 't', 't', 'r', 'h')
            items_put.append(req)
        
        def _items_put(i):
            item = items_put[i]
            time.sleep(random.random()/100)
            self.fwq.put(item)

        def _items_get():
            # keep hammering with more gets
            for i in range(n*2):
                time.sleep(random.random()/100)
                item = self.fwq.get()
                if item:
                    items_get.append(item)

        gt = threading.Thread(target=_items_get)
        gt.start()
        
        threads = []
        for i in range(n):
            t = threading.Thread(target=_items_put, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        
        gt.join(timeout=5.0)

        items_put_json = [r.to_json() for r in items_put]
        for i in range(len(items_put) - len(items_get)):
            items_get.append(self.fwq.get())
        
        self.assertSetEqual(set(items_get), set(items_put_json))
      
        # verify that the add metadata thread ran and we were able
        # to collect some data on size of Q in # of bytes 
        state_vars = server.statemon.state.get_all_variables()
        qsize = ['api.server.queue_size_bytes']
        self.assertGreater(qsize, 0)

if __name__ == '__main__':
    unittest.main()
