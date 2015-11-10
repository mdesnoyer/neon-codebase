#!/usr/bin/env python
'''
Unit test for Video Server
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)

import boto.sqs
from boto.sqs.message import Message
from cmsdb import neondata
import concurrent.futures
import logging
import json
from mock import patch, MagicMock
import os
import re
import random
import subprocess
from StringIO import StringIO
import test_utils.mock_boto_s3 as boto_mock
import test_utils.redis
import test_utils.neontest
from test_utils import sqsmock
import time
import threading
import tornado.gen
import tornado.web
import tornado.httpclient
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import unittest
import urllib
from utils.imageutils import PILImageUtils
import utils.neon
from utils.options import define, options
from utils import statemon
import video_processor

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
        self.sq = video_processor.server.SimpleThreadSafeDictQ(int)

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

    def test_inserting_duplicate(self):
        k = 1    
        self.assertTrue(self.sq.put(k, k))
        self.assertFalse(self.sq.put(k, k))

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
        self.fwq = video_processor.server.FairWeightedRequestQueue(nqueues=2)
        self.fwq._schedule_metadata_thread = MagicMock()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
        
        #set up 2 test accounts with diff priorities
        self.nuser1 = neondata.NeonUserAccount("acc1")
        self.nuser1.save()
        self.nuser2 = neondata.NeonUserAccount("acc2")
        self.nuser2.set_processing_priority(0)
        self.nuser2.save()

        # Patch the video length lookup
        self.send_request_patcher = patch('video_processor.server.utils.http.send_request')
        self.mock_send_request = self._future_wrap_mock(
            self.send_request_patcher.start())
        self.video_size = MagicMock()
        self.video_size.return_value = 10.0
        self.mock_send_request.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, 200, buffer=StringIO(''),
        headers={'Content-Length': self.video_size()})

    def tearDown(self):
        self.send_request_patcher.stop()
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
            self.assertEqual(item.api_request.video_id, 'vid%s' % i)
            self.assertEqual(item.api_request.job_id, 'job%s' % i)
            self.assertEqual(item.api_request.api_key,
                             self.nuser1.neon_api_key)

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

    @tornado.testing.gen_test
    def test_adding_metadata(self):
        vsize = 1024
        self.video_size.return_value = vsize
        req = neondata.NeonApiRequest('job0', self.nuser1.neon_api_key,
                                      url='http://someurl')
        yield self.fwq.put(req, async=True)
        self.assertTrue(self.mock_send_request.called)
     
        # check request in Q, should be in the Q with priority=1
        # kinda hacky, but we need to get the raw RequestData object
        item = self.fwq.get()
        self.assertEqual(item.get_video_size(), vsize)

    @tornado.testing.gen_test
    def test_with_duration(self):
        req = neondata.NeonApiRequest('job0', self.nuser1.neon_api_key,
                                      url='http://someurl', vid='v1')

        yield self.fwq.put(req, duration=50.3, async=True)
        self.assertFalse(self.mock_send_request.called)

        item = self.fwq.get()
        self.assertEqual(item.duration, 50.3)
    
class TestVideoServer(test_utils.neontest.AsyncHTTPTestCase):
    ''' Video Server test'''

    @classmethod
    def setUpClass(cls):
        super(TestVideoServer, cls).setUpClass()
   
    def setUp(self):
        self.server = video_processor.server.Server()
        super(TestVideoServer, self).setUp()
        
        statemon.state._reset_values()

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
        self.na = neondata.NeonPlatform.modify(self.api_key, '0',
                                               lambda x: x,
                                               create_missing=True)

        # Patch the video length lookup
        self.http_patcher = patch('video_processor.server.utils.http')
        self.mock_http = self._future_wrap_mock(
            self.http_patcher.start().send_request)
        vsize = 1024
        request = tornado.httpclient.HTTPRequest("http://xyz")
        response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO(''), headers={'Content-Length': vsize})
        self.mock_http.side_effect = \
                lambda x, **kw: response

        # Mock out the image download
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.im_download_mock = self.im_download_mocker.start()
        self.random_image = PILImageUtils.create_random_image(480, 640)
        image_future = concurrent.futures.Future()
        image_future.set_result(self.random_image)
        self.im_download_mock.return_value = image_future

        # Mock out cloudinary
        self.cloudinary_patcher = patch('cmsdb.cdnhosting.CloudinaryHosting')
        self.cloudinary_mock = self.cloudinary_patcher.start()
        future = concurrent.futures.Future()
        future.set_result(None)
        self.cloudinary_mock().upload.return_value = future

        # Mock out s3
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('host-thumbnails')
        self.s3conn.create_bucket('n3.neon-images.com')

    def get_app(self):
        return self.server.application
    
    def tearDown(self):
        self.http_patcher.stop()
        self.im_download_mocker.stop()
        self.cloudinary_patcher.stop()
        self.s3_patcher.stop()
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
                "video_id": video_id ,
                "topn":2, 
                "callback_url": "http://callback_push_url", 
                "video_title": "test_title",
                "default_thumbnail" : "http://default_thumb.jpg"
                }
        resp = self.make_api_request(vals)
        return resp

    def test_neon_api_request(self):
        resp = self.add_request("neonapivid123")
        self.assertEqual(resp.code, 201)
        
        # verify request saved in DB
        job_id = json.loads(resp.body)['job_id']
        api_request = neondata.NeonApiRequest.get(job_id, self.api_key)
        self.assertEqual(api_request.video_id, "neonapivid123")
        
        # verify that the video has been added to the account
        np = neondata.NeonPlatform.get(self.api_key, '0')
        vids = np.get_videos()
        self.assertIn("neonapivid123", vids)

        # Verify that the default thumb is in the database
        video = neondata.VideoMetadata.get('%s_neonapivid123' % self.api_key)
        self.assertIsNotNone(video)
        self.assertEquals(len(video.thumbnail_ids), 1)
        self.assertTrue(video.serving_enabled)
        thumb = neondata.ThumbnailMetadata.get(video.thumbnail_ids[0])
        self.assertEquals(thumb.type, neondata.ThumbnailType.DEFAULT)
        self.assertEquals(thumb.rank, 0)

    def test_no_default_thumb(self):
        vals = {
           "api_key": self.api_key, 
           "video_url": "http://testurl/video.mp4", 
           "video_id": 'neonapivid123',
           "topn":2, 
           "callback_url": "http://callback_push_url", 
           "video_title": "test_title",
            }
        response = self.make_api_request(vals)
        self.assertEquals(response.code, 201)

        # Verify that there is a video entry in the database but it
        # won't serve anything.
        video = neondata.VideoMetadata.get('%s_neonapivid123' % self.api_key)
        self.assertIsNotNone(video)
        self.assertEquals(len(video.thumbnail_ids), 0)
        self.assertFalse(video.serving_enabled)
        self.assertEquals(video.url, 'http://testurl/video.mp4')
        self.assertEquals(video.integration_id, self.na.integration_id)
        self.assertEquals(video.job_id, json.loads(response.body)['job_id'])
    

    def test_broken_default_thumb(self):
        vals = {
           "api_key": self.api_key, 
           "video_url": "http://testurl/video.mp4", 
           "video_id": '', 
           "topn":2, 
           "callback_url": "http://callback_push_url", 
           "video_title": "test_title",
           "default_thumbnail": "http://broken_image",
            }
        
        return_values = [IOError, HTTPError(404), HTTPError(500)]
        N = len(return_values)
        
        def _image_exception(*args, **kwargs):
            raise return_values.pop(0) 
        self.im_download_mock.side_effect = _image_exception 
        for i in range(N):
            vid = "neonapivid123%s" % i 
            vals['video_url'] = "http://testurl%s/video.mp4"  %vid
            vals['video_id'] = vid 
            response = self.make_api_request(vals)
            self.assertEquals(response.code, 201)
            # Check video entry in DB 
            video = neondata.VideoMetadata.get('%s_%s' % (self.api_key, vid))
            self.assertIsNotNone(video)

            # Check request state and message
            resp = json.loads(response.body)
            api_request = neondata.NeonApiRequest.get(resp['job_id'],
                                                      self.api_key)
            self.assertEqual(api_request.state, neondata.RequestState.SUBMIT)
            

            self.assertEqual(
                statemon.state.get('video_processor.server.default_thumb_error'),
                1)
            
            statemon.state._reset_values()

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

    def test_video_object_exists(self):
        internal_video_id = \
          neondata.InternalVideoID.generate(self.api_key, 'vid1')
        neondata.VideoMetadata(
            internal_video_id,
            video_url="http://testurl/video.mp4",
            i_id='iid2',
            serving_enabled=False,
            duration=12345.6,
            custom_data={'some_key': 'some_fun_data'},
            publish_date='2015-07-02T13:09:00Z').save()
        
        vals = {
            "api_key": self.api_key, 
            "video_url": "http://testurl/video.mp4", 
            "video_id": 'vid1',
            "topn":2, 
            "callback_url": "http://callback_push_url", 
            "video_title": "test_title",
            "integration_id" : 'iid2',
            "publish_date": '2015-07-02T13:10:00Z'
            }
        resp = self.make_api_request(vals)
        self.assertEqual(resp.code, 201)
        job_id = json.loads(resp.body)['job_id']
        self.assertIsNotNone(job_id)

        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEqual(video.integration_id, 'iid2')
        self.assertEqual(video.url, "http://testurl/video.mp4")
        self.assertEqual(video.custom_data, {'some_key': 'some_fun_data'})
        self.assertEqual(video.duration, 12345.6)
        self.assertEqual(video.publish_date, '2015-07-02T13:10:00Z')
        self.assertFalse(video.serving_enabled)
        self.assertEqual(video.job_id, job_id)
        self.assertEqual(video.thumbnail_ids, [])

        job = neondata.NeonApiRequest.get(video.job_id, self.api_key)
        self.assertEqual(job.integration_id, 'iid2')
        self.assertEqual(job.callback_url, 'http://callback_push_url')
        self.assertEqual(job.video_title, 'test_title')
        self.assertEqual(job.publish_date, '2015-07-02T13:10:00Z')

    def test_default_thubmnail(self):
        internal_video_id = \
          neondata.InternalVideoID.generate(self.api_key, 'vid1')
        vals = {
            "api_key": self.api_key, 
            "video_url": "http://testurl/video.mp4", 
            "video_id": 'vid1',
            "topn":2, 
            "callback_url": "http://callback_push_url", 
            "video_title": "test_title",
            "integration_id" : 'iid2',
            "default_thumbnail" : 'default_thumb.jpg',
            "external_thumbnail_id" : 'ext_thumb_id'
            }
        resp = self.make_api_request(vals)
        self.assertEqual(resp.code, 201)
        job_id = json.loads(resp.body)['job_id']
        self.assertIsNotNone(job_id)

        video = neondata.VideoMetadata.get(internal_video_id)
        thumb = neondata.ThumbnailMetadata.get(video.thumbnail_ids[0])
        self.assertEquals(thumb.type, neondata.ThumbnailType.DEFAULT)
        self.assertEquals(thumb.external_id, 'ext_thumb_id')
        self.assertIn('default_thumb.jpg', thumb.urls)
        

    def test_brightcove_request(self):

        i_id = "i125"
        bp = neondata.BrightcovePlatform.modify(
            self.api_key, i_id,
            lambda x: x, create_missing=True)

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
        
        job_id = json.loads(resp.body)['job_id']
        api_request = neondata.NeonApiRequest.get(job_id, self.api_key)
        self.assertEqual(api_request.video_id, "testid123")

        # Check that the default thumbnail is set
        video = neondata.VideoMetadata.get('%s_testid123' % self.api_key)
        self.assertIsNotNone(video)
        self.assertEquals(len(video.thumbnail_ids), 1)
        self.assertTrue(video.serving_enabled)
        thumb = neondata.ThumbnailMetadata.get(video.thumbnail_ids[0])
        self.assertEquals(thumb.type, neondata.ThumbnailType.BRIGHTCOVE)
        self.assertEquals(thumb.rank, 0)
        self.assertEquals(thumb.urls[-1], 'http://prev_thumb')
        
    
    def test_brightcove_request_invalid(self):

        i_id = "i125"
        bp = neondata.BrightcovePlatform.modify(
            self.api_key, i_id,
            lambda x: x, create_missing=True)
        vals = {"api_key": self.api_key, 
                "video_url": "http://testurl/video.mp4", 
                "video_id": "testid123", "topn":2, 
                "callback_url": "http://callback_push_url"}
        url = self.get_url('/api/v1/submitvideo/brightcove')
        resp = self.make_api_request(vals, url)
        self.assertEqual(resp.code, 400)

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

    @tornado.testing.gen_test
    def test_jobs_handler(self):
        req = neondata.NeonApiRequest('job21', self.api_key,
                    'vid1', 't', 't', 'r', 'h')
        req.save()
        jdata = req.to_json()
        resp = yield self.http_client.fetch(self.get_url('/job'),
                                            method="POST", body=jdata)
        self.assertEquals(resp.code, 200)

    @tornado.testing.gen_test
    def test_requeue_handler(self):
        req = neondata.NeonApiRequest('job21', self.api_key,
                    'vid1', 't', 't', 'r', 'h')
        req.save()
        jdata = req.to_json()
        resp = yield self.http_client.fetch(self.get_url('/requeue'),
                                            method="POST", body=jdata)
        self.assertEqual(resp.code, 200)
        self.assertEqual(
            neondata.NeonApiRequest.get('job21', self.api_key).state,
            neondata.RequestState.REQUEUED)
        # Make sure when we dequeue, the request is there
        deq_resp = yield self.http_client.fetch(
            self.get_url('/dequeue'),
            method='GET',
            headers={'X-Neon-Auth' : 'secret_token'})
        self.assertEquals(deq_resp.code, 200)
        job = json.loads(deq_resp.body)
        self.assertEquals(job['api_key'], self.api_key)
        self.assertEquals(job['job_id'], 'job21')

    @tornado.testing.gen_test
    def test_reprocess_handler(self):
        req = neondata.NeonApiRequest('job21', self.api_key,
                    'vid1', 't', 't', 'r', 'h')
        req.save()
        jdata = req.to_json()
        resp = yield self.http_client.fetch(self.get_url('/reprocess'),
                                            method="POST", body=jdata)
        self.assertEqual(resp.code, 200)
        self.assertEqual(
            neondata.NeonApiRequest.get('job21', self.api_key).state,
            neondata.RequestState.REPROCESS)
        # Make sure when we dequeue, the request is there
        deq_resp = yield self.http_client.fetch(
            self.get_url('/dequeue'),
            method='GET',
            headers={'X-Neon-Auth' : 'secret_token'})
        self.assertEquals(deq_resp.code, 200)
        job = json.loads(deq_resp.body)
        self.assertEquals(job['api_key'], self.api_key)
        self.assertEquals(job['job_id'], 'job21')

    def test_requeue_invalid_request(self):
        self.http_client.fetch(self.get_url('/requeue'),
                               callback=self.stop, method="POST", body='{}')
        resp = self.wait()
        self.assertEqual(resp.code, 400)

    def test_requeue_handler_duplicate(self):
        ''' requeue handler '''
        resp = self.add_request()

        # on requeue, we use the json neonapirequest to requeue
        jid = json.loads(resp.body)["job_id"]
        api_request = neondata.NeonApiRequest.get(jid, self.api_key)
        jdata = api_request.to_json()
        self.http_client.fetch(self.get_url('/requeue'),
                callback=self.stop, method="POST", body=jdata)
        resp = self.wait()
        self.assertEqual(resp.code, 409)

    def test_request_without_callback_url(self):
        vals = {"api_key": self.api_key, 
                    "video_url": "http://testurl/video.mp4", 
                    "video_id": "vid1" , "topn":2, 
                    "video_title": "test_title"}
        resp = self.make_api_request(vals)
        self.assertEqual(resp.code, 201)

    def test_requeue_failed_job(self):
        req = neondata.NeonApiRequest('job21', self.api_key)
        req.fail_count = 99
        req.state = neondata.RequestState.FAILED
        req.save()
        with self.assertLogExists(logging.ERROR, 'Failed processing job'):
          resp = self.fetch('/requeue',
                            method="POST",
                            body=req.to_json())
        self.assertEquals(resp.code, 409)

        # Make sure there is nothing in the queue
        deq_resp = self.fetch(
          '/dequeue',
          method='GET',
          headers={'X-Neon-Auth' : 'secret_token'})
        self.assertEquals(deq_resp.code, 200)
        self.assertEquals(deq_resp.body, '{}')
        

    def test_healthcheck(self):
        ''' Health check handler of server '''

        nuser = neondata.NeonUserAccount("acc1")
        nuser.save()
        with options._set_bounded('video_processor.server.test_key', nuser.neon_api_key):
            self.http_client.fetch(self.get_url('/healthcheck'),
                    callback=self.stop, method="GET", headers={})
            resp = self.wait()
            self.assertEqual(resp.code, 200)

        # shut down redis and ensure you get a 503 from healthcheck fail
        self.redis.stop()
        self.http_client.fetch(self.get_url('/healthcheck'),
                callback=self.stop, method="GET", headers={})
        resp = self.wait()
        self.assertEqual(resp.code, 503)
       
    def test_statshandler(self):
        self.http_client.fetch(self.get_url('/stats'),
                callback=self.stop, method="GET", headers={})
        resp = self.wait()
        self.assertEqual(json.loads(resp.body)["size"], 0)
        self.assertEqual(resp.code, 200)

class QueueSmokeTest(test_utils.neontest.TestCase):
    def setUp(self):
        super(QueueSmokeTest, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
        random.seed(234895)
        
        self.fwq = video_processor.server.FairWeightedRequestQueue(nqueues=2)
        self.nuser1 = neondata.NeonUserAccount("acc1")
        self.nuser1.save()
        self.nuser2 = neondata.NeonUserAccount("acc2")
        self.nuser2.set_processing_priority(0)
        self.nuser2.save()

        # Patch the video length lookup
        self.send_request_patcher = patch('video_processor.server.utils.http.send_request')
        self.mock_send_request = self._future_wrap_mock(
            self.send_request_patcher.start())
        self.video_size = MagicMock()
        self.video_size.return_value = 10.0
        self.mock_send_request.side_effect = \
            lambda x, **kw: tornado.httpclient.HTTPResponse(
                x, 200, buffer=StringIO(''),
                headers={'Content-Length': self.video_size()})

    def tearDown(self):
        self.send_request_patcher.stop()
        self.redis.stop()
        super(QueueSmokeTest, self).tearDown()

    def test_many_puts_gets(self):

        # producer insert requests
        # thread to DQ requests
        # verify DQ'ed item format to match with client
        # make sure you can fetch counters?

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
                    items_get.append(json.dumps(item.api_request.__dict__))

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

        items_put_json = [json.dumps(r.__dict__) for r in items_put]
        for i in range(len(items_put) - len(items_get)):
            items_get.append(self.fwq.get())
        
        self.assertSetEqual(set(items_get), set(items_put_json))
      
        # verify that the add metadata thread ran and we were able
        # to collect some data on size of Q in # of bytes 
        state_vars = video_processor.server.statemon.state.get_all_variables()
        qsize = state_vars.get('video_processor.server.queue_size_bytes').value
        self.assertGreater(qsize, 0)

class TestJobManager(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestJobManager, self).setUp()

        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
        random.seed(1324)

        # Patch the video length lookup
        self.send_request_patcher = patch('video_processor.server.utils.http.send_request')
        self.mock_send_request = self._future_wrap_mock(
            self.send_request_patcher.start())
        self.video_size = MagicMock()
        self.video_size.return_value = 10.0
        self.mock_send_request.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, 200, buffer=StringIO(''),
              headers={'Content-Length': self.video_size()})

        #create test account
        a_id = "testaccountneonapi"
        self.nuser = neondata.NeonUserAccount(a_id)
        self.nuser.save()
        self.api_key = self.nuser.neon_api_key
        self.na = neondata.NeonPlatform.modify(
            self.api_key, '0',
            lambda x: x, create_missing=True)

        # Make some default jobs
        self.jobs = [
            neondata.NeonApiRequest('job0', self.api_key, vid='vid1',
                                    url='http://somewhere.mp4')]
        neondata.NeonApiRequest.save_all(self.jobs)

        self.base_time = 0.05
        self.job_manager = video_processor.server.JobManager(
            job_check_interval=self.base_time / 10,
            base_time=self.base_time)

    def tearDown(self):
        self.send_request_patcher.stop()
        self.redis.stop()
        super(TestJobManager, self).tearDown()

    def run_loop(self, timeout):
        '''Yield to run the ioloop for a specified amount of time'''
        future = concurrent.futures.Future()
        self.io_loop.call_later(timeout, future.set_result, None)
        return future

    @tornado.testing.gen_test
    def test_job_with_duration(self):
        neondata.VideoMetadata('%s_vid1' % self.api_key,
                               duration=50.3).save()

        #TODO(mdesnoyer): Test the time calculation
        yield self.job_manager.add_job(self.jobs[0])
        job = self.job_manager.get_job()
        self.assertIsNotNone(job)

    @tornado.testing.gen_test
    def test_job_times_out(self):
        yield self.job_manager.add_job(self.jobs[0])
        job = self.job_manager.get_job()
        self.assertIsNotNone(job)

        with self.assertLogExists(logging.ERROR, 'timed out'):
            yield self.run_loop(self.base_time / 2.0)

        # At this point, the job shouldn't be requeued, but it
        # should be flagged in the database
        request = neondata.NeonApiRequest.get(job.api_request.job_id,
                                              job.api_request.api_key)
        self.assertEquals(request.fail_count, 1)
        self.assertEquals(request.state, neondata.RequestState.INT_ERROR)
        self.assertEquals(len(self.job_manager.running_jobs), 0)
        self.assertEquals(self.job_manager.q.qsize(), 0)

        # Now wait and it should be requeued
        with self.assertLogExists(logging.INFO, 'Add job'):
            yield self.run_loop(self.base_time * 3.0)

        self.assertEquals(self.job_manager.q.qsize(), 1)
        self.assertEquals(
            neondata.NeonApiRequest.get(
                job.api_request.job_id, job.api_request.api_key).state,
            neondata.RequestState.REQUEUED)

    @tornado.testing.gen_test
    def test_job_fails(self):
        yield self.job_manager.add_job(self.jobs[0])
        job = self.job_manager.get_job()

        job.api_request.state = neondata.RequestState.FAILED
        job.api_request.fail_count = 1
        job.api_request.save()

        # Now wait and it should be requeued
        with self.assertLogExists(logging.INFO, 'Add job'):
            yield self.run_loop(self.base_time * 10.0)

        self.assertEquals(
            neondata.NeonApiRequest.get(
                job.api_request.job_id, job.api_request.api_key).state,
            neondata.RequestState.REQUEUED)

    @tornado.testing.gen_test
    def test_job_times_out_too_much(self):
        self.video_size.side_effect = [
            10.0 for x in range(options.get('video_processor.server.max_retries'))]
        
        yield self.job_manager.add_job(self.jobs[0])

        for i in range(options.get('video_processor.server.max_retries')-1):
            job = self.job_manager.get_job()
            self.assertIsNotNone(job)
            yield self.run_loop(self.base_time * ((1<<i) + 1))
            request = neondata.NeonApiRequest.get(job.api_request.job_id,
                                                  job.api_request.api_key)
            self.assertEquals(request.state,
                              neondata.RequestState.REQUEUED)
            self.assertEquals(request.fail_count, i+1)

        with self.assertLogExists(logging.ERROR,
                                  'Failed processing .* too many'):
            job = self.job_manager.get_job()
            yield self.run_loop(self.base_time * 5.0)

        request = neondata.NeonApiRequest.get(job.api_request.job_id,
                                              job.api_request.api_key)
        self.assertEquals(request.fail_count, 3)
        self.assertEquals(request.state, neondata.RequestState.INT_ERROR)
        self.assertEquals(len(self.job_manager.running_jobs), 0)
        self.assertEquals(self.job_manager.q.qsize(), 0)

    @tornado.testing.gen_test
    def test_job_finishes(self):
        # A size of 0 gives the default timeout
        self.video_size.side_effect = [0]
        yield self.job_manager.add_job(self.jobs[0])

        job = self.job_manager.get_job()

        # Make sure jobs are kept around if they do not timeout or finish
        self.assertEquals(len(self.job_manager.running_jobs), 1)
        yield self.job_manager.check_running_jobs()
        self.assertEquals(len(self.job_manager.running_jobs), 1)

        # Now finish the job
        job.api_request.state = neondata.RequestState.FINISHED
        job.api_request.save()
        yield self.job_manager.check_running_jobs()
        self.assertEquals(len(self.job_manager.running_jobs), 0)
        self.assertEquals(self.job_manager.q.qsize(), 0)

    @tornado.testing.gen_test
    def test_requeue_pending_jobs(self):
        self.maxDiff = None
        for i in range(1, 13):
            self.jobs.append(
                neondata.NeonApiRequest('job%i' % i, self.api_key))
        self.jobs[0].state = neondata.RequestState.SUBMIT
        self.jobs[1].state = neondata.RequestState.PROCESSING
        self.jobs[2].state = neondata.RequestState.FINALIZING
        self.jobs[3].state = neondata.RequestState.REQUEUED
        self.jobs[4].state = neondata.RequestState.REPROCESS
        self.jobs[5].state = neondata.RequestState.FAILED
        self.jobs[5].fail_count = 1
        self.jobs[6].state = neondata.RequestState.INT_ERROR
        self.jobs[6].fail_count = 1
        self.jobs[7].state = neondata.RequestState.FAILED
        self.jobs[7].fail_count = 99
        self.jobs[8].state = neondata.RequestState.INT_ERROR
        self.jobs[8].fail_count = 99
        self.jobs[9].state = neondata.RequestState.FINISHED
        self.jobs[10].state = neondata.RequestState.SERVING
        self.jobs[11].state = neondata.RequestState.ACTIVE
        self.jobs[12].state = neondata.RequestState.SERVING_AND_ACTIVE
        neondata.NeonApiRequest.save_all(self.jobs)

        yield self.job_manager.requeue_all_pending_jobs()
        jobs_found = []
        while True:
            j = self.job_manager.get_job()
            if j is None:
                break
            self.assertIn(j.api_request.state,
                          [neondata.RequestState.REQUEUED,
                           neondata.RequestState.REPROCESS])
            jobs_found.append(j)
        self.assertItemsEqual([x.api_request.job_id for x in jobs_found],
                              ['job%i' % i for i in range(7)])
         
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
