#!/usr/bin/env python

'''
Video processing client unit test

NOTE: Model call has been mocked out, the results are embedded in the 
pickle file for the calls made from model object

1. Neon api request
2. Brightcoev api request 
3. Brightcove api request with autosync

Inject failures
- error downloading video file
- error with video file
- error with few thumbnails
- error with client callback

'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)

from api import client
import json
import logging
import mock
import multiprocessing
import os
import pickle
import random
import request_template
import signal
import subprocess
import time
import tempfile
import test_utils
import test_utils.mock_boto_s3 as boto_mock
import test_utils.net
import test_utils.redis
import tornado
import unittest
import utils
import utils.ps

from boto.s3.connection import S3Connection
from mock import patch
from mock import MagicMock
from PIL import Image
from supportServices import neondata
from StringIO import StringIO
from utils.options import options
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from tornado.concurrent import Future
from tornado.testing import AsyncHTTPTestCase,AsyncTestCase,AsyncHTTPClient
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError

_log = logging.getLogger(__name__)

class TestVideoClient(unittest.TestCase):
    ''' 
    Test Video Processing client
    '''
    def setUp(self):
        #setup properties,model
        self.model_file = os.path.join(os.path.dirname(__file__), "model.pkl")
        self.model_version = "test" 
        self.model = MagicMock()
        self.ioloop = MagicMock() #mock ioloop

        #Mock Model methods, use pkl to load captured outputs
        ct_output, ft_output = pickle.load(open(self.model_file)) 
        self.model.choose_thumbnails.return_value = (ct_output, 9)
        self.model.filter_duplicates.return_value = ft_output 
        self.model.score.return_value = 1, 2 
        self.test_video_file = os.path.join(os.path.dirname(__file__), 
                                "test.mp4") 
   
        self.dl = None
        self.pv = None

        #Redis
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

        #mock s3
        self.s3patcher = patch('api.client.S3Connection')
        mock_conn = self.s3patcher.start()
        self.s3conn = boto_mock.MockConnection()
        mock_conn.return_value = self.s3conn
       
        #setup process video object
        self.napi_request = None
        self.processvideo_setup()

    #ProcessVideo setup
    def processvideo_setup(self):
        '''
        Setup variables for process video process_all call

        Then run process_all call, which has calls to the model
        mocked out.

        Once process_all has extracted thumbs, scored them subsequent
        methods can do their opertaions depending on the api call type
        '''
        j_id = "j123"
        api_key = "apikey123"
        vid = "video1"
        jparams = request_template.neon_api_request %(
                j_id, vid, api_key, "neon", api_key, j_id)
        params = json.loads(jparams)
        self.dl = client.HttpDownload(jparams, self.ioloop, 
                self.model, self.model_version)
        self.pv = client.ProcessVideo(params, jparams, 
                self.model, self.model_version, False, 123)
        self.dl.pv = self.pv
        nthumbs = params['api_param']
        self.pv.process_all(self.test_video_file, nthumbs)
        self.dl.pv.center_frame = Image.new("RGB",(360, 480))
        self.pv.center_frame = Image.new("RGB",(360, 480))
        self.napi_request = neondata.NeonApiRequest(j_id, api_key, vid, "title",
                            None, None, None)
        self.napi_request.save()
        

    def tearDown(self):
        self.s3patcher.stop()
        self.redis.stop()

    def _create_random_image(self):
        ''' Image data as string '''
        h = 360
        w = 480
        pixels = [(0, 0, 0) for _w in range(h*w)]
        r = random.randrange(0, 255)
        g = random.randrange(0, 255)
        b = random.randrange(0, 255)
        pixels[0] = (r, g, b)
        im = Image.new("RGB", (h, w))
        im.putdata(pixels)
        imgstream = StringIO()
        im.save(imgstream, "jpeg", quality=100)
        imgstream.seek(0)
        data = imgstream.read()
        return imgstream

    def test_process_all(self):
       
        '''
        Verify execution of the process_all call in ProcessVideo
        '''
        
        #verify metadata has been populated
        self.assertEqual(self.pv.video_metadata['codec_name'], 'avc1')
        self.assertEqual(self.pv.video_metadata['duration'], 1980)
        self.assertEqual(self.pv.video_metadata['framerate'], 15)
        self.assertEqual(self.pv.video_metadata['frame_size'], (400, 264))
        self.assertIsNone(self.pv.video_metadata['bitrate'])
       
        #verify that following maps get populated
        self.assertGreater(len(self.pv.data_map), 0)
        self.assertGreater(len(self.pv.attr_map), 0)
        self.assertGreater(len(self.pv.timecodes), 0)
        self.assertNotIn(float('-inf'), self.pv.valence_scores[1])
        
    @patch('api.client.S3Connection')
    def test_save_data_to_s3(self, mock_conntype):

        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('neon-beta-test')
        conn.create_bucket('host-thumbnails')
        mock_conntype.return_value = conn
        
        self.dl.send_client_response()
        
        #save data to s3
        self.pv.save_data_to_s3()
        s3_keys = [x for x in conn.buckets['neon-beta-test'].get_all_keys()]
        self.assertEqual(len(s3_keys), 1)

    @patch('api.client.S3Connection')
    @patch('api.client.tornado.httpclient.HTTPClient')
    @patch('api.client.tornado.httpclient.AsyncHTTPClient')
    @patch('api.client.neondata.BrightcoveApiRequest')
    def test_brightcove_request_process(self, mock_bplatform_patcher, 
                                async_patcher, http_patcher, mock_conntype):
        
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-beta-test')
        #print mock_conntype, http_patcher, mock_bplatform_patcher
        mock_conntype.return_value = conn
        
        # TEST Brightcove request flow and finalize_brightcove_request() 
        # Replace the request parameters of the dl & pv objects to save time on
        # video processing and reuse the setup
        a_id = "testaccountneonapi"
        vid  = "vid123"
        i_id = "i123"
        nuser = neondata.NeonUserAccount(a_id)
        nuser.save()
        bp = neondata.BrightcovePlatform(a_id, i_id, 
                nuser.neon_api_key)
        api_key = bp.neon_api_key
        bp.save()

        jparams = request_template.brightcove_api_request %("j", vid, api_key,
                            "brightcove", api_key, "j", i_id)
        params = json.loads(jparams)
        self.pv.request_map = params
        self.pv.request = jparams
        self.dl.job_params = params
        
        #brightcove platform patcher
        breq = neondata.BrightcoveApiRequest("d", "d", None, None, None,
                                        None, None, None)
        breq.previous_thumbnail = "http://prevthumb"
        mock_bplatform_patcher.get.side_effect = [breq]
       
        #mock tornado http
        request = HTTPRequest('http://google.com')
        response = HTTPResponse(request, 200, buffer=self._create_random_image())
        notification_response = HTTPResponse(request, 200, buffer=StringIO(""))
        http_patcher().fetch.side_effect = [response, response, notification_response]
        async_patcher().fetch.side_effect = [response, response, notification_response]
        
        self.dl.send_client_response()
        bcove_thumb = False
        
        for key in conn.buckets['host-thumbnails'].get_all_keys():
            if "brightcove" in key.name:
                bcove_thumb = True  
        
        self.assertTrue(bcove_thumb)        
        
        #verify thumbnail metadata and video metadata 
        vm = neondata.VideoMetadata.get(api_key + "_" + vid)
        self.assertIsNotNone(vm)
        
        #TODO: Brightcove request with autosync
    
    @patch('api.client.S3Connection')
    @patch('api.client.tornado.httpclient.HTTPClient')
    @patch('api.client.tornado.httpclient.AsyncHTTPClient')
    def test_httpdownload_async_callback_no_data(self, async_patcher, 
                                    http_patcher, mock_conntype):
        ''' test streaming callback and async callback '''

        j_id = "j123"
        api_key = "apikey123"
        jparams = request_template.neon_api_request %(
                j_id, "v", api_key, "neon", api_key, j_id)
        params = json.loads(jparams)
        self.dl = client.HttpDownload(jparams, self.ioloop, 
                self.model, self.model_version)
        request = HTTPRequest('http://neon-lab.com/video.mp4')
        data = ""
        #return no data
        response = HTTPResponse(request, 200, buffer=StringIO(data))
        rq_response = HTTPResponse(request, 500, buffer=StringIO(data))
        
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-beta-test')
        
        http_patcher().fetch.side_effect = [rq_response]
        async_patcher().fetch.side_effect = [response]
        self.dl.async_callback(response)

        #Assert error response
        api_request = neondata.NeonApiRequest.get(api_key, j_id)
        self.assertEqual(api_request.state, neondata.RequestState.INT_ERROR)


    @patch('api.client.tornado.httpclient.HTTPClient')
    def test_httpdownload_async_callback_error_cases(self, http_patcher):
        ''' Failed on async callback '''

        j_id = "j123"
        api_key = "apikey123"
        jparams = request_template.neon_api_request %(
                j_id, "v", api_key, "neon", api_key, j_id)
        params = json.loads(jparams)
        self.dl = client.HttpDownload(jparams, self.ioloop, 
                self.model, self.model_version)
        request = HTTPRequest('http://neon-lab.com/video.mp4')
        data = "somefakevideodata"
        
        #E1. No content length header in response
        response = HTTPResponse(request, 500, buffer=StringIO(data))
        
        #mock requeue job, set requeue job to fail so that error propogates up
        http_patcher().fetch.side_effect = [response] *10
        
        self.dl.async_callback(response)
        api_request = neondata.NeonApiRequest.get(api_key, j_id)
        self.assertEqual(api_request.state, neondata.RequestState.INT_ERROR)

        #E2. Http Response error
        response = HTTPResponse(request, 500, buffer=StringIO(data),
                    headers={'Content-Length':len(data)})
        self.dl.async_callback(response)
        api_request = neondata.NeonApiRequest.get(api_key, j_id)
        
        self.assertEqual(api_request.state, neondata.RequestState.INT_ERROR)

    def test_streaming_callback(self):
    
        '''
        Tornado streaming callback
        '''

        j_id = "j123"
        api_key = "apikey123"
        jparams = request_template.neon_api_request %(j_id, "v",
                api_key, "neon", api_key, j_id)
        params = json.loads(jparams)
        self.dl = client.HttpDownload(jparams, self.ioloop, 
                self.model, self.model_version)
        data = "somefakevideodata"
        self.dl.callback_data_size = len(data) -1 #change callback datasize
        '''process_all method would return an error coz data is not valid 
            video, but we are only testing the streaming callback method here
        '''
        self.dl.streaming_callback(data)
        self.assertFalse(self.dl.tempfile.close_called)
        #verify size of temp file

    #def test_thumbnail_ordering(self):
    #    ''' Test that thumbnails are still sorted and ordere
    #        after filtering '''
    #    res = self.pv.get_topn_thumbnails(5)
    #    ranked_frames = [x[0] for x in res] 
    #    #TODO: Generate data for order check
    
    
    @patch('api.client.S3Connection')
    def test_neon_request_process(self, mock_conntype):
        ''' test processing a neon api request''' 
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-beta-test')
        
        vid  = self.napi_request.video_id 
        api_key = self.napi_request.api_key 
        job_id = self.napi_request.job_id 

        jparams = ('{"api_key": "%s", "video_url": "http://bunny.mp4",'
                    '"video_id": "%s", "topn": 3, "callback_url": "http://callback",'
                    '"video_title": "testtitle3", "request_type":"neon",'
                    '"state":"submit", "job_id":"%s", "api_method":"topn",' 
                    '"api_param": 3}'%(api_key, vid, job_id)) 
        params = json.loads(jparams)
        self.pv.request_map = params
        self.pv.request = jparams
        self.dl.job_params = params
        
        self.dl.send_client_response()
        s3_keys = [x.name for x in conn.buckets['host-thumbnails'].get_all_keys()]
        self.assertIn( "%s/%s/centerframe.jpeg"%(api_key,job_id), s3_keys)
        for i in range(len(s3_keys) -1):
            key = "%s/%s/neon%s.jpeg"%(api_key, job_id, i)
            self.assertIn(key, s3_keys)

        #check api request state
        japi_request = neondata.NeonApiRequest.get_request(api_key, job_id)
        api_request = neondata.NeonApiRequest.create(japi_request)
        self.assertEqual(api_request.state, neondata.RequestState.FINISHED)

        #check thumbnail ids and videometadata
        vm = neondata.VideoMetadata.get(neondata.InternalVideoID.generate(api_key, vid))
        tids = vm.thumbnail_ids
        thumb_mappings = neondata.ThumbnailMetadata.get_many(tids)
        self.assertNotIn(None, thumb_mappings)
       
        s3prefix = "https://host-thumbnails.s3.amazonaws.com/"
        for k in s3_keys:
            key = s3prefix + k
            self.assertIsNotNone(neondata.ThumbnailURLMapper.get_id(key))

    #TODO: test intermittent DB/ processing failure cases

    #TODO: autosync enabled video processing
   
    #TODO: test client response formatting
   
'''
#Disable this test for now. The test is broken and needs
to be fixed 

class TestVideoClientAndServerIntegration(AsyncHTTPTestCase):
    #NOTE: Need to start server using subprocess and can't use tornado 
    #asynctestcase because we can't bind the client and redis to the same
    #port 
    def setUp(self):
        super(TestVideoClientAndServerIntegration,self).setUp()
        self.model_patcher = patch('api.client.model')
        self.model = self.model_patcher.start()
        self.model().load_model = [None]
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        random.seed(2000)
        
        #create test neon account
        a_id = "testaccountneonapi"
        self.nuser = neondata.NeonUserAccount(a_id)
        self.nuser.save()
        self.api_key = self.nuser.neon_api_key
        self.na = neondata.NeonPlatform(a_id, self.api_key)
        self.na.save()

    def tearDown(self):
        self.model_patcher.stop()
        self.redis.stop()
        super(TestVideoClientAndServerIntegration, self).tearDown()

    def get_app(self):
        return server.application

    def test_dequeue_video_client(self):
        model_file = "modelfile.model" 
        vc = client.VideoClient(model_file)
        vc.dequeue_url = self.get_url('/dequeue')
        vc.dequeue_job(callback=self.stop)
        res = self.wait()
        self.assertEqual(res, "{}") #empty queue result

        params = {"api_key": self.api_key, 
                   "video_url": "http://bunny.mp4",
                   "video_id": "testid124",
                   "topn": 3,
                   "callback_url": "http://localhost:8081/testcallback", 
                   "video_title": "testtitle"}
    
        #submit a job
        resp = self.fetch('/api/v1/submitvideo/topn', method="POST", body=json.dumps(params))
        self.assertEqual(resp.code, 201)
        vc.dequeue_job(callback=self.stop)
        res = self.wait()
        self.assertNotEqual(res, "{}") 
'''
if __name__ == '__main__':
    unittest.main()
