#!/usr/bin/env python

'''
Video processing client unit test

NOTE: Model call has been mocked out, the results are embedded in the 
pickle file for the calls made from model object

#TODO:
    Identify all the errors cases and inject them to be tested
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)

import api.client
import api.cdnhosting
from boto.s3.connection import S3Connection
import json
import logging
from mock import MagicMock, patch
import multiprocessing
import numpy as np
import os
import pickle
from PIL import Image
import random
import request_template
import signal
import socket
from StringIO import StringIO
import subprocess
from supportServices import neondata
import time
import tempfile
import test_utils
import test_utils.mock_boto_s3 as boto_mock
import test_utils.neontest
import test_utils.net
import test_utils.redis
from tornado.concurrent import Future
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase,AsyncTestCase,AsyncHTTPClient
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import urllib
import urllib2
import unittest
import utils
from utils.imageutils import PILImageUtils
import utils.ps

_log = logging.getLogger(__name__)

TMD = neondata.ThumbnailMetadata

class TestVideoClient(test_utils.neontest.TestCase):
    ''' 
    Test Video Processing client
    '''
    def setUp(self):
        super(TestVideoClient, self).setUp()
        
        #setup properties,model
        self.model_file = os.path.join(os.path.dirname(__file__), "model.pkl")
        self.model_version = "test" 
        self.model = MagicMock()

        #Mock Model methods, use pkl to load captured outputs
        ct_output, ft_output = pickle.load(open(self.model_file)) 
        self.model.choose_thumbnails.return_value = (ct_output, 9)
        self.model.filter_duplicates.return_value = ft_output 
        self.model.score.return_value = 1, 2 
        self.test_video_file = os.path.join(os.path.dirname(__file__), 
                                "test.mp4") 
        self.test_video_file2 = os.path.join(os.path.dirname(__file__), 
                                "test2.mp4") 
        #Redis
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
       
        #setup process video object
        self.api_request = None
        
        #patch for download_and_add_thumb
        self.utils_patch = patch('supportServices.neondata.utils.http.send_request')
        self.uc = self.utils_patch.start() 

        random.seed(984695198)
        
    def tearDown(self):
        self.redis.stop()
        super(TestVideoClient, self).tearDown()
        self.utils_patch.stop()
        
    def setup_video_processor(self, request_type):
        '''
        Setup the api request for the video processor
        '''
        
        self.na = neondata.NeonUserAccount('acc1')
        self.na.save()
        
        self.np = neondata.NeonPlatform('acc1', self.na.neon_api_key)
        self.np.save()

        j_id = "j123"
        api_key = self.na.neon_api_key 
        vid = "video1"
        i_id = 0

        if request_type == "neon":
            jparams = request_template.neon_api_request % (
                    j_id, vid, api_key, "neon", api_key, j_id)
            self.api_request = neondata.NeonApiRequest(j_id, api_key, vid, "title",
                    'url', 'neon', None)
        elif request_type == "brightcove":
            i_id = "b_id"
            jparams = request_template.brightcove_api_request %(j_id, vid, api_key,
                            "brightcove", api_key, j_id, i_id)
            self.api_request = neondata.BrightcoveApiRequest(
                                        j_id, api_key, vid, 
                                        'title', 'http://url',
                                        'rtok', 'wtok', None) 
            self.api_request.previous_thumbnail = "http://prevthumb"
        elif request_type == "ooyala":
            i_id = "b_id"
            jparams = request_template.ooyala_api_request %(j_id, vid, api_key,
                            "ooyala", api_key, j_id, i_id)
            self.api_request = neondata.OoyalaApiRequest(
                                       j_id, api_key, i_id, vid, 'title', 'url',
                                       'oo_key', 'oo_secret', 'http://p_thumb', 'cb')

        job = json.loads(jparams)
        
        i_vid = neondata.InternalVideoID.generate(api_key, vid)
        vmdata = neondata.VideoMetadata(i_vid, [], j_id, "url", 10,
                                        4, None, i_id, [640,480])
        vmdata.save()
        
        self.api_request.api_method = 'topn'
        self.api_request.api_param = 1 
        self.api_request.save()
        vprocessor = api.client.VideoProcessor(
            job, self.model,
            self.model_version,
            multiprocessing.BoundedSemaphore(1))
        
        return vprocessor

    ##### Process video tests ####
    @patch('api.client.urllib2.urlopen')
    def test_download_video_file(self, mock_client):
        # Createa a 10MB random string
        vdata = StringIO('%030x' % random.randrange(16**(10*1024*1024)))
        mock_client.return_value = vdata
        
        vprocessor = self.setup_video_processor("neon")
        vprocessor.download_video_file()
        vprocessor.tempfile.seek(0) 
        self.assertEqual(vprocessor.tempfile.read(), vdata.getvalue()) 

    @patch('api.client.urllib2.urlopen')
    def test_download_video_errors(self, mock_client):
        mock_client.side_effect = [
            urllib2.URLError('Oops'),
            socket.gaierror(),
            IOError()
            ]
        
        vprocessor = self.setup_video_processor("neon")
        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(api.client.VideoDownloadError):
                vprocessor.download_video_file()

        
        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(api.client.VideoDownloadError):
                vprocessor.download_video_file()
                
        with self.assertLogExists(logging.ERROR, "Error saving video to disk"):
            with self.assertRaises(api.client.VideoDownloadError):
                vprocessor.download_video_file()

    def test_process_video(self):
       
        '''
        Verify execution of the process_all call in ProcessVideo
        '''
        vprocessor = self.setup_video_processor("neon")
        vprocessor.process_video(self.test_video_file)

        #verify video metadata has been populated
        self.assertEqual(vprocessor.video_metadata.duration, 8.8)
        self.assertEqual(vprocessor.video_metadata.frame_size, (400, 264))
       
        #verify that the thumbnails were populated
        self.assertGreater(len(vprocessor.thumbnails), 0)
        self.assertGreater(len([x for x in vprocessor.thumbnails if 
                                x[0].type == neondata.ThumbnailType.NEON]), 0)
        self.assertEquals(len([x for x in vprocessor.thumbnails if 
                               x[0].type == neondata.ThumbnailType.RANDOM]), 1)
        self.assertEquals(
            len([x for x in vprocessor.thumbnails if 
                 x[0].type == neondata.ThumbnailType.CENTERFRAME]), 1)
        self.assertNotIn(float('-inf'), 
                         [x[0].model_score for x in vprocessor.thumbnails])

    def test_missing_video_file(self):
        vprocessor = self.setup_video_processor("neon")

        with self.assertLogExists(logging.ERROR, "Error reading"):
            with self.assertRaises(api.client.VideoReadError):
                vprocessor.process_video('a_garbage_video_thats_gone.mov')

    def test_process_all_filtered_video(self):
        '''Test processing a video where every frame is filtered.'''
        self.model.choose_thumbnails.return_value = (
            [(np.zeros((480, 640, 3), np.uint8), float('-inf'), 120, 4.0,
              'black'),
             (np.zeros((480, 640, 3), np.uint8), float('-inf'), 600, 20.0,
              'black'),
             (np.zeros((480, 640, 3), np.uint8), float('-inf'), 900, 30.0,
              'black')],
             40.0)
        vprocessor = self.setup_video_processor("neon")
        vprocessor.process_video(self.test_video_file, n_thumbs=3)

        # Verify that all the frames were added to the data maps
        neon_thumbs = [x[0] for x in vprocessor.thumbnails if
                       x[0].type == neondata.ThumbnailType.NEON]
        self.assertEquals(len(neon_thumbs), 3)
        self.assertEquals([x.model_score for x in neon_thumbs],
                          [float('-inf'), float('-inf'), float('-inf')])
        self.assertEquals([x.filtered for x in neon_thumbs],
                          ['black', 'black', 'black'])
   
    @patch('utils.sqsmanager')
    @patch('api.cdnhosting.urllib2')
    @patch('api.cdnhosting.S3Connection')
    @patch('api.client.VideoProcessor.finalize_api_request')
    @patch('utils.http')
    def test_finalize_request(self, mock_client, mock_finalize_api,
                               mock_conntype, mock_urllib2, sqsmgr):
        request = tornado.httpclient.HTTPRequest("http://xyz")
        response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=StringIO(''))
        mock_client.send_request.return_value = response
        mock_finalize_api.return_value = True
        
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-image-cdn')
        mock_conntype.return_value = conn
        
        # Mock customerCallback
        sqsmgr = MagicMock()
        sqsmgr.add_callback_response.return_value = True
        
        mresponse = MagicMock()
        mresponse.read.return_value = '{"url": "http://cloudinary.jpg"}' 
        mock_urllib2.urlopen.return_value = mresponse 
       
        vprocessor = self.setup_video_processor("neon")
        vprocessor.process_video(self.test_video_file)
        vprocessor.finalize_request() 

        #verify the center frame thumbnail
        self.assertEqual(vprocessor.thumbnails[-1].type,
                            neondata.ThumbnailType.CENTERFRAME)
    
    @patch('utils.sqsmanager')
    @patch('utils.http')
    def test_finalize_request_error(self, mock_client, sqsmgr):
        '''
        Test finalize request flow when there has been 
        a download or a processing error
        ''' 
        request = tornado.httpclient.HTTPRequest("http://neon-lab.com")
        response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=StringIO(''))
        mock_client.send_request.return_value = response
        vprocessor = self.setup_video_processor("neon")
        vprocessor.error = "error message" 
        vprocessor.finalize_request() 

        #verify request state 
        api_key = vprocessor.job_params['api_key']
        job_id  = vprocessor.job_params['job_id']
        api_request = neondata.NeonApiRequest.get(api_key, job_id)
        self.assertEqual(api_request.state, neondata.RequestState.FAILED)
        
        #NOTE: disabled now, since we don't requeue currently
        #Induce a requeue error
        #This also emulates requeue count > 3
        #response = tornado.httpclient.HTTPResponse(request, 500,
        #                    buffer=StringIO(''))
        #mock_client.send_request.return_value = response
        #vprocessor.finalize_request() 
        #api_request = neondata.NeonApiRequest.get(api_key, job_id)
        #self.assertEqual(api_request.state, neondata.RequestState.INT_ERROR)
        
        #verify callback response
        #TODO: Mock customerCallback
        #callback_result = json.loads(mock_client.send_request.call_args[0][0].body)
        #self.assertEqual(callback_result["data"], [])
        #self.assertEqual(callback_result["thumbnails"], [])
        #self.assertEqual(callback_result["error"], vprocessor.error)

    def test_finalize_api_request(self):

        vprocessor = self.setup_video_processor("neon")
        cr_request = api.client.callback_response_builder("job_id", "video_id", [15],
                            ["s3_url"], "callback_url", error=None)
        vprocessor.finalize_api_request(cr_request.body, "neon")
        api_key = vprocessor.job_params['api_key']
        job_id = vprocessor.job_params['job_id']
        api_request = neondata.NeonApiRequest.get(api_key, job_id)
        self.assertEqual(api_request.state, neondata.RequestState.FINISHED)

    def test_get_center_frame(self):
        '''
        Test center frame extraction
        '''
        
        jparams = request_template.neon_api_request %(
                    "j_id", "vid", "api_key", "neon", "api_key", "j_id")
        job = json.loads(jparams)
        vprocessor = api.client.VideoProcessor(job, self.model,
                self.model_version, multiprocessing.BoundedSemaphore(1))
        vprocessor._get_center_frame(self.test_video_file)
        meta, img = vprocessor.thumbnails[0]
        self.assertIsNotNone(img)
        self.assertTrue(isinstance(img, Image.Image))
        self.assertEqual(meta.type, neondata.ThumbnailType.CENTERFRAME)
        self.assertEqual(meta.rank, 0)
        self.assertEqual(meta.frameno, 66)

    def test_get_random_frame(self):
        '''
        Test random frame extraction
        '''
        
        jparams = request_template.neon_api_request %(
                    "j_id", "vid", "api_key", "neon", "api_key", "j_id")
        job = json.loads(jparams)
        vprocessor = api.client.VideoProcessor(job, self.model,
                self.model_version, multiprocessing.BoundedSemaphore(1))
        vprocessor._get_random_frame(self.test_video_file)
        meta1, img1 = vprocessor.thumbnails[0]
        self.assertIsNotNone(img1)
        self.assertTrue(isinstance(img1, Image.Image))
        self.assertEqual(meta1.type, neondata.ThumbnailType.RANDOM)
        self.assertEqual(meta1.rank, 0)

        vprocessor._get_random_frame(self.test_video_file)
        meta2, img2 = vprocessor.thumbnails[1]
        self.assertNotEqual(meta2.frameno, meta1.frameno)


    @patch('utils.sqsmanager')
    @patch('api.cdnhosting.urllib2')
    @patch('api.cdnhosting.S3Connection')
    @patch('utils.http')
    def test_finalize_request_reprocess(self, mock_client,
                               mock_conntype, mock_urllib2, sqsmgr):
        '''
        Process a request (normal flow)
        modify the video URL
        Change request state
        Change few video metadata params

        Generate a few thumbnails (2 old  & rest new)
        Verify TMData, Apirequest, VMData, Serving URLs

        '''
        request = tornado.httpclient.HTTPRequest("http://xyz")
        response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=StringIO(''))
        mock_client.send_request.return_value = response
        
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-image-cdn')
        mock_conntype.return_value = conn
        
        # Mock customerCallback
        sqsmgr = MagicMock()
        sqsmgr.add_callback_response.return_value = True
        
        mresponse = MagicMock()
        mresponse.read.return_value = '{"url": "http://cloudinary.jpg"}' 
        mock_urllib2.urlopen.return_value = mresponse 
        
        vprocessor = self.setup_video_processor("neon")

        api_key = self.na.neon_api_key
        job_id = "j123"
        i_vid = neondata.InternalVideoID.generate(api_key, "video1")
        
        vprocessor.process_video(self.test_video_file)
        vprocessor.finalize_request() 
        old_tids = [t.key for t in vprocessor.thumbnails]
        vmdata = neondata.VideoMetadata.get(i_vid)
        self.assertEqual(vmdata.thumbnail_ids, old_tids)

        # Set State to reprocess
        api_request = neondata.NeonApiRequest.get(api_key, job_id)
        api_request.video_url = "http://reprocess_video_url"
        api_request.previous_thumbnail = "http://previous_thumb" 
        api_request.state = neondata.RequestState.REPROCESS
        api_request.save()

        # Modify params in vprocessor
        vprocessor.model_version = "reprocess_model"
        vprocessor.job_params = api_request.__dict__
        ct_output, ft_output = pickle.load(open(self.model_file)) 
        for i in range(3):
            ct_output[i][0][0][0] = 255
            ct_output[i][0][0][1] = 255

        vprocessor.model.choose_thumbnails.return_value = (ct_output, 9)
        vprocessor.thumbnails = []
        vprocessor.process_video(self.test_video_file2)
        spr_mock = MagicMock()
        vprocessor.save_previous_thumbnail = spr_mock
        p_tid = TMD('p_tid',i_vid,[0],0,0,0,'custom_upload',0,0,True,False,0)
        spr_mock.return_value = p_tid

        vprocessor.finalize_request() 
        
        # add p_tid to vp object to emulate behavior of spr method
        vprocessor.thumbnails.append(p_tid) 

        vmdata = neondata.VideoMetadata.get(i_vid)

        # Verify Videometadata object
        tids = [t.key for t in vprocessor.thumbnails]
        print tids
        print vmdata.thumbnail_ids
        self.assertEqual(vmdata.thumbnail_ids, tids)
        self.assertEqual(vmdata.frame_size, [1280, 720])
        self.assertEqual(vmdata.model_version, "reprocess_model")

class TestFinalizeResponse(test_utils.neontest.TestCase):
    ''' 
    Test the cleanup and responding after the video has been processed
    '''
    def setUp(self):
        super(TestFinalizeResponse, self).setUp()

        #Redis
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

        # Fill out redis
        na = neondata.NeonUserAccount('acct1')
        self.api_key = na.neon_api_key
        na.save()
        neondata.NeonPlatform('acct1', self.api_key).save()

        video_id = '%s_vid1' % self.api_key
        BrightcoveApiRequest('job1', self.api_key, video_id, 'some fun video',
                             'http://video.mp4', None, None, 'pubid',
                             'http://callback.com', 'int1',
                             'http://default_thumb.jpg').save()
       

        # Setup the processor object
        job = {
            'api_key': self.api_key,
            'video_id' : video_id,
            'job_id' : 'job1',
            'integration_id' : 'int1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            }
        self.vprocessor = api.client.VideoProcessor(
            job,
            MagicMock(),
            'test_version',
            multiprocessing.BoundedSemaphore(1))

        random.seed(984695198)
        
    def tearDown(self):
        self.redis.stop()
        self.utils_patch.stop()
        super(TestFinalizeResponse, self).tearDown()

if __name__ == '__main__':
    unittest.main()
