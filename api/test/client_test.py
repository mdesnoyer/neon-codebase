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
import json
import logging
import mock
import multiprocessing
import os
import pickle
import PIL
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
import urllib
import unittest
import utils
from utils.imageutils import PILImageUtils
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
        #Redis
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
       
        #setup process video object
        self.api_request = None
        
        #patch for download_and_add_thumb
        self.utils_patch = patch('supportServices.neondata.utils.http.send_request')
        self.uc = self.utils_patch.start() 
        
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

        j_id = "j123"
        api_key = self.na.neon_api_key 
        vid = "video1"
        i_id = 0

        if request_type == "neon":
            jparams = request_template.neon_api_request %(
                    j_id, vid, api_key, "neon", api_key, j_id)
            self.api_request = neondata.NeonApiRequest(j_id, api_key, vid, "title",
                            None, None, None)
        elif request_type == "brightcove":
            i_id = "b_id"
            jparams = request_template.brightcove_api_request %(j_id, vid, api_key,
                            "brightcove", api_key, j_id, i_id)
            self.api_request = neondata.BrightcoveApiRequest(
                                        j_id, api_key, vid, 'title', None,
                                        None, None, None)
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
        
        self.api_request.save()
        vprocessor = api.client.VideoProcessor(job, self.model, self.model_version)

        #Add a mock to the get_center_frame
        vprocessor.get_center_frame = mock.Mock(return_value=
                PILImageUtils.create_random_image(360, 480)) 
        return vprocessor 

    @patch('api.cdnhosting.urllib2')
    @patch('api.cdnhosting.S3Connection')
    def test_save_thumbnail_to_s3_and_metadata(self, mock_conntype,
                                                mock_urllib2):
        '''
        
        Test that thumbnail is saved to s3, 
        TM metdata saved in DB
        Videometadata updated with TID in DB
        
        Thumbnail hosted on CDN & Cloudinary
        '''

        random.seed(215)

        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-image-cdn')
        mock_conntype.return_value = conn
        
        mresponse = MagicMock()
        mresponse.read.return_value = '{"url": "http://cloudinary.jpg"}' 
        mock_urllib2.urlopen.return_value = mresponse 
        
        thumb_bucket = conn.buckets['host-thumbnails']

        image = PILImageUtils.create_random_image(360, 480) 
        keyname = "test_key"
        i_vid = "i_vid1"

        vmdata = neondata.VideoMetadata(i_vid, [], "j_id", "url", 10,
                                        4, None, "i_id", [640,480])
        
        tdata = vmdata.save_thumbnail_to_s3_and_store_metadata(
                                            image, 1, 
                                            keyname, 
                                            's3_%s'%keyname, 
                                            'neon')
        vmdata.save()
        s3_keys = [x for x in thumb_bucket.get_all_keys()]
        self.assertEqual(len(s3_keys), 1)
        self.assertEqual(s3_keys[0].name, keyname)
        self.assertEqual(tdata.video_id, i_vid)
        self.assertEqual(vmdata.thumbnail_ids, [tdata.key])

    @patch('api.cdnhosting.urllib2')
    @patch('api.cdnhosting.S3Connection')
    def test_host_images_s3(self, mock_conntype, mock_urllib2):
        
        random.seed(1251)
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-image-cdn')
        mock_conntype.return_value = conn
        
        mresponse = MagicMock()
        mresponse.read.return_value = '{"url": "http://cloudinary.jpg"}' 
        mock_urllib2.urlopen.return_value = mresponse 
       
        api_key = "test_api_key"
        i_id = "i_id" 
        vid = "tvid1"
        i_vid = "test_api_key_%s" %vid
        bfname = "%s/%s" %(api_key, "job_id")
        images = []
        N = 6
        for i in range(N):
            images.append((
                PILImageUtils.create_random_image(360, 480), random.random()))

        vmdata = neondata.VideoMetadata(i_vid, [], "j_id", "url", 10,
                                        4, None, i_id, [640,480])
        vmdata.save()
        thumbnails, s3_urls = api.client.host_images_s3(vmdata, api_key, images, bfname)

        s3_keys = [x for x in conn.buckets['host-thumbnails'].get_all_keys()]
        self.assertEqual(len(thumbnails), N)
        self.assertEqual(len(s3_keys), N)
        for i, s3key in zip(range(N), sorted(s3_keys, key=lambda k: k.name)):
            self.assertEqual(s3key.content_type, 'image/jpeg')
            filestream = StringIO()
            images[i][0].save(filestream, "jpeg", quality=90) 
            filestream.seek(0)
            imgdata = filestream.read()
            self.assertEqual(s3key.data, imgdata) 

        #verify s3 urls
        s3prefix = 'https://host-thumbnails.s3.amazonaws.com/%s/neon%s.jpeg'
        expected_urls = [s3prefix %(bfname, i) for i in range(N)]
        self.assertEqual(s3_urls, expected_urls)

    def test_notification_response_builder(self):
        
        req = api.client.notification_response_builder("a_id", "i_id",
                                    '{"video":"data"}')
        self.assertIn("a_id", req.url)
        req_body = urllib.unquote(req.body).decode('utf8')
        parts = req_body.split('&')
        for part in parts:
            e = part.split("=")
            if "event" in part:
                self.assertEqual(e[1], "processing_complete")
            
            elif "video" in part:
                self.assertEqual(e[1], '{"video":"data"}')

    ##### Process video tests ####
    @patch('api.client.tornado.httpclient.HTTPClient')
    def test_download_video_file(self, mock_client):
        vdata = "aqgqegasghasghashgadshadhgadhg"
        request = tornado.httpclient.HTTPRequest("http://VideoURL.mp4")
        response = tornado.httpclient.HTTPResponse(request, 200,
                            headers = {'Content-Length': len(vdata)},
                            buffer=StringIO(vdata))
        mclient = MagicMock()
        mclient.fetch.return_value = response
        mock_client.return_value = mclient 
        
        vprocessor = self.setup_video_processor("neon")
        vprocessor.download_video_file()
        vprocessor.tempfile.seek(0) 
        self.assertEqual(vprocessor.tempfile.read(), vdata) 

    def test_process_video(self):
       
        '''
        Verify execution of the process_all call in ProcessVideo
        '''
        vprocessor = self.setup_video_processor("neon")
        ret = vprocessor.process_video(self.test_video_file)
        self.assertTrue(ret)

        #verify metadata has been populated
        self.assertEqual(vprocessor.video_metadata['codec_name'], 'h264')
        self.assertEqual(vprocessor.video_metadata['duration'], 8.8)
        self.assertEqual(vprocessor.video_metadata['framerate'], 15)
        self.assertEqual(vprocessor.video_metadata['frame_size'], (400, 264))
        self.assertIsNotNone(vprocessor.video_metadata['bitrate'])
       
        #verify that following maps get populated
        self.assertGreater(len(vprocessor.data_map), 0)
        self.assertGreater(len(vprocessor.attr_map), 0)
        self.assertGreater(len(vprocessor.timecodes), 0)
        self.assertNotIn(float('-inf'), vprocessor.valence_scores[1])
    
    @patch('api.cdnhosting.urllib2')
    @patch('api.cdnhosting.S3Connection')
    @patch('api.client.VideoProcessor.finalize_api_request')
    @patch('utils.http')
    def test_finalize_request(self, mock_client, mock_finalize_api,
                               mock_conntype, mock_urllib2):
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
        
        mresponse = MagicMock()
        mresponse.read.return_value = '{"url": "http://cloudinary.jpg"}' 
        mock_urllib2.urlopen.return_value = mresponse 
       
        vprocessor = self.setup_video_processor("neon")
        vprocessor.process_video(self.test_video_file)
        vprocessor.finalize_request() 

        #TODO: Mock customerCallback
        #verify callback response
        #url_call = mock_client.send_request.call_args[0][0].url
        #self.assertEqual(url_call, "http://localhost:8081/testcallback")
        #self.assertEqual(mock_client.send_request.call_count, 1)

        #verify data in the callback response
        #callback_result = json.loads(mock_client.send_request.call_args[0][0].body)
        #result_data = [15, 30, 60, 45, 105] #hardcoded for now, perhaps extract this from model data  
        #self.assertEqual(callback_result["data"], result_data)
        #self.assertEqual(len(callback_result["thumbnails"]), len(result_data))
        #self.assertEqual(callback_result["video_id"], 'video1')
        #self.assertEqual(callback_result["error"], None)
        #self.assertEqual(callback_result["serving_url"][len("http://i1"):],
        #    ".neon-images.com/v1/client/%s/neonvid_video1" %
        #     self.na.tracker_account_id)
            
        #verify the number of thumbs in self.thumbnails  
        #self.assertEqual(len(vprocessor.thumbnails), len(callback_result["thumbnails"]) + 1)

        #verify the center frame thumbnail
        self.assertEqual(vprocessor.thumbnails[-1].type,
                            neondata.ThumbnailType.CENTERFRAME)
    
    @patch('utils.http')
    def test_finalize_request_error(self, mock_client):
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
        self.assertEqual(api_request.state, neondata.RequestState.REQUEUED)
        
        #Induce a requeue error
        #This also emulates requeue count > 3
        response = tornado.httpclient.HTTPResponse(request, 500,
                            buffer=StringIO(''))
        mock_client.send_request.return_value = response
        vprocessor.finalize_request() 
        api_request = neondata.NeonApiRequest.get(api_key, job_id)
        self.assertEqual(api_request.state, neondata.RequestState.INT_ERROR)
        
        #verify callback response
        #TODO: Mock customerCallback
        #callback_result = json.loads(mock_client.send_request.call_args[0][0].body)
        #self.assertEqual(callback_result["data"], [])
        #self.assertEqual(callback_result["thumbnails"], [])
        #self.assertEqual(callback_result["error"], vprocessor.error)

    def test_get_top_n_thumbnails(self):
        
        vprocessor = self.setup_video_processor("neon")
        vprocessor.process_video(self.test_video_file)
        #Test getting top thumbnail
        res = vprocessor.get_topn_thumbnails(1)
        ranked_frames = [x[0] for x in res]
        self.assertEqual(ranked_frames, [15]) 
        #hardcode frame 15 for now, find a better way to parameterize this test

    def test_finalize_api_request(self):

        vprocessor = self.setup_video_processor("neon")
        cr_request = api.client.callback_response_builder("job_id", "video_id", [15],
                            ["s3_url"], "callback_url", error=None)
        vprocessor.finalize_api_request(cr_request.body, "neon")
        api_key = vprocessor.job_params['api_key']
        job_id = vprocessor.job_params['job_id']
        api_request = neondata.NeonApiRequest.get(api_key, job_id)
        self.assertEqual(api_request.state, neondata.RequestState.FINISHED)
        
    @patch('api.cdnhosting.urllib2')
    @patch('api.client.tornado.httpclient.HTTPClient')
    @patch('api.cdnhosting.S3Connection')
    def test_save_previous_thumbnail(self, mock_conntype,
            mock_client, mock_urllib2):
        '''
        Test saving previous thumbnail for 
        Neon/ bcove/ ooyala requests
        '''
        im = PILImageUtils.create_random_image(360, 480) 
        imgstream = StringIO()
        im.save(imgstream, "jpeg", quality=100)
        imgstream.seek(0)
        data = imgstream.read()

        request = tornado.httpclient.HTTPRequest("http://VideoURL.mp4")
        response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=StringIO(data))
        mclient = MagicMock()
        mclient.fetch.return_value = response
        mock_client.return_value = mclient

        self.uc.side_effect = lambda x, callback:\
            tornado.ioloop.IOLoop.current().add_callback(callback,
                response)
    
        mresponse = MagicMock()
        mresponse.read.return_value = '{"url": "http://cloudinary.jpg"}' 
        mock_urllib2.urlopen.return_value = mresponse 
        
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('neon-image-cdn')
        mock_conntype.return_value = conn
       
        vprocessor = self.setup_video_processor("brightcove")
        tdata = vprocessor.save_previous_thumbnail(self.api_request)
        self.assertGreater(vprocessor.thumbnails, 0)
        i_vid = neondata.InternalVideoID.generate(self.api_request.api_key, self.api_request.video_id)
        self.assertEqual(tdata.video_id, i_vid)
        self.assertIn("brightcove.jpeg", tdata.urls[0])

    @patch("api.ooyala_api.OoyalaAPI.update_thumbnail")
    @patch("api.brightcove_api.BrightcoveApi.update_thumbnail_and_videostill")
    def test_autosync(self, update_bc_mock, update_oo_mock):
        ''' Test Autosync thumbnails '''

        update_bc_mock.return_value = ('refid1', 'still-refid1') 
        update_oo_mock.return_value = True 
        vprocessor = self.setup_video_processor("brightcove")
        vprocessor.process_video(self.test_video_file)
        #Setup  vprocessor.thumbnails
        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            't01': TMD('t01','vid',[0],0,0,0,'neon',0,0,True,False,0),
            't02': TMD('t02','vid',[0],0,0,0,'neon',0,0,True,True,0),
            't03': TMD('t03','vid',[0],0,0,0,'neon',1,0,True,False,0)
            }
        vprocessor.thumbnails = tid_meta.values()
        ret = vprocessor.save_video_metadata()
        im = PILImageUtils.create_random_image(360, 480)
        vprocessor.autosync(self.api_request, im)
        self.assertEqual(update_bc_mock.call_args[0][0], self.api_request.video_id)
        self.assertEqual(update_bc_mock.call_args[0][2], vprocessor.thumbnails[0].key)
        self.assertTrue(vprocessor.thumbnails[0].chosen)

        ## Ooyala Autosync
        vprocessor = self.setup_video_processor("ooyala")
        vprocessor.process_video(self.test_video_file)
        vprocessor.thumbnails = tid_meta.values()
        vprocessor.autosync(self.api_request, im)
        self.assertEqual(update_oo_mock.call_args[0][0], self.api_request.video_id)
        self.assertTrue(vprocessor.thumbnails[0].chosen)

    def test_save_thumbnail_metadata(self):
        '''
        Test save thumbnail metadata
        '''
        
        vprocessor = self.setup_video_processor("neon")
        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            't01': TMD('t01','vid',[0],0,0,0,'neon',0,0,True,False,0),
            't02': TMD('t02','vid',[0],0,0,0,'neon',0,0,True,True,0),
            't03': TMD('t03','vid',[0],0,0,0,'neon',1,0,True,False,0)
            }
        vprocessor.thumbnails = tid_meta.values()
        ret = vprocessor.save_thumbnail_metadata("neon", 0)
        self.assertTrue(ret)

        tmds = TMD.get_many(tid_meta.keys())
        for tmd in tmds:
            self.assertEqual(tmd.__dict__, tid_meta[tmd.key].__dict__)

    def test_save_video_metadata(self):
        '''
        Test save video metadata
        '''
        
        vprocessor = self.setup_video_processor("neon")
        TMD = neondata.ThumbnailMetadata
        tid_meta = {
            't01': TMD('t01','vid',[0],0,0,0,'neon',0,0,True,False,0),
            't02': TMD('t02','vid',[0],0,0,0,'neon',0,0,True,True,0),
            't03': TMD('t03','vid',[0],0,0,0,'neon',1,0,True,False,0)
            }
        vprocessor.thumbnails = tid_meta.values()
        ret = vprocessor.save_video_metadata()
        self.assertTrue(ret)
        
        api_key = vprocessor.job_params['api_key']
        vid =  vprocessor.job_params['video_id']
        i_vid = neondata.InternalVideoID.generate(api_key, vid)
        vmdata = neondata.VideoMetadata.get(i_vid)
        self.assertIsNotNone(vmdata)

    @patch('utils.http')
    def test_requeue_job(self, mock_client):
        request = tornado.httpclient.HTTPRequest("http://neon-lab.com")
        response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=StringIO(''))
        mock_client.send_request.return_value = response
        vprocessor = self.setup_video_processor("neon")
        self.assertTrue(vprocessor.requeue_job())

        #Exceed requeue count
        vprocessor.job_params["requeue_count"] = 4
        self.assertFalse(vprocessor.requeue_job())

    def test_get_center_frame(self):
        '''
        Test center frame extraction
        '''
        
        jparams = request_template.neon_api_request %(
                    "j_id", "vid", "api_key", "neon", "api_key", "j_id")
        job = json.loads(jparams)
        vprocessor = api.client.VideoProcessor(job, self.model,
                self.model_version)
        img = vprocessor.get_center_frame(self.test_video_file)
        self.assertIsNotNone(img)
        self.assertTrue(isinstance(img, PIL.Image.Image))

if __name__ == '__main__':
    unittest.main()
