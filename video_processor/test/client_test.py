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

from boto.sqs.message import Message
import boto.exception
import cmsdb.cdnhosting
from cmsdb import neondata
import json
import logging
from mock import MagicMock, patch
import model.errors
import multiprocessing
import numpy as np
import os
import pdb
import pickle
from PIL import Image
import re
import redis
import random
import request_template
import signal
import socket
from StringIO import StringIO
import subprocess
import time
import tempfile
import test_utils
import test_utils.mock_boto_s3 as boto_mock
import test_utils.neontest
import test_utils.net
import test_utils.redis
from test_utils import sqsmock
from tornado.concurrent import Future
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase,AsyncTestCase,AsyncHTTPClient
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
import urllib
import urlparse
import urllib2
import unittest
import utils
from utils.imageutils import PILImageUtils
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon
import video_processor.client
from video_processor import video_processing_queue

_log = logging.getLogger(__name__)

class TestVideoClient(test_utils.neontest.AsyncTestCase):
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
        self.model.choose_thumbnails.return_value = ct_output
        self.model.score.return_value = 1, 2 
        self.test_video_file = os.path.join(os.path.dirname(__file__), 
                                "test.mp4") 
        self.test_video_file2 = os.path.join(os.path.dirname(__file__), 
                                "test2.mp4") 
        #Redis
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
       
        # Fill out redis
        na = neondata.NeonUserAccount('acct1')
        self.api_key = na.neon_api_key
        na.save()
        neondata.NeonPlatform.modify(self.api_key, '0', 
                                     lambda x: x, create_missing=True)

        cdn = neondata.CDNHostingMetadataList(
            neondata.CDNHostingMetadataList.create_key(self.api_key, '0'),
            [neondata.NeonCDNHostingMetadata(rendition_sizes=[(160,90)])])
        cdn.save()

        self.video_id = '%s_vid1' % self.api_key
        self.api_request = neondata.OoyalaApiRequest(
            'job1', self.api_key,
            'int1', 'vid1',
            'some fun video',
            'http://video.mp4', None, None,
            'http://callback.com',
            'http://default_thumb.jpg')
        self.api_request.save() 

        #setup process video object
        self.api_request = None

        # Mock the SQS implementation
        #self.mock_sqs = sqsmock.SQSConnectionMock()
        self.sqs_patcher = patch('video_processor.video_processing_queue.boto.sqs.' \
                                 'connect_to_region')
        #self.mock_sqs_future = self._future_wrap_mock(
        #                                 self.sqs_patcher.start(),
        #                                 require_async_kw=False)
        
        self.mock_sqs = self.sqs_patcher.start()
        self.mock_sqs.return_value = sqsmock.SQSConnectionMock()
        self.read_patcher = patch('video_processor.video_processing_queue.'\
                                  'VideoProcessingQueue.read_message')
        self.mock_read_future = self._future_wrap_mock(
            self.read_patcher.start(),
            require_async_kw=False)

        self.delete_patcher = patch('video_processor.video_processing_queue.'\
                                    'VideoProcessingQueue.delete_message')
        self.mock_delete_future = self._future_wrap_mock(
            self.delete_patcher.start(),
            require_async_kw=False)

        self.mock_delete_future.return_value = True

        #patch for download_and_add_thumb
        self.utils_patch = patch('cmsdb.neondata.utils.http.send_request')
        self.uc = self.utils_patch.start()

        # create the client object
        self.video_client = video_processor.client.VideoClient(
            'some/dir/my_model.model',
            multiprocessing.BoundedSemaphore(1))

        random.seed(984695198)
        
    def tearDown(self):
        self.utils_patch.stop()
        self.redis.stop()
        self.sqs_patcher.stop()
        self.read_patcher.stop()
        self.delete_patcher.stop()
        super(TestVideoClient, self).tearDown()
        
    def setup_video_processor(self, request_type, url='http://url.com'):
        '''
        Setup the api request for the video processor
        '''
        
        self.na = neondata.NeonUserAccount('acc1')
        self.na.save()
        
        self.np = neondata.NeonPlatform.modify(
            self.na.neon_api_key, '0',
            lambda x: x, create_missing=True)

        j_id = "j123"
        api_key = self.na.neon_api_key 
        vid = "video1"
        i_id = 0

        if request_type == "neon":
            jparams = request_template.neon_api_request % (
                    j_id, vid, api_key, "neon", api_key, j_id)
            self.api_request = neondata.NeonApiRequest(j_id, api_key, vid,
                                                       "title",
                                                       url, 'neon', None)
        elif request_type == "brightcove":
            i_id = "b_id"
            jparams = request_template.brightcove_api_request %(
                j_id, vid, api_key, "brightcove", api_key, j_id, i_id)
            self.api_request = neondata.BrightcoveApiRequest(
                                        j_id, api_key, vid, 
                                        'title', url,
                                        'rtok', 'wtok', None) 
            self.api_request.previous_thumbnail = "http://prevthumb"
        elif request_type == "ooyala":
            i_id = "b_id"
            jparams = request_template.ooyala_api_request %(j_id, vid, api_key,
                            "ooyala", api_key, j_id, i_id)
            self.api_request = neondata.OoyalaApiRequest(
                j_id, api_key, i_id, vid, 'title', url,
                'oo_key', 'oo_secret', 'http://p_thumb', 'cb')

        job = json.loads(jparams)
        job['video_url'] = url
        
        i_vid = neondata.InternalVideoID.generate(api_key, vid)
        vmdata = neondata.VideoMetadata(i_vid, [], j_id, url, 10,
                                        4, None, i_id, [640,480])
        vmdata.save()
        
        self.api_request.api_method = 'topn'
        self.api_request.api_param = 1 
        self.api_request.save()
        vprocessor = video_processor.client.VideoProcessor(
            job, self.model,
            self.model_version,
            multiprocessing.BoundedSemaphore(1))
        
        return vprocessor

    ##### Process video tests ####
    @patch('video_processor.client.urllib2.urlopen')
    def test_download_video_file(self, mock_client):
        # Createa a 10MB random string
        vdata = StringIO('%030x' % random.randrange(16**(10*1024*1024)))
        mock_client.return_value = vdata
        
        vprocessor = self.setup_video_processor("neon")
        vprocessor.download_video_file()
        vprocessor.tempfile.seek(0) 
        self.assertEqual(vprocessor.tempfile.read(), vdata.getvalue()) 

    @patch('video_processor.client.urllib2.urlopen')
    def test_download_video_errors(self, mock_client):
        mock_client.side_effect = [
            urllib2.URLError('Oops'),
            socket.gaierror(),
            IOError()
            ]
        
        vprocessor = self.setup_video_processor("neon")
        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                vprocessor.download_video_file()

        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                vprocessor.download_video_file()
                
        
        with self.assertLogExists(logging.ERROR, "Error saving video to disk"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                vprocessor.download_video_file()
                

    @patch('video_processor.client.S3Connection')
    def test_download_s3_video(self, s3_mock):
        vdata = '%030x' % random.randrange(16**(10*1024*1024))
        
        s3conn = boto_mock.MockConnection()
        s3_mock.return_value = s3conn
        s3conn.create_bucket('customer-videos')
        bucket = s3conn.get_bucket('customer-videos')
        key = bucket.new_key('some/video.mp4')
        key.set_contents_from_string(vdata)

        vprocessor = self.setup_video_processor(
            "neon", url='s3://customer-videos/some/video.mp4')
        vprocessor.download_video_file()
        vprocessor.tempfile.seek(0) 
        self.assertEqual(vprocessor.tempfile.read(), vdata)

    @patch('video_processor.client.S3Connection')
    def test_download_s3_video_http_path(self, s3_mock):
        vdata = '%030x' % random.randrange(16**(10*1024*1024))
        
        s3conn = boto_mock.MockConnection()
        s3_mock.return_value = s3conn
        s3conn.create_bucket('customer-videos')
        bucket = s3conn.get_bucket('customer-videos')
        key = bucket.new_key('some/video.mp4')
        key.set_contents_from_string(vdata)

        vprocessor = self.setup_video_processor(
            "neon", url='https://s3-us-west-2.amazonaws.com/customer-videos/some/video.mp4')
        vprocessor.download_video_file()
        vprocessor.tempfile.seek(0) 
        self.assertEqual(vprocessor.tempfile.read(), vdata)

    @patch('video_processor.client.S3Connection')
    def test_download_s3_video_error(self, s3_mock):
        s3_mock.side_effect = [
            boto.exception.BotoClientError("Permissions error"),
            boto.exception.BotoServerError(404, "Connection error"),
            IOError()
            ]

        vprocessor = self.setup_video_processor(
            "neon", url='s3://customer-videos/some/video.mp4')
       
        with self.assertLogExists(logging.ERROR, "Client error downloading"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                vprocessor.download_video_file()
        
        with self.assertLogExists(logging.ERROR, "Server error downloading"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                vprocessor.download_video_file()
        
        with self.assertLogExists(logging.ERROR, "Error saving video to disk"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                vprocessor.download_video_file()

    def test_process_video(self):
       
        '''
        Verify execution of the process_all call in ProcessVideo
        '''
        vprocessor = self.setup_video_processor("neon", url='http://video.com')
        vprocessor.process_video(self.test_video_file, n_thumbs=5)

        # Check that the model was called correctly
        self.assertTrue(self.model.choose_thumbnails.called)
        cargs, kwargs = self.model.choose_thumbnails.call_args
        self.assertEquals(kwargs, {'n':5,
                                   'video_name':  'http://video.com'})
        self.assertEquals(len(cargs), 1)

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
    
    @unittest.skip('refactor the test')
    def test_process_video_after_int_error(self):
       
        '''
        Verify video gets in to valid state after failing a couple of times
        and being in INT_ERROR state
        '''
        
        vprocessor = self.setup_video_processor("neon", url='http://video.com')
        self.api_request.state = neondata.RequestState.INT_ERROR
        self.api_request.failed_count = 2
        self.api_request.save()

        vprocessor.process_video(self.test_video_file, n_thumbs=5)
        
        # Check that the model was called correctly
        self.assertTrue(self.model.choose_thumbnails.called)
        
        api_request = neondata.NeonApiRequest.get(self.api_request.job_id,
                                    self.na.neon_api_key)
        self.assertEquals(api_request.state, neondata.RequestState.INT_ERROR)
        
        #verify video metadata has been populated
        self.assertEqual(vprocessor.video_metadata.duration, 8.8)

    def test_somebody_else_processed_first(self):
        # Try when somebody else was sucessful
        for state in [neondata.RequestState.FINISHED,
                      neondata.RequestState.SERVING,
                      neondata.RequestState.ACTIVE]:
            vprocessor = self.setup_video_processor('neon')
            self.api_request.state = state
            self.api_request.save()
            with self.assertRaises(video_processor.client.OtherWorkerCompleted):
                vprocessor.process_video(self.test_video_file, n_thumbs=5)

        # Try when the current run should continue
        for state in [neondata.RequestState.SUBMIT,
                      neondata.RequestState.REQUEUED,
                      neondata.RequestState.REPROCESS,
                      neondata.RequestState.FAILED,
                      neondata.RequestState.INT_ERROR,
                      neondata.RequestState.FINALIZING]:
            vprocessor = self.setup_video_processor('neon')
            self.api_request.state = state
            self.api_request.save()
            vprocessor.process_video(self.test_video_file, n_thumbs=5)
            self.assertGreater(len(vprocessor.thumbnails), 0)

    def test_missing_video_file(self):
        vprocessor = self.setup_video_processor("neon")

        with self.assertLogExists(logging.ERROR, "Error reading"):
            with self.assertRaises(model.errors.VideoReadError):
                vprocessor.process_video('a_garbage_video_thats_gone.mov')

    def test_process_all_filtered_video(self):
        '''Test processing a video where every frame is filtered.'''
        self.model.choose_thumbnails.return_value = (
            [(np.zeros((480, 640, 3), np.uint8), float('-inf'), 120, 4.0,
              'black'),
             (np.zeros((480, 640, 3), np.uint8), float('-inf'), 600, 20.0,
              'black'),
             (np.zeros((480, 640, 3), np.uint8), float('-inf'), 900, 30.0,
              'black')])
        vprocessor = self.setup_video_processor("neon")
        vprocessor.process_video(self.test_video_file2, n_thumbs=3)

        # Verify that all the frames were added to the data maps
        neon_thumbs = [x[0] for x in vprocessor.thumbnails if
                       x[0].type == neondata.ThumbnailType.NEON]
        self.assertEquals(len(neon_thumbs), 3)
        self.assertEquals([x.model_score for x in neon_thumbs],
                          [float('-inf'), float('-inf'), float('-inf')])
        self.assertEquals([x.filtered for x in neon_thumbs],
                          ['black', 'black', 'black'])

    def test_get_center_frame(self):
        '''
        Test center frame extraction
        '''
        
        jparams = request_template.neon_api_request %(
                    "j_id", "vid", "api_key", "neon", "api_key", "j_id")
        job = json.loads(jparams)
        vprocessor = video_processor.client.VideoProcessor(job, self.model,
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
        vprocessor = video_processor.client.VideoProcessor(job, self.model,
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

    @tornado.testing.gen_test
    def test_dequeue_job(self):
        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        with self.assertLogExists(logging.DEBUG, "Dequeue Successful"):
            message_read = yield self.video_client.dequeue_job()

        #Dequeue_job returns a tuple, but only one of the return values
        #is used.
        self.assertEqual(message_read[1].get_body(), message.get_body())

    @tornado.testing.gen_test
    def test_dequeue_job_with_empty_server(self):
        self.mock_read_future.return_value = None
        with self.assertRaises(Exception) as cm:
            yield self.video_client.dequeue_job()
        self.assertEqual(cm.exception.message, 'Server is empty')

    @patch('cmsdb.neondata.NeonApiRequest')
    @tornado.testing.gen_test
    def test_dequeue_job_with_failed_attempts(self, neon_mock):
        message = Message()
        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        neon_mock.modify().fail_count = 4
        neon_mock.modify().state = neondata.RequestState.PROCESSING
        with self.assertRaises(Exception) as cm:
            yield self.video_client.dequeue_job()
        self.assertEqual(cm.exception.message, 'Too many attempts. Aborting')

    @patch('cmsdb.neondata.NeonApiRequest')
    @tornado.testing.gen_test
    def test_dequeue_job_with_too_many_attempts(self, neon_mock):
        message = Message()
        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        neon_mock.modify().try_count = 7
        neon_mock.modify().state = neondata.RequestState.PROCESSING
        with self.assertRaises(Exception) as cm:
            yield self.video_client.dequeue_job()
        self.assertEqual(cm.exception.message, 'Too many attempts. Aborting')

class TestFinalizeResponse(test_utils.neontest.TestCase):
    ''' 
    Test the cleanup and responding after the video has been processed
    '''
    def setUp(self):
        super(TestFinalizeResponse, self).setUp()

        statemon.state._reset_values()

        #Redis
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

        random.seed(984695198)

        # Fill out redis
        na = neondata.NeonUserAccount('acct1')
        self.api_key = na.neon_api_key
        na.save()
        neondata.NeonPlatform.modify(self.api_key, '0', 
                                     lambda x: x, create_missing=True)

        cdn = neondata.CDNHostingMetadataList(
            neondata.CDNHostingMetadataList.create_key(self.api_key, '0'),
            [neondata.NeonCDNHostingMetadata(rendition_sizes=[(160,90)])])
        cdn.save()

        self.video_id = '%s_vid1' % self.api_key
        self.api_request = neondata.BrightcoveApiRequest(
            'job1', self.api_key,
            'vid1',
            'some fun video',
            'http://video.mp4',
            None, None, 'pubid',
            'http://callback.com',
            '0',
            'http://default_thumb.jpg')
        self.api_request.api_param = '1'
        self.api_request.api_method = 'topn'
        self.api_request.state = neondata.RequestState.PROCESSING
        self.api_request.save()

        # Mock out s3
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('host-thumbnails')
        self.s3conn.create_bucket('n3.neon-images.com')

        # Mock out the image download
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start(),
            require_async_kw=True)
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock.return_value = self.random_image

        # Mock out http callbacks
        self.http_mocker = patch('video_processor.client.utils.http.send_request')
        self.http_mock = self._future_wrap_mock(self.http_mocker.start(),
                                                require_async_kw=True)
        self.http_mock.side_effect = lambda x, **kw: HTTPResponse(x, 200)

        # Mock out cloudinary
        self.cloudinary_patcher = patch('cmsdb.cdnhosting.CloudinaryHosting')
        self.cloudinary_mock = self.cloudinary_patcher.start()
        future = Future()
        future.set_result(None)
        self.cloudinary_mock().upload.return_value = future

        # Setup the processor object
        job = self.api_request.__dict__
        self.vprocessor = video_processor.client.VideoProcessor(
            job,
            MagicMock(),
            'test_version',
            multiprocessing.BoundedSemaphore(1))
        self.vprocessor.video_metadata.duration = 130.0
        self.vprocessor.video_metadata.frame_size = (640, 480)

        self.vprocessor.thumbnails = [
            (neondata.ThumbnailMetadata(None,
                                        ttype=neondata.ThumbnailType.NEON,
                                        rank=0,
                                        model_score=2.3,
                                        model_version='model1',
                                        frameno=6,
                                        filtered=''),
             utils.imageutils.PILImageUtils.create_random_image(480, 640)),
             (neondata.ThumbnailMetadata(None,
                                         ttype=neondata.ThumbnailType.NEON,
                                         rank=1,
                                         model_score=2.1,
                                         model_version='model1',
                                         frameno=69),
             utils.imageutils.PILImageUtils.create_random_image(480, 640)),
             (neondata.ThumbnailMetadata(None,
                                         ttype=neondata.ThumbnailType.RANDOM,
                                         rank=0,
                                         frameno=67),
              utils.imageutils.PILImageUtils.create_random_image(480, 640))]

        
    def tearDown(self):
        self.s3_patcher.stop()
        self.http_mocker.stop()
        self.im_download_mocker.stop()
        self.cloudinary_patcher.stop()
        self.redis.stop()
        super(TestFinalizeResponse, self).tearDown()

    def test_default_process(self):

        self.vprocessor.finalize_response()

        # Make sure that the api request is updated
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state, neondata.RequestState.FINISHED)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.NOT_SENT)
        self.assertIsInstance(api_request, neondata.BrightcoveApiRequest)

        # Check the video metadata in the database
        video_data = neondata.VideoMetadata.get(self.video_id)
        self.assertEquals(len(video_data.thumbnail_ids), 4)
        self.assertAlmostEquals(video_data.duration, 130.0)
        self.assertEquals(video_data.frame_size, [640, 480])
        self.assertEquals(video_data.url, 'http://video.mp4')
        self.assertEquals(video_data.integration_id, '0')
        self.assertEquals(video_data.model_version, 'test_version')
        self.assertTrue(video_data.serving_enabled)
        self.assertIsNone(video_data.serving_url) # serving_url not saved here
        
        
        # Check the thumbnail information in the database
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.thumbnail_ids)
        default_thumb = [
            x for x in thumbs if x.type == neondata.ThumbnailType.BRIGHTCOVE]
        default_thumb = default_thumb[0]
        self.assertIsNotNone(default_thumb.key)
        rand_thumb = [
            x for x in thumbs if x.type == neondata.ThumbnailType.RANDOM]
        rand_thumb = rand_thumb[0]
        self.assertIsNotNone(rand_thumb.key)
        n_thumbs = [x for x in thumbs if x.type == neondata.ThumbnailType.NEON]
        n_thumbs = sorted(n_thumbs, key= lambda x: x.rank)
        self.assertEquals(n_thumbs[0].frameno, 6)
        self.assertEquals(n_thumbs[1].frameno, 69)
        self.assertEquals(n_thumbs[0].video_id, self.video_id)
        self.assertEquals(n_thumbs[1].video_id, self.video_id)
        self.assertIsNotNone(n_thumbs[0].phash)
        self.assertIsNotNone(n_thumbs[0].key)
        self.assertEquals(n_thumbs[0].urls, [
            'http://s3.amazonaws.com/host-thumbnails/%s.jpg' %
            re.sub('_', '/', n_thumbs[0].key)])
        self.assertEquals(n_thumbs[0].width, 640)
        self.assertEquals(n_thumbs[0].height, 480)
        self.assertIsNotNone(n_thumbs[0].created_time)
        self.assertAlmostEqual(n_thumbs[0].model_score, 2.3)
        self.assertEquals(n_thumbs[0].model_version, 'model1')
        self.assertEquals(n_thumbs[0].filtered, '')
        
        # Check that there are thumbnails in s3
        for thumb in thumbs:
            # Check the main archival image
            self.assertIsNotNone(
                self.s3conn.get_bucket('host-thumbnails').get_key(
                    re.sub('_', '/', thumb.key) + '.jpg'))

            # Check a serving url
            s_url = neondata.ThumbnailServingURLs.get(thumb.key)
            self.assertIsNotNone(s_url)
            s3httpRe = re.compile(
                'http://n[0-9].neon-images.com/([a-zA-Z0-9\-\._/]+)')
            serving_url = s_url.get_serving_url(160, 90)
            self.assertRegexpMatches(serving_url, s3httpRe)
            serving_key = s3httpRe.search(serving_url).group(1)
            self.assertIsNotNone(
                self.s3conn.get_bucket('n3.neon-images.com').get_key(
                    serving_key))

        # Check the response, both that it was added to the callback
        # and that it was recorded in the api request object.
        expected_response = {
            'job_id' : 'job1',
            'video_id' : 'vid1',
            'framenos' : [6],
            'thumbnails' : [n_thumbs[0].urls[0]],
            'error' : None
            }
        self.assertDictContainsSubset(expected_response,
                                      api_request.response)
        
        # Compare serving URL here. Ignore the i* part of serving_url; because subdomains
        # can be different from multiple get_serving_url calls
        self.assertEquals(api_request.response['serving_url'].split('neon-images')[1],
                video_data.get_serving_url(save=False).split('neon-images')[1])

        # Check that a notification was sent
        self.assertTrue(self.http_mock.called)
        cargs, kwargs = self.http_mock.call_args
        request_saw = cargs[0]
        self.assertEquals(request_saw.url, 
                          'http://www.neon-lab.com/api/accounts/acct1/events') 
        data = urlparse.parse_qs(request_saw.body)
        self.assertEquals(data['api_key'][0],
                          options.get('video_processor.client.notification_api_key'))
        self.assertEquals(data['event'][0], 'processing_complete')
        video_dict = json.loads(data['video'][0])
        self.assertEquals(video_dict['video_id'], 'vid1')
        self.assertEquals(video_dict['title'], 'some fun video')
        self.assertEquals(len(video_dict['thumbnails']), 3)
   

        # check video object again to ensure serving_url is not set
        video_data = neondata.VideoMetadata.get(self.video_id)
        self.assertIsNone(video_data.serving_url)
    
    def test_broken_default_thumb(self):
        '''
        Test to validate the flow when default thumb is broken
        '''
        
        self.im_download_mock.side_effect = [IOError, HTTPError(404),
                                             HTTPError(500)]
        for i in range(3):
            with self.assertRaises(video_processor.client.DefaultThumbError):
                self.vprocessor.finalize_response()

            # Check the video metadata in the database. It is still
            # serving, but we will be in an error state (checked in
            # another test)
            video_data = neondata.VideoMetadata.get(self.video_id)
            self.assertEquals(len(video_data.thumbnail_ids), 3) # no default thumb
            self.assertTrue(video_data.serving_enabled)

    def test_reprocess(self):
        # Add the results from the previous run to the database
        thumbs = [
            neondata.ThumbnailMetadata(
                '%s_thumb1' % self.video_id,
                self.video_id,
                model_score=3.0,
                ttype=neondata.ThumbnailType.NEON,
                model_version='old_model',
                frameno=167,
                rank=0),
            neondata.ThumbnailMetadata(
                '%s_thumb2' % self.video_id,
                self.video_id,
                ttype=neondata.ThumbnailType.RANDOM,
                rank=0),
            neondata.ThumbnailMetadata(
                '%s_thumb3' % self.video_id,
                self.video_id,
                ttype=neondata.ThumbnailType.BRIGHTCOVE,
                rank=0)]
        neondata.ThumbnailMetadata.save_all(thumbs)
            
        video_meta = neondata.VideoMetadata(
            self.video_id,
            tids = [x.key for x in thumbs],
            duration=97.0,
            model_version='old_model')
        video_meta.serving_url = 'my_serving_url.jpg'
        video_meta.save()

        # Write the request to the db
        api_request = neondata.BrightcoveApiRequest(
            'job1', self.api_key, 'vid1',
            'some fun video',
            'http://video.mp4', None, None, 'pubid',
            'http://callback.com', 'int1',
            'http://default_thumb.jpg')
        api_request.state = neondata.RequestState.PROCESSING
        api_request.save()

        self.vprocessor.reprocess = True

        self.vprocessor.finalize_response()

        # Make sure that the api request is updated
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state, neondata.RequestState.FINISHED)
        self.assertIsInstance(api_request, neondata.BrightcoveApiRequest)

        # Check the video metadata in the database
        video_data = neondata.VideoMetadata.get(self.video_id)
        self.assertEquals(len(video_data.thumbnail_ids), 5)
        self.assertAlmostEquals(video_data.duration, 130.0)
        self.assertEquals(video_data.frame_size, [640, 480])
        self.assertEquals(video_data.url, 'http://video.mp4')
        self.assertEquals(video_data.integration_id, '0')
        self.assertEquals(video_data.model_version, 'test_version')
        self.assertTrue(video_data.serving_enabled)
        self.assertIsNotNone(video_data.serving_url)

        # Check the default thumbnails in the database. There should be 2 now
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.thumbnail_ids)
        default_thumbs = [
            x for x in thumbs if x.type == neondata.ThumbnailType.BRIGHTCOVE]
        default_thumbs = sorted(default_thumbs, key=lambda x: x.rank)
        self.assertEquals(len(default_thumbs), 2)
        self.assertEquals(default_thumbs[1].key, '%s_thumb3' % self.video_id)
        self.assertEquals(default_thumbs[1].rank, 0)
        self.assertEquals(default_thumbs[0].rank, -1)
        self.assertRegexpMatches(default_thumbs[0].key, '%s_.+'%self.video_id)

        # Check the random thumb. There should only be one
        rand_thumb = [
            x for x in thumbs if x.type == neondata.ThumbnailType.RANDOM][0]
        self.assertNotEqual(rand_thumb.key, '%s_thumb2' % self.video_id)
        self.assertEquals(rand_thumb.rank, 0)
        self.assertIsNotNone(rand_thumb.phash)
        self.assertGreater(len(rand_thumb.urls), 0)

        # Check the neon thumbs. There should only be 2 and they
        # should both be new.
        n_thumbs = [x for x in thumbs if x.type == neondata.ThumbnailType.NEON]
        n_thumbs = sorted(n_thumbs, key= lambda x: x.rank)
        self.assertEquals(len(n_thumbs), 2)
        self.assertEquals(n_thumbs[0].frameno, 6)
        self.assertEquals(n_thumbs[1].frameno, 69)
        self.assertEquals(n_thumbs[0].model_version, 'model1')
        self.assertEquals(n_thumbs[1].model_version, 'model1')
        self.assertIsNotNone(n_thumbs[0].phash)
        self.assertRegexpMatches(n_thumbs[0].key, '%s_.+'%self.video_id)
        self.assertEquals(n_thumbs[0].urls, [
            'http://s3.amazonaws.com/host-thumbnails/%s.jpg' %
            re.sub('_', '/', n_thumbs[0].key)])
    
    def test_processing_after_requeue(self):
        '''
        Test processing video after a failed first attempt due to either internal error or
        failed 
        error (failed to download default thumb)
        '''
        
        # create basic videometadata object 
        video_meta = neondata.VideoMetadata(
            self.video_id,
            tids = [],
            duration=97.0,
            model_version='old_model',
            serving_enabled=False)
        video_meta.save()

        # Write the request to the db
        api_request = neondata.BrightcoveApiRequest(
            'job1', self.api_key, 'vid1',
            'some fun video',
            'http://video.mp4', None, None, 'pubid',
            'http://callback.com', 'int1',
            'http://default_thumb.jpg')
        
        for state in [neondata.RequestState.INT_ERROR,
                      neondata.RequestState.FAILED]:
            api_request.state = state 
            api_request.fail_count = 1
            api_request.save()

            self.vprocessor.finalize_response()

            # Make sure that the api request is updated
            api_request = neondata.NeonApiRequest.get('job1', self.api_key)
            self.assertEquals(api_request.state, 
                        neondata.RequestState.FINISHED)

            # Check the video metadata in the database
            video_data = neondata.VideoMetadata.get(self.video_id)
            self.assertEquals(video_data.url, 'http://video.mp4')
            self.assertEquals(video_data.integration_id, '0')
            self.assertTrue(video_data.serving_enabled)
            self.assertIsNone(video_data.serving_url)
            
            self.assertEqual(
                statemon.state.get('video_processor.client.default_thumb_error'),
                0)

    def test_default_thumb_already_saved(self):
        # Add the video and the default thumb to the database
        self.vprocessor.video_metadata.save()
        thumb_meta = neondata.ThumbnailMetadata(None,
                ttype=neondata.ThumbnailType.BRIGHTCOVE,
                rank=0,
                urls=['http://default_thumb.jpg'])
        thumb_meta = self.vprocessor.video_metadata.add_thumbnail(
            thumb_meta, self.random_image,
            save_objects=True)

        # Make sure that the db is updated before we run the finialization
        self.assertEquals(self.vprocessor.video_metadata,
                          neondata.VideoMetadata.get(self.video_id))
        self.assertEquals(len(self.vprocessor.video_metadata.thumbnail_ids), 1)
        self.assertEquals(thumb_meta, neondata.ThumbnailMetadata.get(
            self.vprocessor.video_metadata.thumbnail_ids[0]))
        self.assertIsNotNone(thumb_meta.key)

        self.vprocessor.finalize_response()

        # Check the video metadata in the database
        video_data = neondata.VideoMetadata.get(self.video_id)
        self.assertEquals(len(video_data.thumbnail_ids), 4)
        self.assertTrue(video_data.serving_enabled)
        self.assertIsNone(video_data.serving_url)

        # Check the thumbnails, we should only have one brightcove thumbnail
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.thumbnail_ids)
        default_thumb = [
            x for x in thumbs if x.type == neondata.ThumbnailType.BRIGHTCOVE]
        self.assertEquals(len(default_thumb), 1)
        default_thumb = default_thumb[0]
        self.assertGreater(len(default_thumb.urls), 1)
        self.assertEquals(default_thumb.rank, 0)
        self.assertEquals(default_thumb.phash, thumb_meta.phash)

    def test_no_thumbnails_found(self):
        self.vprocessor.thumbnails = []

        with self.assertLogExists(logging.WARNING, 'No thumbnails extracted'):
            self.vprocessor.finalize_response()

        # Make sure that serving is enabled
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertTrue(video_meta.serving_enabled)
        self.assertEquals(len(video_meta.thumbnail_ids), 1)

        self.assertEquals(neondata.ThumbnailMetadata.get(
            video_meta.thumbnail_ids[0]).type, 
            neondata.ThumbnailType.BRIGHTCOVE)

    def test_no_thumbnails_found_no_default_thumb(self):
        self.vprocessor.thumbnails = []
        neondata.BrightcoveApiRequest('job1', self.api_key, 'vid1',
                                      'some fun video',
                                      'http://video.mp4', None, None, 'pubid',
                                      'http://callback.com', 'int1',
                                      '').save()

        with self.assertLogExists(logging.WARNING, 'No thumbnails extracted'):
            self.vprocessor.finalize_response()

        # Make sure that serving is disabled
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertFalse(video_meta.serving_enabled)
        self.assertEquals(len(video_meta.thumbnail_ids), 0)

    @patch('video_processor.client.neondata.ThumbnailMetadata.modify_many')
    def test_db_connection_error_thumb(self, modify_mock):
        modify_mock.side_effect = [
            redis.ConnectionError("Connection Error"),
            {}
            ]

        with self.assertLogExists(logging.ERROR,
                                  'Error writing thumbnail data'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()

        self.api_request.state = neondata.RequestState.PROCESSING
        self.api_request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Error writing thumbnail data'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()

    @patch('video_processor.client.neondata.VideoMetadata.modify')
    def test_db_connection_error_video(self, modify_mock):
        modify_mock.side_effect = [
            redis.ConnectionError("Connection Error"),
            False
            ]

        with self.assertLogExists(logging.ERROR,
                                  'Error writing video data'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()

        self.api_request.state = neondata.RequestState.PROCESSING
        self.api_request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Error writing video data'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()

    def test_frame_already_in_dict(self):
        # Add the video and the default thumb to the database
        self.vprocessor.video_metadata.save()
        rand_thumb = self.vprocessor.thumbnails[-1]
        rand_thumb[0].urls = ['random_frame.jpg']
        self.vprocessor.video_metadata.add_thumbnail(
            rand_thumb[0], rand_thumb[1],
            save_objects=True)

        # Make sure that the db is updated before we run the finialization
        self.assertEquals(self.vprocessor.video_metadata,
                          neondata.VideoMetadata.get(self.video_id))
        self.assertEquals(len(self.vprocessor.video_metadata.thumbnail_ids), 1)
        self.assertEquals(rand_thumb[0], neondata.ThumbnailMetadata.get(
            self.vprocessor.video_metadata.thumbnail_ids[0]))
        self.assertIsNotNone(rand_thumb[0].key)

        # Reset the thumb we know about
        self.vprocessor.thumbnails[-1][0].urls = []
        
        self.vprocessor.finalize_response()

        # Check that the thumbnail was updated
        db_thumb = neondata.ThumbnailMetadata.get(rand_thumb[0].key)
        self.assertIn('random_frame.jpg', db_thumb.urls)
        self.assertEquals(len(db_thumb.urls), 2)

    @patch('video_processor.client.neondata.NeonApiRequest.modify')
    def test_api_request_update_fail(self, api_request_mock):
        api_request_mock.side_effect = [
            # Connection error on setting finalizing state
            redis.ConnectionError("Connection Error"), 
            # Api request missing on setting finalizing state
            None,
            #  Connection error on setting finished state
            self.api_request,
            redis.ConnectionError("Connection Error"),
            # Api request missing on setting finished state
            self.api_request,
            None,
        ]

        with self.assertLogExists(logging.ERROR,
                                  'Error writing request state'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()

        with self.assertLogExists(logging.ERROR,
                                  'Api Request finalizing failed'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()
        
        with self.assertLogExists(logging.ERROR,
                                  'Error writing request state'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()

        with self.assertLogExists(logging.ERROR,
                                  'Api Request finished failed'):
            with self.assertRaises(video_processor.client.DBError):
                self.vprocessor.finalize_response()

    def test_somebody_else_processed(self):

        # Try when somebody else was sucessful
        for state in [neondata.RequestState.FINISHED,
                      neondata.RequestState.SERVING,
                      neondata.RequestState.ACTIVE]:
            
            self.api_request.state = state
            self.api_request.save()
            with self.assertRaises(video_processor.client.OtherWorkerCompleted):
                self.vprocessor.finalize_response()
            self.assertEquals(
                neondata.NeonApiRequest.get('job1', self.api_key).state,
                state)

        # Try when the current run should continue
        for state in [neondata.RequestState.SUBMIT,
                      neondata.RequestState.REQUEUED,
                      neondata.RequestState.REPROCESS,
                      neondata.RequestState.FAILED,
                      neondata.RequestState.INT_ERROR]:
            self.api_request.state = state
            self.api_request.save()
            self.vprocessor.finalize_response()
            self.assertEquals(
                neondata.NeonApiRequest.get('job1', self.api_key).state,
                neondata.RequestState.FINISHED)

        
class SmokeTest(test_utils.neontest.AsyncTestCase):
    ''' 
    Smoke test for the video processing client
    '''
    def setUp(self):
        super(SmokeTest, self).setUp()
        statemon.state._reset_values()

        #Redis
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

        random.seed(984695198)

        # Fill out redis
        na = neondata.NeonUserAccount('acct1')
        self.api_key = na.neon_api_key
        na.save()
        neondata.NeonPlatform.modify(self.api_key, '0', 
                                     lambda x: x, create_missing=True)

        cdn = neondata.CDNHostingMetadataList(
            neondata.CDNHostingMetadataList.create_key(self.api_key, '0'),
            [neondata.NeonCDNHostingMetadata(rendition_sizes=[(160,90)])])
        cdn.save()

        self.video_id = '%s_vid1' % self.api_key
        self.api_request = neondata.OoyalaApiRequest(
            'job1', self.api_key,
            'int1', 'vid1',
            'some fun video',
            'http://video.mp4', None, None,
            'http://callback.com',
            'http://default_thumb.jpg')
        self.api_request.save()

        # Mock out the video download
        self.test_video_file = os.path.join(os.path.dirname(__file__), 
                                            "test.mp4") 
        self.video_download_patcher = patch('video_processor.client.urllib2.urlopen')
        self.video_download_mock = self.video_download_patcher.start()
        self.video_download_mock.side_effect = [open(self.test_video_file,
                                                     'rb')]

        # Mock out s3
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('host-thumbnails')
        self.s3conn.create_bucket('n3.neon-images.com')

        # Mock out the image download
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start(),
            require_async_kw=True)
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock.side_effect = [self.random_image]

        # Mock out http requests.
        self.http_mocker = patch(
            'video_processor.client.utils.http.send_request')
        self.http_mock = self._future_wrap_mock(self.http_mocker.start(),
                                                require_async_kw=True)
        self.callback_mock = MagicMock()
        self.callback_mock.side_effect = lambda x: HTTPResponse(x, 200)
        self.job_queue = multiprocessing.Queue() # Queue of job param dics
        def _http_response(request, **kw):
            if request.url.endswith('dequeue'):
                if not self.job_queue.empty():
                    body = json.dumps(self.job_queue.get())
                else:
                    body = '{}'
                return HTTPResponse(request, 200, buffer=StringIO(body))
            elif request.url == 'http://callback.com':
                return self.callback_mock(request)
            else:
                return HTTPResponse(request, 200)
                    
        self.http_mock.side_effect = _http_response

        # Mock out cloudinary
        self.cloudinary_patcher = patch('cmsdb.cdnhosting.CloudinaryHosting')
        self.cloudinary_mock = self.cloudinary_patcher.start()
        future = Future()
        future.set_result(None)
        self.cloudinary_mock().upload.side_effect = [future]

        # Mock out the model
        self.model_patcher = patch('video_processor.client.model.load_model')
        self.model_file = os.path.join(os.path.dirname(__file__), "model.pkl")
        self.model_version = "test" 
        self.model = MagicMock()
        load_model_mock = self.model_patcher.start()
        load_model_mock.return_value = self.model
        ct_output, ft_output = pickle.load(open(self.model_file)) 
        self.model.choose_thumbnails.return_value = ct_output

        # Mock the SQS implementation
        #self.mock_sqs = sqsmock.SQSConnectionMock()
        self.sqs_patcher = patch('video_processor.video_processing_queue.boto.sqs.' \
                                 'connect_to_region')
        #self.mock_sqs_future = self._future_wrap_mock(
        #                                 self.sqs_patcher.start(),
        #                                 require_async_kw=False)
        
        self.mock_sqs = self.sqs_patcher.start()
        self.mock_sqs.return_value = sqsmock.SQSConnectionMock()
        self.read_patcher = patch('video_processor.video_processing_queue.'\
                                  'VideoProcessingQueue.read_message')
        self.mock_read_future = self._future_wrap_mock(
            self.read_patcher.start(),
            require_async_kw=False)

        self.delete_patcher = patch('video_processor.video_processing_queue.'\
                                    'VideoProcessingQueue.delete_message')
        self.mock_delete_future = self._future_wrap_mock(
            self.delete_patcher.start(),
            require_async_kw=False)

        self.mock_delete_future.return_value = True
       
        # create the client object
        self.video_client = video_processor.client.VideoClient(
            'some/dir/my_model.model',
            multiprocessing.BoundedSemaphore(1))
        
    def tearDown(self):
        self.video_download_patcher.stop()
        self.s3_patcher.stop()
        self.http_mocker.stop()
        self.im_download_mocker.stop()
        self.cloudinary_patcher.stop()
        self.model_patcher.stop()
        self.redis.stop()
        self.sqs_patcher.stop()
        self.read_patcher.stop()
        self.delete_patcher.stop()
        super(SmokeTest, self).tearDown()

    @tornado.gen.coroutine
    def _run_job(self, job):
        '''Runs the job'''

        with options._set_bounded('video_processor.client.dequeue_period', 0.01):
            self.video_client.start()

            try:
                # Wait for the job results to show up in the database. We
                # can't check the mocks because it is a separate process
                # and the mocks just get copied. That's why this is a
                # smoke test.
                start_time = time.time()
                while (neondata.NeonApiRequest.get(job['job_id'],
                                                   job['api_key']).state in 
                       [neondata.RequestState.SUBMIT,
                        neondata.RequestState.PROCESSING,
                        neondata.RequestState.REPROCESS]):
                    # See if we timeout
                    self.assertLess(time.time() - start_time, 5.0,
                                    'Timed out while running the smoke test')

                    time.sleep(0.1)

            finally:
                # Clean up the job process
                self.video_client.stop()
                self.video_client.join(5.0)
                if self.video_client.is_alive():
                    # SIGKILL it
                    utils.ps.send_signal_and_wait(signal.SIGKILL,
                                                  [self.video_client.pid])
                    self.fail('The subprocess did not die cleanly')

    @tornado.testing.gen_test(timeout=10)
    def test_smoke_test(self):
        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://L\xc3\xb6rick_video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        job = json.loads(message.get_body())

        yield self._run_job(job)
                    
        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.FINISHED)

        # Check the video data
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertGreater(len(video_meta.thumbnail_ids), 0)
        self.assertEquals(video_meta.model_version, 'my_model')

        # Check the thumbnail data
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_meta.thumbnail_ids)
        self.assertNotIn(None, thumbs)
        self.assertGreater(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.NEON]), 0)
        self.assertEquals(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.OOYALA]), 1)
        self.assertEquals(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.RANDOM]), 1)
        self.assertEquals(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.CENTERFRAME]), 1)

    @tornado.testing.gen_test(timeout=10)
    def test_reprocessing_smoke(self):
        self.api_request.state = neondata.RequestState.REPROCESS
        self.api_request.save()

        # Add the results from the previous run to the database
        thumbs = [
            neondata.ThumbnailMetadata(
                '%s_thumb1' % self.video_id,
                self.video_id,
                model_score=3.0,
                ttype=neondata.ThumbnailType.NEON,
                model_version='old_model',
                frameno=167,
                rank=0),
            neondata.ThumbnailMetadata(
                '%s_thumb2' % self.video_id,
                self.video_id,
                ttype=neondata.ThumbnailType.RANDOM,
                rank=0),
            neondata.ThumbnailMetadata(
                '%s_thumb3' % self.video_id,
                self.video_id,
                ttype=neondata.ThumbnailType.OOYALA,
                rank=0)]
        neondata.ThumbnailMetadata.save_all(thumbs)
        video_meta = neondata.VideoMetadata(
            self.video_id,
            tids = [x.key for x in thumbs],
            duration=97.0,
            model_version='old_model')
        video_meta.serving_url = 'my_serving_url.jpg'
        video_meta.save()

        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        job = json.loads(message.get_body())

        yield self._run_job(job)
        
        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.FINISHED)

        # Check the video data
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertGreater(len(video_meta.thumbnail_ids), 0)
        self.assertEquals(video_meta.model_version, 'my_model')
        self.assertNotIn('%s_thumb1' % self.video_id, video_meta.thumbnail_ids)
        self.assertNotIn('%s_thumb2' % self.video_id, video_meta.thumbnail_ids)
        self.assertIn('%s_thumb3' % self.video_id, video_meta.thumbnail_ids)
        
    @tornado.testing.gen_test(timeout=10)
    def test_video_processing_error(self):
        self.video_download_mock.side_effect = [
            urllib2.URLError('Oops')]

        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        job = json.loads(message.get_body())

        yield self._run_job(job)

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.CUSTOMER_ERROR)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.SUCESS)

        # Check the state variables
        self.assertEquals(statemon.state.get('video_processor.client.processing_error'),
                          1)
        self.assertEquals(
            statemon.state.get('video_processor.client.video_download_error'),
            1)

    @tornado.testing.gen_test(timeout=10)
    @patch('video_processor.client.neondata.VideoMetadata.modify')
    def test_db_update_error(self, modify_mock):
        modify_mock.side_effect = [
            redis.ConnectionError("Connection Error")]

        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)
        self.mock_read_future.return_value = message
        job = json.loads(message.get_body())

        yield self._run_job(job)

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.INT_ERROR)

        # Check the state variables
        self.assertEquals(statemon.state.get('video_processor.client.processing_error'),
                          1)
        self.assertEquals(
            statemon.state.get('video_processor.client.save_vmdata_error'),
            1)

    @tornado.testing.gen_test(timeout=10)
    def test_no_need_to_process(self):
        self.api_request.state = neondata.RequestState.SERVING
        self.api_request.save()

        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        job = json.loads(message.get_body())

        yield self._run_job(job)
        
        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.SERVING)
    
    @tornado.testing.gen_test(timeout=10)
    def test_download_default_thumb_error(self):
        # In this case, we should still allow the video serve, but
        # register it as a customer error in the database.
        self.im_download_mock.side_effect = [IOError('Cannot download')]
        

        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        job = json.loads(message.get_body())

        yield self._run_job(job)

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.CUSTOMER_ERROR)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.SUCESS)
        response = api_request.response
        self.assertEquals(response['video_id'], 'vid1')
        self.assertEquals(response['job_id'], 'job1')
        self.assertRegexpMatches(response['error'],
                                 'Failed to download default')

        # Check the video data
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertGreater(len(video_meta.thumbnail_ids), 0)
        self.assertTrue(video_meta.serving_enabled)

        
        # Check the thumbnail data
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_meta.thumbnail_ids)
        self.assertNotIn(None, thumbs)
        self.assertGreater(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.NEON]), 0)
        self.assertEquals(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.OOYALA]), 0)
        self.assertEquals(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.DEFAULT]), 0)
        self.assertEquals(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.RANDOM]), 1)
        self.assertEquals(
            len([x for x in thumbs if 
                 x.type == neondata.ThumbnailType.CENTERFRAME]), 1)

    @tornado.testing.gen_test(timeout=10)
    def test_unexpected_error(self):
        self.video_download_mock.side_effect = [Exception('Some bad error')]


        message = Message()

        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        message.set_body(message_body)

        self.mock_read_future.return_value = message

        job = json.loads(message.get_body())

        yield self._run_job(job)

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.INT_ERROR)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.NOT_SENT)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
