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
import cv2
from cvutils.imageutils import PILImageUtils
import integrations
from itertools import chain
import json
import logging
from mock import MagicMock, patch, ANY
import model.errors
import model
import multiprocessing
import numpy as np
import os
import pickle
from PIL import Image
import psycopg2
import Queue
import re
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
import test_utils.opencv
import test_utils.postgresql
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
from cvutils import imageutils
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon
import video_processor.client
import video_processor.video_processing_queue
from video_processor.client import VideoClient
import youtube_dl

_log = logging.getLogger(__name__)

def _get_good_model_return_value():
    return [
        model.VideoThumbnail(frameno=20, score=0.2,
               image=PILImageUtils.to_cv(
                   PILImageUtils.create_random_image(300,300)),
               model_version='test_model', features=np.random.randn(1024)),
        model.VideoThumbnail(frameno=42, score=0.3,
               image=PILImageUtils.to_cv(
                   PILImageUtils.create_random_image(300,300)),
               model_version='test_model', features=np.random.randn(1024))
            ]

def _get_good_model_return_value_low_score():
    return [
        model.VideoThumbnail(frameno=10, score=-0.2,
               image=PILImageUtils.to_cv(
                   PILImageUtils.create_random_image(300,300)),
               model_version='test_model', features=np.random.randn(1024)),
        model.VideoThumbnail(frameno=88, score=-0.3,
               image=PILImageUtils.to_cv(
                   PILImageUtils.create_random_image(300,300)),
               model_version='test_model', features=np.random.randn(1024))
            ]

class TestVideoClient(test_utils.neontest.AsyncTestCase):
    ''' 
    Test Video Processing client
    '''
    def setUp(self):
        super(TestVideoClient, self).setUp()
        
        #setup properties,model
        self.model_version = "test" 
        self.model = MagicMock()

        #Mock Model methods,
        self.model.choose_thumbnails.return_value = \
          _get_good_model_return_value(), _get_good_model_return_value_low_score()
        self.test_video_file = os.path.join(os.path.dirname(__file__), 
                                "test.mp4") 
        self.test_video_file2 = os.path.join(os.path.dirname(__file__), 
                                "test2.mp4") 
        self.predictor_patcher = patch(
            'video_processor.client.model.predictor.DeepnetPredictor')
        self.model.predictor = self.predictor_patcher.start()()
        self.predict_mock = self._future_wrap_mock(
            self.model.predictor.predict, require_async_kw=True)
        self.predict_mock.return_value = (99, None, 'model1')
        
        # Fill out database
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
            'int1', 'vid1',
            'some fun video',
            'http://video.mp4', None, None,
            'http://callback.com',
            'http://default_thumb.jpg')
        self.api_request.save() 

        # Mock out the YoutubeDL
        self.youtube_patcher = patch(
            'video_processor.client.youtube_dl.YoutubeDL')
        self.youtube_client_mock = self.youtube_patcher.start()
        self.youtube_extract_info_mock = \
            self.youtube_client_mock().__enter__().extract_info
        self.youtube_extract_info_mock.return_value = {
            u'_type': u'video',
            u'id': 'yces6PZOsgc', 
            u'title': 'my_video',
            u'url': 'http://www.video.com/my_video.mp4'}

        # Mock the video queue
        self.job_queue_patcher = patch(
            'video_processor.video_processing_queue.' \
            'VideoProcessingQueue')
        self.job_queue_mock = self.job_queue_patcher.start()()

        self.job_queue_mock.get_duration.return_value = 600.0

        self.job_delete_mock = self._future_wrap_mock(
            self.job_queue_mock.delete_message)
        self.job_delete_mock.return_value = True
        
        self.job_read_mock = self._future_wrap_mock(
            self.job_queue_mock.read_message)
        self.job_message = Message()
        message_body = json.dumps({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4'
            })

        self.job_message.set_body(message_body)
        self.job_read_mock.side_effect = [self.job_message]
        
        
        self.job_hide_mock = self._future_wrap_mock(
            self.job_queue_mock.hide_message)

        #patch for download_and_add_thumb
        self.utils_patch = patch('cmsdb.neondata.utils.http.send_request')
        self.uc = self._future_wrap_mock(self.utils_patch.start(),
                                         require_async_kw=True)

        # Mock out the aquila lookup
        self.aquila_conn_patcher = patch(
            'video_processor.client.utils.autoscale')
        self.aquila_conn_patcher.start()

        # create the client object
        self.video_client = video_processor.client.VideoClient(
            'some/dir/my_model.model',
            multiprocessing.BoundedSemaphore(1))

        random.seed(984695198)
        
    def tearDown(self):
        self.job_queue_patcher.stop()
        self.aquila_conn_patcher.stop()
        self.youtube_patcher.stop()
        self.utils_patch.stop()
        self.predictor_patcher.stop()
        self.postgresql.clear_all_tables()
        super(TestVideoClient, self).tearDown()

    @classmethod
    def setUpClass(cls):
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()

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
            integration = neondata.BrightcoveIntegration(
                api_key, 
                application_client_id='client',
                application_client_secret='secret')
            integration.save()
            i_id = integration.integration_id
            jparams = request_template.brightcove_api_request %(
                j_id, vid, api_key, "brightcove", api_key, j_id, i_id)
            self.api_request = neondata.BrightcoveApiRequest(
                                        j_id, api_key, vid, 
                                        'title', url,
                                        'rtok', 'wtok', None) 
            self.api_request.previous_thumbnail = "http://prevthumb"
            
        elif request_type == "ooyala":
            i_id = "oid"
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
        vprocessor = video_processor.client.ThumbnailProcessor(
            job,
            self.model,
            self.model_version,
            multiprocessing.BoundedSemaphore(1),
            self.job_queue_mock,
            self.job_message)
        
        return vprocessor

    ##### Process video tests ####
    @tornado.testing.gen_test
    def test_download_video_file(self):

        self.youtube_extract_info_mock.return_value = {
            u'_type': u'video',
            u'upload_date': u'20110620', 
            u'protocol': u'https', 
            u'creator': None, 
            u'format_note': u'hd720', 
            u'height': 720, 
            u'like_count': 0, 
            u'player_url': None, 
            u'id': 'yces6PZOsgc', 
            u'view_count': 328}

        vprocessor = self.setup_video_processor("neon",
                                                'http://www.somefile.com/')
        yield vprocessor.download_video_file()
        args, kwargs = self.youtube_client_mock.call_args
        found_params = args[0]
        self.assertTrue(found_params['restrictfilenames'])
        self.assertGreater(len(found_params['progress_hooks']), 0)
        # This test is to make sure you are deliberately changing the
        # format parameters
        self.assertEquals(found_params['format'],(
            'best[ext=mp4][height<=720][protocol^=?http]/'
            'best[ext=mp4][protocol^=?http]/'
            'best[height<=720][protocol^=?http]/'
            'best[protocol^=?http]/'
            'best/'
            'bestvideo'))
        self.youtube_extract_info_mock.assert_called_with(
            'http://www.somefile.com/',
            download=True)
        self.assertIsNone(vprocessor.extracted_default_thumbnail)

        self.job_hide_mock.assert_called_with(self.job_message,
                                              3.0*600.0)

    @tornado.testing.gen_test
    def test_default_thumb_found_in_video(self):
        self.youtube_extract_info_mock.return_value = {
            u'_type': u'video',
            u'id': 'yces6PZOsgc', 
            u'title': 'my_video',
            u'url': 'http://www.video.com/my_video.mp4',
            u'thumbnail': 'http://my_default_thumbnail.jpg'}

        vprocessor = self.setup_video_processor("neon",
                                                'http://www.somefile.com/')
        yield vprocessor.download_video_file()
        self.assertEquals(vprocessor.extracted_default_thumbnail,
                          'http://my_default_thumbnail.jpg')

    @patch('video_processor.client.integrations.create_ovp_integration')
    @tornado.testing.gen_test
    def test_download_video_moved(self, ovp_mock):
        # When a video cannot be downloaded and it's on an
        # integration, we see if that video has moved and try again.
        valid_response = {
            u'_type': u'video',
            u'upload_date': u'20110620', 
            u'protocol': u'https', 
            u'creator': None, 
            u'format_note': u'hd720', 
            u'height': 720, 
            u'like_count': 0, 
            u'player_url': None, 
            u'id': 'yces6PZOsgc', 
            u'view_count': 328}
            
        self.youtube_extract_info_mock.side_effect = [
            youtube_dl.utils.DownloadError('not there'),
            valid_response,
            valid_response
            ]

        lookup_mock = self._future_wrap_mock(ovp_mock().lookup_videos)
        lookup_mock.side_effect = [[{'url' : 'http://new_url.com'}]]
        ovp_mock().get_video_url.side_effect = lambda x: x['url']       
            
        vprocessor = self.setup_video_processor("brightcove")

        with self.assertLogExists(logging.INFO, 
                                  'Trying to download at its new location'):
            yield vprocessor.download_video_file()

        # Make sure the video was downloaded from the new url
        self.youtube_extract_info_mock.assert_called_with(
            'http://new_url.com',
            download=True)
        self.assertEquals(self.youtube_extract_info_mock.call_count, 3)

        # Make sure the integration was built properly
        bc_int = neondata.BrightcoveIntegration.get(
            vprocessor.job_params['integration_id'])
        ovp_mock.assert_called_with(self.na.neon_api_key, bc_int)
        lookup_mock.assert_called_with(['video1'])

        self.assertEquals(vprocessor.video_metadata.url, 
                          'http://new_url.com')

    @tornado.testing.gen_test
    def test_download_video_errors(self):
        self.youtube_extract_info_mock.side_effect = [
            youtube_dl.utils.DownloadError('bal'),
            youtube_dl.utils.ExtractorError('beck'),
            youtube_dl.utils.UnavailableVideoError('ick'),
            socket.gaierror(),
            IOError()
            ]
        
        vprocessor = self.setup_video_processor("neon")
        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                yield vprocessor.download_video_file()

        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                yield vprocessor.download_video_file()

        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                yield vprocessor.download_video_file()

        with self.assertLogExists(logging.ERROR, "Error downloading video"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                yield vprocessor.download_video_file()
        
        with self.assertLogExists(logging.ERROR, "Error saving video to disk"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                yield vprocessor.download_video_file()
                

    @patch('video_processor.client.S3Connection')
    @tornado.testing.gen_test
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
        yield vprocessor.download_video_file()
        vprocessor.tempfile.seek(0) 
        self.assertEqual(vprocessor.tempfile.read(), vdata)

        self.job_hide_mock.assert_called_with(self.job_message,
                                              3.0*600.0)

    @patch('video_processor.client.S3Connection')
    @tornado.testing.gen_test
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
        yield vprocessor.download_video_file()
        vprocessor.tempfile.seek(0) 
        self.assertEqual(vprocessor.tempfile.read(), vdata)

        self.job_hide_mock.assert_called_with(self.job_message,
                                              3.0*600.0)

    @patch('video_processor.client.S3Connection')
    @tornado.testing.gen_test
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
                yield vprocessor.download_video_file()
        
        with self.assertLogExists(logging.ERROR, "Server error downloading"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                yield vprocessor.download_video_file()
        
        with self.assertLogExists(logging.ERROR, "Error saving video to disk"):
            with self.assertRaises(video_processor.client.VideoDownloadError):
                yield vprocessor.download_video_file()

    @patch('video_processor.client.S3Connection')
    @tornado.testing.gen_test
    def test_download_s3_error_with_valid_httpfallback(self, s3_mock):
        s3_mock.side_effect = [
            boto.exception.S3ResponseError(402, "Permissions error")
            ]

        url = 'https://s3-us-west-2.amazonaws.com/customer-videos/some/video.mp4'

        vprocessor = self.setup_video_processor(
            "neon", url=url)

        with self.assertLogExists(logging.WARNING, 'Falling back on http'):
            yield vprocessor.download_video_file()

        # Make sure the video was downloaded from the url
        self.youtube_extract_info_mock.assert_called_with(url, download=True)

        self.assertEquals(vprocessor.video_metadata.url, url)

    @tornado.testing.gen_test
    def test_download_youtube_video_with_duration_and_title(self):
        self.youtube_extract_info_mock.return_value = {
            u'_type': u'video',
            u'upload_date': u'20110620', 
            u'protocol': u'https', 
            u'creator': None, 
            u'format_note': u'hd720', 
            u'height': 720, 
            u'like_count': 0, 
            u'duration': 15, 
            u'player_url': None, 
            u'id': 'yces6PZOsgc', 
            u'view_count': 328,
            u'title': 'new_title'}
        vprocessor = self.setup_video_processor(
            "neon", url='http://www.youtube.com/watch?v=9bZkp7q19f0')
        self.api_request.video_title = None
        self.api_request.save()
        
        yield vprocessor.download_video_file()
        self.assertEquals(vprocessor.video_metadata.duration, 15)
        self.job_hide_mock.assert_called_with(self.job_message,
                                              3.0*15)

        vid_meta = neondata.VideoMetadata.get(vprocessor.video_metadata.key)
        self.assertEquals(vid_meta.duration, 15.0)

        request = neondata.NeonApiRequest.get(vid_meta.job_id,
                                              self.na.neon_api_key)
        self.assertEquals(request.video_title, 'new_title')

    @tornado.testing.gen_test
    def test_download_youtube_video_missing_duration(self):
        self.youtube_extract_info_mock.return_value = {
            u'_type': u'video',
                u'upload_date': u'20110620', 
                u'protocol': u'https', 
                u'creator': None, 
                u'format_note': u'hd720', 
                u'height': 720, 
                u'like_count': 0, 
                u'player_url': None, 
                u'id': 'yces6PZOsgc', 
                u'view_count': 328}
        vprocessor = self.setup_video_processor(
            "neon", url='http://www.youtube.com/watch?v=9bZkp7q19f0')
        
        yield vprocessor.download_video_file()
        self.assertEquals(vprocessor.video_metadata.duration, 600.0)

        self.job_hide_mock.assert_called_with(self.job_message,
                                              3.0*600.0)

    @tornado.testing.gen_test
    def test_download_youtube_with_list_param(self):

        url = 'https://www.youtube.com/watch?v=k8tzfpeyPzw'
        url_with_list = url + '&list=%s&index=%d' % (
            'PLV4p_y6bK69puhKH0ddPgGNlEy3jMaRSw',
            8)

        def _extracted_info(cur_url, download):
            if cur_url == url:
                return {
                    u'_type': u'video',
                    u'upload_date': u'20140415',
                    u'protocol': u'https',
                    u'height': 720,
                    u'like_count': 118845,
                    u'player_url': u'//s.ytimg.com/yts/jsbin/player-en_US-vflIB5TLK/base.js',
                    u'id': 'k8tzfpeyPzw',
                    u'view_count': 1100328,
                    u'webpage_url': u'https://www.youtube.com/watch?v=k8tzfpeyPzw'}
            elif cur_url == url_with_list:
                return {
                    u'_type': u'playlist',
                    u'upload_date': u'20140415',
                    u'protocol': u'https',
                    u'height': 720,
                    u'like_count': 118845,
                    u'player_url': u'//s.ytimg.com/yts/jsbin/player-en_US-vflIB5TLK/base.js',
                    u'id': 'k8tzfpeyPzw',
                    u'view_count': 1100328,
                    u'webpage_url': u'https://www.youtube.com/watch?v=k8tzfpeyPzw'}
            raise Error('Wrong')
        self.youtube_extract_info_mock.side_effect = _extracted_info

        vp = self.setup_video_processor('neon', url=url_with_list)
        yield vp.download_video_file()
        # We expect the url without the list and index params.
        self.assertEqual(url, self.youtube_extract_info_mock.call_args[0][0])
        self.assertTrue(self.youtube_extract_info_mock.call_args[1]['download'])

    @tornado.testing.gen_test
    def test_no_download_youtube_playlist(self):
        url = u'https://www.youtube.com/playlist?list=PLV4p_y6bK69puhKH0ddPgGNlEy3jMaRSw'
        self.youtube_extract_info_mock.return_value = {
            u'_type': u'playlist',
            u'entries': [1, 2],
            u'webpage_url': url}
        vp = self.setup_video_processor('neon', url=url)
        with self.assertRaises(video_processor.client.VideoDownloadError) as e:
            yield vp.download_video_file()
        self.assertRegexpMatches(e.exception.message, r'(?i)Unhandled video type')

    @patch('video_processor.client.model.generate_model')
    @tornado.testing.gen_test
    def test_fail_and_retry(self, model_mock):
        self.youtube_extract_info_mock.side_effect = [
            youtube_dl.utils.DownloadError('bal'),
            youtube_dl.utils.ExtractorError('beck'),
            youtube_dl.utils.UnavailableVideoError('ick')]

        yield neondata.AccountLimits(
            self.api_key, 
            video_posts=9).save(async=True) 

        self.job_read_mock.side_effect = [self.job_message,
                                          self.job_message,
                                          self.job_message]

        with options._set_bounded('video_processor.client.max_fail_count', 2):
            yield self.video_client.do_work(async=True)
            
            self.assertEquals(self.video_client.videos_processed, 1)
            api_request = neondata.NeonApiRequest.get('job1', self.api_key)
            self.assertIsNotNone(api_request.response['error'])
            self.assertEquals(api_request.state,
                              neondata.RequestState.REQUEUED)
            self.assertEquals(api_request.fail_count, 1)
            self.job_hide_mock.assert_called_with(self.job_message, 5.0)
            self.job_hide_mock.reset_mock()

            yield self.video_client.do_work(async=True)
            self.assertEquals(self.video_client.videos_processed, 2)
            api_request = neondata.NeonApiRequest.get('job1', self.api_key)
            self.assertIsNotNone(api_request.response['error'])
            self.assertEquals(api_request.state,
                              neondata.RequestState.CUSTOMER_ERROR)
            self.assertEquals(api_request.fail_count, 2)
            self.job_hide_mock.assert_not_called()
            self.job_delete_mock.assert_called_with(self.job_message)
            acct_limits = yield neondata.AccountLimits.get(
                self.api_key, 
                async=True)
            self.assertEquals(acct_limits.video_posts, 8) 

    @tornado.testing.gen_test
    def test_process_video(self):
        '''Verify execution of the process_all call in ProcessVideo'''

        vprocessor = self.setup_video_processor("neon", url='http://video.com')
        vprocessor.n_thumbs = 6
        vprocessor.m_thumbs = 6
        yield vprocessor.process_video(self.test_video_file)

        # Check that the model was called correctly
        self.assertTrue(self.model.choose_thumbnails.called)
        cargs, kwargs = self.model.choose_thumbnails.call_args
        self.assertEquals(kwargs, {
            'n': 6,
            'm': 6,
            'video_name': 'http://video.com'})
        self.assertEquals(len(cargs), 1)

        #verify video metadata has been populated
        self.assertEqual(vprocessor.video_metadata.duration, 8.8)
        self.assertEqual(vprocessor.video_metadata.frame_size, (400, 264))

        #verify that the thumbnails were populated
        self.assertEqual(4, len(vprocessor.thumbnails))
        self.assertEqual(2, len(vprocessor.bad_thumbnails))
        self.assertEqual(2, len([x for x in vprocessor.thumbnails if
                                 x[0].type == neondata.ThumbnailType.NEON]))
        self.assertEqual(1, len([x for x in vprocessor.thumbnails if
                                 x[0].type == neondata.ThumbnailType.RANDOM]))
        self.assertEqual(1, len([x for x in vprocessor.thumbnails if
                                 x[0].type == neondata.ThumbnailType.CENTERFRAME]))
        self.assertNotIn(float('-inf'), [x[0].model_score for x in vprocessor.thumbnails])
        self.assertTrue(all([x[0].video_id == vprocessor.video_metadata.key 
                             for x in vprocessor.thumbnails]))
        self.assertTrue(all([x[0].video_id == vprocessor.video_metadata.key 
                             for x in vprocessor.bad_thumbnails]))


        # Assert scores are reasonable: sorted by score.
        self.assertTrue(all(a[0].model_score > b[0].model_score) for a, b in zip(
            vprocessor.thumbnails,
            vprocessor.thumbnails[1:]))
        self.assertTrue(all(a[0].model_score < b[0].model_score) for a, b in zip(
            vprocessor.bad_thumbnails,
            vprocessor.bad_thumbnails[1:]))
        
        # The best bad is worse than the worst good.
        self.assertLess(
            max([t[0].model_score for t in vprocessor.bad_thumbnails]),
            min([t[0].model_score for t in vprocessor.thumbnails 
                 if t[0].model_score is not None]))

    @tornado.testing.gen_test
    def test_somebody_else_processed_first(self):
        # Try when somebody else was sucessful
        for state in [neondata.RequestState.FINISHED,
                      neondata.RequestState.SERVING,
                      neondata.RequestState.ACTIVE]:
            vprocessor = self.setup_video_processor('neon')
            self.api_request.state = state
            self.api_request.save()
            with self.assertRaises(video_processor.client.OtherWorkerCompleted):
                yield vprocessor.process_video(self.test_video_file)

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
            yield vprocessor.process_video(self.test_video_file)
            self.assertGreater(len(vprocessor.thumbnails), 0)

    @tornado.testing.gen_test
    def test_missing_video_file(self):
        vprocessor = self.setup_video_processor("neon")

        with self.assertLogExists(logging.ERROR, "Error reading"):
            with self.assertRaises(video_processor.client.BadVideoError):
                yield vprocessor.process_video(
                    'a_garbage_video_thats_gone.mov')

    @tornado.testing.gen_test
    def test_duration_too_long(self): 
        vprocessor = self.setup_video_processor('neon')
        al = neondata.AccountLimits(
            vprocessor.job_params['api_key'],
            max_video_size=2.0)
        yield al.save(async=True) 
        yield vprocessor.video_metadata.save(async=True)
        with self.assertRaises(video_processor.client.BadVideoError) as e:
            yield vprocessor.process_video(self.test_video_file)

        api_request = yield neondata.NeonApiRequest.get(
            vprocessor.job_params['job_id'],
            vprocessor.job_params['api_key'],
            async=True)
        self.assertEquals(api_request.fail_count,
                          options.get('video_processor.client.max_fail_count'))

    @tornado.testing.gen_test
    def test_process_all_filtered_video(self):
        '''Test processing a video where every frame is filtered.'''
        self.model.choose_thumbnails.return_value = [
            model.VideoThumbnail(np.zeros((480, 640, 3), np.uint8),
                                 float('-inf'), 120,
                                 filtered_reason='black'),
            model.VideoThumbnail(np.zeros((480, 640, 3), np.uint8),
                                 float('-inf'), 600,
                                 filtered_reason='black')], []
        vprocessor = self.setup_video_processor("neon")
        vprocessor.n_thumbs = 2
        yield vprocessor.process_video(self.test_video_file2)

        # Verify that all the frames were added to the data maps
        neon_thumbs = [x[0] for x in vprocessor.thumbnails if
                       x[0].type == neondata.ThumbnailType.NEON]
        self.assertEquals(len(neon_thumbs), 2)
        self.assertEquals([x.model_score for x in neon_thumbs],
                          [float('-inf'), float('-inf')])
        self.assertEquals([x.filtered for x in neon_thumbs],
                          ['black', 'black'])

    @tornado.testing.gen_test
    def test_choose_thumbnail_errors(self):
        self.model.choose_thumbnails.side_effect = [
            model.errors.VideoReadError('Oops'),
            model.errors.PredictionError('Bad connection')]

        vprocessor = self.setup_video_processor("neon")
        with self.assertLogExists(logging.ERROR, 'Error using OpenCV'):
            with self.assertRaises(video_processor.client.BadVideoError):
                yield vprocessor.process_video(self.test_video_file)

        with self.assertRaises(video_processor.client.PredictionError):
            yield vprocessor.process_video(self.test_video_file)
                

    @tornado.testing.gen_test
    def test_get_center_frame(self):
        '''
        Test center frame extraction
        '''
        
        jparams = request_template.neon_api_request %(
                    "j_id", "vid", "api_key", "neon", "api_key", "j_id")
        job = json.loads(jparams)
        vprocessor = video_processor.client.ThumbnailProcessor(
            job,
            self.model,
            self.model_version, multiprocessing.BoundedSemaphore(1),
            self.job_queue_mock,
            self.job_message)
        mov = cv2.VideoCapture(self.test_video_file)
        try:
            yield vprocessor._get_center_frame(mov)
        finally:
            mov.release()
        meta, img = vprocessor.thumbnails[0]
        self.assertIsNotNone(img)
        self.assertTrue(isinstance(img, Image.Image))
        self.assertEqual(meta.type, neondata.ThumbnailType.CENTERFRAME)
        self.assertEqual(meta.rank, 0)
        self.assertEqual(meta.frameno, 66)

    @tornado.testing.gen_test
    def test_get_random_frame(self):
        '''
        Test random frame extraction
        '''
        
        jparams = request_template.neon_api_request %(
                    "j_id", "vid", "api_key", "neon", "api_key", "j_id")
        job = json.loads(jparams)
        vprocessor = video_processor.client.ThumbnailProcessor(
            job,
            self.model,
            self.model_version,
            multiprocessing.BoundedSemaphore(1),
            self.job_queue_mock,
            self.job_message
            )
        mov = cv2.VideoCapture(self.test_video_file)
        try:
            yield vprocessor._get_random_frame(mov)
            meta1, img1 = vprocessor.thumbnails[0]
            self.assertIsNotNone(img1)
            self.assertTrue(isinstance(img1, Image.Image))
            self.assertEqual(meta1.type, neondata.ThumbnailType.RANDOM)
            self.assertEqual(meta1.rank, 0)

            yield vprocessor._get_random_frame(mov)
            meta2, img2 = vprocessor.thumbnails[1]
            self.assertNotEqual(meta2.frameno, meta1.frameno)
        finally:
            mov.release()

    @tornado.testing.gen_test
    def test_dequeue_job(self):

        with self.assertLogExists(logging.DEBUG, "Dequeue Successful"):
            job = yield self.video_client.dequeue_job()

        self.assertEqual(job, {
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4',
            'reprocess' : False
            })

    @tornado.testing.gen_test
    def test_dequeue_job_with_empty_server(self):
        self.job_read_mock.side_effect = [None]
        with self.assertRaises(Queue.Empty) as cm:
            yield self.video_client.dequeue_job()

    @tornado.testing.gen_test
    def test_dequeue_job_with_failed_attempts(self):

        def _change_job_state(request):
            request.fail_count = 4
            request.state = neondata.RequestState.PROCESSING
                      
        req = neondata.NeonApiRequest.modify('job1', self.api_key, 
                                             _change_job_state)
        with self.assertLogExists(logging.ERROR, 'has failed too many times'):
            yield self.video_client.do_work(async=True)

        # Make sure the job was deleted
        self.job_delete_mock.assert_called_with(self.job_message)

    @tornado.testing.gen_test
    def test_dequeue_job_with_too_many_attempts(self):

        def _change_job_state(request):
            request.try_count = 7
            request.fail_count = 1
            request.state = neondata.RequestState.PROCESSING
                      
        req = neondata.NeonApiRequest.modify('job1', self.api_key, 
                                             _change_job_state)
        with self.assertLogExists(logging.ERROR, 'has failed too many times'):
            yield self.video_client.do_work(async=True)

        # Make sure the job was deleted
        self.job_delete_mock.assert_called_with(self.job_message)

    @tornado.testing.gen_test
    def test_dequeue_job_when_missing_from_db(self):
        neondata.NeonApiRequest.delete(self.api_request.job_id, self.api_key)

        with self.assertLogExists(logging.ERROR, 'Could not get job'):
            with self.assertRaises(video_processor.client.DequeueError):
                job = yield self.video_client.dequeue_job()

    @tornado.testing.gen_test
    def test_dequeue_invalid_job(self):
        self.job_read_mock.side_effect = [self.job_message,
                                          self.job_message,]
        
        self.job_message.set_body('{}')
        with self.assertLogExists(logging.WARNING, 'Job body .* uninteresting'):
            yield self.video_client.do_work(async=True)

        # Make sure the job was deleted
        self.job_delete_mock.assert_called_with(self.job_message)
        self.job_delete_mock.reset_mock()


        self.job_message.set_body('sad days')
        with self.assertLogExists(logging.WARNING, 'Job body .* was not JSON'):
            yield self.video_client.do_work(async=True)

        # Make sure the job was deleted
        self.job_delete_mock.assert_called_with(self.job_message)
        
    @tornado.testing.gen_test
    def test_dequeue_job_somebody_else_finished(self):

        def _change_job_state(request):
            request.state = neondata.RequestState.FINISHED
                      
        req = neondata.NeonApiRequest.modify('job1', self.api_key, 
                                             _change_job_state)
        with self.assertLogExists(logging.INFO, 'Dequeued a job that'):
            yield self.video_client.do_work(async=True)

        # Make sure the job was deleted
        self.job_delete_mock.assert_called_with(self.job_message)

    @tornado.testing.gen_test
    def test_dequeue_job_already_serving(self):

        def _change_job_state(request):
            request.state = neondata.RequestState.SERVING
                      
        req = neondata.NeonApiRequest.modify('job1', self.api_key, 
                                             _change_job_state)
        with self.assertLogExists(logging.INFO, 'Dequeued a job that'):
            yield self.video_client.do_work(async=True)

        # Make sure the job was deleted
        self.job_delete_mock.assert_called_with(self.job_message)

    @tornado.testing.gen_test
    def test_dequeue_job_somebody_else_finalizing(self):

        def _change_job_state(request):
            request.state = neondata.RequestState.FINALIZING
                      
        req = neondata.NeonApiRequest.modify('job1', self.api_key, 
                                             _change_job_state)

        with self.assertLogExists(logging.DEBUG, "Dequeue Successful"):
            job = yield self.video_client.dequeue_job()

        self.assertEqual(job, {
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 'http://video.mp4',
            'reprocess' : False
            })

        self.assertEquals(
            neondata.NeonApiRequest.get('job1', self.api_key).state,
            neondata.RequestState.PROCESSING)

        # Make sure the job was not deleted yet
        self.job_delete_mock.assert_not_called()


class TestFinalizeResponse(test_utils.neontest.AsyncTestCase):
    ''' 
    Test the cleanup and responding after the video has been processed
    '''
    def setUp(self):
        super(TestFinalizeResponse, self).setUp()

        statemon.state._reset_values()

        random.seed(984695198)
        
        # Mock out s3
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('host-thumbnails')
        self.s3conn.create_bucket('n3.neon-images.com')

        # Mock out the image download
        self.im_download_mocker = patch(
            'cvutils.imageutils.PILImageUtils.download_image')
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start(),
            require_async_kw=True)
        self.random_image = imageutils.PILImageUtils.create_random_image(480, 640)
        self.im_download_mock.return_value = self.random_image

        # Mock out the model
        self.model_mock = MagicMock()
        self.predict_mock = self._future_wrap_mock(
            self.model_mock.predictor.predict, require_async_kw=True)
        self.predict_mock.return_value = (99, None, 'model1')

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

        # Mock out the smart cropping to speed up the tests
        self.smart_crop_patcher = patch('cmsdb.cdnhosting.smartcrop.SmartCrop')
        crop_mocker = self.smart_crop_patcher.start()
        crop_mocker().crop_and_resize.side_effect = \
          lambda h, w: PILImageUtils.to_cv(
              PILImageUtils.create_random_image(h,w))

        self.submit_mocker = patch('video_processor.client.cmsapiv2.client')
        self.submit_mock = self._future_wrap_mock(
            self.submit_mocker.start().Client().send_request)
        self.submit_mock.side_effect = \
          lambda x, **kwargs: tornado.httpclient.HTTPResponse(
              x, 200)


    def tearDown(self):
        self.s3_patcher.stop()
        self.http_mocker.stop()
        self.im_download_mocker.stop()
        self.cloudinary_patcher.stop()
        self.smart_crop_patcher.stop()
        self.submit_mocker.stop()
        self.postgresql.clear_all_tables() 
        super(TestFinalizeResponse, self).tearDown()

    @classmethod
    def setUpClass(cls):
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()

class TestFinalizeThumbnailResponse(TestFinalizeResponse):
    def setUp(self):
        super(TestFinalizeThumbnailResponse, self).setUp()

        # populate some data
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
        self.api_request = neondata.NeonApiRequest(
            'job1', self.api_key,
            'vid1',
            'some fun video',
            'http://video.mp4',
            http_callback='http://callback.com',
            default_thumbnail='http://default_thumb.jpg')
        self.api_request.api_param = '1'
        self.api_request.api_method = 'topn'
        self.api_request.state = neondata.RequestState.PROCESSING
        self.api_request.save()

        # Setup the processor object
        job = self.api_request.__dict__
        self.vprocessor = video_processor.client.ThumbnailProcessor(
            job,
            self.model_mock,
            'test_version',
            multiprocessing.BoundedSemaphore(1),
            MagicMock(),
            MagicMock())
        self.vprocessor.video_metadata.duration = 130.0
        self.vprocessor.video_metadata.frame_size = (640, 480)

        self.vprocessor.thumbnails = [
            (neondata.ThumbnailMetadata(None,
                                        internal_vid=self.video_id,
                                        ttype=neondata.ThumbnailType.NEON,
                                        rank=0,
                                        model_score=2.3,
                                        model_version='model1',
                                        frameno=6,
                                        filtered=''),
             imageutils.PILImageUtils.create_random_image(480, 640)),
             (neondata.ThumbnailMetadata(None,
                                         internal_vid=self.video_id,
                                         ttype=neondata.ThumbnailType.NEON,
                                         rank=1,
                                         model_score=2.1,
                                         model_version='model1',
                                         frameno=69),
             imageutils.PILImageUtils.create_random_image(480, 640)),
             (neondata.ThumbnailMetadata(None,
                                         internal_vid=self.video_id,
                                         ttype=neondata.ThumbnailType.RANDOM,
                                         rank=0,
                                         frameno=67),
              imageutils.PILImageUtils.create_random_image(480, 640))]

        self.vprocessor.bad_thumbnails = [
            (neondata.ThumbnailMetadata(None,
                                        internal_vid=self.video_id,
                                        ttype=neondata.ThumbnailType.BAD_NEON,
                                        model_score=-3.3,
                                        model_version='model1',
                                        frameno=8,
                                        filtered=''),
             imageutils.PILImageUtils.create_random_image(480, 640)),
             (neondata.ThumbnailMetadata(None,
                                         internal_vid=self.video_id,
                                         ttype=neondata.ThumbnailType.BAD_NEON,
                                         model_score=-2.1,
                                         model_version='model1',
                                         frameno=44),
             imageutils.PILImageUtils.create_random_image(480, 640))]

    def tearDown(self):
        super(TestFinalizeThumbnailResponse, self).tearDown()

    @tornado.testing.gen_test
    def test_send_email_notification_base(self): 
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        api_request.callback_email = 'basetest@invalid.xxx' 
        yield api_request.save(async=True)
        self.vprocessor.video_metadata.save()
        video_data = yield neondata.VideoMetadata.get(
            self.video_id, 
            async=True)
        yield neondata.ThumbnailMetadata(
            'testing_vtid_one',
            urls=['second_best'], 
            model_score=0.4).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_two',
            urls=['best'], 
            model_score=0.5).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_three',
            model_score=0.3,
            urls=['third_best']).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_four',
            urls=['fourth_best']).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_five',
            urls=['default'], 
            model_score=0.3, 
            ttype=neondata.ThumbnailType.DEFAULT).save(async=True) 
        video_data.thumbnail_ids = ['testing_vtid_one','testing_vtid_two',
            'testing_vtid_three','testing_vtid_four','testing_vtid_five']
        yield video_data.save(async=True) 
        rv = yield self.vprocessor.send_notification_email(
            api_request, 
            video_data)
        body_json = json.loads(self.submit_mock.call_args[0][0].body)
        self.assertEquals('/api/v2/%s/email' % api_request.api_key, 
            self.submit_mock.call_args[0][0].url) 
        self.assertTrue(self.submit_mock.called)
        self.assertEquals(body_json['template_slug'], 'video-results') 
        self.assertEquals(body_json['subject'], 'Your Neon Images Are Here!')
        self.assertEquals(body_json['to_email_address'], 'basetest@invalid.xxx')
        tas = body_json['template_args'] 
        self.assertEquals(tas['top_thumbnail'], 'best')  
        self.assertEquals(tas['thumbnail_one'], 'second_best')  
        self.assertEquals(tas['thumbnail_two'], 'third_best')  
        self.assertEquals(tas['thumbnail_three'], 'fourth_best')  
        self.assertEquals(tas['lift'], '67%')
        self.assertEquals(rv, True)
 
    @tornado.testing.gen_test
    def test_send_email_notification_response_error(self): 
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        api_request.callback_email = 'basetest@invalid.xxx' 
        yield api_request.save(async=True)
        self.submit_mock.side_effect = \
          lambda x, **kwargs: tornado.httpclient.HTTPResponse(
              x, 400, error=Exception('blah'))

        self.vprocessor._get_email_params = MagicMock()
        self.vprocessor._get_email_params.side_effect = iter([{}])
        with self.assertLogExists(logging.ERROR, 'Failed to send'):
            rv = yield self.vprocessor.send_notification_email(
                api_request,
                None)
            self.assertTrue(self.submit_mock.called)
            self.assertEquals(rv, False)
            self.assertEquals(
                statemon.state.get(
                    'video_processor.client.failed_to_send_result_email'), 1)

    @tornado.testing.gen_test
    def test_send_email_notification_unexpected(self):
        self.vprocessor.send_notification_email = MagicMock()
        self.vprocessor.send_notification_email.side_effect = Exception('boom') 
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        api_request.callback_email = 'basetest@invalid.xxx' 
        with self.assertRaises(Exception):
            with self.assertLogExists(logging.ERROR, 'Unexpected error'):
                rv = yield self.vprocessor.send_notification_email(api_request)

    @tornado.testing.gen_test
    def test_send_email_notification_no_email(self):
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        api_request.save()

        with self.assertLogNotExists(logging.ERROR, 'Unexpected error'):
            rv = yield self.vprocessor.send_notification_email(api_request,
                                                               None)

        self.assertTrue(rv)

    @tornado.testing.gen_test
    def test_default_process(self):
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        api_request.callback_email = 'test@invalid.xxx' 
        self.vprocessor._get_email_params = MagicMock()
        self.vprocessor._get_email_params.side_effect = iter([{}])

        yield api_request.save(async=True)
        yield self.vprocessor.finalize_response()

        # Make sure that the api request is updated
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)

        self.assertEquals(api_request.state, neondata.RequestState.FINISHED)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.PROCESSED_SENT)
        self.assertIsInstance(api_request, neondata.NeonApiRequest)

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

        # Check the default thumb
        self.assertEquals(len(video_data.non_job_thumb_ids), 1)
        default_thumb = neondata.ThumbnailMetadata.get(
            video_data.non_job_thumb_ids[0])
        self.assertEquals(default_thumb.type, neondata.ThumbnailType.DEFAULT)
        self.assertIsNotNone(default_thumb.key)
        self.assertEquals(default_thumb.model_version, 'model1')
        self.assertEquals(default_thumb.model_score, 99)

        # Check the results list
        self.assertEquals(len(video_data.job_results), 1)
        job_result = video_data.job_results[0]
        self.assertIsNone(job_result.age)
        self.assertIsNone(job_result.gender)
        self.assertEquals(job_result.model_version, 'test_version')
        self.assertEquals(len(job_result.thumbnail_ids), 3)
        self.assertNotIn(default_thumb.key,
                         job_result.thumbnail_ids)
        
        # Check bad thumbnails.
        self.assertEqual(2, len(job_result.bad_thumbnail_ids))
        bad_thumbs = neondata.ThumbnailMetadata.get_many(
            job_result.bad_thumbnail_ids)
        self.assertEqual(2, len(bad_thumbs))
        self.assertEqual(-3.3, bad_thumbs[0].model_score)
        self.assertEqual(8, bad_thumbs[0].frameno)
        self.assertEqual('model1', bad_thumbs[0].model_version)
        self.assertEqual(-2.1, bad_thumbs[1].model_score)
        self.assertEqual(44, bad_thumbs[1].frameno)
        self.assertEqual('model1', bad_thumbs[1].model_version)
        
        # Check the thumbnail information in the database
        thumbs = neondata.ThumbnailMetadata.get_many(
            job_result.thumbnail_ids)
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

        # Check the video has a tag and all the thumbnails have that tag.
        tag = neondata.Tag.get(video_data.tag_id)
        self.assertIsNotNone(tag)
        tag_thumb_ids = set(neondata.TagThumbnail.get(tag_id=tag.get_id()))
        # Validate through the thumbnailmetadata row, not just the
        # thumb id in tag_thumbnail.
        _non_job_thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.non_job_thumb_ids)
        all_thumb_ids = set([t.get_id() for t in thumbs + bad_thumbs + 
                             _non_job_thumbs])
        self.assertEqual(tag_thumb_ids, all_thumb_ids)

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
            'error' : None
            }
        self.assertDictContainsSubset(expected_response,
                                      api_request.response)
        self.assertItemsEqual(api_request.response['framenos'],
                              [69, 6])
        self.assertItemsEqual(api_request.response['thumbnails'],
                              [n_thumbs[1].key, n_thumbs[0].key])

        # Compare serving URL here. Ignore the i* part of serving_url; because subdomains
        # can be different from multiple get_serving_url calls
        self.assertEquals(api_request.response['serving_url'].split('neon-images')[1],
                video_data.get_serving_url(save=False).split('neon-images')[1])
   
        # check video object again to ensure serving_url is not set
        video_data = neondata.VideoMetadata.get(self.video_id)
        self.assertIsNone(video_data.serving_url)

    @tornado.testing.gen_test
    def test_tag_on_video(self):
        '''A video has a tag after finalize when it starts with one'''

        # Setup doesn't save a video, so make it here.
        tag = neondata.Tag(
            None,
            name='Video title',
            tag_type=neondata.TagType.VIDEO,
            account_id=self.api_key)
        tag.save()
        video = neondata.VideoMetadata(
            self.video_id,
            tag_id=tag.get_id(),
            request_id=self.api_request.get_id())
        video.save()

        yield self.vprocessor.finalize_response()

        video = neondata.VideoMetadata.get(self.video_id)
        self.assertEqual(tag.get_id(), video.tag_id)

        tag_thumb_ids = set(neondata.TagThumbnail.get(tag_id=tag.get_id()))
        job_result = video.job_results[0]
        all_video_thumb_ids = set(
            video.non_job_thumb_ids +
            job_result.thumbnail_ids +
            job_result.bad_thumbnail_ids)
        self.assertEqual(tag_thumb_ids, all_video_thumb_ids)

    @tornado.testing.gen_test
    def test_no_tag_on_video(self):
        '''A video has a tag after finalize when it doesn't start with one'''

        # Setup doesn't save a video, so make it here.
        video = neondata.VideoMetadata(
            self.video_id,
            request_id=self.api_request.get_id())

        yield self.vprocessor.finalize_response()

        video = neondata.VideoMetadata.get(self.video_id)
        tag = neondata.Tag.get(video.tag_id)

        self.assertEqual(tag.video_id, video.get_id())
        self.assertEqual(tag.account_id, video.get_account_id())
        self.assertEqual(tag.tag_type, neondata.TagType.VIDEO)

        tag_thumb_ids = set(neondata.TagThumbnail.get(tag_id=tag.get_id()))
        job_result = video.job_results[0]
        all_video_thumb_ids = set(
            video.non_job_thumb_ids +
            job_result.thumbnail_ids +
            job_result.bad_thumbnail_ids)
        self.assertEqual(tag_thumb_ids, all_video_thumb_ids)


    @tornado.testing.gen_test
    def test_broken_default_thumb(self):
        '''
        Test to validate the flow when default thumb is broken
        '''
        
        self.im_download_mock.side_effect = [IOError, HTTPError(404),
                                             HTTPError(500)]
        for i in range(3):
            with self.assertRaises(video_processor.client.DefaultThumbError):
                yield self.vprocessor.finalize_response()

            # Check the video metadata in the database. It is still
            # serving, but we will be in an error state (checked in
            # another test)
            video_data = neondata.VideoMetadata.get(self.video_id)
            self.assertEquals(len(video_data.thumbnail_ids), 3) # no default thumb
            self.assertTrue(video_data.serving_enabled)

    @tornado.testing.gen_test
    def test_error_scoring_default_image(self):
        self.predict_mock.side_effect = [
            (99, None, 'model1'), # Scoring the random frame
            model.errors.PredictionError('oops')]

        with self.assertRaises(video_processor.client.DefaultThumbError):
            yield self.vprocessor.finalize_response()


        # Find the default thumb but it should not have a score
        video_data = neondata.VideoMetadata.get(self.video_id)
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.thumbnail_ids)
        default_thumb = [
            x for x in thumbs if x.type == neondata.ThumbnailType.DEFAULT]
        default_thumb = default_thumb[0]
        self.assertIsNotNone(default_thumb.key)
        self.assertIsNone(default_thumb.model_score)
        self.assertIsNone(default_thumb.model_version)
        self.assertIsNone(default_thumb.features)

    @tornado.testing.gen_test
    def test_error_scoring_bad_images(self):
        self.predict_mock.side_effect = [
            (99, None, 'model1'), # Score the random frame
            model.errors.PredictionError('oops'), # Fail on the bad thumb
            (99, None, 'model1')] # Suceed on the default

        self.vprocessor.bad_thumbnails = [
            (neondata.ThumbnailMetadata(None,
                                        internal_vid=self.video_id,
                                        ttype=neondata.ThumbnailType.BAD_NEON,
                                        model_version='model1',
                                        frameno=8,
                                        filtered=''),
             imageutils.PILImageUtils.create_random_image(480, 640))]

        # No exception should be raised because we dont' really care
        # about the score on the random thumb.
        yield self.vprocessor.finalize_response()


        # Find the default thumb but it should have a score
        video_data = neondata.VideoMetadata.get(self.video_id)
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.thumbnail_ids)
        default_thumb = [
            x for x in thumbs if x.type == neondata.ThumbnailType.DEFAULT]
        default_thumb = default_thumb[0]
        self.assertIsNotNone(default_thumb.key)
        self.assertEquals(default_thumb.model_score, 99)
        self.assertEquals(default_thumb.model_version, 'model1')
        self.assertIsNone(default_thumb.features)

    @tornado.testing.gen_test
    def test_reprocess_no_joblist_run(self):
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
                ttype=neondata.ThumbnailType.DEFAULT,
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
        api_request = neondata.NeonApiRequest(
            'job1', self.api_key, 'vid1',
            'some fun video',
            'http://video.mp4',
            http_callback='http://callback.com',
            default_thumbnail='http://default_thumb.jpg')
        api_request.state = neondata.RequestState.PROCESSING
        api_request.save()
        self.vprocessor.job_params['gender'] = 'M'
        self.vprocessor.reprocess = True
        self.vprocessor.thumb_model_version='model1'

        yield self.vprocessor.finalize_response()

        # Make sure that the api request is updated
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state, neondata.RequestState.FINISHED)
        self.assertIsInstance(api_request, neondata.NeonApiRequest)

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

        # Now there should be the results from two job runs
        self.assertEquals(len(video_data.job_results), 2)
        orig_result = [x for x in video_data.job_results 
                       if x.gender is None][0]
        self.assertItemsEqual(orig_result.thumbnail_ids,
                              ['%s_thumb1' % self.video_id,
                               '%s_thumb2' % self.video_id])
        self.assertEquals(orig_result.model_version, 'old_model')
        new_result = [x for x in video_data.job_results 
                      if x.gender == 'M'][0]

        # The random thumb should have been replaced because the model is new
        self.assertNotIn('%s_thumb2' % self.video_id,
                         new_result.thumbnail_ids)
        self.assertEquals(new_result.model_version, 'test_version')
        

        # Check the default thumbnails in the database. There should be 2 now
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.thumbnail_ids)
        default_thumbs = [
            x for x in thumbs if x.type == neondata.ThumbnailType.DEFAULT]
        default_thumbs = sorted(default_thumbs, key=lambda x: x.rank)
        self.assertEquals(len(default_thumbs), 2)
        self.assertEquals(default_thumbs[1].key, '%s_thumb3' % self.video_id)
        self.assertEquals(default_thumbs[1].rank, 0)
        self.assertEquals(default_thumbs[0].rank, -1)
        self.assertRegexpMatches(default_thumbs[0].key, '%s_.+'%self.video_id)
        self.assertItemsEqual(video_data.non_job_thumb_ids,
                              [x.key for x in default_thumbs])

        # Check the random thumb. There should only be one in all the runs
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

    @tornado.testing.gen_test
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
        api_request = neondata.NeonApiRequest(
            'job1', self.api_key, 'vid1',
            'some fun video',
            'http://video.mp4',
            http_callback='http://callback.com',
            default_thumbnail='http://default_thumb.jpg')
        
        for state in [neondata.RequestState.INT_ERROR,
                      neondata.RequestState.FAILED]:
            api_request.state = state 
            api_request.fail_count = 1
            api_request.save()

            yield self.vprocessor.finalize_response()

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

    @tornado.testing.gen_test
    def test_reprocess_with_previous_jobresult(self):
        # Add the results from the previous run to the database
        thumbs = [
            neondata.ThumbnailMetadata(
                '%s_thumb1' % self.video_id,
                self.video_id,
                model_score=3.0,
                ttype=neondata.ThumbnailType.NEON,
                model_version='model1',
                frameno=167,
                rank=0),
            neondata.ThumbnailMetadata(
                '%s_thumb2' % self.video_id,
                self.video_id,
                ttype=neondata.ThumbnailType.RANDOM,
                model_version='model1',
                rank=0),
            neondata.ThumbnailMetadata(
                '%s_thumb3' % self.video_id,
                self.video_id,
                ttype=neondata.ThumbnailType.DEFAULT,
                rank=0)]
        neondata.ThumbnailMetadata.save_all(thumbs)
            
        video_meta = neondata.VideoMetadata(
            self.video_id,
            tids = [x.key for x in thumbs],
            non_job_thumb_ids=[thumbs[2].key],
            job_results=[neondata.VideoJobThumbnailList(
                thumbnail_ids=[thumbs[0].key, thumbs[1].key],
                model_version='test_version')],
            duration=97.0,
            model_version='test_version')
        video_meta.serving_url = 'my_serving_url.jpg'
        video_meta.save()

        # Write the request to the db
        api_request = neondata.NeonApiRequest(
            'job1', self.api_key, 'vid1',
            'some fun video',
            'http://video.mp4',
            http_callback='http://callback.com',
            default_thumbnail='http://default_thumb.jpg')
        api_request.state = neondata.RequestState.PROCESSING
        api_request.save()
        self.vprocessor.job_params['gender'] = 'M'
        self.vprocessor.reprocess = True
        self.vprocessor.thumb_model_version='model1'

        yield self.vprocessor.finalize_response()

        video_data = neondata.VideoMetadata.get(self.video_id)

        # Check the default thumbnails in the database. There should be 2 now
        default_thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.non_job_thumb_ids)
        self.assertTrue(all([x.type == neondata.ThumbnailType.DEFAULT for x
                            in default_thumbs]))
        default_thumbs = sorted(default_thumbs, key=lambda x: x.rank)
        self.assertEquals(len(default_thumbs), 2)
        self.assertEquals(default_thumbs[1].key, '%s_thumb3' % self.video_id)
        self.assertEquals(default_thumbs[1].rank, 0)
        self.assertEquals(default_thumbs[0].rank, -1)
        self.assertRegexpMatches(default_thumbs[0].key, '%s_.+'%self.video_id)

        # There should be two result sets now
        self.assertEquals(len(video_data.job_results), 2)
        orig_result = None
        new_result = None
        for result in video_data.job_results:
            if result.gender is None:
                orig_result = result
            else:
                new_result = result
        self.assertEquals(new_result.gender, 'M')
        self.assertEquals(new_result.model_version, 'test_version')
        self.assertEquals(len(new_result.bad_thumbnail_ids), 2)
        self.assertEquals(orig_result.model_version, 'test_version')
        orig_thumbs = neondata.ThumbnailMetadata.get_many(
            orig_result.thumbnail_ids)
        new_thumbs = neondata.ThumbnailMetadata.get_many(
            new_result.thumbnail_ids)
                          
        
        # Check the random thumb. There should only be one in all the
        # runs and it is shared because the model version is the same
        # for both runs.
        orig_rand = [x for x in orig_thumbs 
                     if x.type == neondata.ThumbnailType.RANDOM][0]
        new_rand = [x for x in new_thumbs 
                     if x.type == neondata.ThumbnailType.RANDOM][0]
        self.assertEquals(new_rand.key, '%s_thumb2' % self.video_id)
        self.assertEquals(new_rand.key, orig_rand.key)
        self.assertEquals(new_rand.rank, 0)

        # Check the neon thumbs. Each run should have its unique entries
        self.assertItemsEqual(orig_result.thumbnail_ids,
                              [thumbs[0].key, thumbs[1].key])
        n_thumbs = [x for x in new_thumbs if 
                    x.type == neondata.ThumbnailType.NEON]
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

    @tornado.testing.gen_test
    def test_default_thumb_already_saved(self):
        # Add the video and the default thumb to the database
        self.vprocessor.video_metadata.save()
        thumb_meta = neondata.ThumbnailMetadata(None,
                ttype=neondata.ThumbnailType.DEFAULT,
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

        yield self.vprocessor.finalize_response()

        # Check the video metadata in the database
        video_data = neondata.VideoMetadata.get(self.video_id)
        self.assertEquals(len(video_data.thumbnail_ids), 4)
        self.assertTrue(video_data.serving_enabled)
        self.assertIsNone(video_data.serving_url)

        # Check the thumbnails, we should only have one default thumbnail
        thumbs = neondata.ThumbnailMetadata.get_many(
            video_data.thumbnail_ids)
        default_thumb = [
            x for x in thumbs if x.type == neondata.ThumbnailType.DEFAULT]
        self.assertEquals(len(default_thumb), 1)
        default_thumb = default_thumb[0]
        self.assertGreater(len(default_thumb.urls), 1)
        self.assertEquals(default_thumb.rank, 0)
        self.assertEquals(default_thumb.phash, thumb_meta.phash)
        self.assertEquals(default_thumb.model_score, 99)
        self.assertEquals(default_thumb.model_version, 'model1')

    @tornado.testing.gen_test
    def test_no_thumbnails_found(self):
        self.vprocessor.thumbnails = []

        with self.assertLogExists(logging.WARNING, 'No thumbnails extracted'):
            yield self.vprocessor.finalize_response()

        # Make sure that serving is enabled
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertTrue(video_meta.serving_enabled)
        self.assertEquals(len(video_meta.thumbnail_ids), 1)
        self.assertEquals(len(video_meta.non_job_thumb_ids), 1)

        self.assertEquals(neondata.ThumbnailMetadata.get(
            video_meta.thumbnail_ids[0]).type, 
            neondata.ThumbnailType.DEFAULT)
        self.assertEquals(neondata.ThumbnailMetadata.get(
            video_meta.non_job_thumb_ids[0]).type, 
            neondata.ThumbnailType.DEFAULT)

        self.assertEquals(len(video_meta.job_results), 1)
        self.assertEquals(len(video_meta.job_results[0].thumbnail_ids), 0)

    @tornado.testing.gen_test
    def test_no_thumbnails_found_no_default_thumb(self):
        self.vprocessor.thumbnails = []
        neondata.BrightcoveApiRequest('job1', self.api_key, 'vid1',
                                      'some fun video',
                                      'http://video.mp4', None, None, 'pubid',
                                      'http://callback.com', 'int1',
                                      None).save()

        with self.assertLogExists(logging.WARNING, 'No thumbnails extracted'):
            yield self.vprocessor.finalize_response()

        # Make sure that serving is disabled
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertFalse(video_meta.serving_enabled)
        self.assertEquals(len(video_meta.thumbnail_ids), 0)

    @tornado.testing.gen_test
    def test_extracted_default_thumbnail_fail(self):
        neondata.BrightcoveApiRequest('job1', self.api_key, 'vid1',
                                      'some fun video',
                                      'http://video.mp4', None, None, 'pubid',
                                      'http://callback.com', 'int1',
                                      None).save()
        self.vprocessor.extracted_default_thumbnail = \
          'http://extracted_default.jpg'
        self.im_download_mock.side_effect = [IOError('Not an image')]

        yield self.vprocessor.finalize_response()

        # The video should still be successful and we don't have a default
        video_meta = neondata.VideoMetadata.get(self.video_id)
        self.assertTrue(video_meta.serving_enabled)
        self.assertEquals(len(video_meta.thumbnail_ids), 3)

        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state, neondata.RequestState.FINISHED)

    @patch('video_processor.client.neondata.ThumbnailMetadata.modify_many')
    @tornado.testing.gen_test
    def test_db_connection_error_thumb(self, modify_mock):
        modify_mock = self._callback_wrap_mock(modify_mock)
        modify_mock.side_effect = [
            psycopg2.Error("Connection Error"),
            {}
            ]

        with self.assertLogExists(logging.ERROR,
                                  'Error writing thumbnail data'):
            with self.assertRaises(video_processor.client.DBError):
                yield self.vprocessor.finalize_response()

        self.api_request.state = neondata.RequestState.PROCESSING
        self.api_request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Error writing thumbnail data'):
            with self.assertRaises(video_processor.client.DBError):
                yield self.vprocessor.finalize_response()

    @patch('video_processor.client.neondata.VideoMetadata.modify')
    @tornado.testing.gen_test
    def test_db_connection_error_video(self, modify_mock):
        modify_mock = self._callback_wrap_mock(modify_mock)
        modify_mock.side_effect = [
            psycopg2.Error("Connection Error"),
            False
            ]

        with self.assertLogExists(logging.ERROR,
                                  'Error writing video data'):
            with self.assertRaises(video_processor.client.DBError):
                yield self.vprocessor.finalize_response()

        self.api_request.state = neondata.RequestState.PROCESSING
        self.api_request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Error writing video data'):
            with self.assertRaises(video_processor.client.DBError):
                yield self.vprocessor.finalize_response()

    @tornado.testing.gen_test
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
        
        yield self.vprocessor.finalize_response()

        # Check that the thumbnail was updated
        db_thumb = neondata.ThumbnailMetadata.get(rand_thumb[0].key)
        self.assertIn('random_frame.jpg', db_thumb.urls)
        self.assertEquals(len(db_thumb.urls), 2)

    @patch('video_processor.client.neondata.NeonApiRequest.modify')
    @tornado.testing.gen_test
    def test_api_request_update_fail(self, api_request_mock):
        api_request_mock = self._future_wrap_mock(api_request_mock,
                                                  require_async_kw=True)
        api_request_mock.side_effect = [
            # Connection error on setting finalizing state
            psycopg2.Error("Connection Error"), 
            # Api request missing on setting finalizing state
            None,
            #  Connection error on setting finished state
            self.api_request,
            psycopg2.Error("Connection Error"),
            # Api request missing on setting finished state
            self.api_request,
            None,
        ]

        with self.assertLogExists(logging.ERROR,
                                  'Error writing request state'):
            with self.assertRaises(video_processor.client.DBError):
                yield self.vprocessor.finalize_response()

        with self.assertLogExists(logging.WARNING,
                                  'Job .* was deleted'):
            yield self.vprocessor.finalize_response()
        
        with self.assertLogExists(logging.ERROR,
                                  'Error writing request state'):
            with self.assertRaises(video_processor.client.DBError):
                yield self.vprocessor.finalize_response()

        with self.assertLogExists(logging.ERROR,
                                  'Api Request finished failed'):
            with self.assertRaises(video_processor.client.DBError):
                yield self.vprocessor.finalize_response()

    @tornado.testing.gen_test(timeout=10.0)
    def test_somebody_else_processed(self):

        # Try when somebody else was sucessful
        for state in [neondata.RequestState.FINISHED,
                      neondata.RequestState.SERVING,
                      neondata.RequestState.ACTIVE]:
            
            self.api_request.state = state
            self.api_request.save()
            with self.assertRaises(video_processor.client.OtherWorkerCompleted):
                yield self.vprocessor.finalize_response()
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
            yield self.vprocessor.finalize_response()
            self.assertEquals(
                neondata.NeonApiRequest.get('job1', self.api_key).state,
                neondata.RequestState.FINISHED)

    @tornado.testing.gen_test
    def test_get_template_args_base(self):
        self.vprocessor.video_metadata.save()
        video_data = yield neondata.VideoMetadata.get(self.video_id, async=True)
        yield neondata.ThumbnailMetadata(
            'testing_vtid_one',
            urls=['second_best'], 
            model_score=0.4).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_two',
            urls=['best'], 
            model_score=0.5).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_three',
            model_score=0.3,
            urls=['third_best']).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_four',
            urls=['fourth_best']).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_five',
            urls=['default'], 
            model_score=0.3, 
            ttype=neondata.ThumbnailType.DEFAULT).save(async=True) 

        video_data.thumbnail_ids = ['testing_vtid_one','testing_vtid_two',
            'testing_vtid_three','testing_vtid_four','testing_vtid_five']
 
        yield video_data.save(async=True) 
        params = yield self.vprocessor._get_email_params(video_data)
        tas = params['template_args']
        self.assertEquals(tas['top_thumbnail'], 'best')  
        self.assertEquals(tas['thumbnail_one'], 'second_best')  
        self.assertEquals(tas['thumbnail_two'], 'third_best')  
        self.assertEquals(tas['thumbnail_three'], 'fourth_best')  
        self.assertEquals(tas['lift'], '67%') 
 
    @tornado.testing.gen_test
    def test_get_template_args_no_tn_exception(self):
        self.vprocessor.video_metadata.save()
        video_data = yield neondata.VideoMetadata.get(
            self.video_id, 
            async=True)
        with self.assertRaises(Exception): 
            tas = yield self.vprocessor._get_email_params(video_data)

    @tornado.testing.gen_test
    def test_get_template_args_no_dtn_exception(self):
        self.vprocessor.video_metadata.save()
        video_data = yield neondata.VideoMetadata.get(self.video_id, async=True)
        yield neondata.ThumbnailMetadata(
            'testing_vtid_one',
            urls=['second_best'], 
            model_score=0.4).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_two',
            urls=['best'], 
            model_score=0.5).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_three',
            model_score=0.3,
            urls=['third_best']).save(async=True) 
        yield neondata.ThumbnailMetadata(
            'testing_vtid_four',
            urls=['fourth_best']).save(async=True) 
        with self.assertRaises(Exception): 
            tas = yield self.vprocessor._get_email_params(video_data)

class TestFinalizeClipResponse(TestFinalizeResponse):
    def setUp(self):
        super(TestFinalizeClipResponse, self).setUp()

        # populate some data
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
        self.api_request = neondata.NeonApiRequest(
            'job1', self.api_key,
            'vid1',
            'some fun video',
            'http://video.mp4',
            http_callback='http://callback.com',
            result_type=neondata.ResultType.CLIPS,
            default_clip='http://default_clip.mp4',
            n_clips=2,
            callback_email='test@invalid.xxx')
        self.api_request.state = neondata.RequestState.PROCESSING
        self.api_request.save()

        # Setup the processor object
        job = self.api_request.__dict__
        self.vprocessor = video_processor.client.ClipProcessor(
            job,
            self.model_mock,
            'test_version',
            multiprocessing.BoundedSemaphore(1),
            MagicMock(),
            MagicMock())
        self.vprocessor.mov = test_utils.opencv.VideoCaptureMock(
            h=480, w=640, frame_count=2997)
        self.vprocessor.video_metadata.duration = 100.0
        self.vprocessor.video_metadata.frame_size = (640, 480)

        self.vprocessor.clips = [
            neondata.Clip(video_id=self.video_id,
                          ttype=neondata.ClipType.NEON,
                          rank=0,
                          start_frame=15,
                          end_frame=30,
                          score=0.63,
                          model_version='test_version'),
            neondata.Clip(video_id=self.video_id,
                          ttype=neondata.ClipType.NEON,
                          rank=1,
                          start_frame=115,
                          end_frame=210,
                          score=0.55,
                          model_version='test_version')]

    def tearDown(self):
        super(TestFinalizeClipResponse, self).tearDown()

    @tornado.testing.gen_test
    def test_default_process(self):
        yield self.vprocessor.finalize_response()

        # Make sure that the api request is updated
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)

        self.assertEquals(api_request.state, neondata.RequestState.FINISHED)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.PROCESSED_SENT)

        # Check the video metadata in the database
        video_data = neondata.VideoMetadata.get(self.video_id)
        self.assertEquals(len(video_data.thumbnail_ids), 0)
        self.assertEquals(len(video_data.non_job_thumb_ids), 2)
        self.assertEquals(len(video_data.non_job_clip_ids), 1)
        self.assertAlmostEquals(video_data.duration, 100.0)
        self.assertEquals(video_data.frame_size, [640, 480])
        self.assertEquals(video_data.url, 'http://video.mp4')
        self.assertEquals(video_data.integration_id, '0')
        self.assertEquals(video_data.model_version, 'test_version')
        self.assertTrue(video_data.serving_enabled)
        self.assertIsNone(video_data.serving_url) # serving_url not saved here

        # Check the job result list
        self.assertEquals(len(video_data.job_results), 1)
        job_result = video_data.job_results[0]
        self.assertIsNone(job_result.gender)
        self.assertIsNone(job_result.age)
        self.assertEquals(job_result.model_version, 'test_version')
        self.assertEquals(len(job_result.clip_ids), 2)
        self.assertEquals(job_result.thumbnail_ids, [])
        self.assertEquals(job_result.bad_thumbnail_ids, [])

        # Check the default clip
        default_clip = neondata.Clip.get(video_data.non_job_clip_ids[0])
        self.assertEquals(default_clip.type, neondata.ClipType.DEFAULT)
        self.assertEquals(default_clip.video_id, self.video_id)
        self.assertIn('http://default_clip.mp4', default_clip.urls)
        self.assertTrue(default_clip.enabled)
        self.assertEquals(default_clip.rank, 0)

        # Check the clip information
        


class SmokeTest(test_utils.neontest.AsyncTestCase):
    '''Smoke test for the video processing client'''
    def setUp(self):
        super(SmokeTest, self).setUp()
        statemon.state._reset_values()

        random.seed(984695198)

        # Populate some data
        na = neondata.NeonUserAccount('acct1')
        self.api_key = na.neon_api_key
        na.save()
        neondata.NeonPlatform.modify(self.api_key, '0', 
                                     lambda x: x, create_missing=True)

        cdn = neondata.CDNHostingMetadataList(
            neondata.CDNHostingMetadataList.create_key(self.api_key, '0'),
            [neondata.NeonCDNHostingMetadata(rendition_sizes=[(160,90)])])
        cdn.save()

        self.api_request = neondata.OoyalaApiRequest(
            'job1', self.api_key,
            'int1', 'vid1',
            'some fun video',
            's3://my-videos/test.mp4', None, None,
            'http://callback.com',
            'http://default_thumb.jpg')
        self.api_request.save()

        self.tag = neondata.Tag(
            None,
            account_id=na.get_id(),
            name='Good Images')
        self.tag.save()

        self.video_id = '%s_vid1' % self.api_key
        self.video = neondata.VideoMetadata(
            self.video_id,
            request_id=self.api_request.job_id,
            tag_id=self.tag.get_id())
        self.video.save()

        # Mock out s3
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('host-thumbnails')
        self.s3conn.create_bucket('n3.neon-images.com')

        # Mock the video queue
        self.job_queue_patcher = patch(
            'video_processor.video_processing_queue.' \
            'VideoProcessingQueue')
        self.job_queue_mock = self.job_queue_patcher.start()()

        self.job_queue_mock.get_duration.return_value = 600.0

        self.job_delete_mock = self._future_wrap_mock(
            self.job_queue_mock.delete_message)
        self.job_delete_mock.return_value = True

        self.job_read_mock = self._future_wrap_mock(
            self.job_queue_mock.read_message)
        self.job_message = Message()
        self.job_read_mock.side_effect = [self.job_message, None]

        self.job_hide_mock = self._future_wrap_mock(
            self.job_queue_mock.hide_message)

        # Mock out the video download
        self.client_s3_patcher = patch('video_processor.client.S3Connection')
        self.mock_conn2 = self.client_s3_patcher.start()
        self.mock_conn2.return_value = self.s3conn
        self.test_video_file = os.path.join(
            os.path.dirname(__file__),
            "test.mp4")
        self.vid_bucket = self.s3conn.create_bucket('my-videos')
        vid_key = self.vid_bucket.new_key('test.mp4')
        vid_key.set_contents_from_file(open(self.test_video_file, 'rb'))
        utf8key = 'L\xc3\xb6rick_video.mp4'.decode('utf-8')
        vid_key = self.vid_bucket.new_key(utf8key)
        vid_key.set_contents_from_file(open(self.test_video_file, 'rb'))

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
        self.model_patcher = patch(
            'video_processor.client.model.generate_model')
        self.model_file = os.path.join(os.path.dirname(__file__), "model.pkl")
        self.model_version = "test"
        self.model = MagicMock()
        load_model_mock = self.model_patcher.start()
        load_model_mock.return_value = self.model
        self.model.choose_thumbnails.return_value = \
          _get_good_model_return_value(), _get_good_model_return_value_low_score()
        self.predictor_patcher = patch(
            'video_processor.client.model.predictor.DeepnetPredictor')
        self.model.predictor = self.predictor_patcher.start()()
        self.predict_mock = self._future_wrap_mock(
            self.model.predictor.predict, require_async_kw=True)
        self.predict_mock.return_value = (99, None, 'model1')

        # Mock out the image download
        self.im_download_mocker = patch(
            'cvutils.imageutils.PILImageUtils.download_image')
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start(),
            require_async_kw=True)
        self.random_image = imageutils.PILImageUtils.create_random_image(480, 640)
        self.im_download_mock.return_value = self.random_image

        # Mock out the aquila lookup
        self.aquila_conn_patcher = patch(
            'video_processor.client.utils.autoscale')
        self.aquila_conn_patcher.start()

        # create the client object
        self.video_client = VideoClient(
            'some/dir/my_model',
            multiprocessing.BoundedSemaphore(1))

    def tearDown(self):
        self.aquila_conn_patcher.stop()
        self.s3_patcher.stop()
        self.client_s3_patcher.stop()
        self.http_mocker.stop()
        self.im_download_mocker.stop()
        self.cloudinary_patcher.stop()
        self.model_patcher.stop() 
        self.job_queue_patcher.stop()
        self.predictor_patcher.stop()
        self.postgresql.clear_all_tables()
        super(SmokeTest, self).tearDown()

    @classmethod
    def setUpClass(cls):
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()

    @tornado.gen.coroutine
    def _run_job(self, job):
        '''Runs the job'''
        self.job_message.set_body(json.dumps(job))
        # TODO look for a more permanent solution, possibly in smartcrop
        # from https://github.com/Itseez/opencv/issues/5150, set threads to 0 
        # in our main thread, otherwise fork screws things up. 
        cmsdb.cdnhosting.smartcrop.cv2.setNumThreads(0)
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
                    self.assertLess(time.time() - start_time, 10.0,
                                    'Timed out while running the smoke test')

                    time.sleep(0.1)

            finally:
                # Clean up the job process
                self.video_client.stop()
                self.video_client.join(10.0)
                if self.video_client.is_alive():
                    # SIGKILL it
                    utils.ps.send_signal_and_wait(signal.SIGKILL,
                                                  [self.video_client.pid])
                    self.fail('The subprocess did not die cleanly')

    @tornado.testing.gen_test(timeout=10)
    def test_smoke_test(self):
        utf8key = 'L\xc3\xb6rick_video.mp4'.decode('utf-8')

        self._run_job({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 's3://my-videos/%s' % utf8key
            })

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEqual(
            api_request.state,
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

        # Validate each thumb is tagged.
        _ids = set(
            chain(*[j.thumbnail_ids + j.bad_thumbnail_ids
                for j in video_meta.job_results]))
        all_thumb_ids = _ids.union(video_meta.non_job_thumb_ids)
        tagged_thumb_ids = set(
            neondata.TagThumbnail.get(tag_id=video_meta.tag_id))
        self.assertEqual(all_thumb_ids, tagged_thumb_ids)

    @tornado.testing.gen_test
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

        yield self._run_job({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 's3://my-videos/test.mp4'
            })

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

    @tornado.testing.gen_test
    def test_video_processing_error(self):
        self.mock_conn2.side_effect = [IOError('Oops')]

        with options._set_bounded('video_processor.client.max_fail_count', 1):
            yield self._run_job({
                'api_key': self.api_key,
                'video_id' : 'vid1',
                'job_id' : 'job1',
                'video_title': 'some fun video',
                'callback_url': 'http://callback.com',
                'video_url' : 's3://my-videos/test.mp4'
                })

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.CUSTOMER_ERROR)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.FAILED_SENT)

        # Check the state variables
        self.assertEquals(
            statemon.state.get('video_processor.client.processing_error'),
            1)
        self.assertEquals(
            statemon.state.get('video_processor.client.video_download_error'),
            1)

    @patch('video_processor.client.neondata.VideoMetadata.modify')
    @tornado.testing.gen_test
    def test_db_update_error(self, modify_mock):
        modify_mock.side_effect = [
            psycopg2.Error("Connection Error")]

        with options._set_bounded('video_processor.client.max_fail_count', 1):
            yield self._run_job({
                'api_key': self.api_key,
                'video_id': 'vid1',
                'job_id': 'job1',
                'video_title': 'some fun video',
                'callback_url': 'http://callback.com',
                'video_url': 's3://my-videos/test.mp4'})

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state, neondata.RequestState.INT_ERROR)

        # Check the state variables
        self.assertEquals(
            statemon.state.get('video_processor.client.processing_error'),
            1)
        self.assertEquals(
            statemon.state.get('video_processor.client.save_vmdata_error'),
            1)

    @tornado.testing.gen_test
    def test_no_need_to_process(self):
        self.api_request.state = neondata.RequestState.SERVING
        self.api_request.save()

        yield self._run_job({
            'api_key': self.api_key,
            'video_id' : 'vid1',
            'job_id' : 'job1',
            'video_title': 'some fun video',
            'callback_url': 'http://callback.com',
            'video_url' : 's3://my-videos/test.mp4'
            })
        
        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.SERVING)

    @tornado.testing.gen_test
    def test_download_default_thumb_error(self):
        # In this case, we should still allow the video serve, but
        # register it as a customer error in the database.
        self.im_download_mock.side_effect = [IOError('Cannot download')]

        with options._set_bounded('video_processor.client.max_fail_count', 1):
            yield self._run_job({
                'api_key': self.api_key,
                'video_id' : 'vid1',
                'job_id' : 'job1',
                'video_title': 'some fun video',
                'callback_url': 'http://callback.com',
                'video_url' : 's3://my-videos/test.mp4'
                })

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.CUSTOMER_ERROR)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.FAILED_SENT)
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

    @tornado.testing.gen_test
    def test_unexpected_error(self):
        self.mock_conn.side_effect = [Exception('Some bad error')]

        with options._set_bounded('video_processor.client.max_fail_count', 1):
            yield self._run_job({
                'api_key': self.api_key,
                'video_id' : 'vid1',
                'job_id' : 'job1',
                'video_title': 'some fun video',
                'callback_url': 'http://callback.com',
                'video_url' : 's3://my-videos/test.mp4'
                })

        # Check the api request in the database
        api_request = neondata.NeonApiRequest.get('job1', self.api_key)
        self.assertEquals(api_request.state,
                          neondata.RequestState.INT_ERROR)
        self.assertEquals(api_request.callback_state,
                          neondata.CallbackState.NOT_SENT)


if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
