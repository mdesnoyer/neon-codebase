#!/usr/bin/env python
'''
Service that handles a callback and pushes the serving url to the brightcove 
account.

Authors: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
from cmsdb import neondata
import cvutils.imageutils
import integrations.brightcove_pusher
import json
import logging
from mock import patch, MagicMock
import test_utils.neontest
import test_utils.postgresql
import tornado.gen
import tornado.httpclient
import tornado.testing
import unittest
import utils.neon
from utils.options import define, options
from utils import statemon

_log = logging.getLogger(__name__)

class BaseTest(test_utils.neontest.AsyncHTTPTestCase):
    def get_app(self): 
        return integrations.brightcove_pusher.application

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()

    def setUp(self):
        logging.getLogger('integrations.brightcove_pusher').reset_sample_counters()

        # Create default video and thumbnail objects
        self.thumb_map = [
            ('acct1_vid1_tid1', 'default'),
            ('acct1_vid1_n', 'neon'),
            ('acct1_vid1_rand', 'random'),
            ]
        thumbs = [neondata.ThumbnailMetadata(key, ttype=type, rank=0,
                                             urls=['%s.jpg' % key]) for
                  key, type in self.thumb_map]
        neondata.ThumbnailMetadata.save_all(thumbs)
        
        self.t_serving_urls = [neondata.ThumbnailServingURLs(key,
            base_url='http://neon-images.com/',
            sizes=[(160, 90), (320, 180), (480,360)])
            for key, type in self.thumb_map]
        neondata.ThumbnailServingURLs.save_all(self.t_serving_urls)
        self.def_serving_urls = self.t_serving_urls[0]
        
        neondata.VideoMetadata('acct1_vid1', tids=['acct1_vid1_tid1',
                                                   'acct1_vid1_n',
                                                   'acct1_vid1_rand']).save()

        # Mock out the image download
        self.im_download_mocker = patch(
            'cvutils.imageutils.PILImageUtils.download_image')
        self.random_image = \
          cvutils.imageutils.PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start(),
            require_async_kw=True)
        self.im_download_mock.return_value = self.random_image

        # Mock out the image upload
        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]

        super(BaseTest, self).setUp()

    def tearDown(self):
        self.im_download_mock.stop()
        self.cdn_mocker.stop()
        self.postgresql.clear_all_tables() 
        super(BaseTest, self).tearDown()

    @tornado.gen.coroutine
    def submit_callback(self, cb_data, method='PUT'):
        request = tornado.httpclient.HTTPRequest(
            self.get_url('/update_serving_url/%s' %
                         self.integration.integration_id),
            method=method,
            body=json.dumps(cb_data),
            headers={'Content-Type' : 'application/json'})

        response = yield self.http_client.fetch(request)
        raise tornado.gen.Return(response)

class TestCMSAPIPush(BaseTest):

    def setUp(self):        
        # Mock out the Brightcove API
        self.bc_api_mocker = patch(
            'integrations.brightcove_pusher.api.brightcove_api.CMSAPI')
        self.bc_api = MagicMock()
        self.bc_api_mocker.start().return_value = self.bc_api

        # Future wrap all the functions in the Brightcove api
        self.get_video_images_mock = self._future_wrap_mock(
            self.bc_api.get_video_images)
        self.add_thumbnail_mock = self._future_wrap_mock(
            self.bc_api.add_thumbnail)
        self.update_thumbnail_mock = self._future_wrap_mock(
            self.bc_api.update_thumbnail)
        self.delete_thumbnail_mock = self._future_wrap_mock(
            self.bc_api.delete_thumbnail)
        self.add_poster_mock = self._future_wrap_mock(
            self.bc_api.add_poster)
        self.update_poster_mock = self._future_wrap_mock(
            self.bc_api.update_poster)
        self.delete_poster_mock = self._future_wrap_mock(
            self.bc_api.delete_poster)

        # Default images on the video
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : False,
                'sources' : [ {
                    'height' : 360,
                    'width' : 480
                    }],
                'src' : 'http://some_bc_url.com/poster.jpg'
            },
            'thumbnail' : {
                'asset_id' : 'thumborig',
                'remote' : False,
                'sources' : [ {
                    'height' : 180,
                    'width' : 320
                    }],
                'src' : 'http://some_bc_url.com/thumb.jpg'
            }}]

        # Create the default database objects
        self.integration = neondata.BrightcoveIntegration(
            'acct1', 'pub1',
            application_client_id='clientid',
            application_client_secret='secret',
            uses_bc_thumbnail_api=True)
        self.integration.save()
        
        super(TestCMSAPIPush, self).setUp()

    def tearDown(self):
        self.bc_api_mocker.stop()
        super(TestCMSAPIPush, self).tearDown()


    @tornado.testing.gen_test
    def test_new_serving_url_existing_image(self):
        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'serving'})
        self.assertEquals(response.code, 200)

        # Make sure that a poster was added
        self.delete_poster_mock.assert_called_with('vid1', 'poster1')
        self.add_poster_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=480&height=360')

        # Make sure the thumb was added
        self.delete_thumbnail_mock.assert_called_with('vid1', 'thumborig')
        self.add_thumbnail_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=320&height=180')

    @tornado.testing.gen_test
    def test_post_same_as_put(self):
        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'serving'},
            'POST')
        self.assertEquals(response.code, 200)

        # Make sure that a poster was added
        self.delete_poster_mock.assert_called_with('vid1', 'poster1')
        self.add_poster_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=480&height=360')

        # Make sure the thumb was added
        self.delete_thumbnail_mock.assert_called_with('vid1', 'thumborig')
        self.add_thumbnail_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=320&height=180')

    @tornado.testing.gen_test
    def test_not_using_bc_thumbs(self):
        self.integration.uses_bc_thumbnail_api = False
        self.integration.save()
        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'serving'})
        self.assertEquals(response.code, 200)
        self.assertEquals(json.loads(response.body)['message'],
                          'No change because account does not use '
                          'the Brightcove thumbnails')
        self.assertEquals(self.get_video_images_mock.call_count, 0)
        self.assertEquals(self.delete_poster_mock.call_count, 0)
        self.assertEquals(self.delete_thumbnail_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_new_serving_url_existing_remote_url_thumb_missing(self):
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://some_remote_url.com/poster.jpg'
            }}]

        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'serving'})
        self.assertEquals(response.code, 200)

        # Make sure the poster was updated
        self.update_poster_mock.assert_called_with(
            'vid1', 'poster1',
            'http://neon-images.com/neonvid_vid1.jpg')

        # Make sure the thumbnail was added
        self.add_thumbnail_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg')

    @tornado.testing.gen_test
    def test_image_size_found_in_remote_url(self):
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'width' : None
                    }],
                'src' : 'http://some_remote_url.com/poster.jpg?height=480&width=640'
            },
            'thumbnail' : {
                'asset_id' : 'thumborig',
                'remote' : True,
                'sources' : [ {
                    'height' : None
                    }],
                'src' : 'http://some_remote_url.com/thumb.jpg?h=180&w=320'
            }}]
        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'serving'})
        self.assertEquals(response.code, 200)

        # Make sure the poster was updated
        self.update_poster_mock.assert_called_with(
            'vid1', 'poster1',
            'http://neon-images.com/neonvid_vid1.jpg?width=640&height=480')

        # Make sure the thumbnail was updated
        self.update_thumbnail_mock.assert_called_with(
            'vid1', 'thumborig',
            'http://neon-images.com/neonvid_vid1.jpg?width=320&height=180')

    @tornado.testing.gen_test
    def test_must_ingest_image(self):
        # No thumbs attached to the video
        neondata.VideoMetadata('acct1_vid1', tids=[]).save()

        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'serving'})
        self.assertEquals(response.code, 200)

        # Make sure the image was downloaded
        self.assertEquals(self.im_download_mock.call_count, 1)

        # Check that the image was added to the database object
        video = neondata.VideoMetadata.get('acct1_vid1')
        self.assertEquals(len(video.thumbnail_ids), 1)
        self.assertNotEquals(video.thumbnail_ids[0], 'acct1_vid1_tid1')
        thumb = neondata.ThumbnailMetadata.get(video.thumbnail_ids[0])
        self.assertEquals(thumb.type, neondata.ThumbnailType.DEFAULT)
        self.assertEquals(thumb.external_id, 'poster1')
        self.assertEquals(thumb.rank, 0)

        # Make sure that a poster was added
        self.delete_poster_mock.assert_called_with('vid1', 'poster1')
        self.add_poster_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=480&height=360',)

        # Make sure the thumb was added
        self.delete_thumbnail_mock.assert_called_with('vid1', 'thumborig')
        self.add_thumbnail_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=320&height=180')

    @tornado.testing.gen_test
    def test_state_processed_not_our_url(self):
        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'processed'})
        self.assertEquals(response.code, 200)
        
        self.assertEquals(self.add_thumbnail_mock.call_count, 0)
        self.assertEquals(self.update_thumbnail_mock.call_count, 0)
        self.assertEquals(self.delete_thumbnail_mock.call_count, 0)

        self.get_video_images_mock.side_effect = [{
            'thumbnail' : {
                'asset_id' : 'thumborig',
                'remote' : True,
                'sources' : [ {
                    'height' : 180,
                    'width' : 320
                    }],
                'src' : 'http://some_other_url.com/thumb.jpg'
            }}]

        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'processed'})
        self.assertEquals(response.code, 200)
        
        self.assertEquals(self.add_thumbnail_mock.call_count, 0)
        self.assertEquals(self.update_thumbnail_mock.call_count, 0)
        self.assertEquals(self.delete_thumbnail_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_push_back_orig_image(self):
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg?height=360&width=480'
            },
            'thumbnail' : {
                'asset_id' : 'ourthumb',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg?height=180&width=320'
            }}]

        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'processed'})
        self.assertEquals(response.code, 200)

        self.update_thumbnail_mock.assert_called_with(
            'vid1', 'ourthumb', self.def_serving_urls.get_serving_url(320, 180))
        self.update_poster_mock.assert_called_with(
            'vid1', 'poster1', self.def_serving_urls.get_serving_url(480, 360))

    @tornado.testing.gen_test
    def test_push_back_orig_image_odd_sizes(self):
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg'
            },
            'thumbnail' : {
                'asset_id' : 'ourthumb',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg?height=111&width=300'
            }}]

        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'processed'})
        self.assertEquals(response.code, 200)

        self.update_thumbnail_mock.assert_called_with(
            'vid1', 'ourthumb', self.def_serving_urls.get_serving_url(320, 180))
        self.update_poster_mock.assert_called_with(
            'vid1', 'poster1', self.def_serving_urls.get_serving_url(480, 360))

    @tornado.testing.gen_test
    def test_push_back_no_default_image(self):
        neondata.VideoMetadata('acct1_vid1', tids=['acct1_vid1_rand',
                                                   'acct1_vid1_n']).save()
        
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg'
            }}]

        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'processed'})
        self.assertEquals(response.code, 200)
        self.assertEquals(self.update_thumbnail_mock.call_count, 0)
        self.update_poster_mock.assert_called_with(
            'vid1', 'poster1',
            neondata.ThumbnailServingURLs.get('acct1_vid1_n').get_serving_url(
                480, 360))

    @tornado.testing.gen_test
    def test_push_back_no_default_no_neon_image(self):
        neondata.VideoMetadata('acct1_vid1', tids=['acct1_vid1_rand']).save()
        
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg'
            }}]

        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'processed'})
        self.assertEquals(response.code, 200)
        self.assertEquals(self.update_thumbnail_mock.call_count, 0)
        self.update_poster_mock.assert_called_with(
            'vid1', 'poster1',
            neondata.ThumbnailServingURLs.get('acct1_vid1_rand').get_serving_url(
                480, 360))

    @tornado.testing.gen_test
    def test_push_back_no_default_no_thumbs(self):
        neondata.VideoMetadata('acct1_vid1', tids=[]).save()
        
        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg'
            }}]

        with self.assertLogExists(logging.WARNING, 
                                  'No thumbnails found for video '):
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                response = yield self.submit_callback({
                    'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                    'video_id' : 'vid1',
                    'processing_state' : 'processed'})
        self.assertEquals(e.exception.code, 500)
        self.assertEquals(self.update_thumbnail_mock.call_count, 0)
        self.assertEquals(self.update_poster_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_healthcheck(self):
        response = yield self.http_client.fetch(self.get_url('/healthcheck'))
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_unknown_video_id(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.submit_callback({
                'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                'video_id' : 'unknown_vid',
                'processing_state' : 'serving'})
        self.assertEquals(e.exception.code, 404)

        self.get_video_images_mock.side_effect = [{
            'poster' : {
                'asset_id' : 'poster1',
                'remote' : True,
                'sources' : [ {
                    'height' : None,
                    'width' : None
                    }],
                'src' : 'http://www.neon-images.com/neonvid_vid1.jpg'
            }}]

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.submit_callback({
                'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                'video_id' : 'unknown_vid',
                'processing_state' : 'processed'})
        self.assertEquals(e.exception.code, 404)

    @tornado.testing.gen_test
    def test_invalid_json(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url('/update_serving_url/%s' % 
                             self.integration.integration_id),
                method='PUT',
                body='Heohoeoheoe')
        self.assertEquals(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_unknown_integration_id(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url('/update_serving_url/bad_dog'),
                method='PUT',
                body=json.dumps({}))
        self.assertEquals(e.exception.code, 404)

    @tornado.testing.gen_test
    def test_bc_server_error(self):
        self.get_video_images_mock.side_effect = [
            api.brightcove_api.BrightcoveApiServerError('Some crazy guy')
            ]
        
        with self.assertLogExists(logging.ERROR,
                                  'Error with the Brightcove API'):
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                response = yield self.submit_callback({
                    'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                    'video_id' : 'vid1',
                    'processing_state' : 'serving'})

        self.assertEquals(e.exception.code, 500)

    @tornado.testing.gen_test
    def test_unauthorized_client_tokens(self):
        self.get_video_images_mock.side_effect = [
            api.brightcove_api.BrightcoveApiNotAuthorizedError(
                'No you do not')
            ]
        
        with self.assertLogExists(logging.WARNING,
                                  'No valid Brightcove tokens'):
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                response = yield self.submit_callback({
                    'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                    'video_id' : 'vid1',
                    'processing_state' : 'serving'})

        self.assertEquals(e.exception.code, 500)
        self.assertRegexpMatches(e.exception.message,
                                 'No valid Brightcove tokens')

    @tornado.testing.gen_test
    def test_no_auth_tokens(self):
        self.integration.application_client_id = None
        self.integration.application_client_secret = None
        self.integration.save()
        
        with self.assertLogExists(logging.WARNING,
                                  'No valid Brightcove tokens'):
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                response = yield self.submit_callback({
                    'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                    'video_id' : 'vid1',
                    'processing_state' : 'serving'})

        self.assertEquals(e.exception.code, 500)
        self.assertRegexpMatches(e.exception.message,
                                 'No valid Brightcove tokens')

    @tornado.testing.gen_test
    def test_unexpcted_error(self):
        self.get_video_images_mock.side_effect = [
            Exception('Something unexpected')
            ]
        
        with self.assertLogExists(logging.ERROR,
                                  'Unexpected Error'):
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                response = yield self.submit_callback({
                    'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                    'video_id' : 'vid1',
                    'processing_state' : 'serving'})

        self.assertEquals(e.exception.code, 500)
        self.assertRegexpMatches(e.exception.message,
                                 'Internal Server Error')

    @tornado.testing.gen_test
    def test_error_downloading_image(self):
        # No thumbs attached to the video
        neondata.VideoMetadata('acct1_vid1', tids=[]).save()
        
        self.im_download_mock.side_effect = [
            IOError('Could not read image'),
            IOError('Could not read image')
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Error while ingesting image'):
            response = yield self.submit_callback({
                'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                'video_id' : 'vid1',
                'processing_state' : 'serving'})

        self.assertEquals(response.code, 200)
        self.assertEquals(statemon.state.get(
            'integrations.brightcove_pusher.image_ingestion_error'),
            1)

        # Check that the database didn't change
        video = neondata.VideoMetadata.get('acct1_vid1')
        self.assertEquals(len(video.thumbnail_ids), 0)

        # Make sure that a poster was still added
        self.delete_poster_mock.assert_called_with('vid1', 'poster1')
        self.add_poster_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=480&height=360')

        # Make sure the thumb was still added
        self.delete_thumbnail_mock.assert_called_with('vid1', 'thumborig')
        self.add_thumbnail_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=320&height=180')
        
class TestMediaAPIPush(BaseTest):
    def setUp(self):
        # Mock out the Brightcove API
        self.bc_api_mocker = patch(
            'integrations.brightcove_pusher.api.brightcove_api.BrightcoveApi')
        self.bc_api = MagicMock()
        self.bc_api_mocker.start().return_value = self.bc_api

        # Mock out the api call
        self.update_thumbnail_mock = self._future_wrap_mock(
            self.bc_api.update_thumbnail_and_videostill)
        
        # Create the default database objects
        self.integration = neondata.BrightcoveIntegration(
            'acct1', 'pub1',
            rtoken='read_token',
            wtoken='write_token',
            uses_bc_thumbnail_api=True)
        self.integration.save()
        
        super(TestMediaAPIPush, self).setUp()

    def tearDown(self):
        self.bc_api_mocker.stop()
        super(TestMediaAPIPush, self).tearDown()

    @tornado.testing.gen_test
    def test_push_serving_url(self):
        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'serving'})
        self.assertEquals(response.code, 200)

        self.update_thumbnail_mock.assert_called_with(
            'vid1', None, remote_url='http://neon-images.com/neonvid_vid1.jpg')

    @tornado.testing.gen_test
    def test_push_original_image(self):
        response = yield self.submit_callback({
            'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
            'video_id' : 'vid1',
            'processing_state' : 'processed'})
        self.assertEquals(response.code, 200)

        # Check that we are downloading the expected image
        self.im_download_mock.assert_called_with('acct1_vid1_tid1.jpg')

        # Check that we uploaded the image
        self.update_thumbnail_mock.assert_called_with(
            'vid1', 'acct1_vid1_tid1', image=self.random_image)

    @tornado.testing.gen_test
    def test_error_downloading_image(self):
        self.im_download_mock.side_effect = [
            IOError('Could not read image')
            ]

        with self.assertLogExists(logging.WARNING,
                                  'Error while downloading'):
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                response = yield self.submit_callback({
                    'serving_url': 'http://neon-images.com/neonvid_vid1.jpg',
                    'video_id' : 'vid1',
                    'processing_state' : 'processed'})

        self.assertEquals(e.exception.code, 500)
        self.assertRegexpMatches(e.exception.message,
                                 'Error while downloading thumbnail')



if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
