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

_log = logging.getLogger(__name__)

class TestCMSAPIPush(test_utils.neontest.AsyncHTTPTestCase):
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
                    'height' : 540,
                    'width' : 960
                    }],
                'src' : 'http://some_bc_url.com/poster.jpg'
            },
            'thumbnail' : {
                'asset_id' : 'thumborig',
                'remote' : False,
                'sources' : [ {
                    'height' : 90,
                    'width' : 160
                    }],
                'src' : 'http://some_bc_url.com/thumb.jpg'
            }}]

        # Create an integration object in the database
        self.integration = neondata.BrightcoveIntegration(
            'acct1', 'pub1',
            application_client_id='clientid',
            application_client_secret='secret',
            uses_bc_thumbnail_api=True)
        self.integration.save()

        thumb = neondata.ThumbnailMetadata('acct1_vid1_tid1',
                                           ttype='default',
                                           rank=0)
        thumb.save()
        neondata.VideoMetadata('acct1_vid1', tids=['acct1_vid1_tid1']).save()
        
        super(TestCMSAPIPush, self).setUp()

    def tearDown(self):
        self.bc_api_mocker.stop()
        self.postgresql.clear_all_tables() 
        super(TestCMSAPIPush, self).tearDown()

    @tornado.gen.coroutine
    def submit_callback(self, cb_data):
        request = tornado.httpclient.HTTPRequest(
            self.get_url('/update_serving_url/%s' %
                         self.integration.integration_id),
            method='PUT',
            body=json.dumps(cb_data),
            headers={'Content-Type' : 'application/json'})

        response = yield self.http_client.fetch(request)
        raise tornado.gen.Return(response)

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
            'http://neon-images.com/neonvid_vid1.jpg?width=960&height=540',
            'stillservingurl-vid1')

        # Make sure the thumb was added
        self.delete_thumbnail_mock.assert_called_with('vid1', 'thumborig')
        self.add_thumbnail_mock.assert_called_with(
            'vid1',
            'http://neon-images.com/neonvid_vid1.jpg?width=160&height=90',
            'thumbservingurl-vid1')

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
