#!/usr/bin/env python
'''
Copyright: 2014 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
import bcove_responses
import logging
from mock import patch, MagicMock
from StringIO import StringIO
from supportServices import neondata
import test_utils.neontest
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import tornado.ioloop
import test_utils.redis
import urlparse
import unittest

_log = logging.getLogger(__name__)

# TODO(sunil) Add more tests
class TestBrightcoveApi(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestBrightcoveApi, self).setUp()

        self.api = api.brightcove_api.BrightcoveApi(
            'api_key',
            10,
            'read_tok',
            'write_tok')

        self.http_call_patcher = \
          patch('api.brightcove_api.BrightcoveApi.read_connection.send_request')
        self.http_mock = self.http_call_patcher.start()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

    def tearDown(self):
        self.http_mock.stop()
        self.redis.stop()
        super(TestBrightcoveApi, self).tearDown()
    

    def _set_http_response(self, code=200, body='', error=None):
        def do_response(request, callback=None, *args, **kwargs):
            response = HTTPResponse(request, code,
                                    buffer=StringIO(body),
                                    error=error)
            if callback:
                tornado.ioloop.IOLoop.current().add_callback(callback,
                                                             response)
            else:
                return response

        self.http_mock.side_effect = do_response

    def test_get_current_thumbnail(self):
        self._set_http_response(
            body=('{"videoStillURL": '
                  '"http://brightcove.vo.llnwd.net/video_still.jpg?pubId=3", '
                  '"thumbnailURL": '
                  '"http://brightcove.vo.llnwd.net/thumb.jpg?pubId=3"}'))

        thumb_url, still_url = self.api.get_current_thumbnail_url('vid_1')

        self.assertEqual(self.http_mock.call_count, 1)
        url_call = self.http_mock.call_args[0][0].url
        url_params = urlparse.parse_qs(urlparse.urlparse(url_call).query)
        self.assertItemsEqual(url_params.items(),
                              [('command', ['find_video_by_id']),
                               ('token', ['read_tok']),
                               ('media_delivery', ['http']),
                               ('output', ['json']),
                               ('video_id', ['vid_1']),
                               ('video_fields', ['videoStillURL,thumbnailURL'])
                               ])

        self.assertEqual(thumb_url, 'http://brightcove.vo.llnwd.net/thumb.jpg')
        self.assertEqual(still_url,
                         'http://brightcove.vo.llnwd.net/video_still.jpg')

    def test_get_current_thumbnail_async(self):
        self._set_http_response(
            body=('{"videoStillURL": '
                  '"http://brightcove.vo.llnwd.net/video_still.jpg?pubId=3", '
                  '"thumbnailURL": '
                  '"http://brightcove.vo.llnwd.net/thumb.jpg?pubId=3"}'))

        self.api.get_current_thumbnail_url('vid_1', callback=self.stop)
        thumb_url, still_url = self.wait()

        self.assertEqual(self.http_mock.call_count, 1)
        url_call = self.http_mock.call_args[0][0].url
        url_params = urlparse.parse_qs(urlparse.urlparse(url_call).query)
        self.assertItemsEqual(url_params.items(),
                              [('command', ['find_video_by_id']),
                               ('token', ['read_tok']),
                               ('media_delivery', ['http']),
                               ('output', ['json']),
                               ('video_id', ['vid_1']),
                               ('video_fields', ['videoStillURL,thumbnailURL'])
                               ])

        self.assertEqual(thumb_url, 'http://brightcove.vo.llnwd.net/thumb.jpg')
        self.assertEqual(still_url,
                         'http://brightcove.vo.llnwd.net/video_still.jpg')

    def test_get_current_thumbnail_connection_error(self):
        self._set_http_response(code=500, error=HTTPError(500))

        with self.assertLogExists(
                logging.ERROR,
                'Error getting thumbnail for video id vid_1'):
            thumb_url, still_url = self.api.get_current_thumbnail_url('vid_1')
            self.assertIsNone(thumb_url)
            self.assertIsNone(still_url)
        

    def test_get_current_thumbnail_response_missing_thumbnail(self):
        self._set_http_response(
            body=('{"videoStillURL": '
                  '"http://brightcove.vo.llnwd.net/video_still.jpg?pubId=3"}'))

        with self.assertLogExists(
                logging.ERROR,
                'No valid url set for video id vid_1'):
            thumb_url, still_url = self.api.get_current_thumbnail_url('vid_1')
            self.assertIsNone(thumb_url)
            self.assertIsNone(still_url)

    def test_get_current_thumbnail_response_missing_still(self):
        self._set_http_response(
            body=('{"thumbnailURL": '
                  '"http://brightcove.vo.llnwd.net/thumb.jpg?pubId=3"}'))

        with self.assertLogExists(
                logging.ERROR,
                'No valid url set for video id vid_1'):
            thumb_url, still_url = self.api.get_current_thumbnail_url('vid_1')
            self.assertIsNone(thumb_url)
            self.assertIsNone(still_url)

    def test_get_current_thumbnail_bad_json_response(self):
        self._set_http_response(
            body=('{"thumbnailURL": '
                  '"http://brightcove.vo.llnwd.net/thumb.jpg?pubId=3"'))

        with self.assertLogExists(
                logging.ERROR,
                'Invalid JSON response from '):
            thumb_url, still_url = self.api.get_current_thumbnail_url('vid_1')
            self.assertIsNone(thumb_url)
            self.assertIsNone(still_url)
      
    @patch('api.brightcove_api.utils.http.send_request')
    def test_cehck_feed(self, utils_http_patch):
    
        def _side_effect(request, callback=None, *args, **kwargs):
            if "submitvideo" in request.url:
                response = HTTPResponse(request, 200,
                    buffer=StringIO('{"job_id":"j123"}'))
            if "find_modified_videos" in request.url:
                response = bcove_find_modified_videos_response 
            if "find_all_videos" in request.url:
                response = bcove_response
            
            if callback:
                tornado.ioloop.IOLoop.current().add_callback(callback,
                                                    response)
            else:
                return response
        
        bcove_request = HTTPRequest(
            'http://api.brightcove.com/services/library?'
            'get_item_count=true&command=find_all_videos&page_size=5&sort_by='
            'publish_date&token=rtoken&page_number=0&\
             output=json&media_delivery=http')

        bcove_response = HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_all_videos_response))
    
        bcove_find_modified_videos_response = \
                HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_modified_videos_response))

        self.http_mock.side_effect = _side_effect
        utils_http_patch.side_effect = _side_effect
        
        a_id = 'test' 
        i_id = 'i123'
        nvideos = 6
        na = neondata.NeonUserAccount('acct1')
        na.save()
        bp = neondata.BrightcovePlatform(a_id, i_id, na.neon_api_key, 'p1', 'rt', 'wt', 
                last_process_date=21492000000)
        bp.account_created = 21492000
        bp.save()
        bp.check_feed_and_create_api_requests()
        u_bp = neondata.BrightcovePlatform.create(bp.get())
        self.assertEqual(len(u_bp.get_videos()), nvideos)


if __name__ == "__main__" :
    unittest.main()
        
