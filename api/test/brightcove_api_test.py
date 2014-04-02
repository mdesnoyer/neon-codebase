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
import logging
from mock import patch, MagicMock
from StringIO import StringIO
import test_utils.neontest
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import tornado.ioloop
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

    def tearDown(self):
        self.http_mock.stop()
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
        

if __name__ == "__main__" :
    unittest.main()
        
