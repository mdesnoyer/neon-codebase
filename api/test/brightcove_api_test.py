#!/usr/bin/env python
'''
Copyright: 2014 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)

#TODO @MARK: figure out why test_utils.neontest.AsyncTestCase affects
other test cases. Seems like the mock persists across test cases.
disabling this file ensures other tests succeed.

'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
import bcove_responses
from cmsdb import neondata
from cStringIO import StringIO
import json
import logging
from mock import patch, MagicMock
from StringIO import StringIO
import test_utils.neontest
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import tornado.ioloop
import test_utils.redis
import urlparse
import unittest
from cvutils.imageutils import PILImageUtils
import utils.neon
from utils.options import define, options


_log = logging.getLogger(__name__)

define('run_tests_on_test_account', default=0, type=int,
       help='If set, will run tests that hit the real Brightcove APIs')

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
        self.outer_http_mock = self.http_call_patcher.start()
        self.http_mock = self._future_wrap_mock(self.outer_http_mock)
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

    def tearDown(self):
        self.http_call_patcher.stop()
        self.redis.stop()
        super(TestBrightcoveApi, self).tearDown()

    def _set_http_response(self, code=200, body='', error=None):
        def do_response(request, *args, **kwargs):
            return HTTPResponse(request, code,
                                buffer=StringIO(body),
                                error=error)

        self.http_mock.side_effect = do_response

    def _set_videos_to_return(self, videos):
        '''Define the videos to return. Should be video structures.'''
        self.videos_to_return = videos
        def respond_with_videos(request, **kwargs):
            videos = self.videos_to_return
            parsed = urlparse.urlparse(request.url)
            params = urlparse.parse_qs(parsed.query)
            fields = params.get('video_fields', None)
            if fields:
                fields = fields[0].split(',')
                videos = \
                  [dict([(k, v) for k,v in vid.items() if k in fields])
                   for vid in videos]
            retstruct = {
                'items' : videos,
                'page_number' : params.get('page_number', 0),
                'page_size' : params.get('page_size', 100),
                'total_count' : -1
            }
            
            return HTTPResponse(request, 200,
                                buffer=StringIO(json.dumps(retstruct)))

        self.http_mock.side_effect = respond_with_videos

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

    @patch('api.brightcove_api.BrightcoveApi.write_connection.send_request') 
    @tornado.testing.gen_test 
    def test_add_image(self, write_conn_mock):
        write_conn_mock = self._future_wrap_mock(write_conn_mock)
        def verify():
            '''
            Verify the image name when uploaded to Bcove
            '''
            headers = write_conn_mock.call_args[0][0].headers
            self.assertTrue('multipart/form-data' in headers['Content-Type'])
            body = write_conn_mock.call_args[0][0].body

            ##parse multipart request body; #TODO(Sunil) Find python lib to parse this
            separator = body.split('\r\n')[0]
            parts = body.split(separator)
            img_metadata = parts[1]
            img_data = parts[2]
            c_disposition = img_data.split('\r\n')[1]
            #ex: 'Content-Disposition: form-data; name="filePath"; filename="neontnTID.jpg"'
            img_filename = c_disposition.split(';')[-1].split("=")[-1]
            self.assertEqual(img_filename, '"neontn%s.jpg"' % tid)
    
        response = HTTPResponse(HTTPRequest("http://bcove"), 200,
                buffer=StringIO('{"result":{"id":"newtid"}}'))
        write_conn_mock.side_effect = [response]
        image = PILImageUtils.create_random_image(360, 480) 
        tid = "TID"
        
        #verify image name
        yield self.api.add_image("video_id1", tid, image=image,
                                 reference_id=tid)
        verify()
        write_conn_mock.reset()
        
        #verify image name
        self.api.add_image("video_id1", tid, image=image,
                           reference_id="still-%s" % tid)
        verify()

    @patch('api.brightcove_api.BrightcoveApi.write_connection.send_request')
    @tornado.testing.gen_test 
    def test_add_remote_image(self, write_conn_mock):

        '''
        Verify the multipart request construction to brightcove
        '''
        write_conn_mock = self._future_wrap_mock(write_conn_mock)
        r_url = "http://i1.neon-images.com/video_id1?height=10&width=20"
        response = HTTPResponse(HTTPRequest("http://bcove"), 200,
                buffer=StringIO('{"result":{"id":"newtid", "remoteUrl":"%s"}}'
                                % r_url))
        write_conn_mock.side_effect = [response]
        resp = yield self.api.add_image("video_id1", 'tid1', 
                                        remote_url=r_url)
        self.assertEqual(resp, {"id": "newtid",
                                "remoteUrl":r_url})
        headers = write_conn_mock.call_args[0][0].headers
        self.assertTrue('multipart/form-data' in headers['Content-Type'])
        body = write_conn_mock.call_args[0][0].body
        separator = body.split('\r\n')[0]
        parts = body.split(separator)
        j_imdata = parts[1].split('\r\n\r\n')[1].strip('\r\n')
        imdata = json.loads(j_imdata)
        self.assertTrue(imdata["params"]["image"]["remoteUrl"], r_url)


    def test_find_videos_by_ids_basic(self):
        self._set_videos_to_return([
            {'id': 'vid1',
             'name': 'myvid1',
             'accountId': 'acct1'},
            {'id': 'vid2',
             'name': 'myvid2',
             'accountId': 'acct1'}])

        self.assertItemsEqual(
            self.api.find_videos_by_ids(['vid2', 'vid1']),
            [{'id' : 'vid2', 'name': 'myvid2', 'accountId' : 'acct1'},
             {'id' : 'vid1', 'name': 'myvid1', 'accountId' : 'acct1'}])

        cargs, kwargs = self.http_mock.call_args
        urlparsed = urlparse.urlparse(cargs[0].url)
        urlparams = urlparse.parse_qs(urlparsed.query)
        self.assertEquals(
            urlparams,
            {'command' : ['find_videos_by_ids'],
             'token' : ['read_tok'],
             'video_ids' : ['vid2,vid1'],
             'media_delivery' : ['http'],
             'output': ['json']})

        self._set_videos_to_return([
            {'id': 'vid2',
             'name': 'myvid2',
             'accountId': 'acct1'}])

        self.assertItemsEqual(
            self.api.find_videos_by_ids(['vid2'], video_fields=['name']),
            [{'id': 'vid2', 'name': 'myvid2'}])

    def test_find_videos_by_ids_errors(self):
        self._set_http_response(
            body='{"error": "invalid token","code":210}',
            error=tornado.httpclient.HTTPError(500, 'invalid token'))

        with self.assertLogExists(logging.ERROR, 'invalid token'):
            with self.assertRaises(api.brightcove_api.BrightcoveApiClientError):
                self.api.find_videos_by_ids(['vid1'])

        self._set_http_response(
            body='{"error": "server slow","code":103}',
            error=tornado.httpclient.HTTPError(500, 'server slow'))

        with self.assertLogExists(logging.ERROR, 'server slow'):
            with self.assertRaises(api.brightcove_api.BrightcoveApiServerError):
                self.api.find_videos_by_ids(['vid1'])

class TestCMSAPILive(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        if not options.run_tests_on_test_account:
            raise unittest.SkipTest('Should only be run manually because it '
                                    'hits Brightcove')
        
        super(TestCMSAPILive, self).setUp()

        self.publisher_id = '2294876105001'
        self.client_id = '8b089370-ce31-4ecf-9c14-7ffc6ff492b9'
        self.client_secret = 'zZu6_l62UCYhjpTuwEfWrNDrjEqyP9Pg19Sv5BUUGCig1CMA-mIuxy14DjH6n1xQHZi3_RPYfO8_YRGh8xAyyg'
        self.test_video_id = '4049585935001'
        self.test_thumb_url = 'https://s3.amazonaws.com/neon-test/mikey.jpg'

        self.api = api.brightcove_api.CMSAPI(self.publisher_id,
                                             self.client_id,
                                             self.client_secret)
        

    def tearDown(self):
        super(TestCMSAPILive, self).tearDown()

    @tornado.testing.gen_test
    def test_replace_thumbnail(self):

        video_images = yield self.api.get_video_images(self.test_video_id)

        self.assertIn('thumbnail', video_images)

        yield self.api.delete_thumbnail(self.test_video_id,
                                        video_images['thumbnail']['asset_id'])

        tresponse = yield self.api.add_thumbnail(self.test_video_id,
                                                 self.test_thumb_url)
        self.assertEquals(tresponse['remote_url'], self.test_thumb_url)

        update_response = yield self.api.update_thumbnail(
            self.test_video_id,
            tresponse['id'],
            self.test_thumb_url)
        self.assertEquals(tresponse['remote_url'], self.test_thumb_url)

class TestUpdateImages(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestUpdateImages, self).setUp()

        self.api = api.brightcove_api.BrightcoveApi(
            'api_key',
            10,
            'read_tok',
            'write_tok')

        self.http_writer_patcher = \
          patch('api.brightcove_api.BrightcoveApi.write_connection.send_request')
        self.http_func_mock = self._future_wrap_mock(
            self.http_writer_patcher.start(), require_async_kw=True)
        self.http_mock = MagicMock()
        self.http_mock.side_effect = [{'result': {'id': 'bcimgid'}}]
        self.http_func_mock.side_effect = lambda x, **kw: HTTPResponse(
            x, 200,
            buffer=StringIO(json.dumps(self.http_mock())))

    def tearDown(self):
        self.http_writer_patcher.stop()
        super(TestUpdateImages, self).tearDown()

    def get_last_http_request(self):
        cargs, args = self.http_func_mock.call_args
        return cargs[0]

    @unittest.skip('Functionality was manually tested and I do not have time now to figure out how to parse multipart upload jrpc')
    @tornado.testing.gen_test
    def test_add_remote_image(self):
        response = yield self.api.add_image('vid1', 'vid1_tid1',
                                            remote_url='http://remote_url.com')
        self.assertEquals(response, {'id': 'bcimgid'})

        request = self.get_last_http_request()
        self.assertEquals(request.url,
                          'http://api.brightcove.com/services/post')
        self.assertEquals(json.loads(request.body),
                          { "method" : "add_image",
                            "params" : {
                                "token" : "write_tok",
                                "image" : {
                                    "remote_url" : 'http://remote_url.com'
                                },
                                "video_id" : "vid1"
                            }
                        })

    # TODO(mdesnoyer): Write update image tests. That'll take a little time
 
class TestCMSAPI(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestCMSAPI, self).setUp()

        self.api = api.brightcove_api.CMSAPI('pub_id',
                                             'client_id',
                                             'client_secret')

        # Mock out the _send_request
        self.api._send_request = MagicMock()
        self.send_mock = self._future_wrap_mock(self.api._send_request)
        

    def tearDown(self):
        super(TestCMSAPI, self).tearDown()

    def get_request(self):
        cargs, kwargs = self.send_mock.call_args
        return cargs[0]

    @tornado.testing.gen_test
    def test_add_thumbnail(self):
        response = yield self.api.add_thumbnail('vid1', 'remote.jpg',
                                                'ref1')
        request = self.get_request()
        self.assertEquals(request.method, 'POST')
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg',
                           'reference_id' : 'ref1'})
        self.assertEquals(request.url,
                          ('https://cms.api.brightcove.com/v1/accounts/'
                           'pub_id/videos/vid1/assets/thumbnail'))

        self.send_mock.reset_mock()

        response = yield self.api.add_thumbnail('vid1', 'remote.jpg')
        request = self.get_request()
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg'})

    @tornado.testing.gen_test
    def test_add_poster(self):
        response = yield self.api.add_poster('vid1', 'remote.jpg',
                                             'ref1')
        request = self.get_request()
        self.assertEquals(request.method, 'POST')
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg',
                           'reference_id' : 'ref1'})
        self.assertEquals(request.url,
                          ('https://cms.api.brightcove.com/v1/accounts/'
                           'pub_id/videos/vid1/assets/poster'))

        self.send_mock.reset_mock()

        response = yield self.api.add_poster('vid1', 'remote.jpg')
        request = self.get_request()
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg'})

    @tornado.testing.gen_test
    def test_update_thumbnail(self):
        response = yield self.api.update_thumbnail('vid1', 'tid1',
                                                   'remote.jpg',
                                                   'ref1')
        request = self.get_request()
        self.assertEquals(request.method, 'PATCH')
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg',
                           'reference_id' : 'ref1'})
        self.assertEquals(request.url,
                          ('https://cms.api.brightcove.com/v1/accounts/'
                           'pub_id/videos/vid1/assets/thumbnail/tid1'))

        self.send_mock.reset_mock()

        response = yield self.api.update_thumbnail('vid1', 'tid1',
                                                   'remote.jpg')
        request = self.get_request()
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg'})

    @tornado.testing.gen_test
    def test_update_poster(self):
        response = yield self.api.update_poster('vid1', 'tid1',
                                                'remote.jpg',
                                                'ref1')
        request = self.get_request()
        self.assertEquals(request.method, 'PATCH')
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg',
                           'reference_id' : 'ref1'})
        self.assertEquals(request.url,
                          ('https://cms.api.brightcove.com/v1/accounts/'
                           'pub_id/videos/vid1/assets/poster/tid1'))

        self.send_mock.reset_mock()

        response = yield self.api.update_poster('vid1', 'tid1',
                                                'remote.jpg')
        request = self.get_request()
        self.assertEquals(json.loads(request.body), 
                          {'remote_url': 'remote.jpg'})

    @tornado.testing.gen_test
    def test_delete_thumbnail(self):
        response = yield self.api.delete_thumbnail('vid1', 'tid1')
        request = self.get_request()
        self.assertEquals(request.method, 'DELETE')
        self.assertEquals(request.url,
                          ('https://cms.api.brightcove.com/v1/accounts/'
                           'pub_id/videos/vid1/assets/thumbnail/tid1'))

    @tornado.testing.gen_test
    def test_delete_poster(self):
        response = yield self.api.delete_poster('vid1', 'tid1')
        request = self.get_request()
        self.assertEquals(request.method, 'DELETE')
        self.assertEquals(request.url,
                          ('https://cms.api.brightcove.com/v1/accounts/'
                           'pub_id/videos/vid1/assets/poster/tid1'))

    @tornado.testing.gen_test
    def test_get_video_images(self):
        response = yield self.api.get_video_images('vid1')
        request = self.get_request()
        self.assertEquals(request.method, 'GET')
        self.assertEquals(request.url,
                          ('https://cms.api.brightcove.com/v1/accounts/'
                           'pub_id/videos/vid1/images'))

if __name__ == "__main__" :
    args = utils.neon.InitNeon()
    unittest.main(argv=[__name__] + args)
