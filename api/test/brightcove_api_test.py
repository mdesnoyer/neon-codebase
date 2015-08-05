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
from utils.imageutils import PILImageUtils
import utils.neon

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
        self.http_call_patcher.stop()
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

    def _set_videos_to_return(self, videos):
        '''Define the videos to return. Should be video structures.'''
        self.videos_to_return = videos
        def respond_with_videos(request, callback=None):
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
            
            
            response = HTTPResponse(request, 200,
                                    buffer=StringIO(json.dumps(retstruct)))
            if callback:
                tornado.ioloop.IOLoop.current().add_callback(callback,
                                                             response)
            else:
                return response

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
      
    @patch('api.brightcove_api.utils.http.send_request')
    def test_check_feed(self, utils_http_patch):
    
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
        def _setup_plat(x):
            x.publisher_id = 'p1'
            x.read_token = 'rt'
            x.write_token = 'wt'
            x.last_process_date = 21492000000
            x.account_created = 21492000
        bp = neondata.BrightcovePlatform.modify(na.neon_api_key, i_id,
                                                _setup_plat,
                                                create_missing=True)
        bp.check_feed_and_create_api_requests()
        u_bp = neondata.BrightcovePlatform.get(na.neon_api_key, i_id)
        self.assertEqual(len(u_bp.get_videos()), nvideos)

    @patch('api.brightcove_api.BrightcoveApi.write_connection.send_request') 
    @tornado.testing.gen_test 
    def test_add_image(self, write_conn_mock):
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
        write_conn_mock.side_effect = lambda x, callback: callback(response)
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
        r_url = "http://i1.neon-images.com/video_id1?height=10&width=20"
        response = HTTPResponse(HTTPRequest("http://bcove"), 200,
                buffer=StringIO('{"result":{"id":"newtid", "remoteUrl":"%s"}}'
                                % r_url))
        write_conn_mock.side_effect = lambda x, callback: callback(response)
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

    @patch('api.brightcove_api.utils.http')
    def test_create_video_request(self, http_mock): 
        send_request_mock = self._callback_wrap_mock(http_mock.send_request)
        send_request_mock.side_effect = [HTTPResponse(HTTPRequest("http://test"), 200)]
                                          
        bc = api.brightcove_api.BrightcoveApi(
            "neon_api_key", "publisher_id",
            "read_token", "write_token", callback_url="http://callback.invalid")
 
        response = bc.format_neon_api_request('vid1', 'http://fake_dl_url', callback=None)
        cargs, kwargs = send_request_mock.call_args
        call_json = json.loads(cargs[0].body)
        self.assertEquals(call_json['callback_url'], "http://callback.invalid") 

    def test_select_rendition(self):
        '''
        Test the selection of the right rendition
        
        Assert that url is returned irrespective of framewidth 
        '''
        vitems = json.loads(bcove_responses.find_all_videos_response)
        bc = api.brightcove_api.BrightcoveApi(
            "neon_api_key", "publisher_id",
            "read_token", "write_token", False)
       
        frame_widths = [None, 640, 720, 420]
        for item in vitems['items']:
            for fwidth in frame_widths:
                url = bc.get_video_url_to_download(item, fwidth)
                self.assertIsNotNone(url)
        
        # Check max rendition returned of frame width 1280
        item = vitems['items'][-1]
        url = bc.get_video_url_to_download(item, None)
        self.assertEqual(url,
                        "http://brightcove.vo.llnwd.net/e1/uds/pd/2294876105001/2294876105001_2635148067001_PA220134.mp4")

    @patch('api.brightcove_api.utils.http.send_request')
    def test_create_request_from_playlist(self, utils_http):
        p_response = HTTPResponse(HTTPRequest("http://bcove"), 200,
                buffer=StringIO(bcove_responses.find_playlist_by_id_response))
        n_response = HTTPResponse(HTTPRequest("http://neon"), 200,
                    buffer=StringIO('{"job_id":"j123"}'))
        def _side_effect(request, callback=None, *args, **kwargs):
            if "submitvideo" in request.url:
                return n_response
            else:
                return p_response

        utils_http.side_effect = _side_effect
        a_id = 'test' 
        i_id = 'i123'
        nvideos = 2 
        na = neondata.NeonUserAccount('acct1')
        na.save()
        def _setup_plat(x):
            x.publisher_id = 'p1'
            x.read_token = 'rt'
            x.write_token = 'wt'
            x.last_process_date = 21492000000
            x.account_created = 21492000
            x.add_video('v1', 'j1')
            x.playlist_feed_ids.append('1234')
        bp = neondata.BrightcovePlatform.modify(na.neon_api_key, i_id,
                                                _setup_plat,
                                                create_missing=True)
        bp.check_playlist_feed_and_create_requests()
        u_bp = neondata.BrightcovePlatform.get(na.neon_api_key, i_id)
        self.assertEqual(len(u_bp.get_videos()), nvideos)
        self.assertListEqual(u_bp.get_videos(), ['v1', '4100953290001'])

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

if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
