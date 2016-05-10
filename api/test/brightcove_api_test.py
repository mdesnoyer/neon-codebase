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
from api.brightcove_api import BrightcoveOAuth2Session, PlayerAPI
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
from cvutils.imageutils import PILImageUtils
import utils.neon
from cmsdb.neondata import BrightcoveIntegration
from collections import OrderedDict
from requests.models import Response
import voluptuous

_log = logging.getLogger(__name__)

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

class TestBrightcoveOAuth2Session(test_utils.neontest.AsyncTestCase):

    def setUp(self):
        super(TestBrightcoveOAuth2Session, self).setUp()

    def tearDown(self):
        super(TestBrightcoveOAuth2Session, self).tearDown()


class TestPlayerAPI(test_utils.neontest.AsyncTestCase):

    def setUp(self):
        super(TestPlayerAPI, self).setUp()

        # Set up an Api instance to use for each test
        integ = BrightcoveIntegration('test_integration')
        integ.application_client_id = 'test_client_id'
        integ.application_client_secret = 'test_client_secret'
        integ.publisher_id = 12345
        self.api = PlayerAPI(integ)

    def tearDown(self):
        super(TestPlayerAPI, self).tearDown()

    @tornado.testing.gen_test
    def test_has_required_access(self):

        self.api.token = None
        with patch('api.brightcove_api.BrightcoveOAuth2Session._authenticate') as _auth:
            auth = self._future_wrap_mock(_auth)
            auth.side_effect = HTTPError(401)
            rv = yield self.api.has_required_access()
        self.assertFalse(rv)

        self.api._token = 'set'
        with patch('api.brightcove_api.BrightcoveOAuth2Session._send_request') as _send:
            send = self._future_wrap_mock(_send)
            send.side_effect = [HTTPResponse(
                HTTPRequest(''),
                code=401,
                error=HTTPError(401))]
            rv = yield self.api.has_required_access()
        self.assertFalse(rv)

        # Both authorize api calls pass
        with patch('api.brightcove_api.BrightcoveOAuth2Session._send_request') as _send:
            send = self._future_wrap_mock(_send)
            send.side_effect = [
                {'items': [{'id': 123}]},
                HTTPResponse(
                    HTTPRequest(''),
                    code=404,
                    error=HTTPError(404))
            ]
            rv = yield self.api.has_required_access()
            self.assertEqual(send.call_count, 2)
        self.assertTrue(rv)

    @tornado.testing.gen_test
    def test_get_player(self):

        given_ref = 'BkMO9qa8x'
        given_name = 'neon player'
        neondata.BrightcovePlayer(given_ref, name=given_name).save()
        given_account = 12345

        with patch('api.brightcove_api.BrightcoveOAuth2Session._send_request') as _send:
            send = self._future_wrap_mock(_send)
            send.side_effect = [{
                'id': given_ref,
                'name': given_name,
                'accountId': given_account
            }]
            player = yield self.api.get_player(given_ref)
            self.assertEqual(send.call_count, 1)

        self.assertEqual(player['id'], given_ref)
        self.assertEqual(player['name'], given_name)

    @tornado.testing.gen_test
    def test_get_players(self):

        given_ref = 'BkMO9qa8x'
        given_name = 'neon player'
        given_ref_2 = 'h9fO9qa8x'
        given_name_2 = 'alternate player'
        given_account = 12345

        with patch('api.brightcove_api.BrightcoveOAuth2Session._send_request') as _send:
            send = self._future_wrap_mock(_send)
            send.side_effect = [{'items': [
                {
                    'id': given_ref,
                    'name': given_name,
                    'accountId': given_account
                }, {
                    'id': given_ref_2,
                    'name': given_name_2,
                    'accountId': given_account
                }
            ]}]
            result = yield self.api.get_players()
            self.assertEqual(send.call_count, 1)

        players = result['items']
        self.assertEqual(2, len(players))
        self.assertEqual(players[0]['id'], given_ref)
        self.assertEqual(players[0]['name'], given_name)
        self.assertEqual(players[1]['id'], given_ref_2)
        self.assertEqual(players[1]['name'], given_name_2)

    @tornado.testing.gen_test
    def test_valid_patch_player(self):
        given_ref = 'ref'
        payload = {'autoplay': True}
        with patch('api.brightcove_api.BrightcoveOAuth2Session._send_request') as _send:
            send = self._future_wrap_mock(_send)
            send.side_effect = [{'id': given_ref}]
            result = yield self.api.patch_player(given_ref, payload)
            self.assertEqual(send.call_count, 1)
        self.assertEqual(result['id'], given_ref)

    @tornado.testing.gen_test
    def test_publish_player(self):
        given_ref = 'ref'
        with patch('api.brightcove_api.BrightcoveOAuth2Session._send_request') as _send:
            send = self._future_wrap_mock(_send)
            send.side_effect = [{'id': given_ref}]
            result = yield self.api.publish_player(given_ref)
            self.assertEqual(send.call_count, 1)
        self.assertEqual(result['id'], given_ref)


class TestPlayerAPIIntegration(test_utils.neontest.AsyncTestCase):

    # Test integration config
    client_id = '8b089370-ce31-4ecf-9c14-7ffc6ff492b9'
    client_secret = 'zZu6_l62UCYhjpTuwEfWrNDrjEqyP9Pg19Sv5BUUGCig1CMA-mIuxy14DjH6n1xQHZi3_RPYfO8_YRGh8xAyyg'
    publisher_id = 2294876105001
    player_id = 'BkMO9qa8x'

    def setUp(self):
        super(TestPlayerAPIIntegration, self).setUp()

    def tearDown(self):
        super(TestPlayerAPIIntegration, self).tearDown()

    def _get_integration_api(self):
        return PlayerAPI(
            client_id=self.client_id,
            client_secret=self.client_secret,
            publisher_id=self.publisher_id)

    @tornado.testing.gen_test(timeout=15)
    def test_integration_client_credential(self):
        '''Exercise the read loop with a test Brightcove account'''

        api = self._get_integration_api()
        self.assertTrue(api.has_required_access())
        players = yield api.get_players()
        search_ref = players['items'][0]['id']

        # Repeat the search with the id we found
        player = yield api.get_player(search_ref)
        self.assertEqual(search_ref, player['id'])

    @tornado.testing.gen_test(timeout=15)
    def test_patch_and_publish_flow(self):
        '''Exercise the get_player_config, patch, publish apis with real data'''

        api = self._get_integration_api()
        # Let's flip the autoplay flag
        config = yield api.get_player_config(self.player_id)
        orig_autoplay = config['autoplay']
        new_autoplay = not orig_autoplay
        patch = {'autoplay': new_autoplay}
        patch_response = yield api.patch_player(self.player_id, patch)

        # Confirm that the preview config is altered, master is unchanged
        patched_player = yield api.get_player(self.player_id)
        self.assertEqual(orig_autoplay, patched_player['branches']['master']['configuration']['autoplay'])
        # self.assertEqual(new_autoplay, patched_player['branches']['preview']['configuration']['autoplay'])

        # Publish the player and check the master value for autoplay
        yield api.publish_player(self.player_id)
        published_player = yield api.get_player(self.player_id)
        self.assertEqual(new_autoplay, published_player['branches']['master']['configuration']['autoplay'])
        published_config = yield api.get_player_config(self.player_id)
        self.assertEqual(new_autoplay, published_config['autoplay'])


if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
