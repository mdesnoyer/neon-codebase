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
from api.brightcove_api import BrightcoveOAuth2Session, PlayerAPI,\
    BrightcoveApiNotAuthorizedError, BrightcoveApiClientError, \
    BrightcoveApiServerError
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
from cmsdb.neondata import BrightcoveIntegration
from collections import OrderedDict
from requests.models import Response
from utils.options import define, options


_log = logging.getLogger(__name__)

define('run_tests_on_test_account', default=0, type=int,
       help='If set, will run tests that hit the real Brightcove APIs')

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
        yield neondata.BrightcovePlayer(given_ref, name=given_name).save(async=True)
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
        if not options.run_tests_on_test_account:
            raise unittest.SkipTest('Should only be run manually because it '
                                    'hits Brightcove')
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


class TestOAuth(test_utils.neontest.AsyncTestCase):
    '''Tests for the inner OAuth session for Brightcove API

    These were taken from cmsapiv2/test/client_py; these can be refactored'''

    def setUp(self):
        super(TestOAuth, self).setUp()

        # Get an API to exercise the OAuth session
        integration = neondata.BrightcoveIntegration(
            a_id='a0',
            p_id='p0',
            application_client_id='id',
            application_client_secret='secret')
        self.api = PlayerAPI(integration)
        self.expect_token = 'expected'
        self.expect_auth_header = 'aWQ6c2VjcmV0'  # b64e of id:secret

        # Mock out the http requests
        self.auth_mock = MagicMock()
        self.auth_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x,
              code=200,
              buffer=StringIO(
                  '{"access_token":"%s","token_type": "Bearer","expires_in":300}' %
                  self.expect_token))
        self.api_mock = MagicMock()
        # Use a get_players structure response
        self.api_mock.side_effect = \
            lambda x, **kw: tornado.httpclient.HTTPResponse(
                x,
                code=200,
                buffer=StringIO(
                    '{"items": ["a", "b", "c", "d"], "item_count": 4}'))
        self.send_request_patcher = patch('utils.http.send_request')
        self.send_request_mock = self._future_wrap_mock(
            self.send_request_patcher.start(), require_async_kw=True)

        def _handle_http_request(req, **kw):
            if BrightcoveOAuth2Session.TOKEN_URL in req.url:
                return self.auth_mock(req, **kw)
            else:
                return self.api_mock(req, **kw)
        self.send_request_mock.side_effect = _handle_http_request

    def tearDown(self):
        self.send_request_patcher.stop()
        super(TestOAuth, self).tearDown()

    @tornado.testing.gen_test
    def test_second_request_still_authed(self):

        res = yield self.api.get_players()
        self.assertEqual(type(res), dict)

        # Check that there was an authentication call with id, secret
        self.assertEquals(self.auth_mock.call_count, 1)
        cargs, kwargs = self.auth_mock.call_args_list[0]
        self.assertEquals(kwargs['no_retry_codes'], [401])
        auth_request = cargs[0]
        self.assertEquals(auth_request.url, BrightcoveOAuth2Session.TOKEN_URL)
        self.assertEquals(auth_request.method, 'POST')
        self.assertEquals(
            auth_request.headers,
            {'Authorization': 'Basic {}'.format(self.expect_auth_header)})
        self.assertEquals(self.api._token, self.expect_token)

        # Check that the main request went out
        self.assertEquals(self.api_mock.call_count, 1)
        cargs, kwargs = self.api_mock.call_args_list[0]
        self.assertEquals(kwargs['no_retry_codes'], [401])
        api_request = cargs[0]
        self.assertIn(PlayerAPI.BASE_URL, api_request.url)
        self.assertEquals(api_request.method, 'GET')
        self.assertEquals(
            api_request.headers,
            {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer %s' % self.api._token
            })

        # Now reset the mocks
        self.auth_mock.reset_mock()
        self.api_mock.reset_mock()

        # Send another request. Shouldn't need to hit the auth server
        res = yield self.api.get_players()
        self.assertIs(type(res), dict)
        self.assertFalse(self.auth_mock.called)
        self.assertEquals(self.api_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_expired_token_is_handled(self):
        # Trigger normal authentication
        res = yield self.api.get_players()
        self.assertIs(type(res), dict)
        self.assertIsNotNone(self.api._token)
        self.assertEquals(self.api_mock.call_count, 1)
        self.assertEquals(self.auth_mock.call_count, 1)
        self.auth_mock.reset_mock()
        self.api_mock.reset_mock()

        # Now simulate losing authentication
        self.api_mock.side_effect = [
            tornado.httpclient.HTTPResponse(
                HTTPRequest(''),
                code=401,
                error=tornado.httpclient.HTTPError(401)),
            tornado.httpclient.HTTPResponse(
                HTTPRequest(''), code=200)]
        res = yield self.api.get_players()
        self.assertIsNone(res)
        self.assertIsNotNone(self.api._token)
        self.assertEquals(self.api_mock.call_count, 2)
        self.assertEquals(self.auth_mock.call_count, 1)

        # Check the auth call
        cargs, kwargs = self.auth_mock.call_args_list[0]
        self.assertEquals(kwargs['no_retry_codes'], [401])
        auth_request = cargs[0]
        self.assertEquals(auth_request.url, BrightcoveOAuth2Session.TOKEN_URL)
        self.assertEquals(auth_request.method, 'POST')
        self.assertEquals(auth_request.body, 'grant_type=client_credentials')
        self.assertEquals(
            auth_request.headers,
            {'Authorization': 'Basic %s' % self.expect_auth_header})

        # Now reset the mocks
        self.auth_mock.reset_mock()
        self.api_mock.reset_mock()
        self.api_mock.side_effect = \
            lambda x, **kw: tornado.httpclient.HTTPResponse(
                x,
                code=200,
                buffer=StringIO(
                    '{"items": ["a", "b", "c", "d"], "item_count": 4}'))

        # Send another request. Shouldn't need to hit the auth server
        res = yield self.api.get_players()
        self.assertIs(type(res), dict)
        self.assertFalse(self.auth_mock.called)
        self.assertEquals(self.api_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_bad_user_pass(self):
        '''Check that a 401 returned from auth request raises an error'''
        self.auth_mock.side_effect = \
            lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=401)
        with self.assertRaises(BrightcoveApiNotAuthorizedError) as e:
            yield self.api.get_players()
            self.assertEquals(e.errno, 401)
            import pdb; pdb.set_trace()
            self.assertEquals(e.strerror, 401)

    @tornado.testing.gen_test
    def test_not_enough_permissions(self):
        '''Check that an api request that triggers a 401 raises error'''
        self.api_mock.side_effect = \
            lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=500)
        with self.assertRaises(BrightcoveApiServerError) as e:
            yield self.api.get_players()
            self.assertEquals(e.errno, 500)
        self.assertEquals(self.api_mock.call_count, 1)
        self.assertEquals(self.auth_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_service_error(self):
        # Test when Brightcove responds with 500
        self.api_mock.side_effect = \
            lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=401)
        with self.assertRaises(BrightcoveApiNotAuthorizedError) as e:
            yield self.api.get_players()
            self.assertEquals(e.errno, 401)
        self.assertEquals(self.api_mock.call_count, 2)
        self.assertEquals(self.auth_mock.call_count, 2)

    @tornado.testing.gen_test
    def test_invalid_api_call(self):
        self.api_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=404)
        with self.assertRaises(BrightcoveApiClientError) as e:
            yield self.api.get_players()
            self.assertEquals(e.errno, 404)

        self.assertEquals(self.api_mock.call_count, 1)
        self.assertEquals(self.auth_mock.call_count, 1)
        self.assertEquals(self.api._token, self.expect_token)


if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
