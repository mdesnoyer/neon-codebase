#!/usr/bin/env python
'''
Test functionality of the click log server.
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import avro.io
import avro.schema
import base64
import __builtin__
from clickTracker.flume.ttypes import *
import clickTracker.trackserver 
from clickTracker import TTornado
from cStringIO import StringIO
import fake_filesystem
import json
import hashlib
import logging
from mock import patch
from mock import MagicMock
import os
import Queue
import random
import socket
import test_utils.neontest
from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
import time
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import tornado.iostream
import urllib
import urlparse
import unittest
import utils.neon
from utils.options import options

class TestFileBackupHandler(unittest.TestCase):
    def setUp(self):
        schema_path = options.get('clickTracker.trackserver.message_schema')
        with open(schema_path) as f:
            schema_str = f.read()
        schema = avro.schema.parse(schema_str)
        schema_hash = hashlib.md5(json.dumps(schema.to_json())).hexdigest()
        self.schema_url = ('http://bucket.s3.amazonaws.com/%s.avsc' % 
                           schema_hash)
        self.avro_writer = avro.io.DatumWriter(schema)
        self.avro_reader = avro.io.DatumReader(schema, schema)
        
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.fake_os = fake_filesystem.FakeOsModule(self.filesystem)
        self.fake_open = fake_filesystem.FakeFileOpen(self.filesystem)
        clickTracker.trackserver.os = self.fake_os
        clickTracker.trackserver.open = self.fake_open

    def tearDown(self):
        clickTracker.trackserver.os = os
        clickTracker.trackserver.open = __builtin__.open

    def processData(self):
        '''Sends 1000 requests to a handler and waits until they are done.'''
        
        user_agent = "Mozilla/5.0 (Linux; U; Android 2.3.5; en-in; "\
                "HTC_DesireS_S510e Build/GRJ90) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
        mock_click_request = MagicMock()
        click_fields = {'pageid' : 'pageid234',
                        'tai' : 'tai234',
                        'ttype' : 'brightcove',
                        'page' : 'http://go.com',
                        'ref' : 'http://ref.com',
                        'cts' : '23945827',
                        'a' : 'ic',
                        'tid' : 'acct1_vid1_tid345',
                        'x' : '3467',
                        'y' : '123',
                        'wx' : '567',
                        'wy' : '9678',
                        'cx' : '49',
                        'cy' : '65'}
        def mock_click_get_argument(field, default=[]):
            return click_fields[field]
        mock_click_request.get_argument.side_effect = mock_click_get_argument
        mock_click_request.request.remote_ip = '12.43.151.12'
        mock_click_request.get_cookie.return_value = 'cookie1'
        mock_click_request.request.headers = {} 
        mock_click_request.request.headers['User-Agent'] = user_agent 

        mock_view_request = MagicMock()
        view_fields = {'pageid' : 'pageid67',
                       'tai' : 'tai20',
                       'ttype' : 'brightcove',
                       'page' : 'http://go1.com',
                       'ref' : 'http://ref1.com',
                       'cts' : '23945898',
                       'a' : 'iv',
                       'tids' : 'acct1_vid1_tid345,acct1_vid2_tid346'
                       }
        def mock_view_get_request(field, default=[]):
            return view_fields[field]
        mock_view_request.get_argument.side_effect = mock_view_get_request
        mock_view_request.request.remote_ip = '12.43.151.120'
        mock_view_request.get_cookie.return_value = 'cookie2'
        mock_view_request.request.headers = { 
                'User-Agent': user_agent,
                'Geoip_city_country_name': 'United States',
                'Geoip_country_code3': 'USA',
                'Geoip_latitude': '37.7794',
                'Geoip_city': 'San Francisco',
                'Geoip_longitude': '-122.4170',
                'Geoip_postal_code': ''
                }
        
        # Start a thread to handle the data
        dataQ = Queue.Queue()
        handle_thread = clickTracker.trackserver.FileBackupHandler(dataQ)
        handle_thread.start()

        # Send data
        nlines = 400
        for i in range(nlines/2):
            click = clickTracker.trackserver.BaseTrackerDataV2.generate(
                mock_click_request)
            
            dataQ.put(click.to_flume_event(self.avro_writer, self.schema_url))

            view = clickTracker.trackserver.BaseTrackerDataV2.generate(
                mock_view_request)
            dataQ.put(view.to_flume_event(self.avro_writer, self.schema_url))
            
        # Wait until the data is processeed 
        dataQ.join()

        # Force a flush to disk
        handle_thread.backup_stream.flush()

    def test_log_to_disk_normally(self):
        '''Check the mechanism to log to disk.'''
        log_dir = '/tmp/fake_log_dir'
        with options._set_bounded('clickTracker.trackserver.backup_disk',
                                  log_dir):
            with options._set_bounded(
                    'clickTracker.trackserver.backup_max_events_per_file',
                    100):
                self.processData()

                files = self.fake_os.listdir(log_dir)
                self.assertEqual(len(files), 4)

                # Open one of the files and check that it contains
                # what we expect
                with self.fake_open(os.path.join(log_dir, files[0])) as f:
                    transport = TTransport.TFileObjectTransport(f)
                    protocol = TCompactProtocol.TCompactProtocol(transport)
                    event = ThriftFlumeEvent()
                    event.read(protocol)
                    try:
                        while event.body is not None:
                            msgbuf = StringIO(event.body)
                            body = self.avro_reader.read(
                                avro.io.BinaryDecoder(msgbuf))
                            if body['eventType'] == 'IMAGE_CLICK':
                                self.assertEqual(body['pageId'], 'pageid234')
                                self.assertEqual(body['trackerAccountId'],
                                                 'tai234')
                                self.assertEqual(body['trackerType'], 'BRIGHTCOVE')
                                self.assertEqual(body['pageURL'], 'http://go.com')
                                self.assertEqual(body['refURL'], 'http://ref.com')
                                self.assertEqual(body['clientIP'], '12.43.151.12')
                                self.assertEqual(body['clientTime'], 23945827)
                                self.assertGreater(body['serverTime'],
                                                   1300000000000)
                                self.assertEqual(body['eventData']['thumbnailId'],
                                                 'acct1_vid1_tid345')
                                self.assertEqual(
                                    body['eventData']['pageCoords']['x'], 3467)
                                self.assertEqual(
                                    body['eventData']['pageCoords']['y'], 123)
                                self.assertEqual(
                                body['eventData']['windowCoords']['x'], 567)
                                self.assertEqual(
                                    body['eventData']['windowCoords']['y'], 9678)
                                self.assertTrue(body['eventData']['isImageClick'])
                            elif body['eventType'] == 'IMAGES_VISIBLE':
                                self.assertEqual(body['pageId'], 'pageid67')
                                self.assertEqual(body['trackerAccountId'], 'tai20')
                                self.assertEqual(body['trackerType'], 'BRIGHTCOVE')
                                self.assertEqual(body['pageURL'], 'http://go1.com')
                                self.assertEqual(body['refURL'], 'http://ref1.com')
                                self.assertEqual(body['clientIP'], '12.43.151.120')
                                self.assertEqual(body['clientTime'], 23945898)
                                self.assertGreater(body['serverTime'],
                                                   1300000000000)
                                self.assertItemsEqual(
                                    body['eventData']['thumbnailIds'], 
                                    ['acct1_vid1_tid345', 'acct1_vid2_tid346'])
                                self.assertEqual(
                                    body["ipGeoData"]["city"], "San Francisco")
                                self.assertEqual(
                                    body["ipGeoData"]["country"], "USA"),
                                self.assertIsNone(body["ipGeoData"]["region"])
                                self.assertIsNone(body["ipGeoData"]["zip"])
                                self.assertAlmostEqual(body["ipGeoData"]["lat"],
                                                       37.7794, 4)
                                self.assertAlmostEqual(body["ipGeoData"]["lon"],
                                                       -122.4170, 4)
                            else:
                                self.fail('Bad event field %s' % body['eventType'])
                        
                            event.read(protocol)
                    except EOFError:
                        pass
        

class TestFullServer(test_utils.neontest.AsyncHTTPTestCase):
    '''A set of tests that fire up the whole server and throws http requests at it.'''

    def setUp(self):
        schema_path = options.get('clickTracker.trackserver.message_schema')
        with open(schema_path) as f:
            schema_str = f.read()
        schema = avro.schema.parse(schema_str)
        self.avro_reader = avro.io.DatumReader(schema, schema)
        
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.fake_os = fake_filesystem.FakeOsModule(self.filesystem)
        self.fake_open = fake_filesystem.FakeFileOpen(self.filesystem)
        clickTracker.trackserver.os = self.fake_os
        clickTracker.trackserver.open = self.fake_open

        # Put the avro schema in the fake filesystem
        self.fake_os.makedirs(os.path.dirname(schema_path))
        with self.fake_open(schema_path, 'w') as f:
            f.write(schema_str)

        self.thrift_patcher = patch(
            'clickTracker.trackserver.ThriftSourceProtocol.Client')
        self.thrift_mock = MagicMock()
        self.thrift_patcher.start().return_value = self.thrift_mock
        self.thrift_mock.appendBatch.side_effect = \
          lambda events, callback: self.io_loop.add_callback(callback,
                                                             Status.OK)

        self.isp_patcher = patch(
            'clickTracker.trackserver.utils.http.send_request')
        self.bn_map = {}
        self.isp_mock = self.isp_patcher.start()
        self.isp_mock.side_effect = self.mock_isp_response

        self.thrift_transport_patcher = patch('clickTracker.trackserver.TTornado.TTornadoStreamTransport')
        self.thrift_transport_mock = self.thrift_transport_patcher.start()
        self.thrift_transport_mock().open.side_effect = \
          lambda callback: self.io_loop.add_callback(callback)

        # Set the flush interval to 1 so that the data isn't buffered 
        self.old_flush_interval = \
          options.get('clickTracker.trackserver.flume_flush_interval')
        options._set('clickTracker.trackserver.flume_flush_interval', 1)

        self.server_obj = clickTracker.trackserver.Server()
        self.backup_q = self.server_obj.backup_queue
        super(TestFullServer, self).setUp()

        random.seed(168984)

    def tearDown(self):
        options._set('clickTracker.trackserver.flume_flush_interval',
                     self.old_flush_interval)
        self.thrift_patcher.stop()
        self.thrift_transport_patcher.stop()
        self.isp_patcher.stop()
        
        super(TestFullServer, self).tearDown()
        clickTracker.trackserver.os = os
        clickTracker.trackserver.open = __builtin__.open

    def get_app(self):
        return self.server_obj.application

    def get_httpserver_options(self):
        return {'xheaders': True}
    
    def mock_isp_response(self, request, retries=1, callback=None):
        if request.url.endswith('.avsc'):
            retval = HTTPResponse(request, 200)
        else:
            bns = urlparse.parse_qs(urlparse.urlparse(request.url).query
                                    )['params'][0].split(',')
            retval = HTTPResponse(
                request,
                200,
                buffer=StringIO(','.join([self.bn_map.get(x, "null") 
                                          for x in bns])))

        if callback:
            callback(retval)
        else:
            return retval

    def check_message_sent(self, url_params, ebody, neon_id=None,
                           path='/v2'):
        '''Sends a message and checks the body to be what's expected.

        Inputs:
        url_params - Dictionary of url params
        ebody - json dictionary of expected body in the logged message
        neon_id - Neon id to store in a cookie
        path - Endpoint for the server
        '''
        self.thrift_mock.reset_mock()
        
        headers = {}
        headers['User-Agent'] = (
            "Mozilla/5.0 (Linux; U; Android 2.3.5; en-in; HTC_DesireS_S510e "
            "Build/GRJ90) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 "
            "Mobile Safari/533.1")
        if neon_id is not None:
            headers['Cookie'] = 'neonglobaluserid=%s' % neon_id
        headers['X-Forwarded-For'] = '123.4.56.78'
        response = self.fetch(
            '%s?%s' % (path, urllib.urlencode(url_params)),
            headers=headers)

        self.assertEqual(response.code, 200)

        # Check that a response was found
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 1)
        request_saw = self.thrift_mock.appendBatch.call_args[0][0][0]
        headers = request_saw.headers
        binary_body = request_saw.body
        
        self.assertTrue(headers['timestamp'])
        
        if 'v2' in path:
            msgbuf = StringIO(binary_body)
            body = self.avro_reader.read(avro.io.BinaryDecoder(msgbuf))
        
            self.assertEqual(headers['track_vers'], '2.2')
            self.assertEqual(headers['event'], ebody['eventType'])
            self.assertEqual(headers['timestamp'],
                         str(body['serverTime']))
            self.assertEqual(headers['tai'],
                             ebody['trackerAccountId'])
            
            self.assertEqual(body['clientIP'], '123.4.56.78')
            if neon_id is not None:
                self.assertEqual(body['neonUserId'], neon_id)

            self.assertEqual(
                body['agentInfo'],
                {'os': {'name': 'Android', 'version': '2.3.5'},
                 'browser' : {'name' : 'Safari', 'version' : '4.0' }})
        else:
            self.assertEqual(headers['track_vers'], '1')
            self.assertEqual(headers['event'], ebody['a'])
            body = json.loads(binary_body)

        self.assertDictContainsSubset(ebody, body)

    def test_v2_valid_messages(self):
        # Image Visible Message
        
        # TODO(mdesnoyer): Remove dash separated example once the
        # tracker won't send it anymore.
        self.check_message_sent(
            { 'a' : 'iv',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tids' : 'acct1_vid1_tid1,acct1_vid2_tid2,acct2-vid1-tid3'},
            { 'eventType' : 'IMAGES_VISIBLE',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData': { 
                  'isImagesVisible' : True,
                  'thumbnailIds' : ['acct1_vid1_tid1', 'acct1_vid2_tid2',
                                    'acct2-vid1-tid3']
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Image loaded message
        self.check_message_sent(
            { 'a' : 'il',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tids' : 'acct1_vid1_tid1 56 67,acct1_vid2_tid2 89 123'}, #tornado converts + to " "
            { 'eventType' : 'IMAGES_LOADED',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData': {
                  'isImagesLoaded' : True,
                  'images' : [
                  {'thumbnailId': 'acct1_vid1_tid1',
                   'height' : 67,
                   'width' :56 },
                  {'thumbnailId': 'acct1_vid2_tid2',
                   'height' : 123,
                   'width' : 89}]},
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Image clicked message
        self.check_message_sent(
            { 'a' : 'ic',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tid' : 'acct1_vid1_tid1',
              'x' : '56',
              'y' : '23',
              'wx' : '78',
              'wy' : '34',
              'cx' : '6',
              'cy' : '8'},
            { 'eventType' : 'IMAGE_CLICK',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'isImageClick' : True,
                  'thumbnailId' : 'acct1_vid1_tid1',
                  'pageCoords' : {
                      'x' : 56,
                      'y' : 23,
                      },
                  'windowCoords' : {
                      'x' : 78,
                      'y' : 34
                      },
                  'imageCoords' : {
                      'x' : 6,
                      'y' : 8
                      },
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )
        
        #Video clicked message
        self.check_message_sent(
            { 'a' : 'vc',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'vid' : 'vid1',
              'tid' : 'acct1_vid1_tid1',
              'playerid' : 'brightcoveP123',
              },
            { 'eventType' : 'VIDEO_CLICK',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'videoId' : 'vid1',
                  'thumbnailId' : 'acct1_vid1_tid1',
                  'playerId' : 'brightcoveP123',
                  'isVideoClick' : True
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Video play message
        self.check_message_sent(
            { 'a' : 'vp',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tid' : 'acct1_vid1_tid1',
              'vid' : 'vid1',
              'adplay': 'False',
              'adelta': 'null',
              'pcount': '1',
              'playerid' : 'brightcoveP123'},
            { 'eventType' : 'VIDEO_PLAY',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'thumbnailId' : 'acct1_vid1_tid1',
                  'videoId' : 'vid1',
                  'didAdPlay': False,
                  'autoplayDelta': None,
                  'playCount': 1,
                  'playerId' : 'brightcoveP123',
                  'isVideoPlay' : True,
                  'isAutoPlay' : None,
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Ad play message
        self.check_message_sent(
            { 'a' : 'ap',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'gen',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tid' : 'acct1_vid1_tid1',
              'vid' : 'vid1',
              'adelta': '214',
              'pcount' : '1',
              'playerid' : 'brightcoveP123',
              },
            { 'eventType' : 'AD_PLAY',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'GENERAL',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'thumbnailId' : 'acct1_vid1_tid1',
                  'videoId' : 'vid1',
                  'autoplayDelta': 214,
                  'playCount': 1,
                  'playerId' : 'brightcoveP123',
                  'isAdPlay' : True,
                  'isAutoPlay' : None,
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Video view percentage
        self.check_message_sent(
            { 'a' : 'vvp',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'vid' : 'vid1',
              'pcount' : '1',
              'prcnt' : '23'
              },
            { 'eventType' : 'VIDEO_VIEW_PERCENTAGE',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'videoId' : 'vid1',
                  'playCount': 1,
                  'percent': 23.0,
                  'isVideoViewPercentage' : True
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )
    def test_v2_secondary_endpoint(self):
        self.check_message_sent(
            { 'a' : 'iv',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tids' : 'acct1_vid1_tid1,acct1_vid2_tid2'},
            { 'eventType' : 'IMAGES_VISIBLE',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData': { 
                  'isImagesVisible' : True,
                  'thumbnailIds' : ['acct1_vid1_tid1', 'acct1_vid2_tid2']
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1',
              '/v2/track'
            )

    def test_v2_no_referral_url(self):
        #Video clicked message
        self.check_message_sent(
            { 'a' : 'vc',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'cts' : '2345623',
              'vid' : 'vid1',
              'tid' : 'acct1_vid1_tid1',
              'playerid' : 'brightcoveP123',
              },
            { 'eventType' : 'VIDEO_CLICK',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : None,
              'clientTime' : 2345623,
              'eventData' : {
                  'videoId' : 'vid1',
                  'thumbnailId' : 'acct1_vid1_tid1',
                  'playerId' : 'brightcoveP123',
                  'isVideoClick' : True
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

    def test_no_tids_but_basename(self):
        self.bn_map = {
            'vid2' : 'acct1_vid2_tid1',
            'vid3' : 'acct1_vid3_tid0',
            'vid5' : 'acct1_vid5_tid5',
            'my.vid~-3' : 'acct3_my.vid~-3_tid7'
            }

        # Image Visible Message
        self.check_message_sent(
            { 'a' : 'iv',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'bns' : ('neonvid_vid2, neonvid_vid5.jpg?width=50&height=30,'
                       'neontnacct1_vid3_tid2.jpg,neonvid_my.vid~-3')},
            { 'eventType' : 'IMAGES_VISIBLE',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData': { 
                  'isImagesVisible' : True,
                  'thumbnailIds' : ['acct1_vid2_tid1',
                                    'acct1_vid5_tid5',
                                    'acct1_vid3_tid2',
                                    'acct3_my.vid~-3_tid7'
                                    ]
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )
        bnrequest = self.isp_mock.call_args[0][0]
        self.assertEqual(bnrequest.url,
                         'http://127.0.0.1:8089/v1/getthumbnailid/tai123?params=vid2,vid5,my.vid~-3')
        self.assertDictContainsSubset({'Cookie' : 'neonglobaluserid=neon_id1'},
                                      bnrequest.headers)
        self.assertDictContainsSubset({'X-Forwarded-For' : '123.4.56.78'},
                                      bnrequest.headers)

        # Image loaded message
        self.check_message_sent(
            { 'a' : 'il',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'bns' : ('neonvid_vid2 56 67,'
                       'neontnacct1_vid3_tid2.jpg 89 123')}, #tornado converts + to " "
            { 'eventType' : 'IMAGES_LOADED',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData': {
                  'isImagesLoaded' : True,
                  'images' : [
                  {'thumbnailId': 'acct1_vid2_tid1',
                   'height' : 67,
                   'width' :56 },
                  {'thumbnailId': 'acct1_vid3_tid2',
                   'height' : 123,
                   'width' : 89}]},
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Image clicked message
        self.check_message_sent(
            { 'a' : 'ic',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'bn' : 'neonvid_vid3',
              'x' : '56',
              'y' : '23',
              'wx' : '78',
              'wy' : '34',
              'cx' : '6',
              'cy' : '8'},
            { 'eventType' : 'IMAGE_CLICK',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'isImageClick' : True,
                  'thumbnailId' : 'acct1_vid3_tid0',
                  'pageCoords' : {
                      'x' : 56,
                      'y' : 23,
                      },
                  'windowCoords' : {
                      'x' : 78,
                      'y' : 34
                      },
                  'imageCoords' : {
                      'x' : 6,
                      'y' : 8
                      },
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )
        
        #Video clicked message
        self.check_message_sent(
            { 'a' : 'vc',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'vid' : 'vid1',
              'bn' : 'neontnacct1_vid2_tid1.png',
              'playerid' : 'brightcoveP123',
              },
            { 'eventType' : 'VIDEO_CLICK',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'videoId' : 'vid1',
                  'thumbnailId' : 'acct1_vid2_tid1',
                  'playerId' : 'brightcoveP123',
                  'isVideoClick' : True
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Video play message
        self.check_message_sent(
            { 'a' : 'vp',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'bn' : 'neonvid_vid3',
              'vid' : 'vid1',
              'adplay': 'False',
              'pcount': '1',
              'playerid' : 'brightcoveP123',
              'aplay' : 'true'},
            { 'eventType' : 'VIDEO_PLAY',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'thumbnailId' : 'acct1_vid3_tid0',
                  'videoId' : 'vid1',
                  'didAdPlay': False,
                  'autoplayDelta': None,
                  'playCount': 1,
                  'playerId' : 'brightcoveP123',
                  'isVideoPlay' : True,
                  'isAutoPlay' : True,
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Ad play message
        self.check_message_sent(
            { 'a' : 'ap',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'gen',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'bn' : 'neonvid_vid3',
              'vid' : 'vid1',
              'pcount' : '1',
              'playerid' : 'brightcoveP123',
              'aplay' : 'false'
              },
            { 'eventType' : 'AD_PLAY',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'GENERAL',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'thumbnailId' : 'acct1_vid3_tid0',
                  'videoId' : 'vid1',
                  'autoplayDelta': None,
                  'playCount': 1,
                  'playerId' : 'brightcoveP123',
                  'isAdPlay' : True,
                  'isAutoPlay' : False
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

    def test_unknown_basename(self):
        # If the basename is unknown, we should return 200 because it
        # could be a video we don't care about. However, if there's
        # nothing we care about, the data shouldn't be sent on.
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bns' : 'neonvid_vid2'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'il',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bns' : 'some_video.jpg 56 84'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'ic',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bn' : 'some_video.jpg'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'vp',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bn' : 'somevideo.jpg',
             'vid' : 'somevideo',
             'adelta' : '520',
             'pcount' : '2'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'ap',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bn' : 'somevideo.jpg',
             'vid' : 'somevideo',
             'adelta' : '520',
             'pcount' : '2'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

    def test_some_valid_basenames(self):
        # In this case, we will return a 200, and a subset of the data
        # is sent on.
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bns' : ('someotherfile.jpg,acct1_vid2,neontnacct1_vid3_tid.jpg,'
                      'neontnacct1-vid4-tid6.jpg')}))

        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 1)
        request_saw = self.thrift_mock.appendBatch.call_args[0][0][0]
        msgbuf = StringIO(request_saw.body)
        body = self.avro_reader.read(avro.io.BinaryDecoder(msgbuf))
        self.assertEqual(body['eventData']['thumbnailIds'],
                         ['acct1_vid3_tid', 'acct1_vid4_tid6'])

        self.thrift_mock.appendBatch.reset_mock()
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'il',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bns' : 'someotherfile.jpg 65 48,acct1_vid2 32 45,neontnacct1_vid3_tid.jpg 78 94'}))

        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 1)
        request_saw = self.thrift_mock.appendBatch.call_args[0][0][0]
        msgbuf = StringIO(request_saw.body)
        body = self.avro_reader.read(avro.io.BinaryDecoder(msgbuf))
        self.assertEqual(body['eventData']['images'],
                         [{'thumbnailId' : 'acct1_vid3_tid',
                           'height' : 94,
                           'width' : 78}])

    def test_basenames_with_size_params(self):
        self.bn_map = {
            '3b6da68d318061a5a1df3e7ba44f54c7' : 
            'acct1_3b6da68d318061a5a1df3e7ba44f54c7_tid1'
            }
        
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'il',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bns' : 'neonvid_3b6da68d318061a5a1df3e7ba44f54c7?width=240&height=135 240 135'}))

        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 1)
        request_saw = self.thrift_mock.appendBatch.call_args[0][0][0]
        msgbuf = StringIO(request_saw.body)
        body = self.avro_reader.read(avro.io.BinaryDecoder(msgbuf))
        self.assertEqual(body['eventData']['images'],
                         [{'thumbnailId' : 
                           'acct1_3b6da68d318061a5a1df3e7ba44f54c7_tid1',
                           'height' : 135,
                           'width' : 240}])

    def test_bad_connection_to_isp(self):
        def _mock_isp(request, retries=1, callback=None):
            if request.url.endswith('.avsc'):
                retval = HTTPResponse(request, 200)
            else:
                retval = HTTPResponse(
                    request, 404, error=tornado.httpclient.HTTPError(404))

            if callback:
                callback(retval)
            else:
                return retval
        
        self.isp_mock.side_effect = _mock_isp
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bns' : 'neonvid_vid2'}))
        self.assertEqual(response.code, 500)

    def test_unexpected_parsing_exception(self):
        # Inject an exception when isp is queried. It doesn't matter
        # where we cause the injection, so this will work
        self.isp_mock.side_effect = Exception()
        with self.assertLogExists(logging.ERROR, "Unexpected error parsing"):
            response = self.fetch('/v2?%s' % urllib.urlencode(
                {'a' : 'iv',
                 'pageid' : 'pageid123',
                 'tai' : 'tai123',
                 'ttype' : 'brightcove',
                 'page' : 'http://go.com',
                 'ref' : 'http://ref.com',
                 'cts' : '2345623',
                 'bns' : 'neonvid_vid2'}))
            self.assertEqual(response.code, 500)

    def test_unexpected_exception_sending_flume_data(self):
        self.thrift_mock.appendBatch.side_effect = Exception()

        with self.assertLogExists(logging.ERROR,
                                  "Unexpected error sending data to flume:"):
            response = self.fetch('/v2?%s' % urllib.urlencode(
                {'a' : 'iv',
                 'pageid' : 'pageid123',
                 'tai' : 'tai123',
                 'ttype' : 'brightcove',
                 'page' : 'http://go.com',
                 'ref' : 'http://ref.com',
                 'cts' : '2345623',
                 'tids' : 'acct1_vid1_tid1,acct1_vid2_tid2'}))
            self.assertEqual(response.code, 500)

    def test_malformed_isp_response(self):
        def _mock_isp(request, retries=1, callback=None):
            if request.url.endswith('.avsc'):
                retval = HTTPResponse(request, 200)
            else:
                retval = HTTPResponse(
                    request, 200,
                    buffer=StringIO("This is a malformed response"))

            if callback:
                callback(retval)
            else:
                return retval
        
        self.isp_mock.side_effect = _mock_isp
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bns' : 'neonvid_vid2,neonvid_vid4'}))
        self.assertEqual(response.code, 500)

    def test_invalid_event_type(self):
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'abc',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid2_tid1'}))
        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body, 'Invalid event: abc')

    def test_invalid_video_id(self):
        with self.assertLogExists(logging.WARNING,
                                  'Video some_video is not interesting'):
            response = self.fetch('/v2?%s' % urllib.urlencode(
                {'a' : 'vp',
                 'pageid' : 'pageid123',
                 'tai' : 'tai123',
                 'ttype' : 'brightcove',
                 'page' : 'http://go.com',
                 'ref' : 'http://ref.com',
                 'cts' : '2345623',
                 'tid' : 'acct1_vid2_tid1',
                 'vid' : 'some_video',
                 'adplay': 'False',
                 'adelta': 'null',
                 'pcount': '1',
                 'playerid' : 'brightcoveP123'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

    def test_invalid_thumbnail_id(self):
        # For all these tests, we should return a 200, but ignore the data.
        
        # Test on a Image Visible event
        with self.assertLogExists(logging.WARNING,
                                  'No valid thumbnail ids'):
            response = self.fetch('/v2?%s' % urllib.urlencode(
                {'a' : 'iv',
                 'pageid' : 'pageid123',
                 'tai' : 'tai123',
                 'ttype' : 'brightcove',
                 'page' : 'http://go.com',
                 'ref' : 'http://ref.com',
                 'cts' : '2345623',
                 'tids' : 'acct1_some_video_tid1,vid~o_tid1,thumb3,acct1_v!r_tid1'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

        with self.assertLogExists(logging.WARNING,
                                  'No interesting thumbnail ids'):
            response = self.fetch('/v2?%s' % urllib.urlencode(
                {'a' : 'il',
                 'pageid' : 'pageid123',
                 'tai' : 'tai123',
                 'ttype' : 'brightcove',
                 'page' : 'http://go.com',
                 'ref' : 'http://ref.com',
                 'cts' : '2345623',
                 'tids' : 'acct1_some_video_tid1 56 87,vid~o_tid1 48 95,thumb3 32 65,acct1_v!r_tid1 32 45'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

        with self.assertLogExists(logging.WARNING,
                                  'Invalid thumbnail id'):
            response = self.fetch('/v2?%s' % urllib.urlencode(
                { 'a' : 'ic',
                  'pageid' : 'pageid123',
                  'tai' : 'tai123',
                  'ttype' : 'brightcove',
                  'page' : 'http://go.com',
                  'ref' : 'http://ref.com',
                  'cts' : '2345623',
                  'tid' : 'acct1_vid_1_tid1',
                  'x' : '56',
                  'y' : '23',
                  'wx' : '78',
                  'wy' : '34',
                  'cx' : '6',
                  'cy' : '8'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

    def test_caseinsentive_tracker_type(self):
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'BRIGHTCOVE',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid2_tid1'}))
        self.assertEqual(response.code, 200)

    def test_invalid_tracker_type(self):
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'il',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'monkeyland',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid2_tid1 56.3 48'}))
        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body, 'Invalid ttype monkeyland')

    def test_invalid_tid_tuples(self):
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'il',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid2_tid1 45'}))
        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body, 'Missing argument tuple')

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'il',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid2_tid1 width 98'}))
        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body,
                                 'height and width must be ints')

    def test_missing_both_aplay_and_adelta(self):
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'ap',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bn' : 'somevideo.jpg',
             'vid' : 'somevideo',
             'pcount' : '2'}))
        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body, 'either adelta or aplay')

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'vp',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'bn' : 'somevideo.jpg',
             'vid' : 'somevideo',
             'pcount' : '2'}))
        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body, 'either adelta or aplay')

    @patch('clickTracker.trackserver.socket.create_connection')    
    def test_heartbeat_good_flume_connection(self, sockmock):
        response = self.fetch('/healthcheck')
        self.assertEqual(response.code, 200)

    @patch('clickTracker.trackserver.socket.create_connection')    
    def test_heartbeat_bad_flume_connection(self, sockmock):
        def mock_socket(dest, time):
            if dest[1] == options.get('clickTracker.trackserver.flume_port'):
                raise socket.error()
            return MagicMock()
        sockmock.side_effect = mock_socket
        with self.assertLogExists(logging.ERROR, 'Could not open flume port'):
            response = self.fetch('/healthcheck')
        self.assertEqual(response.code, 500)

    @patch('clickTracker.trackserver.socket.create_connection')    
    def test_heartbeat_bad_isp_connection(self, sockmock):
        def mock_socket(dest, time):
            if dest[1] == options.get('clickTracker.trackserver.isp_port'):
                raise socket.error()
            return MagicMock()
        sockmock.side_effect = mock_socket
        with self.assertLogExists(logging.ERROR, 'Could not open isp port'):
            response = self.fetch('/healthcheck')
        self.assertEqual(response.code, 500)

    def test_test_endpoint(self):
        response = self.fetch('/v2/test?%s' % urllib.urlencode(
            { 'a' : 'iv',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tids' : 'tid1,tid2'}))
        self.assertEqual(response.code, 200)
        self.assertEqual(self.thrift_mock.appendBatch.call_count, 0)

    def test_optional_tid(self):
        # Video Play
        self.check_message_sent(
            { 'a' : 'vp',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'vid' : 'vid1',
              'adplay': 'False',
              'adelta': 'null',
              'pcount': '1',
              'playerid' : 'brightcoveP123'},
            { 'eventType' : 'VIDEO_PLAY',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'thumbnailId' : None,
                  'videoId' : 'vid1',
                  'didAdPlay': False,
                  'autoplayDelta': None,
                  'playCount': 1,
                  'playerId' : 'brightcoveP123',
                  'isVideoPlay' : True,
                  'isAutoPlay' : None
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

        # Ad play message
        self.check_message_sent(
            { 'a' : 'ap',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'vid' : 'vid1',
              'adelta': '214',
              'pcount' : '1',
              'playerid' : 'brightcoveP123',
              },
            { 'eventType' : 'AD_PLAY',
              'pageId' : 'pageid123',
              'trackerAccountId' : 'tai123',
              'trackerType' : 'BRIGHTCOVE',
              'pageURL' : 'http://go.com',
              'refURL' : 'http://ref.com',
              'clientTime' : 2345623,
              'eventData' : {
                  'thumbnailId' : None,
                  'videoId' : 'vid1',
                  'autoplayDelta': 214,
                  'playCount': 1,
                  'playerId' : 'brightcoveP123',
                  'isAdPlay' : True,
                  'isAutoPlay' : None
                  },
              'neonUserId' : 'neon_id1'},
              'neon_id1'
            )

    def test_flume_error_code(self):
        # Simulate an error code from the flume server
        self.thrift_mock.appendBatch.side_effect = \
          lambda events, callback: self.io_loop.add_callback(
              callback,
              Status.ERROR)

        self.assertEqual(self.backup_q.qsize(), 0)

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid1_tid1,acct1_vid2_tid2'}))

        # If flume is down, we still want to respond with a 200 if it
        # is stored on disk
        self.assertEqual(response.code, 200)

        # Now check the quere for writing to disk to make sure that
        # the data is there.
        self.assertEqual(self.backup_q.qsize(), 1)

    def test_flume_connection_error(self):
        self.thrift_mock.appendBatch.side_effect = [
            tornado.iostream.StreamClosedError()]
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid1_tid1,acct1_vid2_tid2'}))

        # If flume is down, we still want to respond with a 200 if it
        # is stored on disk
        self.assertEqual(response.code, 200)

        # Now check the quere for writing to disk to make sure that
        # the data is there.
        self.assertEqual(self.backup_q.qsize(), 1)

    @patch('clickTracker.trackserver.TTornado.TTornadoStreamTransport')
    def test_error_opening_flume_connection(self, transport_mock):
        transport_mock().open.side_effect = [TTransport.TTransportException()]
        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'acct1_vid1_tid1,acct1_vid2_tid2'}))

        # If flume is down, we still want to respond with a 200 if it
        # is stored on disk
        self.assertEqual(response.code, 200)

        # Now check the quere for writing to disk to make sure that
        # the data is there.
        self.assertEqual(self.backup_q.qsize(), 1)
        

    def test_bad_percent_viewed_format(self):
        response = self.fetch('/v2?%s' % urllib.urlencode(
            { 'a' : 'vvp',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'vid' : 'vid1',
              'pcount' : '1',
              'prcnt' : 'fifty'
              }))

        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body,
                                 'Missing argument prcnt')

    def test_missing_click_arguments(self):
        response = self.fetch('/v2?%s' % urllib.urlencode(
            { 'a' : 'ic',
              'pageid' : 'pageid123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tid' : 'acct1_vid1_tid1',
              'x' : '56',
              'y' : '23',
              'wx' : '78',
              'wy' : '34',
              'cx' : '6',
              'cy' : '8'
              }))

        self.assertEqual(response.code, 400)
        self.assertRegexpMatches(response.body,
                                 'Missing argument tai')

    def test_utf8_header(self):
        # TODO(mdesnoyer): Make this test cleaner
        response = self.fetch(
            '/v2/track?a=vc&page=http%3A%2F%2Fwww.stack.com%2F2009%2F05%2F01%2Fnutrition-plan-for-football%2F&pageid=MweOiNTpEmyffwqR&ttype=brightcove&ref=http%3A%2F%2Fwww.google.ca%2Furl&tai=1510551506&cts=1401814012336&vid=16863777001&tid=null&playerid=3147600983001&noCacheIE=1401814012337',
            headers = {'Geoip_country_code3': 'CAN', 'Accept-Language': 'en-us', 'Accept-Encoding': 'gzip, deflate', 'Geoip_city': 'Qu\xc3\xa9bec', 'X-Forwarded-Port': '80', 'Dnt': '1', 'Connection': 'close', 'X-Real-Ip': '65.94.184.97', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14', 'Geoip_postal_code': 'G6K', 'Geoip_latitude': '46.7038', 'Geoip_longitude': '-71.2837', 'X-Forwarded-For': '65.94.184.97, 65.94.184.97', 'Accept': '*/*', 'Geoip_region': 'QC', 'Host': 'trackserver-test-691751517.us-east-1.elb.amazonaws.com', 'X-Forwarded-Proto': 'http', 'Referer': 'http://www.stack.com/2009/05/01/nutrition-plan-for-football/'})

        self.assertEqual(response.code, 200)



    # TODO(mdesnoyer) add tests for when the schema isn't on S3 and
    # for when arguments are missing
            

if __name__ == '__main__':
    utils.neon.InitNeon()
    # Turn off the annoying logs
    #logging.getLogger('tornado.access').propagate = False

    unittest.main()
