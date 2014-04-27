#!/usr/bin/env python
'''
Test functionality of the click log server.

#TODO(sunil): Nagios like script to monitor the following issues

- Server not at capacity, 1 or more process died
- S3 uploader has issues with s3 connection
- Memory on the box is running out, Q drain issue 

'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import __builtin__
import clickTracker.trackserver 
import fake_filesystem
import json
import logging
from mock import patch
from mock import MagicMock
import os
import Queue
import random
import time
import tornado.testing
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import urllib
import unittest
import utils.neon
from utils.options import options

class TestFileBackupHandler(unittest.TestCase):
    def setUp(self):
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

        mock_click_request = MagicMock()
        click_fields = {'pageid' : 'pageid234',
                        'tai' : 'tai234',
                        'ttype' : 'brightcove',
                        'page' : 'http://go.com',
                        'ref' : 'http://ref.com',
                        'cts' : '23945827',
                        'a' : 'ic',
                        'tid' : 'tid345',
                        'x' : '3467',
                        'y' : '123',
                        'wx' : '567',
                        'wy' : '9678'}
        mock_click_request.get_argument.side_effect = lambda x: click_fields[x]
        mock_click_request.request.remote_ip = '12.43.151.12'
        mock_click_request.get_cookie.return_value = 'cookie1'

        mock_view_request = MagicMock()
        view_fields = {'pageid' : 'pageid67',
                       'tai' : 'tai20',
                       'ttype' : 'brightcove',
                       'page' : 'http://go1.com',
                       'ref' : 'http://ref1.com',
                       'cts' : '23945898',
                       'a' : 'iv',
                       'tids' : 'tid345,34,89+tid346,67,98'
                       }
        mock_view_request.get_argument.side_effect = lambda x: view_fields[x]
        mock_view_request.request.remote_ip = '12.43.151.120'
        mock_view_request.get_cookie.return_value = 'cookie2'
        
        # Start a thread to handle the data
        dataQ = Queue.Queue()
        handle_thread = clickTracker.trackserver.FileBackupHandler(dataQ)
        handle_thread.start()

        # Send data
        nlines = 400
        for i in range(nlines/2):
            click = clickTracker.trackserver.BaseTrackerDataV2.generate(
                mock_click_request)
            
            dataQ.put(click.to_flume_event())

            view = clickTracker.trackserver.BaseTrackerDataV2.generate(
                mock_view_request)
            dataQ.put(view.to_flume_event())
            
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

                # Make sure the number of lines in the files is what we expect
                nlines = 0
                for fname in files:
                    with self.fake_open(os.path.join(log_dir, fname)) as f:
                        for line in f:
                            nlines += 1
                self.assertEqual(400, nlines)

                # Open one of the files and check that it contains
                # what we expect
                with self.fake_open(os.path.join(log_dir, files[0])) as f:
                    for line in f:
                        line = line.strip()
                        if line == '':
                            continue
                        parsed = json.loads(line)
                        body = parsed[0]['body']
                        if body['event'] == 'ic':
                            self.assertEqual(body['pageid'], 'pageid234')
                            self.assertEqual(body['tai'], 'tai234')
                            self.assertEqual(body['ttype'], 'brightcove')
                            self.assertEqual(body['page'], 'http://go.com')
                            self.assertEqual(body['ref'], 'http://ref.com')
                            self.assertEqual(body['cip'], '12.43.151.12')
                            self.assertEqual(body['cts'], 23945827)
                            self.assertTrue(body['sts'])
                            self.assertEqual(body['tid'], 'tid345')
                            self.assertEqual(body['px'], 3467)
                            self.assertEqual(body['py'], 123)
                            self.assertEqual(body['wx'], 567)
                            self.assertEqual(body['wy'], 9678)
                        elif body['event'] == 'iv':
                            self.assertEqual(body['pageid'], 'pageid67')
                            self.assertEqual(body['tai'], 'tai20')
                            self.assertEqual(body['ttype'], 'brightcove')
                            self.assertEqual(body['page'], 'http://go1.com')
                            self.assertEqual(body['ref'], 'http://ref1.com')
                            self.assertEqual(body['cip'], '12.43.151.120')
                            self.assertEqual(body['cts'], 23945898)
                            self.assertTrue(body['sts'])
                            self.assertItemsEqual(body['tids'], 
                                                  [['tid345', 34, 89],
                                                   ['tid346', 67, 98]])
                        else:
                            self.fail('Bad event field %s' % body['event'])
        

class TestFullServer(tornado.testing.AsyncHTTPTestCase):
    '''A set of tests that fire up the whole server and throws http requests at it.'''

    def setUp(self):
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.fake_os = fake_filesystem.FakeOsModule(self.filesystem)
        self.fake_open = fake_filesystem.FakeFileOpen(self.filesystem)
        clickTracker.trackserver.os = self.fake_os
        clickTracker.trackserver.open = self.fake_open
        
        self.server_obj = clickTracker.trackserver.Server()
        super(TestFullServer, self).setUp()

        random.seed(168984)

        self.http_patcher = patch(
            'clickTracker.trackserver.utils.http.send_request')
        self.http_mock = self.http_patcher.start()
        self.http_mock.side_effect = \
          lambda x, callback: self.io_loop.add_callback(callback,
                                                        HTTPResponse(x, 200))
        self.backup_q = self.server_obj.backup_queue

    def tearDown(self):
        self.http_patcher.stop()
        
        super(TestFullServer, self).tearDown()
        clickTracker.trackserver.os = os
        clickTracker.trackserver.open = __builtin__.open

    def get_app(self):
        return self.server_obj.application

    def check_message_sent(self, url_params, ebody, neon_id=None,
                           path='/v2'):
        '''Sends a message and checks the body to be what's expected.

        Inputs:
        url_params - Dictionary of url params
        ebody - json dictionary of expected body in the logged message
        neon_id - Neon id to store in a cookie
        path - Endpoint for the server
        '''
        self.http_mock.reset_mock()
        
        headers = {}
        if neon_id is not None:
            headers['Cookie'] = 'neonglobaluserid=%s' % neon_id
        response = self.fetch(
            '%s?%s' % (path, urllib.urlencode(url_params)),
            headers=headers)

        self.assertEqual(response.code, 200)

        # Check that a response was found
        self.assertEqual(self.http_mock.call_count, 1)
        request_saw = self.http_mock.call_args[0][0]
        self.assertEqual(request_saw.headers,
                         {'Content-Type': 'application/json'})
        self.assertEqual(request_saw.method, 'POST')

        json_msg = json.loads(request_saw.body)
        self.assertEqual(len(json_msg), 1)
        json_msg = json_msg[0]
        self.assertEqual(json_msg['headers']['tai'], ebody['tai'])
        self.assertTrue(json_msg['headers']['timestamp'])
        self.assertEqual(json_msg['headers']['timestamp'],
                         json_msg['body']['sts'])
        if 'v2' in path:
            self.assertEqual(json_msg['headers']['track_vers'], '2')
        else:
            self.assertEqual(json_msg['headers']['track_vers'], '1')

        self.assertDictContainsSubset(ebody, json_msg['body'])
        self.assertEqual(json_msg['body']['cip'], '127.0.0.1')
        if neon_id is not None:
            self.assertEqual(json_msg['body']['uid'], neon_id)

    def test_v2_valid_messages(self):
        # Image Visible Message
        self.check_message_sent(
            { 'a' : 'iv',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tids' : 'tid1,56,67+tid2,89,123'},
            { 'event' : 'iv',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : 2345623,
              'tids' : [['tid1',56,67],['tid2',89,123]],
              'uid' : 'neon_id1'},
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
              'tids' : 'tid1,56,67+tid2,89,123'},
            { 'event' : 'il',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : 2345623,
              'tids' : [['tid1',56,67],['tid2',89,123]],
              'uid' : 'neon_id1'},
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
              'tid' : 'tid1',
              'x' : '56',
              'y' : '23',
              'wx' : '78',
              'wy' : '34'},
            { 'event' : 'ic',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : 2345623,
              'tid' : 'tid1',
              'px' : 56,
              'py' : 23,
              'wx' : 78,
              'wy' : 34,
              'uid' : 'neon_id1'},
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
              'tid' : 'tid1',
              'vid' : 'vid1',
              'pltype' : 'brightcove'},
            { 'event' : 'vp',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : 2345623,
              'tid' : 'tid1',
              'vid' : 'vid1',
              'pltype' : 'brightcove',
              'uid' : 'neon_id1'},
              'neon_id1'
            )

        # Ad play message
        # TODO(Sunil): Change this message when the ad plays are better defined
        self.check_message_sent(
            { 'a' : 'ap',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : '2345623',
              'tid' : 'tid1',},
            { 'event' : 'ap',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : 2345623,
              'tid' : 'tid1',
              'uid' : 'neon_id1'},
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
              'tids' : 'tid1,56,67+tid2,89,123'},
            { 'event' : 'iv',
              'pageid' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'brightcove',
              'page' : 'http://go.com',
              'ref' : 'http://ref.com',
              'cts' : 2345623,
              'tids' : [['tid1',56,67],['tid2',89,123]],
              'uid' : 'neon_id1'},
              'neon_id1',
              '/v2/track'
            )

    def test_v1_valid_messages(self):
        # A load message
        self.check_message_sent(
            { 'a' : 'load',
              'id' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'html5',
              'page' : 'http://go.com',
              'ts' : '2345623',
              'cvid' : 'vid1',
              'imgs' : '["http://img1.jpg", "img2.jpg"]'},
            { 'a' : 'load',
              'id' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'html5',
              'page' : 'http://go.com',
              'ts' : '2345623',
              'cvid' : 'vid1',
              'imgs' : ["http://img1.jpg", "img2.jpg"],
              'cip': '127.0.0.1'},
              path='/track'
            )

        # An image click message
        self.check_message_sent(
            { 'a' : 'click',
              'id' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'html5',
              'page' : 'http://go.com',
              'ts' : '2345623',
              'img' : 'http://img1.jpg',
              'xy' : '23,45'},
            { 'a' : 'click',
              'id' : 'pageid123',
              'tai' : 'tai123',
              'ttype' : 'html5',
              'page' : 'http://go.com',
              'ts' : '2345623',
              'img' : "http://img1.jpg",
              'xy' : '23,45',
              'cip': '127.0.0.1'},
              path='/track'
            )

    def test_error_connecting_to_flume(self):
        # Simulate a connection error
        self.http_mock.side_effect = \
          lambda x, callback: self.io_loop.add_callback(
              callback,
              HTTPResponse(x, 404, error=HTTPError(404)))

        self.assertEqual(self.backup_q.qsize(), 0)

        response = self.fetch('/v2?%s' % urllib.urlencode(
            {'a' : 'iv',
             'pageid' : 'pageid123',
             'tai' : 'tai123',
             'ttype' : 'brightcove',
             'page' : 'http://go.com',
             'ref' : 'http://ref.com',
             'cts' : '2345623',
             'tids' : 'tid1,56,67+tid2,89,123'}))

        self.assertEqual(response.code, 404)

        # Now check the quere for writing to disk to make sure that
        # the data is there.
        self.assertEqual(self.backup_q.qsize(), 1)
        msg = json.loads(self.backup_q.get())
        self.assertEqual(len(msg), 1)
        self.assertEqual(msg[0]['body']['event'], 'iv')
            

if __name__ == '__main__':
    utils.neon.InitNeonTest()
    # Turn off the annoying logs
    logging.getLogger('tornado.access').propagate = False
    unittest.main()
