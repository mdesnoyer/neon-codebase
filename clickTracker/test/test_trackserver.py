#!/usr/bin/env python
'''
Test functionality of the click log server.

#TODO: Nagios like script to monitor the following issues

- Server not at capacity, 1 or more process died
- S3 uploader has issues with s3 connection
- Memory on the box is running out, Q drain issue 

'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import __builtin__
import boto
import boto.exception
from boto.s3.connection import S3Connection
import test_utils.mock_boto_s3 as boto_mock
import clickTracker.trackserver 
import fake_filesystem
import json
import logging
import mock
from mock import patch
from mock import MagicMock
import multiprocessing
import os
import Queue
import random
import socket
import threading
import tornado.ioloop
import time
import unittest
import urllib2
import utils.neon
from utils.options import options

class TestS3Handler(unittest.TestCase):
    def setUp(self):
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.fake_os = fake_filesystem.FakeOsModule(self.filesystem)
        self.fake_open = fake_filesystem.FakeFileOpen(self.filesystem)
        clickTracker.trackserver.os = self.fake_os
        clickTracker.trackserver.open = self.fake_open
        boto_mock.open = self.fake_open

    def tearDown(self):
        clickTracker.trackserver.os = os
        clickTracker.trackserver.open = __builtin__.open
        boto_mock.open = __builtin__.open

    def processData(self):
        '''Sends 1000 requests to a handler and waits until they are done.'''
        
        # Start a thread to handle the data
        dataQ = Queue.Queue()
        handle_thread = clickTracker.trackserver.S3Handler(dataQ)
        handle_thread.start()

        # Send data
        nlines = 1000
        for i in range(nlines/2):
            cd = clickTracker.trackserver.TrackerData(
                "load", 1, "flashonlytracker", time.time(),
                time.time(), "http://localhost",
                "127.0.0.1", ['i1.jpg','i2.jpg'],'v1')
            dataQ.put(cd.to_json())

            click = clickTracker.trackserver.TrackerData(
                "click", 1, "flashonlytracker", time.time(),
                time.time(), "http://localhost",
                "127.0.0.1", 'i1.jpg')
            dataQ.put(click.to_json())
            
        # Wait until the data is processeed 
        dataQ.join()

    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_s3(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn
        
        # Create the S3 bucket for the logs
        bucket = conn.create_bucket('neon-tracker-logs')

        self.processData()
        
        # Check that there are 10 files in the bucket because each
        # file by default is 100 lines
        s3_keys = [x for x in bucket.get_all_keys()]
        self.assertEqual(len(s3_keys), 10)

        # Make sure that the files have the same number of lines
        file_lines = None
        for key in s3_keys:
            nlines = 0
            for line in key.get_contents_as_string().split('\n'):
                nlines += 1
            if file_lines is None:
                file_lines = nlines
            else:
                self.assertEqual(file_lines, nlines)

        # Open one of the files and check that it contains what we expect
        for line in s3_keys[0].get_contents_as_string().split('\n'):
            line = line.strip()
            if line == '':
                continue
            parsed = json.loads(line)
            self.assertEqual(parsed['id'], 1)
            self.assertEqual(parsed['ttype'], 'flashonlytracker')
            self.assertEqual(parsed['page'], 'http://localhost')
            self.assertEqual(parsed['cip'], '127.0.0.1')
            self.assertTrue(parsed['ts'])
            self.assertTrue(parsed['sts'])

            if parsed['a'] == 'load':
                self.assertEqual(parsed['cvid'], 'v1')
                self.assertEqual(parsed['imgs'], ['i1.jpg','i2.jpg'])
            elif parsed['a'] == 'click':
                self.assertEqual(parsed['img'], 'i1.jpg')
            else:
                self.fail('Bad action field %s' % parsed['a'])

    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_s3_with_timeout(self, mock_conntype):
        '''Make sure that a package is sent to S3 after a timeout.'''
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn

        # Create the S3 bucket for the logs
        bucket = conn.create_bucket('neon-tracker-logs')

        with options._set_bounded('clickTracker.trackserver.flush_interval',
                                  0.5):
            
            # Start a thread to handle the data
            dataQ = Queue.Queue()
            handle_thread = clickTracker.trackserver.S3Handler(dataQ)
            handle_thread.start()

            # Send the data
            nlines = 56
            for i in range(nlines):
                cd = clickTracker.trackserver.TrackerData(
                    "load", 1, "flashonlytracker", time.time(),
                    time.time(), "http://localhost",
                    "127.0.0.1", ['i1.jpg','i2.jpg'],'v1')
                dataQ.put(cd.to_json())
            
            # Wait for the timeout
            time.sleep(0.6)

        # Check that the file is in the bucket
        s3_keys = [x for x in bucket.get_all_keys()]
        self.assertEqual(len(s3_keys), 1)

        # Make sure that all the data is in the file
        nlines = 0
        for line in s3_keys[0].get_contents_as_string().split('\n'):
            nlines += 1
        self.assertEqual(nlines, 56)

    def test_log_to_disk_normally(self):
        '''Check the mechanism to log to disk.'''
        log_dir = '/tmp/fake_log_dir'
        with options._set_bounded('clickTracker.trackserver.output',
                                  log_dir):
            self.processData()

            files = self.fake_os.listdir(log_dir)
            self.assertEqual(len(files), 10)

            # Make sure the files have the same number of lines
            file_lines = None
            for fname in files:
                with self.fake_open(os.path.join(log_dir, fname)) as f:
                    nlines = 0
                    for line in f:
                        nlines += 1
                    if file_lines is None:
                        file_lines = nlines
                    else:
                        self.assertEqual(file_lines, nlines)

            # Open one of the files and check that it contains what we expect
            with self.fake_open(os.path.join(log_dir, files[0])) as f:
                for line in f:
                    line = line.strip()
                    if line == '':
                        continue
                    parsed = json.loads(line)
                    self.assertEqual(parsed['id'], 1)
                    self.assertEqual(parsed['ttype'], 'flashonlytracker')
                    self.assertEqual(parsed['page'], 'http://localhost')
                    self.assertEqual(parsed['cip'], '127.0.0.1')
                    self.assertTrue(parsed['ts'])
                    self.assertTrue(parsed['sts'])

                    if parsed['a'] == 'load':
                        self.assertEqual(parsed['cvid'], 'v1')
                        self.assertEqual(parsed['imgs'], ['i1.jpg','i2.jpg'])
                    elif parsed['a'] == 'click':
                        self.assertEqual(parsed['img'], 'i1.jpg')
                    else:
                        self.fail('Bad action field %s' % parsed['a'])
        
    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_disk_on_connection_error(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn

        # Mock out the exception to throw
        mock_bucket = MagicMock()
        conn.buckets['neon-tracker-logs'] = mock_bucket
        mock_bucket.new_key.side_effect = socket.gaierror()

        self.processData()

        # Make sure there are 10 files on the disk
        log_dir = '/mnt/neon/s3diskbacklog'
        files = self.fake_os.listdir(log_dir)
        self.assertEqual(len(files), 10)

        # Make sure the files have the same number of lines
        file_lines = None
        for fname in files:
            with self.fake_open(os.path.join(log_dir, fname)) as f:
                nlines = 0
                for line in f:
                    nlines += 1
                if file_lines is None:
                    file_lines = nlines
                else:
                    self.assertEqual(file_lines, nlines)

        # Open one of the files and check that it contains what we expect
        with self.fake_open(os.path.join(log_dir, files[0])) as f:
            for line in f:
                line = line.strip()
                if line == '':
                    continue
                parsed = json.loads(line)
                self.assertEqual(parsed['id'], 1)
                self.assertEqual(parsed['ttype'], 'flashonlytracker')
                self.assertEqual(parsed['page'], 'http://localhost')
                self.assertEqual(parsed['cip'], '127.0.0.1')
                self.assertTrue(parsed['ts'])
                self.assertTrue(parsed['sts'])

                if parsed['a'] == 'load':
                    self.assertEqual(parsed['cvid'], 'v1')
                    self.assertEqual(parsed['imgs'], ['i1.jpg','i2.jpg'])
                elif parsed['a'] == 'click':
                    self.assertEqual(parsed['img'], 'i1.jpg')
                else:
                    self.fail('Bad action field %s' % parsed['a'])

    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_disk_on_key_creation_error(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn

        # Mock out the exception to throw
        mock_bucket = MagicMock()
        conn.buckets['neon-tracker-logs'] = mock_bucket
        mock_bucket.new_key.side_effect = boto.exception.S3CreateError(
            500, 'Error')

        self.processData()

        # Make sure there are 10 files on the disk
        files = self.fake_os.listdir('/mnt/neon/s3diskbacklog')
        self.assertEqual(len(files), 10)

    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_disk_on_s3_response_error(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn

        # Mock out the exception to throw
        mock_bucket = MagicMock()
        conn.buckets['neon-tracker-logs'] = mock_bucket
        mock_bucket.new_key.side_effect = boto.exception.S3ResponseError(
            500, 'An error')

        self.processData()

        # Make sure there are 10 files on the disk
        files = self.fake_os.listdir('/mnt/neon/s3diskbacklog')
        self.assertEqual(len(files), 10)

    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_disk_on_upload_connection_error(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn

        # Mock out the exception to throw
        mock_bucket = MagicMock()
        mock_key = MagicMock()
        conn.buckets['neon-tracker-logs'] = mock_bucket
        mock_bucket.new_key.return_value = mock_key
        mock_key.set_contents_from_string.side_effect = socket.error()

        self.processData()

        # Make sure there are 10 files on the disk
        files = self.fake_os.listdir('/mnt/neon/s3diskbacklog')
        self.assertEqual(len(files), 10)

    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_disk_on_upload_response_error(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn

        # Mock out the exception to throw
        mock_bucket = MagicMock()
        mock_key = MagicMock()
        conn.buckets['neon-tracker-logs'] = mock_bucket
        mock_bucket.new_key.return_value = mock_key
        mock_key.set_contents_from_string.side_effect = \
          boto.exception.S3ResponseError(500, 'An error')

        self.processData()

        # Make sure there are 10 files on the disk
        files = self.fake_os.listdir('/mnt/neon/s3diskbacklog')
        self.assertEqual(len(files), 10)

    @patch('clickTracker.trackserver.S3Connection')
    def test_upload_to_s3_once_connection_back(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn

        # Create the bucket
        bucket = conn.create_bucket('neon-tracker-logs')

        # Mock out the exception to throw for the first chunk of data
        mock_bucket = MagicMock()
        conn.buckets['neon-tracker-logs'] = mock_bucket
        mock_bucket.new_key.side_effect = socket.gaierror()

        self.processData()

        # Now, mock out a valid connection
        conn.buckets['neon-tracker-logs'] = bucket

        self.processData()

        # Make sure there are 20 files on S3
        s3_keys = [x for x in bucket.get_all_keys()]
        self.assertEqual(len(s3_keys), 20)

        # Make sure that there are no files on disk
        files = self.fake_os.listdir('/mnt/neon/s3diskbacklog')
        self.assertEqual(len(files), 0)
        

class TestFullServer(unittest.TestCase):
    '''A set of tests that fire up the whole server and throws http requests at it.'''

    def setUp(self):
        self.port = random.randint(9000,10000)

        self.load_url = 'http://localhost:'+str(self.port)+'/track?a=load&id=288edb2d31c34507&imgs=%5B%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F34%2F2294876105001_2727914703001_thumbnail-2296855887001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F354%2F2294876105001_2727881607001_thumbnail-2369368872001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F2294876105001_2660525568001_thumbnail-2296855886001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fe1%2Fpd%2F2294876105001%2F2294876105001_2617231423001_thumbnail-2323153341001.jpg%22%5D&cvid=2296855887001&ts=1381257030328&page=http%3A%2F%2Flocalhost%2Fbcove%2Ffplayerabtest.html&ttype=flashonlyplayer&noCacheIE=1381257030328'

        self.click_url = 'http://localhost:'+str(self.port)+'/track?a=click&id=14b150ad6a59e93c&img=http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F34%2F2294876105001_2727914703001_thumbnail-2296855887001.jpg&ts=1381264478544&page=http%3A%2F%2Flocalhost%2Fbcove%2Ffplayerabtest.html&ttype=flashonlyplayer'

        random.seed(168984)

        self.filesystem = fake_filesystem.FakeFilesystem()
        self.fake_os = fake_filesystem.FakeOsModule(self.filesystem)
        self.fake_open = fake_filesystem.FakeFileOpen(self.filesystem)
        clickTracker.trackserver.os = self.fake_os
        clickTracker.trackserver.open = self.fake_open
        boto_mock.open = self.fake_open

        self.patcher = patch('clickTracker.trackserver.S3Connection')
        mock_conn = self.patcher.start()
        self.s3conn = boto_mock.MockConnection()
        mock_conn.return_value = self.s3conn

        # Start the server in its own thread
        with options._set_bounded('clickTracker.trackserver.port', self.port):
            self.server = clickTracker.trackserver.Server()
            self.server.daemon = True
            self.server.start()
            self.server.wait_until_running()

    def tearDown(self):
        self.server.stop()
        self.patcher.stop()

        clickTracker.trackserver.os = os
        clickTracker.trackserver.open = __builtin__.open
        boto_mock.open = __builtin__.open

    def test_send_data_to_server(self):        
        # Fire requests to the server      
        nlines = 1000
        for i in range(nlines):
            rd = random.randint(1,100)
            if rd <2:
                response = urllib2.urlopen(self.click_url) 
            else:
                response = urllib2.urlopen(self.load_url)
            self.assertEqual(response.getcode(), 200)

        self.server.wait_for_processing()

        # Now check that all the data got sent to S3
        bucket = self.s3conn.get_bucket('neon-tracker-logs')
        s3_keys = [x for x in bucket.get_all_keys()]
        self.assertEqual(len(s3_keys), 10)
            

if __name__ == '__main__':
    utils.neon.InitNeonTest()
    # Turn off the annoying logs
    logging.getLogger('tornado.access').propagate = False
    unittest.main()
