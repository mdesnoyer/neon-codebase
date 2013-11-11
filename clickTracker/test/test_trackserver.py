#!/usr/bin/env python
'''
Test functionality of the click log server.

- Spin up the server ( n-1 click loggers, 1 s3 uploader)
- Generate 'X' requests 
- Drain 'X' requests 
- If successful drain and upload to s3, exit with success message
    
: False injections, disconnect s3 

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


import boto
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
import test_utils.mock_boto_s3 as boto_mock
import clickTracker.trackserver as cs 
import json
import mock
from mock import patch
import Queue
import random
import time
import unittest
import urllib2
import utils.neon

class TestLogger(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('clickTracker.trackserver.S3Connection')
    def test_log_to_s3(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn
        
        # Create the S3 bucket for the logs
        bucket = conn.create_bucket('neon-tracker-logs')

        # Start a thread to handle the data
        dataQ = Queue.Queue()
        handle_thread = cs.S3Handler(dataQ)
        handle_thread.start()

        # Send data
        nlines = 1000
        for i in range(nlines/2):
            cd = cs.TrackerData("load", 1, "flashonlytracker", time.time(),
                                time.time(), "http://localhost",
                                "127.0.0.1", ['i1.jpg','i2.jpg'],'v1')
            dataQ.put(cd.to_json())

            click = cs.TrackerData("click", 1, "flashonlytracker", time.time(),
                                time.time(), "http://localhost",
                                "127.0.0.1", 'i1.jpg')
            dataQ.put(click.to_json())
            
        # Wait until the data is processeed 
        dataQ.join()
        
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
        
        
    def _test_log_to_disk(self):
        nlines = 100
        #conn = S3Connection('test','test')
        #bucket = conn.create_bucket('neon-tracker-logs')
        S3_ACCESS_KEY = 'AKIAJ5G2RZ6BDNBZ2VBA' 
        S3_SECRET_KEY = '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'
        s3conn = S3Connection(aws_access_key_id=S3_ACCESS_KEY,
                                   aws_secret_access_key =S3_SECRET_KEY)
        bucket = Bucket(connection=s3conn,name="fake-bucket")
        dataQ = Queue.Queue()
        #add data
        for i in range(nlines):
            cd = cs.TrackerData("load",1,"flashonlytracker",time.time(),time.time(),
                "http://localhost","127.0.0.1",['i1.jpg','i2.jpg'],'v1')
            data = cd.to_json()
            dataQ.put(data)
        
        batch_size = 100
        fetch_count = 100
        handler = cs.S3Handler(dataQ,batch_size,fetch_count,bucket)
        for i in range(nlines/batch_size):
            self.assertEqual(handler.do_work(),"disk")

            # TODO(Sunil): Check that the data on disk is what we
            # expect it to be.
            
    # TODO(Sunil): Test that data which gets put on disk, gets
    # uploaded to S3 once the connection is back.

    def _test_send_data_to_server(self):
        port = 9080
        nlines = 10
        l_url = 'http://localhost:'+str(port)+'/track?a=load&id=288edb2d31c34507&imgs=%5B%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F34%2F2294876105001_2727914703001_thumbnail-2296855887001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F354%2F2294876105001_2727881607001_thumbnail-2369368872001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F2294876105001_2660525568001_thumbnail-2296855886001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fe1%2Fpd%2F2294876105001%2F2294876105001_2617231423001_thumbnail-2323153341001.jpg%22%5D&cvid=2296855887001&ts=1381257030328&page=http%3A%2F%2Flocalhost%2Fbcove%2Ffplayerabtest.html&ttype=flashonlyplayer&noCacheIE=1381257030328'

        c_url = 'http://localhost:'+str(port)+'/track?a=click&id=14b150ad6a59e93c&img=http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F34%2F2294876105001_2727914703001_thumbnail-2296855887001.jpg&ts=1381264478544&page=http%3A%2F%2Flocalhost%2Fbcove%2Ffplayerabtest.html&ttype=flashonlyplayer'

        p = subprocess.Popen("python trackserver.py --port=" + str(port), shell=True, stdout=subprocess.PIPE)
        import pdb; pdb.set_trace()
        #put data ( 99% click, 1% load)
        for i in range(nlines):
            rd = random.randint(1,100)
            if rd <2:
                r = urllib2.urlopen(c_url) 
            else:
                r = urllib2.urlopen(l_url) 
            r.read()

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
