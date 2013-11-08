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
import subprocess
import sys
import urllib2
import random
import boto
from boto.s3.connection import S3Connection
from moto import mock_s3
import unittest
import json
import time
import Queue
import clickTracker.trackserver as cs 

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

class TestLogger(unittest.TestCase):

    @mock_s3
    def test_log_to_s3(self):
        nlines = 1000
        conn = S3Connection('test','test')
        bucket = conn.create_bucket('neon-tracker-logs')
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
            #self.assertIn(handler.do_work(),["s3","disk"])
            self.assertEqual(handler.do_work(),"s3")

            # TODO(Sunil): Check that the data on s3 is what we expect it to be

    def test_log_to_disk(self):
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
    unittest.main()
