#!/usr/bin/env python

'''
API/ Integration tests

Neon Image serving platform API tests
Test the responses for all the defined APIs for ISP

#NOTE: TEMP test file to duplicate the test without gzip mastermind file
Just run one test to check that you can parse the file

'''

import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..',
    '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)
   
import atexit
from boto.s3.connection import S3Connection
from boto.s3.bucketlistresultset import BucketListResultSet
import json
import gzip
import logging
import httplib
import nginx_isp_test_conf
import os
import random
import re
import signal
import shutil
import subprocess
import s3cmd_fakes3cfg
from StringIO import StringIO
import unittest
import urllib
import urllib2
import utils
import utils.neon
import utils.ps
import time
from utils import gzipstream
import tempfile
from test_utils import net
import test_utils.neontest

_log = logging.getLogger(__name__)

def LaunchFakeS3(s3host, s3port, s3disk, fakes3root):
    '''Launch a fakes3 instance if the settings call for it.'''
    if s3host == 'localhost':
        _log.info('Launching fakes3')
        proc = subprocess.Popen([
            '/usr/bin/env', 'fakes3',
            '--root', fakes3root,
            '--port', str(s3port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        upRe = re.compile('port=')
        fakes3_log = []
        while proc.poll() is None:
            line = proc.stdout.readline()
            fakes3_log.append(line)
            if upRe.search(line):
                break

        if proc.poll() is not None:
            raise Exception('Error starting fake s3. Log:\n%s' %
                            '\n'.join(fakes3_log))
        _log.info('FakeS3 is up with pid %i' % proc.pid)
        return proc  

def ShutdownFakeS3(proc):
        still_running = utils.ps.send_signal_and_wait(signal.SIGTERM,
                                                      [proc.pid],
                                                      timeout=10)
        if still_running:
            proc.kill()

        proc.wait()

def FakeS3Upload(fpath, s3cfg, s3fname):
    '''
    Upload a file to s3 using s3cmd
    '''
    ret = subprocess.call(["s3cmd", "--config=%s" % s3cfg,
        "--add-header=Content-Type:application/x-gzip", "put", "%s" % fpath,
        "%s" % s3fname]) 
    
    #cmd = "s3cmd --config=%s put %s %s" % (s3cfg, fpath, s3fname)
    #os.system(cmd)

def FakeS3CreateBucket(bucket, s3cfg):
    ret = subprocess.call(["s3cmd", "--config=%s" % s3cfg, "mb", "%s" % bucket])


class ISP:
    '''
    Class to manage the spinning up and shutting down of Nginx server with ISP
    module

    Usage

    self.isp = ISP()
    self.isp.start()
    self.isp.stop()

    For test cases, just spin the server up once by using the start/stop methods
    in setUpClass & tearDownClass respectively
    '''

    def __init__(self, mastermind_s3file_url, s3downloader, s3cfg, port=None):
        self.port = port
        if self.port is None:
            self.port = net.find_free_port()
        
        self.config_file = tempfile.NamedTemporaryFile()
        error_log = tempfile.NamedTemporaryFile()
        mastermind_download_file = tempfile.NamedTemporaryFile()
        mastermind_validated_file = tempfile.NamedTemporaryFile()
        config_contents = nginx_isp_test_conf.conf % \
                                (error_log.name, mastermind_s3file_url, 
                                    s3downloader, s3cfg, 
                                    mastermind_validated_file.name, 
                                    mastermind_download_file.name,
                                    self.port)

        self.config_file.write(config_contents)
        self.config_file.flush()

        #get build path
        self.nginx_path = \
                base_path + "/imageservingplatform/nginx-1.8.1/objs/nginx"

    def start(self):
        self.proc = subprocess.Popen([
            '/usr/bin/env', self.nginx_path, "-c",
            self.config_file.name],
            stdout=subprocess.PIPE)

    def stop(self):
        _log.info("shutting down nginx %s" %self.proc.pid)

        still_running = utils.ps.send_signal_and_wait(signal.SIGTERM,
                                                      [self.proc.pid],
                                                      timeout=10)
        if still_running:
            self.proc.kill()

        self.proc.wait()
    
    def get_port(self):
        return self.port

### URLLIB2 Redirect Handler
class MyHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    '''
    A redirect handler for urllib2 requests 
    
    usage:
    cookieprocessor = urllib2.HTTPCookieProcessor()
    opener = urllib2.build_opener(MyHTTPRedirectHandler, cookieprocessor)
    
    Since adding a callback on this method is tedious, use a class variable
    redirect_response to store the intermediate result

    in a single threaded process, you can use get_last_redirect_response()
    to get the intermediate 302 response
    '''
    
    redirect_response = None
    
    def http_error_302(self, req, fp, code, msg, headers):
        MyHTTPRedirectHandler.redirect_response = headers
        return urllib2.HTTPRedirectHandler.http_error_302(self, 
                                    req, fp, code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302

    @classmethod
    def get_last_redirect_response(cls):
        return cls.redirect_response

class TestImageServingPlatformAPI(test_utils.neontest.TestCase):
    '''
    API testing
    '''

    @classmethod
    def setUpClass(cls):
        
        s3host = 'localhost'
        s3port = net.find_free_port()
        cls.s3disk = tempfile.mkdtemp()
        cls.fakes3root = tempfile.mkdtemp()
        cls.fakes3proc = LaunchFakeS3(s3host, s3port, cls.s3disk, cls.fakes3root)
        
        s3cmd_fakes3cfg.conf % (s3port, '%', s3port)  
        cls.s3cfg = tempfile.NamedTemporaryFile()
        cls.s3cfg.write(s3cmd_fakes3cfg.conf % (s3port, '%', s3port))
        cls.s3cfg.flush()
  
        mfile_path = base_path + \
                        "/imageservingplatform/neon_isp/test/mastermind.api.test"

        bucket_name = "s3://my_bucket"
        FakeS3CreateBucket(bucket_name, cls.s3cfg.name)
        cls.s3_mfile_url = "%s/mastermind.api.test" % bucket_name # mastermind s3 file url
        
        # Gzip the mastermind file that is being uploaded to fake s3
        # Lets keep the mastermind file as ascii the the test repo as its 
        # human readable
        gzip_mfile = tempfile.NamedTemporaryFile()
        with open(mfile_path, 'r') as f:
            gzip_data = f.read()
            gzip_mfile.write(gzip_data)
            gzip_mfile.flush()
        
        FakeS3Upload(gzip_mfile.name, cls.s3cfg.name, cls.s3_mfile_url)

        s3downloader = base_path +\
                    "/imageservingplatform/neon_isp/isp_s3downloader.py"

        cls.isp = ISP(cls.s3_mfile_url, s3downloader, s3port) 
        cls.isp.start()
        
        time.sleep(1) # allow mastermind file to be parsed

    def setUp(self):

        self.port = str(TestImageServingPlatformAPI.isp.get_port())
        self.base_url = "http://localhost:"+ self.port +"/v1/%s/%s/%s/"
        self.get_params = "?width=%s&height=%s"
        
        #default ids
        self.pub_id = "pub1"
        self.vid = "vid1"
        # Also the max fraction URL
        self.expected_img_url =\
                        "http://neon-image-cdn.s3.amazonaws.com/pixel.jpg"
        self.neon_cookie_name = "neonglobaluserid"
        self.cookie_domain = ".neon-images.com"
        self.default_url = "http://default_image_url.jpg"

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.isp.stop()
        shutil.rmtree(cls.s3disk, True)
        shutil.rmtree(cls.fakes3root, True)
        ShutdownFakeS3(cls.fakes3proc)

    @classmethod
    def get_header_value(cls, headers, key):
        '''
        helper function to return the header
        value from a list of headers
        '''

        for header in headers:
            if key in header:
                return header.split(key + ": ")
    
    def parse_cookie(self, cookie_header_value):
        '''
        Parse a simple cookie in to parts
        '''
        cookie_parts = cookie_header_value.rstrip('\r\n').split('; ')
        cookie_pair = cookie_parts[0]
        cookie_expiry = cookie_parts[1]
        cookie_domain = cookie_parts[2].split("=")[-1]
        cookie_path = cookie_parts[3].rstrip(";").split("=")[-1]

        return cookie_pair, cookie_expiry, cookie_domain, cookie_path 

    def make_api_request(self, url, headers):
        '''
        helper method to make outbound request
        '''

        try:
            req = urllib2.Request(url, headers=headers)
            response = urllib2.urlopen(req)
            return response.read(), response.code

        except urllib2.HTTPError, e:
            print e
            return None, e.code

        except urllib2.URLError, e:
            return None, None
        
        except httplib.BadStatusLine, e:
            return None, None

    def server_api_request(self, pub_id, vid, width, height, ip=None):
        '''
        basic server api requester

        Add X-Client-IP header if ip is not None
        '''

        url = self.base_url % ("server", pub_id, vid)
        url += self.get_params % (width, height)
        headers = {}
        if ip:
            headers = {"X-Client-IP" : ip}
        return self.make_api_request(url, headers)

    def client_api_request(self, pub_id, vid, width, height, ip, **kwargs):
        '''
        Client request api requester 
        '''
        
        #Register urllib2 cookie processor and a Redirect handler
        cookieprocessor = urllib2.HTTPCookieProcessor()
        opener = urllib2.build_opener(MyHTTPRedirectHandler, cookieprocessor)
        
        url = self.base_url % ("client", pub_id, vid)
        url += self.get_params % (width, height)
        headers = kwargs.get('headers', {})
        if len(headers) == 0: 
            headers = {"X-Forwarded-For" : ip}
        else:
            headers["X-Forwarded-For"] = ip

        req = urllib2.Request(url, headers=headers)
        
        try:
            r = opener.open(req)
            return r
        except urllib2.URLError, e:
            # ok to throw urlerror, the image url returned are invalid
            pass

    def test_healthcheck(self):
        '''
        Verify the healthcheck
        '''

        url = "http://localhost:%s/healthcheck" % self.port
        req = urllib2.Request(url)
        r = urllib2.urlopen(req)
        self.assertEqual(r.code, 200)
        self.assertEqual(r.read(), 'In service with current Mastermind')


if __name__ == '__main__':
    utils.neon.InitNeon()
    test_utils.neontest.main()
