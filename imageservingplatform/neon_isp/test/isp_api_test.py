#!/usr/bin/env python

'''
API/ Integration tests

Neon Image serving platform API tests
Test the responses for all the defined APIs for ISP
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
                base_path + "/imageservingplatform/nginx-1.6.2/objs/nginx"

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
            gzip_mfile.write(gzipstream.GzipStream().read(StringIO(gzip_data)))
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

    def test_client_api_request(self):
        '''
        Test Client API call

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
       
        prefix = "neonvid_"
        response = self.client_api_request(self.pub_id, prefix + self.vid, 600, 500, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)
        
        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")
            if "Set-Cookie" in header:
                cookie = header.split("Set-Cookie: ")[-1]

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, self.expected_img_url)
        self.assertIsNotNone(cookie)
        
        #verify cookie values
        cookie_pair, cookie_expiry, cookie_domain, cookie_path = \
                                                self.parse_cookie(cookie)

        cookie_name, cookie_value = cookie_pair.split('=')
        self.assertEqual(cookie_name, self.neon_cookie_name)
        self.assertEqual(cookie_domain, self.cookie_domain)
        self.assertEqual(cookie_path, "/")

        #Verify cookie inclusion of timestamp in the cookie
        ts = int(time.time()) / 100
        self.assertTrue(str(ts) in cookie_value)

    #
    # directive is invalid and rejected but the rest of mastermind is loaded. 
    # the  default account thumbnail is returned

    def test_client_api_directive_rejected(self):
        '''
        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("pubmissingpct", prefix + "vidmissingpct", 800, 700, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/default_url_defaccmissingpct.jpg")

    def test_client_api_default_thumbnail(self):
        '''
        A video directive doesnt exists, an scaled default thumbnail of exact size
        is found.  

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 800, 600, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb_600_800_default_url_defaccount1.jpg")

    #
    # Account-wide default thumbnail 
    #


    def test_client_api_default_thumbnail_good_enough_size_match_height(self):
        '''
        A video directive doesnt exists, a scaled default thumbnail of 
        size that is a perfect match is returned despite the existence of a good match 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub2", prefix + "novid", 800, 600, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb_600_800_default_url_defaccount2.jpg")



    def test_client_api_default_thumbnail_good_enough_size_match_height(self):
        '''
        A video directive doesnt exists, a scaled default thumbnail of 
        size that is within acceptable tolerances is selected. 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 800, 606, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb_600_800_default_url_defaccount1.jpg")


    def test_client_api_default_thumbnail_good_enough_size_match_width(self):
        '''
        A video directive doesnt exists, a scaled default thumbnail of 
        size that is within acceptable tolerances is selected. 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 806, 600, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb_600_800_default_url_defaccount1.jpg")



    def test_client_api_default_thumbnail_good_enough_size_match_width_height(self):
        '''
        A video directive doesnt exists, a scaled default thumbnail of 
        size that is within acceptable tolerances is selected. 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 806, 606, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb_600_800_default_url_defaccount1.jpg")


    def test_client_api_default_thumbnail_good_enough_size_match_smaller_height(self):
        '''
        A video directive doesnt exists, a scaled default thumbnail of 
        size that is within acceptable tolerances is selected. 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 800, 594, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb_600_800_default_url_defaccount1.jpg")


    def test_client_api_default_thumbnail_no_size_match_smaller_height(self):
        '''
        A video directive doesnt exists, a scaled default thumbnail of 
        size that is within acceptable tolerances is selected. 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 800, 593, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/default_url_defaccount1.jpg")


    def test_client_api_default_thumbnail_no_size_match_smaller_width(self):
        '''
        A video directive doesnt exists, a scaled default thumbnail of 
        size that is within acceptable tolerances is selected. 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 793, 600, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/default_url_defaccount1.jpg")


    def test_client_api_default_thumbnail_no_scaled_image_match_heigth(self):
        '''
        A video directive doesnt exists, no scaled default thumbnail of appropriate height
        is found, so the default_url is returned.

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 800, 607, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/default_url_defaccount1.jpg")

    def test_client_api_default_thumbnail_no_scaled_image_match_width(self):
        '''
        A video directive doesnt exists, no scaled default thumbnail of appropriate width
        is found, so the default_url is returned.

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub1", prefix + "novid", 807, 600, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None
        
        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")
        
        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/default_url_defaccount1.jpg")

    def test_client_api_default_thumbnail_no_scaled_image_defined(self):
        '''
        A video directive doesnt exists, no default scaled thumbnails are listed 
        (empty set), so the default_url is returned.

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub3", prefix + "novid", 800, 600, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None
        
        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")
        
        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/default_url_defaccount3.jpg")


    def test_client_api_default_thumbnail_directive_exist_no_default(self):
        '''
        A video directive exists, so no default thumbnails is returned. 

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("defaultpub4", prefix + "vid0", 800, 700, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb2_700_800.jpg")


    # Perfect fix first feature testing. A perfectly matched scale should be returned 
    # if it exist in the fraction scaled images list.  Otherwise an approximately fitting scaled 
    # image should be returned if one qualifies.    

    def test_client_api_perfect_match_thumbnail(self):
        '''
        a perfectly matched scaled image is returned when one exists in a fraction  
        and others which are also qualifying approx match.  
        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("perfectpub1", prefix + "vid0", 805, 705, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb2_705_805.jpg")

    def test_client_api_no_perfect_match_multiple_approx_match_thumbnail(self):
        '''
        No perfectly matched scaled image is available, a qualifying approximate
        one is returned. At this time no specific order is required in the selection
        when multiple approx choices are available.

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
        prefix = "neonvid_"
        response = self.client_api_request("perfectpub1", prefix + "vid0", 812, 712, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)

        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, "http://neon/thumb2_706_806.jpg")



    # ISP should understand and support request having a .jpg extension in
    # lower and upper case
    def test_client_api_request_jpg_extention(self):
        '''
        Test Client API call

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''

        prefix = "neonvid_"
        jpg_extention = '.jpg'
        response = self.client_api_request(self.pub_id, prefix + self.vid + jpg_extention, 600, 500, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)
        
        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")
            if "Set-Cookie" in header:
                cookie = header.split("Set-Cookie: ")[-1]

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, self.expected_img_url)
        self.assertIsNotNone(cookie)
        
        #verify cookie values
        cookie_pair, cookie_expiry, cookie_domain, cookie_path = \
                                                self.parse_cookie(cookie)

        cookie_name, cookie_value = cookie_pair.split('=')
        self.assertEqual(cookie_name, self.neon_cookie_name)
        self.assertEqual(cookie_domain, self.cookie_domain)
        self.assertEqual(cookie_path, "/")

        #Verify cookie inclusion of timestamp in the cookie
        ts = int(time.time()) / 100
        self.assertTrue(str(ts) in cookie_value)


    def test_client_api_request_jpg_extention_uppercase(self):
        '''
        Test Client API call

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''
       
        prefix = "neonvid_"
        jpg_extention = '.JPG'
        response = self.client_api_request(self.pub_id, prefix + self.vid + jpg_extention, 600, 500, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)
        
        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")
            if "Set-Cookie" in header:
                cookie = header.split("Set-Cookie: ")[-1]

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, self.expected_img_url)
        self.assertIsNotNone(cookie)
        
        #verify cookie values
        cookie_pair, cookie_expiry, cookie_domain, cookie_path = \
                                                self.parse_cookie(cookie)

        cookie_name, cookie_value = cookie_pair.split('=')
        self.assertEqual(cookie_name, self.neon_cookie_name)
        self.assertEqual(cookie_domain, self.cookie_domain)
        self.assertEqual(cookie_path, "/")

        #Verify cookie inclusion of timestamp in the cookie
        ts = int(time.time()) / 100
        self.assertTrue(str(ts) in cookie_value)


    def test_client_api_request_with_cookie(self):
        '''
        Test client api request when a neonglobaluserid 
        cookie is present

        Expect only the bucketid cookie
        '''
       
        # use an old timestamp for the neonglobaluseridcookie
        h = {"Cookie" : "neonglobaluserid=dummyuid1406003475"}
        prefix = "neonvid_"
        response = self.client_api_request(self.pub_id, prefix + self.vid, 600, 500,
                                            "12.2.2.4", headers=h)
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)
        
        #Assert location header and cookie
        im_url = None
        cookie = None
        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")
            if "Set-Cookie" in header:
                cookie = header.split("Set-Cookie: ")[-1]

        self.assertIsNotNone(im_url)
        self.assertEqual(im_url, self.expected_img_url)
        
        #Assert that the cookie is keyed by neonimg_{PUB}_{VID} 
        cookie_pair, cookie_expiry, cookie_domain, cookie_path = \
                                                self.parse_cookie(cookie)

        cookie_name, cookie_value = cookie_pair.split('=')
        exp_cookie_name = "neonimg_%s_%s" % (self.pub_id, self.vid)
        self.assertEqual(cookie_name, exp_cookie_name)
        self.assertEqual(cookie_domain, self.cookie_domain)
        self.assertEqual(cookie_path, "/v1/client/%s/%s" % (self.pub_id,
                            self.vid))

    def test_client_api_request_with_fresh_uuid_cookie(self):
        '''
        Test with a newly generated neonglobaluserid cookie
        
        Expect no cookie to be sent back since the user isnt'
        ready for AB Testing yet!
        '''
        ts = int(time.time())
        h = {"Cookie" : "neonglobaluserid=dummyuid%s" % ts}
        prefix = "neonvid_"
        response = self.client_api_request(self.pub_id, prefix + self.vid, 600, 500,
                                            "12.2.2.4", headers=h)
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)
        
        im_url = None
        cookie = None
        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1]
            if "Set-Cookie" in header:
                cookie = header.split("Set-Cookie: ")[-1]

        self.assertEqual(im_url, self.expected_img_url + "\r\n")
        #TODO: Should check the bucket id cookie ?

    def test_client_api_with_bucket_id_cookie(self):
        #TODO: finish test when we start using the bucketID
        pass

    def test_client_api_request_with_invalid_video(self):

        '''
        '''
        ts = int(time.time())
        response = self.client_api_request(self.pub_id, self.vid, 600, 500,
                                            "12.2.2.4", headers={})
        self.assertEquals(response.code, 204)

    def test_client_video_id_url_token_missing(self):
        '''
        video id is missing in url
        '''
        url = "http://localhost:" + self.port + "/v1/client/22334223432/"
        ip = "203.2.113.7"
        headers = {"X-Forwarded-For" : ip}
        response, code = self.make_api_request(url, headers)
        self.assertEqual(response, '')
        self.assertEqual(code, 204)


    ################### Server API tests #####################

    def test_server_api_request(self):
        '''
        Server API request
        '''

        response, code = self.server_api_request(self.pub_id, self.vid, 600, 500, "12.2.2.4")
        self.assertIsNotNone(response)
        self.assertTrue(code, 200)
        im_url = json.loads(response)["data"]
        #headers = response.info().headers
        #self.assertEqual(headers[2], 'Content-Type: application/json\r\n')
        self.assertIsNotNone(im_url)
        #self.assertIsNotNone(TestImageServingPlatformAPI.get_header_value(headers,
        #                        "application/json"))
    
    def test_server_api_request_without_custom_header(self):
        '''
        Server api when client ip is missing
        '''
        
        response, code = self.server_api_request(self.pub_id, self.vid, 600, 500)
        self.assertIsNotNone(response)
        self.assertTrue(code, 200)
        im_url = json.loads(response)["data"]
        self.assertEqual(im_url, self.expected_img_url)

    def test_server_api_without_width(self):
        url = self.base_url % ("server", self.pub_id, self.vid)
        url += "?height=500"
        response, code = self.make_api_request(url, {})
        im_url = json.loads(response)["data"]
        self.assertEqual(im_url, self.default_url)

    def test_server_api_without_height(self):
        '''
        Returns default Image URL
        '''

        url = self.base_url % ("server", self.pub_id, self.vid)
        url += "?width=600"
        response, code = self.make_api_request(url, {})
        im_url = json.loads(response)["data"]
        self.assertEqual(im_url, self.default_url)

    @unittest.skip("cloudinary URL not being sent currently") 
    def test_server_api_with_non_standard_size(self):
        '''
        Returns a cloudinary URL
        '''
        h = 1; w =1
        tid = 'thumb1' 
        response, code = self.server_api_request(self.pub_id, self.vid, w, h)
        self.assertIsNotNone(response)
        im_url = json.loads(response)["data"]
        cloudinary_url =\
                "http://res.cloudinary.com/neon-labs/image/upload/w_%s,h_%s/neontn%s_w%s_h%s.jpg.jpg";
        self.assertEqual(im_url, cloudinary_url % (w, h, tid, "0", "0"))
    
    def test_server_api_with_invalid_pubid(self):
        response, code = self.server_api_request("invalid_pub", self.vid, 1, 1)
        self.assertIsNone(response)
        self.assertEqual(code, 400)
    
    def test_server_api_with_invalid_video_id(self):
        response, code = self.server_api_request(self.pub_id, "invalid_vid", 600, 500)
        self.assertIsNone(response)
        self.assertEqual(code, 400)
    
    def test_server_api_with_cip_argument(self):
        '''
        use cip argument
        '''
        cip = "12.12.12.24"
        width = 600
        height = 500
        url = self.base_url % ("server", self.pub_id, self.vid)
        url += self.get_params % (width, height)
        url += "&cip=%s" % cip
        headers = {}
        response, code = self.make_api_request(url, headers)
        data = json.loads(response)["data"]
        self.assertIsNotNone(data)

    def test_ab_test_ratio(self):
        '''
        Test sending a bunch of requests
        '''
        random.seed(135215) 
        def client_api_call():
            r = random.randrange(1000, 9999)
            h = {"Cookie" : "neonglobaluserid=dummyuid130600%d" % r}
            prefix = "neonvid_"
            response = self.client_api_request(self.pub_id, prefix + self.vid, 600, 500,
                                                "12.2.2.4", headers=h)
            redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
            headers = redirect_response.headers
            self.assertIsNotNone(redirect_response)
            
            #Assert location header and cookie
            im_url = None
            cookie = None
            for header in headers:
                if "Location" in header:
                    im_url = header.split("Location: ")[-1].rstrip("\r\n")
                if "Set-Cookie" in header:
                    cookie = header.split("Set-Cookie: ")[-1]

            return im_url

        urls = {}
        N = 50
        for i in range(N):
            url = client_api_call()
            try:
               urls[url] += 1
            except KeyError:
               urls[url] = 1

        expected_count = {
        'http://neont2/thumb1_500_600.jpg': 0.2 * N,
        'http://neon-image-cdn.s3.amazonaws.com/pixel.jpg': 0.7 * N,
        'http://neont3/thumb1_500_600.jpg': 0.1 * N}

        # Expect close to 80% accuracy 
        for url in urls:
            self.assertTrue(expected_count[url] * 0.8 <= urls[url] <=
                    expected_count[url] * 1.2)

    def test_get_thumbnailid(self):
        '''
        Test get thumbnailid API
        
        Since no cookie is sent, hash is based on ip address
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s/?params=%s" %\
                ("getthumbnailid", self.pub_id, self.vid)
        ip = "203.2.113.7"
        headers = {"X-Forwarded-For" : ip}
        response, code = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response, "thumb2")
    
    def test_get_thumbnailid_with_html_extension(self):
        '''
        Test get thumbnailid API
        
        Since no cookie is sent, hash is based on ip address
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s.html/?params=%s" %\
                ("getthumbnailid", self.pub_id, self.vid)
        ip = "203.2.113.7"
        headers = {"X-Forwarded-For" : ip}
        response, code = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response, "<!DOCTYPE html><html><head><script type='text/javascript'>window.parent.postMessage('thumb2', '*')" 
                                   "</script></head><body></body></html>")

    def test_multiple_thumbnailids(self):
        '''
        Test CSV response from the API
        Since no cookie is sent, hash is based on ip address
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s/?params=%s,%s" %\
                ("getthumbnailid", self.pub_id, self.vid, self.vid)
        ip = "203.2.113.7"
        headers = {"X-Forwarded-For" : ip}
        response, code = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response, "thumb2,thumb2")
    
    def test_multiple_thumbnails_with_html_extension(self):
        '''
        Test get thumbnailid API
        
        Since no cookie is sent, hash is based on ip address
        '''
        
        url = "http://localhost:" + self.port + "/v1/%s/%s.html/?params=%s,%s" %\
                ("getthumbnailid", self.pub_id, self.vid, self.vid)
        ip = "203.2.113.7"
        headers = {"X-Forwarded-For" : ip}
        response, code = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response, "<!DOCTYPE html><html><head><script type='text/javascript'>window.parent.postMessage('thumb2,thumb2', '*')" 
                                   "</script></head><body></body></html>")
    
    def test_multiple_thumbnailids_with_invalid_vid(self):
        '''
        Test that the API return "null" for invalid video id
        Since no cookie is sent, hash is based on ip address
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s/?params=%s,%s" %\
                ("getthumbnailid", self.pub_id, self.vid, "invalid_vid")
        ip = "203.2.113.7"
        headers = {"X-Forwarded-For" : ip}
        response, code = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response, "thumb2,null")
    
    def test_thumbnailids_with_malformed_url(self):
        '''
        malformed URL with multiple "??"
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s/??params=%s,%s" %\
                ("getthumbnailid", self.pub_id, self.vid, "invalid_vid")
        ip = "203.2.113.7"
        headers = {"X-Forwarded-For" : ip}
        response, code = self.make_api_request(url, headers)
        self.assertEqual(code, 204)

if __name__ == '__main__':
    utils.neon.InitNeon()
    test_utils.neontest.main()
