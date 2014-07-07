'''
Neon Image serving platform API tests

Test the responses for all the defined APIs for ISP

'''
#!/usr/bin/env python
import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..',
    '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)
   
import atexit
import json
import logging
import os
import random
import signal
import subprocess
import unittest
import urllib
import urllib2
import utils
import utils.ps
import time
import tempfile

_log = logging.getLogger(__name__)

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

    def __init__(self, port=None):
        self.config_file = base_path + "/imageservingplatform/neon_isp/test/nginx-test.conf"
        self.nginx_path = base_path + "/imageservingplatform/nginx-1.4.7/objs/nginx" #get build path

    def start(self):
        self.proc = subprocess.Popen([
            '/usr/bin/env', self.nginx_path, "-c",
            self.config_file],
            stdout=subprocess.PIPE)

    def stop(self):
        _log.info("shutting down nginx %s" %self.proc.pid)

        still_running = utils.ps.send_signal_and_wait(signal.SIGTERM,
                                                      [self.proc.pid],
                                                      timeout=10)
        if still_running:
            self.proc.kill()

        self.proc.wait()

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
        return urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302

    @classmethod
    def get_last_redirect_response(cls):
        return cls.redirect_response


class TestImageServingPlatformAPI(unittest.TestCase):
    '''
    API testing
    '''
    isp = ISP()

    @classmethod
    def setUpClass(cls):
        cls.isp.start()
        time.sleep(1)

    def setUp(self):

        self.port = "8080"
        self.base_url = "http://localhost:"+ self.port +"/v1/%s/%s/%s/"
        self.get_params = "?width=%s&height=%s"
        #default ids
        self.pub_id = "pub1"
        self.vid = "vid1"

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.isp.stop()

    @classmethod
    def get_header_value(cls, headers, key):
        '''
        helper function to return the header
        value from a list of headers
        '''

        for header in headers:
            if key in header:
                return header.split(key + ": ")
        
    def make_api_request(self, url, headers):
        '''
        helper method to make outbound request
        '''

        try:
            req = urllib2.Request(url, headers=headers)
            r = urllib2.urlopen(req)
            return r

        except urllib2.HTTPError, e:
            print e
            pass
        except urllib2.URLError, e:
            pass

    def server_api_request(self, pub_id, vid, width, height, ip=None):
        '''
        server api requester

        Add X-Client-IP header if ip is not None
        '''

        url = self.base_url % ("server", pub_id, vid)
        url += self.get_params % (width, height)
        headers = {}
        if ip:
            headers = {"X-Client-IP" : ip}
        return self.make_api_request(url, headers)

    def client_api_request(self, pub_id, vid, width, height, ip):
        '''
        Client request api requester 
        '''
        
        #Register urllib2 cookie processor and a Redirect handler
        cookieprocessor = urllib2.HTTPCookieProcessor()
        opener = urllib2.build_opener(MyHTTPRedirectHandler, cookieprocessor)
        
        url = self.base_url % ("client", pub_id, vid)
        url += self.get_params % (width, height)
        headers = {"X-Forwarded-For" : ip}
        req = urllib2.Request(url, headers=headers)
        
        try:
            r = opener.open(req)
            return r
        except urllib2.URLError, e:
            # ok to throw urlerror, the image url returned are invalid
            pass

    def test_client_api_request(self):
        '''
        Test Client API call

        verify:
        response code
        location header
        presence of a valid Set-Cookie header
        '''

        response = self.client_api_request(self.pub_id, self.vid, 600, 500, "12.2.2.4")
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        self.assertIsNotNone(redirect_response)
        
        #Assert location header and cookie
        im_url = None
        cookie = None

        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1]
            if "Set-Cookie" in header:
                cookie = header.split("Set-Cookie: ")[-1]

        self.assertIsNotNone(im_url)
        self.assertIsNotNone(cookie)
        
        #headers = response.info().headers
        #self.assertTrue(response.code, 302)

    def test_client_api_request_with_cookie(self):
        pass

    ### Server API tests

    def test_server_api_request(self):
        '''
        Server API request
        '''

        response = self.server_api_request(self.pub_id, self.vid, 600, 500, "12.2.2.4")
        self.assertIsNotNone(response)
        self.assertTrue(response.code, 200)
        headers = response.info().headers
        im_url = json.loads(response.read())["data"]
        self.assertIsNotNone(im_url)
        self.assertIsNotNone(TestImageServingPlatformAPI.get_header_value(headers,
                                "application/json"))
    
    def test_server_api_request_without_custom_header(self):
        '''
        Server api when client ip is missing
        '''
        
        response = self.server_api_request(self.pub_id, self.vid, 600, 500)
        self.assertIsNotNone(response)
        self.assertTrue(response.code, 200)

    def test_server_api_without_width(self):
        url = self.base_url % ("server", self.pub_id, self.vid)
        url += "?height=500"
        response = self.make_api_request(url, {})
        im_url = json.loads(response.read())["data"]
        self.assertEqual(im_url, "")

    def test_server_api_without_height(self):
        url = self.base_url % ("server", self.pub_id, self.vid)
        url += "?width=600"
        response = self.make_api_request(url, {})
        im_url = json.loads(response.read())["data"]
        self.assertEqual(im_url, "")

    def test_server_api_with_non_standard_size(self):
        response = self.server_api_request(self.pub_id, self.vid, 1, 1)
        im_url = json.loads(response.read())["data"]
        self.assertEqual(im_url, "")
    
    def test_server_api_with_invalid_pubid(self):
        response = self.server_api_request("invalid_pub", self.vid, 1, 1)
        self.assertIsNone(response)
    
    def test_server_api_with_invalid_video_id(self):
        response = self.server_api_request(self.pub_id, "invalid_vid", 600, 500)
        self.assertIsNone(response)
    
    def test_server_api_with_cip_argument(self):
        '''
        use cip argument
        '''
        pass

    def test_ab_test_ratio(self):
        '''
        Test sending a bunch of requests
        '''
        pass

    def test_get_thumbnailid(self):
        '''
        Test get thumbnailid API
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s/?params=%s" %\
                ("getthumbnailid", self.pub_id, self.vid)
        ip = "203.02.113.7"
        headers = {"X-Forwarded-For" : ip}
        response = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response.read(), "thumb1")

    def test_multiple_thumbnailids(self):
        '''
        Test CSV response from the API
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s/?params=%s,%s" %\
                ("getthumbnailid", self.pub_id, self.vid, self.vid)
        ip = "203.02.113.7"
        headers = {"X-Forwarded-For" : ip}
        response = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response.read(), "thumb1,thumb1")
    
    def test_multiple_thumbnailids_with_invalid_vid(self):
        '''
        Test that the API return "null" for invalid video id
        '''

        url = "http://localhost:" + self.port + "/v1/%s/%s/?params=%s,%s" %\
                ("getthumbnailid", self.pub_id, self.vid, "invalid_vid")
        ip = "203.02.113.7"
        headers = {"X-Forwarded-For" : ip}
        response = self.make_api_request(url, headers)
        self.assertIsNotNone(response)
        self.assertEqual(response.read(), "thumb1,null")


if __name__ == '__main__':
    unittest.main()
