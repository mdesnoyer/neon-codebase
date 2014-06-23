import json
import logging
import random
import os
import subprocess
import unittest
import urllib
import urllib2
import tempfile

_log = logging.getLogger(__name__)

class ISP:
    def __init__(self, port=None):
        pass
    
    def start(self):
        pass

    def stop(self):
        pass

### URLLIB2 Redirect Handler
class MyHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_302(self, req, fp, code, msg, headers):
        print "Redirect", headers
        return urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302

class TestISPApi(unittest.TestCase):
    '''
    API testing
    '''

    def setUp(self):
        self.base_url = "http://localhost:8080/v1/%s/%s/%s/?width=%s&height=%s"
        self.base_url = "http://107.22.73.156/v1/%s/%s/%s/?width=%s&height=%s"
        #default ids
        self.pub_id = "pub1"
        self.vid = "vid1"

    def tearDown(self):
        pass

    def make_api_request(self, url, headers):
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
        url = self.base_url % ("server", pub_id, vid, width, height)
        headers = {}
        if ip:
            headers = {"X-Client-IP" : ip}
        return self.make_api_request(url, headers)

    def client_api_request(self, pub_id, vid, width, height, ip):
        cookieprocessor = urllib2.HTTPCookieProcessor()
        opener = urllib2.build_opener(MyHTTPRedirectHandler, cookieprocessor)
        url = self.base_url % ("client", pub_id, vid, width, height) 
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
        #self.assertIsNotNone(response)
        #headers = response.info().headers
        #self.assertTrue(response.code, 302)
        #self.assertEqual(response.read(), '')
        #self.assertIn("Location", headers)
        #self.assertIn("Set-Cookie", headers)

    def test_client_api_request_with_cookie(self):
        pass

    def test_client_api_request_without_cookie(self):
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
        im_url = json.loads(response.read())["data"]["vid"]
        self.assertIsNotNone(im_url)
        #self.assertIn("application/json", headers)
    
    def test_server_api_request_without_custom_header(self):
        '''
        Server api when client ip is missing
        '''
        response = self.server_api_request(self.pub_id, self.vid, 600, 500)
        self.assertIsNotNone(response)
        self.assertTrue(response.code, 200)

    def test_server_api_without_width(self):
        pass
    
    def test_server_api_without_height(self):
        pass

    def test_server_api_with_non_standard_size(self):
        response = self.server_api_request(self.pub_id, self.vid, 1, 1)
        self.assertIsNone(response)
    
    def test_server_api_with_invalid_pubid(self):
        response = self.server_api_request("invalid_pub", self.vid, 1, 1)
        self.assertIsNone(response)
    
    def test_server_api_with_invalid_video_id(self):
        response = self.server_api_request(self.pub_id, "invalid_vid", 600, 500)
        self.assertIsNone(response)
    
    def test_server_api_with_cip_argument(self):
        pass

    def test_ab_test_ratio(self):
        '''
        Test sending a bunch of requests
        '''
        pass

if __name__ == '__main__':
    unittest.main()
