import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)

from cmsapiv2 import controllers
import json
import tornado.gen
import tornado.ioloop
import tornado.testing
import tornado.httpclient
import test_utils.redis
import unittest
import utils.neon
import urllib
import test_utils.neontest
from mock import patch
from cmsdb import neondata
from StringIO import StringIO
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse 
from tornado.httputil import HTTPServerRequest

import bcove_responses
#from tornado.testing import AsyncHTTPTestCase

class TestAccountHandler(tornado.testing.AsyncHTTPTestCase):
    def get_app(self): 
        return controllers.application

    @tornado.testing.gen_test
    def test_get_acct_does_not_exist(self):
        try: 
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method="GET")
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 400) 
	    pass 
    
    @tornado.testing.gen_test 
    def test_get_acct_does_exist(self):
        try: 
            url = '/api/v2/accounts?customer_name=123abc'
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    body='', 
                                                    method='POST', 
                                                    allow_nonstandard_methods=True)
	    self.assertEquals(response.code, 200)
            rjson = json.loads(response.body)
            self.assertEquals(rjson['customer_name'], '123abc') 
            url = '%s%s' % ('/api/v2/', rjson['neon_api_key']) 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method="GET")
            rjson2 = json.loads(response.body) 
            self.assertEquals(rjson['account_id'],rjson2['account_id']) 
        except tornado.httpclient.HTTPError as e:
            pass

    @tornado.testing.gen_test 
    def test_basic_post_account(self):
        url = '/api/v2/accounts?customer_name=123abc'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], '123abc') 

    @tornado.testing.gen_test 
    def test_update_acct(self):  
        self.assertEquals(1,1)
 
    @tornado.testing.gen_test 
    def test_not_implemented_methods(self):  
        self.assertEquals(1,1)
 
class TestOoyalaIntegrationHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application
    def setUp(self):
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testiid' 
        defop = neondata.OoyalaPlatform.modify(self.account_id_api_key, self.test_i_id, lambda x: x, create_missing=True) 
        super(TestOoyalaIntegrationHandler, self).setUp()

    @tornado.testing.gen_test 
    def test_post_integration(self):
        url = '%s%s%s' % ('/api/v2/',self.account_id_api_key,'/integrations/ooyala?publisher_id=123123abc')
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                          rjson['neon_api_key'], 
                                          rjson['integration_id'])

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_get_integration(self):
        url = '%s%s%s%s' % ('/api/v2/',self.account_id_api_key,'/integrations/ooyala?integration_id=',self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_put_integration(self):
        ooyala_api_key = 'testapikey' 
        url = '%s%s%s%s%s%s' % ('/api/v2/',
                            self.account_id_api_key,
                            '/integrations/ooyala?integration_id=',
                            self.test_i_id, 
                            '&ooyala_api_key=', 
                            ooyala_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 200)
        platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(platform.ooyala_api_key, ooyala_api_key)  

class TestBrightcoveIntegrationHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application
    def setUp(self):
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        defop = neondata.BrightcovePlatform.modify(self.account_id_api_key, self.test_i_id, lambda x: x, create_missing=True) 
        super(TestBrightcoveIntegrationHandler, self).setUp()

    @tornado.testing.gen_test 
    def test_post_integration(self):
        url = '%s%s%s' % ('/api/v2/',self.account_id_api_key,'/integrations/brightcove?publisher_id=123123abc')
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          rjson['neon_api_key'], 
                                          rjson['integration_id'])

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_get_integration(self):
        url = '%s%s%s%s' % ('/api/v2/',self.account_id_api_key,'/integrations/brightcove?integration_id=',self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_put_integration(self):
        read_token = 'readtoken' 
        url = '%s%s%s%s%s%s' % ('/api/v2/',
                            self.account_id_api_key,
                            '/integrations/brightcove?integration_id=',
                            self.test_i_id, 
                            '&read_token=', 
                            read_token)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 200)
        platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(platform.read_token, read_token)  

class TestVideoHandler(test_utils.neontest.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application
    def setUp(self):
        super(TestVideoHandler, self).setUp()
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testvideohiid'
        defop = neondata.BrightcovePlatform.modify(self.account_id_api_key, self.test_i_id, lambda x: x, create_missing=True) 
        user.modify(self.account_id_api_key, lambda p: p.add_platform(defop))
    
    @unittest.skip("do not need a post right now") 
    @patch('api.brightcove_api.BrightcoveApi.read_connection.send_request') 
    @tornado.testing.gen_test
    def test_post_video(self, http_mock):
        url = '%s%s%s%s%s' % ('/api/v2/',self.account_id_api_key,'/videos?integration_id=',self.test_i_id,'&external_video_ref=1234ascs')
        send_request_mock = self._callback_wrap_mock(http_mock.send_request)
        send_request_mock.fetch().side_effect = [HTTPResponse(HTTPRequest('http://test_bc'), 200, buffer=StringIO('{"job_id":"j123"}'))]
        request = HTTPServerRequest(uri=self.get_url(url), method='POST') 
        response = yield controllers.VideoHelper.addVideo(request, self.account_id_api_key)
        #rv = yield self.http_client.fetch(self.get_url(url),
        #                                   body='',
        #                                   method='POST',
        #                                   allow_nonstandard_methods=True)
    @tornado.testing.gen_test
    def test_get_without_video_id(self):
        try: 
            vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
            vm.save()
            url = '%s%s%s' % ('/api/v2/',self.account_id_api_key,'/videos')
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
        except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 400) 

    @tornado.testing.gen_test
    def test_get_single_video(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '%s%s%s' % ('/api/v2/',self.account_id_api_key,'/videos?video_id=vid1')
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_get_two_videos(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid2'))
        vm.save()
        url = '%s%s%s' % ('/api/v2/',self.account_id_api_key,'/videos?video_id=vid1,vid2')
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        rjson = json.loads(response.body) 
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 2)

if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
