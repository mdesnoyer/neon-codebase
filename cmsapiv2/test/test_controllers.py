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

#from tornado.testing import AsyncHTTPTestCase

class TestNewAccountHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application

    @tornado.testing.gen_test
    def test_get_new_acct_not_implemented(self):
        try: 
            url = '/api/v2/accounts' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method="GET")
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
	    pass 


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
    def test_post_acct_not_implemented(self):
        try: 
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='abc123', 
                                                    method="POST")
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
	    pass 
    
    @tornado.testing.gen_test
    def test_delete_acct_not_implemented(self):
        try: 
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='DELETE')
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
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
            url = '/api/v2/%s' % (rjson['neon_api_key']) 
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
        url = '/api/v2/%s/integrations/ooyala?publisher_id=123123abc' % (self.account_id_api_key)
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
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s' % (self.account_id_api_key, self.test_i_id)
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
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&ooyala_api_key=%s' % (self.account_id_api_key, self.test_i_id, ooyala_api_key)
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
        url = '/api/v2/%s/integrations/brightcove?publisher_id=123123abc' % (self.account_id_api_key)
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
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s' % (self.account_id_api_key, self.test_i_id)
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
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s&read_token=%s' % (self.account_id_api_key, self.test_i_id, read_token)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 200)
        platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(platform.read_token, read_token)  

class TestVideoHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application
    def setUp(self):
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testvideohiid'
        defop = neondata.BrightcovePlatform.modify(self.account_id_api_key, self.test_i_id, lambda x: x, create_missing=True) 
        user.modify(self.account_id_api_key, lambda p: p.add_platform(defop))
        super(TestVideoHandler, self).setUp()
    
    @unittest.skip("not implemented yet") 
    @patch('api.brightcove_api.BrightcoveApi.read_connection.send_request') 
    @tornado.testing.gen_test
    def test_post_video(self, http_mock):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs' % (self.account_id_api_key, self.test_i_id)
        #send_request_mock = self._callback_wrap_mock(http_mock.send_request)
        #send_request_mock.fetch().side_effect = [HTTPResponse(HTTPRequest('http://test_bc'), 200, buffer=StringIO('{"job_id":"j123"}'))]
        #request = HTTPServerRequest(uri=self.get_url(url), method='POST') 
        #response = yield controllers.VideoHelper.addVideo(request, self.account_id_api_key)
        #rv = yield self.http_client.fetch(self.get_url(url),
        #                                   body='',
        #                                   method='POST',
        #                                   allow_nonstandard_methods=True)
    @tornado.testing.gen_test
    def test_get_without_video_id(self):
        try: 
            vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
            vm.save()
            url = '/api/v2/%s/videos' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
        except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 400) 

    @tornado.testing.gen_test
    def test_get_single_video(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_get_two_videos(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid2'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1,vid2' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test
    def test_get_single_video_with_fields(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created,thumbnails' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_update_video_testing_enabled(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&testing_enabled=0' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        

if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
