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
import utils.http
import urllib
import test_utils.neontest
import uuid
import jwt
from mock import patch
from cmsdb import neondata
from StringIO import StringIO
from utils.imageutils import PILImageUtils
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse 
from tornado.httputil import HTTPServerRequest

class TestControllersBase(test_utils.neontest.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application

    def post_exceptions(self, url, params, exception_mocker): 
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = Exception('blah blah')  
        header = { 'Content-Type':'application/json' }
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='POST', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 500)

        exception_mock.side_effect = ValueError('blah blah')  
        header = { 'Content-Type':'application/json' }
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='POST', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 500)

        exception_mock.side_effect = KeyError('blah blah')  
        header = { 'Content-Type':'application/json' }
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='POST', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 500)
 
        exception_mock.side_effect = controllers.Invalid('blah blah')  
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='POST', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 400) 
        exception_mocker.stop()

    def get_exceptions(self, url, exception_mocker):
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = Exception('blah blah')  
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
              			 method="GET")
        response = self.wait()
        self.assertEquals(response.code, 500)

        exception_mock.side_effect = ValueError('blah blah')  
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
              			 method="GET")
        response = self.wait()
        self.assertEquals(response.code, 500)

        exception_mock.side_effect = controllers.NotFoundError('blah blah')  
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
              			 method="GET")
        response = self.wait()
        self.assertEquals(response.code, 404) 
        exception_mocker.stop()

    def put_exceptions(self, url, params, exception_mocker):
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = Exception('blah blah')  
        header = { 'Content-Type':'application/json' }
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='PUT', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 500)

        exception_mock.side_effect = controllers.NotFoundError('blah blah')  
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='PUT', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 404)
 
        exception_mock.side_effect = controllers.AlreadyExists('blah blah')  
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='PUT', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 409)
 
        exception_mock.side_effect = controllers.Invalid('blah blah')  
	self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 body=params, 
                                 method='PUT', 
                                 headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 400) 
        exception_mocker.stop()

class TestNewAccountHandler(TestControllersBase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        super(TestNewAccountHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()

    @tornado.testing.gen_test 
    def test_create_new_account_query(self):
        url = '/api/v2/accounts?customer_name=meisnew'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], 'meisnew')
 
    @tornado.testing.gen_test 
    def test_create_new_account_json(self):
        params = json.dumps({'customer_name': 'meisnew'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], 'meisnew') 

    @tornado.testing.gen_test
    def test_get_new_acct_not_implemented(self):
        with self.assertRaises(tornado.httpclient.HTTPError):  
            url = '/api/v2/accounts' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method="GET")

    def test_post_acct_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.AccountHandler.post')
        params = json.dumps({'rando': '123123abc'})
	url = '/api/v2/124234234?param=123'
        self.post_exceptions(url, params, exception_mocker) 

class TestAccountHandler(TestControllersBase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.user = neondata.NeonUserAccount(uuid.uuid1().hex,customer_name='testingaccount')
        self.user.save() 
        self.verify_account_mocker = patch(
            'cmsapiv2.controllers.APIV2Handler.verify_account')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.side_effect = True
        super(TestAccountHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()
        self.verify_account_mocker.stop()

    @tornado.testing.gen_test
    def test_get_acct_does_not_exist(self):
        try: 
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method="GET")
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 404) 
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
	url = '/api/v2/accounts?customer_name=123abc'
	response = yield self.http_client.fetch(self.get_url(url), 
			       body='',
   			       method='POST', 
   			       allow_nonstandard_methods=True)
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
	self.assertEquals(rjson['customer_name'], '123abc') 
	url = '/api/v2/%s' % (rjson['account_id']) 
        header = { 'X-Neon-API-Key': rjson['api_key'] }
	response = yield self.http_client.fetch(self.get_url(url),
                                         headers=header,  
                       			 method="GET")
        rjson2 = json.loads(response.body) 
        self.assertEquals(rjson['account_id'],rjson2['account_id']) 

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
    def test_update_acct_base(self): 
        header = { 'X-Neon-API-Key': self.user.api_v2_key }
        url = '/api/v2/%s?default_height=1200&default_width=1500' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT',
                                                headers=header,  
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                headers = header,  
                                                method="GET")
        rjson = json.loads(response.body)
        default_size = rjson['default_size']
        self.assertEquals(default_size[0],1500)
        self.assertEquals(default_size[1],1200)

    @tornado.testing.gen_test 
    def test_update_acct_height_only(self): 
        header = { 'X-Neon-API-Key': self.user.api_v2_key }
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                headers=header,  
                                                method="GET")
        orig_user = json.loads(response.body)
        default_size_old = orig_user['default_size'] 

        url = '/api/v2/%s?default_height=1200' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT', 
                                                headers=header,  
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                headers=header,  
                                                method="GET")
        new_user = json.loads(response.body)
        default_size_new = new_user['default_size']
        self.assertEquals(default_size_new[1],1200)
        self.assertEquals(default_size_new[0],default_size_old[0])

    @tornado.testing.gen_test 
    def test_update_acct_width_only(self): 
        # do a get here to test and make sure the height wasn't messed up
        header = { 'X-Neon-API-Key': self.user.api_v2_key }
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                headers=header,  
                                                method="GET")
        orig_user = json.loads(response.body)
        default_size_old = orig_user['default_size'] 

        url = '/api/v2/%s?default_width=1200' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT', 
                                                headers=header,  
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                headers=header,  
                                                method="GET")
        new_user = json.loads(response.body)
        default_size_new = new_user['default_size']
        self.assertEquals(default_size_new[0],1200)
        self.assertEquals(default_size_new[1],default_size_old[1])

    def test_get_acct_unauthorized(self):
        header = { 'X-Neon-API-Key': 'this_is_invalid_for_sure' }
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        self.http_client.fetch(self.get_url(url), 
                               headers=header, 
                               callback=self.stop,  
                               method="GET")
        response = self.wait()
        self.assertEquals(response.code, 401) 
        
    def test_update_acct_unauthorized(self):
        header = { 'X-Neon-API-Key': 'this_is_invalid_for_sure' }
        url = '/api/v2/%s?default_width=1200' % (self.user.neon_api_key) 
        response = self.http_client.fetch(self.get_url(url), 
                                          body='', 
                                          method='PUT', 
                                          headers=header,  
                                          callback=self.stop, 
                                          allow_nonstandard_methods=True)
        response = self.wait() 
        self.assertEquals(response.code, 401) 
 
    def test_get_acct_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.AccountHandler.get')
	url = '/api/v2/%s' % '1234234'
        self.get_exceptions(url, exception_mocker)  

    def test_put_acct_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.AccountHandler.put')
	url = '/api/v2/124234234?param=123'
        params = json.dumps({'rando': '123123abc'})
        self.put_exceptions(url, params, exception_mocker)
 
class TestOoyalaIntegrationHandler(TestControllersBase): 
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        user = neondata.NeonUserAccount(uuid.uuid1().hex,customer_name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testiid' 
        defop = neondata.OoyalaIntegration.modify(self.test_i_id, lambda x: x, create_missing=True) 
        self.verify_account_mocker = patch(
            'cmsapiv2.controllers.APIV2Handler.verify_account')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.side_effect = True
        super(TestOoyalaIntegrationHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()
        self.verify_account_mocker.stop()

    @tornado.testing.gen_test 
    def test_post_integration(self):
        url = '/api/v2/%s/integrations/ooyala?publisher_id=123123abc' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          rjson['integration_id'])

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_get_integration(self):
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s' % (self.account_id_api_key, self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id)
 
    def test_get_integration_dne(self):
        url = '/api/v2/%s/integrations/ooyala?integration_id=idontexist' % (self.account_id_api_key)
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait()
        self.assertEquals(response.code, 404)
        rjson = json.loads(response.body)
        self.assertTrue('idontexist' in rjson['message']) 
 
    @tornado.testing.gen_test 
    def test_put_integration(self):
        ooyala_api_key = 'testapikey' 
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&ooyala_api_key=%s' % (self.account_id_api_key, self.test_i_id, ooyala_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(platform.ooyala_api_key, ooyala_api_key)
    @tornado.testing.gen_test 
    def test_put_integration_dne(self):
        try: 
            ooyala_api_key = 'testapikey' 
            url = '/api/v2/%s/integrations/ooyala?integration_id=nope&ooyala_api_key=%s' % (self.account_id_api_key, ooyala_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='PUT', 
                                                    allow_nonstandard_methods=True)
        except Exception as e:
            self.assertEquals(e.code, 404)  
 
    @tornado.testing.gen_test 
    def test_put_integration_ensure_old_info_not_nulled(self):
        ooyala_api_key = 'testapikey' 
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&ooyala_api_key=%s' % (self.account_id_api_key, self.test_i_id, ooyala_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        ooyala_api_secret = 'testapisecret' 
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&ooyala_api_secret=%s' % (self.account_id_api_key, self.test_i_id, ooyala_api_secret)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(platform.ooyala_api_key, ooyala_api_key) 
        self.assertEquals(platform.api_secret, ooyala_api_secret)
 
    def test_get_integration_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.OoyalaIntegrationHandler.get')
	url = '/api/v2/%s/integrations/ooyala' % '1234234'
        self.get_exceptions(url, exception_mocker)  

    def test_put_integration_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.OoyalaIntegrationHandler.put')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/integrations/ooyala' % '1234234'
        self.put_exceptions(url, params, exception_mocker) 
 
    def test_post_integration_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.OoyalaIntegrationHandler.post')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/integrations/ooyala' % '1234234'
        self.post_exceptions(url, params, exception_mocker)  
 
class TestBrightcoveIntegrationHandler(TestControllersBase): 
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        user = neondata.NeonUserAccount(uuid.uuid1().hex,customer_name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        self.defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True)
        self.verify_account_mocker = patch(
            'cmsapiv2.controllers.APIV2Handler.verify_account')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.side_effect = True
        super(TestBrightcoveIntegrationHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()
        self.verify_account_mocker.stop()

    @tornado.testing.gen_test 
    def test_post_integration(self):
        url = '/api/v2/%s/integrations/brightcove?publisher_id=123123abc' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])

        self.assertEquals(rjson['integration_id'], platform.integration_id)
        # make sure the defaults are the default 
        self.assertEquals(rjson['playlist_feed_ids'], self.defop.playlist_feed_ids) 
        self.assertEquals(rjson['read_token'], self.defop.read_token) 
        self.assertEquals(rjson['write_token'], self.defop.write_token) 
        self.assertEquals(rjson['callback_url'], self.defop.callback_url) 
        self.assertEquals(rjson['uses_batch_provisioning'], self.defop.uses_batch_provisioning)
        self.assertEquals(rjson['id_field'], self.defop.id_field)
 
    @tornado.testing.gen_test 
    def test_post_integration_body_params(self):
        params = json.dumps({'publisher_id': '123123abc'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['publisher_id'], '123123abc')

        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])

        self.assertEquals(rjson['integration_id'], platform.integration_id)
        # make sure the defaults are the default 
        self.assertEquals(rjson['playlist_feed_ids'], self.defop.playlist_feed_ids) 
        self.assertEquals(rjson['read_token'], self.defop.read_token) 
        self.assertEquals(rjson['write_token'], self.defop.write_token) 
        self.assertEquals(rjson['callback_url'], self.defop.callback_url) 
        self.assertEquals(rjson['uses_batch_provisioning'], self.defop.uses_batch_provisioning)
        self.assertEquals(rjson['id_field'], self.defop.id_field)
 
    @tornado.testing.gen_test 
    def test_get_integration(self):
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s' % (self.account_id_api_key, self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
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
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(platform.read_token, read_token)
 
    @tornado.testing.gen_test 
    def test_put_integration_dne(self):
        try: 
            read_token = 'readtoken' 
            url = '/api/v2/%s/integrations/brightcove?integration_id=nope&read_token=%s' % (self.account_id_api_key, read_token)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='PUT', 
                                                    allow_nonstandard_methods=True)
        except Exception as e:
            self.assertEquals(e.code, 404)  
 
    @tornado.testing.gen_test 
    def test_put_integration_ensure_old_info_not_nulled(self):
        read_token = 'readtoken' 
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s&read_token=%s' % (self.account_id_api_key, self.test_i_id, read_token)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        write_token = 'writetoken' 
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s&write_token=%s' % (self.account_id_api_key, self.test_i_id, write_token)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(platform.read_token, read_token) 
        self.assertEquals(platform.write_token, write_token)
 
    @tornado.testing.gen_test 
    def test_post_integration_one_playlist_feed_id(self):
        url = '/api/v2/%s/integrations/brightcove?publisher_id=123123abc&playlist_feed_ids=abc' \
                   % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])

        playlist_feed_ids = rjson['playlist_feed_ids'] 

        self.assertEquals(playlist_feed_ids[0], 'abc')
        self.assertEquals(platform.playlist_feed_ids[0], 'abc')
 
    @tornado.testing.gen_test 
    def test_post_integration_multiple_playlist_feed_ids(self):
        url = '/api/v2/%s/integrations/brightcove?publisher_id=123123abc&playlist_feed_ids=abc,def,ghi' \
                   % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])

        playlist_feed_ids = rjson['playlist_feed_ids'] 

        self.assertEquals(playlist_feed_ids[0], 'abc')
        self.assertEquals(playlist_feed_ids[1], 'def')
        self.assertEquals(playlist_feed_ids[2], 'ghi')
        self.assertEquals(platform.playlist_feed_ids[0], 'abc')
        self.assertEquals(platform.playlist_feed_ids[1], 'def')
        self.assertEquals(platform.playlist_feed_ids[2], 'ghi')

    @tornado.testing.gen_test 
    def test_post_integration_body_playlist_feed_ids(self):
        params = json.dumps({'publisher_id': '123123abc',
                             'playlist_feed_ids': 'abc,def,ghi,123'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])

        playlist_feed_ids = rjson['playlist_feed_ids'] 
        self.assertEquals(playlist_feed_ids[0], 'abc')
        self.assertEquals(playlist_feed_ids[1], 'def')
        self.assertEquals(playlist_feed_ids[2], 'ghi')
        self.assertEquals(playlist_feed_ids[3], '123')
        self.assertEquals(platform.playlist_feed_ids[0], 'abc')
        self.assertEquals(platform.playlist_feed_ids[1], 'def')
        self.assertEquals(platform.playlist_feed_ids[2], 'ghi')
        self.assertEquals(platform.playlist_feed_ids[3], '123')

    @tornado.testing.gen_test 
    def test_post_integration_with_uses_batch_provisioning(self):
        params = json.dumps({'publisher_id': '123123abc',
                             'uses_batch_provisioning': 1})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])
        self.assertTrue(platform.uses_batch_provisioning, True)

    @tornado.testing.gen_test 
    def test_put_integration_playlist_feed_ids(self):
        params = json.dumps({'publisher_id': '123123abc',
                             'playlist_feed_ids': 'abc,def,ghi,123'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
 
        rjson = json.loads(response.body)
        params = json.dumps({'integration_id': rjson['integration_id'],
                             'playlist_feed_ids': 'putupdate'})

        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='PUT', 
                                                headers=header) 
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])
        playlist_feed_ids = rjson['playlist_feed_ids']
        self.assertEquals(platform.playlist_feed_ids[0], 'putupdate')
 
    @tornado.testing.gen_test 
    def test_put_integration_uses_batch_provisioning(self):
        params = json.dumps({'publisher_id': '123123abc',
                             'uses_batch_provisioning': 1})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
 
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])
        self.assertEquals(platform.uses_batch_provisioning, True) 
        params = json.dumps({'integration_id': rjson['integration_id'],
                             'uses_batch_provisioning': 0})

        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='PUT', 
                                                headers=header) 
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          rjson['integration_id'])
        self.assertEquals(platform.uses_batch_provisioning, False)
 
    def test_get_integration_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.BrightcoveIntegrationHandler.get')
	url = '/api/v2/%s/integrations/brightcove' % '1234234'
        self.get_exceptions(url, exception_mocker)  

    def test_put_integration_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.BrightcoveIntegrationHandler.put')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/integrations/brightcove' % '1234234'
        self.put_exceptions(url, params, exception_mocker) 
 
    def test_post_integration_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.BrightcoveIntegrationHandler.post')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/integrations/brightcove' % '1234234'
        self.post_exceptions(url, params, exception_mocker)  

class TestVideoHandler(TestControllersBase): 

    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        user = neondata.NeonUserAccount(uuid.uuid1().hex,customer_name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testvideohiid'
        neondata.ThumbnailMetadata('testing_vtid_one', width=500).save()
        neondata.ThumbnailMetadata('testing_vtid_two', width=500).save()
        defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True) 
        user.modify(self.account_id_api_key, lambda p: p.add_platform(defop))
        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [self.random_image]
        self.http_mocker = patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
              self.http_mocker.start()) 
        self.verify_account_mocker = patch(
            'cmsapiv2.controllers.APIV2Handler.verify_account')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.side_effect = True
        super(TestVideoHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
        self.http_mocker.stop()
        self.verify_account_mocker.stop()
    
    @tornado.testing.gen_test
    def test_post_video(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&default_thumbnail_url=url.invalid' % (self.account_id_api_key, self.test_i_id)
        cmsdb_download_image_mocker = patch('cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image]
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')
        cmsdb_download_image_mocker.stop()

    @tornado.testing.gen_test
    def test_post_video_with_dots(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234a.s.cs&default_thumbnail_url=url.invalid' % (self.account_id_api_key, self.test_i_id)
        cmsdb_download_image_mocker = patch('cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image] 
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body)
        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        self.assertNotEquals(rjson['job_id'],'')
        self.assertNotEquals(rjson['video']['key'],internal_video_id)
        cmsdb_download_image_mocker.stop()

    def test_post_failed_to_download_thumbnail(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&default_thumbnail_url=url.invalid' \
                      % (self.account_id_api_key, self.test_i_id)
        self.im_download_mock.side_effect = neondata.ThumbDownloadError('boom')
        response = self.http_client.fetch(self.get_url(url),
                                          body='',
                                          callback=self.stop,
                                          method='POST',
                                          allow_nonstandard_methods=True)
        response = self.wait()
        rjson = json.loads(response.body)
        # TODO ??? should this be a 400 ??? 
        self.assertEquals(response.code,400)
        self.assertEquals(rjson['message'],'failed to download thumbnail')

    @tornado.testing.gen_test
    def test_post_video_with_duration(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&duration=1354' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals(1354, video.duration)

    @tornado.testing.gen_test
    def test_post_video_with_float_duration(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&duration=1354.54' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals(1354.54, video.duration)

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_valid_one(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-08-18T06:36:40.123Z' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals('2015-08-18T06:36:40.123Z', video.publish_date)

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_valid_two(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-08-18T06:36:40Z' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals('2015-08-18T06:36:40Z', video.publish_date)

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_valid_three(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-08-18' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals('2015-08-18', video.publish_date)

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_invalid(self):
        with self.assertRaises(tornado.httpclient.HTTPError):  
            url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-0' % (self.account_id_api_key, self.test_i_id)
            self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='POST',
                                                    allow_nonstandard_methods=True)

    @tornado.testing.gen_test
    def test_post_video_with_custom_data(self):
        custom_data = urllib.quote(json.dumps({ "a" : 123456 }))
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&custom_data=%s' % (self.account_id_api_key, self.test_i_id, custom_data)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertTrue(video.custom_data is not None)

    def test_post_two_videos(self):
        # use self.stop/wait to make sure we get the response back and 
        # not an exception, we don't want no exception
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        self.http_client.fetch(self.get_url(url),
                               callback = self.stop, 
                               body='',
                               method='POST',
                               allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        first_job_id = rjson['job_id']  
        self.assertNotEquals(first_job_id,'')
        
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop,
                               body='',
                               method='POST',
                              allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 409) 
        rjson = json.loads(response.body) 
        data = rjson['data'] 
        self.assertTrue(first_job_id in data)

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
        url = '/api/v2/%s/videos?video_id=vid1&fields=created' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        self.assertEquals(response.code, 200)
    
    @tornado.testing.gen_test
    def test_get_single_video_with_invalid_fields(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created,me_is_invalid' % (self.account_id_api_key)
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
        rjson = json.loads(response.body)
        self.assertFalse(rjson['testing_enabled'])
        self.assertEquals(response.code, 200)

        url = '/api/v2/%s/videos?video_id=vid1&testing_enabled=1' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        rjson = json.loads(response.body)
        self.assertTrue(rjson['testing_enabled'])
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_get_single_video_with_thumbnails_field(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'), 
                                    tids=['testing_vtid_one', 'testing_vtid_two'])
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created,thumbnails' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 1)
        
        thumbnail_array = rjson['videos'][0]['thumbnails']
        thumbnail_one = thumbnail_array[0] 
        thumbnail_two = thumbnail_array[1] 
        self.assertEquals(len(thumbnail_array), 2)
        self.assertEquals(thumbnail_one['width'], 500)
        self.assertEquals(thumbnail_one['key'], 'testing_vtid_one') 
        self.assertEquals(thumbnail_two['width'], 500)
        self.assertEquals(thumbnail_two['key'], 'testing_vtid_two') 
    
    @tornado.testing.gen_test 
    def test_get_video_with_thumbnails_field_no_thumbnails(self): 
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'), 
                                    tids=[])
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created,thumbnails' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        thumbnail_array = rjson['videos'][0]['thumbnails']
        self.assertEquals(len(rjson['videos']), 1)
        self.assertEquals(len(thumbnail_array), 0)
    
    @tornado.testing.gen_test
    def test_update_video_does_not_exist(self):
        try: 
            url = '/api/v2/%s/videos?video_id=vid_does_not_exist&testing_enabled=0' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='PUT', 
                                                    allow_nonstandard_methods=True)
	except Exception as e:
            self.assertEquals(e.code, 404)

    def test_get_video_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.VideoHandler.get')
	url = '/api/v2/%s/videos' % '1234234'
        self.get_exceptions(url, exception_mocker)  

    def test_put_video_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.VideoHandler.put')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/videos' % '1234234'
        self.put_exceptions(url, params, exception_mocker) 
 
    def test_post_video_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.VideoHandler.post')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/videos' % '1234234'
        self.post_exceptions(url, params, exception_mocker)  

class TestThumbnailHandler(TestControllersBase): 
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        user = neondata.NeonUserAccount(uuid.uuid1().hex,customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        neondata.ThumbnailMetadata('testingtid', width=500).save()
        self.test_video = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,
                             'tn_test_vid1')).save()
        neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,
                             'tn_test_vid2')).save()

        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [self.random_image] 
        self.verify_account_mocker = patch(
            'cmsapiv2.controllers.APIV2Handler.verify_account')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.side_effect = True
        super(TestThumbnailHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
        self.verify_account_mocker.stop()
    
    @tornado.testing.gen_test
    def test_add_new_thumbnail(self):
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid1&thumbnail_location=blah.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code,202)
        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'tn_test_vid1')
        video = neondata.VideoMetadata.get(internal_video_id)
         
        self.assertEquals(len(video.thumbnail_ids), 1)

    @tornado.testing.gen_test
    def test_add_two_new_thumbnails(self):
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid2&thumbnail_location=blah.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code,202)
        self.im_download_mock.side_effect = [self.random_image] 
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid2&thumbnail_location=blah2.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code,202)
        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'tn_test_vid2')
        video = neondata.VideoMetadata.get(internal_video_id)
        thumbnail_ids = video.thumbnail_ids
        self.assertEquals(len(video.thumbnail_ids), 2)
        #for tid in thumbnail_ids: 
            

    @tornado.testing.gen_test
    def test_get_thumbnail_exists(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['width'],500)
        self.assertEquals(rjson['key'],'testingtid')

    @tornado.testing.gen_test
    def test_get_thumbnail_does_not_exist(self):
        try: 
            url = '/api/v2/%s/thumbnails?thumbnail_id=testingtiddoesnotexist' % (self.account_id_api_key) 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
	except Exception as e:
            self.assertEquals(e.code, 404)

    @tornado.testing.gen_test
    def test_thumbnail_update_enabled(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        old_tn = json.loads(response.body)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid&enabled=0' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body)
        self.assertEquals(new_tn['enabled'],False)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid&enabled=1' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body) 
        self.assertEquals(new_tn['enabled'],True)

    @tornado.testing.gen_test
    def test_thumbnail_update_no_params(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        old_tn = json.loads(response.body)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body) 
        self.assertEquals(response.code,200)
        self.assertEquals(new_tn['enabled'],old_tn['enabled'])

    @tornado.testing.gen_test
    def test_delete_thumbnail_not_implemented(self):
        try: 
            url = '/api/v2/%s/thumbnails?thumbnail_id=12234' % (self.account_id_api_key)  
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='DELETE')
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
	    pass
 
    def test_get_thumbnail_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.ThumbnailHandler.get')
	url = '/api/v2/%s/thumbnails' % '1234234'
        self.get_exceptions(url, exception_mocker)  

    def test_put_thumbnail_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.ThumbnailHandler.put')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/thumbnails' % '1234234'
        self.put_exceptions(url, params, exception_mocker) 
 
    def test_post_thumbnail_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.ThumbnailHandler.post')
        params = json.dumps({'integration_id': '123123abc'})
	url = '/api/v2/%s/thumbnails' % '1234234'
        self.post_exceptions(url, params, exception_mocker)  

class TestHealthCheckHandler(TestControllersBase): 
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.http_mocker = patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
              self.http_mocker.start()) 
        super(TestHealthCheckHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()
        self.http_mocker.stop()
 
    def test_healthcheck_success(self): 
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
	url = '/healthcheck/'
        response = self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait()
        self.assertEquals(response.code, 200) 
        
    def test_healthcheck_failure(self): 
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,400))
	url = '/healthcheck/'
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait()
        self.assertEquals(response.code, 500) 

class TestAPIKeyRequired(TestControllersBase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.user = neondata.NeonUserAccount(uuid.uuid1().hex,customer_name='testingaccount')
        self.user.save() 
        super(TestAPIKeyRequired, self).setUp()

    def tearDown(self): 
        self.redis.stop()
    
    def make_calls_and_asserts(self, url, method): 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body='', 
                               method=method, 
                               allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 401)

    def test_all_urls(self): 
        urls = [ ('/api/v2/a1', 'GET'), 
                 ('/api/v2/a1', 'PUT'), 
                 ('/api/v2/a1/integrations/brightcove', 'GET'), 
                 ('/api/v2/a1/integrations/brightcove', 'PUT'), 
                 ('/api/v2/a1/integrations/brightcove', 'POST'), 
                 ('/api/v2/a1/integrations/ooyala', 'GET'), 
                 ('/api/v2/a1/integrations/ooyala', 'PUT'), 
                 ('/api/v2/a1/integrations/ooyala', 'POST'), 
                 ('/api/v2/a1/videos', 'GET'), 
                 ('/api/v2/a1/videos', 'PUT'), 
                 ('/api/v2/a1/videos', 'POST'), 
                 ('/api/v2/a1/thumbnails', 'GET'), 
                 ('/api/v2/a1/thumbnails', 'PUT'), 
                 ('/api/v2/a1/thumbnails', 'POST') ]

        for url, method in urls:
            self.make_calls_and_asserts(url, method)  

if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
