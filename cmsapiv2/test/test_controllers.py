#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsapiv2.apiv2 import *
from cmsapiv2 import controllers
from cmsapiv2 import authentication
from datetime import datetime, timedelta
import json
import tornado.gen
import tornado.ioloop
import tornado.testing
import tornado.httpclient
import test_utils.postgresql
import test_utils.redis
import time
import unittest
import utils.neon
import utils.http
import urllib
import test_utils.neontest
import uuid
import jwt
from mock import patch
from cmsdb import neondata
from passlib.hash import sha256_crypt
from StringIO import StringIO
from cvutils.imageutils import PILImageUtils
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse 
from tornado.httputil import HTTPServerRequest
from utils.options import options

class TestBase(test_utils.neontest.AsyncHTTPTestCase): 
    def setUp(self):
        self.send_email_mocker = patch(
            'cmsapiv2.authentication.NewAccountHandler.send_email')
        self.send_email_mock = self.send_email_mocker.start()
        self.send_email_mock.return_value = True
        super(test_utils.neontest.AsyncHTTPTestCase, self).setUp()

    def tearDown(self): 
        self.send_email_mocker.stop()
        super(test_utils.neontest.AsyncHTTPTestCase, self).tearDown()
        
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
        self.assertIn('application/json', response.headers['Content-Type'])
        
        exception_mock.side_effect = ValueError('blah blah')  
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body=params, 
                               method='POST', 
                               headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 500)
        self.assertIn('application/json', response.headers['Content-Type'])
        
        exception_mock.side_effect = KeyError('blah blah')  
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body=params, 
                               method='POST', 
                               headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 500)
        self.assertIn('application/json', response.headers['Content-Type'])
        
        exception_mock.side_effect = controllers.Invalid('blah blah')  
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body=params, 
                               method='POST', 
                               headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 400) 
        self.assertIn('application/json', response.headers['Content-Type'])
        exception_mocker.stop()

    def get_exceptions(self, url, exception_mocker):
        exception_mock = self._future_wrap_mock(exception_mocker.start())
        exception_mock.side_effect = Exception('blah blah')  
        self.http_client.fetch(self.get_url(url),
                                 callback=self.stop, 
                                 method="GET")
        response = self.wait()
        self.assertEquals(response.code, 500)
        self.assertIn('application/json', response.headers['Content-Type'])
        
        exception_mock.side_effect = ValueError('blah blah')  
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method="GET")
        response = self.wait()
        self.assertEquals(response.code, 500)
        self.assertIn('application/json', response.headers['Content-Type'])
        
        exception_mock.side_effect = controllers.NotFoundError('blah blah')  
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method="GET")
        response = self.wait()
        self.assertEquals(response.code, 404)
        self.assertIn('application/json', response.headers['Content-Type'])
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
        self.assertIn('application/json', response.headers['Content-Type'])

        exception_mock.side_effect = controllers.NotFoundError('blah blah')  
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body=params, 
                               method='PUT', 
                               headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 404)
        self.assertIn('application/json', response.headers['Content-Type'])
 
        exception_mock.side_effect = controllers.AlreadyExists('blah blah')  
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body=params, 
                               method='PUT', 
                               headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 409)
        self.assertIn('application/json', response.headers['Content-Type'])
        
        exception_mock.side_effect = controllers.Invalid('blah blah')  
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body=params, 
                               method='PUT', 
                               headers=header) 
        response = self.wait()
        self.assertEquals(response.code, 400) 
        self.assertIn('application/json', response.headers['Content-Type'])
        exception_mocker.stop()

class TestAuthenticationBase(TestBase): 
    def get_app(self): 
        return authentication.application

class TestControllersBase(TestBase): 
    def get_app(self): 
        return controllers.application

class TestNewAccountHandler(TestAuthenticationBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestNewAccountHandler, self).setUp()

    def tearDown(self): 
        self.verify_account_mocker.stop()
        self.postgresql.clear_all_tables()
        super(TestNewAccountHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
        super(TestNewAccountHandler, cls).tearDownClass() 

    @tornado.testing.gen_test 
    def test_create_new_account_query(self):
        url = '/api/v2/accounts?customer_name=meisnew&email=a@a.bc'\
              '&admin_user_username=a@a.com'\
              '&admin_user_password=b1234567'\
              '&admin_user_first_name=kevin'\
              '&admin_user_last_name=fenger'\
              '&admin_user_title=Mr.'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        self.assertIn('application/json', response.headers['Content-Type'])
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['message'],
                                 'account verification email sent to')

        # verifier row gets created 
        verifier = yield neondata.Verification.get('a@a.bc', async=True) 
        
        # now send this token to the verify endpoint        
        url = '/api/v2/accounts/verify?token=%s' % verifier.token
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], 'meisnew')
        self.assertEquals(rjson['serving_enabled'], 0) 
        account_id = rjson['account_id'] 
        account = yield neondata.NeonUserAccount.get(account_id, 
                      async=True)
        self.assertEquals(account.name, 'meisnew')
        self.assertEquals(account.email, 'a@a.bc')
        self.assertEquals(account.serving_enabled, 0)

        user = yield neondata.User.get('a@a.com', 
                   async=True) 
        self.assertEquals(user.username, 'a@a.com')
        self.assertEquals(user.first_name, 'kevin')
        self.assertEquals(user.last_name, 'fenger')
        self.assertEquals(user.title, 'Mr.')

        limits = yield neondata.AccountLimits.get(account_id, async=True)
        self.assertEquals(limits.key, account_id) 
        self.assertEquals(limits.video_posts, 0)
        
    @tornado.testing.gen_test 
    def test_create_new_account_json(self):
        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.bc', 
                             'admin_user_username':'a@a.com', 
                             'admin_user_password':'testacpas', 
                             'admin_user_first_name':'kevin'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['message'],
                                 'account verification email sent to')

        # verifier row gets created 
        verifier = yield neondata.Verification.get('a@a.bc', async=True)

        url = '/api/v2/accounts/verify?token=%s' % verifier.token
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)

 
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], 'meisnew')
        self.assertEquals(rjson['serving_enabled'], 0) 
        account_id = rjson['account_id'] 
        account = yield neondata.NeonUserAccount.get(account_id, 
                      async=True)
        self.assertEquals(account.name, 'meisnew')
        self.assertEquals(account.email, 'a@a.bc')
        self.assertEquals(account.serving_enabled, 0)

        user = yield neondata.User.get('a@a.com', 
                   async=True) 
        self.assertEquals(user.username, 'a@a.com')
        self.assertEquals(user.first_name, 'kevin')

        limits = yield neondata.AccountLimits.get(account_id, async=True)
        self.assertEquals(limits.key, account_id) 
        self.assertEquals(limits.video_posts, 0)
          
    @tornado.testing.gen_test 
    def test_create_new_account_duplicate_users(self):
        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.bc', 
                             'admin_user_username':'a@a.com', 
                             'admin_user_password':'testacpas'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        self.assertEquals(response.code, 200) 
        verifier = yield neondata.Verification.get('a@a.bc', async=True)

        url = '/api/v2/accounts/verify?token=%s' % verifier.token
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
 
        params = json.dumps({'customer_name': 'meisnew2', 
                             'email': 'a@a.bc', 
                             'admin_user_username':'a@a.com', 
                             'admin_user_password':'testacpas'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        with self.assertRaises(tornado.httpclient.HTTPError) as e: 
            response = yield self.http_client.fetch(
                self.get_url(url), 
                body=params, 
                method='POST', 
                headers=header)
        
        # fails with a conflict
        self.assertEquals(e.exception.code, 409) 
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['data'],
                                 'user with that email already exists')


    @tornado.testing.gen_test 
    def test_create_new_account_invalid_email(self):
        url = '/api/v2/accounts?customer_name=meisnew&email=aa.bc'\
              '&admin_user_username=abcd1234'\
              '&admin_user_password=b1234567'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:  
            response = yield self.http_client.fetch(
                self.get_url(url), 
                body='', 
                method='POST', 
                allow_nonstandard_methods=True)

        self.assertEquals(e.exception.code, 400)

    @tornado.testing.gen_test 
    def test_create_new_account_tracker_accounts(self):
        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.bc', 
                             'admin_user_username':'a@a.com', 
                             'admin_user_password':'testacpas'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
        self.assertEquals(response.code, 200)
        verifier = yield neondata.Verification.get('a@a.bc', async=True)

        url = '/api/v2/accounts/verify?token=%s' % verifier.token
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], 'meisnew')
        prod_t_id = rjson['tracker_account_id'] 
        staging_t_id = rjson['staging_tracker_account_id'] 
        tai_p = neondata.TrackerAccountIDMapper.get_neon_account_id(prod_t_id) 
        self.assertEquals(tai_p[0], rjson['account_id'])
        tai_s = neondata.TrackerAccountIDMapper.get_neon_account_id(staging_t_id)
        self.assertEquals(tai_s[0], rjson['account_id'])
 
    @tornado.testing.gen_test 
    def test_account_is_verified(self):
        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.bc', 
                             'admin_user_username':'a@a.com', 
                             'admin_user_password':'testacpas'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test 
    def test_create_account_send_email_exception(self):
        self.send_email_mock.return_value = None 
        self.send_email_mock.side_effect = Exception('blah blah') 
        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.invalid', 
                             'admin_user_username':'a@a.invalid', 
                             'admin_user_password':'testacpas'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:  
            yield self.http_client.fetch(self.get_url(url), 
                                         body=params, 
                                         method='POST', 
                                         headers=header)
        self.assertEquals(e.exception.code, 500)

    @tornado.testing.gen_test
    def test_create_account_send_email_ses_exception(self):
        self.send_email_mocker.stop() 
        ses_mocker = patch('boto.ses.connection.SESConnection.send_email')
        ses_mock = ses_mocker.start()
        ses_mock.side_effect = Exception('random exception')  
        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.bc.invalid', 
                             'admin_user_username':'a@a.invalid', 
                             'admin_user_password':'testacpas'})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:  
            yield self.http_client.fetch(self.get_url(url), 
                                         body=params, 
                                         method='POST', 
                                         headers=header)
        rjson = json.loads(e.exception.response.body)
        self.assertEquals(e.exception.code, 500) 
        self.assertRegexpMatches(rjson['error']['data'],
                                 'unable to send verification')
        ses_mocker.stop() 
        self.send_email_mocker.start() 
 
    @tornado.testing.gen_test
    def test_get_new_acct_not_implemented(self):
        with self.assertRaises(tornado.httpclient.HTTPError):  
            url = '/api/v2/accounts' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method="GET")

    def test_post_acct_exceptions(self):
        exception_mocker = patch('cmsapiv2.authentication.NewAccountHandler.post')
        params = json.dumps({'name': '123123abc'})
	url = '/api/v2/accounts'
        self.post_exceptions(url, params, exception_mocker)

class TestAccountHandler(TestControllersBase):
    def setUp(self):
        self.user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingaccount')
        self.user.save() 
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestAccountHandler, self).setUp()

    def tearDown(self): 
        self.verify_account_mocker.stop()
        self.postgresql.clear_all_tables()
        super(TestAccountHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
        super(TestAccountHandler, cls).tearDownClass() 

    @tornado.testing.gen_test
    def test_get_acct_does_not_exist(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method="GET")
        self.assertEquals(e.exception.code, 404) 
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'resource was not found')

    @tornado.testing.gen_test
    def test_post_acct_not_implemented(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            header = { 'Content-Type':'application/json' }
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='{"abc123":"1"}', 
                                                    method="POST", 
                                                    headers=header)
        self.assertEquals(e.exception.code, 501) 
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'access levels are not defined')
    
    @tornado.testing.gen_test
    def test_delete_acct_not_implemented(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='DELETE')
        self.assertEquals(e.exception.code, 501) 
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'access levels are not defined')
    
    @tornado.testing.gen_test 
    def test_get_acct_does_exist(self):
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method="GET")
        rjson = json.loads(response.body) 
        self.assertEquals(self.user.neon_api_key, rjson['account_id']) 
        self.assertEquals(0, rjson['serving_enabled']) 

    @tornado.testing.gen_test 
    def test_update_acct_base(self): 
        url = '/api/v2/%s?default_height=1200&default_width=1500' % (
            self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT',
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method="GET")
        rjson = json.loads(response.body)
        default_size = rjson['default_size']
        self.assertEquals(default_size[0],1500)
        self.assertEquals(default_size[1],1200)

    @tornado.testing.gen_test 
    def test_update_acct_height_only(self): 
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        orig_user = json.loads(response.body)
        default_size_old = orig_user['default_size'] 

        url = '/api/v2/%s?default_height=1200' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        new_user = json.loads(response.body)
        default_size_new = new_user['default_size']
        self.assertEquals(default_size_new[1], 1200)
        self.assertEquals(default_size_new[0], default_size_old[0])

    @tornado.testing.gen_test 
    def test_update_acct_no_content_type(self): 
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='{"default_width":"1"}', 
                method="PUT") 
	    self.assertEquals(e.exception.code, 400) 
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Content-Type must be JSON')

    @tornado.testing.gen_test 
    def test_update_acct_width_only(self): 
        # do a get here to test and make sure the height wasn't messed up
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        orig_user = json.loads(response.body)
        default_size_old = orig_user['default_size'] 

        url = '/api/v2/%s?default_width=1200' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        new_user = json.loads(response.body)
        default_size_new = new_user['default_size']
        self.assertEquals(default_size_new[0], 1200)
        self.assertEquals(default_size_new[1], default_size_old[1])
    
    def test_update_account_does_not_exist(self): 
        url = '/api/v2/doesnotexist?default_height=1200&default_width=1500' 
        self.http_client.fetch(self.get_url(url), 
                               body='',
                               method='PUT',
                               callback=self.stop, 
                               allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 404) 
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'resource was not found')
 
    def test_get_acct_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.AccountHandler.get')
	url = '/api/v2/%s' % '1234234'
        self.get_exceptions(url, exception_mocker)  

    def test_put_acct_exceptions(self):
        exception_mocker = patch('cmsapiv2.controllers.AccountHandler.put')
	url = '/api/v2/124234234?param=123'
        params = json.dumps({'rando': '123123abc'})
        self.put_exceptions(url, params, exception_mocker)

class TestNewUserHandler(TestAuthenticationBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestNewUserHandler, self).setUp()

    def tearDown(self): 
        self.verify_account_mocker.stop()
        self.postgresql.clear_all_tables()
        super(TestNewUserHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
        super(TestNewUserHandler, cls).tearDownClass()
 
    @tornado.testing.gen_test 
    def test_create_new_user_query(self):
        url = '/api/v2/users?username=abcd1234&password=b1234567&access_level=63'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        self.assertIn('application/json', response.headers['Content-Type'])
        rjson = json.loads(response.body)
        self.assertEquals(rjson['username'], 'abcd1234')
        user = yield neondata.User.get('abcd1234', async=True) 
        self.assertEquals(user.username, 'abcd1234') 

    @tornado.testing.gen_test 
    def test_create_new_user_json(self):
        params = json.dumps({'username': 'abcd1234', 
                             'password': 'b1234567', 
                             'access_level': 63})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/users'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['username'], 'abcd1234')
        user = yield neondata.User.get('abcd1234', async=True) 
        self.assertEquals(user.username, 'abcd1234') 

    def test_post_user_exceptions(self):
        exception_mocker = patch('cmsapiv2.authentication.NewUserHandler.post')
        params = json.dumps({'username': '123123abc'})
	url = '/api/v2/users'
        self.post_exceptions(url, params, exception_mocker)

class TestUserHandler(TestControllersBase):
    def setUp(self):
        self.neon_user = neondata.NeonUserAccount(
            uuid.uuid1().hex,
            name='testingaccount')
        self.neon_user.save() 
        super(TestUserHandler, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(TestUserHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
        super(TestUserHandler, cls).tearDownClass()

    # token creation can be slow give it some extra time just in case
    @tornado.testing.gen_test(timeout=10.0) 
    def test_get_user_does_exist(self):
        user = neondata.User(username='testuser', 
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',  
                             access_level=neondata.AccessLevels.CREATE | 
                                          neondata.AccessLevels.READ)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        url = '/api/v2/%s/users?username=%s&token=%s' % (
                   self.neon_user.neon_api_key,
                   'testuser', 
                   token)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['username'], 'testuser')
        self.assertEquals(rjson['first_name'], 'kevin')
        self.assertEquals(rjson['last_name'], 'kevin')

        user = yield neondata.User.get('testuser', async=True) 
        self.assertEquals(user.username, 'testuser') 
 
    @tornado.testing.gen_test(timeout=10.0) 
    def test_get_user_unauthorized(self):
        user1 = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.READ)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user1.access_token = token 
        yield user1.save(async=True)

        user2 = neondata.User(username='testuser2', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.READ)
        token = JWTHelper.generate_token({'username' : 'testuser2'})  
        user2.access_token = token 
        yield user2.save(async=True)

        self.neon_user.users.append('testuser')
        self.neon_user.users.append('testuser2')
        self.neon_user.save()
 
        url = '/api/v2/%s/users?username=%s&token=%s' % (
                   self.neon_user.neon_api_key,
                   'testuser', 
                   token)
        with self.assertRaises(tornado.httpclient.HTTPError) as e: 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method='GET')
        self.assertEquals(e.exception.code, 401)  
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'Can not view')

    @tornado.testing.gen_test(timeout=10.0) 
    def test_get_user_does_not_exist(self):
        user = neondata.User(
                   username='testuser', 
                   password='testpassword', 
                   access_level=neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        url = '/api/v2/%s/users?username=%s&token=%s' % (
                   self.neon_user.neon_api_key,
                   'doesnotexist', 
                   token)
        with self.assertRaises(tornado.httpclient.HTTPError) as e: 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method='GET')
        self.assertEquals(e.exception.code, 404)  
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'resource was not')

    # token creation can be slow give it some extra time just in case
    @tornado.testing.gen_test(timeout=10.0) 
    def test_update_user_does_exist(self):
        user = neondata.User(
                   username='testuser', 
                   password='testpassword', 
                   access_level=neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        params = json.dumps({'username':'testuser', 
                             'access_level': 1,
                             'first_name' : 'kevin',  
                             'last_name' : 'kevin',  
                             'title' : 'DOCTOR',  
                             'token' : token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/users' % (self.neon_user.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='PUT', 
                                                headers=header)
        self.assertEquals(response.code, 200)
        updated_user = yield neondata.User.get('testuser', async=True) 
        self.assertEquals(updated_user.access_level, 1)
        self.assertEquals(updated_user.first_name, 'kevin')
        self.assertEquals(updated_user.last_name, 'kevin')
        self.assertEquals(updated_user.title, 'DOCTOR')
 
    # token creation can be slow give it some extra time just in case
    @tornado.testing.gen_test(timeout=10.0) 
    def test_update_user_bad_access_level(self):
        user = neondata.User(
                   username='testuser', 
                   password='testpassword', 
                   access_level=neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        params = json.dumps({'username':'testuser', 'access_level': 63, 'token' : token})
        header = { 'Content-Type':'application/json' }
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/users' % (self.neon_user.neon_api_key)
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    body=params, 
                                                    method='PUT', 
                                                    headers=header)
        self.assertEquals(e.exception.code, 401)  
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'Can not set')
 
    # token creation can be slow give it some extra time just in case
    @tornado.testing.gen_test(timeout=10.0) 
    def test_update_user_wrong_username(self):
        user1 = neondata.User(
                    username='testuser', 
                    password='testpassword', 
                    access_level=neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user1.access_token = token 
        yield user1.save(async=True)
        self.neon_user.users.append('testuser')
        self.neon_user.users.append('testuser2')
        self.neon_user.save() 

        user2 = neondata.User(
                    username='testuser2', 
                    password='testpassword', 
                    access_level=neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser2'})  
        user2.access_token = token 
        yield user2.save(async=True)
        params = json.dumps({'username':'testuser', 
                             'access_level': 63, 
                             'token' : token})
        header = { 'Content-Type':'application/json' }
        # testuser2 should not be able to update testuser 
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/users' % (self.neon_user.neon_api_key)
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    body=params, 
                                                    method='PUT', 
                                                    headers=header)

        self.assertEquals(e.exception.code, 401)  
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'Can not update')

class TestOoyalaIntegrationHandler(TestControllersBase): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testiid' 
        defop = neondata.OoyalaIntegration.modify(self.test_i_id, lambda x: x, create_missing=True) 
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestOoyalaIntegrationHandler, self).setUp()

    def tearDown(self): 
        conn = neondata.DBConnection.get(neondata.VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(neondata.ThumbnailMetadata)
        conn.clear_db()
        self.verify_account_mocker.stop()
        super(TestOoyalaIntegrationHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()
        super(TestOoyalaIntegrationHandler, cls).tearDownClass() 

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
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s' % (
            self.account_id_api_key, 
            self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id)
        self.assertEquals(rjson['account_id'], platform.account_id)
        self.assertEquals(rjson['partner_code'], platform.partner_code)
        self.assertEquals(rjson['api_key'], platform.api_key)
        self.assertEquals(rjson['api_secret'], platform.api_secret)

    @tornado.testing.gen_test 
    def test_get_integration_with_fields(self):
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s'\
              '&fields=%s' % (self.account_id_api_key, 
                   self.test_i_id, 
                   'integration_id,account_id,partner_code')
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id)
        self.assertEquals(rjson['account_id'], platform.account_id)
        self.assertEquals(rjson['partner_code'], platform.partner_code)
        self.assertEquals(rjson.get('api_key',None), None)
        self.assertEquals(rjson.get('api_secret',None), None)
 
    def test_get_integration_dne(self):
        url = '/api/v2/%s/integrations/ooyala?integration_id=idontexist' % (self.account_id_api_key)
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait()
        self.assertEquals(response.code, 404)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'idontexist') 
 
    @tornado.testing.gen_test 
    def test_put_integration(self):
        api_key = 'testapikey' 
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&api_key=%s' % (self.account_id_api_key, self.test_i_id, api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(platform.api_key, api_key)
    @tornado.testing.gen_test 
    def test_put_integration_dne(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            api_key = 'testapikey' 
            url = '/api/v2/%s/integrations/ooyala?integration_id=nope&api_key=%s' % (self.account_id_api_key, api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='PUT', 
                                                    allow_nonstandard_methods=True)
        self.assertEquals(e.exception.code, 404)  
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'unable to find') 
 
    @tornado.testing.gen_test 
    def test_put_integration_ensure_old_info_not_nulled(self):
        api_key = 'testapikey' 
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&api_key=%s' % (self.account_id_api_key, self.test_i_id, api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        api_secret = 'testapisecret' 
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&api_secret=%s' % (self.account_id_api_key, self.test_i_id, api_secret)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(platform.api_key, api_key) 
        self.assertEquals(platform.api_secret, api_secret)
 
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

# TODO KF here until hot swap is done
class TestOoyalaIntegrationHandlerPG(TestOoyalaIntegrationHandler): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testiid' 
        defop = neondata.OoyalaIntegration.modify(self.test_i_id, lambda x: x, create_missing=True) 
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestOoyalaIntegrationHandler, self).setUp()

    def tearDown(self): 
        self.verify_account_mocker.stop()
        self.postgresql.clear_all_tables()
        super(TestOoyalaIntegrationHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
        super(TestOoyalaIntegrationHandlerPG, cls).tearDownClass() 
 
class TestBrightcoveIntegrationHandler(TestControllersBase): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        self.defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True)
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestBrightcoveIntegrationHandler, self).setUp()

    def tearDown(self): 
        self.verify_account_mocker.stop()
        self.postgresql.clear_all_tables()
        super(TestBrightcoveIntegrationHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
        super(TestBrightcoveIntegrationHandler, cls).tearDownClass() 

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
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s' % (
            self.account_id_api_key, 
            self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id)
        self.assertEquals(rjson['playlist_feed_ids'], platform.playlist_feed_ids) 
        self.assertEquals(rjson['read_token'], platform.read_token) 
        self.assertEquals(rjson['write_token'], platform.write_token) 
        self.assertEquals(rjson['callback_url'], platform.callback_url) 
        self.assertEquals(rjson['uses_batch_provisioning'], platform.uses_batch_provisioning)
        self.assertEquals(rjson['id_field'], platform.id_field)

    @tornado.testing.gen_test 
    def test_get_integration_with_fields(self):
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s'\
              '&fields=%s' % (
            self.account_id_api_key, 
            self.test_i_id, 'read_token,write_token,callback_url')
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                          self.test_i_id)

        self.assertEquals(rjson['read_token'], platform.read_token) 
        self.assertEquals(rjson['write_token'], platform.write_token) 
        self.assertEquals(rjson['callback_url'], platform.callback_url) 
        self.assertEquals(rjson.get('uses_batch_provisioning', None), None)
        self.assertEquals(rjson.get('id_field', None), None)
 
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
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            read_token = 'readtoken' 
            url = '/api/v2/%s/integrations/brightcove?integration_id=nope&read_token=%s' % (self.account_id_api_key, read_token)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='PUT', 
                                                    allow_nonstandard_methods=True)
        self.assertEquals(e.exception.code, 404)  
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'unable to find') 
 
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
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testvideohiid'
        neondata.ThumbnailMetadata('testing_vtid_one', width=500,
                                   urls=['s']).save()
        neondata.ThumbnailMetadata('testing_vtid_two', width=500,
                                   urls=['d']).save()
        neondata.NeonApiRequest('job1', self.account_id_api_key).save()
        defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True) 
        user.modify(self.account_id_api_key, lambda p: p.add_platform(defop))
        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]
        self.im_download_mocker = patch(
            'cvutils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [self.random_image]
        self.http_mocker = patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
              self.http_mocker.start()) 
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        self.maxDiff = 5000
        super(TestVideoHandler, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
        self.http_mocker.stop()
        self.verify_account_mocker.stop()
        super(TestVideoHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @tornado.testing.gen_test
    def test_post_video(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&default_thumbnail_url=url.invalid'\
              '&title=a_title&url=some_url'\
              '&thumbnail_ref=ref1' % (self.account_id_api_key, self.test_i_id)
        cmsdb_download_image_mocker = patch(
            'cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(
            cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image]
        self.http_mock.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body) 
        self.assertNotEquals(rjson['job_id'],'')
        cmsdb_download_image_mocker.stop()

    @tornado.testing.gen_test
    def test_post_video_with_limits_refresh_date_reset(self):
        cmsdb_download_image_mocker = patch(
            'cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(
            cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image]
        
        limit = neondata.AccountLimits(self.account_id_api_key, 
            refresh_time_video_posts=datetime(1999,1,1), 
            video_posts=10)
        yield limit.save(async=True) 

        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&default_thumbnail_url=url.invalid'\
              '&title=a_title&url=some_url'\
              '&thumbnail_ref=ref1' % (self.account_id_api_key, self.test_i_id)

        self.http_mock.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x,200))

        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
       
        yield self.assertWaitForEquals(
            lambda: neondata.AccountLimits.get(self.account_id_api_key).video_posts, 
            1,
            async=True)
        yield self.assertWaitForEquals(
            lambda: neondata.AccountLimits.get(
                self.account_id_api_key).refresh_time_video_posts \
            > '2016-04-15 00:00:00.000000', True,
            async=True)
        # sanity check on the video make sure it made it 
        video = yield neondata.VideoMetadata.get(
            self.account_id_api_key + '_' + '1234ascs', 
            async=True)
        self.assertEquals(video.url, 'some_url') 
        cmsdb_download_image_mocker.stop()

    @tornado.testing.gen_test
    def test_post_video_with_limits_increase_post_videos(self):
        cmsdb_download_image_mocker = patch(
            'cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(
            cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image]
        
        limit = neondata.AccountLimits(self.account_id_api_key, 
            refresh_time_video_posts=datetime(2050,1,1), 
            video_posts=3)
        yield limit.save(async=True) 

        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&default_thumbnail_url=url.invalid'\
              '&title=a_title&url=some_url'\
              '&thumbnail_ref=ref1' % (self.account_id_api_key, self.test_i_id)

        self.http_mock.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x,200))

        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
         
        yield self.assertWaitForEquals(
            lambda: neondata.AccountLimits.get(self.account_id_api_key).video_posts, 
            4,
            async=True)
        yield self.assertWaitForEquals(
            lambda: neondata.AccountLimits.get(
                self.account_id_api_key).refresh_time_video_posts, 
            '2050-01-01 00:00:00.000000',
            async=True)
        # sanity check on the video make sure it made it 
        video = yield neondata.VideoMetadata.get(
            self.account_id_api_key + '_' + '1234ascs', 
            async=True)
        self.assertEquals(video.url, 'some_url') 
        
        # finally lets sanity check the limits endpoint
        url = '/api/v2/%s/limits' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_posts'], 4) 
        cmsdb_download_image_mocker.stop()

    @tornado.testing.gen_test
    def test_post_video_with_limits_too_many_requests(self):
        limit = neondata.AccountLimits(self.account_id_api_key, 
            video_posts=10)
        yield limit.save(async=True) 
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&default_thumbnail_url=url.invalid'\
              '&title=a_title&url=some_url'\
              '&thumbnail_ref=ref1' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x,200))
        with self.assertRaises(tornado.httpclient.HTTPError) as e: 
            yield self.http_client.fetch(self.get_url(url),
                                         body='',
                                         method='POST',
                                         allow_nonstandard_methods=True)

        self.assertEquals(e.exception.code, 402)

    @tornado.testing.gen_test
    def test_post_video_video_exists_in_db(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&default_thumbnail_url=url.invalid&url=some_url' % (self.account_id_api_key, self.test_i_id)
        cmsdb_download_image_mocker = patch('cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image]
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals(video.key, internal_video_id)
        cmsdb_download_image_mocker.stop()

    @tornado.testing.gen_test
    def test_post_video_thumbnail_exists_in_db(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&default_thumbnail_url=url.invalid&url=some_url' % (self.account_id_api_key, self.test_i_id)
        cmsdb_download_image_mocker = patch('cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image]
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        thumbnail_id = video.thumbnail_ids[0]
        thumbnail = neondata.ThumbnailMetadata.get(thumbnail_id)
        self.assertEquals(thumbnail_id, thumbnail.key) 
        cmsdb_download_image_mocker.stop()

    @tornado.testing.gen_test
    def test_post_video_with_dots(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234a.s.cs&default_thumbnail_url=url.invalid&url=some_url' % (self.account_id_api_key, self.test_i_id)
        cmsdb_download_image_mocker = patch(
            'cmsdb.neondata.VideoMetadata.download_image_from_url') 
        cmsdb_download_image_mock = self._future_wrap_mock(
            cmsdb_download_image_mocker.start())
        cmsdb_download_image_mock.side_effect = [self.random_image] 
        self.http_mock.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x,200))
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body)
        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key, '1234.ascs')
        self.assertNotEquals(rjson['job_id'],'')
        self.assertNotEquals(rjson['video']['video_id'], '1234.ascs')
        cmsdb_download_image_mocker.stop()

    def test_post_failed_to_download_thumbnail(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&default_thumbnail_url=url.invalid&url=some_url' \
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
        self.assertEquals(rjson['error']['message'],
                          'failed to download thumbnail')

    @tornado.testing.gen_test
    def test_post_video_with_duration(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&duration=1354&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&duration=1354.54&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-08-18T06:36:40.123Z&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-08-18T06:36:40Z&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-08-18&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        with self.assertRaises(tornado.httpclient.HTTPError) as e:  
            url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&publish_date=2015-0&url=some_url' % (self.account_id_api_key,                                                      self.test_i_id)
            self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='POST',
                                                    allow_nonstandard_methods=True)
        self.assertEquals(e.exception.response.code, 400) 

    @tornado.testing.gen_test
    def test_post_video_missing_url(self):
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&title=a_title' % (self.account_id_api_key, self.test_i_id)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='POST',
                                                    allow_nonstandard_methods=True)
        self.assertEquals(e.exception.response.code, 400)

    @tornado.testing.gen_test
    def test_post_url_and_reprocess(self):
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&title=a_title&url=some_url&reprocess=True' % (self.account_id_api_key, self.test_i_id)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='POST',
                                                    allow_nonstandard_methods=True)
        self.assertEquals(e.exception.response.code, 400)

    @tornado.testing.gen_test
    def test_post_video_with_custom_data(self):
        custom_data = urllib.quote(json.dumps({ "a" : 123456 }))
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&custom_data=%s&url=some_url' % (self.account_id_api_key, self.test_i_id, custom_data)
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

    def test_post_video_with_bad_custom_data(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&custom_data=%s&url=some_url' % (self.account_id_api_key, self.test_i_id, 4)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        self.http_client.fetch(self.get_url(url),
                               body='',
                               method='POST',
                               callback=self.stop, 
                               allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 400) 
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'not a dictionary') 

    def test_post_two_videos(self):
        # use self.stop/wait to make sure we get the response back and 
        # not an exception, we don't want no exception
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&url=some_url' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop,
                               body='',
                               method='POST',
                              allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 409) 
        rjson = json.loads(response.body) 
        data = rjson['error']['data'] 
        self.assertTrue(first_job_id in data)

    def test_post_two_videos_with_reprocess(self):
        # use self.stop/wait to make sure we get the response back and 
        # not an exception, we don't want no exception
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&reprocess=true' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,200))
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop,
                               body='',
                               method='POST',
                              allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 202) 
        rjson = json.loads(response.body)
        self.assertEquals(first_job_id, rjson['job_id'])

    def test_post_video_with_vserver_fail(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&url=some_url' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,400))
        self.http_client.fetch(self.get_url(url),
                               callback = self.stop, 
                               body='',
                               method='POST',
                               allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 500) 
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'Internal Server') 

    def test_post_two_videos_with_reprocess_fail(self):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&url=some_url' % (self.account_id_api_key, self.test_i_id)
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
        
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs&reprocess=1' % (self.account_id_api_key, self.test_i_id)
        self.http_mock.side_effect = lambda x, callback: callback(tornado.httpclient.HTTPResponse(x,400))
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop,
                               body='',
                               method='POST',
                              allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 500) 
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Internal Server Error') 

    @tornado.testing.gen_test
    def test_get_without_video_id(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
            vm.save()
            url = '/api/v2/%s/videos' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'key not provided') 

    @tornado.testing.gen_test
    def test_get_single_video(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key, 'vid1'),
            request_id='job1',
            i_id='int2',
            testing_enabled=False,
            duration=31.5,
            custom_data={'my_data' : 'happygo'},
            tids=['vid1_t1', 'vid1_t2'],
            video_url='http://someurl.com')
        vm.save()
        request = neondata.NeonApiRequest('job1', self.account_id_api_key,
                                          title='Title',
                                          publish_date='2015-06-10')
        request.state = neondata.RequestState.FINISHED
        request.save()
        url = '/api/v2/%s/videos?video_id=vid1' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals({
            'job_id': 'job1',
            'testing_enabled' : False,
            'url' : 'http://someurl.com',
            'state': neondata.ExternalRequestState.PROCESSED,
            'video_id': 'vid1',
            'publish_date' : '2015-06-10',
            'title' : 'Title'
            },
            rjson['videos'][0])

    @tornado.testing.gen_test
    def test_get_all_fields(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key, 'vid1'),
            request_id='job1',
            i_id='int2',
            testing_enabled=False,
            duration=31.5,
            custom_data={'my_data' : 'happygo'},
            tids=['vid1_t1', 'vid1_t2'])
        vm.save()
        thumbs = [neondata.ThumbnailMetadata('vid1_t1', 'vid1', ['here.com']),
                  neondata.ThumbnailMetadata('vid1_t2', 'vid1', ['there.com'])]
        neondata.ThumbnailMetadata.save_all(thumbs)
        request = neondata.NeonApiRequest('job1', self.account_id_api_key,
                                          title='Title',
                                          publish_date='2015-06-10')
        request.response = {'error': 'Some error'}
        request.state = neondata.RequestState.FINISHED
        request.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=state,integration_id,testing_enabled,job_id,title,video_id,serving_url,publish_date,thumbnails,duration,custom_data' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertDictContainsSubset({
            'state': neondata.ExternalRequestState.PROCESSED,
            'integration_id': 'int2',
            'testing_enabled': False,
            'job_id': 'job1',
            'title' : 'Title',
            'video_id': 'vid1',
            'serving_url' : None,
            'publish_date' : '2015-06-10',
            'duration' : 31.5,
            'custom_data' : {'my_data' : 'happygo'},
            'error' : 'Some error'
            },
            rjson['videos'][0])
        tids = [x['thumbnail_id'] for x in rjson['videos'][0]['thumbnails']]
        self.assertItemsEqual(tids, ['vid1_t1', 'vid1_t2'])

    @tornado.testing.gen_test
    def test_get_two_videos(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1'),
            request_id='job1')
        vm.save()
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid2'),
            request_id='job2')
        vm.save()
        neondata.NeonApiRequest('job2', self.account_id_api_key).save()
        url = '/api/v2/%s/videos?video_id=vid1,vid2' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test
    def test_get_two_videos_one_dne(self):
        vm1 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'),
            request_id='job1')
        vm1.save()
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key,'vid2'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1,viddoesnotexist' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 1)
        videos = rjson['videos']
        self.assertEquals(videos[0]['video_id'], 'vid1') 
 
    def test_get_video_dne(self):
        url = '/api/v2/%s/videos?video_id=viddoesnotexist' % (self.account_id_api_key)
        response = self.http_client.fetch(self.get_url(url),
                                          self.stop, 
                                          method='GET')
       
        response = self.wait()
        self.assertEquals(response.code, 404)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'do not exist .* viddoesnotexist') 

    def test_get_multiple_video_dne(self):
        url = '/api/v2/%s/videos?video_id=viddoesnotexist,vidoerwe,w3asdfa324ad' % (self.account_id_api_key)
        response = self.http_client.fetch(self.get_url(url),
                                          self.stop, 
                                          method='GET')
       
        response = self.wait()
        self.assertEquals(response.code, 404)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 r'do not exist with id') 

    @tornado.testing.gen_test
    def test_get_single_video_with_fields(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'),
            request_id='job1')
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')

        rjson = json.loads(response.body)
        videos = rjson['videos']
        self.assertEquals(response.code, 200)

        self.assertIsNotNone(videos[0]['created']) 
        self.assertNotIn('error', videos[0])
        self.assertItemsEqual(videos[0].keys(), ['created'])
    
    @tornado.testing.gen_test
    def test_get_single_video_with_invalid_fields(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'),
            request_id='job1')
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created,me_is_invalid' % (self.account_id_api_key)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
        self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'invalid field') 

    @tornado.testing.gen_test(timeout=10.0)
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
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'), 
            tids=['testing_vtid_one', 'testing_vtid_two'],
            request_id='job1')
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
        self.assertEquals(thumbnail_one['thumbnail_id'], 'testing_vtid_one') 
        self.assertEquals(thumbnail_two['width'], 500)
        self.assertEquals(thumbnail_two['thumbnail_id'], 'testing_vtid_two') 
    
    @tornado.testing.gen_test 
    def test_get_video_with_thumbnails_field_no_thumbnails(self): 
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.account_id_api_key,'vid1'), 
                tids=[],
            request_id='job1')
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created,thumbnails' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        thumbnail_array = rjson['videos'][0]['thumbnails']
        self.assertEquals(len(rjson['videos']), 1)
        self.assertEquals(len(thumbnail_array), 0)
    
    @tornado.testing.gen_test
    def test_update_video_does_not_exist(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?video_id=vid_does_not_exist&testing_enabled=0' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='PUT', 
                                                    allow_nonstandard_methods=True)
        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'vid_does_not_exist') 

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
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        neondata.ThumbnailMetadata('testingtid', width=500, urls=['s']).save()
        self.test_video = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,
                             'tn_test_vid1')).save()
        neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,
                             'tn_test_vid2')).save()

        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]
        self.im_download_mocker = patch(
            'cvutils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [self.random_image] 
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestThumbnailHandler, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
        self.verify_account_mocker.stop()
        super(TestThumbnailHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @tornado.testing.gen_test
    def test_add_new_thumbnail(self):
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid1&url=blah.jpg' % (self.account_id_api_key)
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
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid2&url=blah.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202)
        self.im_download_mock.side_effect = [self.random_image] 
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid2&url=blah2.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202)
        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,'tn_test_vid2')
        video = neondata.VideoMetadata.get(internal_video_id)
        thumbnail_ids = video.thumbnail_ids
        self.assertEquals(len(video.thumbnail_ids), 2)
        #for tid in thumbnail_ids: 
            

    @tornado.testing.gen_test
    def test_get_thumbnail_exists(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (
            self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['width'], 500)
        self.assertEquals(rjson['thumbnail_id'], 'testingtid')

    @tornado.testing.gen_test
    def test_get_thumbnail_does_not_exist(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = ('/api/v2/%s/thumbnails?thumbnail_id=testingtiddoesnotexist'
                   % (self.account_id_api_key))
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'does not exist') 

    @tornado.testing.gen_test
    def test_thumbnail_update_enabled(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (
            self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        old_tn = json.loads(response.body)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid&enabled=0' % (
            self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body)
        self.assertEquals(new_tn['enabled'],False)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid&enabled=1' % (
            self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body) 
        self.assertEquals(new_tn['enabled'],True)

    @tornado.testing.gen_test
    def test_thumbnail_update_no_params(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (
            self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        old_tn = json.loads(response.body)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (
            self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body) 
        self.assertEquals(response.code, 200)
        self.assertEquals(new_tn['enabled'],old_tn['enabled'])

    @tornado.testing.gen_test
    def test_delete_thumbnail_not_implemented(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/thumbnails?thumbnail_id=12234' % (
                self.account_id_api_key)  
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='DELETE')
	    self.assertEquals(e.excpetion.code, 501) 
 
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
        self.http_mocker = patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
              self.http_mocker.start()) 
        super(TestHealthCheckHandler, self).setUp()

    def tearDown(self): 
        conn = neondata.DBConnection.get(neondata.VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(neondata.ThumbnailMetadata)
        conn.clear_db()
        self.http_mocker.stop()
        super(TestHealthCheckHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()
        super(TestHealthCheckHandler, cls).tearDownClass() 
 
    def test_healthcheck_success(self): 
        self.http_mock.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x, 200))
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
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'Internal Server') 

class TestVideoStatsHandler(TestControllersBase): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        self.defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True)
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestVideoStatsHandler, self).setUp()

    def tearDown(self): 
        conn = neondata.DBConnection.get(neondata.VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(neondata.ThumbnailMetadata)
        conn.clear_db()
        self.verify_account_mocker.stop()
        super(TestVideoStatsHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()
        super(TestVideoStatsHandler, cls).tearDownClass() 
    
    @tornado.testing.gen_test
    def test_one_video_id(self): 
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'), 
                                    tids=[])
        vm.save()
        vid_status = neondata.VideoStatus(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'),
                                          experiment_state=neondata.ExperimentState.COMPLETE)
        vid_status.winner_tid = '%s_t2' % neondata.InternalVideoID.generate(self.account_id_api_key,'vid1')
        vid_status.save()

        url = '/api/v2/%s/stats/videos?video_id=vid1' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['count'], 1)
        statistic_one = rjson['statistics'][0] 
        self.assertEquals(statistic_one['experiment_state'], neondata.ExperimentState.COMPLETE) 
 
    @tornado.testing.gen_test
    def test_two_video_ids(self): 
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1'), 
                                    tids=[])
        vm.save()
        vid_status = neondata.VideoStatus(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1'),
            experiment_state=neondata.ExperimentState.COMPLETE)
        vid_status.winner_tid = '%s_t2' % neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1')
        vid_status.save()
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid2'), 
                                    tids=[])
        vm.save()
        vid_status = neondata.VideoStatus(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid2'),
            experiment_value_remaining=50, 
            experiment_state=neondata.ExperimentState.RUNNING)
        vid_status.winner_tid = '%s_t2' % neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid2')
        vid_status.save()

        url = '/api/v2/%s/stats/videos?video_id=vid1,vid2' % (
            self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['count'], 2)
        statistic_one = rjson['statistics'][0] 
        self.assertEquals(statistic_one['experiment_state'],
                          neondata.ExperimentState.COMPLETE)  
        statistic_two = rjson['statistics'][1] 
        self.assertEquals(statistic_two['experiment_state'],
                          neondata.ExperimentState.RUNNING) 

    @tornado.testing.gen_test
    def test_one_video_id_dne(self): 
        url = '/api/v2/%s/stats/videos?video_id=does_not_exist' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)

    def test_no_video_id(self): 
        url = '/api/v2/%s/stats/videos' % (self.account_id_api_key) 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait() 
        self.assertEquals(response.code, 400)

class TestVideoStatsHandlerPG(TestVideoStatsHandler): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        self.defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True)
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestVideoStatsHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()  
        self.postgresql.clear_all_tables()
        super(TestVideoStatsHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
 
class TestThumbnailStatsHandler(TestControllersBase): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        self.defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True)
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        neondata.ThumbnailMetadata('testingtid', width=800).save()
        neondata.ThumbnailMetadata('testing_vtid_one', width=500).save()
        neondata.ThumbnailMetadata('testing_vtid_two', width=500).save()
        super(TestThumbnailStatsHandler, self).setUp()

    def tearDown(self): 
        conn = neondata.DBConnection.get(neondata.VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(neondata.ThumbnailMetadata)
        conn.clear_db()
        self.verify_account_mocker.stop()
        super(TestThumbnailStatsHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()

    @tornado.testing.gen_test
    def test_account_id_video_id(self): 
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'), 
                                    tids=['testingtid','testing_vtid_one'])
        vm.save()
        ts = neondata.ThumbnailStatus('testingtid', serving_frac=0.8, ctr=0.23)
        ts.save() 
        ts = neondata.ThumbnailStatus('testing_vtid_one', serving_frac=0.3, ctr=0.12)
        ts.save() 
        url = '/api/v2/%s/stats/thumbnails?video_id=vid1,vid2' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 2)
        status_one = rjson['statistics'][0]  
        status_two = rjson['statistics'][1] 
        self.assertEquals(status_one['ctr'], 0.23)
        self.assertEquals(status_two['ctr'], 0.12)

    @tornado.testing.gen_test
    def test_account_id_video_id_dne(self): 
        url = '/api/v2/%s/stats/thumbnails?video_id=does_not_exist' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200) 
        self.assertEquals(rjson['count'], 0) 
        self.assertEquals(len(rjson['statistics']), 0) 

    @tornado.testing.gen_test
    def test_account_id_thumbnail_id(self): 
        ts = neondata.ThumbnailStatus('testingtid', serving_frac=0.8, ctr=0.23)
        ts.save() 
        url = '/api/v2/%s/stats/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 1)
        status_one = rjson['statistics'][0] 
        self.assertEquals(status_one['ctr'], 0.23)

    @tornado.testing.gen_test
    def test_account_id_multiple_thumbnail_ids(self): 
        ts = neondata.ThumbnailStatus('testingtid', serving_frac=0.8, ctr=0.23)
        ts.save() 
        ts = neondata.ThumbnailStatus('testing_vtid_one', serving_frac=0.3, ctr=0.12)
        ts.save() 
        url = '/api/v2/%s/stats/thumbnails?thumbnail_id=testingtid,testing_vtid_one' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 2)
        status_one = rjson['statistics'][0]  
        status_two = rjson['statistics'][1] 
        self.assertEquals(status_one['ctr'], 0.23)
        self.assertEquals(status_two['ctr'], 0.12)
        
        # test url encoded 
        encoded_params = urllib.urlencode({ 'thumbnail_id' : 'testingtid,testing_vtid_one' })
        self.assertEquals('thumbnail_id=testingtid%2Ctesting_vtid_one', encoded_params) 
        url = '/api/v2/%s/stats/thumbnails?%s' % (self.account_id_api_key, encoded_params) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 2)
        status_one = rjson['statistics'][0]  
        status_two = rjson['statistics'][1] 
        self.assertEquals(status_one['ctr'], 0.23)
        self.assertEquals(status_two['ctr'], 0.12)

    def test_video_id_limit(self): 
        url = '/api/v2/%s/stats/thumbnails?video_id=1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o' % (self.account_id_api_key) 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'list exceeds limit') 

    def test_video_id_and_thumbnail_id(self): 
        url = '/api/v2/%s/stats/thumbnails?video_id=1&thumbnail_id=abc' % (self.account_id_api_key) 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'you can only have') 

    def test_no_video_id_or_thumbnail_id(self): 
        url = '/api/v2/%s/stats/thumbnails' % (self.account_id_api_key) 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'thumbnail_id or video_id is required') 

class TestThumbnailStatsHandlerPG(TestControllersBase): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        self.defop = neondata.BrightcoveIntegration.modify(self.test_i_id, lambda x: x, create_missing=True)
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        neondata.ThumbnailMetadata('testingtid', width=800).save()
        neondata.ThumbnailMetadata('testing_vtid_one', width=500).save()
        neondata.ThumbnailMetadata('testing_vtid_two', width=500).save()
        super(TestThumbnailStatsHandlerPG, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()  
        self.postgresql.clear_all_tables()
        super(TestThumbnailStatsHandlerPG, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()

    @tornado.testing.gen_test
    def test_account_id_video_id(self): 
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'), 
                                    tids=['testingtid','testing_vtid_one'])
        vm.save()
        ts = neondata.ThumbnailStatus('testingtid', serving_frac=0.8, ctr=0.23)
        ts.save() 
        ts = neondata.ThumbnailStatus('testing_vtid_one', serving_frac=0.3, ctr=0.12)
        ts.save() 
        url = '/api/v2/%s/stats/thumbnails?video_id=vid1,vid2' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 2)
        status_one = rjson['statistics'][0]  
        status_two = rjson['statistics'][1] 
        self.assertEquals(status_one['ctr'], 0.23)
        self.assertEquals(status_two['ctr'], 0.12)

    @tornado.testing.gen_test
    def test_account_id_video_id_dne(self): 
        url = '/api/v2/%s/stats/thumbnails?video_id=does_not_exist' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200) 
        self.assertEquals(rjson['count'], 0) 
        self.assertEquals(len(rjson['statistics']), 0) 

    @tornado.testing.gen_test
    def test_account_id_thumbnail_id(self): 
        ts = neondata.ThumbnailStatus('testingtid', serving_frac=0.8, ctr=0.23)
        ts.save() 
        url = '/api/v2/%s/stats/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 1)
        status_one = rjson['statistics'][0] 
        self.assertEquals(status_one['ctr'], 0.23)

    @tornado.testing.gen_test
    def test_account_id_multiple_thumbnail_ids(self): 
        ts = neondata.ThumbnailStatus('testingtid', serving_frac=0.8, ctr=0.23)
        ts.save() 
        ts = neondata.ThumbnailStatus('testing_vtid_one', serving_frac=0.3, ctr=0.12)
        ts.save() 
        url = '/api/v2/%s/stats/thumbnails?thumbnail_id=testingtid,testing_vtid_one' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 2)
        status_one = rjson['statistics'][0]  
        status_two = rjson['statistics'][1] 
        self.assertEquals(status_one['ctr'], 0.23)
        self.assertEquals(status_two['ctr'], 0.12)
        
        # test url encoded 
        encoded_params = urllib.urlencode({ 'thumbnail_id' : 'testingtid,testing_vtid_one' })
        self.assertEquals('thumbnail_id=testingtid%2Ctesting_vtid_one', encoded_params) 
        url = '/api/v2/%s/stats/thumbnails?%s' % (self.account_id_api_key, encoded_params) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['count'], 2)
        status_one = rjson['statistics'][0]  
        status_two = rjson['statistics'][1] 
        self.assertEquals(status_one['ctr'], 0.23)
        self.assertEquals(status_two['ctr'], 0.12)

    def test_video_id_limit(self): 
        url = '/api/v2/%s/stats/thumbnails?video_id=1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o' % (self.account_id_api_key) 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'list exceeds limit') 

    def test_video_id_and_thumbnail_id(self): 
        url = '/api/v2/%s/stats/thumbnails?video_id=1&thumbnail_id=abc' % (self.account_id_api_key) 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'you can only have') 

    def test_no_video_id_or_thumbnail_id(self): 
        url = '/api/v2/%s/stats/thumbnails' % (self.account_id_api_key) 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'thumbnail_id or video_id is required') 

class TestAPIKeyRequired(TestControllersBase, TestAuthenticationBase):
    def setUp(self):
        self.neon_user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingaccount')
        self.neon_user.save() 
        super(TestAPIKeyRequired, self).setUp()

    def tearDown(self):
        self.postgresql.clear_all_tables()
        super(TestAPIKeyRequired, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    def make_calls_and_assert_401(self, 
                                  url, 
                                  method, 
                                  body_params='', 
                                  message=None): 
        self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               body=body_params, 
                               method=method, 
                               allow_nonstandard_methods=True)
        response = self.wait()
        self.assertEquals(response.code, 401)
        if message: 
            rjson = json.loads(response.body)
            self.assertEquals(rjson['error']['message'], message) 

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
            self.make_calls_and_assert_401(url, method, message='account does not exist')
 
    def test_urls_with_valid_account(self):
        urls = [ 
                 ('/api/v2/%s' % self.neon_user.neon_api_key, 'GET'),  
                 ('/api/v2/%s' % self.neon_user.neon_api_key, 'PUT'),  
                 ('/api/v2/%s/integrations/brightcove' % self.neon_user.neon_api_key, 'GET'),  
                 ('/api/v2/%s/integrations/brightcove' % self.neon_user.neon_api_key, 'PUT'),  
                 ('/api/v2/%s/integrations/brightcove' % self.neon_user.neon_api_key, 'POST'),  
                 ('/api/v2/%s/integrations/ooyala' % self.neon_user.neon_api_key, 'GET'),  
                 ('/api/v2/%s/integrations/ooyala' % self.neon_user.neon_api_key, 'PUT'),  
                 ('/api/v2/%s/integrations/ooyala' % self.neon_user.neon_api_key, 'POST'),  
                 ('/api/v2/%s/videos' % self.neon_user.neon_api_key, 'GET'),  
                 ('/api/v2/%s/videos' % self.neon_user.neon_api_key, 'PUT'),  
                 ('/api/v2/%s/videos' % self.neon_user.neon_api_key, 'POST'),  
                 ('/api/v2/%s/thumbnails' % self.neon_user.neon_api_key, 'GET'),  
                 ('/api/v2/%s/thumbnails' % self.neon_user.neon_api_key, 'PUT'),  
                 ('/api/v2/%s/thumbnails' % self.neon_user.neon_api_key, 'POST')  
               ]  
        for url, method in urls:
            self.make_calls_and_assert_401(
                url, method, message='this endpoint requires an access token')

    def test_with_invalid_token_bad_secret_qs(self):
        user = neondata.User(username='testuser', 
                             password='testpassword')
        
        token = jwt.encode({  
                             'username': 'testuser',
                             'exp' : datetime.utcnow() +
                                     timedelta(seconds=324234)
                           },
                           'iisabadsecret',
                           algorithm='HS256')
        user.access_token = token 
        user.save()

        urls = [ 
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST')
               ]  

        for url, method in urls:
            self.make_calls_and_assert_401(url, method, message='invalid token')
    
    def test_with_valid_token_wrong_access_level(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.READ)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        urls = [ 
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST')
               ]  
        for url, method in urls:
            self.make_calls_and_assert_401(url, method, message='you can not access this resource')

    def test_with_valid_token_wrong_access_level_nua_level(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.READ)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        urls = [ 
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST')
               ]  
        for url, method in urls:
            self.make_calls_and_assert_401(url, method, message='you can not access this resource')

    def test_401_with_expired_token(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.READ)
        
        token = JWTHelper.generate_token({'username' : 'testuser', 'exp' : -1 }) 
        user.access_token = token 
        user.save()
        urls = [ 
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'GET'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'PUT'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, user.access_token), 'POST')
               ] 
 
        for url, method in urls:
            self.make_calls_and_assert_401(url, 
                                           method, 
                                           message='access token is expired, please refresh the token')

    def test_401_with_not_valid_user(self):
        access_token = JWTHelper.generate_token({'username' : 'testuser'}) 
        urls = [ 
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, access_token), 'GET'),
                 ('/api/v2/%s?token=%s' % (self.neon_user.neon_api_key, access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, access_token), 'GET'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, access_token), 'PUT'),
                 ('/api/v2/%s/integrations/brightcove?token=%s' % (self.neon_user.neon_api_key, access_token), 'POST'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, access_token), 'GET'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, access_token), 'PUT'),
                 ('/api/v2/%s/integrations/ooyala?token=%s' % (self.neon_user.neon_api_key, access_token), 'POST'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, access_token), 'GET'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, access_token), 'PUT'),
                 ('/api/v2/%s/videos?token=%s' % (self.neon_user.neon_api_key, access_token), 'POST'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, access_token), 'GET'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, access_token), 'PUT'),
                 ('/api/v2/%s/thumbnails?token=%s' % (self.neon_user.neon_api_key, access_token), 'POST')
               ] 
 
        for url, method in urls:
            self.make_calls_and_assert_401(url, 
                                           method, 
                                           message='user does not exist')
    
    @tornado.testing.gen_test 
    def test_create_brightcove_integration_god_mode(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.GLOBAL_ADMIN)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()

        params = json.dumps({'publisher_id': '123123abc', 'token': token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.neon_user.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        self.assertEquals(response.code, 200) 
        
    @tornado.testing.gen_test 
    def test_create_brightcove_integration_create_mode(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.CREATE)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        params = json.dumps({'publisher_id': '123123abc', 'token' : token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.neon_user.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        self.assertEquals(response.code, 200)

    def test_create_brightcove_integration_read_mode(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.READ)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        params = json.dumps({'publisher_id': '123123abc', 'token' : token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.neon_user.neon_api_key)
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop,
                               headers=header)
        response = self.wait() 
        self.assertEquals(response.code, 401)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'you can not access') 
 
    @tornado.testing.gen_test 
    def test_create_brightcove_integration_all_normal_mode(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        self.neon_user.users.append('testuser')
        self.neon_user.save() 
        params = json.dumps({'publisher_id': '123123abc', 'token' : token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.neon_user.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        self.assertEquals(response.code, 200)
 
    @tornado.testing.gen_test 
    def test_internal_search_access_level_normal(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        url = '/api/v2/videos/search?token=%s' % (token)
        # should get a 401 unauth, because this is an internal only 
        # resource, and this is not an internal_only user  
        with self.assertRaises(tornado.httpclient.HTTPError) as e: 
            yield self.http_client.fetch(self.get_url(url), 
                                          method='GET')

        self.assertEquals(e.exception.code, 401)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'internal only resource')

    @tornado.testing.gen_test 
    def test_internal_search_access_level_internal_only(self): 
        user = neondata.User(
            username='testuser', 
            password='testpassword', 
            access_level=neondata.AccessLevels.INTERNAL_ONLY_USER | 
                         neondata.AccessLevels.ALL_NORMAL_RIGHTS)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()
        url = '/api/v2/videos/search?token=%s' % (token)
        # should get a 200 as this user has access to this resource 
        # resource, and this is not an internal_only user  
        response = yield self.http_client.fetch(self.get_url(url), 
                       method='GET')

        self.assertEquals(response.code, 200)

class TestAPIKeyRequiredAuth(TestAuthenticationBase):
    def setUp(self):
        self.neon_user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingaccount')
        self.neon_user.save() 
        super(TestAPIKeyRequiredAuth, self).setUp()

    def tearDown(self):
        self.postgresql.clear_all_tables()
        super(TestAPIKeyRequiredAuth, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()

    @tornado.testing.gen_test 
    def test_create_new_account_god_mode(self): 
        user = neondata.User(username='testuser', 
                             password='testpassword', 
                             access_level=neondata.AccessLevels.GLOBAL_ADMIN)
        
        token = JWTHelper.generate_token({'username' : 'testuser'})  
        user.access_token = token 
        user.save()

        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.bc', 
                             'admin_user_username':'a@a.com', 
                             'admin_user_password':'testacpas', 
                             'token' : token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/accounts'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
        self.assertEquals(response.code, 200)

        params = json.dumps({'customer_name': 'meisnew', 
                             'email': 'a@a.bc', 
                             'admin_user_username':'bb@a.com', 
                             'admin_user_password':'testacpas'})
        header = { 
                   'Content-Type':'application/json', 
                   'Authorization': 'Bearer %s' % token 
                 }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header) 
        self.assertEquals(response.code, 200)

        url = '/api/v2/accounts?customer_name=meisnew&email=a@a.com'\
              '&admin_user_username=a123@a.com'\
              '&admin_user_password=abc123456&token=%s' % token
        response = yield self.http_client.fetch(self.get_url(url),
                                                allow_nonstandard_methods=True,
                                                body='', 
                                                method='POST') 
	self.assertEquals(response.code, 200)

class TestAuthenticationHandler(TestAuthenticationBase): 
    def setUp(self): 
        TestAuthenticationHandler.username = 'kevin' 
        TestAuthenticationHandler.password = '12345678'
        TestAuthenticationHandler.first_name = 'kevin'
        TestAuthenticationHandler.last_name = 'keviniii'
        TestAuthenticationHandler.title = 'blah' 
        self.user = neondata.User(username=TestAuthenticationHandler.username, 
            password=TestAuthenticationHandler.password, 
            first_name=TestAuthenticationHandler.first_name,
            last_name=TestAuthenticationHandler.last_name,
            title=TestAuthenticationHandler.title)
        self.user.save()
        super(TestAuthenticationHandler, self).setUp()

    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(TestAuthenticationHandler, self).tearDown()
 
    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()

    def test_no_username(self): 
        url = '/api/v2/authenticate' 
        params = json.dumps({'password': '123123abc'})
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop, 
                               headers=header)
        response = self.wait() 
        rjson = json.loads(response.body) 
        self.assertEquals(response.code, 400)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'required key not.*username')

    def test_no_password(self): 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': '123123abc'})
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop, 
                               headers=header)
        response = self.wait() 
        rjson = json.loads(response.body) 
        self.assertEquals(response.code, 400)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'required key not.*password')

    def test_invalid_user_dne(self):
        url = '/api/v2/authenticate'
        params = json.dumps({
            'username': 'abc',
            'password':TestAuthenticationHandler.password})
        header = {'Content-Type':'application/json' }
            
        self.http_client.fetch(self.get_url(url),
                               body=params,
                               method='POST',
                               callback=self.stop,
                               headers=header)
        response =self.wait()

        rjson = json.loads(response.body)
        self.assertEquals(response.code, 401)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'User is Not Authorized')

    def test_invalid_user_wrong_password(self):
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestAuthenticationHandler.username,
                             'password': 'notvalidpw'})
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop, 
                               headers=header)
        response = self.wait() 
        rjson = json.loads(response.body) 
        self.assertEquals(response.code, 401)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'User is Not Authorized')

    @tornado.testing.gen_test
    def test_token_returned(self): 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestAuthenticationHandler.username, 
                             'password': TestAuthenticationHandler.password }) 
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        user = yield neondata.User.get(
            TestAuthenticationHandler.username, 
            async=True)
        self.assertEquals(user.access_token, rjson['access_token'])
        self.assertEquals(user.refresh_token, rjson['refresh_token'])
        user_info = rjson['user_info'] 
        self.assertEquals(user_info['first_name'],
            TestAuthenticationHandler.first_name) 
        self.assertEquals(user_info['last_name'],
            TestAuthenticationHandler.last_name) 
        self.assertEquals(user_info['title'],
            TestAuthenticationHandler.title) 
 
    @tornado.testing.gen_test
    def test_token_changed(self):  
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestAuthenticationHandler.username, 
                             'password': TestAuthenticationHandler.password})
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        token1 = rjson['access_token'] 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        token2 = rjson['access_token'] 
        self.assertNotEquals(token1, token2)

    @tornado.testing.gen_test 
    def test_account_ids_returned_single(self): 
        new_account_one = neondata.NeonUserAccount('test_account1')
        new_account_one.users.append(self.user.username)
        yield new_account_one.save(async=True)
 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestAuthenticationHandler.username, 
                             'password': TestAuthenticationHandler.password})
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        account_ids = rjson['account_ids'] 
        self.assertEquals(1, len(account_ids)) 
        a_id = account_ids[0] 
        self.assertEquals(a_id, new_account_one.neon_api_key)
 
    @tornado.testing.gen_test 
    def test_account_ids_returned_multiple(self): 
        new_account_one = neondata.NeonUserAccount('test_account1')
        new_account_one.users.append(self.user.username)
        yield new_account_one.save(async=True)

        new_account_two = neondata.NeonUserAccount('test_account2')
        new_account_two.users.append(self.user.username)
        yield new_account_two.save(async=True)
 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestAuthenticationHandler.username, 
                             'password': TestAuthenticationHandler.password})
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        account_ids = rjson['account_ids'] 
        self.assertEquals(2, len(account_ids)) 
        self.assertTrue(new_account_one.neon_api_key in account_ids) 
        self.assertTrue(new_account_two.neon_api_key in account_ids)
 
    @tornado.testing.gen_test 
    def test_account_ids_returned_empty(self): 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestAuthenticationHandler.username, 
                             'password': TestAuthenticationHandler.password})
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        account_ids = rjson['account_ids'] 
        account_ids = rjson['account_ids'] 
        self.assertEquals(0, len(account_ids)) 

class TestRefreshTokenHandler(TestAuthenticationBase): 
    def setUp(self): 
        self.refresh_token_exp = options.get('cmsapiv2.apiv2.refresh_token_exp') 
        super(TestRefreshTokenHandler, self).setUp()
         
    def tearDown(self): 
        options._set('cmsapiv2.apiv2.refresh_token_exp', self.refresh_token_exp)
        super(TestRefreshTokenHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)
        TestRefreshTokenHandler.username = 'kevin' 
        TestRefreshTokenHandler.password = '12345678'
        cls.user = neondata.User(username=TestRefreshTokenHandler.username, 
                             password=TestRefreshTokenHandler.password)
        cls.user.save()

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.clear_all_tables()
        cls.postgresql.stop()
 

    def test_no_token(self): 
        url = '/api/v2/refresh_token' 
        params = json.dumps({})
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop, 
                               headers=header)
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'required key not')

    def test_refresh_token_expired(self):
        refresh_token_exp = options.get('cmsapiv2.apiv2.refresh_token_exp')
        options._set('cmsapiv2.apiv2.refresh_token_exp', -1) 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestRefreshTokenHandler.username, 
                             'password': TestRefreshTokenHandler.password})
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop, 
                               headers=header)
        response = self.wait() 
        rjson = json.loads(response.body)
        refresh_token = rjson['refresh_token']
        url = '/api/v2/refresh_token' 
        params = json.dumps({'token': refresh_token }) 
        time.sleep(1.0) 
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop, 
                               headers=header)

        response = self.wait()
        rjson = json.loads(response.body)
        self.assertEquals(
            rjson['error']['message'],
            'refresh token has expired, please authenticate again') 
        self.assertEquals(response.code, 401) 
        options._set('cmsapiv2.apiv2.refresh_token_exp', refresh_token_exp) 

    @tornado.testing.gen_test 
    def test_get_new_access_token(self):
        new_account_one = neondata.NeonUserAccount('test_account1')
        new_account_one.users.append(self.user.username)
        yield new_account_one.save(async=True)
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestRefreshTokenHandler.username, 
                             'password': TestRefreshTokenHandler.password})
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson1 = json.loads(response.body)
        refresh_token = rjson1['refresh_token']
        url = '/api/v2/refresh_token' 
        params = json.dumps({'token': refresh_token })  
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson2 = json.loads(response.body)
        refresh_token2 = rjson2['refresh_token']
        self.assertEquals(refresh_token, refresh_token2) 
        account_ids = rjson2['account_ids'] 
        self.assertEquals(1, len(account_ids)) 
        user = yield tornado.gen.Task(neondata.User.get, 
            TestRefreshTokenHandler.username)
        # verify that the access_token was indeed updated 
        self.assertNotEquals(user.access_token, rjson1['access_token'])
        self.assertEquals(user.access_token, rjson2['access_token'])
        # verify refresh tokens stay the same 
        self.assertEquals(user.refresh_token, rjson1['refresh_token'])

class TestLogoutHandler(TestAuthenticationBase): 
    def setUp(self): 
        super(TestLogoutHandler, self).setUp()
    def tearDown(self): 
        super(TestLogoutHandler, self).tearDown()
   
    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)
        TestLogoutHandler.username = 'kevin' 
        TestLogoutHandler.password = '12345678'
        user = neondata.User(username=TestLogoutHandler.username, 
                             password=TestLogoutHandler.password)
        user.save()

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.clear_all_tables()
        cls.postgresql.stop()

    def test_no_token(self): 
        url = '/api/v2/logout' 
        params = json.dumps({})
        header = { 'Content-Type':'application/json' }
        self.http_client.fetch(self.get_url(url), 
                               body=params, 
                               method='POST', 
                               callback=self.stop, 
                               headers=header)
        response = self.wait() 
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        self.assertRegexpMatches(rjson['error']['message'], 'key not provided')
    
    @tornado.testing.gen_test
    def test_proper_logout(self): 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestLogoutHandler.username, 
                             'password': TestLogoutHandler.password})
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        access_token = rjson['access_token'] 

        url = '/api/v2/logout' 
        params = json.dumps({'token': access_token })
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_logout_with_expired_token(self): 
        token_exp = options.get('cmsapiv2.apiv2.access_token_exp') 
        
        options._set('cmsapiv2.apiv2.access_token_exp', -1) 
        url = '/api/v2/authenticate' 
        params = json.dumps({'username': TestLogoutHandler.username, 
                             'password': TestLogoutHandler.password})
        header = { 'Content-Type':'application/json' }
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        access_token = rjson['access_token'] 
        url = '/api/v2/logout' 
        params = json.dumps({'token': access_token })
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body=params, 
                                                method='POST', 
                                                headers=header)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['message'],
                                 'logged out expired user')
        self.assertEquals(response.code, 200)
        options._set('cmsapiv2.apiv2.access_token_exp', token_exp) 

class TestAuthenticationHealthCheckHandler(TestAuthenticationBase): 
    def setUp(self):
        super(TestAuthenticationHealthCheckHandler, self).setUp()

    def tearDown(self): 
        conn = neondata.DBConnection.get(neondata.VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(neondata.ThumbnailMetadata)
        conn.clear_db()
        super(TestAuthenticationHealthCheckHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()
 
    def test_healthcheck_success(self): 
	url = '/healthcheck/'
        response = self.http_client.fetch(self.get_url(url),
                               callback=self.stop, 
                               method='GET')
        response = self.wait()
        self.assertEquals(response.code, 200)
 
class TestVideoSearchInternalHandler(TestControllersBase): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestVideoSearchInternalHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()  
        self.postgresql.clear_all_tables()
        super(TestVideoSearchInternalHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()

    @tornado.testing.gen_test 
    def test_search_no_videos(self):
        url = '/api/v2/videos/search?account_id=kevin&fields='\
              'video_id,title,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 0)
    
    @tornado.testing.gen_test 
    def test_search_base(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/videos/search?account_id=kevin&fields='\
              'video_id,title,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test 
    def test_search_get_newer_prev_page(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/videos/search?account_id=kevin&fields='\
              'video_id,title,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')

        video = neondata.VideoMetadata('kevin_vid3', request_id='job3')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job3', 'kevin', 
                  title='really kevins best video yet').save(async=True)
        rjson1 = json.loads(response.body)
        url = rjson1['prev_page'] 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 1)
        video = rjson['videos'][0]
        self.assertEquals('really kevins best video yet', video['title'])

    @tornado.testing.gen_test 
    def test_search_get_older_next_page(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/videos/search?account_id=kevin&fields='\
              'video_id,title,created,updated&limit=1'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')

        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 1)
        video = rjson['videos'][0]
        self.assertEquals('kevins best video yet', video['title'])

        url = rjson['next_page']
        url += '&limit=1'  
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 1)
        video = rjson['videos'][0]
        self.assertEquals('kevins video', video['title'])
        
    @tornado.testing.gen_test 
    def test_search_with_limit(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/videos/search?account_id=kevin&fields='\
              'video_id,title,created,updated&limit=1'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 1)
        video = rjson['videos'][0]
        # this should grab the most recently created video
        self.assertEquals('kevins best video yet', video['title'])

    @tornado.testing.gen_test 
    def test_search_without_account_id(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin2_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin2', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/videos/search?fields='\
              'video_id,title,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        # should return all the videos despite no account
        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test 
    def test_search_without_requests(self):
        video = neondata.VideoMetadata('kevin_vid1')
        yield video.save(async=True)   
        url = '/api/v2/videos/search?fields='\
              'video_id,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        video_count = rjson['video_count'] 
        videos = rjson['videos']
        self.assertEquals(video_count, 0) 
        self.assertEquals(videos, None) 
       
class TestVideoSearchExternalHandler(TestControllersBase): 
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestVideoSearchExternalHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()  
        self.postgresql.clear_all_tables()
        super(TestVideoSearchExternalHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()
    
    @tornado.testing.gen_test 
    def test_search_base(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/kevin/videos/search?fields='\
              'video_id,title,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test 
    def test_search_get_older_prev_page(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/kevin/videos/search?fields='\
              'video_id,title,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')

        video = neondata.VideoMetadata('kevin_vid3', request_id='job3')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job3', 'kevin', 
                  title='really kevins best video yet').save(async=True)
        rjson1 = json.loads(response.body)
        url = rjson1['prev_page'] 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 1)
        video = rjson['videos'][0]
        self.assertEquals('really kevins best video yet', video['title'])

    @tornado.testing.gen_test 
    def test_search_with_limit(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job1', 
            'kevin', 
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)   
        yield neondata.NeonApiRequest('job2', 
            'kevin', 
            title='kevins best video yet').save(async=True)
        url = '/api/v2/kevin/videos/search?fields='\
              'video_id,title,created,updated&limit=1'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 1)
        video = rjson['videos'][0]
        # this should grab the most recently created video
        self.assertEquals('kevins best video yet', video['title'])

class TestAccountLimitsHandler(TestControllersBase): 
    def setUp(self):
        self.user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        self.user.save()
        self.account_id_api_key = self.user.neon_api_key
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestAccountLimitsHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()  
        self.postgresql.clear_all_tables()
        super(TestAccountLimitsHandler, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0) 
        cls.postgresql.stop()

    @tornado.testing.gen_test 
    def test_search_with_limit(self):
        limits = neondata.AccountLimits(self.user.neon_api_key)
        yield limits.save(async=True)
 
        url = '/api/v2/%s/limits' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_posts'], 0)

if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
