#!/usr/bin/env python
# encoding: utf-8

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
from boto.sqs.message import Message
import boto.exception
from cmsapiv2.apiv2 import *
from cmsapiv2 import controllers
from cmsapiv2 import authentication
from datetime import datetime, timedelta
import json
import model
import logging
import numpy as np
import random
from requests_toolbelt import MultipartEncoder
import stripe
import tornado.gen
import tornado.testing
import tornado.httpclient
import test_utils.postgresql
import time
import unittest
import utils.neon
import utils.http
import urllib
import urlparse
import test_utils.neontest
from test_utils import sqsmock
import uuid
import jwt
from mock import patch, DEFAULT, MagicMock
from cmsdb import neondata
from passlib.hash import sha256_crypt
import PIL.Image
from StringIO import StringIO
from cvutils.imageutils import PILImageUtils
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
from tornado.httputil import HTTPServerRequest
import video_processor.video_processing_queue
from utils.options import options
from utils import statemon

define('run_stripe_on_test_account', default=0, type=int,
       help='If set, will run tests that hit the real Stripe APIs')

_log = logging.getLogger(__name__)

# Supress momoko debug.
logging.getLogger('momoko').setLevel(logging.INFO)

class TestBase(test_utils.neontest.AsyncHTTPTestCase):
    def setUp(self):
        self.send_email_mocker = patch(
            'cmsapiv2.authentication.AccountHelper.send_verification_email')
        self.send_email_mock = self._future_wrap_mock(
            self.send_email_mocker.start())
        self.send_email_mock.return_value = True
        self.send_email_mocker_two = patch(
            'cmsapiv2.authentication.ForgotPasswordHandler._send_email')
        self.send_email_mock_two = self._future_wrap_mock(
            self.send_email_mocker_two.start()) 
        self.send_email_mock_two.return_value = True

        # Mock out the aquila lookup
        self.aquila_conn_patcher = patch(
            'cmsapiv2.controllers.utils.autoscale')
        self.aquila_conn_patcher.start()
        
        super(TestBase, self).setUp()

    def tearDown(self):
        self.send_email_mocker.stop()
        self.send_email_mocker_two.stop()
        self.aquila_conn_patcher.stop()
        self.postgresql.clear_all_tables()
        super(TestBase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        super(TestBase, cls).tearDownClass()
        cls.max_io_loop_size = options.get(
            'cmsdb.neondata.max_io_loop_dict_size')
        options._set('cmsdb.neondata.max_io_loop_dict_size', 10)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()
        options._set('cmsdb.neondata.max_io_loop_dict_size',
            cls.max_io_loop_size)
        super(TestBase, cls).tearDownClass()

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

        exception_mock.side_effect = controllers.MultipleInvalid('blah blah')
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

class TestAPIV2(test_utils.neontest.AsyncHTTPTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_write_error_io_error(self):
        app = MagicMock()
        req = MagicMock()
        handler = APIV2Handler(app, req)
        handler.error = MagicMock()

        exc_info = (None, IOError())
        handler.write_error(500, exc_info=exc_info)
        handler.error.assert_called_with('', code=500)

        # Uses the message prop in exception.
        exc_info = (None, IOError('problem'))
        handler.write_error(500, exc_info=exc_info)
        handler.error.assert_called_with('problem', code=500)

        # Uses the errno in exception if set.
        exc_info = (None, IOError(501, 'with status code'))
        handler.write_error(500, exc_info=exc_info)
        handler.error.assert_called_with('with status code', code=501)

        # Uses the errno only if it is a valid http status code.
        exc_info = (None, IOError(4444, 'invalid status code'))
        handler.write_error(500, exc_info=exc_info)
        handler.error.assert_called_with('invalid status code', code=500)

        # Doesn't use a filename.
        exc_info = (None, IOError(401, 'filename ignored', 'ignore'))
        handler.write_error(500, exc_info=exc_info)
        handler.error.assert_called_with('filename ignored', code=401)

class TestAuthorizedControllerBase(TestControllersBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        super(TestAuthorizedControllerBase, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestAuthorizedControllerBase, self).tearDown()


class TestNewAccountHandler(TestAuthenticationBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestNewAccountHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestNewAccountHandler, self).tearDown()

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
        self.assertEquals(rjson['serving_enabled'], 1)
        account_id = rjson['account_id']
        account = yield neondata.NeonUserAccount.get(account_id,
                      async=True)
        self.assertEquals(account.name, 'meisnew')
        self.assertEquals(account.email, 'a@a.bc')
        self.assertEquals(account.serving_enabled, True)

        user = yield neondata.User.get('a@a.com',
                   async=True)
        self.assertEquals(user.username, 'a@a.com')
        self.assertEquals(user.first_name, 'kevin')
        self.assertEquals(user.last_name, 'fenger')
        self.assertEquals(user.title, 'Mr.')

        limits = yield neondata.AccountLimits.get(account_id, async=True)
        self.assertEquals(limits.key, account_id)
        self.assertEquals(limits.video_posts, 0)
        # An account with an email register has a certain video limit.
        expect_limit = neondata.AccountLimits.MAX_VIDEOS_ON_DEMO_SIGNUP
        self.assertEqual(expect_limit, limits.max_video_posts)

        exps = yield neondata.ExperimentStrategy.get(account_id, async=True)
        self.assertEquals(exps.get_id(), account_id)
        self.assertEquals(exps.exp_frac, 1.0)

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
        self.assertIsNotNone(verifier)

        url = '/api/v2/accounts/verify?token=%s' % verifier.token
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)


        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], 'meisnew')
        self.assertEquals(rjson['serving_enabled'], True)
        account_id = rjson['account_id']
        account = yield neondata.NeonUserAccount.get(account_id,
                      async=True)
        self.assertEquals(account.name, 'meisnew')
        self.assertEquals(account.email, 'a@a.bc')
        self.assertEquals(account.serving_enabled, True)

        user = yield neondata.User.get('a@a.com',
                   async=True)
        self.assertEquals(user.username, 'a@a.com')
        self.assertEquals(user.first_name, 'kevin')
        self.assertTrue(user.is_email_verified())

        limits = yield neondata.AccountLimits.get(account_id, async=True)
        self.assertEquals(limits.key, account_id)
        self.assertEquals(limits.video_posts, 0)
        expect_limit = neondata.AccountLimits.MAX_VIDEOS_ON_DEMO_SIGNUP
        self.assertEqual(expect_limit, limits.max_video_posts)

        exps = yield neondata.ExperimentStrategy.get(account_id, async=True)
        self.assertEquals(exps.get_id(), account_id)
        self.assertEquals(exps.exp_frac, 1.0)
        self.assertEquals(exps.holdback_frac, 0.05)

    @tornado.testing.gen_test
    def test_create_new_account_uppercase_username(self):
        params = json.dumps({'email': 'a@a.bc',
                             'admin_user_username':'A@A.com',
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
        self.assertEquals(rjson['customer_name'], None)
        self.assertEquals(rjson['serving_enabled'], True)
        account_id = rjson['account_id']
        account = yield neondata.NeonUserAccount.get(account_id,
                      async=True)
        self.assertEquals(account.name, None)
        self.assertEquals(account.email, 'a@a.bc')
        self.assertEquals(account.serving_enabled, True)

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
        self.assertRegexpMatches(rjson['error']['message'],
                                 'user with that email already exists')

    @tornado.testing.gen_test
    def test_create_new_account_without_email(self):
        url = '/api/v2/accounts'
        response = yield self.http_client.fetch(
            self.get_url(url),
            method='POST',
            allow_nonstandard_methods=True)
        body = json.loads(response.body)
        self.assertIn('account_ids', body)
        account_id = body['account_ids'][0]
        self.assertIn('access_token', body)
        self.assertIn('refresh_token', body)
        # Assert database-backed objects were saved.
        self.assertIsNotNone(neondata.NeonUserAccount.get(account_id))
        # One mapper for each of prod, stage.
        mappers = neondata.TrackerAccountIDMapper.get_all()
        self.assertEqual(2, len(mappers))
        (self.assertEqual(account_id, mapper.value) for mapper in mappers)

        # Check their limits against the demo plan.
        limits = neondata.AccountLimits.get(account_id)
        expect_plan = neondata.BillingPlans.get(neondata.BillingPlans.PLAN_DEMO)
        self.assertEqual(expect_plan.max_video_posts, expect_plan.max_video_posts)

        self.assertIsNotNone(neondata.ExperimentStrategy.get(account_id))
        account = neondata.NeonUserAccount.get(account_id)
        self.assertFalse(account.serving_enabled)

    @tornado.testing.gen_test
    def test_create_account_with_utf8_name(self):
        url = self.get_url('/api/v2/accounts/')
        username = 'a@a.bc'
        first_name = u'Lucía'
        last_name = u'Rolagár'
        body = json.dumps({'customer_name': 'meisnew',
                             'email': username,
                             'admin_user_username':'a@a.com',
                             'admin_user_password':'testacpas',
                             'admin_user_first_name': first_name,
                             'admin_user_last_name': last_name})
        headers = {'Content-Type':'application/json'}
        yield self.http_client.fetch(
                url,
                method='POST',
                headers=headers,
                body=body)
        verification = yield neondata.Verification.get(username, async=True)
        verif_user = json.loads(verification.extra_info['user'])
        self.assertEqual(first_name, verif_user['_data']['first_name'])
        self.assertEqual(last_name, verif_user['_data']['last_name'])


    @tornado.testing.gen_test
    def test_create_new_account_invalid_email(self):
        url = '/api/v2/accounts?customer_name=meisnew&email=aa.bc'\
              '&admin_user_username=abcd1234'\
              '&admin_user_password=b1234567'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url),
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
    def test_get_new_acct_not_implemented(self):
        with self.assertRaises(tornado.httpclient.HTTPError):
            url = '/api/v2/accounts'
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method="GET")


class TestAccountHandler(TestControllersBase):
    def setUp(self):
        self.user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingaccount')
        self.user.save()
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestAccountHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestAccountHandler, self).tearDown()

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
        self.assertEquals(1, rjson['serving_enabled'])

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


class TestAuthUserHandler(TestAuthenticationBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestAuthUserHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestAuthUserHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_create_new_user_query(self):
        username = 'abcd1234@gmail.com'
        url = '/api/v2/users?username=%s&password=b1234567&access_level=6' % username
        yield neondata.NeonUserAccount(None, 'a0').save(async=True)
        yield neondata.User('a0').save(async=True)
        token = JWTHelper.generate_token({
            'account_id': 'a0',
            'username': 'a0'})
        headers = {
            'Authorization': 'Bearer %s' % token
        }
        response = yield self.http_client.fetch(
            self.get_url(url),
            headers=headers,
            body='',
            method='POST')
        self.assertEquals(response.code, 200)
        self.assertIn('application/json', response.headers['Content-Type'])
        rjson = json.loads(response.body)
        self.assertIn('message', rjson)
        user = yield neondata.User.get(username, async=True)
        # User hasn't been created yet.
        self.assertIsNone(user)

        verification = yield neondata.Verification.get(username, async=True)
        account = json.loads(verification.extra_info['account'])
        self.assertEqual([username], account['users'])
        user = json.loads(verification.extra_info['user'])

    @tornado.testing.gen_test
    def test_create_new_user_json(self):
        username = 'abcd1234@gmail.com'
        params = json.dumps({
            'username': username,
            'password': 'b1234567',
            'access_level': '6',
            'cell_phone_number':'867-5309',
            'secondary_email':'rocking@invalid.com'})
        yield neondata.NeonUserAccount(None, 'a0').save(async=True)
        yield neondata.User('a0').save(async=True)
        token = JWTHelper.generate_token({
            'account_id': 'a0',
            'username': 'a0'})
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % token}
        url = '/api/v2/users'
        response = yield self.http_client.fetch(self.get_url(url),
                                                body=params,
                                                method='POST',
                                                headers=headers)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        user = yield neondata.User.get(username, async=True)
        self.assertIsNone(user)
        verification = yield neondata.Verification.get(username, async=True)
        account = json.loads(verification.extra_info['account'])
        self.assertEqual([username], account['users'])
        user = json.loads(verification.extra_info['user'])
        self.assertEqual('867-5309', user['_data']['cell_phone_number'])
        self.assertEqual('rocking@invalid.com', user['_data']['secondary_email'])

    @tornado.testing.gen_test
    def test_create_user_with_utf8(self):
        username = 'gianna@gmail.com'
        first_name = u'전지현'
        params = json.dumps({
            'username': username,
            'password': 'passw0rd',
            'first_name': first_name})
        yield neondata.NeonUserAccount(None, 'a0').save(async=True)
        yield neondata.User('a0').save(async=True)
        token = JWTHelper.generate_token({
            'account_id': 'a0',
            'username': 'a0'})
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % token}
        url = '/api/v2/users'
        response = yield self.http_client.fetch(self.get_url(url),
                                                body=params,
                                                method='POST',
                                                headers=headers)
        verification = yield neondata.Verification.get(username, async=True)
        verif_user = json.loads(verification.extra_info['user'])
        self.assertEqual(first_name, verif_user['_data']['first_name'])

        username = 'lucia@gmail.com'
        first_name = u'Lucía'
        last_name = u'Rolagár'
        params = json.dumps({
            'username': username,
            'password': 'passw0rd',
            'first_name': first_name,
            'last_name': last_name})
        token = JWTHelper.generate_token({
            'account_id': 'a0',
            'username': 'a0'})
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % token}

        response = yield self.http_client.fetch(self.get_url(url),
                                                body=params,
                                                method='POST',
                                                headers=headers)
        verification = yield neondata.Verification.get(username, async=True)
        verif_user = json.loads(verification.extra_info['user'])
        self.assertEqual(first_name, verif_user['_data']['first_name'])
        self.assertEqual(last_name, verif_user['_data']['last_name'])

    def test_post_user_exceptions(self):
        exception_mocker = patch('cmsapiv2.authentication.UserHandler.post')
        params = json.dumps({'username': '123123abc'})
	url = '/api/v2/users'
        self.post_exceptions(url, params, exception_mocker)

    @tornado.testing.gen_test
    def test_put_user_reset_password_no_user(self):
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'dne@test.invalid',
             'new_password': 'newpassword',
             'reset_password_token': 'sdfasdfasdfasdfasdfasdf'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="PUT",
                headers=header)
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'User was not found')

    @tornado.testing.gen_test
    def test_put_user_reset_password_bad_pw_token(self):
        user = neondata.User(username='testuser@test.invalid',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)

        header = { 'Content-Type':'application/json' }
        token = JWTHelper.generate_token(
            {'username' : 'testuser@test.invalid'},
            token_type=TokenTypes.RESET_PASSWORD_TOKEN)
        user.reset_password_token = token
        yield user.save(async=True)
        params = json.dumps(
            {'username': 'testuser@test.invalid',
             'new_password': 'newpassword',
             'reset_password_token': 'ohsobadmeissixteenchars'})

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="PUT",
                headers=header)
	    self.assertEquals(e.exception.code, 401)

        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Token mismatch')

    @tornado.testing.gen_test
    def test_put_user_reset_password_token_expired(self):
        user = neondata.User(username='testuser@test.invalid',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)

        header = { 'Content-Type':'application/json' }
        token = JWTHelper.generate_token(
            {'username' : 'testuser@test.invalid', 'exp' : -1},
            token_type=TokenTypes.RESET_PASSWORD_TOKEN)
        user.reset_password_token = token
        yield user.save(async=True)
        params = json.dumps(
            {'username': 'testuser@test.invalid',
             'new_password': 'newpassword',
             'reset_password_token': token})

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="PUT",
                headers=header)
	    self.assertEquals(e.exception.code, 401)

        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'reset password token has')

    @tornado.testing.gen_test
    def test_put_user_reset_password_full(self):
        user = neondata.User(username='testuser@test.invalid',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)

        token = JWTHelper.generate_token(
            {'username' : 'testuser@test.invalid'},
            token_type=TokenTypes.RESET_PASSWORD_TOKEN)
        user.reset_password_token = token
        yield user.save(async=True)
        params = json.dumps(
            {'username': 'testuser@test.invalid',
             'new_password': 'newpassword',
             'reset_password_token': user.reset_password_token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/users'
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=params,
            method='PUT',
            headers=header)

        # verify the password was changed
        user = yield user.get('testuser@test.invalid', async=True)
        self.assertTrue(sha256_crypt.verify('newpassword', user.password_hash))
        self.assertEqual(None, user.reset_password_token)


class TestVerifyAccountHandler(TestAuthenticationBase):

    @tornado.testing.gen_test
    def test_verify_with_no_user_saved(self):
        email = 'yo@notgmail.com'
        account = neondata.NeonUserAccount('name', 'a0', serving_enabled=False)
        yield account.save(async=True)
        account.email = email
        cell = '867-5309'

        # User wants to verify yo@notgmail.com.
        user = neondata.User(email, cell_phone_number=cell)
        account.users = [email]
        info = {
            'account': account.to_json(),
            'user': user.to_json()
        }
        token = JWTHelper.generate_token(
            {'email': email},
            token_type=TokenTypes.VERIFY_TOKEN)
        verifier = neondata.Verification(email, token, extra_info=info)
        yield verifier.save(async=True)

        # Now verify.
        url = self.get_url('/api/v2/accounts/verify')
        headers = {'Content-Type': 'application/json'}
        body = json.dumps({'token': token})
        response = yield self.http_client.fetch(
            url,
            method='POST',
            headers=headers,
            body=body)

        self.assertEqual(200, response.code)
        rjson = json.loads(response.body)
        self.assertEqual('a0', rjson['account_id'])
        self.assertEqual(email, rjson['email'])
        self.assertEqual([email], rjson['users'])
        account = yield neondata.NeonUserAccount.get('a0', async=True)
        self.assertTrue(account.serving_enabled)
        user = yield neondata.User.get(email, async=True)
        self.assertEqual(email, user.username)
        self.assertEqual(cell, user.cell_phone_number)
        self.assertTrue(user.is_email_verified())

    @tornado.testing.gen_test
    def test_verify_with_account_keyed_user(self):
        email = 'yo@notgmail.com'
        account = neondata.NeonUserAccount('name', 'a0', serving_enabled=False)
        yield account.save(async=True)
        account.email = email
        user = neondata.User('a0', access_level=neondata.AccessLevels.ADMIN,
                             email_verified=False)
        self.assertFalse(user.is_email_verified())
        yield user.save(async=True)

        cell = '867-5309'
        email2 = 'rocking@invalid.com'
        user.key = user.format_key(email)
        user.username = email
        user.cell_phone_number = cell
        user.secondary_email = email2
        account.users = [email]
        info = {
            'account': account.to_json(),
            'user': user.to_json()
        }
        token = JWTHelper.generate_token(
            {'email': email},
            token_type=TokenTypes.VERIFY_TOKEN)
        verifier = neondata.Verification(email, token, extra_info=info)
        yield verifier.save(async=True)
        user = yield neondata.User.get('a0', async=True)
        self.assertFalse(user.is_email_verified())

        # Now verify.
        url = self.get_url('/api/v2/accounts/verify')
        headers = {'Content-Type': 'application/json'}
        body = json.dumps({'token': token})
        response = yield self.http_client.fetch(
            url,
            method='POST',
            headers=headers,
            body=body)
        self.assertEqual(200, response.code)
        rjson = json.loads(response.body)
        self.assertEqual('a0', rjson['account_id'])
        self.assertEqual(email, rjson['email'])
        self.assertEqual([email], rjson['users'])
        account = yield neondata.NeonUserAccount.get('a0', async=True)
        self.assertTrue(account.serving_enabled)
        user = yield neondata.User.get(email, async=True)
        self.assertTrue(user.is_email_verified())
        self.assertEqual(email, user.username)
        self.assertEqual(cell, user.cell_phone_number)
        self.assertEqual(email2, user.secondary_email)


class TestUserHandler(TestControllersBase):
    def setUp(self):
        self.neon_user = neondata.NeonUserAccount(
            uuid.uuid1().hex,
            name='testingaccount')
        self.neon_user.save()
        super(TestUserHandler, self).setUp()


    # token creation can be slow give it some extra time just in case
    @tornado.testing.gen_test(timeout=10.0)
    def test_get_user_does_exist(self):
        user = neondata.User(username='testuser',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             cell_phone_number='867-5309',
                             secondary_email='rocking@invalid.com',
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
        self.assertEquals(rjson['cell_phone_number'], '867-5309')
        self.assertEquals(rjson['secondary_email'], 'rocking@invalid.com')

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
        self.assertRegexpMatches(rjson['error']['message'], 'Cannot view')

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
                             'first_name' : 'kevin',
                             'last_name' : 'kevin',
                             'title' : 'DOCTOR',
                             'cell_phone_number':'867-5309',
                             'secondary_email':'rocking@invalid.com',
                             'send_emails':False,
                             'token' : token})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/users' % (self.neon_user.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body=params,
                                                method='PUT',
                                                headers=header)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        updated_user = yield neondata.User.get('testuser', async=True)
        self.assertEquals(updated_user.first_name, 'kevin')
        self.assertEquals(updated_user.last_name, 'kevin')
        self.assertEquals(updated_user.title, 'DOCTOR')
        self.assertEquals(rjson['cell_phone_number'], '867-5309')
        self.assertEquals(rjson['secondary_email'], 'rocking@invalid.com')
        self.assertEquals(updated_user.cell_phone_number, '867-5309')
        self.assertEquals(updated_user.secondary_email, 'rocking@invalid.com')
        self.assertEquals(updated_user.send_emails, False)

    # token creation can be slow give it some extra time just in case
    @unittest.skip('revisit when access levels are better defined')
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
        params = json.dumps({'username':'testuser', 'token' : token})
        header = { 'Content-Type':'application/json' }
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/users' % (self.neon_user.neon_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body=params,
                                                    method='PUT',
                                                    headers=header)
        self.assertEquals(e.exception.code, 401)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'Cannot set')

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
        self.assertRegexpMatches(rjson['error']['message'], 'Cannot update')


class TestOoyalaIntegrationHandler(TestControllersBase):
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        defop = neondata.OoyalaIntegration.modify(
            'acct1',
            lambda x: x,
            create_missing=True)
        self.test_i_id = defop.integration_id
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestOoyalaIntegrationHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestOoyalaIntegrationHandler, self).tearDown()


    @tornado.testing.gen_test
    def test_post_integration(self):
        url = '/api/v2/%s/integrations/ooyala?publisher_id=123123abc' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
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
        url = '/api/v2/%s/integrations/ooyala?integration_id=idontexist' % (
            self.account_id_api_key)
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
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&api_key=%s' % (
            self.account_id_api_key, self.test_i_id, api_key)
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
            url = '/api/v2/%s/integrations/ooyala?integration_id=nope'\
                  '&api_key=%s' % (self.account_id_api_key, api_key)

            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='PUT',
                allow_nonstandard_methods=True)

        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'unable to find')

    @tornado.testing.gen_test
    def test_put_integration_ensure_old_info_not_nulled(self):
        api_key = 'testapikey'
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s'\
              '&api_key=%s' % (self.account_id_api_key,
                  self.test_i_id,
                  api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT',
                                                allow_nonstandard_methods=True)
        api_secret = 'testapisecret'
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s'\
              '&api_secret=%s' % (self.account_id_api_key,
                  self.test_i_id,
                  api_secret)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT',
                                                allow_nonstandard_methods=True)

        platform = yield tornado.gen.Task(neondata.OoyalaIntegration.get,
                                          self.test_i_id)

        self.assertEquals(platform.api_key, api_key)
        self.assertEquals(platform.api_secret, api_secret)

    def test_get_integration_exceptions(self):
        exception_mocker = patch(
            'cmsapiv2.controllers.OoyalaIntegrationHandler.get')
        url = '/api/v2/%s/integrations/ooyala' % '1234234'
        self.get_exceptions(url, exception_mocker)

    def test_put_integration_exceptions(self):
        exception_mocker = patch(
            'cmsapiv2.controllers.OoyalaIntegrationHandler.put')
        params = json.dumps({'integration_id': '123123abc'})
        url = '/api/v2/%s/integrations/ooyala' % '1234234'
        self.put_exceptions(url, params, exception_mocker)

    def test_post_integration_exceptions(self):
        exception_mocker = patch(
            'cmsapiv2.controllers.OoyalaIntegrationHandler.post')
        params = json.dumps({'integration_id': '123123abc'})
        url = '/api/v2/%s/integrations/ooyala' % '1234234'
        self.post_exceptions(url, params, exception_mocker)


class TestBrightcoveIntegrationHandler(TestControllersBase):
    def setUp(self):
        user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.defop = neondata.BrightcoveIntegration.modify(
            'acct1',
            lambda x: x,
            create_missing=True)
        self.test_i_id = self.defop.integration_id
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestBrightcoveIntegrationHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestBrightcoveIntegrationHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_post_integration(self):
        url = (('/api/v2/%s/integrations/brightcove?publisher_id=123123abc'
                '&uses_bc_gallery=false') % (self.account_id_api_key))
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
        self.assertEquals(rjson['application_client_id'], self.defop.application_client_id)
        self.assertEquals(rjson['application_client_secret'], self.defop.application_client_secret)
        self.assertEquals(rjson['callback_url'], self.defop.callback_url)
        self.assertEquals(rjson['uses_batch_provisioning'], self.defop.uses_batch_provisioning)
        self.assertEquals(rjson['id_field'], self.defop.id_field)
        self.assertEquals(rjson['uses_bc_thumbnail_api'], self.defop.uses_bc_thumbnail_api)
        self.assertEquals(rjson['uses_bc_videojs_player'], self.defop.uses_bc_videojs_player)
        self.assertEquals(rjson['uses_bc_smart_player'], self.defop.uses_bc_smart_player)
        self.assertFalse(rjson['uses_bc_gallery'])

    @tornado.testing.gen_test
    def test_post_integration_body_params(self):
        params = json.dumps({'publisher_id': '123123abc',
                             'uses_bc_gallery': False})
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
        self.assertEquals(rjson['application_client_id'], self.defop.application_client_id)
        self.assertEquals(rjson['application_client_secret'], self.defop.application_client_secret)
        self.assertEquals(rjson['callback_url'], self.defop.callback_url)
        self.assertEquals(rjson['uses_batch_provisioning'], self.defop.uses_batch_provisioning)
        self.assertEquals(rjson['id_field'], self.defop.id_field)
        self.assertEquals(rjson['uses_bc_thumbnail_api'], self.defop.uses_bc_thumbnail_api)
        self.assertEquals(rjson['uses_bc_videojs_player'], self.defop.uses_bc_videojs_player)
        self.assertEquals(rjson['uses_bc_smart_player'], self.defop.uses_bc_smart_player)
        self.assertFalse(rjson['uses_bc_gallery'])

    @tornado.testing.gen_test
    def test_post_gallery_required(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = ('/api/v2/%s/integrations/brightcove?publisher_id=123123abc'
                    % (self.account_id_api_key))
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)
        self.assertEquals(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_post_gallery(self):
        params = json.dumps({'publisher_id': '123123abc',
                             'uses_bc_gallery': True})
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body=params,
                                                method='POST',
                                                headers=header)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertTrue(rjson['uses_bc_gallery'])

        # Check that uses_bc_thumbnail_api is set
        self.assertTrue(rjson['uses_bc_thumbnail_api'])

        # Check that the CDN was set properly
        cdns = neondata.CDNHostingMetadataList.get(
            neondata.CDNHostingMetadataList.create_key(
                self.account_id_api_key,
                rjson['integration_id']))
        self.assertItemsEqual(cdns.cdns[0].rendition_sizes,[
            [120, 67],
            [120, 90],
            [160, 90],
            [160, 120],
            [210, 118],
            [320, 180],
            [374, 210],
            [320, 240],
            [460, 260],
            [480, 270],
            [622, 350],
            [480, 360],
            [640, 360],
            [640, 480],
            [960, 540],
            [1280, 720]])

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
        self.assertEquals(rjson['application_client_id'], platform.application_client_id)
        self.assertEquals(rjson['application_client_secret'], platform.application_client_secret)
        self.assertEquals(rjson['callback_url'], platform.callback_url)
        self.assertEquals(rjson['uses_batch_provisioning'], platform.uses_batch_provisioning)
        self.assertEquals(rjson['id_field'], platform.id_field)
        self.assertEquals(rjson['uses_bc_thumbnail_api'], platform.uses_bc_thumbnail_api)
        self.assertEquals(rjson['uses_bc_videojs_player'], platform.uses_bc_videojs_player)
        self.assertEquals(rjson['uses_bc_smart_player'], platform.uses_bc_smart_player)
        self.assertEquals(rjson['uses_bc_gallery'], platform.uses_bc_gallery)

    @tornado.testing.gen_test
    def test_get_integration_with_fields(self):
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s'\
              '&fields=%s' % (
            self.account_id_api_key,
            self.test_i_id, 'read_token,write_token,application_client_id,callback_url')
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield tornado.gen.Task(neondata.BrightcoveIntegration.get,
                                          self.test_i_id)

        self.assertEquals(rjson['read_token'], platform.read_token)
        self.assertEquals(rjson['write_token'], platform.write_token)
        self.assertEquals(rjson['application_client_id'], platform.application_client_id)
        self.assertEquals(rjson['callback_url'], platform.callback_url)
        self.assertEquals(rjson.get('uses_batch_provisioning', None), None)
        self.assertEquals(rjson.get('id_field', None), None)

    @tornado.testing.gen_test
    def test_put_integration(self):
        read_token = 'readtoken'
        url = '/api/v2/%s/integrations/brightcove?'\
              'integration_id=%s&read_token=%s' % (
                  self.account_id_api_key,
                  self.test_i_id,
                  read_token)
        response = yield self.http_client.fetch(
            self.get_url(url),
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
        url = (('/api/v2/%s/integrations/brightcove?publisher_id=123123abc'
                '&uses_bc_gallery=false&playlist_feed_ids=abc') %
                (self.account_id_api_key))
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
        url = (('/api/v2/%s/integrations/brightcove?publisher_id=123123abc'
                '&uses_bc_gallery=false&playlist_feed_ids=abc,def,ghi') %
                (self.account_id_api_key))
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
                             'uses_bc_gallery' : 'true',
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
                             'uses_bc_gallery' : 0,
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
                             'uses_bc_gallery': True,
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
                             'uses_batch_provisioning': 1,
                             'uses_bc_gallery': False})
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

    @tornado.testing.gen_test
    def test_post_integration_videos_empty(self):
        with patch('api.brightcove_api.CMSAPI') as gvp:
            gvp.return_value.get_videos = MagicMock()
            get_videos_mock = self._future_wrap_mock(
                gvp.return_value.get_videos)
            get_videos_mock.return_value = []
            params = json.dumps({
                'publisher_id': '123123abc',
                'uses_bc_gallery': False,
                'application_client_id': '5',
                'application_client_secret': 'some secret'})
            header = { 'Content-Type':'application/json' }
            url = '/api/v2/%s/integrations/brightcove' % (
                self.account_id_api_key)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        rjson = json.loads(response.body)
        platform = yield neondata.BrightcoveIntegration.get(
            rjson['integration_id'], async=True)
        self.assertNotEqual(platform.last_process_date, None)

    @tornado.testing.gen_test
    def test_post_integration_videos_none(self):
        with patch('api.brightcove_api.CMSAPI') as gvp:
            gvp.return_value.get_videos = MagicMock()
            get_videos_mock = self._future_wrap_mock(
                gvp.return_value.get_videos)
            get_videos_mock.return_value = None
            params = json.dumps({
                'publisher_id': '123123abc',
                'uses_bc_gallery' : False,
                'application_client_id': '5',
                'application_client_secret': 'some secret'})
            header = { 'Content-Type':'application/json' }
            url = '/api/v2/%s/integrations/brightcove' % (
                self.account_id_api_key)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        rjson = json.loads(response.body)
        platform = yield neondata.BrightcoveIntegration.get(
            rjson['integration_id'], async=True)
        self.assertNotEqual(platform.last_process_date, None)

    @tornado.testing.gen_test
    def test_post_integration_videos_brightcove_errors(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('api.brightcove_api.CMSAPI') as gvp:
                gvp.return_value.get_videos = MagicMock()
                get_videos_mock = self._future_wrap_mock(
                    gvp.return_value.get_videos)
                get_videos_mock.side_effect = [
                    api.brightcove_api.BrightcoveApiServerError('test')]
                params = json.dumps({
                    'publisher_id': '123123abc',
                    'uses_bc_gallery' : False,
                    'application_client_id': '5',
                    'application_client_secret': 'some secret'})
                header = { 'Content-Type':'application/json' }
                url = '/api/v2/%s/integrations/brightcove' % (
                    self.account_id_api_key)
                response = yield self.http_client.fetch(
                    self.get_url(url),
                    body=params,
                    method='POST',
                    headers=header)

        self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Brightcove credentials are bad')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('api.brightcove_api.CMSAPI') as gvp:
                gvp.return_value.get_videos = MagicMock()
                get_videos_mock = self._future_wrap_mock(
                    gvp.return_value.get_videos)
                get_videos_mock.side_effect = [
                    api.brightcove_api.BrightcoveApiNotAuthorizedError('test')]
                params = json.dumps({
                    'publisher_id': '123123abc',
                    'uses_bc_gallery' : False,
                    'application_client_id': '5',
                    'application_client_secret': 'some secret'})
                header = { 'Content-Type':'application/json' }
                url = '/api/v2/%s/integrations/brightcove' % (
                    self.account_id_api_key)
                response = yield self.http_client.fetch(
                    self.get_url(url),
                    body=params,
                    method='POST',
                    headers=header)

        self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Brightcove credentials are bad')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('api.brightcove_api.CMSAPI') as gvp:
                gvp.return_value.get_videos = MagicMock()
                get_videos_mock = self._future_wrap_mock(
                    gvp.return_value.get_videos)
                get_videos_mock.side_effect = [
                    api.brightcove_api.BrightcoveApiClientError('test')]
                params = json.dumps({
                    'publisher_id': '123123abc',
                    'uses_bc_gallery' : False,
                    'application_client_id': '5',
                    'application_client_secret': 'some secret'})
                header = { 'Content-Type':'application/json' }
                url = '/api/v2/%s/integrations/brightcove' % (
                    self.account_id_api_key)
                response = yield self.http_client.fetch(
                    self.get_url(url),
                    body=params,
                    method='POST',
                    headers=header)

        self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Brightcove credentials are bad')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('api.brightcove_api.CMSAPI') as gvp:
                gvp.return_value.get_videos = MagicMock()
                get_videos_mock = self._future_wrap_mock(
                    gvp.return_value.get_videos)
                get_videos_mock.side_effect = [
                    api.brightcove_api.BrightcoveApiError('test')]
                params = json.dumps({
                    'publisher_id': '123123abc',
                    'uses_bc_gallery' : False,
                    'application_client_id': '5',
                    'application_client_secret': 'some secret'})
                header = { 'Content-Type':'application/json' }
                url = '/api/v2/%s/integrations/brightcove' % (
                    self.account_id_api_key)
                response = yield self.http_client.fetch(
                    self.get_url(url),
                    body=params,
                    method='POST',
                    headers=header)

        self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Brightcove credentials are bad')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('api.brightcove_api.CMSAPI') as gvp:
                gvp.return_value.get_videos = MagicMock()
                get_videos_mock = self._future_wrap_mock(
                    gvp.return_value.get_videos)
                get_videos_mock.side_effect = [Exception('test')]
                params = json.dumps({
                    'publisher_id': '123123abc',
                    'uses_bc_gallery' : False,
                    'application_client_id': '5',
                    'application_client_secret': 'some secret'})
                header = { 'Content-Type':'application/json' }
                url = '/api/v2/%s/integrations/brightcove' % (
                    self.account_id_api_key)
                response = yield self.http_client.fetch(
                    self.get_url(url),
                    body=params,
                    method='POST',
                    headers=header)

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['data'],
                                 'test')

    @tornado.testing.gen_test
    def test_post_and_put_integration_client_id_and_secret(self):
        with patch('api.brightcove_api.CMSAPI') as gvp:
            gvp.return_value.get_videos = MagicMock()
            get_videos_mock = self._future_wrap_mock(
                gvp.return_value.get_videos)
            get_videos_mock.return_value = [
                {"updated_at" : "2015-04-20T21:18:32.351Z"}]
            params = json.dumps({
                'publisher_id': '123123abc',
                'uses_bc_gallery': False,
                'application_client_id': '5',
                'application_client_secret': 'some secret'})
            header = { 'Content-Type':'application/json' }
            url = '/api/v2/%s/integrations/brightcove' % (
                self.account_id_api_key)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        rjson = json.loads(response.body)
        platform = yield neondata.BrightcoveIntegration.get(
            rjson['integration_id'], async=True)
        self.assertEqual(platform.application_client_id, '5')
        params = json.dumps({'integration_id': rjson['integration_id'],
                             'application_client_id': '6',
                             'application_client_secret': 'another secret'})

        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)
        with patch('api.brightcove_api.CMSAPI') as gvp:
            gvp.return_value.get_videos = MagicMock()
            get_videos_mock = self._future_wrap_mock(
                gvp.return_value.get_videos)
            get_videos_mock.return_value = [
                {'updated_at': '2015-04-20T21:18:32.351Z'}]
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='PUT',
                headers=header)

        self.assertEqual(response.code, 200)
        rjson = json.loads(response.body)
        platform = yield neondata.BrightcoveIntegration.get(
            rjson['integration_id'], async=True)
        self.assertEqual(platform.application_client_id, '6')
        self.assertEqual(platform.application_client_secret, 'another secret')
        self.assertEqual(platform.last_process_date, '2015-04-20T21:18:32.351Z')

    @tornado.testing.gen_test
    def test_put_client_id_missing_secret(self):
        with patch('api.brightcove_api.CMSAPI') as gvp:
            gvp.return_value.get_videos = MagicMock()
            get_videos_mock = self._future_wrap_mock(
                gvp.return_value.get_videos)
            get_videos_mock.return_value = [
                {"updated_at" : "2015-04-20T21:18:32.351Z"}]
            params = json.dumps({'publisher_id': '123123abc',
                'application_client_id': '5',
                'application_client_secret': 'some secret',
                'uses_bc_gallery': True,
                'uses_bc_videojs_player': 'True'})
            header = {'Content-Type':'application/json'}
            url = '/api/v2/%s/integrations/brightcove' % (
                self.account_id_api_key)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        rjson = json.loads(response.body)
        platform = yield neondata.BrightcoveIntegration.get(
            rjson['integration_id'], async=True)
        self.assertEqual(platform.application_client_id, '5')
        self.assertEqual(platform.uses_bc_gallery, True)
        self.assertTrue(platform.uses_bc_videojs_player)
        params = json.dumps({'integration_id': rjson['integration_id'],
                             'application_client_id': 'not 5',
                             'application_client_secret': None})
        url = '/api/v2/%s/integrations/brightcove' % (self.account_id_api_key)

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url), body=params, method='PUT')
        self.assertEqual(e.exception.code, 400, 'Bad parameters by client for PUT')
        platform = yield neondata.BrightcoveIntegration.get(
            rjson['integration_id'], async=True)
        self.assertEqual(
            platform.application_client_id, '5', 'A malformed PUT makes no change')

        params = json.dumps({'integration_id': rjson['integration_id'],
                             'application_client_id': None,
                             'application_client_secret': None,
                             'uses_bc_videojs_player': False})
        response = yield self.http_client.fetch(
            self.get_url(url), body=params, method='PUT', headers=header)
        self.assertEqual(response.code, 200)
        platform = yield neondata.BrightcoveIntegration.get(
            rjson['integration_id'], async=True)
        self.assertEqual(platform.uses_bc_videojs_player, False,
                         'Valid PUT updates this field')

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
        random.seed(1324689)
        np.random.seed(1348)
        
        user = neondata.NeonUserAccount(
            uuid.uuid1().hex,
            name='testingme',
            processing_priority=2)
        user.save()
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testvideohiid'
        neondata.ThumbnailMetadata('testing_vtid_one', width=500,
                                   urls=['s'],
                                   ttype='neon',
                                   model_score=0.32,
                                   model_version='aqv1').save()
        neondata.ThumbnailMetadata('testing_vtid_two', width=500,
                                   urls=['d'],
                                   ttype='neon',
                                   features=np.random.rand(1024),
                                   model_version='20160713-test').save()
        neondata.ThumbnailMetadata('testing_vtid_bad', width=500,
                                   urls=['bad'],
                                   features=np.random.rand(1024),
                                   model_version='20160713-test').save()
        neondata.ThumbnailMetadata('testing_vtid_rand', 
                                   ttype='random',
                                   urls=['rand.jpg']).save()
        neondata.ThumbnailMetadata('testing_vtid_default',
                                   ttype='default',
                                   urls=['default.jpg'],
                                   features=np.random.rand(1024),
                                   model_version='20160713-test').save()
        neondata.NeonApiRequest('job1', self.account_id_api_key).save()
        defop = neondata.BrightcoveIntegration.modify(self.test_i_id,
            lambda x: x,
            create_missing=True)
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
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        self.maxDiff = 5000

        # Mock the video queue
        self.job_queue_patcher = patch(
            'video_processor.video_processing_queue.' \
            'VideoProcessingQueue')
        self.job_queue_mock = self.job_queue_patcher.start()()
        self.job_write_mock = self._future_wrap_mock(
            self.job_queue_mock.write_message)
        self.job_write_mock.side_effect = ['message']

        super(TestVideoHandler, self).setUp()

    def tearDown(self):
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
        self.verify_account_mocker.stop()
        self.job_queue_patcher.stop()

        super(TestVideoHandler, self).tearDown()

    @patch('cmsdb.neondata.ThumbnailMetadata.download_image_from_url')
    @tornado.testing.gen_test
    def test_post_video(self, cmsdb_download_image_mock):
        
        url = ('/api/v2/{0}/videos?integration_id={1}'\
               '&external_video_ref=1234ascs'\
               '&default_thumbnail_url=url.invalid'\
               '&title=a_title'\
               '&url=some_url'\
               '&thumbnail_ref=ref1'\
               '&duration=16').format(self.account_id_api_key, self.test_i_id)
        cmsdb_download_image_mock = self._future_wrap_mock(cmsdb_download_image_mock)
        cmsdb_download_image_mock.side_effect = [self.random_image]
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')
        job = yield neondata.NeonApiRequest.get(rjson['job_id'],
                                                self.account_id_api_key,
                                                async=True)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[0], 2)
        self.assertDictContainsSubset(json.loads(cargs[1]), job.__dict__)
        self.assertEquals(cargs[2], 16)
        self.assertEquals(job.api_param, 5)
        self.assertIsNone(job.age)
        self.assertIsNone(job.gender)

    @tornado.testing.gen_test
    def test_post_video_with_clip(self):
        
        url = ('/api/v2/{0}/videos?integration_id={1}'\
               '&external_video_ref=1234ascs'\
               '&title=a_title'\
               '&url=some_url'\
               '&thumbnail_ref=ref1'\
               '&duration=16'\
               '&result_type=clips').format(self.account_id_api_key, self.test_i_id)
        
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')
        job = yield neondata.NeonApiRequest.get(rjson['job_id'],
                                                self.account_id_api_key,
                                                async=True)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[0], 2)
        self.assertDictContainsSubset(json.loads(cargs[1]), job.__dict__)
        self.assertEquals(cargs[2], 16)
        self.assertEquals(job.api_param, None)
        self.assertEquals(job.n_clips, 1)
        self.assertEquals(job.result_type, neondata.ResultType.CLIPS)
        self.assertIsNone(job.clip_length)
        self.assertIsNone(job.age)
        self.assertIsNone(job.gender)

    @tornado.testing.gen_test
    def test_post_video_with_clip_with_length(self):
        url = ('/api/v2/{0}/videos?integration_id={1}'\
               '&external_video_ref=1234ascs'\
               '&title=a_title'\
               '&url=some_url'\
               '&thumbnail_ref=ref1'\
               '&duration=16'\
               '&result_type=clips'\
               '&clip_length=5.3'\
               '&n_clips=3').format(self.account_id_api_key,
                                    self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')
        job = yield neondata.NeonApiRequest.get(rjson['job_id'],
                                                self.account_id_api_key,
                                                async=True)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[0], 2)
        self.assertDictContainsSubset(json.loads(cargs[1]), job.__dict__)
        self.assertEquals(cargs[2], 16)
        self.assertEquals(job.api_param, None)
        self.assertEquals(job.n_clips, 3)
        self.assertEquals(job.result_type, neondata.ResultType.CLIPS)
        self.assertAlmostEquals(job.clip_length, 5.3)
        self.assertIsNone(job.age)
        self.assertIsNone(job.gender)

    @tornado.testing.gen_test
    def test_post_video_with_default_clip(self):
        body = {
                'external_video_ref': '1234ascs33',
                'url': 'some_url',
                'title': 'my_clip_video',
                'gender': 'M',
                'age': '50+',
                'result_type': 'clips',
                'default_clip_url': 'clipurl.mp4'
            }
        header = {"Content-Type": "application/json"}
        url = '/api/v2/%s/videos' % (self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=json.dumps(body),
            method='POST',
            headers=header)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')
        job = yield neondata.NeonApiRequest.get(rjson['job_id'],
                                                self.account_id_api_key,
                                                async=True)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[0], 2)
        self.assertDictContainsSubset(json.loads(cargs[1]), job.__dict__)
        self.assertIsNone(cargs[2]) # Unknown duration
        self.assertIsNone(job.api_param)
        self.assertEquals(job.default_clip, 'clipurl.mp4')
        self.assertEquals(job.result_type, neondata.ResultType.CLIPS)
        self.assertEquals(job.age, '50+')
        self.assertEquals(job.gender, 'M')

    @tornado.testing.gen_test
    def test_post_demographic(self):        
        body = {
                'external_video_ref': '1234ascs33',
                'url': 'some_url',
                'title': 'my_demo_video',
                'gender': 'M',
                'age': '50+'
            }
        header = {"Content-Type": "application/json"}
        url = '/api/v2/%s/videos' % (self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=json.dumps(body),
            method='POST',
            headers=header)
        rjson = json.loads(response.body)
        job_id = rjson['job_id']
        self.assertEquals(response.code, 202)
        video = yield neondata.VideoMetadata.get('%s_%s' % (
            self.account_id_api_key, '1234ascs33'), async=True)
        self.assertEquals(video.url, 'some_url')

        request = neondata.NeonApiRequest.get(job_id, self.account_id_api_key)
        self.assertEquals(request.gender, 'M')
        self.assertEquals(request.age, '50+')

        # Check the submitted job
        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        job = json.loads(cargs[1])
        self.assertEquals(job['gender'], 'M')
        self.assertEquals(job['age'], '50+')

    @tornado.testing.gen_test
    def test_post_bad_demographic(self):
        url = '/api/v2/%s/videos?external_video_ref=1234ascs&url=some_url&gender={}&age={}' % (self.account_id_api_key)

        for demo_combos in [('alien', '18-19'),
                            ('M', '0-18'),
                            ('F', 56)]:
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                yield self.http_client.fetch(
                    self.get_url(url.format(*demo_combos)),
                    body='',
                    method='POST',
                    allow_nonstandard_methods=True)

            self.assertEquals(e.exception.code, 400)
            rjson = json.loads(e.exception.response.body)
            self.assertRegexpMatches(rjson['error']['message'],
                                     'not allowed for .*(gender|age)')

    @tornado.testing.gen_test
    def test_post_reprocess_demographic(self):        
        body = {
                'external_video_ref': '1234ascs33',
                'url': 'some_url',
                'title': 'my_demo_video',
                'gender': 'M',
                'age': '50+'
            }
        header = {"Content-Type": "application/json"}
        url = '/api/v2/%s/videos' % (self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=json.dumps(body),
            method='POST',
            headers=header)
        rjson = json.loads(response.body)
        job_id = rjson['job_id']
        self.assertEquals(response.code, 202)
        video = yield neondata.VideoMetadata.get('%s_%s' % (
            self.account_id_api_key, '1234ascs33'), async=True)
        self.assertEquals(video.url, 'some_url')

        request = neondata.NeonApiRequest.get(job_id, self.account_id_api_key)
        self.assertEquals(request.gender, 'M')
        self.assertEquals(request.age, '50+')
        self.assertEquals(request.state, neondata.RequestState.SUBMIT)

        # Check the submitted job
        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        job = json.loads(cargs[1])
        self.assertEquals(job['gender'], 'M')
        self.assertEquals(job['age'], '50+')
        self.job_write_mock.reset_mock()
        self.job_write_mock.side_effect = ['message']

        # Try to reprocess, but the last job didn't finish
        body['reprocess'] = True
        body['gender'] = 'F'
        body['url'] = None
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=json.dumps(body),
                method='POST',
                headers=header)
        self.assertEquals(e.exception.code, 409)
        self.assertEquals(self.job_write_mock.call_count, 0)

        # Finish the job and try reprocessing
        request = neondata.NeonApiRequest.get(job_id, self.account_id_api_key)
        request.state = neondata.RequestState.FINISHED
        request.save()
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=json.dumps(body),
            method='POST',
            headers=header)
        self.assertEquals(response.code, 202)
        request = neondata.NeonApiRequest.get(job_id, self.account_id_api_key)
        self.assertEquals(request.gender, 'F')
        self.assertEquals(request.age, '50+')
        self.assertEquals(request.state, neondata.RequestState.REPROCESS)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        job = json.loads(cargs[1])
        self.assertEquals(job['gender'], 'F')
        self.assertEquals(job['age'], '50+')
        self.job_write_mock.reset_mock()        

        # Check for a tag.
        tag_id = rjson['video']['tag_id']
        self.assertIsNotNone(tag_id)
        tag = neondata.Tag.get(tag_id)
        self.assertEqual(tag.account_id, self.account_id_api_key)
        self.assertEqual(tag.tag_type, neondata.TagType.VIDEO)

    @tornado.testing.gen_test
    def test_post_video_with_limits_refresh_date_reset(self):
        cmsdb_download_image_mocker = patch(
            'cmsdb.neondata.ThumbnailMetadata.download_image_from_url')
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
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        with self._future_wrap_mock(
             patch(pstr)) as cmsdb_download_image_mock:
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

            response = yield self.http_client.fetch(
                self.get_url(url),
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
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(self.get_url(url),
                                         body='',
                                         method='POST',
                                         allow_nonstandard_methods=True)

        self.assertEquals(e.exception.code, 402)

        # Make sure the job isn't in the database or submitted
        self.assertIsNone(neondata.VideoMetadata.get(
            neondata.InternalVideoID.generate(self.account_id_api_key,
                                              '1234ascs')))
        self.assertEquals(self.job_write_mock.call_count, 0)
        

    @tornado.testing.gen_test(timeout=600)
    def test_post_video_reprocess_doesnt_hit_limit(self):
        # Add a video to reprocess
        body = {
                'external_video_ref': '1234ascs33',
                'url': 'some_url',
                'title': 'my_demo_video',
                'gender': 'M',
                'age': '50+'
            }
        header = {"Content-Type": "application/json"}
        url = '/api/v2/%s/videos' % (self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=json.dumps(body),
            method='POST',
            headers=header)
        rjson = json.loads(response.body)
        job_id = rjson['job_id']
        self.assertEquals(response.code, 202)

        # Simulate the job finishing
        def _finish_job(x):
            x.state = neondata.RequestState.FINISHED
        neondata.NeonApiRequest.modify(job_id, self.account_id_api_key,
                                       _finish_job)

        # Simulate being at the end of the limits
        neondata.AccountLimits(self.account_id_api_key,
            video_posts=10).save()

        # Submitting a new job will hit the limit
        body['external_video_ref'] = 'new-job'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(self.get_url(url),
                                         body=json.dumps(body),
                                         method='POST',
                                         headers=header)

        self.assertEquals(e.exception.code, 402)

        # Reprocessing the first job will not hit the limit
        self.job_write_mock.reset_mock()
        self.job_write_mock.side_effect = ['message']
        body['external_video_ref'] = '1234ascs33'
        body['reprocess'] = True
        body['url'] = None
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=json.dumps(body),
            method='POST',
            headers=header)
        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        job_id = rjson['job_id']

        # Check that the limit count isn't increased
        self.assertEquals(
            neondata.AccountLimits.get(self.account_id_api_key).video_posts,
            10)

    @tornado.testing.gen_test
    def test_post_video_video_exists_in_db(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&default_thumbnail_url=url.invalid'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        with self._future_wrap_mock(
             patch(pstr)) as cmsdb_download_image_mock:
            cmsdb_download_image_mock.side_effect = [self.random_image]
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,
            '1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals(video.key, internal_video_id)

    @tornado.testing.gen_test
    def test_post_video_bad_id(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=id_with_underscore'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        rjson = json.loads(e.exception.response.body)
        self.assertEquals(e.exception.code, 400)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Invalid video reference')

    @tornado.testing.gen_test
    def test_post_video_thumbnail_exists_in_db(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&default_thumbnail_url=url.invalid'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        with self._future_wrap_mock(
             patch(pstr)) as cmsdb_download_image_mock:
            cmsdb_download_image_mock.side_effect = [self.random_image]
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        thumbnail_id = video.thumbnail_ids[0]
        thumbnail = neondata.ThumbnailMetadata.get(thumbnail_id)
        self.assertEquals(thumbnail_id, thumbnail.key)

    @tornado.testing.gen_test
    def test_post_video_with_dots(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234a.s.cs'\
              '&default_thumbnail_url=url.invalid'\
              '&url=some_url' % (self.account_id_api_key,
                  self.test_i_id)
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        with self._future_wrap_mock(
             patch(pstr)) as cmsdb_download_image_mock:
            cmsdb_download_image_mock.side_effect = [self.random_image]
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key, '1234.ascs')
        self.assertNotEquals(rjson['job_id'],'')
        self.assertNotEquals(rjson['video']['video_id'], '1234.ascs')

    @tornado.testing.gen_test
    def test_post_failed_to_download_thumbnail(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&default_thumbnail_url=url.invalid'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)
        self.im_download_mock.side_effect = neondata.ThumbDownloadError('boom')
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        rjson = json.loads(e.exception.response.body)
        self.assertEquals(e.exception.code, 400)
        self.assertEquals(rjson['error']['message'],
                          'failed to download thumbnail')

    @tornado.testing.gen_test
    def test_post_video_with_duration_and_nthumbs(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&duration=1354&url=some_url&n_thumbs=10' % (
                  self.account_id_api_key,
                  self.test_i_id)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,
            '1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals(1354, video.duration)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[2], 1354.)

        job = yield neondata.NeonApiRequest.get(rjson['job_id'],
                                                self.account_id_api_key,
                                                async=True)
        self.assertEquals(job.api_param, 10)

    @tornado.testing.gen_test
    def test_post_video_with_float_duration(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&duration=1354.54&url=some_url' % (
                  self.account_id_api_key, self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals(1354.54, video.duration)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[2], 1354.54)

    @tornado.testing.gen_test
    def test_post_video_no_duration(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&url=some_url' % (
                  self.account_id_api_key, self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertIsNone(video.duration)

        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertIsNone(cargs[2])

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_valid_one(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&publish_date=2015-08-18T06:36:40.123Z'\
              '&url=some_url' % (self.account_id_api_key,
                  self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals('2015-08-18T06:36:40.123Z', video.publish_date)

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_valid_two(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&publish_date=2015-08-18T06:36:40Z'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals('2015-08-18T06:36:40Z', video.publish_date)

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_valid_three(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&publish_date=2015-08-18'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,'1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals('2015-08-18', video.publish_date)

    @tornado.testing.gen_test
    def test_post_video_with_publish_date_invalid(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?integration_id=%s'\
                  '&external_video_ref=1234ascs'\
                  '&publish_date=2015-0'\
                  '&url=some_url' % (
                      self.account_id_api_key,
                      self.test_i_id)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)
        self.assertEquals(e.exception.response.code, 400)

    @tornado.testing.gen_test
    def test_post_video_missing_url(self):

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?integration_id=%s'\
                  '&external_video_ref=1234ascs'\
                  '&title=a_title' % (self.account_id_api_key,
                      self.test_i_id)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)
        self.assertEquals(e.exception.response.code, 400)

    @tornado.testing.gen_test
    def test_post_url_and_reprocess(self):

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?integration_id=%s'\
                  '&external_video_ref=1234ascs'\
                  '&title=a_title&url=some_url'\
                  '&reprocess=True' % (self.account_id_api_key, self.test_i_id)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        self.assertEquals(e.exception.response.code, 400)

    @tornado.testing.gen_test
    def test_post_video_with_custom_data(self):
        custom_data = urllib.quote(json.dumps({ "a" : 123456 }))
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&custom_data=%s&url=some_url' % (self.account_id_api_key,
                  self.test_i_id, custom_data)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertNotEquals(rjson['job_id'],'')

        internal_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key,
            '1234ascs')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertTrue(video.custom_data is not None)

    @tornado.testing.gen_test
    def test_post_video_with_bad_custom_data(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&custom_data=%s&url=some_url' % (self.account_id_api_key,
                  self.test_i_id, 4)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        self.assertEquals(e.exception.response.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'not a dictionary')

    @tornado.testing.gen_test
    def test_post_two_videos(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        first_job_id = rjson['job_id']
        self.assertNotEquals(first_job_id,'')

        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&url=some_url' % (self.account_id_api_key, self.test_i_id)

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        self.assertEquals(e.exception.response.code, 409)
        rjson = json.loads(e.exception.response.body)
        data = rjson['error']['message']
        self.assertTrue(first_job_id in data)

    @tornado.testing.gen_test
    def test_post_two_videos_with_reprocess(self):
        self.job_write_mock.side_effect = [True, True]

        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs&duration=31'\
              '&url=some_url' % (self.account_id_api_key,
                  self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        first_job_id = rjson['job_id']
        self.assertNotEquals(first_job_id,'')
        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[0], 2)
        self.assertEquals(cargs[2], 31)
        self.job_write_mock.reset_mock()

        # Simulate a job failure
        def _mod(x):
            x.fail_count = 1
            x.try_count = 1
            x.response = {'error': 'Ooops'}
            x.state = neondata.RequestState.INT_ERROR
        neondata.NeonApiRequest.modify(first_job_id, self.account_id_api_key,
                                       _mod)

        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&reprocess=true' % (self.account_id_api_key,
                  self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='POST',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        self.assertEquals(first_job_id, rjson['job_id'])
        self.assertEquals(self.job_write_mock.call_count, 1)
        cargs, kwargs = self.job_write_mock.call_args
        self.assertEquals(cargs[0], 2)
        self.assertEquals(cargs[2], 31)

        # Make sure that the fail and try counts are reset
        job = neondata.NeonApiRequest.get(first_job_id,
                                          self.account_id_api_key)
        self.assertEquals(job.try_count, 0)
        self.assertEquals(job.fail_count, 0)
        self.assertEquals(job.state, neondata.RequestState.REPROCESS)

    @tornado.testing.gen_test
    def test_post_two_videos_with_reprocess_fail(self):
        self.job_write_mock.side_effect = ['message', None]

        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&url=some_url' % (self.account_id_api_key,
                  self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 202)
        rjson = json.loads(response.body)
        first_job_id = rjson['job_id']
        self.assertNotEquals(first_job_id,'')

        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=1234ascs'\
              '&reprocess=1' % (self.account_id_api_key,
                  self.test_i_id)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(self.get_url(url),
                                         body='',
                                         method='POST',
                                         allow_nonstandard_methods=True)
        response = e.exception.response
        self.assertEquals(response.code, 409)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'A job for this video is currently underway')

    @tornado.testing.gen_test
    def test_get_without_video_id(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key,'vid134'))
        yield vm.save(async=True)

        url = '/api/v2/%s/videos' % (self.account_id_api_key)

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
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
        response = yield self.http_client.fetch(self.get_url(url))

        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals({
                'estimated_time_remaining': None,
                'job_id': 'job1',
                'testing_enabled' : False,
                'url' : 'http://someurl.com',
                'state': neondata.ExternalRequestState.PROCESSED,
                'video_id': 'vid1',
                'publish_date' : '2015-06-10',
                'title' : 'Title',
                'tag_id': None
            },
            rjson['videos'][0])

    @tornado.testing.gen_test
    def test_get_single_video_processing(self):
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
        request.state = neondata.RequestState.PROCESSING
        request.save()
        url = '/api/v2/%s/videos?video_id=vid1' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')

        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        vid1 = rjson['videos'][0] 
 
        self.assertEquals(vid1['video_id'], 'vid1')
        self.assertGreaterEqual(vid1['estimated_time_remaining'], 0.0)
  
    @tornado.testing.gen_test
    def test_get_single_video_processing_none_duration(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key, 'vid1'),
            request_id='job1',
            i_id='int2',
            testing_enabled=False,
            duration=None,
            custom_data={'my_data' : 'happygo'},
            tids=['vid1_t1', 'vid1_t2'],
            video_url='http://someurl.com')
        vm.save()
        request = neondata.NeonApiRequest('job1', self.account_id_api_key,
                                          title='Title',
                                          publish_date='2015-06-10')
        request.state = neondata.RequestState.PROCESSING
        request.save()
        url = '/api/v2/%s/videos?video_id=vid1' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')

        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        vid1 = rjson['videos'][0] 
 
        self.assertEquals(vid1['video_id'], 'vid1')
        self.assertEquals(vid1['estimated_time_remaining'], 0.0)  

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
        url = '/api/v2/%s/videos?video_id=vid1'\
              '&fields=state,integration_id,testing_enabled,'\
              'job_id,title,video_id,serving_url,publish_date,'\
              'thumbnails,duration,custom_data,demographic_thumbnails' % (
                  self.account_id_api_key)
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
                                 'do not exist .*viddoesnotexist*')

    def test_get_multiple_video_dne(self):
        url = '/api/v2/%s/videos?'\
              'video_id=viddoesnotexist,vidoerwe,w3asdfa324ad' % (
                  self.account_id_api_key)
        response = self.http_client.fetch(self.get_url(url),
                                          self.stop,
                                          method='GET')

        response = self.wait()
        self.assertEquals(response.code, 404)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'do not exist with id')

    @tornado.testing.gen_test
    def test_get_single_video_with_fields(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'),
            request_id='job1')
        yield vm.save(async=True)
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
        yield vm.save(async=True)
        url = '/api/v2/%s/videos?video_id=vid1'\
              '&fields=created,me_is_invalid' % (
                  self.account_id_api_key)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url),
                method='GET')

        self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'invalid field')

    @tornado.testing.gen_test(timeout=10.0)
    def test_update_video_testing_enabled(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1'))
        yield vm.save(async=True)
        url = '/api/v2/%s/videos?video_id=vid1&testing_enabled=0' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='PUT')

        rjson = json.loads(response.body)
        self.assertFalse(rjson['testing_enabled'])
        self.assertEquals(response.code, 200)

        url = '/api/v2/%s/videos?video_id=vid1&testing_enabled=1' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='PUT')

        rjson = json.loads(response.body)
        self.assertTrue(rjson['testing_enabled'])
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test(timeout=10.0)
    def test_update_video_set_hidden(self):
        tag = neondata.Tag('tag1') 
        yield tag.save(async=True) 
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1'), tag_id='tag1')
        yield vm.save(async=True)
        url = '/api/v2/%s/videos?video_id=vid1&testing_enabled=0' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='PUT')

        rjson = json.loads(response.body)
        self.assertFalse(rjson['testing_enabled'])
        self.assertEquals(response.code, 200)

        url = '/api/v2/%s/videos?video_id=vid1&hidden=1' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='PUT')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)

        tag = yield neondata.Tag.get(vm.tag_id, async=True)
        self.assertTrue(tag.hidden)
        video = yield vm.get(vm.key, async=True)
        self.assertTrue(video.hidden)
 
    @tornado.testing.gen_test(timeout=10.0)
    def test_update_video_default_thumbnail_url(self):
        self.im_download_mock.side_effect = [
            self.random_image, 
            self.random_image]
        tag = neondata.Tag('tag1') 
        yield tag.save(async=True) 
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1'), tag_id='tag1')
        yield vm.save(async=True)
        url = '/api/v2/%s/videos?video_id=vid1&default_thumbnail_url=test' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='PUT')

        vid = yield vm.get(vm.key, async=True)
        self.assertEquals(len(vid.thumbnail_ids), 1)
        first_thumb_id = vid.thumbnail_ids[0]
        tn = yield neondata.ThumbnailMetadata.get(
            first_thumb_id, 
            async=True)

        self.assertEquals(tn.type, neondata.ThumbnailType.DEFAULT)
        self.assertEquals(tn.rank, -1)
        url = '/api/v2/%s/videos?video_id=vid1&default_thumbnail_url=test2' % (
            self.account_id_api_key)

        yield self.http_client.fetch(
            self.get_url(url),
            body='',
            method='PUT')
        vid = yield vm.get(vm.key, async=True)
        self.assertEquals(len(vid.thumbnail_ids), 2)
        second_thumb_id = vid.thumbnail_ids[1]
        tn = yield neondata.ThumbnailMetadata.get(
            second_thumb_id, 
            async=True)
        self.assertEquals(tn.type, neondata.ThumbnailType.DEFAULT)
        self.assertEquals(tn.rank, -2)


    @tornado.testing.gen_test
    def test_update_video_multipart_upload(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(
            self.account_id_api_key,'vid1'))
        yield vm.save(async=True)

        url = self.get_url('/api/v2/{}/videos?video_id=vid1'.format(
            self.account_id_api_key))
        buf = StringIO()
        self.random_image.save(buf, 'JPEG')
        body = MultipartEncoder({
            'video_id': 'vid1',
            'upload': ('image1.jpg', buf.getvalue())})
        headers = {'Content-Type': body.content_type}
        response = yield self.http_client.fetch(
            url,
            headers=headers,
            body=body.to_string(),
            method='PUT')
        self.assertEqual(response.code, 200)
        vid = yield vm.get(vm.key, async=True)
        self.assertEquals(len(vid.thumbnail_ids), 1)
        first_thumb_id = vid.thumbnail_ids[0]
        tn = yield neondata.ThumbnailMetadata.get(
            first_thumb_id, 
            async=True)

        self.assertEquals(tn.type, neondata.ThumbnailType.DEFAULT)
        self.assertEquals(tn.rank, -1)

    @tornado.testing.gen_test
    def test_get_single_video_with_thumbnails_field(self):
        tids = ['testing_vtid_one', 'testing_vtid_two']
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key, 'vid1'),
            tids=tids,
            request_id='job1')
        vm.save()

        for tid in tids:
            base_url = 'http://n3.neon-images.com/xbo/neontn'
            neondata.ThumbnailServingURLs(
                tid,
                size_map={
                    (210, 118): '%s%s_w210_h118.jpg' % (base_url, tid),
                    (120, 67): '%s%s_w120_h67.jpg' % (base_url, tid),
                    (320, 180): '%s%s_w320_h180.jpg' % (base_url, tid)}).save()


        url = '/api/v2/%s/videos?video_id=vid1&fields=created,thumbnails' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url))
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 1)

        thumbnail_array = rjson['videos'][0]['thumbnails']
        self.assertEquals(len(thumbnail_array), 2)
        self.assertItemsEqual([x['thumbnail_id'] for x in thumbnail_array],
                              tids)
        if thumbnail_array[0]['thumbnail_id'] == 'testing_vtid_one':
            thumbnail_one = thumbnail_array[0]
            thumbnail_two = thumbnail_array[1]
        else:
            thumbnail_one = thumbnail_array[1]
            thumbnail_two = thumbnail_array[0]
        self.assertEquals(thumbnail_one['width'], 500)
        self.assertEquals(thumbnail_two['width'], 500)
        self.assertEqual(3, len(thumbnail_two['renditions']))
        rendition = {
            u'aspect_ratio': u'16x9',
            u'width': 120,
            u'height': 67,
            u'url': u'http://n3.neon-images.com/xbo/neontntesting_vtid_two_w120_h67.jpg'
        }
        self.assertIn(rendition, thumbnail_two['renditions'])

        neondata.ThumbnailServingURLs(
            tids[0],
            size_map={}).save()
        response = yield self.http_client.fetch(
            self.get_url(url))
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)

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
    def test_demographic_thumbnails_available(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.account_id_api_key, 'vid1'),
            tids=[],
            non_job_thumb_ids=['testing_vtid_default','testing_vtid_rand'],
            job_results=[
                neondata.VideoJobThumbnailList(
                    thumbnail_ids=['testing_vtid_one'],
                    model_version='localsearch'),
                neondata.VideoJobThumbnailList(
                    gender='F',
                    age='20-29',
                    thumbnail_ids=['testing_vtid_two'],
                    bad_thumbnail_ids=['testing_vtid_bad'],
                    model_version='localsearch')],
            request_id='job1')
        vm.save()

        # Check that the demographic thumbnails are returned as expected
        url = ('/api/v2/%s/videos?video_id=vid1&fields=demographic_thumbnails'
               %(self.account_id_api_key))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(len(rjson['videos'][0]['demographic_thumbnails']), 2)
        self.assertIn('bad_thumbnails',
                         rjson['videos'][0]['demographic_thumbnails'][0])
        self.assertIn('bad_thumbnails',
                         rjson['videos'][0]['demographic_thumbnails'][1])

        # Each demographic response should include the non_job_thumb_ids list
        demos = dict([((x['gender'], x['age']), x['thumbnails'])
                      for x in rjson['videos'][0]['demographic_thumbnails']])
        self.assertItemsEqual([x['thumbnail_id'] for x in demos[(None, None)]],
                              ['testing_vtid_default', 'testing_vtid_rand',
                               'testing_vtid_one'])
        self.assertItemsEqual([x['thumbnail_id'] for x in
                               demos[('F', '20-29')]],
                              ['testing_vtid_default', 'testing_vtid_rand',
                               'testing_vtid_two'])
        # Make sure that the scores are different for the default
        # thumb for the different demographics
        female_default = [x for x in demos[('F', '20-29')]
                          if x['thumbnail_id'] == 'testing_vtid_default'][0]
        general_default = [x for x in demos[(None, None)]
                          if x['thumbnail_id'] == 'testing_vtid_default'][0]
        self.assertNotEqual(female_default['neon_score'],
                            general_default['neon_score'])

        # Now, ask for bad thumbnails too
        url = ('/api/v2/%s/videos?video_id=vid1&fields=%s'
               % (self.account_id_api_key,
                  'demographic_thumbnails,bad_thumbnails'))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(len(rjson['videos'][0]['demographic_thumbnails']), 2)
        thumbs = dict([((x['gender'], x['age']), x['thumbnails'])
                       for x in rjson['videos'][0]['demographic_thumbnails']])
        bad_thumbs = dict([((x['gender'], x['age']), x['bad_thumbnails'])
                       for x in rjson['videos'][0]['demographic_thumbnails']])
        self.assertEquals(bad_thumbs[(None, None)], [])
        self.assertEquals(len(bad_thumbs[('F', '20-29')]), 1)
        self.assertEquals(bad_thumbs[('F', '20-29')][0]['thumbnail_id'],
                          'testing_vtid_bad')
        # No demo score bucket is greater than the model score, so 0.
        self.assertGreater(bad_thumbs[('F', '20-29')][0]['neon_score'], 0)

        # Ask for the thumbnails and they should return the result
        # from the (None, None) demographic response
        url = ('/api/v2/%s/videos?video_id=vid1&fields=%s'
               % (self.account_id_api_key, 'thumbnails'))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertItemsEqual(
            [x['thumbnail_id'] for x in rjson['videos'][0]['thumbnails']],
            ['testing_vtid_default', 'testing_vtid_rand', 'testing_vtid_one'])

    @tornado.testing.gen_test
    def test_demographic_thumbnails_not_available(self):
        # When these is no job list, but the demographic thumbs are asked for
        # This is for backwards compatilibity

        # Start with the default being available (in processing)
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.account_id_api_key, 'vid1'),
            tids=['testing_vtid_default'],
            request_id='job1')
        vm.save()

        url = ('/api/v2/%s/videos?video_id=vid1&fields=%s'
               % (self.account_id_api_key,
                  'demographic_thumbnails,thumbnails'))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(len(rjson['videos'][0]['demographic_thumbnails']), 0)
        self.assertEquals(len(rjson['videos'][0]['thumbnails']), 1)
        self.assertEquals(rjson['videos'][0]['thumbnails'][0]['thumbnail_id'],
                          'testing_vtid_default')

        # Now, after the job is done, more thumbs will appear
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.account_id_api_key, 'vid1'),
            tids=['testing_vtid_default', 'testing_vtid_rand',
                  'testing_vtid_one'],
            request_id='job1')
        vm.save()

        url = ('/api/v2/%s/videos?video_id=vid1&fields=%s'
               % (self.account_id_api_key,
                  'demographic_thumbnails,thumbnails'))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(len(rjson['videos'][0]['demographic_thumbnails']), 1)

        demo = rjson['videos'][0]['demographic_thumbnails'][0]
        self.assertIsNone(demo['gender'])
        self.assertIsNone(demo['age'])
        self.assertItemsEqual([x['thumbnail_id'] for x in demo['thumbnails']],
                              ['testing_vtid_default', 'testing_vtid_rand',
                               'testing_vtid_one'])
        
        self.assertItemsEqual([x['thumbnail_id'] for x in 
                               rjson['videos'][0]['thumbnails']],
                              ['testing_vtid_default', 'testing_vtid_rand',
                               'testing_vtid_one'])

    @tornado.testing.gen_test
    def test_update_video_does_not_exist(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?video_id=vid_does_not_exist'\
                  '&testing_enabled=0' % (self.account_id_api_key)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='PUT',
                allow_nonstandard_methods=True)

        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'vid_does_not_exist')

    @tornado.testing.gen_test
    def test_update_video_title(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=vid1'\
              '&title=kevinsvid&url=some_url' % (
                  self.account_id_api_key,
                  self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            method='POST',
            allow_nonstandard_methods=True)
        rjson = json.loads(response.body)
        job_id = rjson['job_id']
        url = '/api/v2/%s/videos?video_id=vid1&title=vidkevinnew' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            method='PUT',
            allow_nonstandard_methods=True)

        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['title'], 'vidkevinnew')
        request = yield neondata.NeonApiRequest.get(
            job_id,
            self.account_id_api_key,
            async=True)
        self.assertEquals(request.video_title, 'vidkevinnew')

    @tornado.testing.gen_test
    def test_put_video_title_with_tag(self):
        # With no request, nor tag
        video_id = neondata.InternalVideoID.generate(self.account_id_api_key, 'v0')
        video = neondata.VideoMetadata(video_id)
        video.save()
        url = self.get_url('/api/v2/%s/videos' % self.account_id_api_key)
        headers = {'Content-Type': 'application/json'}
        body = json.dumps({
            'video_id': 'v0',
            'title': 'New title'})
        response = yield self.http_client.fetch(
            url,
            method='PUT',
            headers=headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)

        video_id = neondata.InternalVideoID.generate(self.account_id_api_key, 'v1')
        video = neondata.VideoMetadata(video_id, request_id='j1')
        video.save()
        neondata.NeonApiRequest('j1', self.account_id_api_key).save()
        body = json.dumps({
            'video_id': 'v1',
            'title': 'New title'})
        response = yield self.http_client.fetch(
            url,
            method='PUT',
            headers=headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        request = neondata.NeonApiRequest.get('j1', self.account_id_api_key)
        self.assertEqual('New title', request.video_title)

    @tornado.testing.gen_test
    def test_update_video_cb_email(self):
        url = '/api/v2/%s/videos?integration_id=%s'\
              '&external_video_ref=vid1'\
              '&title=kevinsvid&url=some_url' % (
                  self.account_id_api_key,
                  self.test_i_id)

        response = yield self.http_client.fetch(
            self.get_url(url),
            method='POST',
            allow_nonstandard_methods=True)

        rjson = json.loads(response.body)
        job_id = rjson['job_id']
        url = '/api/v2/%s/videos?video_id=vid1&callback_email=a@a.com' % (
            self.account_id_api_key)
        response = yield self.http_client.fetch(
            self.get_url(url),
            method='PUT',
            allow_nonstandard_methods=True)

        self.assertEqual(response.code, 200)
        rjson = json.loads(response.body)
        request = yield neondata.NeonApiRequest.get(
            job_id,
            self.account_id_api_key,
            async=True)
        self.assertEqual(request.callback_email, 'a@a.com')

    @tornado.testing.gen_test
    def test_post_video_sub_required_active(self):
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        so = neondata.NeonUserAccount('kevinacct')
        so.billed_elsewhere = False

        stripe_sub = stripe.Subscription()
        stripe_sub.status = 'active'
        stripe_sub.plan = stripe.Plan(id='pro_monthly')
        so.subscription_information = stripe_sub

        so.verify_subscription_expiry = datetime(2100, 10, 1).strftime(
            "%Y-%m-%d %H:%M:%S.%f")
        yield so.save(async=True)

        with self._future_wrap_mock(
             patch(pstr)) as cmsdb_download_image_mock:
            cmsdb_download_image_mock.side_effect = [self.random_image]

            url = '/api/v2/%s/videos?integration_id=%s'\
                  '&external_video_ref=1234ascs'\
                  '&default_thumbnail_url=url.invalid'\
                  '&title=a_title&url=some_url'\
                  '&thumbnail_ref=ref1' % (so.neon_api_key, self.test_i_id)

            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        # video should be posted with no issues
        video = yield neondata.VideoMetadata.get(
            so.neon_api_key + '_' + '1234ascs',
            async=True)
        self.assertEquals(video.url, 'some_url')

    @tornado.testing.gen_test
    def test_post_video_sub_required_trialing(self):
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        so = neondata.NeonUserAccount('kevinacct')
        so.billed_elsewhere = False

        stripe_sub = stripe.Subscription()
        stripe_sub.status = 'trialing'
        stripe_sub.plan = stripe.Plan(id='pro_monthly')
        so.subscription_information = stripe_sub

        so.verify_subscription_expiry = datetime(2100, 10, 1).strftime(
            "%Y-%m-%d %H:%M:%S.%f")
        yield so.save(async=True)
        with self._future_wrap_mock(
             patch(pstr)) as cmsdb_download_image_mock:
            cmsdb_download_image_mock.side_effect = [self.random_image]

            url = '/api/v2/%s/videos?integration_id=%s'\
                  '&external_video_ref=1234ascs'\
                  '&default_thumbnail_url=url.invalid'\
                  '&title=a_title&url=some_url'\
                  '&thumbnail_ref=ref1' % (so.neon_api_key, self.test_i_id)

            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        # video should be posted with no issues
        video = yield neondata.VideoMetadata.get(
            so.neon_api_key + '_' + '1234ascs',
            async=True)
        self.assertEquals(video.url, 'some_url')

    @tornado.testing.gen_test
    def test_post_video_sub_required_no_good(self):
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        so = neondata.NeonUserAccount('kevinacct')
        so.billed_elsewhere = False

        stripe_sub = stripe.Subscription()
        stripe_sub.status = 'unpaid'
        stripe_sub.plan = stripe.Plan(id='pro_monthly')
        so.subscription_information = stripe_sub

        so.verify_subscription_expiry = datetime(2100, 10, 1).strftime(
            "%Y-%m-%d %H:%M:%S.%f")
        yield so.save(async=True)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/%s/videos?integration_id=%s'\
                  '&external_video_ref=1234ascs'\
                  '&default_thumbnail_url=url.invalid'\
                  '&title=a_title&url=some_url'\
                  '&thumbnail_ref=ref1' % (so.neon_api_key, self.test_i_id)
            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)
        self.assertEquals(e.exception.code, 402)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'Your subscription is not valid')

    @tornado.testing.gen_test
    def test_post_video_sub_check_subscription_state(self):
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        so = neondata.NeonUserAccount('kevinacct')
        so.billed_elsewhere = False
        stripe_sub = stripe.Subscription()
        stripe_sub.status = 'unpaid'
        stripe_sub.plan = stripe.Plan(id='pro_monthly')
        so.subscription_information = stripe_sub

        so.verify_subscription_expiry = datetime(2000, 10, 1).strftime(
            "%Y-%m-%d %H:%M:%S.%f")

        cust_return = stripe.Customer.construct_from({
            'id': 'cus_foo',
            'subscriptions': {
                'object': 'list',
                'url': 'localhost',
            }
        }, 'api_key')

        sub_return = stripe.Subscription()
        sub_return.status = 'active'
        sub_return.plan = stripe.Plan(id='pro_monthly')
        yield so.save(async=True)
        with self._future_wrap_mock(
             patch(pstr)) as cmsdb_download_image_mock,\
             patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:

            sr.return_value.subscriptions.all.return_value = {
                'data' : [sub_return] }
            cmsdb_download_image_mock.side_effect = [self.random_image]

            url = '/api/v2/%s/videos?integration_id=%s'\
                  '&external_video_ref=1234ascs'\
                  '&default_thumbnail_url=url.invalid'\
                  '&title=a_title&url=some_url'\
                  '&thumbnail_ref=ref1' % (so.neon_api_key, self.test_i_id)

            response = yield self.http_client.fetch(
                self.get_url(url),
                body='',
                method='POST',
                allow_nonstandard_methods=True)

        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)

        # verify we added an hour to subscription_expiry
        self.assertTrue(
            dateutil.parser.parse(
                acct.verify_subscription_expiry) > datetime.utcnow())
        self.assertEquals(
            acct.subscription_info['status'],
            'active')

        # video should be posted with no issues
        video = yield neondata.VideoMetadata.get(
            so.neon_api_key + '_' + '1234ascs',
            async=True)
        self.assertEquals(video.url, 'some_url')

    @tornado.testing.gen_test
    def test_post_video_sub_check_subscription_exception(self):
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        so = neondata.NeonUserAccount('kevinacct')
        so.billed_elsewhere = False

        stripe_sub = stripe.Subscription()
        stripe_sub.status = 'trialing'
        stripe_sub.plan = stripe.Plan(id='pro_monthly')
        so.subscription_information = stripe_sub

        so.verify_subscription_expiry = datetime(2000, 10, 1).strftime(
            "%Y-%m-%d %H:%M:%S.%f")

        cust_return = stripe.Customer.construct_from({
            'id': 'cus_foo',
            'subscriptions': {
                'object': 'list',
                'url': 'localhost',
            }
        }, 'api_key')

        yield so.save(async=True)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.return_value.subscriptions.all.side_effect = [
                    Exception('blah')]
                url = '/api/v2/%s/videos?integration_id=%s'\
                      '&external_video_ref=1234ascs'\
                      '&default_thumbnail_url=url.invalid'\
                      '&title=a_title&url=some_url'\
                      '&thumbnail_ref=ref1' % (so.neon_api_key, self.test_i_id)

                yield self.http_client.fetch(
                    self.get_url(url),
                    body='',
                    method='POST',
                    allow_nonstandard_methods=True)

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'blah')

    @tornado.testing.gen_test
    def test_post_video_body(self):
        pstr = 'cmsdb.neondata.VideoMetadata.download_image_from_url'
        with self._future_wrap_mock(
           patch(pstr)) as cmock:
            cmock.side_effect = [self.random_image]
            body = {
                'external_video_ref': '1234ascs33',
                'url': 'some_url',
                'title': 'de pol\xc3\xb6tica de los EE.UU.-'.decode('utf-8'),
                'default_thumbnail_url': 'invalid',
                'thumbnail_ref': 'ref1'
            }
            header = {"Content-Type": "application/json"}
            url = '/api/v2/%s/videos' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                body=json.dumps(body),
                method='POST',
                headers=header)
        self.assertEquals(response.code, 202)
        video = yield neondata.VideoMetadata.get('%s_%s' % (
            self.account_id_api_key, '1234ascs33'), async=True)
        self.assertEquals(video.url, 'some_url')

    @tornado.testing.gen_test
    def test_post_video_body_nones(self):
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        with self._future_wrap_mock(
           patch(pstr)) as cmock:
            cmock.side_effect = [self.random_image]
            body = {
                'external_video_ref': '1234ascs33',
                'url': 'some_url',
                'title': 'de pol\xc3\xb6tica de los EE.UU.-'.decode('utf-8'),
                'default_thumbnail_url': 'invalid',
                'thumbnail_ref': 'ref1',
                'duration' : None, 
                'publish_date': None,
                'thumbnail_ref': None, 
                'custom_data': None
            }
            header = {"Content-Type": "application/json"}
            url = '/api/v2/%s/videos' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                body=json.dumps(body),
                method='POST',
                headers=header)
        self.assertEquals(response.code, 202) 
        video = yield neondata.VideoMetadata.get('%s_%s' % (
            self.account_id_api_key, '1234ascs33'), async=True)
        self.assertEquals(video.url, 'some_url')
        self.assertEquals(video.publish_date, None)
        self.assertEquals(video.custom_data, {})

    @tornado.testing.gen_test
    def test_post_video_too_big_duration(self):
        pstr = 'cmsdb.neondata.ThumbnailMetadata.download_image_from_url'
        with self._future_wrap_mock(
           patch(pstr)) as cmock:
            cmock.side_effect = [self.random_image]
            body = {
                'external_video_ref': '1234ascs33',
                'url': 'some_url',
                'title': 'de pol\xc3\xb6tica de los EE.UU.-'.decode('utf-8'),
                'default_thumbnail_url': 'invalid',
                'thumbnail_ref': 'ref1',
                'duration': 8532234.3,
            }
            header = {"Content-Type": "application/json"}
            url = '/api/v2/%s/videos' % (self.account_id_api_key)
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                response = yield self.http_client.fetch(self.get_url(url),
                    body=json.dumps(body),
                    method='POST',
                    headers=header)
        self.assertEquals(e.exception.code, 400)

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

    @tornado.testing.gen_test
    def test_get_video_with_clips(self):
        vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.account_id_api_key, 'vid1'),
            request_id='job1',
            non_job_clip_ids=['clip1'],
            job_results=[
                neondata.VideoJobThumbnailList(
                    clip_ids=['clip2'],
                    model_version='localsearch'),
                neondata.VideoJobThumbnailList(
                    gender='F',
                    age='20-29',
                    clip_ids=['clip3'],
                    model_version='localsearch')])
        vm.save()

        url = ('/api/v2/%s/videos?video_id=vid1&fields=demographic_clip_ids'
               %(self.account_id_api_key))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')

        rjson = json.loads(response.body)
        self.assertEquals(len(rjson['videos'][0]['demographic_clip_ids']), 2)
        clip_lists = {x['gender']: x for x in 
                      rjson['videos'][0]['demographic_clip_ids']}
        self.assertEquals(clip_lists[None]['gender'], None)
        self.assertEquals(clip_lists[None]['age'], None)
        self.assertItemsEqual(clip_lists[None]['clip_ids'], ['clip1', 'clip2'])

        self.assertEquals(clip_lists['F']['gender'], 'F')
        self.assertEquals(clip_lists['F']['age'], '20-29')
        self.assertItemsEqual(clip_lists['F']['clip_ids'], ['clip1', 'clip3'])
        


class TestThumbnailHandler(TestControllersBase):
    def setUp(self):

        self.user = neondata.NeonUserAccount(uuid.uuid1().hex, name='testingme')
        self.user.save()
        self.account_id_api_key = self.user.neon_api_key

        np.random.seed(2341235)
        self.thumb = neondata.ThumbnailMetadata(
            '%s_vid0_testingtid' % self.account_id_api_key,
            width=500,
            urls=['s'],
            features=np.random.rand(1024),
            model_version='20160713-test',
            internal_vid='%s_vid0' % self.account_id_api_key)
        self.thumb.save()

        # I use "video_id" to be the id without the account prefix,
        # and video.get_id() to be the internal video id used for lookup.
        self.video_id = self.video0_id = 'vid0'
        self.video = self.video0 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.account_id_api_key,
                self.video_id),
            tids=[self.thumb.get_id()])
        self.video0.save()
        self.video1_id = 'vid1'
        self.video1 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.account_id_api_key,
                self.video1_id))
        self.video1.save()

        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]
        self.im_download_mocker = patch(
            'cvutils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.random_image2 = PILImageUtils.create_random_image(480, 640)
        self.random_image3 = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [
            self.random_image,
            self.random_image2,
            self.random_image3]
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True

        neondata.Tag(
            'tag_0',
            name='A',
            account_id=self.account_id_api_key).save()
        neondata.Tag(
            'tag_2',
            name='B',
            account_id=self.account_id_api_key).save()
        neondata.TagThumbnail.save(tag_id='tag_2', thumbnail_id='testingtid')

        self.model_connect = patch('model.predictor.DeepnetPredictor.connect')
        self.model_connect.start()
        self.model_predict_mocker = patch(
           'model.predictor.DeepnetPredictor.predict')
        self.model_predict_mock = self._future_wrap_mock(
            self.model_predict_mocker.start())
        self.model_predict_mock.side_effect = [(
            .5,
            np.random.rand(1024),
            '20160713-test')]
        self.verify_account_mock.return_value = True

        super(TestThumbnailHandler, self).setUp()

    def tearDown(self):
        self.model_connect.stop()
        self.model_predict_mocker.stop()
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
        self.verify_account_mocker.stop()
        super(TestThumbnailHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_add_new_thumbnail_by_url(self):

        thumbnail_ref = 'kevin'
        image_filename = 'blah.jpg'
        _url = '/api/v2/{}/thumbnails?video_id={}&thumbnail_ref={}&url={}&tag_id={}'
        url = self.get_url(_url.format(
            self.account_id_api_key,
            self.video_id,
            thumbnail_ref,
            image_filename,
            'tag_0,tag_1,tag_2'))

        response = yield self.http_client.fetch(url, body='', method='POST')
        self.assertEqual(response.code, 202)

        video = neondata.VideoMetadata.get(self.video.get_id())
        self.assertEqual(len(video.thumbnail_ids), 2)
        self.assertEqual(self.im_download_mock.call_args[0][0], image_filename)

        thumbnail = neondata.ThumbnailMetadata.get(video.thumbnail_ids[-1])
        self.assertEqual(thumbnail.external_id, thumbnail_ref)
        self.assertEqual(thumbnail.video_id, self.video.get_id())

        url2 = self.get_url('/api/v2/{}/thumbnails/?thumbnail_id={}'.format(
            self.account_id_api_key,
            thumbnail.get_id()))
        response = yield self.http_client.fetch(url2)
        rjson = json.loads(response.body)
        valid_tag_ids = {'tag_0', 'tag_2'}
        thumb1 = rjson['thumbnails'][0]
        self.assertEqual(set(thumb1['tag_ids']), valid_tag_ids)

        # Try two.
        _url = '/api/v2/{}/thumbnails?thumbnail_ref={}&url={}&tag_id={}'
        url = self.get_url(_url.format(
            self.account_id_api_key,
            thumbnail_ref,
            '1.jpg,2.jpg',
            'tag_0,tag_1,tag_2'))

        r = yield self.http_client.fetch(url, body='', method='POST')
        self.assertEqual(202, r.code)
        rj = json.loads(r.body)['thumbnails']
        new_ids = [t['thumbnail_id'] for t in rj]
        new_thumbs = neondata.ThumbnailMetadata.get_many(new_ids)
        self.assertEqual(2, len(new_thumbs))

    @tornado.testing.gen_test
    def test_add_new_thumbnail_with_some_there(self):

        # Add another thumbnail to the video.
        rand_thumb = neondata.ThumbnailMetadata(
            '%s_rand' % self.video.get_id(),
            ttype=neondata.ThumbnailType.RANDOM,
            rank=1,
            urls=['rand.jpg'])
        rand_thumb.save()
        neondata.VideoMetadata.modify(
            self.video.get_id(),
            lambda x: x.thumbnail_ids.append(rand_thumb.key))

        thumbnail_ref = 'kevin'
        image_url = 'blah.jpg'
        url = self.get_url(
            '/api/v2/{}/thumbnails?video_id={}&thumbnail_ref={}&url={}'.format(
                self.account_id_api_key,
                self.video_id,
                thumbnail_ref,
                'blah.jpg'))
        response = yield self.http_client.fetch(url, body='', method='POST')
        self.assertEqual(response.code, 202)

        video = neondata.VideoMetadata.get(self.video.get_id())
        self.assertEqual(len(video.thumbnail_ids), 3)
        self.assertEqual(self.im_download_mock.call_args[0][0], 'blah.jpg')

        thumbnail = neondata.ThumbnailMetadata.get(video.thumbnail_ids[-1])
        self.assertEqual(thumbnail.external_id, 'kevin')
        self.assertEqual(thumbnail.video_id, video.get_id())
        self.assertEqual(thumbnail.type, neondata.ThumbnailType.CUSTOMUPLOAD)
        self.assertEqual(thumbnail.rank, 0)

    @tornado.testing.gen_test
    def test_add_new_thumbnail_by_body(self):

        start_thumb_ct = len(self.video.thumbnail_ids)

        thumbnail_ref = 'kevin'
        url = self.get_url('/api/v2/{}/thumbnails?thumbnail_ref={}'.format(
            self.account_id_api_key, thumbnail_ref))
        buf = StringIO()
        self.random_image.save(buf, 'JPEG')
        body = MultipartEncoder({
            'video_id': self.video_id,
            'upload': ('image1.jpg', buf.getvalue())})
        headers = {'Content-Type': body.content_type}

        self.im_download_mock.side_effect = Exception('No download allowed')
        response = yield self.http_client.fetch(
            url,
            headers=headers,
            body=body.to_string(),
            method='POST')
        self.assertEqual(response.code, 202)

        video = neondata.VideoMetadata.get(self.video.get_id())
        self.assertEqual(start_thumb_ct + 1, len(video.thumbnail_ids))

        thumbnail = neondata.ThumbnailMetadata.get(video.thumbnail_ids[-1])
        self.assertEqual(thumbnail.external_id, 'kevin')
        self.assertEqual(thumbnail.video_id, video.get_id())
        self.assertEqual(thumbnail.type, neondata.ThumbnailType.CUSTOMUPLOAD)
        self.assertEqual(thumbnail.rank, 0)

    @tornado.testing.gen_test
    def test_add_new_thumbnail_by_body_no_video(self):

        thumbnail_ref = 'kevin'
        url = self.get_url('/api/v2/{}/thumbnails?thumbnail_ref={}'.format(
            self.account_id_api_key, thumbnail_ref))
        buf = StringIO()
        self.random_image.save(buf, 'JPEG')
        body = MultipartEncoder({
            'upload': ('image1.jpg', buf.getvalue(), 'multipart/form-data')})
        headers = {'Content-Type': body.content_type}

        self.im_download_mock.side_effect = Exception('No download allowed')
        response = yield self.http_client.fetch(
            url,
            headers=headers,
            body=body.to_string(),
            method='POST')
        self.assertEqual(response.code, 202)
        r = json.loads(response.body)
        r = r['thumbnails'][0]

        thumbnail = neondata.ThumbnailMetadata.get(r['thumbnail_id'])
        expect_video_id = neondata.InternalVideoID.generate(
            self.account_id_api_key)

        self.assertEqual(expect_video_id, thumbnail.video_id)
        self.assertEqual(thumbnail.external_id, thumbnail_ref)
        self.assertIn('some_cdn_url.jpg', thumbnail.urls)
        thumbnail_id_parts = r['thumbnail_id'].split('_')
        self.assertEqual(3, len(thumbnail_id_parts))
        self.assertEqual(self.account_id_api_key, thumbnail_id_parts[0])
        self.assertEqual(neondata.InternalVideoID.NOVIDEO, thumbnail_id_parts[1])
        self.assertIsNotNone(thumbnail_id_parts[2])
        # Expect that scoring has been done.
        self.assertIsNotNone(thumbnail.model_score)
        self.assertIsNotNone(thumbnail.model_version)
        print(url)

    @tornado.testing.gen_test
    def test_add_two_thumbs_by_body(self):

        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_ref=a&tag_id=tag_0,tag_2' %
            self.account_id_api_key)

        buf = StringIO()
        self.random_image.save(buf, 'JPEG')
        buf2 = StringIO()
        self.random_image2.save(buf2, 'JPEG')

        #body = MultipartEncoder({
        #    'upload': ('image1.jpg', buf.getvalue(), 'multipart/form-data')})
        # Try two.
        body = MultipartEncoder([
            ('upload', ('image1.jpg', buf.getvalue(), 'multipart/form-data')),
            ('upload', ('image2.jpg', buf2.getvalue(), 'multipart/form-data'))])

        headers = {'Content-Type': body.content_type}

        response = yield self.http_client.fetch(
            url,
            headers=headers,
            body=body.to_string(),
            method='POST')

        self.assertEqual(response.code, 202)
        r = json.loads(response.body)
        new_thumbs = r['thumbnails']
        self.assertEqual(2, len(new_thumbs))
        self.assertEqual('a', new_thumbs[0]['external_ref'])
        new_ids = [t['thumbnail_id'] for t in new_thumbs]
        thumbs = neondata.ThumbnailMetadata.get_many(new_ids)
        self.assertEqual(2, len(thumbs))
        tag_thumb_ids = neondata.TagThumbnail.get_many(tag_id=['tag_0', 'tag_2'])
        self.assertEqual(set(new_ids), set(tag_thumb_ids['tag_0']))
        # Tag 0 has a thumbnail from setUp, testingtid.
        ids = new_ids + ['testingtid']
        self.assertEqual(set(ids), set(tag_thumb_ids['tag_2']))


    @tornado.testing.gen_test
    def test_bad_add_new_thumbnail_no_upload(self):
        thumbnail_ref = 'kevin'
        url = self.get_url('/api/v2/{}/thumbnails?video_id={}&thumbnail_ref={}'.format(
            self.account_id_api_key, self.video_id, thumbnail_ref))
        self.im_download_mock.side_effect = Exception('No download')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                url,
                body='',
                method='POST')
        self.assertEquals(e.exception.code, 400)

        url = self.get_url('/api/v2/{}/thumbnails?thumbnail_ref={}'.format(
            self.account_id_api_key, thumbnail_ref))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                url,
                body='',
                method='POST')
        self.assertEquals(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_bad_add_new_thumbnail_not_image(self):
        url = self.get_url('/api/v2/{}/thumbnails?video_id={}'.format(
            self.account_id_api_key,
            self.video_id))

        # Make a random, non-image file.
        buf = StringIO()
        buf.write(bytearray(os.urandom(100000)))
        body = MultipartEncoder({
            'upload': ('image1.jpg', buf.getvalue(), 'multipart/form-data')})
        headers = {'Content-Type': body.content_type}

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                url,
                headers=headers,
                body=body.to_string(),
                method='POST')
        self.assertEqual(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_add_two_new_thumbnails(self):

        start_thumb_ct = len(self.video.thumbnail_ids)

        url = self.get_url('/api/v2/%s/thumbnails?video_id=%s&url=%s' % (
            self.account_id_api_key,
            self.video_id,
            'blah.jpg'))
        response = yield self.http_client.fetch(url, body='', method='POST')

        self.assertEqual(response.code, 202)
        self.im_download_mock.side_effect = [PILImageUtils.create_random_image(480, 640)]
        self.assertEqual(self.im_download_mock.call_args[0][0], 'blah.jpg')

        url = self.get_url('/api/v2/%s/thumbnails?video_id=%s&url=%s' % (
            self.account_id_api_key,
            self.video_id,
            'blah2.jpg'))
        response = yield self.http_client.fetch(url, body='', method='POST')

        self.assertEqual(self.im_download_mock.call_args[0][0], 'blah2.jpg')
        self.assertEqual(response.code, 202)
        video = neondata.VideoMetadata.get(self.video.get_id())
        thumbnail_ids = video.thumbnail_ids
        self.assertEqual(start_thumb_ct + 2, len(video.thumbnail_ids))

    @tornado.testing.gen_test
    def test_get_thumbnail_exists(self):
        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s&fields=%s' % (
            self.account_id_api_key,
            self.thumb.get_id(),
            'thumbnail_id,width'))
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertEqual(rjson['width'], 500)
        self.assertEqual(rjson['thumbnail_id'], self.thumb.get_id())

    @tornado.testing.gen_test
    def test_feature_ids(self):

        thumbnail_id = '%s_vid0_testingtid' % self.account_id_api_key
        url = '/api/v2/%s/thumbnails?thumbnail_id=%s&fields=%s' % (
            self.account_id_api_key, thumbnail_id, 'thumbnail_id,feature_ids')
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)['thumbnails'][0]

        feature_ids = rjson['feature_ids']
        self.assertEqual(len(feature_ids), 1024)
        self.assertEqual(len(feature_ids[0]), 2)
        self.assertEqual(feature_ids, sorted(feature_ids, reverse=True,
                                             key=lambda x: x[1]))

        # Check the format of the keys
        splits = feature_ids[0][0].split('_')
        self.assertEqual(len(splits), 2)
        self.assertEqual(splits[0], '20160713-test')
        self.assertGreaterEqual(int(splits[1]), 0)
        self.assertLess(int(splits[1]), 1024)

    @tornado.testing.gen_test
    def test_feature_values(self):
        thumbnail_id = '%s_vid0_testingtid' % self.account_id_api_key
        url = '/api/v2/%s/thumbnails?thumbnail_id=%s&fields=%s' % (
            self.account_id_api_key, thumbnail_id, 'thumbnail_id,features')
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)['thumbnails'][0]

        feature_vals = rjson['features']
        self.assertEqual(len(feature_vals), 1024)
        self.assertEquals(feature_vals, list(self.thumb.features))

    @tornado.testing.gen_test
    def test_share_token_allows_get(self):
        payload = {
            'content_type': 'VideoMetadata',
            'content_id': self.video.get_id()}
        share_token = ShareJWTHelper.encode(payload)
        self.video.share_token = share_token
        self.video.save()
        headers = {'Content-Type': 'application/json'}

        url = self.get_url('/api/v2/{}/thumbnails/?thumbnail_id={}&share_token={}'.format(
            self.account_id_api_key,
            self.thumb.get_id(),
            share_token))

        self.verify_account_mocker.stop()

        r = yield self.http_client.fetch(url, headers=headers)
        self.assertEqual(ResponseCode.HTTP_OK, r.code)
        rjson = json.loads(r.body)
        self.assertEqual(self.thumb.get_id(), rjson['thumbnails'][0]['thumbnail_id'])

        self.verify_account_mocker.start()

    @tornado.testing.gen_test
    def test_share_token_wrong_thumbnail(self):
        payload = {
            'content_type': 'VideoMetadata',
            'content_id': self.video.get_id()}
        share_token = ShareJWTHelper.encode(payload)
        self.video.share_token = share_token
        self.video.save()
        headers = {'Content-Type': 'application/json'}
        bad_tid = 'notmyacct_notmyvideo_t0'
        neondata.ThumbnailMetadata(bad_tid).save()

        url = self.get_url('/api/v2/{aid}/thumbnails/?thumbnail_id={tid}&share_token={st}'.format(
            aid=self.user.get_api_key(),
            tid=bad_tid,
            st=share_token))

        self.verify_account_mocker.stop()
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url, headers=headers)
        self.assertEqual(403, e.exception.code)

        # Another case is that the thumbnail is of the account
        # but not the shared video. This is a 403.
        bad_tid_2 = '%s_notmyvideo_t0' % self.account_id_api_key
        neondata.ThumbnailMetadata(bad_tid_2).save()

        url = self.get_url('/api/v2/{aid}/thumbnails/?thumbnail_id={tid}&share_token={st}'.format(
            aid=self.user.get_api_key(),
            tid=bad_tid_2,
            st=share_token))

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url, headers=headers)
        self.assertEqual(403, e.exception.code)

        self.verify_account_mocker.start()

    @tornado.testing.gen_test
    def test_no_post_put_with_share_token(self):
        payload = {
            'content_type': 'VideoMetadata',
            'content_id': self.video.get_id()}
        share_token = ShareJWTHelper.encode(payload)
        headers = {'Content-Type': 'application/json'}

        # Let's try to add a thumbnail to the existing video.
        body = json.dumps({
            'video_id': 'vid0',
            'url': 'https://instagram.com/1234.jpg',
            'thumbnail_ref': 'my-photo'
        })
        url = self.get_url('/api/v2/{aid}/thumbnails/'.format(
            aid=self.user.get_api_key()))

        self.verify_account_mocker.stop()

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                url,
                body=body,
                headers=headers,
                method='POST')
        self.assertEqual(401, e.exception.code)
        self.im_download_mock.assert_not_called()

        # Now try to update the existing thumb.
        body = json.dumps({
            'video_id': 'vid0',
            'thumbnail_id': self.thumb.get_id(),
            'enabled': False
        })
        url = self.get_url('/api/v2/{aid}/thumbnails/'.format(
            aid=self.user.get_api_key()))

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                url,
                body=body,
                headers=headers,
                method='PUT')
        self.assertEqual(401, e.exception.code)
        self.im_download_mock.assert_not_called()

        self.verify_account_mocker.start()


    @tornado.testing.gen_test
    def test_get_multiple_thumbnails(self):

        thumb2 = neondata.ThumbnailMetadata(
            '%s_%s' % (self.video.get_id(), 'testingtid2'),
            width=750,
            urls=['s'],
            features=np.random.rand(1024),
            model_version='20160713-test')
        thumb2.save()

        tids = [self.thumb.get_id(), thumb2.get_id()]

        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s&fields=%s' % (
            self.account_id_api_key,
            ','.join(tids),
            'thumbnail_id,width'))

        response = yield self.http_client.fetch(url)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)

        self.assertEqual(rjson['thumb_count'],2)
        tn1 = rjson['thumbnails'][0]
        self.assertEquals(tn1['width'], 500)
        self.assertEquals(tn1['thumbnail_id'], self.thumb.get_id())
        tn2 = rjson['thumbnails'][1]
        self.assertEquals(tn2['width'], 750)
        self.assertEquals(tn2['thumbnail_id'], thumb2.get_id())

    @tornado.testing.gen_test
    def test_get_one_thumbnail_not_mine(self):

        thumb2 = neondata.ThumbnailMetadata(
            '%s_%s' % (self.video.get_id(), 'testingtid2'))
        thumb2.save()

        not_my_thumb = neondata.ThumbnailMetadata(
            '%s_%s' % ('somebody_else', 'testingtid3'))
        not_my_thumb.save()

        # Try all kinds of different combinations.
        for tids in [
            [self.thumb.get_id(), not_my_thumb.get_id()],
            [self.thumb.get_id(), not_my_thumb.get_id(), thumb2.get_id()],
            [not_my_thumb.get_id(), self.thumb.get_id()],
            [not_my_thumb.get_id()],
            [not_my_thumb.get_id(), not_my_thumb.get_id()]]:

            url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s' % (
                self.account_id_api_key,
                ','.join(tids)))

            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                yield self.http_client.fetch(url)
            self.assertEqual(403, e.exception.code)

    @tornado.testing.gen_test
    def test_score_map(self):

        thumb = neondata.ThumbnailMetadata(
            '%s_%s' % (self.video.get_id(), 'a'),
            urls=['http://asdf.com/1.jpg'],
            model_score='0.31337',
            model_version='p_20151216_localsearch_v2'
        )
        thumb.save()

        url = '/api/v2/%s/thumbnails?thumbnail_id=%s' % (
            self.account_id_api_key,
            thumb.get_id())
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertEqual(rjson['neon_score'], 7)

        thumb2 = neondata.ThumbnailMetadata(
            '%s_%s' % (self.video.get_id(), 'a'),
            urls=['http://asdf.com/1.jpg'],
            model_score='0.31337',
            model_version='local_search_input_20160523-aqv1.1.250'
        )
        thumb2.save()

        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertEqual(rjson['neon_score'], 13)

    @tornado.testing.gen_test
    def test_get_thumbnail_with_renditions(self):

        base_url = 'http://n3.neon-images.com/xbo/neontn'
        tid = self.thumb.get_id()
        neondata.ThumbnailServingURLs(
            tid,
            size_map={
                (210, 118): '%s%s_w210_h118.jpg' % (base_url, tid),
                (160, 90): '%s%s_w160_h90.jpg' % (base_url, tid),
                (320, 180): '%s%s_w320_h180.jpg' % (base_url, tid)}).save()

        url = self.get_url(
            '/api/v2/{ac}/thumbnails?thumbnail_id={tid}&fields={fs}'.format(
                tid=tid,
                ac=self.account_id_api_key,
                fs='thumbnail_id,renditions'))

        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertEqual(3, len(rjson['renditions']))
        self.assertIn({
            u'aspect_ratio': u'105x59',
            u'height': 118,
            u'url': u'%s%s_w210_h118.jpg' % (base_url, tid),
            u'width': 210}, rjson['renditions'])

        neondata.ThumbnailServingURLs(
            tid,
            sizes=[(210, 118), (160, 90), (320, 180)])

        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertEqual(3, len(rjson['renditions']))
        self.assertIn({
            u'aspect_ratio': u'105x59',
            u'height': 118,
            u'url': u'%s%s_w210_h118.jpg' % (base_url, tid),
            u'width': 210}, rjson['renditions'])

    @tornado.testing.gen_test
    def test_get_thumbnail_does_not_exist(self):

        tid = '%s_%s_%s' % (
            self.account_id_api_key,
            self.video_id,
            'doesnotexist')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = ('/api/v2/%s/thumbnails?thumbnail_id=%s' % (
                self.account_id_api_key,
                tid))
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
        self.assertEqual(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'], 'do not exist')

    @tornado.testing.gen_test
    def test_thumbnail_update_enabled(self):
        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s' % (
            self.account_id_api_key,
            self.thumb.get_id()))
        response = yield self.http_client.fetch(url)
        old_tn = json.loads(response.body)

        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s&enabled=0' % (
            self.account_id_api_key,
            self.thumb.get_id()))
        response = yield self.http_client.fetch(url, body='', method='PUT')
        new_tn = json.loads(response.body)
        self.assertEquals(new_tn['enabled'],False)

        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s&enabled=1' % (
            self.account_id_api_key,
            self.thumb.get_id()))
        response = yield self.http_client.fetch(url, body='', method='PUT')
        new_tn = json.loads(response.body)
        self.assertEquals(new_tn['enabled'], True)

    @tornado.testing.gen_test
    def test_score_from_feature_vector(self):
        features = np.random.rand(1024)
        tid = '%s_vid_%s' % (self.account_id_api_key, 'featandscore')
        neondata.ThumbnailMetadata(
            tid,
            urls=['http://asdf.com/1.jpg'],
            model_score='-1e-3',
            model_version='20160713-test',
            features=features
        ).save()
        url = '/api/v2/%s/thumbnails?thumbnail_id=%s' % (
            self.account_id_api_key,
            tid)
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertGreater(rjson['neon_score'], 0)

        tid2 = '%s_vid_%s' % (self.account_id_api_key, 'featonly')
        neondata.ThumbnailMetadata(
            tid2,
            urls=['http://asdf.com/1.jpg'],
            model_score=None,
            model_version='20160713-test',
            features=features
        ).save()
        url = '/api/v2/%s/thumbnails?thumbnail_id=%s' % (
            self.account_id_api_key,
            tid2)
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertGreater(rjson['neon_score'], 0)

        tid3 = '%s_vid_%s' % (self.account_id_api_key, 'scoreonly')
        neondata.ThumbnailMetadata(
            tid3,
            urls=['http://asdf.com/1.jpg'],
            model_score='-1e-3',
            model_version='20160713-test',
            features=None
        ).save()
        url = '/api/v2/%s/thumbnails?thumbnail_id=%s' % (
            self.account_id_api_key,
            tid3)
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)['thumbnails'][0]
        self.assertGreater(rjson['neon_score'], 0)

    @tornado.testing.gen_test
    def test_thumbnail_update_no_params(self):
        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s' % (
            self.account_id_api_key,
            self.thumb.get_id()))
        response = yield self.http_client.fetch(url)
        old_tn = json.loads(response.body)['thumbnails'][0]

        url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=%s' % (
            self.account_id_api_key,
            self.thumb.get_id()))
        response = yield self.http_client.fetch(url, body='', method='PUT')
        new_tn = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(new_tn['enabled'],old_tn['enabled'])

    @tornado.testing.gen_test
    def test_delete_thumbnail_not_implemented(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = self.get_url('/api/v2/%s/thumbnails?thumbnail_id=12234' % (
                self.account_id_api_key))
            response = yield self.http_client.fetch(url, method='DELETE')
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
        self.http_mocker.stop()
        super(TestHealthCheckHandler, self).tearDown()

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
        self.assertEquals(response.code, 404)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['error']['message'], 
            'unable to get to the v1 api')


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
        self.verify_account_mock.return_value = True
        super(TestVideoStatsHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestVideoStatsHandler, self).tearDown()


    @tornado.testing.gen_test
    def test_one_video_id(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'),
                                    tids=[])
        vm.save()
        vid_status = neondata.VideoStatus(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'),
                                          experiment_state=neondata.ExperimentState.COMPLETE)
        vid_status.winner_tid = '%s_t2' % neondata.InternalVideoID.generate(self.account_id_api_key,'vid1')
        vid_status.save()

        fields = ['video_id', 'experiment_state', 'winner_thumbnail', 'created']

        url = '/api/v2/%s/stats/videos?video_id=vid1&fields=%s' % (
            self.account_id_api_key, ','.join(fields))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['count'], 1)
        statistic_one = rjson['statistics'][0]
        self.assertEquals(statistic_one['experiment_state'], neondata.ExperimentState.COMPLETE)
        self.assertIn('created', statistic_one)
        self.assertNotIn('updated', statistic_one)


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
        fields = ['video_id', 'experiment_state', 'winner_thumbnail', 'created']
        url = '/api/v2/%s/stats/videos?video_id=vid1,vid2&fields=%s' % (
            self.account_id_api_key, ','.join(fields))
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['count'], 2)
        statistic_one = next(s for s in rjson['statistics']
            if s['video_id'] == 'vid1')
        self.assertEqual(statistic_one['experiment_state'],
                          neondata.ExperimentState.COMPLETE)
        statistic_two = next(s for s in rjson['statistics']
            if s['video_id'] == 'vid2')
        self.assertEqual(statistic_two['experiment_state'],
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
        self.verify_account_mock.return_value = True
        neondata.ThumbnailMetadata('testingtid', width=800).save()
        neondata.ThumbnailMetadata('testing_vtid_one', width=500).save()
        neondata.ThumbnailMetadata('testing_vtid_two', width=500).save()
        super(TestThumbnailStatsHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestThumbnailStatsHandler, self).tearDown()


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
        self.assertEqual(response.code, 200)
        self.assertEqual(rjson['count'], 2)
        status_one = next(s for s in rjson['statistics']
            if s['thumbnail_id'] == 'testingtid')
        status_two = next(s for s in rjson['statistics']
            if s['thumbnail_id'] == 'testing_vtid_one')
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
        status_one = next(s for s in rjson['statistics']
            if s['thumbnail_id'] == 'testingtid')
        status_two = next(s for s in rjson['statistics']
            if s['thumbnail_id'] == 'testing_vtid_one')
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


class TestGetSharedContent(TestControllersBase):

    def setUp(self):
        super(TestControllersBase, self).setUp()
        # Add account, video, request.
        neondata.NeonUserAccount('u', 'u').save()
        self.video1 = neondata.VideoMetadata('u_1', request_id='1')
        # Generate share token.
        content_type = neondata.VideoMetadata.__name__
        payload = {
            'content_type': content_type,
            'content_id': self.video1.get_id()
        }
        share_token = ShareJWTHelper.encode(payload)
        self.video1.share_token = share_token
        self.video1.save()

        # A video with no token.
        neondata.NeonApiRequest('1', 'u').save()
        self.video2 = neondata.VideoMetadata('u_2', request_id='2')
        self.video2.save()
        neondata.NeonApiRequest('2', 'u').save()

    @tornado.testing.gen_test
    def test_token_matches_unshared_video(self):

        content_type = neondata.VideoMetadata.__name__
        payload = {
            'content_type': content_type,
            'content_id': self.video2.get_id()
        }
        token = ShareJWTHelper.encode(payload)

        url = self.get_url('/api/v2/u/videos/?video_id=%s&share_token=%s' %
            (self.video2.get_id(), token))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(403, e.exception.code)

    @tornado.testing.gen_test
    def test_token_isnt_shareable(self):
        thumbnail_id = 'u_nvd_t0'
        content_type = 'ThumbnailMetadata'
        payload = {
            'content_type': content_type,
            'content_id': thumbnail_id
        }
        token = ShareJWTHelper.encode(payload)
        url = self.get_url('/api/v2/u/thumbnails/?thumbnail_id=%s&share_token=%s' %
            (thumbnail_id, token))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(401, e.exception.code)

    @tornado.testing.gen_test
    def test_token_invalid(self):
        video_id = 'u_v0'
        payload = {
            'content_type': 'VideoMetadata',
            'content_id': video_id}
        token = JWTHelper.generate_token(payload, TokenTypes.ACCESS_TOKEN)
        url = self.get_url('/api/v2/u/videos/?video_id=%s&share_token=%s' %
            (video_id, token))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(401, e.exception.code)

    @tornado.testing.gen_test
    def test_token_to_nonsharing_endpoint(self):
        video_id = '1'
        url = self.get_url('/api/v2/u/videos/?video_id=%s&share_token=%s' %
            (video_id, self.video1.share_token))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url, method='POST',
                                         allow_nonstandard_methods=True)
        self.assertEqual(401, e.exception.code)

        url = self.get_url('/api/v2/u/stats/videos/?video_id%s&share_token=%s' %
            (video_id, self.video1.share_token))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(401, e.exception.code)

    @tornado.testing.gen_test
    def test_get_shared_tag(self):
        tag = neondata.Tag('t0', account_id='u')
        tag.share_token = ShareJWTHelper.encode({
            'content_type': neondata.Tag.__name__,
            'content_id': tag.get_id()
        })
        tag.save()
        url = self.get_url('/api/v2/u/tags/?tag_id=%s&share_token=%s')
        r = yield self.http_client.fetch(url % (tag.get_id(), tag.share_token))
        self.assertEqual(200, r.code)
        rjson = json.loads(r.body)
        self.assertIn(tag.get_id(), rjson)
        self.assertEqual(tag.get_id(), rjson[tag.get_id()]['tag_id'])

    @tornado.testing.gen_test
    def test_get_unshared_tag(self):
        tag = neondata.Tag('t0', account_id='u')
        tag.save()
        share_token = ShareJWTHelper.encode({
            'content_type': neondata.Tag.__name__,
            'content_id': tag.get_id()
        })
        url = self.get_url('/api/v2/u/tags/?tag_id=%s&share_token=%s')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url % (tag.get_id(), share_token))
        self.assertEqual(403, e.exception.code)




class TestLiftStatsHandler(TestControllersBase):

    def setUp(self):
        super(TestLiftStatsHandler, self).setUp()
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True

        self.account_id = 'testaccount'
        self.video_id = '%s_%s' % (self.account_id, 'testvid')
        self.video = neondata.VideoMetadata(self.video_id)
        self.video.save()

        self.base_thumb_id = '%s_%s' % (self.video_id, 'a')
        self.base_thumb = neondata.ThumbnailMetadata(
            self.base_thumb_id,
            internal_vid=self.video_id)
        self.base_thumb.save()

        self.thumbs = [neondata.ThumbnailMetadata('%s_%s' %
            (self.video_id, _id),
            internal_vid=self.video_id)
                for _id in ['b', 'c', 'd']]
        [t.save() for t in self.thumbs]
        self.thumb_ids = [t.get_id() for t in self.thumbs]

        self.url = self.get_url(
            '/api/v2/{}/statistics/estimated_lift?base_id=%s&thumbnail_ids=%s'.format(
                self.account_id))
        np.random.seed(234235)

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestLiftStatsHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_response_has_structure(self):
        url = self.url % (self.base_thumb_id, ','.join(self.thumb_ids))
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)

        self.assertEqual(self.base_thumb_id, rjson['baseline_thumbnail_id'])
        self.assertIn('lift', rjson)
        lift = rjson['lift']
        self.assertIsInstance(lift, list)
        [self.assertIsInstance(i, dict) for i in lift]
        [self.assertIn('thumbnail_id', i) for i in lift]
        [self.assertIn('lift', i) for i in lift]
        self.assertNotIn('a', [i['thumbnail_id'] for i in lift])
        [self.assertIsNone(i['lift']) for i in lift
            if i['thumbnail_id'] in ['b', 'd']]
        [self.assertEqual(i['lift'], 0.25) for i in lift
            if i['thumbnail_id'] == 'c']

    @tornado.testing.gen_test
    def test_different_demographic(self):
        base_thumb_id = '%s_%s' % (self.video_id, 'base')
        other_thumb_id = '%s_%s' % (self.video_id, 'other')
        neondata.ThumbnailMetadata(base_thumb_id,
                                   model_version='20160713-test',
                                   features=np.random.rand(1024)).save()
        neondata.ThumbnailMetadata(other_thumb_id,
                                   model_version='20160713-test',
                                   features=np.random.rand(1024)).save()

        url = self.url % (base_thumb_id, other_thumb_id)
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)
        neutral_lift = rjson['lift']
        self.assertEquals(len(neutral_lift), 1)
        self.assertGreaterEqual(neutral_lift[0]['lift'], -1.0)
        self.assertEquals(neutral_lift[0]['thumbnail_id'], other_thumb_id)

        url = url + '&age=40-49&gender=M'
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)
        demo_lift = rjson['lift']
        self.assertEquals(len(demo_lift), 1)
        self.assertGreaterEqual(demo_lift[0]['lift'], -1.0)
        self.assertEquals(demo_lift[0]['thumbnail_id'], other_thumb_id)

        self.assertNotAlmostEqual(demo_lift[0]['lift'],
                                  neutral_lift[0]['lift'])

    @tornado.testing.gen_test
    def test_base_thumb_does_not_exist(self):
        none_thumb_id = '%s_%s' % (self.video_id, 'e')
        url = self.url % (none_thumb_id, ','.join(self.thumb_ids))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(404, e.exception.code)

    @tornado.testing.gen_test
    def test_model_score_is_none(self):

        # Ensure a model score of None doesn't break api.
        neondata.ThumbnailMetadata(self.thumb_ids[0], model_score=.4).save()
        neondata.ThumbnailMetadata(self.thumb_ids[1], model_score=.5).save()

        url = self.url % (self.base_thumb_id, ','.join(self.thumb_ids))
        neondata.ThumbnailMetadata(self.thumb_ids[0], model_score=None).save()
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)
        self.assertIsNone(rjson['lift'][0]['lift'])

        neondata.ThumbnailMetadata(self.thumb_ids[0], model_score=None).save()
        neondata.ThumbnailMetadata(self.thumb_ids[1], model_score=.5).save()
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)
        self.assertIsNone(rjson['lift'][0]['lift'])

        neondata.ThumbnailMetadata(self.thumb_ids[1], model_score=None).save()
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)
        self.assertIsNone(rjson['lift'][0]['lift'])

    @tornado.testing.gen_test
    def test_thumbnail_not_mine_403(self):

        not_my_thumb_id = 'notmy_thumb_id0'
        neondata.ThumbnailMetadata(not_my_thumb_id).save()

        url = self.url % (
            self.base_thumb_id,
            ','.join(self.thumb_ids + [not_my_thumb_id]))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(403, e.exception.code)

        url = self.url % (
            not_my_thumb_id,
            ','.join(self.thumb_ids))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(403, e.exception.code)

    @tornado.testing.gen_test
    def test_share_token_allows_get(self):

        payload = {
            'content_type': 'VideoMetadata',
            'content_id': self.video_id
        }
        share_token = ShareJWTHelper.encode(payload)
        self.video.share_token = share_token
        self.video.save()

        self.verify_account_mocker.stop()

        url = self.url % (self.base_thumb_id, ','.join(self.thumb_ids))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(401, e.exception.code)

        url = url + '&share_token=%s' % share_token
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)

        self.verify_account_mocker.start()

    @tornado.testing.gen_test
    def test_no_post_put_with_share_token(self):

        payload = {
            'content_type': 'VideoMetadata',
            'content_id': self.video_id
        }
        share_token = ShareJWTHelper.encode(payload)
        self.video.share_token = share_token
        self.video.save()

        self.verify_account_mocker.stop()

        # These raise NotImplementedError.
        url = self.url % (self.base_thumb_id, ','.join(self.thumb_ids))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url, body='', method='POST')
        self.assertEqual(501, e.exception.code)

        url = self.url % (self.base_thumb_id, ','.join(self.thumb_ids))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url, body='', method='PUT')
        self.assertEqual(501, e.exception.code)

        self.verify_account_mocker.start()


class TestAPIKeyRequired(TestControllersBase, TestAuthenticationBase):
    def setUp(self):
        self.neon_user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingaccount')
        self.neon_user.save()
        super(TestAPIKeyRequired, self).setUp()

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
            self.make_calls_and_assert_401(url, method, message='this endpoint requires an access token')

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
            self.make_calls_and_assert_401(url, method, message='You cannot access this resource.')

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
            self.make_calls_and_assert_401(url, method, message='You cannot access this resource.')

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

        params = json.dumps({'publisher_id': '123123abc', 'token': token,
                             'uses_bc_gallery': False})
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
        params = json.dumps({'publisher_id': '123123abc', 'token' : token,
                             'uses_bc_gallery': False})
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
                                 'You cannot access')

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
        params = json.dumps({'publisher_id': '123123abc', 'token' : token,
                             'uses_bc_gallery': False})
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
        TestAuthenticationHandler.account_id = 'test_account'
        self.account = neondata.NeonUserAccount(
            TestAuthenticationHandler.account_id,
            users=[TestAuthenticationHandler.username])
        self.account.save()
        self.user = neondata.User(username=TestAuthenticationHandler.username,
            password=TestAuthenticationHandler.password,
            first_name=TestAuthenticationHandler.first_name,
            last_name=TestAuthenticationHandler.last_name,
            title=TestAuthenticationHandler.title)
        self.user.save()
        super(TestAuthenticationHandler, self).setUp()


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
        a_payload = JWTHelper.decode_token(user.access_token)
        self.assertEqual(self.account.get_api_key(), a_payload['account_id'])
        r_payload = JWTHelper.decode_token(user.refresh_token)
        self.assertEqual(self.account.get_api_key(), r_payload['account_id'])
        user_info = rjson['user_info']
        self.assertEquals(user_info['first_name'],
            TestAuthenticationHandler.first_name)
        self.assertEquals(user_info['last_name'],
            TestAuthenticationHandler.last_name)
        self.assertEquals(user_info['title'],
            TestAuthenticationHandler.title)

    @tornado.testing.gen_test
    def test_token_returned_upper_case_username(self):
        url = '/api/v2/authenticate'
        params = json.dumps(
            {'username': TestAuthenticationHandler.username.upper(),
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
        with patch('cmsapiv2.apiv2.datetime') as mock_dt:
            mock_dt.utcnow.return_value = datetime.utcnow()
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body=params,
                                                    method='POST',
                                                    headers=header)
            rjson = json.loads(response.body)
            token1 = rjson['access_token']
            mock_dt.utcnow.return_value += timedelta(1)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body=params,
                                                    method='POST',
                                                    headers=header)
        rjson = json.loads(response.body)
        token2 = rjson['access_token']
        self.assertNotEquals(token1, token2)

    @tornado.testing.gen_test
    def test_account_ids_returned_single(self):

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
        self.assertEquals(a_id, self.account.get_api_key())

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
        self.assertEquals(3, len(account_ids))
        self.assertTrue(new_account_one.neon_api_key in account_ids)
        self.assertTrue(new_account_two.neon_api_key in account_ids)


class TestRefreshTokenHandler(TestAuthenticationBase):
    def setUp(self):
        self.refresh_token_exp = options.get('cmsapiv2.apiv2.refresh_token_exp')
        TestRefreshTokenHandler.username = 'kevin'
        TestRefreshTokenHandler.password = '12345678'
        self.user = neondata.User(username=TestRefreshTokenHandler.username,
                             password=TestRefreshTokenHandler.password)
        self.user.save()
        super(TestRefreshTokenHandler, self).setUp()
        self.url = self.get_url('/api/v2/refresh_token')
        self.headers = {'Content-Type': 'application/json'}

    def tearDown(self):
        options._set('cmsapiv2.apiv2.refresh_token_exp', self.refresh_token_exp)
        super(TestRefreshTokenHandler, self).tearDown()

    def test_no_token(self):
        params = json.dumps({})
        self.http_client.fetch(self.url,
                               body=params,
                               method='POST',
                               callback=self.stop,
                               headers=self.headers)
        response = self.wait()
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 400)
        self.assertRegexpMatches(rjson['error']['message'], 'required key not')

    @tornado.testing.gen_test
    def test_user_does_not_exist(self):
        _, refresh_token = authentication.AccountHelper.get_auth_tokens(
            {'username': 'no_user'})
        params = json.dumps({'token': refresh_token})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(self.url, body=params, method='POST',
                                         headers=self.headers)
        self.assertEqual(404, e.exception.code)

    @tornado.testing.gen_test
    def test_user_has_no_account(self):
        username = 'valid'
        neondata.User(username).save()
        _, refresh_token = authentication.AccountHelper.get_auth_tokens(
            {'username': username})
        params = json.dumps({'token': refresh_token})
        result = yield self.http_client.fetch(self.url, body=params, method='POST',
                                     headers=self.headers)
        self.assertEqual(200, result.code)
        rjson = json.loads(result.body)
        self.assertIn('access_token', rjson)
        self.assertEqual(refresh_token, rjson['refresh_token'])
        self.assertFalse(rjson['account_ids'])


    def test_refresh_token_expired(self):
        neondata.NeonUserAccount('test', users=self.user.get_id()).save()
        refresh_token_exp = options.get('cmsapiv2.apiv2.refresh_token_exp')
        options._set('cmsapiv2.apiv2.refresh_token_exp', -1)
        url = '/api/v2/authenticate'
        params = json.dumps({'username': TestRefreshTokenHandler.username,
                             'password': TestRefreshTokenHandler.password})
        header = { 'Content-Type':'application/json' }
        with patch('cmsapiv2.apiv2.datetime') as mock_dt:
            # Fix time
            mock_dt.utcnow.return_value = datetime.utcnow()

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

            mock_dt.utcnow.return_value += timedelta(1)
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

        with patch('cmsapiv2.apiv2.datetime') as mock_dt:
            mock_dt.utcnow.return_value = datetime.utcnow()

            response = yield self.http_client.fetch(self.get_url(url),
                                                    body=params,
                                                    method='POST',
                                                    headers=header)
            rjson1 = json.loads(response.body)
            refresh_token = rjson1['refresh_token']
            url = '/api/v2/refresh_token'
            params = json.dumps({'token': refresh_token })
            header = { 'Content-Type':'application/json' }
            # Set time forward one second so token will change
            mock_dt.utcnow.return_value += timedelta(1)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body=params,
                                                    method='POST',
                                                    headers=header)
        rjson2 = json.loads(response.body)
        access_token = rjson2['access_token']
        payload = JWTHelper.decode_token(access_token)
        self.assertEqual(new_account_one.get_id(), payload['account_id'])
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
        TestLogoutHandler.username = 'kevin'
        TestLogoutHandler.password = '12345678'
        user = neondata.User(username=TestLogoutHandler.username,
                             password=TestLogoutHandler.password)
        user.save()
        account = neondata.NeonUserAccount('test', users=user.get_id())
        account.save()
        super(TestLogoutHandler, self).setUp()
    def tearDown(self):
        super(TestLogoutHandler, self).tearDown()

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
        super(TestAuthenticationHealthCheckHandler, self).tearDown()

    def test_healthcheck_success(self):
	url = '/healthcheck/'
        response = self.http_client.fetch(self.get_url(url),
                               callback=self.stop,
                               method='GET')
        response = self.wait()
        self.assertEquals(response.code, 200)


class TestVerifiedControllersBase(TestControllersBase):
    def setUp(self):
        self.user = neondata.NeonUserAccount(uuid.uuid1().hex,name='testingme')
        self.user.save()
        self.account_id_api_key = self.user.neon_api_key
        self.account_id = self.user.neon_api_key
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.sife_effect = True
        self.headers = {'Content-Type': 'application/json'}
        super(TestVerifiedControllersBase, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestVerifiedControllersBase, self).tearDown()


class TestShareHandler(TestVerifiedControllersBase):

    @tornado.testing.gen_test
    def test_get_video(self):

        video = neondata.VideoMetadata('u_1', request_id='1')
        video.save()
        neondata.NeonApiRequest('1', 'u').save()
        url = self.get_url('/api/v2/u/videos/share/?video_id=1')
        response = yield self.http_client.fetch(url)
        rjson = json.loads(response.body)
        share_token = rjson['share_token']
        payload = ShareJWTHelper.decode(share_token)
        self.assertEqual('u_1', payload['content_id'])
        self.assertEqual('VideoMetadata', payload['content_type'])
        # Calling the API sets the db video's share_token.
        video = neondata.VideoMetadata.get(video.get_id())
        self.assertEqual(video.share_token, share_token)

    @tornado.testing.gen_test
    def test_get_video_missing(self):
        neondata.VideoMetadata('u2_1', request_id='1').save()
        neondata.NeonApiRequest('1', 'u2').save()
        url = self.get_url('/api/v2/u/videos/share/?video_id=1')
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(404, e.exception.code)

    @tornado.testing.gen_test
    def test_too_many_ids(self):
        url = self.get_url('/api/v2/u/videos/share/?tag_id=%s&video_id=%s' %
            ('tag_id', 'video_id'))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(400, e.exception.code)

    @tornado.testing.gen_test
    def test_get_tag(self):
        tag = neondata.Tag('u_t0', account_id='u')
        given_token = ShareJWTHelper.encode({
            'content_id': tag.get_id(),
            'content_type': type(tag).__name__})
        tag.share_token = given_token
        tag.save()
        url = self.get_url('/api/v2/u/tags/share/?tag_id=%s' % tag.get_id())
        response = yield self.http_client.fetch(url)
        self.assertEqual(200, response.code)
        rjson = json.loads(response.body)
        token = rjson['share_token']
        tag = neondata.Tag.get('u_t0')
        self.assertEqual(tag.share_token, token)
        payload = ShareJWTHelper.decode(token)
        self.assertEqual(tag.get_id(), payload['content_id'])
        self.assertEqual(type(tag).__name__, payload['content_type'])

    @tornado.testing.gen_test
    def test_get_tag_no_token(self):
        tag = neondata.Tag('u_t0', account_id='u')
        tag.save()
        url = self.get_url('/api/v2/u/tags/share/?tag_id=%s' % tag.get_id())
        response = yield self.http_client.fetch(url)
        self.assertEqual(200, response.code)
        rjson = json.loads(response.body)
        token = rjson['share_token']
        self.assertIsNotNone(token)
        payload = ShareJWTHelper.decode(token)
        self.assertEqual(tag.get_id(), payload['content_id'])
        self.assertEqual(type(tag).__name__, payload['content_type'])
        # Validate token in DB matches.
        tag = neondata.Tag.get('u_t0')
        self.assertEqual(tag.share_token, token)

    @tornado.testing.gen_test
    def test_tag_not_mine(self):
        tag = neondata.Tag('u2_t0', account_id='u2')
        tag.save()
        url = self.get_url('/api/v2/u/tags/share/?tag_id=%s' % tag.get_id())
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(403, e.exception.code)

    @unittest.skip('TODO: Setup share handling that uses the video tags share tag')
    @tornado.testing.gen_test
    def test_get_clip(self):
        clip = neondata.Clip('u_c0', video_id='u_v0')
        clip.save()
        url = self.get_url('/api/v2/u/clips/share/?clip_id=%s' % clip.get_id())
        response = yield self.http_client.fetch(url)
        self.assertEqual(200, response.code)
        rjson = json.loads(response.body)
        token = rjson['share_token']
        clip = neondata.Clip.get('u_c0')
        self.assertEqual(clip.share_token, token)
        payload = ShareJWTHelper.decode(token)
        self.assertEqual(clip.get_id(), payload['content_id'])
        self.assertEqual(type(clip).__name__, payload['content_type'])

    @tornado.testing.gen_test
    def test_get_tag_not_mine(self):
        neondata.Tag('u2_1', account_id='u2').save()
        url = self.get_url('/api/v2/u/tags/share/?tag_id=u2_1')
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(403, e.exception.code)

        neondata.Tag('u3_1').save()
        url = self.get_url('/api/v2/u/tags/share/?tag_id=u3_1')
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(403, e.exception.code)

class TestVideoSearchInternalHandler(TestVerifiedControllersBase):
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
        self.assertEquals(
            statemon.state.get('cmsapiv2.controllers.get_internal_search_oks'),
            1)

        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test
    def test_search_no_api_request(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)
        yield neondata.NeonApiRequest('job1',
            'kevin',
             title='kevins video').save(async=True)
        video = neondata.VideoMetadata('kevin_vid2', request_id='job2')
        yield video.save(async=True)
        url = '/api/v2/videos/search?account_id=kevin&fields='\
              'video_id,title,created,updated'
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['video_count'], 1)

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
        self.assertEquals(videos, [])


class TestVideoSearchExternalHandler(TestVerifiedControllersBase):

    @tornado.testing.gen_test
    def test_hidden_video(self):
        # Add videos and hide one. The hidden doesn't show in a search.
        neondata.VideoMetadata('u_1', request_id='1').save()
        neondata.VideoMetadata('u_2', request_id='2').save()
        neondata.VideoMetadata('u_3', request_id='3').save()
        neondata.NeonApiRequest('1', 'u').save()
        neondata.NeonApiRequest('2', 'u').save()
        neondata.NeonApiRequest('3', 'u').save()
        url = self.get_url('/api/v2/u/videos/')
        body = json.dumps({
            'video_id': '2',
            'hidden': True
        })
        headers = {'Content-Type': 'application/json'}
        response = yield self.http_client.fetch(
            url,
            method='PUT',
            headers=headers,
            body=body)
        self.assertEqual(200, response.code)
        search_url = self.get_url('/api/v2/u/videos/search?fields=video_id')
        response = yield self.http_client.fetch(search_url)
        rjson = json.loads(response.body)
        self.assertEqual(2, rjson['video_count'])
        self.assertIn('1', [v['video_id'] for v in rjson['videos']])
        self.assertNotIn('2', [v['video_id'] for v in rjson['videos']])

        # Put it back.
        body = json.dumps({
            'video_id': '2',
            'hidden': False
        })
        response = yield self.http_client.fetch(
            url,
            method='PUT',
            headers=headers,
            body=body)
        self.assertEqual(200, response.code)
        response = yield self.http_client.fetch(search_url)
        rjson = json.loads(response.body)
        self.assertEqual(3, rjson['video_count'])
        self.assertIn('1', [v['video_id'] for v in rjson['videos']])
        self.assertIn('2', [v['video_id'] for v in rjson['videos']])

    @tornado.testing.gen_test
    def test_since_and_until_param(self):
        # Add a number of videos and get the time at third's creation
        for i in range(6):
            c = str(i)
            neondata.VideoMetadata('u0_v' + c, request_id='j' + c).save()
            neondata.NeonApiRequest('j' + c, 'u0').save()
            if i == 3:
                video = neondata.VideoMetadata.get('u0_v' + c)
                time_param = dateutil.parser.parse(video.created).strftime('%s.%f')

        url = '/api/v2/u0/videos/search?fields=video_id,created&since={}'.format(
            time_param)
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)
        self.assertEqual(2, len(rjson['videos']), 'Two videos after timestamp')

        url = '/api/v2/u0/videos/search?fields=video_id,created&until={}'.format(
            time_param)
        response = yield self.http_client.fetch(self.get_url(url))
        rjson = json.loads(response.body)
        self.assertEqual(3, len(rjson['videos']), 'Three videos before timestamp')

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
        self.assertEquals(
            statemon.state.get('cmsapiv2.controllers.get_external_search_oks'),
            1)
        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test
    def test_search_no_api_request(self):
        video = neondata.VideoMetadata('kevin_vid1', request_id='job1')
        yield video.save(async=True)
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
        videos = rjson['videos']
        self.assertEquals(videos[0]['video_id'], 'vid2') 
        self.assertEquals(rjson['video_count'], 1)

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
        response = yield self.http_client.fetch(self.get_url(url))

        video = neondata.VideoMetadata('kevin_vid3', request_id='job3')
        yield video.save(async=True)
        yield neondata.NeonApiRequest('job3', 'kevin',
                  title='really kevins best video yet').save(async=True)
        rjson1 = json.loads(response.body)
        url = rjson1['prev_page']
        response = yield self.http_client.fetch(self.get_url(url))
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


class TestVideoSearchExtHandlerQuery(TestVerifiedControllersBase):

    def setUp(self):
        super(TestVideoSearchExtHandlerQuery, self).setUp()

        neondata.VideoMetadata('u0_v0', request_id='j0').save()
        neondata.NeonApiRequest('j0', 'u0',
                                title='Title title0 title').save()
        neondata.VideoMetadata('u0_v1', request_id='j1').save()
        neondata.NeonApiRequest('j1', 'u0',
                                title='*.*.* Title2 title1 title *.*.*').save()
        neondata.VideoMetadata('u0_v2', request_id='j2').save()
        neondata.NeonApiRequest('j2', 'u0',
                                title='Another title0 title1 title?').save()
        self.url = self.get_url(
            '/api/v2/u0/videos/search?fields=video_id,title&query={}')

    @tornado.testing.gen_test
    def test_regex(self):
        '''Allow POSIX features.'''
        response = yield self.http_client.fetch(self.url.format('^[A|T].*title0.*'))
        rjson = json.loads(response.body)
        self.assertEqual(2, len(rjson['videos']))

    @tornado.testing.gen_test
    def test_case_insensitive(self):
        '''Allow case insensitive matches'''
        response = yield self.http_client.fetch(self.url.format('.*title2.*'))
        rjson = json.loads(response.body)
        self.assertEqual(1, len(rjson['videos']), 'Matches "Title2" in request j1')

    @tornado.testing.gen_test
    def test_special_character_in_param(self):
        '''Handle pattern that has special characters'''
        response = yield self.http_client.fetch(self.url.format('*.*.*'))
        rjson = json.loads(response.body)
        self.assertEqual(1, len(rjson['videos']))
        response = yield self.http_client.fetch(self.url.format('?'))
        rjson = json.loads(response.body)
        self.assertEqual(1, len(rjson['videos']))

    @tornado.testing.gen_test
    def test_instring_query_param(self):
        '''Falls back to using in-string (LIKE %s<param>%s)'''
        response = yield self.http_client.fetch(self.url.format('title%20title0'))
        rjson = json.loads(response.body)
        self.assertEqual(1, len(rjson['videos']), 'Search is case insensitive')

    @tornado.testing.gen_test
    def test_order_query_param(self):
        '''Find only titles where tokens in query appear in order'''
        response = yield self.http_client.fetch(self.url.format('title1%20title2'))
        rjson = json.loads(response.body)
        self.assertEqual(0, len(rjson['videos']), 'Search is strict on token order')


class TestAccountLimitsHandler(TestVerifiedControllersBase):
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


class TestAccountIntegrationsHandler(TestControllersBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestAccountIntegrationsHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestAccountIntegrationsHandler, self).tearDown()


    @tornado.testing.gen_test
    def test_one_integration(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        bi = neondata.BrightcoveIntegration('kevinacct')
        yield bi.save(async=True)
        url = '/api/v2/%s/integrations' % (so.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method="GET")
        rjson = json.loads(response.body)
        self.assertEquals(rjson['integration_count'], 1)
        self.assertEquals(len(rjson['integrations']), 1)
        self.assertEquals(rjson['integrations'][0]['account_id'], 'kevinacct')

    @tornado.testing.gen_test
    def test_two_integrations_one_type(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        bi = neondata.BrightcoveIntegration('kevinacct')
        yield bi.save(async=True)
        bi = neondata.BrightcoveIntegration('kevinacct')
        yield bi.save(async=True)
        url = '/api/v2/%s/integrations' % (so.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method="GET")
        rjson = json.loads(response.body)
        self.assertEquals(rjson['integration_count'], 2)
        self.assertEquals(len(rjson['integrations']), 2)
        self.assertEquals(rjson['integrations'][0]['account_id'], 'kevinacct')
        self.assertNotEqual(
            rjson['integrations'][0]['integration_id'],
            rjson['integrations'][1]['integration_id'])

    @tornado.testing.gen_test
    def test_wrong_account(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        url = '/api/v2/%s/integrations' % ('doesnotexist')
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(self.get_url(url),
                                          method='GET')

        self.assertEquals(e.exception.code, 404)

    @tornado.testing.gen_test
    def test_multiple_integrations_multiple_types(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)

        yield neondata.BrightcoveIntegration('kevinacct').save(async=True)
        yield neondata.OoyalaIntegration('kevinacct').save(async=True)
        yield neondata.BrightcoveIntegration('kevinacct').save(async=True)
        yield neondata.BrightcoveIntegration('kevinacct').save(async=True)
        yield neondata.OoyalaIntegration('kevinacct').save(async=True)
        yield neondata.BrightcoveIntegration('kevinacct').save(async=True)

        url = '/api/v2/%s/integrations' % (so.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method="GET")
        rjson = json.loads(response.body)
        self.assertEquals(rjson['integration_count'], 6)
        self.assertEquals(len(rjson['integrations']), 6)
        # the first four intergrations should be type brightcove
        self.assertEquals(rjson['integrations'][0]['type'], 'brightcove')
        self.assertEquals(rjson['integrations'][1]['type'], 'brightcove')
        self.assertEquals(rjson['integrations'][2]['type'], 'brightcove')
        self.assertEquals(rjson['integrations'][3]['type'], 'brightcove')
        # the last two intergrations should be type ooyala
        self.assertEquals(rjson['integrations'][4]['type'], 'ooyala')
        self.assertEquals(rjson['integrations'][5]['type'], 'ooyala')

    @tornado.testing.gen_test
    def test_no_integrations_exist(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)

        yield neondata.BrightcoveIntegration(
            'differentaccount').save(async=True)
        url = '/api/v2/%s/integrations' % (so.neon_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method="GET")
        rjson = json.loads(response.body)
        self.assertEquals(rjson['integration_count'], 0)


class TestBillingAccountHandler(TestControllersBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestBillingAccountHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestBillingAccountHandler, self).tearDown()


    @tornado.testing.gen_test
    def test_post_billing_account_no_account(self):
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/noaccount/billing/account'
        params = json.dumps({'billing_token_ref' : 'testa'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        self.assertEquals(e.exception.code, 404)

    @tornado.testing.gen_test
    def test_post_billing_account_customer_exists(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        params = json.dumps({'billing_token_ref' : 'testa'})

        customer = stripe.Customer(id='test')
        customer.email = 'test@test.com'
        with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as stripe_ret,\
              patch('cmsapiv2.apiv2.stripe.Customer.save') as stripe_save:
            stripe_ret.return_value = customer
            yield self.http_client.fetch(self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        # let's grab our account and make sure we saved the
        # id value, and set billed_elsewhere correctly
        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertEquals(acct.billed_elsewhere, False)
        self.assertEquals(acct.billing_provider_ref, 'test')

    @tornado.testing.gen_test
    def test_post_billing_account_customer_retrieve_exception(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        params = json.dumps({'billing_token_ref' : 'testa'})

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [Exception('testa')]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 500)
        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertEquals(acct.billed_elsewhere, True)
        self.assertEquals(acct.billing_provider_ref, None)

    @tornado.testing.gen_test
    def test_post_billing_account_customer_save_exception(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        params = json.dumps({'billing_token_ref' : 'testa'})

        patch_class = 'cmsapiv2.stripe.Customer'
        customer = stripe.Customer(id='test')
        customer.email = 'test@test.com'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr,\
                 patch('cmsapiv2.apiv2.stripe.Customer.save') as ss:
                sr.return_value = customer
                ss.side_effect = [Exception('testa')]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 500)
        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertEquals(acct.billed_elsewhere, True)
        self.assertEquals(acct.billing_provider_ref, None)

    @tornado.testing.gen_test
    def test_post_billing_account_create_new_customer(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.email = 'kevin@test.invalid'
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        params = json.dumps({'billing_token_ref' : 'testa'})

        customer = stripe.Customer(id='test')
        customer.email = 'test@test.com'
        with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr,\
             patch('cmsapiv2.apiv2.stripe.Customer.create') as sc:
            sr.side_effect = [ stripe.error.InvalidRequestError(
                'No such customer', 'test') ]
            sc.return_value = customer
            yield self.http_client.fetch(self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertEquals(acct.billed_elsewhere, False)
        self.assertEquals(acct.billing_provider_ref, 'test')
        self.assertEquals(sc.call_args[1]['source'], 'testa')
        self.assertEquals(sc.call_args[1]['email'], 'kevin@test.invalid')

    @tornado.testing.gen_test
    def test_post_billing_account_customer_different_invalid_request(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        params = json.dumps({'billing_token_ref' : 'testa'})

        patch_class = 'cmsapiv2.stripe.Customer'
        customer = stripe.Customer(id='test')
        customer.email = 'test@test.com'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.InvalidRequestError(
                    'not recognized', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 500)
        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertEquals(acct.billed_elsewhere, True)
        self.assertEquals(acct.billing_provider_ref, None)

    @tornado.testing.gen_test
    def test_get_billing_account_no_account(self):
        url = '/api/v2/dne/billing/account'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(self.get_url(url),
                method='GET')
        self.assertEquals(e.exception.code, 404)

    @tornado.testing.gen_test
    def test_get_billing_account_not_recongnized_invalid(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/account' % so.neon_api_key

        customer = stripe.Customer(id='test')
        customer.email = 'test@test.com'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.InvalidRequestError(
                    'not recognized', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                    method='GET')
        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'Unknown')

    @tornado.testing.gen_test
    def test_get_billing_account_recognized_invalid(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/account' % so.neon_api_key

        customer = stripe.Customer(id='test')
        customer.email = 'test@test.com'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.InvalidRequestError(
                    'No such customer', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                    method='GET')
        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'No billing')

    @tornado.testing.gen_test
    def test_get_billing_account_customer_exists(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/account' % so.neon_api_key

        customer = stripe.Customer(id='test')
        customer.email = 'test@test.com'
        with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
            sr.return_value = customer
            response = yield self.http_client.fetch(self.get_url(url),
                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['id'], customer.id)
        self.assertEquals(rjson['email'], customer.email)

    @tornado.testing.gen_test
    def test_get_billing_account_normal_exception(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ Exception(
                    'Unknown', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                     method='GET')

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'Unknown')

    @tornado.testing.gen_test
    def test_post_actual_int(self):
        if not options.run_stripe_on_test_account:
            raise unittest.SkipTest(
                'actually talks to stripe, skipped in normal testing')
        so = neondata.NeonUserAccount('kevinacct')
        so.email = 'test@invalid24.xxx.test'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        params = json.dumps({'billing_token_ref' : 'testa'})
        response = yield self.http_client.fetch(self.get_url(url),
            body=params,
            method='POST',
            headers=header)
        print response.body

    @tornado.testing.gen_test
    def test_get_actual_int(self):
        if not options.run_stripe_on_test_account:
            raise unittest.SkipTest(
                'actually talks to stripe, skipped in normal testing')
        options._set('cmsapiv2.apiv2.stripe_api_key',
            'sk_test_mOzHk0K8yKfe57T63jLhfCa8')
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = 'cus_8QKom2aGH0u1Vs'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/account' % so.neon_api_key
        response = yield self.http_client.fetch(self.get_url(url),
            method='GET')
        print response.body


class TestBillingSubscriptionHandler(TestControllersBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestBillingSubscriptionHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestBillingSubscriptionHandler, self).tearDown()


    @tornado.testing.gen_test
    def test_post_billing_subscription_no_account(self):
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/noaccount/billing/subscription'
        params = json.dumps({'plan_type' : 'pro_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Neon Account was not found')

    @tornado.testing.gen_test
    def test_post_billing_subscription_no_billing_provider_ref(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method='POST',
                headers=header)

        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'There is not a billing account')

    @tornado.testing.gen_test
    def test_post_billing_subscription_invalid_request_error_known(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.InvalidRequestError(
                    'No such customer', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'No billing account found in Stripe')

    @tornado.testing.gen_test
    def test_post_billing_subscription_no_billing_plan(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'proe_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(self.get_url(url),
                 body=params,
                 method='POST',
                 headers=header)

        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'No billing plan for that plan_type')

    @tornado.testing.gen_test
    def test_post_billing_subscription_invalid_request_error_not_known(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.InvalidRequestError(
                    'not known', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'not known')

    @tornado.testing.gen_test
    def test_post_billing_subscription_bad_card_error(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.CardError(
                    'not known', 'test', 402) ]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 402)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'not known')

    @tornado.testing.gen_test
    def test_post_billing_subscription_bad_card_error_different_status(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.CardError(
                    'not known', 'test', 402, http_status=433) ]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 433)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'not known')

    @tornado.testing.gen_test
    def test_post_billing_subscription_retrieve_exception(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ Exception('not known') ]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'not known')

    @tornado.testing.gen_test
    def test_post_billing_subscription_full_upgrade(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})

        cust_return = stripe.Customer.construct_from({
            'id': 'cus_foo',
            'subscriptions': {
                'object': 'list',
                'url': 'localhost',
            }
        }, 'api_key')
        sub_return = stripe.Subscription()
        sub_return.status = 'active'
        sub_return.plan = stripe.Plan(id='pro_monthly')
        with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr,\
             patch('cmsapiv2.apiv2.stripe.Subscription.delete') as sd:
            sr.return_value.subscriptions.all.return_value = { 'data' : [] }
            sr.return_value.subscriptions.create.return_value = sub_return
            sd.delete.return_value = True
            yield self.http_client.fetch(self.get_url(url),
                 body=params,
                 method='POST',
                 headers=header)

        current_time = datetime.utcnow()
        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertTrue(current_time < dateutil.parser.parse(
            acct.verify_subscription_expiry))
        self.assertEquals(acct.billing_provider_ref, '123')
        self.assertEquals(acct.subscription_information['status'], 'active')
        self.assertEquals(
            acct.subscription_information['plan']['id'],
            'pro_monthly')

        acct_limits = yield neondata.AccountLimits.get(
            so.neon_api_key,
            async=True)

        bp = yield neondata.BillingPlans.get(
            'pro_monthly',
            async=True)

        self.assertEquals(acct_limits.max_video_posts,
            bp.max_video_posts)
        next_refresh_time = datetime.utcnow() + \
            timedelta(seconds=bp.seconds_to_refresh_video_posts - 100)
        self.assertTrue(
            dateutil.parser.parse(
                acct_limits.refresh_time_video_posts) > next_refresh_time)
        self.assertEquals(acct_limits.max_video_size,
            bp.max_video_size)

    @tornado.testing.gen_test
    def test_post_billing_subscription_full_downgrade(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'demo'})

        cust_return = stripe.Customer.construct_from({
            'id': 'cus_foo',
            'subscriptions': {
                'object': 'list',
                'url': 'localhost',
            },
            'sources' : {
                'object' : 'list',
                "data": [
                {
                    "id": "card_18AYHJBbJLCvOlUnloCk6f5k"
                },
                {
                    "id": "card_18AYHJBbJLCvOlUnloCk6f5k"
                }
                ]
            }
        }, 'api_key')
        sub_return = stripe.Subscription()
        sub_return.status = 'active'
        sub_return.plan = stripe.Plan(id='pro_monthly')
        with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr,\
             patch('cmsapiv2.apiv2.stripe.Subscription.delete') as sd:
            sr.return_value.sources.all.return_value = \
                cust_return.sources['data']
            for rv in sr.return_value.sources.all.return_value:
                rv.delete = MagicMock()
            sr.return_value.subscriptions.all.return_value = { 'data' : [] }
            sr.return_value.subscriptions.create.return_value = sub_return
            sd.delete.return_value = True
            yield self.http_client.fetch(self.get_url(url),
                 body=params,
                 method='POST',
                 headers=header)

        current_time = datetime.utcnow()
        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertEquals(acct.subscription_information, None)
        self.assertEquals(acct.billed_elsewhere, True)
        # should always serve for them
        self.assertEquals(acct.serving_enabled, True)
        acct_limits = yield neondata.AccountLimits.get(
            so.neon_api_key,
            async=True)

        bp = yield neondata.BillingPlans.get(
            'demo',
            async=True)
        self.assertEquals(acct_limits.max_video_posts,
            bp.max_video_posts)
        next_refresh_time = datetime.utcnow() + \
            timedelta(seconds=bp.seconds_to_refresh_video_posts - 100)
        self.assertTrue(
            dateutil.parser.parse(
                acct_limits.refresh_time_video_posts) > next_refresh_time)
        self.assertEquals(acct_limits.max_video_size,
            bp.max_video_size)

    @tornado.testing.gen_test
    def test_post_billing_subscription_change(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})

        cust_return = stripe.Customer.construct_from({
            'id': 'cus_foo',
            'subscriptions': {
                'object': 'list',
                'url': 'localhost',
            }
        }, 'api_key')
        sub_return_one = stripe.Subscription()
        sub_return_one.status = 'active'
        sub_return_one.plan = stripe.Plan(id='pro_monthly')
        sub_return_two = stripe.Subscription()
        sub_return_two.status = 'active'
        sub_return_two.plan = stripe.Plan(id='pro_yearly')
        with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr,\
             patch('cmsapiv2.apiv2.stripe.Subscription.delete') as sd:
            sr.return_value.subscriptions.all.return_value = {
                'data' : [sub_return_one] }
            sr.return_value.subscriptions.create.return_value = sub_return_two
            sd.delete.return_value = True
            yield self.http_client.fetch(self.get_url(url),
                 body=params,
                 method='POST',
                 headers=header)

        self.assertEquals(sd.call_count, 1)
        current_time = datetime.utcnow()
        acct = yield neondata.NeonUserAccount.get(
            so.neon_api_key,
            async=True)
        self.assertTrue(current_time < dateutil.parser.parse(
            acct.verify_subscription_expiry))
        self.assertEquals(acct.billing_provider_ref, '123')
        self.assertEquals(acct.subscription_information['status'], 'active')
        self.assertEquals(acct.serving_enabled, True)
        self.assertEquals(
            acct.subscription_information['plan']['id'],
            'pro_yearly')

    @tornado.testing.gen_test
    def test_post_billing_subscription_create_exception(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})

        cust_return = stripe.Customer.construct_from({
            'id': 'cus_foo',
            'subscriptions': {
                'object': 'list',
                'url': 'localhost',
            }
        }, 'api_key')
        sub_return = stripe.Subscription()
        sub_return.status = 'active'
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.return_value.subscriptions.create.side_effect = [
                    Exception('not known') ]
                yield self.http_client.fetch(self.get_url(url),
                     body=params,
                     method='POST',
                     headers=header)

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'not known')

    @tornado.testing.gen_test
    def test_get_billing_subscription(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key

        sub_return_one = stripe.Subscription()
        sub_return_one.status = 'active'
        sub_return_one.plan = stripe.Plan(id='pro_monthly')

        with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
            sr.return_value.subscriptions.all.return_value = {
                'data' : [sub_return_one] }
            response = yield self.http_client.fetch(self.get_url(url),
                 method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['plan']['id'], 'pro_monthly')

    @tornado.testing.gen_test
    def test_get_billing_subscription_no_billing_ref(self):
        so = neondata.NeonUserAccount('kevinacct')
        yield so.save(async=True)
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key

        sub_return_one = stripe.Subscription()
        sub_return_one.status = 'active'
        sub_return_one.plan = stripe.Plan(id='pro_monthly')

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(self.get_url(url),
                 method='GET')
        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'No billing')

    @tornado.testing.gen_test
    def test_get_billing_subscription_no_stripe_customer(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.InvalidRequestError(
                    'No such customer', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                     method='GET')

        self.assertEquals(e.exception.code, 404)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['message'],
            'No billing account found')

    @tornado.testing.gen_test
    def test_get_billing_subscription_invalid_diff_error(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ stripe.error.InvalidRequestError(
                    'invalid', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                     method='GET')

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'Unknown')

    @tornado.testing.gen_test
    def test_get_billing_subscription_normal_exception(self):
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = '123'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            with patch('cmsapiv2.apiv2.stripe.Customer.retrieve') as sr:
                sr.side_effect = [ Exception(
                    'Unknown', 'test') ]
                yield self.http_client.fetch(self.get_url(url),
                     method='GET')

        self.assertEquals(e.exception.code, 500)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(
            rjson['error']['data'],
            'Unknown')

    @tornado.testing.gen_test
    def test_post_billing_actual_talking(self):
        if not options.run_stripe_on_test_account:
            raise unittest.SkipTest(
                'actually talks to stripe, skipped in normal testing')

        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = 'cus_8P7y8RI3gRyhF0'
        yield so.save(async=True)
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        params = json.dumps({'plan_type' : 'pro_monthly'})
        response = yield self.http_client.fetch(self.get_url(url),
                                                body=params,
                                                method='POST',
                                                headers=header)
        print response

    @tornado.testing.gen_test
    def test_get_actual_sub(self):
        if not options.run_stripe_on_test_account:
            raise unittest.SkipTest(
                'actually talks to stripe, skipped in normal testing')

        options._set('cmsapiv2.apiv2.stripe_api_key',
            'sk_test_mOzHk0K8yKfe57T63jLhfCa8')
        so = neondata.NeonUserAccount('kevinacct')
        so.billing_provider_ref = 'cus_8P7y8RI3gRyhF0'
        yield so.save(async=True)
        url = '/api/v2/%s/billing/subscription' % so.neon_api_key
        response = yield self.http_client.fetch(self.get_url(url),
            method='GET')
        print response.body


class TestTelemetrySnippet(TestControllersBase):
    def setUp(self):
        self.acct = neondata.NeonUserAccount(uuid.uuid1().hex,
                                        name='testingme')
        self.acct.save()
        user = neondata.User('my_user',
                             access_level=neondata.AccessLevels.GLOBAL_ADMIN)
        user.save()
        self.account_id = self.acct.neon_api_key

        # Mock out the token decoding
        self.token_decode_patcher = patch(
            'cmsapiv2.apiv2.JWTHelper.decode_token')
        self.token_decode_mock = self.token_decode_patcher.start()
        self.token_decode_mock.return_value = {
            'username' : 'my_user'
            }
        super(TestTelemetrySnippet, self).setUp()

    def tearDown(self):
        self.token_decode_patcher.stop()
        super(TestTelemetrySnippet, self).tearDown()

    @tornado.gen.coroutine
    def _send_authed_request(self, url):
        request = tornado.httpclient.HTTPRequest(
            self.get_url(url),
            headers={'Authorization' : 'Bearer my_token'})
        response = yield self.http_client.fetch(request)
        raise tornado.gen.Return(response)

    @tornado.testing.gen_test
    def test_invalid_account_id(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self._send_authed_request(
                '/api/v2/badacct/telemetry/snippet')

        self.assertEquals(e.exception.code, 401)

    @tornado.testing.gen_test
    def test_no_integrations(self):
        response = yield self._send_authed_request(
            '/api/v2/%s/telemetry/snippet' % self.account_id)

        self.assertEquals(response.headers['Content-Type'],
                          'text/plain')
        self.assertEquals(response.code, 200)


        self.assertIn(
            "var neonPublisherId = '%s';" % self.acct.tracker_account_id,
            response.body)
        self.assertNotIn('neonBrightcoveGallery', response.body)
        self.assertIn('cdn.neon-lab.com/neonoptimizer_dixon.js', response.body)

    @tornado.testing.gen_test
    def test_non_gallery_bc_integration(self):
        neondata.BrightcoveIntegration(self.account_id, 'pub_id').save()

        response = yield self._send_authed_request(
            '/api/v2/%s/telemetry/snippet' % self.account_id)

        self.assertEquals(response.headers['Content-Type'],
                          'text/plain')
        self.assertEquals(response.code, 200)

        self.assertIn(
            "var neonPublisherId = '%s';" % self.acct.tracker_account_id,
            response.body)
        self.assertNotIn('neonBrightcoveGallery', response.body)
        self.assertIn('cdn.neon-lab.com/neonoptimizer_dixon.js', response.body)

    @tornado.testing.gen_test
    def test_gallery_bc_integration(self):
        neondata.BrightcoveIntegration(self.account_id, 'pub_id',
                                       uses_bc_gallery=True).save()

        response = yield self._send_authed_request(
            '/api/v2/%s/telemetry/snippet' % self.account_id)

        self.assertEquals(response.headers['Content-Type'],
                          'text/plain')
        self.assertEquals(response.code, 200)

        self.assertIn(
            "var neonPublisherId = '%s';" % self.acct.tracker_account_id,
            response.body)
        self.assertIn('neonBrightcoveGallery = true', response.body)
        self.assertNotIn('insertBefore', response.body)
        self.assertIn("src='//cdn.neon-lab.com/neonoptimizer_dixon.js'",
                      response.body)


class TestBrightcovePlayerHandler(TestControllersBase):
    '''Test the handler and helper classes for BrightcovePlayer'''

    def setUp(self):
        super(TestBrightcovePlayerHandler, self).setUp()

        # Mock our user authorization
        self.user = neondata.NeonUserAccount('a0')
        self.user.save()
        self.account_id = self.user.neon_api_key
        self.publisher_id = 'p0'
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True

        # Create a mock integration
        self.integration = neondata.BrightcoveIntegration(
            self.account_id,
            self.publisher_id,
            application_client_id='id',
            application_client_secret='secret')
        self.integration.save()
        self.api = api.brightcove_api.PlayerAPI(self.integration)

        # Set up two initial players
        self.player = neondata.BrightcovePlayer(
            player_ref='pl0',
            integration_id=self.integration.integration_id,
            name='db name',
            is_tracked=True,
            published_plugin_version='0.0.1');
        self.player.save()
        self.untracked_player = neondata.BrightcovePlayer(
            player_ref='pl2',
            integration_id=self.integration.integration_id,
            name='untracked player',
            is_tracked=False)
        self.untracked_player.save()

        # An example player configuration
        self.tracked_player_config = {
            "autoadvance": 0,
            "autoplay": False,
            "compatibility": True,
            "flashHlsDisabledByStudio": False,
            "fullscreenControl": True,
            "id": "BkMO9qa8x",
            "player": {
                "inactive": False,
                "template": {
                    "locked": False,
                    "name": "single-video-template",
                    "version": "5.1.14"
                }
            },
            "plugins": [
                {
                    "name": "other-plugin-2",
                    "options": {
                        "flag": False,
                    },
                },
                {
                    "name": "neon",
                    "options": {
                        "publisher": {
                            "id": 12345
                        },
                    },
                },
                {
                    "name": "other-plugin-1",
                    "options": {
                        "flag": True
                    },
                },
            ],
            "scripts": [
                "example.js",
                "another.js",
                "https://s3.amazonaws.com/neon-cdn-assets/old-version/videojs-neon-plugin.min.js",
                "other.js"
            ],
            "skin": "graphite",
            "studio_configuration": {
                "player": {
                    "adjusted": True
                }
            },
            "stylesheets": [],
            "video_cloud": {
                "policy_key": "BCpkADawqM2Z5-2XLiQna9qL7qIuHETaqzXl1fdmHcVOFOP6Rf8uUnlhNxNlh9MLNjb5lkodGFv2yBU9suVWdnXZTcFWEMx2qvNACzbVDIyco9fvRTAi43xUeygF_GPQqOUGomo8Bg1s-V7J"
            }
        }

        # Mock bc player get
        self.get_player_mocker = patch('api.brightcove_api.PlayerAPI.get_player')
        self.get_player = self._future_wrap_mock(self.get_player_mocker.start())

    def tearDown(self):
        self.get_player_mocker.stop()
        self.verify_account_mocker.stop()
        super(TestBrightcovePlayerHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_get_players(self):

        header = { 'Content-Type':'application/json' }
        url = '/api/v2/{}/integrations/brightcove/players?integration_id={}'.format(
             self.account_id,
             self.integration.integration_id)

        with patch('api.brightcove_api.PlayerAPI.get_players') as _get:
            get = self._future_wrap_mock(_get)
            get.side_effect = [{
                'items': [
                    {
                        'accountId': self.publisher_id,
                        'id':'pl0',
                        'name':'Neon Tracking Player',
                        'description':'Neon tracking plugin bundled.'
                    },
                    {
                        'accountId': self.publisher_id,
                        'id':'pl1',
                        'name':'Neon Player 2: Neoner',
                        'description':'Another description.'
                    }],
                'item_count': 2
            }]
            r = yield self.http_client.fetch(
                self.get_url(url),
                headers=header)
            self.assertEqual(get.call_count, 1)
            self.assertEqual(200, r.code)
        rjson = json.loads(r.body)
        players, count = rjson.values()
        self.assertEqual(2, len(players))
        self.assertEqual(2, count)
        player0, player1 = players
        self.assertNotIn('id', player0)
        self.assertEqual('pl0', player0['player_ref'])
        self.assertEqual('Neon Tracking Player', player0['name'])
        self.assertNotIn('description', player0)
        self.assertEqual('pl1', player1['player_ref'])
        self.assertEqual('Neon Player 2: Neoner', player1['name'])
        self.assertIsNone(neondata.BrightcovePlayer.get('pl1'))

    @tornado.testing.gen_test
    def test_get_no_default_player(self):
        # TODO factor these header, etc.
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/{}/integrations/brightcove/players?integration_id={}'.format(
             self.account_id,
             self.integration.integration_id)
        with patch('api.brightcove_api.PlayerAPI.get_players') as _get:
            get = self._future_wrap_mock(_get)
            default_bc_player = {
                'accountId': self.publisher_id,
                'id':'default',
                'name':'Default Player',
                'description':'Default Brightcove player.'
            }
            get.side_effect = [{
                'items': [
                    {
                        'accountId': self.publisher_id,
                        'id':'pl0',
                        'name':'Neon Tracking Player',
                        'description':'Neon tracking plugin bundled.'
                    },
                    default_bc_player,
                    {
                        'accountId': self.publisher_id,
                        'id':'pl1',
                        'name':'Neon Player 2: Neoner',
                        'description':'Another description.'
                    },
                    default_bc_player],
                'item_count': 2
            }]
            r = yield self.http_client.fetch(
                self.get_url(url),
                headers=header)
        players, count = json.loads(r.body).values()
        self.assertEqual(players[0]['player_ref'], 'pl0')
        self.assertEqual(players[1]['player_ref'], 'pl1')
        self.assertEqual(count, 2)

    @tornado.testing.gen_test
    def test_get_players_bc_401(self):
        '''Test that a BrightcoveApiClientError for authorization translates to 401'''
        headers = { 'Content-Type':'application/json' }
        url = '/api/v2/{}/integrations/brightcove/players?integration_id={}'.format(
             self.account_id, self.integration.integration_id)
        with patch('api.brightcove_api.PlayerAPI.get_players') as _get:
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                get = self._future_wrap_mock(_get)
                get.side_effect = api.brightcove_api.BrightcoveApiClientError(
                    401,
                    'Insufficient access for operation')
                yield self.http_client.fetch(
                    self.get_url(url),
                    headers=headers)
        self.assertEqual(e.exception.code, 401)

    @tornado.testing.gen_test
    def test_get_players_bc_500(self):
        '''Test that a BrightcoveApiServerError for authorization translates to 500'''
        headers = { 'Content-Type':'application/json' }
        url = '/api/v2/{}/integrations/brightcove/players?integration_id={}'.format(
             self.account_id, self.integration.integration_id)
        with patch('api.brightcove_api.PlayerAPI.get_players') as _get:
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                get = self._future_wrap_mock(_get)
                get.side_effect = api.brightcove_api.BrightcoveApiServerError(
                    500,
                    'Internal server error')
                yield self.http_client.fetch(
                    self.get_url(url),
                    headers=headers)
        self.assertEqual(e.exception.code, 500)

    @tornado.testing.gen_test
    def test_put_tracked_player(self):
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/{}/integrations/brightcove/players'.format(self.account_id)

        with patch('cmsapiv2.controllers.BrightcovePlayerHelper.publish_player') as _pub:
            pub = self._future_wrap_mock(_pub)
            pub.side_effect = [True]
            self.get_player.side_effect = [{
                'id': 'pl0',
                'name': 'new name',
                'branches': {
                    'master': {
                        'configuration': self.tracked_player_config
            }}}]

            r = yield self.http_client.fetch(
                self.get_url(url),
                headers=header,
                method='PUT',
                body=json.dumps({
                    'player_ref': 'pl0',
                    'is_tracked': True,
                    'integration_id': self.integration.integration_id
                }))
            self.assertEqual(1, pub.call_count)

        self.assertEqual(self.get_player.call_args[0][0], 'pl0')
        our_url = controllers.BrightcovePlayerHelper._get_current_tracking_url()
        self.assertEqual(pub.call_args[0][0], 'pl0')
        self.assertIn(our_url, pub.call_args[0][1]['scripts'])
        player = json.loads(r.body)
        self.assertTrue(player['is_tracked'])
        self.assertEqual(player['player_ref'], 'pl0')
        self.assertEqual(player['name'], 'new name')

        # Try with a new player
        with patch('cmsapiv2.controllers.BrightcovePlayerHelper.publish_player') as _pub:
            pub = self._future_wrap_mock(_pub)
            self.get_player.side_effect = [{
                'player_ref': 'pl-new',
                'name': 'new name',
                'branches': {
                    'master': {
                        'configuration': self.tracked_player_config
            }}}]
            r = yield self.http_client.fetch(
                self.get_url(url),
                headers=header,
                method='PUT',
                body=json.dumps({
                    'player_ref': 'pl-new',
                    'is_tracked': True,
                    'integration_id': self.integration.integration_id
                }))
            self.assertEqual(1, pub.call_count)

        player = json.loads(r.body)
        self.assertEqual(player['player_ref'], 'pl-new')
        self.assertEqual(player['name'],'new name')
        self.assertTrue(player['is_tracked'])
        player = yield neondata.BrightcovePlayer.get('pl-new', async=True)
        self.assertEqual(player.get_id(), 'pl-new')
        self.assertEqual(player.name,'new name')
        self.assertTrue(player.is_tracked)
        self.assertEqual(
            player.integration_id, self.integration.integration_id)

    @tornado.testing.gen_test
    def test_put_untracked_player(self):
        header = { 'Content-Type':'application/json' }
        url = '/api/v2/{}/integrations/brightcove/players'.format(self.account_id)

        with patch('cmsapiv2.controllers.BrightcovePlayerHelper.publish_player') as _pub:
            pub = self._future_wrap_mock(_pub)
            self.get_player.side_effect = [{
                'id': 'pl2',
                'name': 'Player 2',
                'branches': {
                    'master': {
                        'configuration': self.tracked_player_config
                    }
                }
            }]

            r = yield self.http_client.fetch(
                self.get_url(url),
                headers=header,
                method='PUT',
                body=json.dumps({
                    'player_ref': 'pl2',
                    'is_tracked': False,
                    'integration_id': self.integration.integration_id
                }))

        self.assertEqual(1, pub.call_count)
        player = json.loads(r.body)
        self.assertEqual(player['player_ref'], 'pl2')
        self.assertFalse(player['is_tracked'])
        uninstall = controllers.BrightcovePlayerHelper._uninstall_plugin_patch(
            self.tracked_player_config)
        self.assertEqual(pub.call_args[0][0], 'pl2')
        self.assertEqual(pub.call_args[0][1], uninstall)

    @tornado.testing.gen_test
    def test_put_player_bc_404(self):
        '''Test that a BrightcoveApiClientError translates to HTTPError(404)'''
        self.get_player.side_effect = api.brightcove_api.BrightcoveApiClientError(
            404,
            'not found')
        headers = { 'Content-Type':'application/json' }
        url = '/api/v2/{}/integrations/brightcove/players'.format(self.account_id)
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url(url),
                method='PUT',
                headers=headers,
                body=json.dumps({
                    'player_ref': 'pl0',
                    'is_tracked': True,
                    'integration_id': self.integration.integration_id}))
        self.assertEqual(e.exception.code, 404)

    @tornado.testing.gen_test
    def test_install_patch(self):

        config = self.tracked_player_config
        install = controllers.BrightcovePlayerHelper._install_plugin_patch(config, 100)
        self.assertIn('example.js', install['scripts'],
                      'Keeps the non-Neon script')
        our_url = controllers.BrightcovePlayerHelper._get_current_tracking_url()
        self.assertIn(our_url, install['scripts'], 'Adds this url to scripts')
        self.assertEqual(
            2,
            len([p for p in install['plugins'] if p['name'] != 'neon']),
            'Keeps the non-Neon plugin')
        self.assertEqual(
            1,
            len([p for p in install['plugins'] if p['name'] == 'neon']),
            'Has one Neon plugin')
        self.assertNotIn('stylesheets', install, 'Patch skips stylesheets')

        # Running it again makes no change
        self.assertEqual(
            install,
            controllers.BrightcovePlayerHelper._install_plugin_patch(install, 100))

    @tornado.testing.gen_test
    def test_uninstall_patch(self):
        config = self.tracked_player_config
        uninstall = controllers.BrightcovePlayerHelper._uninstall_plugin_patch(config)

        self.assertIn('example.js', uninstall['scripts'],
                      'Keeps the non-Neon script')
        our_url = controllers.BrightcovePlayerHelper._get_current_tracking_url()
        self.assertNotIn(our_url, uninstall['scripts'], 'Remove this url from scripts')
        self.assertEqual(
            2,
            len([p for p in uninstall['plugins'] if p['name'] != 'neon']),
            'Keeps the non-Neon plugin')
        self.assertEqual(
            0,
            len([p for p in uninstall['plugins'] if p['name'] == 'neon']),
            'Has no Neon plugin')
        self.assertNotIn('stylesheets', uninstall, 'Patch skips stylesheets')

        # Running it again makes no change
        self.assertFalse(
            controllers.BrightcovePlayerHelper._uninstall_plugin_patch(uninstall))

    @tornado.testing.gen_test
    def test_uninstall_patch_string_equality(self):
        '''Ensure that plugins are removed by uninstall '''
        config = self.tracked_player_config
        config['plugins'][1]['name'] = 'neo'
        config['plugins'][1]['name'] += 'n'
        uninstall = controllers.BrightcovePlayerHelper._uninstall_plugin_patch(config)
        self.assertFalse(any([p for p in uninstall['plugins'] if p['name'] == 'neon']))

    @tornado.testing.gen_test
    def test_no_publish_patch_not_found(self):
        '''Ensure no call to publish a player is made if no Neon reference found'''

        # Build a player with no reference to Neon's plugin
        config = {
            'scripts': [
                'https://cdn.google.com/google-analytics.min.js',
                'https://optimizely.js'
            ],
            'plugins': [{
                'name': 'plugin0',
                'options': {
                    'flag': True
                }
            }]}
        player = {
            'name': 'Name of Player',
            'branches': {
                'master': {
                    'configuration': self.tracked_player_config}}}
        player['branches']['master']['configuration'].update(config)
        self.get_player.side_effect = [player]
        headers = { 'Content-Type':'application/json' }
        with patch('cmsapiv2.controllers.BrightcovePlayerHelper.publish_player') as _pub:
            pub = self._future_wrap_mock(_pub)
            url = '/api/v2/{}/integrations/brightcove/players'.format(self.account_id)
            yield self.http_client.fetch(
                self.get_url(url),
                method='PUT',
                headers=headers,
                body=json.dumps({
                    'player_ref': 'pl0',
                    'is_tracked': False,
                    'integration_id': self.integration.integration_id}))
        self.assertEqual(0, pub.call_count)
        self.assertFalse(controllers.BrightcovePlayerHelper._uninstall_plugin_patch(config))


class TestForgotPasswordHandler(TestAuthenticationBase):
    def setUp(self):
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True
        super(TestForgotPasswordHandler, self).setUp()

    def tearDown(self):
        self.verify_account_mocker.stop()
        super(TestForgotPasswordHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_no_user(self):
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'dne@test.invalid'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users/forgot_password'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="POST",
                headers=header)
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'User was not found')

    @tornado.testing.gen_test
    def test_non_email_username_no_secondary(self):
        user = neondata.User(username='testuser',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)
        yield user.save(async=True)
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'testuser'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users/forgot_password'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="POST",
                headers=header)
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'No recovery email')

    @tornado.testing.gen_test
    def test_non_email_username_with_secondary(self):
        user = neondata.User(username='testuser',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             secondary_email='kevindfenger@gmail.com',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)
        yield user.save(async=True)
        header = { 'Content-Type':'application/json' }
        params = json.dumps({'username': 'testuser'})
        url = '/api/v2/users/forgot_password'
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=params,
            method="POST",
            headers=header)
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['message'],
            'Reset Password')
        user = yield neondata.User.get(user.username, async=True)
        self.assertNotEqual(None, user.reset_password_token)

    @tornado.testing.gen_test
    def test_non_email_username_with_phone_not_available(self):
        user = neondata.User(username='testuser',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)
        yield user.save(async=True)
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'testuser', 'communication_type' : 'cell_phone'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users/forgot_password'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="POST",
                headers=header)
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'No cell phone number')

    @tornado.testing.gen_test
    def test_non_email_username_with_phone_available(self):
        user = neondata.User(username='testuser',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             cell_phone_number='123-245-3423',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)
        yield user.save(async=True)
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'testuser', 'communication_type' : 'cell_phone'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users/forgot_password'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="POST",
                headers=header)
	    self.assertEquals(e.exception.code, 501)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'recovery by phone is not ready')

    @tornado.testing.gen_test
    def test_invalid_communication_type(self):
        user = neondata.User(username='testuser',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)
        yield user.save(async=True)
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'testuser', 'communication_type' : 'carrier_pigeon'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users/forgot_password'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="POST",
                headers=header)
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Communication Type not')

    @tornado.testing.gen_test
    def test_non_expired_token(self):
        user = neondata.User(username='testuser',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             secondary_email='kf@kf.com',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)
        token = JWTHelper.generate_token(
            {'username' : 'testuser'},
            token_type=TokenTypes.RESET_PASSWORD_TOKEN)
        user.reset_password_token = token
        yield user.save(async=True)
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'testuser', 'communication_type' : 'email'})
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            url = '/api/v2/users/forgot_password'
            response = yield self.http_client.fetch(
                self.get_url(url),
                body=params,
                method="POST",
                headers=header)
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'There is a password reset comm')

    @tornado.testing.gen_test
    def test_existing_token_expired(self):
        user = neondata.User(username='kevindfenger@gmail.com',
                             password='testpassword',
                             first_name='kevin',
                             last_name='kevin',
                             access_level=neondata.AccessLevels.CREATE |
                                          neondata.AccessLevels.READ)
        token = JWTHelper.generate_token(
            {'username' : 'kevindfenger@gmail.com', 'exp': -1},
            token_type=TokenTypes.RESET_PASSWORD_TOKEN)
        user.reset_password_token = token
        yield user.save(async=True)
        header = { 'Content-Type':'application/json' }
        params = json.dumps(
            {'username': 'kevindfenger@gmail.com',
             'communication_type' : 'email'})
        url = '/api/v2/users/forgot_password'
        response = yield self.http_client.fetch(
            self.get_url(url),
            body=params,
            method="POST",
            headers=header)
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertRegexpMatches(rjson['message'],
            'Reset Password')
        user = yield neondata.User.get(user.username, async=True)
        self.assertNotEqual(None, user.reset_password_token)


class TestEmailHandler(TestControllersBase):
    def setUp(self):
        self.acct = neondata.NeonUserAccount(uuid.uuid1().hex,
                                        name='testingme')
        self.acct.save()
        user = neondata.User('fenger@neon-lab.com',
            access_level=neondata.AccessLevels.GLOBAL_ADMIN)
        user.save()
        self.account_id = self.acct.neon_api_key

        # Mock out the token decoding
        self.token_decode_patcher = patch(
            'cmsapiv2.apiv2.JWTHelper.decode_token')
        self.token_decode_mock = self.token_decode_patcher.start()
        self.token_decode_mock.return_value = {
            'username' : 'fenger@neon-lab.com'
            }
        self.http_mocker = patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
              self.http_mocker.start())
        super(TestEmailHandler, self).setUp()

    def tearDown(self):
        self.http_mocker.stop()
        self.token_decode_patcher.stop()
        super(TestEmailHandler, self).tearDown()

    @tornado.gen.coroutine
    def _send_authed_request(self, url, body, method='POST'):
        request = tornado.httpclient.HTTPRequest(
            self.get_url(url),
            method=method,
            body=json.dumps(body), 
            headers={'Authorization' : 'Bearer my_token', 
                     'Content-Type':'application/json'})
        response = yield self.http_client.fetch(request)
        raise tornado.gen.Return(response)

    @tornado.testing.gen_test
    def test_send_email_base(self): 
        url = '/api/v2/%s/email' % self.account_id 
        body = { 
            'template_slug' : 'reset-password'
        }
        self.http_mock.side_effect = lambda x: tornado.httpclient.HTTPResponse(
                x, 
                200, 
                buffer=StringIO('{"code": "Hello There you fool"}'))
        limit = neondata.AccountLimits(self.account_id)
        yield limit.save(async=True)

        response = yield self._send_authed_request(url, body) 
        
        # happens on on_finish (meaning we got our response already) 
        # wait a bit before checking 
        yield self.assertWaitForEquals(
            lambda: neondata.AccountLimits.get(self.account_id).email_posts,
            1,
            async=True)
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test 
    def test_send_email_limit_hit(self): 
        url = '/api/v2/%s/email' % self.account_id 
        body = { 
            'template_slug' : 'reset-password'
        }
        self.http_mock.side_effect = lambda x: tornado.httpclient.HTTPResponse(
                x, 
                200, 
                buffer=StringIO('{"code": "Hello There you fool"}'))
        limit = neondata.AccountLimits(
            self.account_id, 
            max_email_posts=0, 
            refresh_time_email_posts=datetime(2050,1,1))

        yield limit.save(async=True)

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self._send_authed_request(url, body)
            self.assertEquals(e.exception.code, 429)

    @tornado.testing.gen_test 
    def test_send_email_limit_reset(self): 
        url = '/api/v2/%s/email' % self.account_id 
        body = { 
            'template_slug' : 'reset-password'
        }
        self.http_mock.side_effect = lambda x: tornado.httpclient.HTTPResponse(
                x, 
                200, 
                buffer=StringIO('{"code": "Hello There you fool"}'))
        limit = neondata.AccountLimits(
            self.account_id, 
            email_posts=2, 
            max_email_posts=2, 
            refresh_time_email_posts=datetime(2000,1,1))

        yield limit.save(async=True)
        response = yield self._send_authed_request(url, body)
 
        yield self.assertWaitForEquals(
            lambda: neondata.AccountLimits.get(self.account_id).email_posts,
            1,
            async=True)
        self.assertEquals(response.code, 200)
 
    @tornado.testing.gen_test
    def test_send_email_error(self): 
        url = '/api/v2/%s/email' % self.account_id 
        body = { 
            'template_slug' : 'reset-password'
        }
        self.http_mock.side_effect = lambda x: tornado.httpclient.HTTPResponse(
                x, 
                400) 
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self._send_authed_request(url, body) 
	    self.assertEquals(e.exception.code, 400)
        rjson = json.loads(e.exception.response.body)
        self.assertRegexpMatches(rjson['error']['message'],
                                 'Mandrill')

    @tornado.testing.gen_test
    def test_send_email_user_turned_off(self): 
        url = '/api/v2/%s/email' % self.account_id 
        body = { 
            'template_slug' : 'reset-password'
        }
        user = yield neondata.User.get('fenger@neon-lab.com', async=True)
        user.send_emails = False 
        yield user.save(async=True) 
        response = yield self._send_authed_request(url, body) 
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        self.assertRegexpMatches(rjson['message'],
            'user does not')

class TestFeatureHandler(TestControllersBase):
    def setUp(self):
        self.acct = neondata.NeonUserAccount(uuid.uuid1().hex,
                                        name='testingme')
        self.acct.save()
        user = neondata.User('fenger@neon-lab.com',
            access_level=neondata.AccessLevels.GLOBAL_ADMIN)
        user.save()
        self.account_id = self.acct.neon_api_key

        # Mock out the token decoding
        self.token_decode_patcher = patch(
            'cmsapiv2.apiv2.JWTHelper.decode_token')
        self.token_decode_mock = self.token_decode_patcher.start()
        self.token_decode_mock.return_value = {
            'username' : 'fenger@neon-lab.com'
            }
        self.http_mocker = patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
              self.http_mocker.start())
        super(TestFeatureHandler, self).setUp()

    def tearDown(self):
        self.http_mocker.stop()
        self.token_decode_patcher.stop()
        super(TestFeatureHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_one_or_other_required(self):
        url = '/api/v2/feature' 
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.http_client.fetch(
                self.get_url(url))
            self.assertEquals(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_get_by_model_name(self):
        key = neondata.Feature.create_key('kfmodel', 1)
        yield neondata.Feature(key).save(async=True)
        key = neondata.Feature.create_key('kfmodel', 2)
        yield neondata.Feature(key).save(async=True)
 
        url = '/api/v2/feature?model_name=%s' % 'kfmodel' 
        response = yield self.http_client.fetch(
            self.get_url(url))
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        self.assertEquals(rjson['feature_count'], 2)
        f1 = rjson['features'][0]  
        self.assertEquals(f1['index'], 1) 
        self.assertEquals(f1['name'], None) 
        self.assertEquals(f1['variance_explained'], 0.0) 
        self.assertEquals(f1['model_name'], 'kfmodel')
 
        f2 = rjson['features'][1]  
        self.assertEquals(f2['index'], 2) 
        self.assertEquals(f2['name'], None) 
        self.assertEquals(f2['variance_explained'], 0.0) 
        self.assertEquals(f2['model_name'], 'kfmodel')
 
    @tornado.testing.gen_test
    def test_get_by_key(self):
        key = neondata.Feature.create_key('kfmodel', 1)
        yield neondata.Feature(key).save(async=True)
        key = neondata.Feature.create_key('kfmodel', 2)
        yield neondata.Feature(key).save(async=True)
        url = '/api/v2/feature?key=%s' % 'kfmodel_1,kfmodel_2' 
        response = yield self.http_client.fetch(
            self.get_url(url))

        rjson = json.loads(response.body)
        self.assertEquals(rjson['feature_count'], 2)
        f1 = rjson['features'][0]  
        self.assertEquals(f1['index'], 2) 
        self.assertEquals(f1['name'], None) 
        self.assertEquals(f1['variance_explained'], 0.0) 
        self.assertEquals(f1['model_name'], 'kfmodel')
 
        f2 = rjson['features'][1]  
        self.assertEquals(f2['index'], 1) 
        self.assertEquals(f2['name'], None) 
        self.assertEquals(f2['variance_explained'], 0.0) 
        self.assertEquals(f2['model_name'], 'kfmodel')
 
class TestEmailSupportHandler(TestControllersBase):

    def setUp(self):
        super(TestEmailSupportHandler, self).setUp()
        # Mock communication with Mandrill service.
        self.http_mocker = patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
              self.http_mocker.start())
        self.url = self.get_url('/api/v2/email/support/')
        self.headers = {'Content-Type': 'application/json'}

        self.http_mock.side_effect = lambda x: tornado.httpclient.HTTPResponse(
            x,
            200,
            buffer=StringIO('{"code": "{from_name}{from_email}{message}"}'))

    def tearDown(self):
        self.http_mocker.stop()
        self.http_mock.reset_mock()
        super(TestEmailSupportHandler, self).tearDown()

    @tornado.testing.gen_test
    def test_success(self):

        from_email = 'email@gmail.com'
        from_name = 'Joe Coolguy'
        message = 'I am contacting you in respect of a family treasure' \
                  'of Gold deposited in my name'

        body = json.dumps({
            'from_email': from_email,
            'from_name': from_name,
            'message': message
        })
        r = yield self.http_client.fetch(
            self.url,
            method='POST',
            headers=self.headers,
            body=body)
        response = json.loads(r.body)['message']
        self.assertRegexpMatches(response, 'Email sent')

        # Check that Mandrill was contacted twice (for template, for send).
        self.assertEqual(2, self.http_mock.call_count)

        # Check the contents of those requests.
        req1 = self.http_mock.call_args_list[0][0][0]
        expect_slug = controllers.EmailSupportHandler.SUPPORT_TEMPLATE_SLUG
        expect_address = controllers.EmailSupportHandler.SUPPORT_ADDRESS
        parsed = urlparse.urlparse(req1.url)
        got_slug = urlparse.parse_qs(parsed.query)['name'][0]
        self.assertEqual(expect_slug, got_slug)
        req2 = self.http_mock.call_args_list[1][0][0]
        sent = json.loads(req2.body)
        self.assertEqual(sent['message']['headers']['Reply-To'], from_email)
        expect_html = ''.join([from_name, from_email, message])
        self.assertEqual(sent['message']['html'], expect_html)
        self.assertEqual(sent['message']['to'][0]['email'], expect_address)

    @tornado.testing.gen_test
    def test_arg_is_missing(self):
        from_email = 'email@gmail.com'
        message = 'I am contacting you in respect of a family treasure' \
                  'of Gold deposited in my name'
        body = json.dumps({
            'from_email': from_email,
            'message': message
        })
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.url,
                method='POST',
                headers=self.headers,
                body=body)
        self.assertEqual(ResponseCode.HTTP_BAD_REQUEST, e.exception.code)

        from_email = 'email@gmail.com'
        from_name = 'Joe Coolguy'
        message = ''
        body = json.dumps({
            'from_email': from_email,
            'from_name': from_name,
            'message': message
        })
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.url,
                method='POST',
                headers=self.headers,
                body=body)
        self.assertEqual(ResponseCode.HTTP_BAD_REQUEST, e.exception.code)

    @tornado.testing.gen_test
    def test_email_invalid(self):
        from_email = 'emailgmail.com'
        from_name = 'Joe Coolguy'
        message = 'I am contacting you in respect of a family treasure' \
                  'of Gold deposited in my name'

        body = json.dumps({
            'from_email': from_email,
            'from_name': from_name,
            'message': message
        })
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.url,
                method='POST',
                headers=self.headers,
                body=body)
        self.assertEqual(ResponseCode.HTTP_BAD_REQUEST, e.exception.code)

class TestSocialImageGeneration(TestControllersBase):
    def setUp(self):
        self.acct = neondata.NeonUserAccount(uuid.uuid1().hex,
                                        name='testingme')
        self.acct.save()
        self.account_id = self.acct.neon_api_key

        # Mock out the verification
        self.verify_account_mocker = patch(
            'cmsapiv2.apiv2.APIV2Handler.is_authorized')
        self.verify_account_mock = self._future_wrap_mock(
            self.verify_account_mocker.start())
        self.verify_account_mock.return_value = True

        # Setup a video with a couple of thumbnails in it
        self.vid_id = neondata.InternalVideoID.generate(self.account_id, 'vid1')
        self.video_tag = neondata.Tag(None, account_id=self.account_id,
                                      video_id=self.vid_id,
                                      tag_type=neondata.TagType.VIDEO)
        self.video_tag.save()
        self.video = video = neondata.VideoMetadata(self.vid_id, 
                                       tag_id=self.video_tag.get_id(),
                                       non_job_thumb_ids=[
                                           '%s_def' % self.vid_id],
                                       job_results = [
                                           neondata.VideoJobThumbnailList(
                                               thumbnail_ids=[
                                                   '%s_n1' % self.vid_id])])
        video.save()
        t0 = neondata.ThumbnailMetadata('%s_def' % self.vid_id,
                                   ttype='default',
                                   rank=0,
                                   model_version='20160713-test',
                                   model_score=0.2,
                                   urls=['640x480'])
        t0.save()
        t1 = neondata.ThumbnailMetadata('%s_n1' % self.vid_id,
                                   ttype='neon',
                                   rank=1,
                                   model_version='20160713-test',
                                   model_score=0.4)
        t1.save()
        neondata.TagThumbnail.save_many(
            tag_id=self.video_tag.get_id(),
            thumbnail_id=[t0.get_id(), t1.get_id()])

        urls = neondata.ThumbnailServingURLs('%s_n1' % self.vid_id)
        urls.add_serving_url('800x800', 800, 800)
        urls.add_serving_url('875x500', 875, 500)
        urls.save()

        self.col_tag = neondata.Tag(None, account_id=self.account_id,
                           tag_type=neondata.TagType.COLLECTION)
        self.col_tag.save()
        t2 = neondata.ThumbnailMetadata('%s_nvd_t2' % self.account_id,
                                   ttype='neon',
                                   rank=0,
                                   model_version='20160713-test',
                                   model_score=0.2,
                                   urls=['640x480'])
        t2.save()
        t3 = neondata.ThumbnailMetadata('%s_nvd_t3' % self.account_id,
                                   ttype='neon',
                                   rank=0,
                                   model_version='20160713-test',
                                   model_score=0.8,
                                   urls=['640x480'])
        t3.save()
        neondata.TagThumbnail.save_many(
            tag_id=self.col_tag.get_id(),
            thumbnail_id=[t2.get_id(), t3.get_id()])

        # Mock out the image download
        self.im_download_mocker = patch(
            'cvutils.imageutils.PILImageUtils.download_image')
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start(), require_async_kw=True)

        def _generate_image(url):
            w, h = url.split('x')
            return PILImageUtils.create_random_image(int(h), int(w))
        self.im_download_mock.side_effect = _generate_image
        
        super(TestSocialImageGeneration, self).setUp()

    def tearDown(self):
        self.im_download_mocker.stop()
        self.verify_account_mocker.stop()
        super(TestSocialImageGeneration, self).tearDown()

    @tornado.gen.coroutine
    def get_response_image(self, video_id, platform=''):
        url = self.get_url('/api/v2/{}/social/image/{}?video_id={}'.format(
            self.account_id, platform, video_id))
        response = yield self.http_client.fetch(url, method='GET')

        raise tornado.gen.Return((PIL.Image.open(response.buffer), response))

    @tornado.testing.gen_test
    def test_basic(self):
        im, response = yield self.get_response_image('vid1')

        self.assertEquals(response.code, 200)
        self.assertEquals(response.headers['Content-Type'], 'image/jpg')
        self.assertEquals(im.size, (800,800))

        # Uncomment this to see the image for manual inspection purposes
        #im.show()

        # Check the different platforms that should ahve the same result
        im, response = yield self.get_response_image('vid1', 'facebook')

        self.assertEquals(response.code, 200)
        self.assertEquals(response.headers['Content-Type'], 'image/jpg')
        self.assertEquals(im.size, (800,800))

    @tornado.testing.gen_test
    def test_basic_twitter(self):
        im, response = yield self.get_response_image('vid1', 'twitter')

        self.assertEquals(response.code, 200)
        self.assertEquals(response.headers['Content-Type'], 'image/jpg')
        self.assertEquals(im.size, (875,500))

        # Uncomment this to see the image for manual inspection purposes
        #im.show()

    @tornado.testing.gen_test
    def test_unknown_video_id(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.get_response_image('unknownvid')

        self.assertEquals(e.exception.code, 400)
        self.assertRegexpMatches(
            json.loads(e.exception.response.body)['error']['message'],
            'Invalid video id')

    @tornado.testing.gen_test
    def test_badparams(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url('/api/v2/{}/social/image?tag_id={}'.format(
                    self.account_id, 'tag')))

        self.assertEquals(e.exception.code, 400)

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url(
                    '/api/v2/{}/social/image?tag_id={}&video_id={}'.format(
                        self.account_id, 'tag', 'vid1')))

        self.assertEquals(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_good_video_payload(self):
        self.verify_account_mock.side_effect = [NotAuthorizedError(
            'Invalid token')]
        self.video.share_token = ShareJWTHelper.encode({
             'content_type': 'VideoMetadata',
             'content_id': self.video.get_id()})
        self.video.save()
        response = yield self.http_client.fetch(
            self.get_url('/api/v2/{}/social/image?video_id={}&'
                         'share_token={}'.format(
                             self.account_id,
                             'vid1',
                             self.video.share_token)))
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_bad_video_payload(self):
        self.verify_account_mock.side_effect = [
            NotAuthorizedError('Invalid token'),
            NotAuthorizedError('Invalid token')]
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url('/api/v2/{}/social/image?video_id={}&'
                             'share_token={}'.format(
                                 self.account_id, 'vid1',
                                 ShareJWTHelper.encode({
                                     'content_type': 'VideoMetadata',
                                     'content_id': 'notexist_123'
                                     }))))

        self.assertEquals(e.exception.code, 401)

        neondata.VideoMetadata('%s_vid2' % self.account_id).save()

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.get_url('/api/v2/{}/social/image?video_id={}&'
                             'share_token={}'.format(
                                 self.account_id, 'vid1',
                                 ShareJWTHelper.encode({
                                     'content_type': 'VideoMetadata',
                                     'content_id': '%s_vid2' % self.account_id
                                     }))))

        self.assertEquals(e.exception.code, 403)

    @tornado.testing.gen_test
    def test_video_id_from_payload(self):
        self.verify_account_mock.side_effect = [NotAuthorizedError(
            'Invalid token')]
        token = ShareJWTHelper.encode({
            'content_type': 'VideoMetadata',
            'content_id': self.video.get_id()})

        self.video.share_token = token
        self.video.save()
        response = yield self.http_client.fetch(
            self.get_url('/api/v2/{}/social/image?'
                         'share_token={}'.format(
                             self.account_id,
                             token)))

        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_tag_from_url(self):
        url = self.get_url('/api/v2/{}/social/image?tag_id={}'.format(
                self.account_id,
                self.col_tag.get_id()))
        response = yield self.http_client.fetch(url)
        self.assertEqual(response.code, 200)

    @tornado.testing.gen_test
    def test_tag_from_payload(self):
        self.verify_account_mock.side_effect = [NotAuthorizedError(
            'Invalid token')]
        token = ShareJWTHelper.encode({
            'content_type': 'Tag',
            'content_id': self.col_tag.get_id()})
        self.col_tag.share_token = token
        self.col_tag.save()
        response = yield self.http_client.fetch(
            self.get_url('/api/v2/{}/social/image?'
                'share_token={}'.format(self.account_id, token)))
        self.assertEqual(response.code, 200)

    @tornado.testing.gen_test
    def test_tag_mismatch_payload(self):
        self.verify_account_mock.side_effect = [NotAuthorizedError(
            'Invalid token')]
        token = ShareJWTHelper.encode({
            'content_type': 'Tag',
            'content_id': 'bad_id'})
        self.col_tag.share_token = token
        self.col_tag.save()
        url = self.get_url('/api/v2/{}/social/image?'
            'share_token={}'.format(self.account_id, token))
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(url)
        self.assertEqual(e.exception.code, 401)

    @tornado.testing.gen_test
    def test_old_video_format(self):
        video = neondata.VideoMetadata(self.vid_id, 
                                       tids=[
                                           '%s_def' % self.vid_id,
                                           '%s_n1' % self.vid_id])
        video.save()

        im, response = yield self.get_response_image('vid1')

        self.assertEquals(response.code, 200)
        self.assertEquals(response.headers['Content-Type'], 'image/jpg')

    @tornado.testing.gen_test
    def test_no_neon_thumb(self):
        video = neondata.VideoMetadata(self.vid_id, 
                                       tids=['%s_def' % self.vid_id])
        video.save()

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.get_response_image('vid1')

        self.assertEquals(e.exception.code, 400)
        self.assertRegexpMatches(
            json.loads(e.exception.response.body)['error']['message'],
            'Video does not have any valid good thumbs')

    @tornado.testing.gen_test
    def test_no_precut_rendition(self):
        neondata.ThumbnailMetadata('%s_n2' % self.vid_id,
                                   ttype='neon',
                                   rank=0,
                                   model_version='20160713-test',
                                   model_score=0.6,
                                   urls=['640x480']).save()
        video = neondata.VideoMetadata(self.vid_id, 
                                       tids=['%s_n2' % self.vid_id])
        video.save()

        im, response = yield self.get_response_image('vid1')

        self.assertEquals(response.code, 200)
        self.assertEquals(response.headers['Content-Type'], 'image/jpg')
        self.assertEquals(im.size, (800,800))

        # Uncomment this to see the image for manual inspection purposes
        #im.show()
        

    @tornado.testing.gen_test
    def test_failed_image_download(self):
        def _fail_download(x):
            raise IOError('Ooops')
        self.im_download_mock.side_effect = _fail_download

        with self.assertLogExists(logging.ERROR,
                                  'Error downloading source image'):
            with self.assertRaises(tornado.httpclient.HTTPError) as e:
                yield self.get_response_image('vid1')

        self.assertEquals(e.exception.code, 500)
        self.assertRegexpMatches(
            json.loads(e.exception.response.body)['error']['message'],
            'Internal Server Error')

    @tornado.testing.gen_test
    def test_missing_rendition(self):
        neondata.ThumbnailMetadata('%s_n2' % self.vid_id,
                                   ttype='neon',
                                   rank=0,
                                   model_version='20160713-test',
                                   model_score=0.6).save()
        video = neondata.VideoMetadata(self.vid_id, 
                                       tids=['%s_n2' % self.vid_id])
        video.save()

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.get_response_image('vid1')

        self.assertEquals(e.exception.code, 400)
        self.assertRegexpMatches(
            json.loads(e.exception.response.body)['error']['message'],
            'Could not generate a rendition')

class TestTagHandler(TestVerifiedControllersBase):
    def setUp(self):
        super(TestTagHandler, self).setUp()
        # Create several thumbnails.
        acct = neondata.NeonUserAccount(uuid.uuid1().hex, name='me')
        acct.save()
        self.account_id = acct.neon_api_key
        thumbnails = [neondata.ThumbnailMetadata(
            '%s_vid0_%s' % (self.account_id, uuid.uuid1().hex))
            for _ in range(5)]
        [t.save() for t in thumbnails]
        self.thumbnail_ids = [t.get_id() for t in thumbnails]
        self.url = self.get_url('/api/v2/%s/tags/' % self.account_id)

    @tornado.testing.gen_test
    def test_get(self):
        tag_1 = neondata.Tag(
            account_id=self.account_id,
            name='Green',
            tag_type=neondata.TagType.COLLECTION)
        tag_1.save()
        thumb_1 = neondata.ThumbnailMetadata('%s_t1' % self.account_id)
        thumb_2 = neondata.ThumbnailMetadata('%s_t2' % self.account_id)
        neondata.TagThumbnail.save_many(
            tag_id=tag_1.get_id(),
            thumbnail_id=[thumb_1.get_id(), thumb_2.get_id()])
        tag_2 = neondata.Tag(
            account_id=self.account_id,
            name='Blue',
            tag_type='INVALID')
        tag_2.save()
        tag_id = ','.join([tag_1.get_id(), tag_2.get_id(), 'invalid'])
        url = '%s?tag_id=%s' % (self.url, tag_id)
        response = yield self.http_client.fetch(url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual({tag_1.get_id(), tag_2.get_id()}, set(rjson.keys()))
        green_gallery_tag = rjson[tag_1.get_id()]
        self.assertEqual('Green', green_gallery_tag['name'])
        self.assertEqual(neondata.TagType.COLLECTION, green_gallery_tag['tag_type'])
        ids = set([thumb_1.get_id(), thumb_2.get_id()])
        self.assertEqual(ids, set(green_gallery_tag['thumbnail_ids']))
        blue_tag = rjson[tag_2.get_id()]
        self.assertEqual('Blue', blue_tag['name'])
        self.assertEqual(None, blue_tag['tag_type'])

    @tornado.testing.gen_test
    def test_int_vs_ext_video_id(self):
        tag = neondata.Tag(
            account_id=self.account_id,
            name="Red",
            video_id=neondata.InternalVideoID.generate(self.account_id, 'a0b1c2'),
            tag_type=neondata.TagType.VIDEO)
        tag.save()


        url = '%s?tag_id=%s' % (self.url, tag.get_id())
        response = yield self.http_client.fetch(url, headers=self.headers)
        self.assertEqual(200, response.code)
        rjson = json.loads(response.body)

        expected_external_video_id = neondata.InternalVideoID.to_external(tag.video_id)
        self.assertEqual(expected_external_video_id, rjson[tag.get_id()]['video_id'])



    @tornado.testing.gen_test
    def test_put(self):
        tag = neondata.Tag(
            account_id=self.account_id,
            name='Green',
            tag_type=neondata.TagType.COLLECTION)
        tag.save()
        thumbnails = [neondata.ThumbnailMetadata(
            '%s_vid0_%d' % (self.account_id, i)) for i in range(20)]
        [t.save() for t in thumbnails]
        thumb_ids = [t.get_id() for t in thumbnails]
        body = json.dumps({
            'tag_id': tag.get_id(),
            'thumbnail_ids': ','.join(thumb_ids),
            'hidden': True})
        response = yield self.http_client.fetch(
            self.url,
            method='PUT',
            headers=self.headers,
            body=body)
        self.assertEqual(200, response.code)
        rjson = json.loads(response.body)
        self.assertEqual(tag.get_id(), rjson['tag_id'])
        self.assertEqual(tag.account_id, rjson['account_id'])
        self.assertEqual(set(thumb_ids), set(rjson['thumbnail_ids']))
        self.assertEqual(tag.name, rjson['name'])
        self.assertNotIn('hidden', rjson)
        tag = neondata.Tag.get(tag.get_id())
        thumb_ids = neondata.TagThumbnail.get(tag_id=tag.get_id())
        self.assertEqual(tag.get_id(), rjson['tag_id'])
        self.assertEqual(tag.account_id, rjson['account_id'])
        self.assertEqual(set(thumb_ids), set(rjson['thumbnail_ids']))
        self.assertEqual(tag.name, rjson['name'])
        self.assertTrue(tag.hidden)

    @tornado.testing.gen_test
    def test_post_tag_no_thumbnail_id(self):
        '''Create a tag without any thumbnail_id.'''
        name = 'My Vacation in Djibouti 2016/05/01'
        body = json.dumps({'name': name})
        response = yield self.http_client.fetch(
            self.url,
            method='POST',
            headers=self.headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual(name, rjson['name'])
        tag = yield neondata.Tag.get(rjson['tag_id'], async=True)
        self.assertEqual(rjson['tag_id'], tag.get_id())
        self.assertEqual(name, tag.name)

    @tornado.testing.gen_test
    def test_post_tag_one_thumbnail_id(self):
        '''Create a tag with one thumbnail_id.'''
        name = u'Photos from مسجد سليمان 2015/11/02'
        thumbnail_id = random.choice(self.thumbnail_ids)
        body = json.dumps({'name': name, 'thumbnail_ids': thumbnail_id})
        response = yield self.http_client.fetch(
            self.url,
            method='POST',
            headers=self.headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertIsNotNone(rjson['tag_id'])
        self.assertEqual(name, rjson['name'])
        self.assertIn(thumbnail_id, rjson['thumbnail_ids'])
        tag = yield neondata.Tag.get(rjson['tag_id'], async=True)
        self.assertEqual(rjson['tag_id'], tag.get_id())
        self.assertEqual(name, tag.name)
        has_result = yield neondata.TagThumbnail.has(
            tag_id=tag.get_id(),
            thumbnail_id=thumbnail_id,
            async=True)
        self.assertTrue(has_result)

        name = u'That time in Baden-Württemberg'
        thumbnail_id = '%s_vid0_invalidid' % self.account_id
        body = json.dumps({'name': name, 'thumbnail_ids': thumbnail_id})
        # Let the tag be created with no thumbnail.
        response = yield self.http_client.fetch(
            self.url,
            method='POST',
            headers=self.headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual(name, rjson['name'])
        self.assertNotIn(thumbnail_id, rjson['thumbnail_ids'])
        tag = yield neondata.Tag.get(rjson['tag_id'], async=True)
        self.assertEqual(rjson['tag_id'], tag.get_id())
        self.assertEqual(name, tag.name)
        has_result = yield neondata.TagThumbnail.has(
            tag_id=tag.get_id(),
            thumbnail_id=thumbnail_id,
            async=True)
        self.assertFalse(has_result)

    @tornado.testing.gen_test
    def test_post_tag_many_thumbnail_ids(self):
        '''Post a tag with a mix of valid and invalid thumbnail_ids.'''
        name = u'서울'
        thumbnail_ids = list({
            random.choice(self.thumbnail_ids),
            random.choice(self.thumbnail_ids),
            random.choice(self.thumbnail_ids)})
        body = json.dumps({
            'name': name,
            'thumbnail_ids': ','.join(thumbnail_ids)})
        response = yield self.http_client.fetch(
            self.url,
            method='POST',
            headers=self.headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual(name, rjson['name'])
        self.assertEqual(set(thumbnail_ids), set(rjson['thumbnail_ids']))
        tag = yield neondata.Tag.get(rjson['tag_id'], async=True)
        self.assertEqual(rjson['tag_id'], tag.get_id())
        self.assertEqual(name, tag.name)
        has_result = yield neondata.TagThumbnail.has_many(
            tag_id=tag.get_id(),
            thumbnail_id=thumbnail_ids,
            async=True)
        for a in thumbnail_ids:
            self.assertTrue(has_result[(tag.get_id(), a)])

        # Try with some invalid thumbnail_ids.
        invalid_assocs = [
            '%s_invalid_id1' % self.account_id,
            '%s_invalid_id2' % self.account_id]
        mixed_assocs = thumbnail_ids + invalid_assocs
        name = 'That time in Paris... <we both say> Jean-Luc!'
        body = json.dumps({'name': name, 'thumbnail_ids': ','.join(mixed_assocs)})
        response = yield self.http_client.fetch(
            self.url,
            method='POST',
            headers=self.headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual(name, rjson['name'])
        self.assertEqual(set(thumbnail_ids), set(rjson['thumbnail_ids']))
        tag = yield neondata.Tag.get(rjson['tag_id'], async=True)
        self.assertEqual(rjson['tag_id'], tag.get_id())
        self.assertEqual(name, tag.name)
        tts = yield neondata.TagThumbnail.get_many(tag_id=tag.get_id(), async=True)
        self.assertEqual(set(thumbnail_ids), set(tts[tag.get_id()]))

        # Try with only invalid thumbnail_ids.
        name = 'Poughkeepsie'
        body = json.dumps({'name': name, 'thumbnail_ids': ','.join(invalid_assocs)})
        response = yield self.http_client.fetch(
            self.url,
            method='POST',
            headers=self.headers,
            body=body)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual(name, rjson['name'])
        self.assertEqual([], rjson['thumbnail_ids'])
        tag = yield neondata.Tag.get(rjson['tag_id'], async=True)
        self.assertEqual(rjson['tag_id'], tag.get_id())
        self.assertEqual(name, tag.name)
        tts = yield neondata.TagThumbnail.get_many(tag_id=tag.get_id(), async=True)
        self.assertEqual([], list(tts[tag.get_id()]))

    @tornado.testing.gen_test
    def test_cannot_create_without_name(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.url,
                method='POST',
                headers=self.headers,
                body='')
        self.assertEqual(ResponseCode.HTTP_BAD_REQUEST, e.exception.code)

        # Even with an empty string name, I cannot create a tag.
        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.url,
                method='POST',
                headers=self.headers,
                body=json.dumps({'name': ''}))
        self.assertEqual(ResponseCode.HTTP_BAD_REQUEST, e.exception.code)

    @tornado.testing.gen_test
    def test_cannot_create_with_thumbnail_of_other_user(self):

        # Mix valid thumbs and invalid thumb.
        other_user_thumb = neondata.ThumbnailMetadata(
            'otheruser_vid0_a0b1c2')
        other_user_thumb.save()
        ids = self.thumbnail_ids + [other_user_thumb.key]
        random.shuffle(ids)

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                self.url,
                method='POST',
                headers=self.headers,
                body=json.dumps({
                    'name': 'Failure',
                    'thumbnail_ids': ','.join(ids)}))
        self.assertEqual(ResponseCode.HTTP_FORBIDDEN, e.exception.code)

    @tornado.testing.gen_test
    def test_delete(self):
        tag = neondata.Tag(account_id=self.account_id, name='Red')
        tag.save()
        thumbnail_ids = list({
            random.choice(self.thumbnail_ids),
            random.choice(self.thumbnail_ids),
            random.choice(self.thumbnail_ids)})
        neondata.TagThumbnail.save_many(
            tag_id=tag.get_id(),
            thumbnail_id=thumbnail_ids)

        r = yield self.http_client.fetch(
            '%s?tag_id=%s' % (self.url, tag.get_id()),
            headers=self.headers,
            method='DELETE')
        self.assertEqual(200, r.code)
        r = json.loads(r.body)
        self.assertEqual(tag.get_id(), r['tag_id'])
        no_tag = neondata.Tag.get(tag.get_id())
        self.assertIsNone(no_tag)
        no_thumbs = neondata.TagThumbnail.get(tag_id=tag.get_id())
        self.assertFalse(no_thumbs)

    @tornado.testing.gen_test
    def test_cant_delete(self):
        tag = neondata.Tag(account_id='somebody else', name='Green')
        tag.save()
        thumbnail_ids = list({
            random.choice(self.thumbnail_ids),
            random.choice(self.thumbnail_ids),
            random.choice(self.thumbnail_ids)})
        neondata.TagThumbnail.save_many(
            tag_id=tag.get_id(),
            thumbnail_id=thumbnail_ids)

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                '%s?tag_id=%s' % (self.url, tag.get_id()),
                headers=self.headers,
                method='DELETE')
        self.assertEqual(ResponseCode.HTTP_FORBIDDEN, e.exception.code)
        tag = neondata.Tag.get(tag.get_id())
        self.assertIsNotNone(tag)
        thumbs = neondata.TagThumbnail.get(tag_id=tag.get_id())
        self.assertEqual(set(thumbs), set(thumbnail_ids))

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            yield self.http_client.fetch(
                '%s?tag_id=%s' % (self.url, 'not_a_id'),
                headers=self.headers,
                method='DELETE')
        self.assertEqual(ResponseCode.HTTP_NOT_FOUND, e.exception.code)
        tag = neondata.Tag.get(tag.get_id())
        self.assertIsNotNone(tag)
        thumbs = neondata.TagThumbnail.get(tag_id=tag.get_id())
        self.assertEqual(set(thumbs), set(thumbnail_ids))


class TestTagSearchInternalHandler(TestVerifiedControllersBase):

    @tornado.testing.gen_test
    def test_search_with_default_limit(self):
        pstr = 'cmsdb.neondata.Tag.search_for_objects_and_times'
        search = patch(pstr)
        wrapped = self._future_wrap_mock(search.start())
        def side_effect(**kwargs):
            return [], None, None
        wrapped.side_effect = side_effect

        # In particular we expect limit to be defaulted to 25.
        url = self.get_url('/api/v2/tags/search/')
        r = yield self.http_client.fetch(url)
        self.assertEqual(r.code, 200)
        wrapped.assert_called_with(
            account_id=None,
            since=0.0,
            limit=25,
            show_hidden=False,
            query=None,
            until=0.0,
            tag_type=None)

        search.stop()


class TestTagSearchExternalHandler(TestVerifiedControllersBase):

    def setUp(self):
        super(TestTagSearchExternalHandler, self).setUp()
        self.url = self.get_url(
            '/api/v2/%s/tags/search/' % self.account_id)
        self.thumbs_url = self.get_url(
            '/api/v2/%s/tags/?tag_id={tag_id}' % self.account_id)

    @tornado.testing.gen_test
    def test_search_with_default_limit(self):
        pstr = 'cmsdb.neondata.Tag.search_for_objects_and_times'
        search = patch(pstr)
        wrapped = self._future_wrap_mock(search.start())

        # Search expects a list and two times to be raised.
        def side_effect(**kwargs):
            return [], None, None
        wrapped.side_effect = side_effect
        r = yield self.http_client.fetch(self.url)
        self.assertEqual(r.code, 200)
        # We must see a limit of 25 set.
        wrapped.assert_called_with(
            account_id=self.account_id,
            since=0.0,
            limit=25,
            show_hidden=False,
            query=None,
            until=0.0,
            tag_type=None)

        search.stop()

    @tornado.testing.gen_test
    def test_search_no_item(self):
        response = yield self.http_client.fetch(self.url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual([], rjson['items'])
        self.assertEqual(0, rjson['count'])

    @tornado.testing.gen_test
    def test_search_one_tag_some_thumbs(self):
        thumbnails = [
            neondata.ThumbnailMetadata(
                '%s_%s' % (self.account_id_api_key, uuid.uuid1().hex))
            for _ in range(5)]
        [t.save() for t in thumbnails]
        tag = neondata.Tag(
            None,
            name='My Photos',
            account_id=self.account_id,
            tag_type=neondata.TagType.COLLECTION)
        tag.save()

        neondata.TagThumbnail.save_many(
            tag_id=tag.get_id(),
            thumbnail_id=[t.get_id() for t in thumbnails])

        response = yield self.http_client.fetch(self.url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, response.code)
        rjson = json.loads(response.body)
        self.assertEqual(1, rjson['count'])
        tags = rjson['items']
        self.assertEqual('My Photos', tags[0]['name'])

        tag_id = ','.join([t['tag_id'] for t in tags])
        tags_response = yield self.http_client.fetch(
            self.thumbs_url.format(tag_id=tag_id),
            headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, tags_response.code)
        tags_rjson = json.loads(tags_response.body)
        response_ids = set(tags_rjson[tag_id]['thumbnail_ids'])
        given_ids = {thumb.get_id() for thumb in thumbnails}
        self.assertEqual(given_ids, response_ids)

    @tornado.testing.gen_test
    def test_search_on_many(self):

        # Make some thumbnails.
        thumbnails = [neondata.ThumbnailMetadata(
            '%s_vid0_%s' % (
                self.account_id,
                uuid.uuid1().hex))
              for _ in range(5)]
        # This won't appear in the results.
        removed_thumb = random.choice(thumbnails)
        [t.save() for t in thumbnails]
        given_thumb_ids = {t.get_id() for t in thumbnails
            if t.get_id() != removed_thumb.get_id()}

        # Make some tags.
        tags = [neondata.Tag(
                i,
                name='name' + i,
                account_id=self.account_id,
                tag_type=neondata.TagType.COLLECTION)
            for i in 'abced']
        [t.save() for t in tags]
        removed_tag = random.choice(tags)
        given_tag_ids = {t.get_id() for t in tags
            if t.get_id() != removed_tag.get_id()}

        # Map each tag to each thumbnail, minus the removed one.
        neondata.TagThumbnail.save_many(
            tag_id=given_tag_ids,
            thumbnail_id=given_thumb_ids)

        # Do search.
        r = yield self.http_client.fetch(self.url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, r.code)
        r = json.loads(r.body)
        self.assertIn('next_page', r)
        self.assertIn('prev_page', r)
        self.assertNotEqual(r['next_page'], r['prev_page'])

        # Even the empty tag is in the response.
        self.assertEqual(len(tags), r['count'])

        # Get the tags thumbs.
        tag_id = ','.join([t.key for t in tags])
        tags_response = yield self.http_client.fetch(
            self.thumbs_url.format(tag_id=tag_id))
        self.assertEqual(ResponseCode.HTTP_OK, tags_response.code)
        tags_rjson = json.loads(tags_response.body)

        # Each item is a tag->thumbs mapping.
        for item in r['items']:
            _tag = tags_rjson[item['tag_id']]
            if item['tag_id'] == removed_tag.get_id():
                self.assertEqual('name' + item['tag_id'], item['name'])
                self.assertFalse(_tag['thumbnail_ids'])
            else:
                self.assertEqual('name' + item['tag_id'], item['name'])
                self.assertEqual(
                    given_thumb_ids,
                    set(_tag['thumbnail_ids']))

    @tornado.testing.gen_test
    def test_search_query_in_name(self):
        wanted_tag = neondata.Tag(
            uuid.uuid1().hex,
            account_id=self.account_id,
            name='Apple orange pear')
        wanted_tag.save()
        unwanted_tag = neondata.Tag(
            uuid.uuid1().hex,
            account_id=self.account_id,
            name='Croissant bagel doughnut')
        unwanted_tag.save()

        # Query term in middle.
        url = self.url + '?query=%s' % 'orange'
        r = yield self.http_client.fetch(url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, r.code)
        r = json.loads(r.body)
        self.assertEqual(wanted_tag.key, r['items'][0]['tag_id'])

        # Case insensitive.
        url = self.url + '?query=%s' % 'apple'
        r = yield self.http_client.fetch(url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, r.code)
        r = json.loads(r.body)
        self.assertEqual(wanted_tag.key, r['items'][0]['tag_id'])

        # Regex style wildcards.
        url = self.url + '?query=%s' % 'p..r'  # matches pear.
        r = yield self.http_client.fetch(url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, r.code)
        r = json.loads(r.body)
        self.assertEqual(wanted_tag.key, r['items'][0]['tag_id'])

        url = self.url + '?query=%s' % 'pp.*r'  # matches apple pear.
        r = yield self.http_client.fetch(url, headers=self.headers)
        self.assertEqual(ResponseCode.HTTP_OK, r.code)
        r = json.loads(r.body)
        self.assertEqual(wanted_tag.key, r['items'][0]['tag_id'])

class TestClipHandler(TestVerifiedControllersBase):
    def setUp(self):
        super(TestClipHandler, self).setUp()
        acct = neondata.NeonUserAccount('testa', name='me')
        acct.save()
        self.account_id = acct.neon_api_key
        # save a video
        vjtl = neondata.VideoJobThumbnailList(
            clip_ids=['testa_vid1_1', 'testa_vid1_2', 'testa_vid1_3'])
        self.vm = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.account_id,'vid1'),
                job_results=vjtl)
        self.vm.save()
        # save a few clips 
        clip = neondata.Clip(
            'testa_vid1_1', 
            video_id='testa_vid1',
            thumbnail_id='tid1',
            urls=['myclip.mp4'],
            ttype=neondata.ClipType.NEON,
            rank=2,
            model_version='model1',
            enabled=True,
            score=0.45,
            start_frame=47,
            end_frame=89,
            duration=1.4)
        clip.save() 
        clip = neondata.Clip(
            'testa_vid1_2', 
            video_id='vid1')
        clip.save() 
        clip = neondata.Clip(
            'testa_vid1_3', 
            video_id='vid1')
        clip.save() 

        neondata.VideoRendition(url='1_640_480.mp4',
                                width=640,
                                height=480,
                                duration=36.6,
                                codec='h264',
                                container='mp4',
                                clip_id='testa_vid1_1').save()
        neondata.VideoRendition(url='2_300_400.gif',
                                width=300,
                                height=400,
                                duration=36.6,
                                codec=None,
                                container='gif',
                                clip_id='testa_vid1_1').save()

        # Save a few renditions for the first clip
        self.url = self.get_url(
            '/api/v2/%s/clips' % self.account_id)

    @tornado.testing.gen_test
    def test_get_clip_does_exist(self): 
        clip_ids = self.vm.job_results.clip_ids
        url = self.url + '?clip_ids=%s' % clip_ids[0]
        res = yield self.http_client.fetch(url, headers=self.headers)
        rj = json.loads(res.body)
        self.assertEquals(rj['count'], 1) 
        rv_clip = rj['clips'][0] 

        self.assertDictContainsSubset({
            'video_id' : 'vid1',
            'rank' : 2,
            'start_frame' : 47,
            'end_frame' : 89,
            'enabled': True,
            'url': 'myclip.mp4',
            'type': 'neon',
            'neon_score': 0.45,
            'duration' : 1.4},
            rv_clip)

        self.assertNotIn('renditions', rv_clip)

    @tornado.testing.gen_test
    def test_get_clip_renditions(self): 
        url = self.url + '?clip_ids=testa_vid1_1&fields=renditions'
        res = yield self.http_client.fetch(url, headers=self.headers)
        rj = json.loads(res.body)
        self.assertEquals(rj['count'], 1) 
        rv_clip = rj['clips'][0] 

        self.assertEquals(len(rv_clip['renditions']), 2)
        rends = {x['url']: x for x in rv_clip['renditions']}
        self.assertEquals(rends['1_640_480.mp4']['width'], 640)
        self.assertEquals(rends['1_640_480.mp4']['height'], 480)
        self.assertAlmostEqual(rends['1_640_480.mp4']['duration'], 36.6)
        self.assertEquals(rends['1_640_480.mp4']['codec'], 'h264')
        self.assertEquals(rends['1_640_480.mp4']['container'], 'mp4')
        self.assertEquals(rends['2_300_400.gif']['width'], 300)
        self.assertEquals(rends['2_300_400.gif']['height'], 400)
        self.assertAlmostEqual(rends['2_300_400.gif']['duration'], 36.6)
        self.assertIsNone(rends['2_300_400.gif']['codec'])
        self.assertEquals(rends['2_300_400.gif']['container'], 'gif')
 
    @tornado.testing.gen_test
    def test_get_clip_does_not_exist(self): 
        url = self.url + '?clip_ids=x'
        res = yield self.http_client.fetch(url, headers=self.headers)
        rj = json.loads(res.body) 
        self.assertEquals(rj['clips'], [{}]) 
        self.assertEquals(rj['count'], 1) 
 
    @tornado.testing.gen_test
    def test_get_many_clips_all_exist(self): 
        clip_ids = self.vm.job_results.clip_ids
        url = self.url + '?clip_ids=%s,%s' % (clip_ids[0], clip_ids[1])
        res = yield self.http_client.fetch(url, headers=self.headers)
        rj = json.loads(res.body)
        rv_clip_one = rj['clips'][0] 
        rv_clip_two = rj['clips'][1]
 
        self.assertEquals(rv_clip_one['video_id'], 'vid1')  
        self.assertEquals(rv_clip_one['clip_id'], 'testa_vid1_1')  
        self.assertEquals(rv_clip_two['video_id'], 'vid1')  
        self.assertEquals(rv_clip_two['clip_id'], 'testa_vid1_2')  
        self.assertEquals(rj['count'], 2) 
 
    @tornado.testing.gen_test
    def test_get_many_clips_some_exist(self): 
        clip_ids = self.vm.job_results.clip_ids
        url = self.url + '?clip_ids=%s,%s,sdfa' % (clip_ids[0], clip_ids[1])
        res = yield self.http_client.fetch(url, headers=self.headers)
        rj = json.loads(res.body)
        rv_clip_one = rj['clips'][0] 
        rv_clip_two = rj['clips'][1]
        rv_clip_three = rj['clips'][2]

        self.assertEquals(rv_clip_one['video_id'], 'vid1')  
        self.assertEquals(rv_clip_one['clip_id'], 'testa_vid1_1')  
        self.assertEquals(rv_clip_two['video_id'], 'vid1')  
        self.assertEquals(rv_clip_two['clip_id'], 'testa_vid1_2')  
        self.assertEquals(rv_clip_three, {})  
        self.assertEquals(rj['count'], 3) 
 
if __name__ == "__main__" :
    args = utils.neon.InitNeon()
    unittest.main(argv=(['%prog']+args))
