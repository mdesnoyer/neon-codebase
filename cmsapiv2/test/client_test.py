#!/usr/bin/env python
'''
Unittests for the cmsapiv2 client

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cmsapiv2.client
from cStringIO import StringIO
import json
import logging
from mock import MagicMock, patch
import test_utils.neontest
import tornado.httpclient
import tornado.testing
import unittest
import utils.neon

class AuthTest(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(AuthTest, self).setUp()
        self.client = cmsapiv2.client.Client('someuser', 'somepass')

        # Mock out the http requests
        self.auth_mock = MagicMock()
        self.auth_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, code=200,
              buffer=StringIO('{"access_token":"atok","refresh_token":"rtok"}')
              )
        self.api_mock = MagicMock()
        self.api_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=200)
        self.send_request_patcher = patch('utils.http.send_request')
        self.send_request_mock = self._future_wrap_mock(
            self.send_request_patcher.start(), require_async_kw=True)
        def _handle_http_request(req, **kw):
            if 'auth.neon-lab.com' in req.url:
                return self.auth_mock(req, **kw)
            else:
                return self.api_mock(req, **kw)
        self.send_request_mock.side_effect = _handle_http_request

    def tearDown(self):
        self.send_request_patcher.stop()
        super(AuthTest, self).tearDown()

    @tornado.testing.gen_test
    def test_second_request_still_authed(self):
        request = tornado.httpclient.HTTPRequest(
            '/api/v2/video', method='GET')
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 200)

        # Check that there was an authentication call with username
        # and password
        self.assertEquals(self.auth_mock.call_count, 1)
        cargs, kwargs = self.auth_mock.call_args_list[0]
        self.assertEquals(kwargs['no_retry_codes'], [401])
        auth_request = cargs[0]
        self.assertEquals(auth_request.url,
                          'https://auth.neon-lab.com/api/v2/authenticate')
        self.assertEquals(auth_request.method, 'POST')
        auth_body = json.loads(auth_request.body)
        self.assertEquals(auth_body, {'username': 'someuser',
                                      'password': 'somepass'})
        self.assertEquals(auth_request.headers,
                          {'Content-Type': 'application/json'})
        self.assertEquals(self.client.access_token, 'atok')
        self.assertEquals(self.client.refresh_token, 'rtok')

        # Check that the main request went out
        self.assertEquals(self.api_mock.call_count, 1)
        cargs, kwargs = self.api_mock.call_args_list[0]
        self.assertEquals(kwargs['no_retry_codes'], [401])
        api_request = cargs[0]
        self.assertEquals(api_request.url,
                          'https://services.neon-lab.com/api/v2/video')
        self.assertEquals(api_request.method, 'GET')
        self.assertEquals(api_request.headers, 
                          {'Authorization' : 'Bearer %s' % 
                           self.client.access_token})

        # Now reset the mocks
        self.auth_mock.reset_mock()
        self.api_mock.reset_mock()

        # Send another request. Shouldn't need to hit the auth server
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 200)
        self.assertFalse(self.auth_mock.called)
        self.assertEquals(self.api_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_need_to_refresh_token(self):
        request = tornado.httpclient.HTTPRequest(
            '/api/v2/video', method='GET')
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 200)

        self.assertIsNotNone(self.client.access_token)
        self.assertIsNotNone(self.client.refresh_token)
        self.assertEquals(self.api_mock.call_count, 1)
        self.assertEquals(self.auth_mock.call_count, 1)
        self.auth_mock.reset_mock()
        self.api_mock.reset_mock()

        refresh_token = self.client.refresh_token

        # Now simulate losing authentication
        self.api_mock.side_effect = [
          tornado.httpclient.HTTPResponse(
              request, code=401,
              error=tornado.httpclient.HTTPError(401)),
          tornado.httpclient.HTTPResponse(request, code=200)]
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 200)

        self.assertIsNotNone(self.client.access_token)
        self.assertIsNotNone(self.client.refresh_token)
        self.assertEquals(self.api_mock.call_count, 2)
        self.assertEquals(self.auth_mock.call_count, 1)

        # Check the auth call
        cargs, kwargs = self.auth_mock.call_args_list[0]
        self.assertEquals(kwargs['no_retry_codes'], [401])
        auth_request = cargs[0]
        self.assertEquals(auth_request.url,
                          'https://auth.neon-lab.com/api/v2/authenticate')
        self.assertEquals(auth_request.method, 'POST')
        self.assertEquals(auth_request.body, '')
        self.assertEquals(auth_request.headers,
                          {'Authorization': 'Bearer %s' % refresh_token})
        self.assertIsNotNone(self.client.access_token)
        self.assertIsNotNone(self.client.refresh_token)

    @tornado.testing.gen_test
    def test_bad_user_pass(self):
        self.auth_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=401)
        
        request = tornado.httpclient.HTTPRequest(
            '/api/v2/video', method='GET')
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 401)
        self.assertEquals(res.error.code, 401)

    @tornado.testing.gen_test
    def test_not_enough_permissions(self):
        # In this case, the token returned doesn't have permission to
        # make the call, so re should return a 401
        self.api_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=401)

        request = tornado.httpclient.HTTPRequest(
            '/api/v2/video', method='GET')
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 401)
        self.assertEquals(res.error.code, 401)

        self.assertEquals(self.api_mock.call_count, 2)
        self.assertEquals(self.auth_mock.call_count, 2)

    @tornado.testing.gen_test
    def test_invalid_api_call(self):
        self.api_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, code=404)

        request = tornado.httpclient.HTTPRequest(
            '/api/v2/video', method='GET')
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 404)
        self.assertEquals(res.error.code, 404)

        self.assertEquals(self.api_mock.call_count, 1)
        self.assertEquals(self.auth_mock.call_count, 1)

        self.assertEquals(self.client.access_token, 'atok')
        self.assertEquals(self.client.refresh_token, 'rtok')

    @tornado.testing.gen_test
    def test_expired_refresh_token(self):
        request = tornado.httpclient.HTTPRequest(
            '/api/v2/video', method='GET')
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 200)

        self.auth_mock.reset_mock()
        self.api_mock.reset_mock()

        # Now mock out the api call being rejected and the
        # authentication from the refresh token
        self.api_mock.side_effect = [
          tornado.httpclient.HTTPResponse(
              request, code=401,
              error=tornado.httpclient.HTTPError(401)),
          tornado.httpclient.HTTPResponse(request, code=200)]

        self.auth_mock.side_effect = [
          tornado.httpclient.HTTPResponse(
              request, code=401,
              error=tornado.httpclient.HTTPError(401)),
          tornado.httpclient.HTTPResponse(
              request, code=200,
              buffer=StringIO('{"access_token":"atok2",'
                              '"refresh_token":"rtok2"}'))]

        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 200)

        self.assertEquals(self.client.access_token, 'atok2')
        self.assertEquals(self.client.refresh_token, 'rtok2')
        self.assertEquals(self.api_mock.call_count, 2)
        self.assertEquals(self.auth_mock.call_count, 2)

    @tornado.testing.gen_test
    def test_full_url(self):
        request = tornado.httpclient.HTTPRequest(
            'http://10.56.34.78/api/v2/video', method='GET')
        res = yield self.client.send_request(request)
        self.assertEquals(res.code, 200)

        self.assertEquals(self.api_mock.call_count, 1)
        cargs, kwargs = self.api_mock.call_args_list[0]
        api_request = cargs[0]
        self.assertEquals(api_request.url,
                          'http://10.56.34.78/api/v2/video')
        self.assertEquals(api_request.method, 'GET')
        self.assertEquals(api_request.headers, 
                          {'Authorization' : 'Bearer %s' % 
                           self.client.access_token})

        
        

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
