#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cnn_api
from cmsdb import neondata
import integrations.cnn
from mock import patch, MagicMock
import test_utils.redis
import test_utils.neontest
import tornado.gen
import tornado.httpclient
import tornado.testing
import unittest

class TestSubmitVideo(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestSubmitVideo, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.submit_mocker = patch('integrations.ovp.utils.http.send_request')
        self.submit_mock = self._callback_wrap_mock(self.submit_mocker.start())
        self.submit_mock.side_effect = \
          lambda x, **kwargs: tornado.httpclient.HTTPResponse(
              x, 201, buffer=StringIO('{"job_id": "job1"}'))

        user_id = '134234adfs' 
        self.user = neondata.NeonUserAccount(user_id,name='testingaccount')
        self.user.save()
        self.integration = neondata.CNNIntegration(self.user.neon_api_key,  
                                                   last_process_date='2015-10-29T23:59:59Z', 
                                                   api_key_ref='c2vfn5fb8gubhrmd67x7bmv9')
        self.integration.save()

        self.external_integration = integrations.cnn.CNNIntegration(
            self.user.neon_api_key, self.integration)
        mock_api = MagicMock() 
        self.external_integration.api = lambda: mock_api
   
    def tearDown(self):
        self.redis.stop()
        super(TestSubmitVideo, self).tearDown()

    @tornado.testing.gen_test
    def test_submit_success(self):
        yield self.external_integration.submit_new_videos()
