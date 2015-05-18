#!/usr/bin/env python
'''
Optimizely Platform Test
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import json
import logging
import unittest
import utils.neon
import test_utils.neontest
import tornado.gen
from mock import patch
from cmsdb import neondata
from test_utils.optimizely_aux import OptimizelyAux
from tornado.httpclient import HTTPRequest, HTTPResponse
from StringIO import StringIO
import test_utils.redis
_log = logging.getLogger(__name__)

# Constants
API_KEY = "key"
A_ID = "0001"
I_ID = "1001"
PROJECT_NAME = "neon-lab"


class TestOptimizelyPlatform(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestOptimizelyPlatform, self).setUp()
        self.o_platform = neondata.OptimizelyPlatform(api_key=API_KEY,
                                                      a_id=A_ID, i_id=I_ID)
        self.optimizely_aux = OptimizelyAux()
        self.project_name = PROJECT_NAME

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

    def tearDown(self):
        self.redis.stop()
        super(TestOptimizelyPlatform, self).tearDown()

    @patch('api.optimizely_api.utils.http.send_request')
    @tornado.testing.gen_test
    def test_get_or_create_project_with_invalid_access_token(self, utils_http):
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely", method='GET'),
            401,
            buffer=StringIO(json.dumps("Authentication failed"))
        )

        response = yield self.o_platform.get_or_create_project()
        self.assertEqual(response['status_code'], 401)

    @patch('api.optimizely_api.utils.http.send_request')
    @tornado.testing.gen_test
    def test_get_or_create_project_existing_neonlabs(self, utils_http):
        self.optimizely_aux.response_project_create(
            project_name="abcd")
        ob2 = self.optimizely_aux.response_project_create(
            project_name=self.project_name)

        project_list = self.optimizely_aux.response_project_list()
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely", method='GET'),
            200,
            buffer=StringIO(json.dumps(project_list))
        )

        response = yield self.o_platform.get_or_create_project()
        self.assertEqual(response['status_code'], 200)
        self.assertEqual(response['data']['id'], ob2['id'])

        oc = neondata.OptimizelyPlatform.get(API_KEY, I_ID)
        self.assertEqual(oc.optimizely_project_id, ob2['id'])

    @patch('api.optimizely_api.utils.http.send_request')
    @tornado.testing.gen_test
    def test_get_or_create_project_creating_neonlabs(self, utils_http):
        self.optimizely_aux.response_project_create(
            project_name="abcd")

        before = list(self.optimizely_aux.response_project_list())

        def _side_effect(request, callback=None, *args, **kwargs):
            if request.method == 'GET':
                return HTTPResponse(
                    HTTPRequest("https://optimizely", method='GET'),
                    200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_aux.response_project_list()
                    ))
                )
            else:
                return HTTPResponse(
                    HTTPRequest("https://optimizely", method='POST'),
                    201,
                    buffer=StringIO(json.dumps(
                        self.optimizely_aux.response_project_create(
                            project_name=self.project_name
                        )
                    ))
                )

        utils_http.side_effect = _side_effect
        response = yield self.o_platform.get_or_create_project()
        project_id = response['data']['id']
        self.assertEqual(response['status_code'], 200)
        self.assertEqual(len(before), 1)
        self.assertEqual(len(self.optimizely_aux.response_project_list()), 2)
        self.assertEqual(
            self.optimizely_aux.response_project_read(project_id)['id'],
            project_id
        )

        oc = neondata.OptimizelyPlatform.get(API_KEY, I_ID)
        self.assertEqual(oc.optimizely_project_id, project_id)

if __name__ == "__main__":
    utils.neon.InitNeon()
    unittest.main()
