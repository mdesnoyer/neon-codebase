#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cnn_api
import json
import logging
from mock import patch, MagicMock

import test_utils.neontest
import test_utils.redis
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import tornado.ioloop
import utils.neon

class TestCNNApi(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestCNNApi, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

    def tearDown(self):
        self.redis.stop()
    
    @tornado.testing.gen_test
    def test_base_search(self):
        c = api.cnn_api.CNNApi('c2vfn5fb8gubhrmd67x7bmv9')
        yield c.search('2015-10-29T00:00:00Z') 
        
