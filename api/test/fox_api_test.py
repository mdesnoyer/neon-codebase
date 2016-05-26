#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.fox_api
from dateutil.parser import parse
import json
import logging
from mock import patch, MagicMock

import test_utils.neontest
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
import tornado.ioloop
import utils.neon

class TestFoxApi(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestFoxApi, self).setUp()
        self.fox_api_mocker = patch('api.fox_api.FoxApi.search')
        self.fox_api_mock = self._future_wrap_mock(self.fox_api_mocker.start()) 

    def tearDown(self):
        self.fox_api_mocker.stop()
        super(TestFoxApi, self).tearDown()
 
    @tornado.testing.gen_test
    def test_base_search(self):
        c = api.fox_api.FoxApi('c2vfn5fb8gubhrmd67x7bmv9')
        self.fox_api_mock.side_effect = ['{ somejson: json }']
        yield c.search(parse('2015-10-29T00:00:00Z'))
        cargs_list = self.fox_api_mock.call_args_list
        self.assertEquals(cargs_list[0][0][0].year, 2015) 
        self.assertEquals(cargs_list[0][0][0].month, 10) 
        self.assertEquals(cargs_list[0][0][0].day, 29) 
