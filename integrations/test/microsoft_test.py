#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.microsoft_api
import calendar
from cmsdb import neondata
from cStringIO import StringIO
import datetime
import dateutil.parser 
import integrations.microsoft
import json
import logging  
from mock import patch, MagicMock
import random 
import string 
import test_utils.redis
import test_utils.neontest
import time
import tornado.gen
import tornado.httpclient
import tornado.testing
import unittest

class TestSubmitVideo(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestSubmitVideo, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.submit_mocker = patch('integrations.ovp.OVPIntegration.submit_video')
        self.submit_mock = self._future_wrap_mock(self.submit_mocker.start())

        user_id = '134234adfs' 
        self.user = neondata.NeonUserAccount(user_id,name='testingaccount')
        self.user.save()
        self.integration = neondata.MicrosoftIntegration(self.user.neon_api_key,  
                                                   last_process_date=1451942786, 
                                                   feed_id='meisafeedpidref')
        self.integration.save()

        self.external_integration = integrations.microsoft.MicrosoftIntegration(
            self.user.neon_api_key, self.integration)
        self.microsoft_api_mocker = patch('api.microsoft_api.MicrosoftApi.search')
        self.microsoft_api_mock = self._future_wrap_mock(self.microsoft_api_mocker.start()) 
         
    def tearDown(self):
        self.redis.stop()
        self.submit_mocker.stop() 
        self.microsoft_api_mocker.stop() 
        super(TestSubmitVideo, self).tearDown()

    @tornado.testing.gen_test
    def test_submit_success(self):
        response = self.create_search_response(2)
        self.microsoft_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        {"job_id": "job2"}] 
        yield self.external_integration.submit_new_videos()
        cargs_list = self.submit_mock.call_args_list
        videos = response 
        video_one = videos[0]  
        video_two = videos[1]
        call_one = cargs_list[0][1] 
        call_two = cargs_list[1][1]
 
        self.assertEquals(self.submit_mock.call_count, 2)
        self.assertEquals(video_one['_id'], call_one['video_id'])  
        self.assertEquals(video_two['_id'], call_two['video_id']) 
 
    @tornado.testing.gen_test
    def test_submit_one_failure(self):
        response = self.create_search_response(2)
        self.microsoft_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        Exception('on noes not again')]
        with self.assertLogExists(logging.INFO, 'Added or found 1 jobs'): 
            yield self.external_integration.submit_new_videos()
        self.assertEquals(self.submit_mock.call_count, 2)
  
    @tornado.testing.gen_test(timeout=100)
    def test_last_processed_date(self):
        ''' the way we query for the data, should sort_by 
              publish_date asc, meaning the most recent date 
              would be the last video we process
            assert that integration.last_process_date is equal
              to firstPublishDate of the last video we see
        ''' 
        response = self.create_search_response(2)
        self.microsoft_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        {"job_id": "job2"}] 
        yield self.external_integration.submit_new_videos()
        integration = neondata.AbstractIntegration.get(self.integration.integration_id)
        videos = response 
        video_one = videos[0]
        video_two = videos[1]
        dt = dateutil.parser.parse(video_two['_lastEditedDateTime'])
        ts = calendar.timegm(dt.timetuple())
        self.assertEquals(ts, integration.last_process_date) 

    def create_search_response(self, num_of_results=random.randint(5,10)): 
        def _string_generator(): 
            length=random.randint(10,25)  
            return ''.join([random.choice(string.ascii_letters+string.digits) 
                      for _ in range(length)]) 
 
        def _generate_docs():
            def _generate_facets():
                facets = []

            def _generate_video_files(): 
                videoFiles = [] 
                file_one = {} 
                file_one['format'] = "1001" 
                file_one['sourceHref'] = _string_generator()
                videoFiles.append(file_one) 
                return videoFiles 
                
            docs = [] 
            updated_time = '2016-02-16T03:36:05Z'
            for i in range(num_of_results): 
                doc = {} 
                doc['_id'] = _string_generator()
                doc['title'] = _string_generator()
                doc['_lastEditedDateTime'] = updated_time
                doc['_lastPublishedDateTime'] = updated_time
                doc['thumbnail'] = {} 
                doc['thumbnail']['address'] = _string_generator()
                doc['thumbnail']['id'] = _string_generator()
                doc['playTime'] = random.randint(100,500)
                doc['videoFiles'] = _generate_video_files()
                docs.append(doc)
            return docs  
            
        response = {}
        #response['entryCount'] = num_of_results
        response = _generate_docs()
        return response
