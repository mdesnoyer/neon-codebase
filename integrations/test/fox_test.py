#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.fox_api
from cmsdb import neondata
from cStringIO import StringIO
import datetime 
import integrations.fox
import json
import logging  
from mock import patch, MagicMock
import random 
import string 
import test_utils.redis
import test_utils.neontest
import test_utils.postgresql
import time
import tornado.gen
import tornado.httpclient
import tornado.testing
from utils.options import define, options
import unittest

class TestSubmitVideo(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestSubmitVideo, self).setUp()
        self.submit_mocker = patch('integrations.ovp.OVPIntegration.submit_video')
        self.submit_mock = self._future_wrap_mock(self.submit_mocker.start())

        user_id = '134234adfs' 
        self.user = neondata.NeonUserAccount(user_id,name='testingaccount')
        self.user.save()
        self.integration = neondata.FoxIntegration(self.user.neon_api_key,  
                                                   last_process_date=1451942786, 
                                                   feed_pid_ref='meisafeedpidref')
        self.integration.save()

        self.external_integration = integrations.fox.FoxIntegration(
            self.user.neon_api_key, self.integration)
        self.fox_api_mocker = patch('api.fox_api.FoxApi.search')
        self.fox_api_mock = self._future_wrap_mock(self.fox_api_mocker.start()) 
         
    def tearDown(self):
        self.submit_mocker.stop() 
        self.fox_api_mocker.stop() 
        conn = neondata.DBConnection.get(neondata.VideoMetadata)
        conn.clear_db() 
        conn = neondata.DBConnection.get(neondata.ThumbnailMetadata)
        conn.clear_db()
        super(TestSubmitVideo, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.redis = test_utils.redis.RedisServer()
        cls.redis.start()

    @classmethod
    def tearDownClass(cls): 
        cls.redis.stop()

    @tornado.testing.gen_test
    def test_submit_success(self):
        response = self.create_search_response(2)
        self.fox_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        {"job_id": "job2"}] 
        yield self.external_integration.submit_new_videos()
        cargs_list = self.submit_mock.call_args_list
        videos = response['entries'] 
        video_one = videos[0]  
        video_two = videos[1]
        call_one = cargs_list[0][1] 
        call_two = cargs_list[1][1]
 
        self.assertEquals(self.submit_mock.call_count, 2)
        self.assertEquals(video_one['id'], call_one['video_id'])  
        self.assertEquals(video_two['id'], call_two['video_id']) 
 
    @tornado.testing.gen_test
    def test_submit_one_failure(self):
        response = self.create_search_response(2)
        self.fox_api_mock.side_effect = [response]
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
        self.fox_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        {"job_id": "job2"}] 
        yield self.external_integration.submit_new_videos()
        integration = neondata.AbstractIntegration.get(self.integration.integration_id) 
        videos = response['entries'] 
        video_one = videos[0]
        video_two = videos[1]
        self.assertEquals(video_two['pubDate']/1000, integration.last_process_date) 

    def create_search_response(self, num_of_results=random.randint(5,10)): 
        def _string_generator(): 
            length=random.randint(10,25)  
            return ''.join([random.choice(string.ascii_letters+string.digits) 
                      for _ in range(length)]) 
 
        def _generate_docs():
            def _generate_media_content(): 
                media_content = [] 
                media_content.append({ 'plfile$duration' : 1234.56, 
                                     'plfile$streamingUrl' : 'http://blah.mp4' 
                                   })
                return media_content 
                
            docs = [] 
            updated_time = 1451942787000
            for i in range(num_of_results): 
                doc = {} 
                doc['id'] = _string_generator()
                doc['updated'] = updated_time
                doc['title'] = _string_generator()
                doc['pubDate'] = updated_time
                doc['fox$contentType'] = _string_generator()
                doc['fox$showcode'] = _string_generator()
                doc['plmedia$defaultThumbnailUrl'] = _string_generator()
                doc['media$content'] = _generate_media_content()
                docs.append(doc)
            return docs  
            
        response = {}
        response['entryCount'] = num_of_results
        response['entries'] = _generate_docs()
        return response

class TestSubmitVideoPG(TestSubmitVideo):
    def setUp(self):
        super(test_utils.neontest.AsyncTestCase, self).setUp()
        self.submit_mocker = patch('integrations.ovp.OVPIntegration.submit_video')
        self.submit_mock = self._future_wrap_mock(self.submit_mocker.start())

        user_id = '134234adfs' 
        self.user = neondata.NeonUserAccount(user_id,name='testingaccount')
        self.user.save()
        self.integration = neondata.FoxIntegration(self.user.neon_api_key,  
                                                   last_process_date=1451942786, 
                                                   feed_pid_ref='meisafeedpidref')
        self.integration.save()

        self.external_integration = integrations.fox.FoxIntegration(
            self.user.neon_api_key, self.integration)
        self.fox_api_mocker = patch('api.fox_api.FoxApi.search')
        self.fox_api_mock = self._future_wrap_mock(self.fox_api_mocker.start()) 
         
    def tearDown(self):
        self.submit_mocker.stop() 
        self.fox_api_mocker.stop()
        self.postgresql.clear_all_tables() 
        super(test_utils.neontest.AsyncTestCase, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()
