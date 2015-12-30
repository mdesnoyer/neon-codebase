#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cnn_api
from cmsdb import neondata
from cStringIO import StringIO
import datetime 
import integrations.cnn
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

class TestParseFeed(test_utils.neontest.TestCase): 
    def setUp(self):
        super(TestParseFeed, self).setUp()
    def tearDown(self):
        super(TestParseFeed, self).setUp()

    def test_cdn_urls_one_valid(self):
        cdn_urls = {} 
        cdn_urls['1920x1080_5500k_mp4'] = 'http://5500k-url.com'
        url = integrations.cnn.CNNIntegration._find_best_cdn_url(cdn_urls)
        self.assertEquals(url, 'http://5500k-url.com')
   
    def test_cdn_urls_multiple_valid(self):
        cdn_urls = {} 
        cdn_urls['1920x1080_5500k_mp4'] = 'http://5500k-url.com'
        cdn_urls['1920x500_3000k_mp4'] = 'http://3000k-url.com'
        cdn_urls['720x100_1000k_mp4'] = 'http://100k-url.com'
        url = integrations.cnn.CNNIntegration._find_best_cdn_url(cdn_urls)
        self.assertEquals(url, 'http://5500k-url.com')
   
    def test_cdn_urls_no_valid(self):
        cdn_urls = {} 
        cdn_urls['1_5500k_mp4'] = 'http://5500k-url.com'
        cdn_urls['2_3000k_mp4'] = 'http://3000k-url.com'
        cdn_urls['3_1000k_mp4'] = 'http://100k-url.com'
        with self.assertRaises(Exception): 
            integrations.cnn.CNNIntegration._find_best_cdn_url(cdn_urls)

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
        self.integration = neondata.CNNIntegration(self.user.neon_api_key,  
                                                   last_process_date='2015-10-29T23:59:59Z', 
                                                   api_key_ref='c2vfn5fb8gubhrmd67x7bmv9')
        self.integration.save()

        self.external_integration = integrations.cnn.CNNIntegration(
            self.user.neon_api_key, self.integration)
        self.cnn_api_mocker = patch('api.cnn_api.CNNApi.search')
        self.cnn_api_mock = self._future_wrap_mock(self.cnn_api_mocker.start()) 
         
    def tearDown(self):
        self.redis.stop()
        self.submit_mocker.stop() 
        self.cnn_api_mocker.stop() 
        super(TestSubmitVideo, self).tearDown()

    @tornado.testing.gen_test
    def test_submit_success(self):
        response = self.create_search_response(2)
        self.cnn_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        {"job_id": "job2"}] 
        yield self.external_integration.submit_new_videos()
        cargs_list = self.submit_mock.call_args_list
        videos = response['docs'] 
        video_one = videos[0]  
        video_two = videos[1]
        call_one = cargs_list[0][1] 
        call_two = cargs_list[1][1]
 
        self.assertEquals(self.submit_mock.call_count, 2)
        self.assertEquals(video_one['videoId'], call_one['video_id'])  
        self.assertEquals(video_two['videoId'], call_two['video_id']) 
 
    @tornado.testing.gen_test
    def test_submit_one_failure(self):
        response = self.create_search_response(2)
        self.cnn_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        Exception('on noes not again')]
        with self.assertLogExists(logging.INFO, 'Added 1 jobs'): 
            yield self.external_integration.submit_new_videos()
        self.assertEquals(self.submit_mock.call_count, 2)
  
    @tornado.testing.gen_test
    def test_last_processed_date(self):
        ''' the way we query for the data, should sort_by 
              publish_date asc, meaning the most recent date 
              would be the last video we process
            assert that integration.last_process_date is equal
              to firstPublishDate of the last video we see
        ''' 
        response = self.create_search_response(2)
        self.cnn_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{"job_id": "job1"}, 
                                        {"job_id": "job2"}] 
        yield self.external_integration.submit_new_videos()
        integration = neondata.CNNIntegration.get(self.integration.integration_id) 
        videos = response['docs'] 
        video_one = videos[1]
        video_two = videos[1]
        self.assertEquals(video_two['firstPublishDate'], integration.last_process_date) 

    #TODO move this to a mock class 
    def create_search_response(self, num_of_results=random.randint(5,10)): 
        def _string_generator(): 
            length=random.randint(10,25)  
            return ''.join([random.choice(string.ascii_letters+string.digits) 
                      for _ in range(length)]) 
 
        def _generate_docs():
            def _generate_topics():
                topics = [] 
                length=random.randint(0,10)  
                for i in range(length): 
                    topic = {} 
                    topic['label'] = _string_generator() 
                    topic['class'] = 'Subject' 
                    topic['id'] = _string_generator() 
                    topic['topicId'] = '234' 
                    topic['confidenceScore'] = 0.23
                    topics.append(topic) 
                return topics

            def _generate_related_media():  
                length=random.randint(1,5)  
                related_media = {} 
                related_media['hasImage'] = True
                related_media['media'] = []
                for i in range(length): 
                    media = {} 
                    media['id'] = _string_generator() 
                    media['imageId'] = _string_generator()
                    media['type'] = 'image'  
                    media['cuts'] = {} 
                    media['cuts']['exlarge16to9'] = { 'url' : 'http://test_url.com' }
                    related_media['media'].append(media) 
                return related_media 
            
            def _generate_cdn_urls(): 
                cdn_urls = {} 
                cdn_urls['1920x1080_5500k_mp4'] = 'http://5500k-url.com'
                return cdn_urls  
                
            publish_time = datetime.datetime.fromtimestamp(time.time())  
            docs = [] 
            for i in range(num_of_results): 
                publish_time = publish_time + datetime.timedelta(minutes=30) 
                doc = {} 
                doc['id'] = _string_generator()
                doc['videoId'] = _string_generator()
                doc['title'] = _string_generator()
                doc['duration'] = publish_time.strftime('%H:%M:%S')
                doc['firstPublishDate'] = publish_time.strftime('%Y-%m-%dT%H:%M:%S') 
                doc['lastPublishDate'] = publish_time.strftime('%Y-%m-%dT%H:%M:%S')
                doc['topics'] = _generate_topics()
                doc['relatedMedia'] = _generate_related_media()
                doc['cdnUrls'] = _generate_cdn_urls() 
                docs.append(doc) 
            return docs  
            
        response = {}
        response['status'] = 200 
        response['generated'] = "2015-11-16T21:36:25.369Z" 
        response['results'] = num_of_results
        response['docs'] = _generate_docs()
        return response
