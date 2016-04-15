#!/usr/bin/env python

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
from cmsdb import neondata
from cmsdb.neondata import DBConnection, NeonUserAccount
from cmsdb.neondata import ThumbnailMetadata, ThumbnailType, VideoMetadata
from cvutils.imageutils import PILImageUtils
import datetime
from integrations.cnn import CNNIntegration
from integrations.ovp import OVPNoValidURL
import logging
from mock import patch
import random
import string
import test_utils.neontest
import test_utils.postgresql
import test_utils.redis
import time
import tornado.gen
import tornado.httpclient
import tornado.testing
from utils.options import options


class TestParseFeed(test_utils.neontest.TestCase):

    def setUp(self):
        super(TestParseFeed, self).setUp()

    def tearDown(self):
        super(TestParseFeed, self).setUp()

    def test_cdn_urls_one_valid(self):
        cdn_urls = {}
        cdn_urls['1920x1080_5500k_mp4'] = 'http://5500k-url.com'
        url = CNNIntegration._find_best_cdn_url(cdn_urls)
        self.assertEquals(url, 'http://5500k-url.com')

    def test_cdn_urls_multiple_valid(self):
        cdn_urls = {}
        cdn_urls['1920x1080_5500k_mp4'] = 'http://5500k-url.com'
        cdn_urls['1920x500_3000k_mp4'] = 'http://3000k-url.com'
        cdn_urls['720x100_1000k_mp4'] = 'http://100k-url.com'
        url = CNNIntegration._find_best_cdn_url(cdn_urls)
        self.assertEquals(url, 'http://5500k-url.com')

    def test_cdn_urls_no_valid(self):
        cdn_urls = {}
        cdn_urls['1_5500k_mp4'] = 'http://5500k-url.com'
        cdn_urls['2_3000k_mp4'] = 'http://3000k-url.com'
        cdn_urls['3_1000k_mp4'] = 'http://100k-url.com'
        with self.assertRaises(OVPNoValidURL):
            CNNIntegration._find_best_cdn_url(cdn_urls)

    _mock_video_response = {
        'id': 'video_id',
        'relatedMedia': {
            'media': [
                {
                    'type': 'reference',
                    'id': 'refid0'
                }, {
                    'type': 'image',
                    'id': 'thumbid0',
                    'cuts': {
                        'large4to3': {
                            'height': 480,
                            'width': 640,
                            'url': 'http://i2.cdn.com/2152-gfx-cnn-video-synd-large-0.jpg'
                        },
                        'medium4to3': {
                            'height': 300,
                            'width': 400,
                            'url': 'http://i2.cdn.com/2152-gfx-cnn-video-synd-med-0.jpg'
                        },
                        'exlarge16to9': {
                            'height': 619,
                            'width': 1100,
                            'url': 'http://i2.cdn.com/2152-gfx-cnn-video-synd-ex-0.jpg'
                        }
                    }
                }, {
                    'type': 'image',
                    'id': 'thumbid1',
                    'cuts': {
                        'large4to3': {
                            'height': 480,
                            'width': 640,
                            'url': 'http://i2.cdn.com/2152-gfx-cnn-video-synd-large-1.jpg'
                        },
                        'medium4to3': {
                            'height': 300,
                            'width': 400,
                            'url': 'http://i2.cdn.com/2152-gfx-cnn-video-synd-med-1.jpg'
                        },
                        'exlarge16to9': {
                            'height': 619,
                            'width': 1100,
                            'url': 'http://i2.cdn.com/2152-gfx-cnn-video-synd-ex-1.jpg'
                        }
                    }
                }
            ]
        }
    }

    def test_extract_image_field(self):
        ids = CNNIntegration._extract_image_field(
                self._mock_video_response, 'id')
        self.assertIn(u'thumbid0', ids)
        self.assertIn(u'thumbid1', ids)

    def test_extract_image_urls(self):
        url0 = 'http://i2.cdn.com/2152-gfx-cnn-video-synd-ex-0.jpg'
        url1 = 'http://i2.cdn.com/2152-gfx-cnn-video-synd-ex-1.jpg'
        extract = CNNIntegration._extract_image_urls(
                self._mock_video_response)
        self.assertIn(url0, extract)
        self.assertIn(url1, extract)

    def test_normalize_thumbnail_url(self):
        given = ('http://ht.cdn.turner.com/cnn/big/world/2016/03/01/'
                 'child-china-orig-vstan-bpb.cnn_512x288_550k.mp4?param=0&param=1%20')
        want = ('ht.cdn.turner.com/cnn/big/world/2016/03/01/'
                'child-china-orig-vstan-bpb.cnn_512x288_550k.mp4')
        self.assertEqual(
                CNNIntegration._normalize_thumbnail_url(given), want)

    def test_get_video_title_with_both_title_and_headline(self):
        '''Video title is just title when it is present'''
        submit = {
            'id': 'h_4f9ca8a64c2911905bd2196b8a246253',
            'title': 'Sanders, Clinton spar over Wall Street ties',
            'headline': 'Sanders, Clinton spar'
        }
        self.assertEqual(
                CNNIntegration.get_video_title(submit),
                'Sanders, Clinton spar over Wall Street ties',
                'prefer title over headline')

    def test_get_video_title_with_just_title(self):
        '''Video title is title in submission'''
        submit = {
            'id': 'h_4f9ca8a64c2911905bd2196b8a246254',
            'title': 'Sanders and Clinton team up to fight the Nazi zombie horde',
            'headline': None
        }
        self.assertEqual(
                CNNIntegration.get_video_title(submit),
                'Sanders and Clinton team up to fight the Nazi zombie horde',
                'use a title if given one')

    def test_get_video_title_just_headline(self):
        '''Video title falls back to headline if no title'''
        submit = {
            'id': 'h_4f9ca8a64c2911905bd2196b8a246255',
            'title': None,
            'headline': 'Clinton and Sanders battle over auto industry bailout'
        }
        self.assertEqual(
                CNNIntegration.get_video_title(submit),
                'Clinton and Sanders battle over auto industry bailout')


class TestSubmitVideo(test_utils.neontest.AsyncTestCase):

    def setUp(self):
        super(TestSubmitVideo, self).setUp()
        self.submit_mocker = patch(
            'integrations.ovp.OVPIntegration.submit_video')
        self.submit_mock = self._future_wrap_mock(self.submit_mocker.start())

        user_id = '134234adfs'
        self.user = NeonUserAccount(user_id, name='testingaccount')
        self.user.save()
        self.integration = neondata.CNNIntegration(
            self.user.neon_api_key,
            last_process_date='2015-10-29T23:59:59Z',
            api_key_ref='c2vfn5fb8gubhrmd67x7bmv9')
        self.integration.save()

        self.external_integration = CNNIntegration(
            self.user.neon_api_key, self.integration)
        self.cnn_api_mocker = patch('api.cnn_api.CNNApi.search')
        self.cnn_api_mock = self._future_wrap_mock(self.cnn_api_mocker.start())

    def tearDown(self):
        self.submit_mocker.stop()
        self.cnn_api_mocker.stop()
        conn = DBConnection.get(VideoMetadata)
        conn.clear_db()
        conn = DBConnection.get(ThumbnailMetadata)
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
        self.cnn_api_mock.side_effect = [response]
        self.submit_mock.side_effect = [{'job_id': 'job1'},
                                        {'job_id': 'job2'}]
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
        self.submit_mock.side_effect = [{'job_id': 'job1'},
                                        Exception('on noes not again')]
        with self.assertLogExists(logging.INFO, 'Added or found 1 jobs'):
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
        self.submit_mock.side_effect = [{'job_id': 'job1'},
                                        {'job_id': 'job2'}]
        yield self.external_integration.submit_new_videos()
        integration = neondata.CNNIntegration.get(
            self.integration.integration_id)
        videos = response['docs']
        video_two = videos[1]
        self.assertEquals(
            video_two['firstPublishDate'],
            integration.last_process_date)

    @tornado.testing.gen_test
    def test_video_has_title(self):
        '''CNN video's NeonApiRequest has a title field after submission.'''

        # Make a mock video with no title but a headline
        response = self.create_search_response(1)
        response['docs'][0]['title'] = None
        expect_title = response['docs'][0]['headline'] = 'Shocking headline'
        self.cnn_api_mock.side_effect = [response]

        self.submit_mock.side_effect = [{'job_id': 'job1'}]
        yield self.external_integration.submit_new_videos()

        cargs_list = self.submit_mock.call_args_list
        call_one = cargs_list[0][1]
        self.assertEquals(self.submit_mock.call_count, 1)
        self.assertEquals(expect_title, call_one['video_title'])

    # TODO move this to a mock class
    def create_search_response(self, num_of_results=random.randint(5, 10)):
        def _string_generator():
            length = random.randint(10, 25)
            return ''.join([random.choice(string.ascii_letters + string.digits)
                            for _ in range(length)])

        def _generate_docs():
            def _generate_topics():
                topics = []
                length = random.randint(0, 10)
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
                length = random.randint(1, 5)
                related_media = {}
                related_media['hasImage'] = True
                related_media['media'] = []
                for i in range(length):
                    media = {}
                    media['id'] = _string_generator()
                    media['imageId'] = _string_generator()
                    media['type'] = 'image'
                    media['cuts'] = {}
                    media['cuts']['exlarge16to9'] = {
                        'url': 'http://test_url.com/image%d' % i}
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
                doc['firstPublishDate'] = publish_time.strftime(
                    '%Y-%m-%dT%H:%M:%S')
                doc['lastPublishDate'] = publish_time.strftime(
                    '%Y-%m-%dT%H:%M:%S')
                doc['topics'] = _generate_topics()
                doc['relatedMedia'] = _generate_related_media()
                doc['cdnUrls'] = _generate_cdn_urls()
                docs.append(doc)
            return docs

        response = {}
        response['status'] = 200
        response['generated'] = '2015-11-16T21:36:25.369Z'
        response['results'] = num_of_results
        response['docs'] = _generate_docs()
        return response


class TestSubmitVideoPG(TestSubmitVideo):

    def setUp(self):
        super(test_utils.neontest.AsyncTestCase, self).setUp()
        self.submit_mocker = patch(
            'integrations.ovp.OVPIntegration.submit_video')
        self.submit_mock = self._future_wrap_mock(self.submit_mocker.start())

        user_id = '134234adfs'
        self.user = NeonUserAccount(user_id, name='testingaccount')
        self.user.save()
        self.integration = neondata.CNNIntegration(
            self.user.neon_api_key,
            last_process_date='2015-10-29T23:59:59Z',
            api_key_ref='c2vfn5fb8gubhrmd67x7bmv9')
        self.integration.save()

        self.external_integration = CNNIntegration(
            self.user.neon_api_key, self.integration)
        self.cnn_api_mocker = patch('api.cnn_api.CNNApi.search')
        self.cnn_api_mock = self._future_wrap_mock(self.cnn_api_mocker.start())

        # Set up one previously uploaded video
        search_result = self.create_search_response(1)['docs'][0]
        ext_video_id = search_result['videoId']
        ext_thumb_ids = CNNIntegration._extract_image_field(search_result, 'id')
        int_video_id = neondata.InternalVideoID.generate(
                self.external_integration.neon_api_key, ext_video_id)
        int_thumb_ids = ['%s_%s' % (int_video_id, ext_thumb_id) for
                         ext_thumb_id in ext_thumb_ids]
        video_meta = VideoMetadata(int_video_id, tids=int_thumb_ids)
        video_meta.save()
        thumbs_meta = []
        i = 0
        for int_thumb_id in int_thumb_ids:
            xl_url = search_result['relatedMedia']['media'][i][
                    'cuts']['exlarge16to9']['url']
            thumb_meta = ThumbnailMetadata(
                    int_thumb_id, int_video_id, [xl_url],
                    ttype=ThumbnailType.DEFAULT, rank=i)
            thumb_meta.save()
            thumbs_meta.append(thumb_meta)
            i += 1

        self.previous_video = {
            'search_result': search_result,
            'ext_video_id': ext_video_id,
            'ext_thumb_ids': ext_thumb_ids,
            'int_video_id': int_video_id,
            'int_thumb_ids': int_thumb_ids,
            'video_meta': video_meta,
            'thumbs_meta': thumbs_meta
        }

        # Mock out the image download
        self.im_download_mocker = patch(
                'cvutils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [self.random_image]
        # Mock out the image upload
        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)

    def tearDown(self):
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
        self.cnn_api_mocker.stop()
        self.submit_mocker.stop()
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

    @tornado.testing.gen_test
    def test_when_default_thumb_changes(self):
        '''Default thumbnail changes on submit

           When a CNN video is processed and a new image
           is included, the new image is made the default
           thumbnail.
        '''
        prev_thumbs_count = len(self.previous_video['video_meta'].thumbnail_ids)

        # Replace the media in the search result to trigger change
        video_data = self.previous_video['search_result']
        new_id = 'new_id'
        new_url = 'http://cdn.cnn.com/thumbnail_new.jpg'
        # Prepend new media including one thumbnail
        media = [
            {'id': 'ref123', 'type': 'reference', 'cuts': {}},
            {'id': new_id, 'type': 'image', 'cuts': {'exlarge16to9': {'url': new_url}}}
        ]
        video_data['relatedMedia'] = {'media': media}

        # Submit the video with new thumbnail
        yield self.external_integration.submit_one_video_object(video_data)
        video = VideoMetadata.get(self.previous_video['int_video_id'])
        self.assertEqual(len(video.thumbnail_ids), 1 + prev_thumbs_count)
        thumbs = ThumbnailMetadata.get_many(video.thumbnail_ids)
        self.assertEqual(self.im_download_mock.call_count, 1)

        # For the thumbnail not in the previous video submission,
        # assert properties are correct
        previous_thumbs = self.previous_video['video_meta'].thumbnail_ids
        for thumb in thumbs:
            if thumb.key not in previous_thumbs:
                self.assertEqual(thumb.rank, -1)
                self.assertEqual(thumb.type, ThumbnailType.DEFAULT)
                self.assertEqual(thumb.urls, [new_url])
                self.assertEqual(thumb.external_id, new_id)

    @tornado.testing.gen_test
    def test_when_default_thumb_has_no_change(self):
        '''Default thumbnail remains on submit

           When a CNN is processed and the image is recognized,
           the original default thumbnail remains.
        '''
        prev_thumbs_count = len(self.previous_video['video_meta'].thumbnail_ids)
        video_data = self.previous_video['search_result']
        yield self.external_integration.submit_one_video_object(video_data)

        # Nothing changes so counts are the same
        video = VideoMetadata.get(self.previous_video['int_video_id'])
        self.assertEqual(len(video.thumbnail_ids), prev_thumbs_count)
        thumbs = ThumbnailMetadata.get_many(video.thumbnail_ids)
        self.assertEqual(self.im_download_mock.call_count, 0)

        previous_thumbs = self.previous_video['video_meta'].thumbnail_ids
        for thumb in thumbs:
            self.assertIn(thumb.key, previous_thumbs)
