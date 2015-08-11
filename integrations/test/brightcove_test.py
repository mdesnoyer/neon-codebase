#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
from cmsdb import neondata
from cmsdb.neondata import ThumbnailMetadata, ThumbnailType, VideoMetadata
from cStringIO import StringIO
import integrations.brightcove
import json
import logging
from mock import patch, MagicMock
import multiprocessing
import test_utils.redis
import test_utils.neontest
import tornado.gen
import tornado.httpclient
import tornado.testing
import unittest
from utils.imageutils import PILImageUtils
from utils.options import define, options
import utils.neon

class TestGrabNewThumb(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        # Mock out the image download
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [self.random_image]

        # Mock out the image upload
        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]

        # Mock out the brightcove api and build the platform
        mock_bc_api = MagicMock()
        self.platform = neondata.BrightcovePlatform('acct1', 'i1')
        self.platform.get_api = lambda: mock_bc_api
        self.integration = integrations.brightcove.BrightcoveIntegration(
            'a1', self.platform)
        
        # Add a video to the database
        vid = VideoMetadata('acct1_v1',
                            ['acct1_v1_n1', 'acct1_v1_bc1'],
                            i_id='i1')
        vid.save()
        self.platform.add_video('v1', None)
        ThumbnailMetadata('acct1_v1_n1', 'acct1_v1',
                          ttype=ThumbnailType.NEON, rank=1).save()

        super(TestGrabNewThumb, self).setUp()

    def tearDown(self):
        self.im_download_mocker.stop()
        self.cdn_mocker.stop()
        self.redis.stop()

        super(TestGrabNewThumb, self).tearDown()

    @tornado.testing.gen_test
    def test_match_urls(self):
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/vid_still.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1).save()

        yield self.integration.submit_one_video_object(
            { 'id' : 'v1',
              'length' : 100,
              'videoStillURL' : 'http://bc.com/vid_still.jpg?x=5',
              'videoStill' : {
                  'id' : 'still_id',
                  'referenceId' : None,
                  'remoteUrl' : None
              },
              'thumbnailURL' : 'http://bc.com/thumb_still.jpg?x=8',
              'thumbnail' : {
                  'id' : 123456,
                  'referenceId' : None,
                  'remoteUrl' : None
                  }
                  }
            )

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          'still_id')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_moved_bc_urls(self):
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bcsecure01-a.akamaihd.net/4/vid_still.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1).save()

        yield self.integration.submit_one_video_object({
            'id' : 'v1',
            'length' : 100,
            'videoStillURL' : 'http://brightcove.com/3/vid_still.jpg?x=5',
            'videoStill' : {
                'id' : 'still_id',
                'referenceId' : None,
                'remoteUrl' : None
            },
            'thumbnailURL' : 'http://brightcove.com/3/thumb_still.jpg?x=8',
            'thumbnail' : {
                'id' : 123456,
                'referenceId' : None,
                'remoteUrl' : None
                }
                })
        

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          'still_id')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_remote_urls(self):
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://some_remote_still?c=90'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1).save()
        yield self.integration.submit_one_video_object({
            'id' : 'v1',
            'length' : 100,
            'videoStillURL' : 'http://bc.com/vid_still.jpg?x=5',
            'videoStill' : {
                'id' : 'still_id',
                'referenceId' : None,
                'remoteUrl' : 'http://some_remote_still'
                },
            'thumbnailURL' : 'http://bc.com/thumb_still.jpg?x=8',
            'thumbnail' : {
                'id' : 'thumb_id',
                'referenceId' : None,
                'remoteUrl' : None
                }
            }
        )

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          'still_id')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_reference_id(self):
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_moved_location'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref').save()
        yield self.integration.submit_one_video_object({
            'id' : 'v1',
            'length' : 100,
            'videoStillURL' : 'http://bc.com/vid_still.jpg?x=5',
            'videoStill' : {
                'id' : 'still_id',
                'referenceId' : 'my_still_ref',
                'remoteUrl' : None
            },
            'thumbnailURL' : 'http://bc.com/thumb_still.jpg?x=8',
            'thumbnail' : {
                'id' : 'thumb_id',
                'referenceId' : 'my_thumb_ref',
                'remoteUrl' : None
            }
            }
        )

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          'still_id')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_external_id(self):
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_moved_location'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref',
                          external_id='thumb_id').save()
        
        yield self.integration.submit_one_video_object({
            'id' : 'v1',
            'length' : 100,
            'videoStillURL' : 'http://bc.com/vid_still.jpg?x=5',
            'videoStill' : {
                'id' : 'still_id',
                'referenceId' : None,
                'remoteUrl' : None
                },
            'thumbnailURL' : 'http://bc.com/thumb_still.jpg?x=8',
            'thumbnail' : {
                'id' : 'thumb_id',
                'referenceId' : None,
                'remoteUrl' : None
                }
                }
            )

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_new_thumb_found(self):
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_old_thumb.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref',
                          external_id='old_thumb_id').save()
        yield self.integration.submit_one_video_object({
            'id' : 'v1',
            'length': 100,
            'videoStillURL' : 'http://bc.com/new_still.jpg?x=8',
            'videoStill' : {
                'id' : 'still_id',
                'referenceId' : None,
                'remoteUrl' : None
            },
            'thumbnailURL' : 'http://bc.com/new_thumb.jpg?x=8',
            'thumbnail' : {
                'id' : 'thumb_id',
                'referenceId' : None,
                'remoteUrl' : None
            }
            })


        # Make sure a new image was added to the database
        video_meta = VideoMetadata.get('acct1_v1')
        self.assertEquals(len(video_meta.thumbnail_ids), 3)
        thumbs = ThumbnailMetadata.get_many(video_meta.thumbnail_ids)
        for thumb in thumbs:
            if thumb.key not in ['acct1_v1_bc1', 'acct1_v1_n1']:
                self.assertEquals(thumb.rank, 0)
                self.assertEquals(thumb.type, ThumbnailType.BRIGHTCOVE)
                self.assertEquals(thumb.urls, [
                    'some_cdn_url.jpg',
                    'http://bc.com/new_still.jpg?x=8'])
                self.assertEquals(thumb.external_id, 'still_id')

        # Make sure the new image was uploaded
        self.im_download_mock.assert_called_with(
            'http://bc.com/new_still.jpg?x=8', async=True)
        self.assertGreater(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_error_downloading_image(self):
        self.im_download_mock.side_effect = [IOError('Image Download Error')]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_old_thumb.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref',
                          external_id='old_thumb_id').save()
        
        with self.assertLogExists(logging.ERROR, 'Could not find valid image'):
            yield self.integration.submit_one_video_object({
                'id' : 'v1',
                'length' : 100,
                'videoStillURL' : None,
                'videoStill' : None,
                'thumbnailURL' : 'http://bc.com/new_thumb.jpg?x=8',
                'thumbnail' : {
                    'id' : 'thumb_id',
                    'referenceId' : None,
                    'remoteUrl' : None
                }
                }
                )

        # Make sure there was no change to the database
        video_meta = VideoMetadata.get('acct1_v1')
        self.assertEquals(len(video_meta.thumbnail_ids), 2)

        self.im_download_mock.assert_called_with(
            'http://bc.com/new_thumb.jpg?x=8', async=True)
        self.assertEquals(self.cdn_mock.call_count, 0)

class TestSubmitVideo(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        # Mock out the call to services
        self.submit_mocker = patch('integrations.ovp.utils.http.send_request')
        self.submit_mock = self._callback_wrap_mock(self.submit_mocker.start())
        self.submit_mock.side_effect = \
          lambda x: tornado.httpclient.HTTPResponse(
              x, 201, buffer=StringIO('{"job_id": "job1"}'))
        

        # Create the platform object
        self.platform = neondata.BrightcovePlatform('acct1', 'i1')
        self.integration = integrations.brightcove.BrightcoveIntegration(
            'a1', self.platform)

        super(TestSubmitVideo, self).setUp()

    def tearDown(self):
        self.submit_mocker.stop()
        self.redis.stop()

        super(TestSubmitVideo, self).tearDown()

    def _get_video_submission(self):
        '''Returns, the url, parsed json submition'''
        cargs, kwargs = self.submit_mock.call_args

        response = cargs[0]
        return response.url, json.loads(response.body)

    @tornado.testing.gen_test
    def test_submit_typical_bc_video(self):
        job_id = yield self.integration.submit_one_video_object(
            { 'id' : 'v1',
              'referenceId': None,
              'name' : 'Some video',
              'length' : 100,
              'videoStillURL' : 'http://bc.com/vid_still.jpg?x=5',
              'videoStill' : {
                  'id' : 'still_id',
                  'referenceId' : None,
                  'remoteUrl' : None
              },
              'thumbnailURL' : 'http://bc.com/thumb_still.jpg?x=8',
              'thumbnail' : {
                  'id' : 123456,
                  'referenceId' : None,
                  'remoteUrl' : None
              },
              'FLVURL' : 'http://video.mp4'
            })

        self.assertIsNotNone(job_id)
        
        url, submission = self._get_video_submission()
        self.assertEquals(
            url, ('http://services.neon-lab.com:80/api/v1/accounts/a1/'
                  'neon_integrations/i1/create_thumbnail_api_request'))
        self.maxDiff = None
        self.assertDictEqual(
            submission,
            {'video_id': 'v1',
             'video_url': 'http://video.mp4',
             'video_title': 'Some video',
             'callback_url': None,
             'default_thumbnail': 'http://bc.com/vid_still.jpg?x=5',
             'external_thumbnail_id': 'still_id',
             'custom_data': { '_bc_int_data' :
                              { 'bc_id' : 'v1', 'bc_refid': None }},
             'duration' : 0.1
             })
        
            
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
