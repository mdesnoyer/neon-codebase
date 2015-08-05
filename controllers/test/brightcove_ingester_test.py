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
import concurrent.futures
import controllers.brightcove_ingester
from cStringIO import StringIO
import json
import logging
from mock import patch, MagicMock
import multiprocessing
import test_utils.redis
import test_utils.neontest
import tornado.gen
import tornado.testing
import unittest
from utils.imageutils import PILImageUtils
from utils.options import define, options
import utils.neon

class TestProcessOneAccount(test_utils.neontest.AsyncTestCase):
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
        self.mock_bc_response = self._future_wrap_mock(
            mock_bc_api.find_videos_by_ids)
        
        # Add a video to the database
        vid = VideoMetadata('acct1_v1',
                                     ['acct1_v1_n1', 'acct1_v1_bc1'],
                                     i_id='i1')
        vid.save()
        self.platform.add_video(vid.key, None)
        ThumbnailMetadata('acct1_v1_n1', 'acct1_v1',
                          ttype=ThumbnailType.NEON, rank=1).save()

        super(TestProcessOneAccount, self).setUp()

    def tearDown(self):
        self.im_download_mocker.stop()
        self.cdn_mocker.stop()
        self.redis.stop()

        super(TestProcessOneAccount, self).tearDown()

    @tornado.testing.gen_test
    def test_match_urls(self):
        self.mock_bc_response.side_effect = [[ 
            { 'id' : 'v1',
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
            ]]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/vid_still.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1).save()

        yield controllers.brightcove_ingester.process_one_account(
            self.platform)

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          '123456')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_moved_bc_urls(self):
        self.mock_bc_response.side_effect = [[{
            'id' : 'v1',
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
                }
            ]]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bcsecure01-a.akamaihd.net/4/vid_still.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1).save()

        yield controllers.brightcove_ingester.process_one_account(
            self.platform)

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          '123456')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_remote_urls(self):
        self.mock_bc_response.side_effect = [[{
            'id' : 'v1',
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
        ]]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://some_remote_still?c=90'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1).save()

        yield controllers.brightcove_ingester.process_one_account(
            self.platform)

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          'thumb_id')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_reference_id(self):
        self.mock_bc_response.side_effect = [[{
            'id' : 'v1',
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
        ]]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_moved_location'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref').save()

        yield controllers.brightcove_ingester.process_one_account(
            self.platform)

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          'thumb_id')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_match_external_id(self):
        self.mock_bc_response.side_effect = [[{
            'id' : 'v1',
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
            ]]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_moved_location'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref',
                          external_id='thumb_id').save()

        yield controllers.brightcove_ingester.process_one_account(
            self.platform)

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_new_thumb_found(self):
        self.mock_bc_response.side_effect = [[{
            'id' : 'v1',
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
                }
            ]]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_old_thumb.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref',
                          external_id='old_thumb_id').save()

        yield controllers.brightcove_ingester.process_one_account(
            self.platform)

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
                self.assertEquals(thumb.external_id, 'thumb_id')

        # Make sure the new image was uploaded
        self.im_download_mock.assert_called_with(
            'http://bc.com/new_still.jpg?x=8', async=True)
        self.assertGreater(self.cdn_mock.call_count, 0)

    @tornado.testing.gen_test
    def test_error_downloading_image(self):
        self.im_download_mock.side_effect = [IOError('Image Download Error')]

        self.mock_bc_response.side_effect = [[{
            'id' : 'v1',
                    'videoStillURL' : None,
                    'videoStill' : None,
                    'thumbnailURL' : 'http://bc.com/new_thumb.jpg?x=8',
                    'thumbnail' : {
                        'id' : 'thumb_id',
                        'referenceId' : None,
                        'remoteUrl' : None
                    }
                }
            ]]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/some_old_thumb.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          refid='my_thumb_ref',
                          external_id='old_thumb_id').save()

        with self.assertLogExists(logging.ERROR, 'Could not find valid image'):
            yield controllers.brightcove_ingester.process_one_account(
                self.platform)

        # Make sure there was no change to the database
        video_meta = VideoMetadata.get('acct1_v1')
        self.assertEquals(len(video_meta.thumbnail_ids), 2)

        self.im_download_mock.assert_called_with(
            'http://bc.com/new_thumb.jpg?x=8', async=True)
        self.assertEquals(self.cdn_mock.call_count, 0)
        

    @tornado.testing.gen_test
    def test_bc_client_error(self):
        self.mock_bc_response.side_effect = [
            api.brightcove_api.BrightcoveApiClientError('200 error')
        ]
        with self.assertLogExists(logging.ERROR, 'Client error calling'):
            yield controllers.brightcove_ingester.process_one_account(
                self.platform)

    @tornado.testing.gen_test
    def test_bc_server_error(self):
        self.mock_bc_response.side_effect = [
            api.brightcove_api.BrightcoveApiServerError('200 error')
        ]
        with self.assertLogExists(logging.ERROR, 'Server error calling'):
            yield controllers.brightcove_ingester.process_one_account(
                self.platform)

class SmokeTesting(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        # Mock api call
        self.bc_api_mocker = patch(
            'api.brightcove_api.BrightcoveApi.read_connection')
        self.mock_bc_response = MagicMock()
        self.bc_api_mocker.start().send_request.side_effect = \
          lambda x, callback: callback(self.mock_bc_response())
          
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
        self.cdn_mock.return_value = 'some_cdn_url.jpg'

        # Build a platform with a video in it
        platform = neondata.BrightcovePlatform('a1', 'i1', 'acct1')
        vid = VideoMetadata('acct1_v1',
                            ['acct1_v1_n1', 'acct1_v1_bc1'],
                            i_id='i1')
        vid.save()
        neondata.BrightcovePlatform.modify(
            'acct1', 'i1',
            lambda x: x.add_video(vid.key, None), create_missing=True)
        ThumbnailMetadata('acct1_v1_n1', 'acct1_v1',
                          ttype=ThumbnailType.NEON, rank=1).save()

        self.old_poll_cycle = options.get(
            'controllers.brightcove_ingester.poll_period')
        options._set('controllers.brightcove_ingester.poll_period', 0)
        

        super(SmokeTesting, self).setUp()

    def tearDown(self):
        options._set('controllers.brightcove_ingester.poll_period',
                     self.old_poll_cycle)
        self.bc_api_mocker.stop()
        self.im_download_mocker.stop()
        self.cdn_mocker.stop()
        self.redis.stop()

        super(SmokeTesting, self).tearDown()

    @tornado.testing.gen_test
    def test_single_video(self):
        self.mock_bc_response.side_effect = [
            tornado.httpclient.HTTPResponse(
                tornado.httpclient.HTTPRequest('bcurl'),
                200,
                buffer=StringIO(json.dumps({'items' : [
                    { 'id' : 2790007957001,
                    'videoStillURL' : 'http://bc.com/vid_still.jpg?x=5',
                    'videoStill' : {
                        'id' : 279,
                        'referenceId' : None,
                        'remoteUrl' : None
                    },
                    'thumbnailURL' : 'http://bc.com/thumb_still.jpg?x=8',
                    'thumbnail' : {
                        'id' : 278,
                        'referenceId' : None,
                        'remoteUrl' : None
                    }},
                    None]})))]
        
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/vid_still.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1,
                          external_id='279').save()

        # Run the cycle. Clearing the flag after getting back the
        # future will case the loop to stop after the first iteration.
        run_flag = multiprocessing.Event()
        run_flag.set()
        main_future = controllers.brightcove_ingester.main(run_flag)
        run_flag.clear()
        yield main_future

        # Make sure we asked for brightcove data
        self.assertEquals(self.mock_bc_response.call_count, 1)

        # Make sure there was no upload
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.call_count, 0)
        
            
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
