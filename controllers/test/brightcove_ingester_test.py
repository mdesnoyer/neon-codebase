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
import logging
from mock import patch, MagicMock
import test_utils.redis
import test_utils.neontest
import tornado.gen
import tornado.testing
import unittest
from utils.imageutils import PILImageUtils
import utils.neon

class TestProcessOneAccount(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        # Mock out the image download
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.im_download_mock = self.im_download_mocker.start()
        self.random_image = PILImageUtils.create_random_image(480, 640)
        image_future = concurrent.futures.Future()
        image_future.set_result(self.random_image)
        self.im_download_mock.return_value = image_future

        # Mock out the image upload
        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        upload_future = concurrent.futures.Future()
        upload_future.set_result('some_cdn_url.jpg')
        self.cdn_mock = self.cdn_mocker.start()
        self.cdn_mock.create().upload.return_value = upload_future

        # Mock out the brightcove api and build the platform
        mock_bc_api = MagicMock()
        self.platform = neondata.BrightcovePlatform('a1', 'i1', 'acct1')
        self.platform.get_api = lambda: mock_bc_api
        self.mock_bc_response = MagicMock()
        def _build_future(*args, **kwargs):
            future = concurrent.futures.Future()
            try:
                future.set_result(self.mock_bc_response(*args, **kwargs))
            except Exception as e:
                future.set_exception(e)
            return future
        mock_bc_api.find_videos_by_ids.side_effect = _build_future
        
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
        self.mock_bc_response.side_effect = [{
            'v1': { 'id' : 'v1',
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
            }]
        ThumbnailMetadata('acct1_v1_bc1', 'acct1_v1',
                          ['http://bc.com/vid_still.jpg'],
                          ttype=ThumbnailType.BRIGHTCOVE,
                          rank=1).save()

        yield controllers.brightcove_ingester.process_one_account(
            self.platform)

        # Check that the external id was set
        self.assertEquals(ThumbnailMetadata.get('acct1_v1_bc1').external_id,
                          'thumb_id')

        # Make sure no image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 0)
        self.assertEquals(self.cdn_mock.create().upload.call_count, 0)

    @tornado.testing.gen_test
    def test_match_remote_urls(self):
        self.mock_bc_response.side_effect = [{
            'v1': { 'id' : 'v1',
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
            }]
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
        self.assertEquals(self.cdn_mock.create().upload.call_count, 0)

    @tornado.testing.gen_test
    def test_match_reference_id(self):
        self.mock_bc_response.side_effect = [{
            'v1': { 'id' : 'v1',
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
            }]
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
        self.assertEquals(self.cdn_mock.create().upload.call_count, 0)

    @tornado.testing.gen_test
    def test_match_external_id(self):
        self.mock_bc_response.side_effect = [{
            'v1': { 'id' : 'v1',
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
            }]
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
        self.assertEquals(self.cdn_mock.create().upload.call_count, 0)

    @tornado.testing.gen_test
    def test_new_thumb_found(self):
        self.mock_bc_response.side_effect = [{
            'v1': { 'id' : 'v1',
                    'videoStillURL' : None,
                    'videoStill' : None,
                    'thumbnailURL' : 'http://bc.com/new_thumb_still.jpg?x=8',
                    'thumbnail' : {
                        'id' : 'thumb_id',
                        'referenceId' : None,
                        'remoteUrl' : None
                    }
                }
            }]
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
                    'http://bc.com/new_thumb_still.jpg?x=8'])
                self.assertEquals(thumb.external_id, 'thumb_id')

        # Make sure the new image was uploaded
        self.assertEquals(self.im_download_mock.call_count, 1)
        self.assertGreater(self.cdn_mock.create().upload.call_count, 0)

        

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
            
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
