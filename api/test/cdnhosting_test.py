#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cdnhosting
import boto.exception
from cStringIO import StringIO
import json
import logging
from mock import MagicMock, patch
from supportServices import neondata
import PIL
import random
import re
import test_utils.mock_boto_s3 as boto_mock
import test_utils.neontest
import test_utils.redis
import tornado.testing
import unittest
from utils.imageutils import PILImageUtils

_log = logging.getLogger(__name__)

class TestAWSHosting(test_utils.neontest.AsyncTestCase):
    ''' 
    Test the ability to host images on an aws cdn (aka S3)
    '''
    def setUp(self):
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('api.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('hosting-bucket')
        self.bucket = self.s3conn.get_bucket('hosting-bucket')

        # Mock neondata
        self.neondata_patcher = patch('api.cdnhosting.supportServices.neondata')
        self.datamock = self.neondata_patcher.start()
        self.datamock.S3CDNHostingMetadata = neondata.S3CDNHostingMetadata
        self.datamock.CloudinaryCDNHostingMetadata = \
          neondata.CloudinaryCDNHostingMetadata
        self.datamock.NeonCDNHostingMetadata = neondata.NeonCDNHostingMetadata

        random.seed(1654984)

        self.image = PILImageUtils.create_random_image(480, 640)
        super(TestAWSHosting, self).setUp()

    def tearDown(self):
        self.neondata_patcher.stop()
        self.s3_patcher.stop()
        super(TestAWSHosting, self).tearDown()

    @tornado.testing.gen_test
    def test_host_single_image(self):
        metadata = neondata.S3CDNHostingMetadata(None,
            'access_key', 'secret_key',
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', False, False, False)

        hoster = api.cdnhosting.CDNHosting.create(metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        self.mock_conn.assert_called_with('access_key', 'secret_key')

        s3_key = self.bucket.get_key('folder1/neontnacct1_vid1_tid1_w640_h480.jpg')
        self.assertIsNotNone(s3_key)
        self.assertEqual(s3_key.content_type, 'image/jpeg')
        self.assertNotEqual(s3_key.policy, 'public-read')

        # Make sure that the serving urls weren't added
        self.assertEquals(self.datamock.ThumbnailServingURLs.modify.call_count,
                          0)


    @tornado.testing.gen_test
    def test_permissions_error_uploading_image(self):
        self.s3conn.get_bucket = MagicMock()
        self.s3conn.get_bucket().new_key().set_contents_from_string.side_effect = [boto.exception.S3PermissionsError('Permission error')]
        
        metadata = neondata.S3CDNHostingMetadata(None,
            'access_key', 'secret_key',
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', False, False)
        hoster = api.cdnhosting.CDNHosting.create(metadata)

        with self.assertLogExists(logging.ERROR, 'AWS client error'):
            with self.assertRaises(IOError):
                yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

    @tornado.testing.gen_test
    def test_create_error_uploading_image(self):
        self.s3conn.get_bucket = MagicMock()
        self.s3conn.get_bucket().new_key().set_contents_from_string.side_effect = [boto.exception.S3CreateError('oops', 'seriously, oops')]
        
        metadata = neondata.S3CDNHostingMetadata(None,
            'access_key', 'secret_key',
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', False, False)
        hoster = api.cdnhosting.CDNHosting.create(metadata)

        with self.assertLogExists(logging.ERROR, 'AWS Server error'):
            with self.assertRaises(IOError):
                yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

    @tornado.testing.gen_test
    def test_s3_redirect(self):
        self.s3conn.create_bucket('my-bucket')
        self.s3conn.create_bucket('host-bucket')
        self.s3conn.create_bucket('obucket')
        self.s3conn.create_bucket('mine')

        with patch('api.cdnhosting.get_s3_hosting_bucket') as location_mock:
            location_mock.return_value = 'host-bucket'
            yield [
                api.cdnhosting.create_s3_redirect(
                    'dest/image.jpg', 'src/samebuc.jpg', 'my-bucket',
                    'my-bucket', async=True),
                api.cdnhosting.create_s3_redirect(
                    'dest/image.jpg', 'src/diffbuc.jpg', 'my-bucket',
                    'obucket', async=True),
                api.cdnhosting.create_s3_redirect(
                        'dest/image.jpg', 'src/bothdefault.jpg', async=True),
                api.cdnhosting.create_s3_redirect(
                        'dest/image.jpg', 'src/destdefault.jpg',
                        src_bucket='mine', async=True),
                api.cdnhosting.create_s3_redirect(
                        'dest/image.jpg', 'src/srcdefault.jpg',
                        dest_bucket='mine', async=True), 
                ]

        self.assertEqual(self.s3conn.get_bucket('my-bucket').get_key(
            'src/samebuc.jpg').redirect_destination,
            '/dest/image.jpg')
        self.assertEqual(self.s3conn.get_bucket('obucket').get_key(
            'src/diffbuc.jpg').redirect_destination,
            'https://s3.amazonaws.com/my-bucket/dest/image.jpg')
        self.assertEqual(self.s3conn.get_bucket('host-bucket').get_key(
            'src/bothdefault.jpg').redirect_destination,
            '/dest/image.jpg')
        self.assertEqual(self.s3conn.get_bucket('mine').get_key(
            'src/destdefault.jpg').redirect_destination,
            'https://s3.amazonaws.com/host-bucket/dest/image.jpg')
        self.assertEqual(self.s3conn.get_bucket('host-bucket').get_key(
            'src/srcdefault.jpg').redirect_destination,
            'https://s3.amazonaws.com/mine/dest/image.jpg')

    @tornado.testing.gen_test
    def test_permissions_error_s3_redirect(self):
        self.s3conn.get_bucket = MagicMock()
        self.s3conn.get_bucket().new_key().set_contents_from_string.side_effect = [boto.exception.S3PermissionsError('Permission Error')]
        self.s3conn.create_bucket('host-bucket')

        with self.assertLogExists(logging.ERROR, 'AWS client error'):
            with self.assertRaises(IOError):
                yield api.cdnhosting.create_s3_redirect('dest.jpg', 'src.jpg',
                                                        async=True)

    @tornado.testing.gen_test
    def test_create_error_s3_redirect(self):
        self.s3conn.get_bucket = MagicMock()
        self.s3conn.get_bucket().new_key().set_contents_from_string.side_effect = [boto.exception.S3CreateError('oops', 'seriously, oops')]
        self.s3conn.create_bucket('host-bucket')

        with self.assertLogExists(logging.ERROR, 'AWS Server error'):
            with self.assertRaises(IOError):
                yield api.cdnhosting.create_s3_redirect('dest.jpg', 'src.jpg',
                                                        async=True)

    @patch('api.cdnhosting.utils.http.send_request')
    def test_cloudinary_hosting(self, mock_http):
        
        mock_response = '{"public_id":"bfea94933dc752a2def8a6d28f9ac4c2","version":1406671711,"signature":"26bd2ffa2b301b9a14507d152325d7692c0d4957","width":480,"height":268,"format":"jpg","resource_type":"image","created_at":"2014-07-29T22:08:19Z","bytes":74827,"type":"upload","etag":"99fd609b49a802fdef7e2952a5e75dc3","url":"http://res.cloudinary.com/neon-labs/image/upload/v1406671711/bfea94933dc752a2def8a6d28f9ac4c2.jpg","secure_url":"https://res.cloudinary.com/neon-labs/image/upload/v1406671711/bfea94933dc752a2def8a6d28f9ac4c2.jpg"}'

        metadata = neondata.CloudinaryCDNHostingMetadata()
        cd = api.cdnhosting.CDNHosting.create(metadata)
        im = 'https://s3.amazonaws.com/host-thumbnails/image.jpg'
        tid = 'bfea94933dc752a2def8a6d28f9ac4c2'
        mresponse = tornado.httpclient.HTTPResponse(
            tornado.httpclient.HTTPRequest('http://cloudinary.com'), 
            200, buffer=StringIO(mock_response))
        mock_http.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x, 200,
                                            buffer=StringIO(mock_response)))
        cd.upload(im, tid)
        self.assertEquals(mock_http.call_count, 1)
        # TODO(sunil): Check the contents of the request that was seen
        # to make sure it is what was expected.

    # TODO(Sunil): Add error testing for the uploads of cloudinary

class TestAWSHostingWithServingUrls(test_utils.neontest.AsyncTestCase):
    ''' 
    Test the ability to host images on an aws cdn (aka S3)
    '''
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('api.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('hosting-bucket')
        self.bucket = self.s3conn.get_bucket('hosting-bucket')

        random.seed(1654984)

        self.image = PILImageUtils.create_random_image(480, 640)
        super(TestAWSHostingWithServingUrls, self).setUp()

    def tearDown(self):
        self.s3_patcher.stop()
        self.redis.stop()
        super(TestAWSHostingWithServingUrls, self).tearDown()

    @tornado.testing.gen_test
    def test_host_resized_images(self):
        metadata = neondata.NeonCDNHostingMetadata(None,
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', True, True, False)

        hoster = api.cdnhosting.CDNHosting.create(metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

        sizes = api.properties.CDN_IMAGE_SIZES 
        for w, h in sizes:

            # check that the image is in s3
            key_name = 'folder1/neontnacct1_vid1_tid1_w%i_h%i.jpg' % (w, h)
            s3key = self.bucket.get_key(key_name)
            self.assertIsNotNone(s3key)
            buf = StringIO()
            s3key.get_contents_to_file(buf)
            buf.seek(0)
            im = PIL.Image.open(buf)
            self.assertEqual(im.size, (w, h))
            self.assertEqual(im.mode, 'RGB')
            self.assertEqual(s3key.policy, 'public-read')

            # Check that the serving url is included
            url = serving_urls.get_serving_url(w, h)
            self.assertRegexpMatches(
                url, 'http://cdn[1-2].cdn.com/%s' % key_name)

    @tornado.testing.gen_test
    def test_salted_path(self):
        metadata = neondata.NeonCDNHostingMetadata(None,
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', True, True, True)

        hoster = api.cdnhosting.CDNHosting.create(metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

        keyRe = re.compile('folder1/[0-9a-zA-Z]{3}/neontnacct1_vid1_tid1_'
                           'w([0-9]+)_h([0-9]+).jpg')
        sizes_found = []
        for s3key in self.bucket.list():
            # Make sure that the key is the expected format with salt
            match = keyRe.match(s3key.name)
            self.assertIsNotNone(match)

            width = int(match.group(1))
            height = int(match.group(2))
            sizes_found.append((width, height))

            # Check that the serving url is included
            url = serving_urls.get_serving_url(width, height)
            self.assertRegexpMatches(
                url, 'http://cdn[1-2].cdn.com/%s' % s3key.name)

            # Check that the image is as expected
            buf = StringIO()
            s3key.get_contents_to_file(buf)
            buf.seek(0)
            im = PIL.Image.open(buf)
            self.assertEqual(im.size, (width, height))
            self.assertEqual(im.mode, 'RGB')
            self.assertEqual(s3key.policy, 'public-read')

        # Make sure that all the expected files were found
        self.assertItemsEqual(sizes_found, api.properties.CDN_IMAGE_SIZES)

if __name__ == '__main__':
    unittest.main()
