#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from api import akamai_api
import boto.exception
from cStringIO import StringIO
import cmsdb.cdnhosting
import json
import logging
from mock import MagicMock, patch
from cmsdb import neondata
import PIL
import random
import re
import test_utils.mock_boto_s3 as boto_mock
import test_utils.neontest
import test_utils.redis
import tornado.testing
import unittest
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from utils.imageutils import PILImageUtils

_log = logging.getLogger(__name__)

class TestAWSHosting(test_utils.neontest.AsyncTestCase):
    ''' 
    Test the ability to host images on an aws cdn (aka S3)
    '''
    def setUp(self):
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        self.s3conn.create_bucket('hosting-bucket')
        self.bucket = self.s3conn.get_bucket('hosting-bucket')

        # Mock neondata
        self.neondata_patcher = patch('cmsdb.cdnhosting.cmsdb.neondata')
        self.datamock = self.neondata_patcher.start()
        self.datamock.S3CDNHostingMetadata = neondata.S3CDNHostingMetadata
        self.datamock.CloudinaryCDNHostingMetadata = \
          neondata.CloudinaryCDNHostingMetadata
        self.datamock.NeonCDNHostingMetadata = neondata.NeonCDNHostingMetadata
        self.datamock.PrimaryNeonHostingMetadata = \
                            neondata.PrimaryNeonHostingMetadata

        random.seed(1654984)

        self.image = PILImageUtils.create_random_image(480, 640)
        super(TestAWSHosting, self).setUp()

    def tearDown(self):
        self.neondata_patcher.stop()
        self.s3_patcher.stop()
        super(TestAWSHosting, self).tearDown()

    @tornado.testing.gen_test
    def test_host_single_image(self):
        '''
        Test hosting a single image with CDN prefixes into a S3 bucket
        '''
        metadata = neondata.S3CDNHostingMetadata(None,
            'access_key', 'secret_key',
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', False, False, False)

        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        self.mock_conn.assert_called_with('access_key', 'secret_key')

        s3_key = self.bucket.get_key(
            'folder1/neontnacct1_vid1_tid1_w640_h480.jpg')
        self.assertIsNotNone(s3_key)
        self.assertEqual(s3_key.content_type, 'image/jpeg')
        self.assertNotEqual(s3_key.policy, 'public-read')

        # Make sure that the serving urls weren't added
        self.assertEquals(self.datamock.ThumbnailServingURLs.modify.call_count,
                          0)
    
        
    @tornado.testing.gen_test
    def test_primary_hosting_single_image(self):
        '''
        Test hosting the Primary copy for a image in Neon's primary 
        hosting bucket
        '''
       
        # use the default bucket in the class to test with
        self.s3conn.create_bucket('host-thumbnails')
        metadata = neondata.PrimaryNeonHostingMetadata('acct1')

        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        url = yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)
        self.assertEqual(
            url,
            "http://s3.amazonaws.com/host-thumbnails/acct1/vid1/tid1.jpg")
        self.bucket = self.s3conn.get_bucket('host-thumbnails')
        s3_key = self.bucket.get_key('acct1/vid1/tid1.jpg')
        self.assertIsNotNone(s3_key)
        self.assertEqual(s3_key.content_type, 'image/jpeg')
        self.assertEqual(s3_key.policy, 'public-read')

    @tornado.testing.gen_test
    def test_primary_hosting_with_folder(self):
        '''
        Test hosting the Primary copy for a image in Neon's primary 
        hosting bucket
        '''
       
        # use the default bucket in the class to test with
        self.s3conn.create_bucket('host-thumbnails')
        metadata = neondata.PrimaryNeonHostingMetadata(
            'acct1',
            folder_prefix='my/folder/path')

        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        url = yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)
        self.assertEqual(
            url,
            "http://s3.amazonaws.com/host-thumbnails/my/folder/path/acct1/vid1/tid1.jpg")
        self.bucket = self.s3conn.get_bucket('host-thumbnails')
        s3_key = self.bucket.get_key('my/folder/path/acct1/vid1/tid1.jpg')
        self.assertIsNotNone(s3_key)
        self.assertEqual(s3_key.content_type, 'image/jpeg')
        self.assertEqual(s3_key.policy, 'public-read')

    @tornado.testing.gen_test
    def test_permissions_error_uploading_image(self):
        self.s3conn.get_bucket = MagicMock()
        self.s3conn.get_bucket().new_key().set_contents_from_string.side_effect = [boto.exception.S3PermissionsError('Permission error')]
        
        metadata = neondata.S3CDNHostingMetadata(None,
            'access_key', 'secret_key',
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', False, False)
        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)

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
        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)

        with self.assertLogExists(logging.ERROR, 'AWS Server error'):
            with self.assertRaises(IOError):
                yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

    @tornado.testing.gen_test
    def test_s3_redirect(self):
        self.s3conn.create_bucket('my-bucket')
        self.s3conn.create_bucket('host-bucket')
        self.s3conn.create_bucket('obucket')
        self.s3conn.create_bucket('mine')

        with patch('cmsdb.cdnhosting.get_s3_hosting_bucket') as location_mock:
            location_mock.return_value = 'host-bucket'
            yield [
                cmsdb.cdnhosting.create_s3_redirect(
                    'dest/image.jpg', 'src/samebuc.jpg', 'my-bucket',
                    'my-bucket', async=True),
                cmsdb.cdnhosting.create_s3_redirect(
                    'dest/image.jpg', 'src/diffbuc.jpg', 'my-bucket',
                    'obucket', async=True),
                cmsdb.cdnhosting.create_s3_redirect(
                        'dest/image.jpg', 'src/bothdefault.jpg', async=True),
                cmsdb.cdnhosting.create_s3_redirect(
                        'dest/image.jpg', 'src/destdefault.jpg',
                        src_bucket='mine', async=True),
                cmsdb.cdnhosting.create_s3_redirect(
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
                yield cmsdb.cdnhosting.create_s3_redirect('dest.jpg', 'src.jpg',
                                                        async=True)

    @tornado.testing.gen_test
    def test_create_error_s3_redirect(self):
        self.s3conn.get_bucket = MagicMock()
        self.s3conn.get_bucket().new_key().set_contents_from_string.side_effect = [boto.exception.S3CreateError('oops', 'seriously, oops')]
        self.s3conn.create_bucket('host-bucket')

        with self.assertLogExists(logging.ERROR, 'AWS Server error'):
            with self.assertRaises(IOError):
                yield cmsdb.cdnhosting.create_s3_redirect('dest.jpg', 'src.jpg',
                                                        async=True)

    @patch('cmsdb.cdnhosting.utils.http.send_request')
    def test_cloudinary_hosting(self, mock_http):
        
        mock_response = '{"public_id":"bfea94933dc752a2def8a6d28f9ac4c2","version":1406671711,"signature":"26bd2ffa2b301b9a14507d152325d7692c0d4957","width":480,"height":268,"format":"jpg","resource_type":"image","created_at":"2014-07-29T22:08:19Z","bytes":74827,"type":"upload","etag":"99fd609b49a802fdef7e2952a5e75dc3","url":"http://res.cloudinary.com/neon-labs/image/upload/v1406671711/bfea94933dc752a2def8a6d28f9ac4c2.jpg","secure_url":"https://res.cloudinary.com/neon-labs/image/upload/v1406671711/bfea94933dc752a2def8a6d28f9ac4c2.jpg"}'

        metadata = neondata.CloudinaryCDNHostingMetadata()
        cd = cmsdb.cdnhosting.CDNHosting.create(metadata)
        url = 'https://s3.amazonaws.com/host-thumbnails/image.jpg'
        tid = 'bfea94933dc752a2def8a6d28f9ac4c2'
        mresponse = tornado.httpclient.HTTPResponse(
            tornado.httpclient.HTTPRequest('http://cloudinary.com'), 
            200, buffer=StringIO(mock_response))
        mock_http.side_effect = lambda x, callback: callback(
            tornado.httpclient.HTTPResponse(x, 200,
                                            buffer=StringIO(mock_response)))
        url = cd.upload(None, tid, url)
        self.assertEquals(mock_http.call_count, 1)
        self.assertIsNotNone(mock_http._mock_call_args_list[0][0][0]._body)
        self.assertEqual(mock_http._mock_call_args_list[0][0][0].url,
                "https://api.cloudinary.com/v1_1/neon-labs/image/upload")

    @patch('cmsdb.cdnhosting.utils.http.send_request')
    def test_cloudinary_error(self, mock_http):

        metadata = neondata.CloudinaryCDNHostingMetadata()
        cd = cmsdb.cdnhosting.CDNHosting.create(metadata)
        url = 'https://s3.amazonaws.com/host-thumbnails/image.jpg'
        tid = 'bfea94933dc752a2def8a6d28f9ac4c2'
        mresponse = tornado.httpclient.HTTPResponse(
            tornado.httpclient.HTTPRequest('http://cloudinary.com'), 
            502, buffer=StringIO("gateway error"))
        mock_http.side_effect = lambda x, callback: callback(
                                    tornado.httpclient.HTTPResponse(x, 502,
                                    buffer=StringIO("gateway error")))
        with self.assertLogExists(logging.ERROR,
                'Failed to upload image to cloudinary for tid %s' % tid):
            url = cd.upload(None, tid, url)
        self.assertEquals(mock_http.call_count, 1)


class TestAWSHostingWithServingUrls(test_utils.neontest.AsyncTestCase):
    ''' 
    Test the ability to host images on an aws cdn (aka S3)
    '''
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
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
        sizes = [(640, 480), (160, 90)]
        metadata = neondata.NeonCDNHostingMetadata(None,
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', True, True, False, False, sizes)

        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

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
        sizes = [(640, 480), (160, 90), (1960, 1080)]
        metadata = neondata.NeonCDNHostingMetadata(None,
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', True, True, True, False, sizes)

        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
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
        self.assertItemsEqual(sizes_found, sizes)

class TestAkamaiHosting(test_utils.neontest.AsyncTestCase):
    '''
    Test uploading images to Akamai
    '''
    def setUp(self):
        self.http_call_patcher = \
          patch('api.akamai_api.utils.http.send_request')
        self.http_mock = self.http_call_patcher.start()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 
        
        metadata = neondata.AkamaiCDNHostingMetadata(key=None,
                host='http://akamai',
                akamai_key='akey',
                akamai_name='aname',
                baseurl='base',
                cdn_prefixes=['cdn.akamai.com']
                )

        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        
        random.seed(1654985)
        self.image = PILImageUtils.create_random_image(480, 640)
        super(TestAkamaiHosting, self).setUp()

    def tearDown(self):
        self.http_call_patcher.stop()
        self.redis.stop()
        super(TestAkamaiHosting, self).tearDown()

    def _set_http_response(self, code=200, body='', error=None):
        def do_response(request, callback=None, *args, **kwargs):
            response = HTTPResponse(request, code,
                                    buffer=StringIO(body),
                                    error=error)
            if callback:
                tornado.ioloop.IOLoop.current().add_callback(callback,
                                                             response)
            else:
                return response
        self.http_mock.side_effect = do_response

    @tornado.testing.gen_test
    def test_upload_image(self):
        self._set_http_response()
        tid = 'akamai_vid1_tid1'
        
        yield self.hoster.upload(self.image, tid, async=True)
       
        # Check http mock and Akamai request
        # make sure the http mock was called
        self.assertGreater(self.http_mock._mock_call_count, 0)
        headers = self.http_mock._mock_call_args_list[0][0][0].headers 
        self.assertListEqual(headers.keys(), ['Content-Length',
            'X-Akamai-ACS-Auth-Data', 'X-Akamai-ACS-Auth-Sign',
            'X-Akamai-ACS-Action'])

        # Check serving URLs
        ts = neondata.ThumbnailServingURLs.get(tid)
        self.assertGreater(len(ts.size_map), 0)

        # Verify the final image URLs
        for (w, h), url in ts.size_map.iteritems():
            url = ts.get_serving_url(w, h)
            self.assertRegexpMatches(
                    url, 'http://cdn.akamai.com/[a-zA-Z]/[a-zA-Z]/neontn%s_w%s_h%s.jpg' % (tid, w, h))
    
    @tornado.testing.gen_test
    def test_upload_image_error(self):
        self._set_http_response(code=500)
        tid = 'akamai_vid1_tid2'
        
        with self.assertLogExists(logging.WARNING, 
                'Error uploading image to akamai for tid %s' % tid):
            yield self.hoster.upload(self.image, tid, async=True)
        
        self.assertGreater(self.http_mock._mock_call_count, 0)
        
        ts = neondata.ThumbnailServingURLs.get(tid)
        self.assertEqual(len(ts.size_map), 0)

if __name__ == '__main__':
    unittest.main()
