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
import urlparse
from utils.imageutils import PILImageUtils
import utils.neon

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

        # Mock out the cdn url check
        self.cdn_check_patcher = patch('cmsdb.cdnhosting.utils.http')
        self.mock_cdn_url = self._callback_wrap_mock(
            self.cdn_check_patcher.start().send_request)
        self.mock_cdn_url.side_effect = lambda x, **kw: HTTPResponse(x, 200)

        random.seed(1654984)

        self.image = PILImageUtils.create_random_image(480, 640)
        super(TestAWSHosting, self).setUp()

    def tearDown(self):
        self.neondata_patcher.stop()
        self.s3_patcher.stop()
        self.cdn_check_patcher.stop()
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
        metadata = neondata.PrimaryNeonHostingMetadata(
            'acct1', 'hosting-bucket')

        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        urls = yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)
        self.assertEqual(
            urls[0][0],
            "http://s3.amazonaws.com/hosting-bucket/acct1/vid1/tid1.jpg")
        self.bucket = self.s3conn.get_bucket('hosting-bucket')
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
        metadata = neondata.PrimaryNeonHostingMetadata(
            'acct1',
            'hosting-bucket',
            folder_prefix='my/folder/path')

        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        urls = yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)
        self.assertEqual(
            urls[0][0],
            "http://s3.amazonaws.com/hosting-bucket/my/folder/path/acct1/vid1/tid1.jpg")
        self.bucket = self.s3conn.get_bucket('hosting-bucket')
        s3_key = self.bucket.get_key('my/folder/path/acct1/vid1/tid1.jpg')
        self.assertIsNotNone(s3_key)
        self.assertEqual(s3_key.content_type, 'image/jpeg')
        self.assertEqual(s3_key.policy, 'public-read')

    @tornado.testing.gen_test
    def test_overwrite_image(self):
        metadata = neondata.PrimaryNeonHostingMetadata(
            'acct1', 'hosting-bucket')
        hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)

        # Do initial upload
        url = yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)
        s3key = self.bucket.get_key('acct1/vid1/tid1.jpg')
        orig_etag = s3key.etag

        # Now upload, but don't overwrite
        new_image = PILImageUtils.create_random_image(480, 640)
        yield hoster.upload(new_image, 'acct1_vid1_tid1', overwrite=False,
                            async=True)

        # Check the file contents
        s3key = self.bucket.get_key('acct1/vid1/tid1.jpg')
        self.assertIsNotNone(s3key)
        self.assertEquals(s3key.etag, orig_etag)

        # Now overwrite
        yield hoster.upload(new_image, 'acct1_vid1_tid1', async=True)
            
        buf = StringIO()
        s3key = self.bucket.get_key('acct1/vid1/tid1.jpg')
        self.assertNotEquals(s3key.etag, orig_etag)
            

    @tornado.testing.gen_test
    def test_permissions_error_uploading_image(self):
        self.s3conn.get_bucket = MagicMock()
        self.s3conn.get_bucket().get_key.side_effect = [None]
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
        self.s3conn.get_bucket().get_key.side_effect = [None]
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
        mock_http.side_effect = lambda x, callback=None, **kw: callback(
            tornado.httpclient.HTTPResponse(x, 200,
                                            buffer=StringIO(mock_response)))
        url = cd.upload(None, tid, url)
        self.assertEquals(mock_http.call_count, 2)
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
            with self.assertRaises(IOError):
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

        # Mock out the cdn url check
        self.cdn_check_patcher = patch('cmsdb.cdnhosting.utils.http')
        self.mock_cdn_url = self._callback_wrap_mock(
            self.cdn_check_patcher.start().send_request)
        self.mock_cdn_url.side_effect = lambda x, **kw: HTTPResponse(x, 200)

        random.seed(1654984)

        sizes = [(640, 480), (160, 90)]
        self.metadata = neondata.NeonCDNHostingMetadata(None,
            'hosting-bucket', ['cdn1.cdn.com', 'cdn2.cdn.com'],
            'folder1', True, True, False, False, sizes)

        self.image = PILImageUtils.create_random_image(480, 640)
        super(TestAWSHostingWithServingUrls, self).setUp()

    def tearDown(self):
        self.s3_patcher.stop()
        self.cdn_check_patcher.stop()
        self.redis.stop()
        super(TestAWSHostingWithServingUrls, self).tearDown()

    @tornado.testing.gen_test
    def test_host_resized_images(self):
        hoster = cmsdb.cdnhosting.CDNHosting.create(self.metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

        for w, h in self.metadata.rendition_sizes:

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
    def test_https_cdn_prefix(self):
        self.metadata.cdn_prefixes = ['https://cdn1.cdn.com/neon',
                                      'https://cdn2.cdn.com/neon']

        hoster = cmsdb.cdnhosting.CDNHosting.create(self.metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

        for w, h in self.metadata.rendition_sizes:
            key_name = 'folder1/neontnacct1_vid1_tid1_w%i_h%i.jpg' % (w, h)
            s3key = self.bucket.get_key(key_name)
            self.assertIsNotNone(s3key)

            url = serving_urls.get_serving_url(w, h)
            self.assertRegexpMatches(
                url, 'https://cdn[1-2].cdn.com/neon/%s' % key_name)
                                                   

    @tornado.testing.gen_test
    def test_salted_path(self):
        self.metadata.do_salt = True

        hoster = cmsdb.cdnhosting.CDNHosting.create(self.metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

        keyRe = re.compile('(folder1/[0-9a-zA-Z]{3})/neontnacct1_vid1_tid1_'
                           'w([0-9]+)_h([0-9]+).jpg')
        sizes_found = []
        folders_found = []
        base_urls = []
        for s3key in self.bucket.list():
            # Make sure that the key is the expected format with salt
            match = keyRe.match(s3key.name)
            self.assertIsNotNone(match)

            width = int(match.group(2))
            height = int(match.group(3))
            sizes_found.append((width, height))
            folders_found.append(match.group(1))

            # Check that the serving url is included
            url = serving_urls.get_serving_url(width, height)
            self.assertRegexpMatches(
                url, 'http://cdn[1-2].cdn.com/%s' % s3key.name)
            base_urls.append(url.rpartition('/')[0])

            # Check that the image is as expected
            buf = StringIO()
            s3key.get_contents_to_file(buf)
            buf.seek(0)
            im = PIL.Image.open(buf)
            self.assertEqual(im.size, (width, height))
            self.assertEqual(im.mode, 'RGB')
            self.assertEqual(s3key.policy, 'public-read')

        # Make sure that all the expected files were found
        self.assertItemsEqual(sizes_found, self.metadata.rendition_sizes)

        # Make sure that the folders were the same
        self.assertEquals(len(folders_found),
                          len(self.metadata.rendition_sizes))
        self.assertEquals(len(set(folders_found)), 1)

        # Make sure the base urls were the same
        self.assertEquals(len(base_urls), len(self.metadata.rendition_sizes))
        self.assertEquals(len(set(base_urls)), 1)

    @tornado.testing.gen_test
    def test_delete_salted_image(self):
        self.metadata.do_salt = True

        hoster = cmsdb.cdnhosting.CDNHosting.create(self.metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        self.assertEquals(len(list(self.bucket.list())), 2)

        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

        yield hoster.delete(serving_urls.get_serving_url(640,480), async=True)

        self.assertEquals(len(list(self.bucket.list())), 1)

    @tornado.testing.gen_test
    def test_change_rendition_sizes(self):
        # Upload 640x480 & 160x90
        hoster = cmsdb.cdnhosting.CDNHosting.create(self.metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)
        
        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        self.assertIsNotNone(serving_urls)

        for w, h in self.metadata.rendition_sizes:
            key_name = 'folder1/neontnacct1_vid1_tid1_w%i_h%i.jpg' % (w, h)
            s3key = self.bucket.get_key(key_name)
            self.assertIsNotNone(s3key)

        # Change the rendition sizes to be 320x240 & 160x90 but keep
        # the old rendition around
        self.metadata.rendition_sizes = [(320, 240), (160, 90)]
        hoster = cmsdb.cdnhosting.CDNHosting.create(self.metadata)
        yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)

        # Check that the serving urls include the intersection
        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        for w, h in [(320, 240), (160, 90), (640, 480)]:
            self.assertRegexpMatches(serving_urls.get_serving_url(w, h),
                                     'http://cdn[1-2].cdn.com/.*\.jpg')

        # Now overwrite the serving urls and check that the 640x480 isn't there
        yield hoster.upload(self.image, 'acct1_vid1_tid1',
                            servingurl_overwrite=True, async=True)
        serving_urls = neondata.ThumbnailServingURLs.get('acct1_vid1_tid1')
        for w, h in [(320, 240), (160, 90)]:
            self.assertRegexpMatches(serving_urls.get_serving_url(w, h),
                                     'http://cdn[1-2].cdn.com/.*\.jpg')
        with self.assertRaises(KeyError):
            serving_urls.get_serving_url(640, 480)

        # Make sure the keys are consistent in S3
        for w, h in self.metadata.rendition_sizes:
            key_name = 'folder1/neontnacct1_vid1_tid1_w%i_h%i.jpg' % (w, h)
            s3key = self.bucket.get_key(key_name)
            self.assertIsNotNone(s3key)

        # Don't delete the 640x480 image because it will take a while
        # until we stop serving it.
        s3key = self.bucket.get_key(
            'folder1/neontnacct1_vid1_tid1_w640_h480.jpg')
        self.assertIsNotNone(s3key)

    @tornado.testing.gen_test
    def test_bad_url_generated(self):
        self.mock_cdn_url.side_effect = lambda x, **kw: HTTPResponse(
            x, 404, error=HTTPError(404))
        hoster = cmsdb.cdnhosting.CDNHosting.create(self.metadata)

        with self.assertRaisesRegexp(IOError, 'CDN url .* is invalid'):
            yield hoster.upload(self.image, 'acct1_vid1_tid1', async=True)
            
        # Make sure there are no serving urls
        self.assertIsNone(neondata.ThumbnailServingURLs.get('acct1_vid1_tid1'))
        

class TestAkamaiHosting(test_utils.neontest.AsyncTestCase):
    '''
    Test uploading images to Akamai
    '''
    def setUp(self):
        super(TestAkamaiHosting, self).setUp()

        # Mock out the http requests, one mock for each type
        self.akamai_mock = MagicMock()
        self.akamai_mock.side_effect = lambda x, **kw: HTTPResponse(x, 200)
        self.cdn_mock = MagicMock()
        self.cdn_mock.side_effect = lambda x, **kw: HTTPResponse(x, 200)
        self.http_patcher = patch('cmsdb.cdnhosting.utils.http')
        self.http_mock = self._callback_wrap_mock(
            self.http_patcher.start().send_request)
        def _handle_http_request(request, *args, **kwargs):
            if 'cdn' in request.url:
                return self.cdn_mock(request, *args, **kwargs)
            else:
                return self.akamai_mock(request, *args, **kwargs)
        self.http_mock.side_effect = _handle_http_request
        
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        random.seed(1654984)
        
        self.image = PILImageUtils.create_random_image(480, 640)

    def tearDown(self):
        self.http_patcher.stop()
        self.redis.stop()
        super(TestAkamaiHosting, self).tearDown()

    @tornado.testing.gen_test
    def test_upload_image(self):
        metadata = neondata.AkamaiCDNHostingMetadata(
            key=None,
            host='akamai',
            akamai_key='akey',
            akamai_name='aname',
            cpcode='168974',
            folder_prefix=None,
            cdn_prefixes=['cdn1.akamai.com', 'cdn2.akamai.com']
            )

        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        
        tid = 'customeraccountnamelabel_vid1_tid1'

        # the expected root of the url is the first 24 characters of the tid
        url_root_folder = tid[:24]

        yield self.hoster.upload(self.image, tid, async=True)
       
        # Check http mock and Akamai request
        # make sure the http mock was called
        self.assertGreater(self.akamai_mock._mock_call_count, 0)
        upload_requests = [x[0][0] for x in
                           self.akamai_mock._mock_call_args_list]
        for request in upload_requests:
            self.assertItemsEqual(request.headers.keys(),
                                  ['Content-Length',
                                   'X-Akamai-ACS-Auth-Data',
                                   'X-Akamai-ACS-Auth-Sign',
                                   'X-Akamai-ACS-Action'])
            actions = urlparse.parse_qs(request.headers['X-Akamai-ACS-Action'])
            self.assertDictContainsSubset(
                { 'version': ['1'],
                  'action' : ['upload'],
                  'format' : ['xml']
                  },
                  actions)
            self.assertIn('md5', actions)
                  
            self.assertRegexpMatches(
                request.url, 
                ('http://akamai/168974/%s/[a-zA-Z]/[a-zA-Z]/[a-zA-Z]/'
                 'neontn%s_w[0-9]+_h[0-9]+.jpg' % (url_root_folder, tid)))
            self.assertEquals(request.method, "POST")

        # Check serving URLs
        ts = neondata.ThumbnailServingURLs.get(tid)
        self.assertGreater(len(ts.size_map), 0)

        base_urls = []

        # Verify the final image URLs. This should be the account id 
        # followed by 3 sub folders whose name should be a single letter
        # (lower or uppercase) choosen randomly, then the thumbnail file
        for (w, h), url in ts.size_map.iteritems():
            url = ts.get_serving_url(w, h)
            url_re = ('(http://cdn[12].akamai.com/%s/[a-zA-Z]/[a-zA-Z]/'
                      '[a-zA-Z])/neontn%s_w%s_h%s.jpg' % 
                      (url_root_folder,tid, w, h))
                
            self.assertRegexpMatches(url, url_re)

            # Grab the base url
            base_urls.append(re.compile(url_re).match(url).group(1))

        # Make sure all the base urls are the same for a given thumb
        self.assertGreater(len(base_urls), 1)
        self.assertEquals(len(set(base_urls)), 1)

        # Make sure that the url is exactly what we expect. If this
        # check fails, then the python random module had changed
        self.assertEquals(
            base_urls[0],
            'http://cdn1.akamai.com/customeraccountnamelabel/G/l/l')

    @tornado.testing.gen_test
    def test_with_folder_prefix(self):
        metadata = neondata.AkamaiCDNHostingMetadata(
            key=None,
            host='http://akamai.com/',
            akamai_key='akey',
            akamai_name='aname',
            cpcode='168974',
            folder_prefix='neon/prod',
            cdn_prefixes=['https://cdn1.akamai.com', 'https://cdn2.akamai.com']
            )

        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        
        tid = 'customeraccountnamelabel_vid1_tid1'

        # the expected root of the url is the first 24 characters of the tid
        url_root_folder = tid[:24]

        yield self.hoster.upload(self.image, tid, async=True)

        # Check http mock and Akamai request
        # make sure the http mock was called
        self.assertGreater(self.akamai_mock._mock_call_count, 0)
        upload_requests = [x[0][0] for x in
                           self.akamai_mock._mock_call_args_list]
        for request in upload_requests:
            self.assertItemsEqual(request.headers.keys(),
                                  ['Content-Length',
                                   'X-Akamai-ACS-Auth-Data',
                                   'X-Akamai-ACS-Auth-Sign',
                                   'X-Akamai-ACS-Action'])
            actions = urlparse.parse_qs(request.headers['X-Akamai-ACS-Action'])
            self.assertDictContainsSubset(
                { 'version': ['1'],
                  'action' : ['upload'],
                  'format' : ['xml']
                  },
                  actions)
            self.assertIn('md5', actions)
                  
            self.assertRegexpMatches(
                request.url, 
                ('http://akamai.com/168974/neon/prod/%s/[a-zA-Z]/[a-zA-Z]/'
                 '[a-zA-Z]/neontn%s_w[0-9]+_h[0-9]+.jpg' % 
                 (url_root_folder, tid)))
            self.assertEquals(request.method, "POST")

        # Check serving URLs
        ts = neondata.ThumbnailServingURLs.get(tid)
        self.assertGreater(len(ts.size_map), 0)

        # Verify the final image URLs. This should be the account id 
        # followed by 3 sub folders whose name should be a single letter
        # (lower or uppercase) choosen randomly, then the thumbnail file
        for (w, h), url in ts.size_map.iteritems():
            url = ts.get_serving_url(w, h)
            url_re = ('(https://cdn[12].akamai.com/neon/prod/%s/[a-zA-Z]/'
                      '[a-zA-Z]/[a-zA-Z])/neontn%s_w%s_h%s.jpg' % 
                      (url_root_folder,tid, w, h))
                
            self.assertRegexpMatches(url, url_re)

    @tornado.testing.gen_test
    def test_no_overwrite(self):
        metadata = neondata.AkamaiCDNHostingMetadata(
            key=None,
            host='akamai',
            akamai_key='akey',
            akamai_name='aname',
            cpcode='168974',
            folder_prefix=None,
            cdn_prefixes=['cdn1.akamai.com', 'cdn2.akamai.com'],
            rendition_sizes=[(640, 480)]
            )

        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)

        yield self.hoster.upload(self.image, 'some_vid_tid', overwrite=False,
                                 async=True)

        # Only one call should have been sent to akamai and it should be a stat
        self.assertEquals(self.akamai_mock.call_count, 1)
        cargs, kwargs = self.akamai_mock.call_args
        request = cargs[0]
        self.assertIn('action=stat', request.headers['X-Akamai-ACS-Action'])
        
    
    @tornado.testing.gen_test
    def test_upload_image_error(self):
        self.akamai_mock.side_effect = lambda x, **kw: HTTPResponse(x, 500)
        metadata = neondata.AkamaiCDNHostingMetadata(
            key=None,
            host='http://akamai',
            akamai_key='akey',
            akamai_name='aname',
            cpcode='168974',
            cdn_prefixes=['cdn1.akamai.com', 'cdn2.akamai.com']
            )

        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        tid = 'akamai_vid1_tid2'
        
        with self.assertLogExists(logging.ERROR, 
                'Error uploading image to akamai for tid %s' % tid):
            with self.assertRaises(IOError):
                yield self.hoster.upload(self.image, tid, async=True)
        
        self.assertGreater(self.akamai_mock._mock_call_count, 0)
        
        ts = neondata.ThumbnailServingURLs.get(tid)
        self.assertIsNone(ts)

    @tornado.testing.gen_test
    def test_bad_url_generated(self):
        self.cdn_mock.side_effect = lambda x, **kw: HTTPResponse(
            x, 404, error=HTTPError(404))
        
        metadata = neondata.AkamaiCDNHostingMetadata(
            key=None,
            host='akamai',
            akamai_key='akey',
            akamai_name='aname',
            cpcode='168974',
            folder_prefix=None,
            cdn_prefixes=['cdn1.akamai.com', 'cdn2.akamai.com']
            )

        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)
        
        tid = 'customeraccountnamelabel_vid1_tid1'

        with self.assertRaisesRegexp(IOError, 'CDN url .* is invalid'):
            yield self.hoster.upload(self.image, tid, async=True)
            
        # Make sure there are no serving urls
        self.assertIsNone(neondata.ThumbnailServingURLs.get(tid))
        
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
