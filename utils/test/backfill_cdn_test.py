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
from utils import backfill_cdn

_log = logging.getLogger(__name__)

class TestBackfillCDN(test_utils.neontest.AsyncTestCase):
    ''' 
    '''
    def setUp(self):
        
        self.s3conn = boto_mock.MockConnection()
        self.s3_patcher = patch('cmsdb.cdnhosting.S3Connection')
        self.mock_conn = self.s3_patcher.start()
        self.mock_conn.return_value = self.s3conn
        bname = 'n3.neon-images.com'
        self.s3conn.create_bucket(bname)
        self.bucket = self.s3conn.get_bucket(bname)
        
        self.http_call_patcher = \
          patch('utils.http.send_request')
        self.http_mock = self.http_call_patcher.start()
        
        metadata = neondata.AkamaiCDNHostingMetadata(key=None,
                host='http://akamai',
                akamai_key='akey',
                akamai_name='aname',
                baseurl='base',
                cdn_prefixes=['cdn.akamai.com']
                )
        
        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)

        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

        super(TestBackfillCDN, self).setUp()
    
    
    def tearDown(self):
        self.s3_patcher.stop()
        self.http_call_patcher.stop()
        self.redis.stop()
        super(TestBackfillCDN, self).tearDown()

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

    # NOTE: Adding a very basic test for this script, add more tests as
    # necessary in the future

    def test_new_size(self):
        
        # create image response
        image = PILImageUtils.create_random_image(480, 640)
        imdata = StringIO()
        image.save(imdata, "JPEG")
        imdata.seek(0)
        self._set_http_response(body=imdata.read())
       
        api_key = "test"
        vid = 'v1'
        tid = '%s_%s_t1' % (api_key, vid)

        # create brightcove account
        ba = neondata.BrightcovePlatform('aid', 'iid', api_key)
        ba.add_video(vid, 'j1')
        ba.save()
        v1 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(api_key, vid),
            [tid],
            '', '', 0, 0.0, 1, '')
        v1.save()
        t1 = neondata.ThumbnailMetadata(
                tid, v1.key, ['http://someimg.jpg'],
                None, None, None, None, None, None, enabled=True)
        t1.save()

        input1 = neondata.ThumbnailServingURLs(tid)
        input1.add_serving_url('http://that_800_600.jpg', 800, 600) 
        input1.save()

        backfill_cdn.backfill(api_key, 'iid')

    
        cdn_key = neondata.CDNHostingMetadataList.create_key(api_key, 'iid')
        clist = neondata.CDNHostingMetadataList.get(cdn_key)
        # Check then newly added images
        s_urls = neondata.ThumbnailServingURLs.get(tid)
        for sz in clist.cdns[0].rendition_sizes:
            c_url = s_urls.get_serving_url(sz[0], sz[1])
            self.assertIsNotNone(c_url)

if __name__ == '__main__':
    unittest.main()
