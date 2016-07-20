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
import random
import re
import test_utils.mock_boto_s3 as boto_mock
import test_utils.neontest
import test_utils.postgresql 
import tornado.testing
import unittest
from tools import backfill_cdn
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from cvutils.imageutils import PILImageUtils
from utils.options import options

_log = logging.getLogger(__name__)

class ServerAsyncPostgresTest(test_utils.neontest.AsyncTestCase):
    def tearDown(self): 
        self.postgresql.clear_all_tables()
        super(ServerAsyncPostgresTest, self).tearDown()

    @classmethod
    def setUpClass(cls):
        super(ServerAsyncPostgresTest, cls).tearDownClass() 
        cls.max_io_loop_size = options.get(
            'cmsdb.neondata.max_io_loop_dict_size')
        options._set('cmsdb.neondata.max_io_loop_dict_size', 10)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)

    @classmethod
    def tearDownClass(cls): 
        cls.postgresql.stop()
        options._set('cmsdb.neondata.max_io_loop_dict_size', 
            cls.max_io_loop_size)
        super(ServerAsyncPostgresTest, cls).tearDownClass() 

class TestBackfillCDN(ServerAsyncPostgresTest):
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

        self.imdownload_patcher = patch(
            'tools.backfill_cdn.PILImageUtils.download_image')
        self.image = PILImageUtils.create_random_image(480, 640)
        self.imdownload_patcher.start().side_effect = [self.image]
        
        self.http_call_patcher = \
          patch('utils.http.send_request')
        self.http_mock = self._future_wrap_mock(
            self.http_call_patcher.start())
        self.http_mock.side_effect = lambda x, **kw: HTTPResponse(x, 200)
        
        metadata = neondata.AkamaiCDNHostingMetadata(key=None,
                host='http://akamai',
                akamai_key='akey',
                akamai_name='aname',
                cpcode='34563',
                cdn_prefixes=['cdn.akamai.com']
                )
        
        self.hoster = cmsdb.cdnhosting.CDNHosting.create(metadata)

        super(TestBackfillCDN, self).setUp()
    
    
    def tearDown(self):
        self.s3_patcher.stop()
        self.http_call_patcher.stop()
        self.imdownload_patcher.stop()
        super(TestBackfillCDN, self).tearDown()

    # TODO: Add error tests

    def test_new_size(self):
       
        api_key = "test"
        vid = 'v1'
        tid = '%s_%s_t1' % (api_key, vid)

        # create brightcove account
        ba = neondata.BrightcovePlatform.modify(
            api_key, 'aid',
            lambda x: x.add_video(vid, 'j1'),
            create_missing=False)
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
        input1.add_serving_url('http://that_12_8.jpg', 12, 8) 
        self.assertIsNotNone(input1.get_serving_url(12, 8))
        input1.save()

        backfill_cdn.process_one_video(v1.key)

    
        cdn_key = neondata.CDNHostingMetadataList.create_key(api_key, 'iid')
        clist = neondata.CDNHostingMetadataList.get(cdn_key)
        # Check the newly added images
        s_urls = neondata.ThumbnailServingURLs.get(tid)
        for sz in clist.cdns[0].rendition_sizes:
            c_url = s_urls.get_serving_url(sz[0], sz[1])
            self.assertIsNotNone(c_url)

        # Make sure the old size is gone
        with self.assertRaises(KeyError):
            s_urls.get_serving_url(12, 8)

if __name__ == '__main__':
    unittest.main()
