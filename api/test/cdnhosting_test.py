#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cdnhosting
import logging
from mock import patch
from supportServices import neondata
import test_utils.mock_boto_s3 as boto_mock
import test_utils.redis
import unittest
from utils.imageutils import PILImageUtils

_log = logging.getLogger(__name__)

class TestCDNHosting(unittest.TestCase):
    ''' 
    Test Video Processing client
    '''
    def setUp(self):
        super(TestCDNHosting, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start() 

    def tearDown(self):
        self.redis.stop()
        super(TestCDNHosting, self).tearDown()

        
    @patch('api.cdnhosting.S3Connection')
    def test_neon_hosting(self, mock_conntype):
        hosting = api.cdnhosting.AWSHosting()
        
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('neon-image-cdn')
        mock_conntype.return_value = conn
        imbucket = conn.get_bucket("neon-image-cdn")
        image = PILImageUtils.create_random_image(360, 480) 
        keyname = "test_key"
        tid = "test_tid"
        hosting.upload(image, tid)
        sizes = api.properties.CDN_IMAGE_SIZES   
        s3_keys = [x for x in imbucket.get_all_keys()]
        self.assertEqual(len(s3_keys), len(sizes))
        
        # Verify the contents in serving url 
        serving_urls = neondata.ThumbnailServingURLs.get(tid)
        for w, h in sizes:
            url = serving_urls.get_serving_url(w, h)
            fname = "neontntest_tid_w%s_h%s.jpg" % (w, h)
            exp_url = "http://%s/%s" % ("imagecdn.neon-lab.com", fname)
            self.assertEqual(exp_url, url)

        #image-cdn/NEON_PUB_ID/neontntest_tid_w800_h600.jpg
        #Build a per video stats page to monitor each video

if __name__ == '__main__':
    unittest.main()
