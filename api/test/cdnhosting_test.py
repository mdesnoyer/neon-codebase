#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cdnhosting
import json
import logging
from mock import MagicMock, patch
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

    @patch('api.cdnhosting.urllib2')
    def test_cloudinary_hosting(self, mock_http):
        
        mock_response = '{"public_id":"bfea94933dc752a2def8a6d28f9ac4c2","version":1406671711,"signature":"26bd2ffa2b301b9a14507d152325d7692c0d4957","width":480,"height":268,"format":"jpg","resource_type":"image","created_at":"2014-07-29T22:08:19Z","bytes":74827,"type":"upload","etag":"99fd609b49a802fdef7e2952a5e75dc3","url":"http://res.cloudinary.com/neon-labs/image/upload/v1406671711/bfea94933dc752a2def8a6d28f9ac4c2.jpg","secure_url":"https://res.cloudinary.com/neon-labs/image/upload/v1406671711/bfea94933dc752a2def8a6d28f9ac4c2.jpg"}'

        cd = api.cdnhosting.CloudinaryHosting()
        im = 'https://host-thumbnails.s3.amazonaws.com/image.jpg'
        tid = 'bfea94933dc752a2def8a6d28f9ac4c2'
        mresponse = MagicMock()
        mresponse.read.return_value = mock_response
        mock_http.urlopen.return_value = mresponse 
        uploaded_url = cd.upload(im, tid)
        self.assertEqual(uploaded_url, json.loads(mock_response)['url'])


if __name__ == '__main__':
    unittest.main()
