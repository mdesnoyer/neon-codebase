#!/usr/bin/env python
'''
Copyright: 2014 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
from mock import patch, MagicMock
from supportServices import neondata
import random
from supportServices.url2thumbnail import URL2ThumbnailIndex
import test_utils.neontest
import test_utils.redis
import unittest
from utils import imageutils

_log = logging.getLogger(__name__)

class TestURL2ThumbIndex(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        random.seed(198948)

        self.images = [imageutils.PILImageUtils.create_random_image(640, 480) for x in range(5)]

        # Create some simple entries in the account database
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        acct1 = neondata.BrightcovePlatform('acct_1', 'i_1', 'api_1')
        acct1.add_video('v1', 'j1')
        acct1.add_video('v2', 'j2')
        acct1.save()
        v1 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate('api_1', 'v1'), ['t1', 't2'], 
            '', '', 0, 0.0, 1, '')
        v1.save()
        t1 = neondata.ThumbnailIDMapper(
            't1', v1.key,
            neondata.ThumbnailMetaData('t1', ['one.jpg', 'one_cmp.jpg'],
                                       None,None,None,None,None,None))
        t1.save()
        t2 = neondata.ThumbnailIDMapper(
            't2', v1.key,
            neondata.ThumbnailMetaData('t2', ['two.jpg'],
                                       None,None,None,None,None,None))
        t2.save()
        v2 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate('api_1', 'v2'), ['t3'], 
            '', '', 0, 0.0, 1, '')
        v2.save()
        t3 = neondata.ThumbnailIDMapper(
            't3', v2.key,
            neondata.ThumbnailMetaData('t3', ['three.jpg',],
                                       None,None,None,None,None,None))
        t3.save()

        acct2 = neondata.BrightcovePlatform('acct_2', 'i_2', 'api_2')
        acct2.add_video('v3', 'j3')
        acct2.save()
        v3 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate('api_2', 'v3'), ['t1', 't2'], 
            '', '', 0, 0.0, 1, '')
        v3.save()
        t4 = neondata.ThumbnailIDMapper(
             't4', v3.key,
             neondata.ThumbnailMetaData('t4', ['four.jpg'],
                                        None,None,None,None,None,None))
        t4.save()
        
        # Define the mapping that will be mocked out from url to images. 
        self.url2img = {}
        self.get_img_patcher = \
          patch('supportServices.url2thumbnail.utils.http.send_request')
        self.get_img_mock = self.get_img_patcher.start()
        self.get_img_mock.side_effect = \
          lambda x, callback: self.io_loop.add_callback(
              callback,
              self._returnValidImage(x))

    def tearDown(self):
        self.get_img_patcher.stop()
        self.redis.stop()

    def _returnValidImage(self, http_request):
        response = tornado.httpclient.HTTPResponse(request, 200)
        self.url2img[request.url].save(response.buffer, 'JPEG')
        return response

    def setURLImageMapping(self, mapping):
        '''Define the mapping that will be mocked out from url to images.

        mapping is a dictionary from url -> PIL image data (use self.images
        '''
        self.url2img = mapping

    def test_all_different_images(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3]})
        index = URL2ThumbnailIndex()

        
        self.assertEqual(index.get_thumbnail_info('one.jpg').key,
                         't1')
        self.assertEqual(index.get_thumbnail_info('one_cmp.jpg').key,
                         't1')
        self.assertEqual(index.get_thumbnail_info('two.jpg').key,
                         't2')
        self.assertEqual(index.get_thumbnail_info('three.jpg').key,
                         't3')
        self.assertEqual(index.get_thumbnail_info('four.jpg').key,
                         't4')

    

if __name__ == '__main__':
    test_utils.neontest.main()
