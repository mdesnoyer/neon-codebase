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
import PIL.Image
import random
from StringIO import StringIO
from supportServices.url2thumbnail import URL2ThumbnailIndex
import test_utils.neontest
import test_utils.redis
import tornado.httpclient
import tornado.ioloop
import unittest
from utils import imageutils

_log = logging.getLogger(__name__)

class TestURL2ThumbIndex(test_utils.neontest.AsyncTestCase):    
    def setUp(self):
        super(TestURL2ThumbIndex, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        
        random.seed(198948)

        self.images = [imageutils.PILImageUtils.create_random_image(640, 480) for x in range(5)]

        # Create some simple entries in the account database
        acct1 = neondata.BrightcovePlatform('acct1', 'i1', 'api1')
        acct1.add_video('v1', 'j1')
        acct1.add_video('v2', 'j2')
        acct1.save()
        v1 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate('api1', 'v1'), ['t1', 't2'], 
            '', '', 0, 0.0, 1, '')
        v1.save()
        t1 = neondata.ThumbnailMetadata(
            't1', v1.key, ['one.jpg', 'one_cmp.jpg'],
            None, None, None, None, None, None, enabled=True)
        t1.save()
        neondata.ThumbnailURLMapper('one.jpg', 't1').save()
        neondata.ThumbnailURLMapper('one_cmp.jpg', 't1').save()
        t2 = neondata.ThumbnailMetadata(
            't2', v1.key,  ['two.jpg'],
            None, None, None, None, None, None)
        t2.phash = self._get_phash_of_jpegimage(self.images[1])
        t2.save()
        neondata.ThumbnailURLMapper('two.jpg', 't2').save()
        v2 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate('api1', 'v2'), ['t3'], 
            '', '', 0, 0.0, 1, '')
        v2.save()
        t3 = neondata.ThumbnailMetadata(
            't3', v2.key, ['three.jpg',],
            None, None, None, None, None, None)
        t3.save()
        neondata.ThumbnailURLMapper('three.jpg', 't3').save()

        acct2 = neondata.BrightcovePlatform('acct2', 'i2', 'api2')
        acct2.add_video('v3', 'j3')
        acct2.save()
        v3 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate('api2', 'v3'), ['t4'], 
            '', '', 0, 0.0, 1, '')
        v3.save()
        t4 = neondata.ThumbnailMetadata(
             't4', v3.key, ['four.jpg'],
             None, None, None, None, None, None)
        t4.save()
        neondata.ThumbnailURLMapper('four.jpg', 't4').save()
        
        # Define the mapping that will be mocked out from url to images. 
        self.url2img = {}
        self.get_img_patcher = \
          patch('supportServices.url2thumbnail.utils.http.send_request')
        self.get_img_mock = self.get_img_patcher.start()
        self.get_img_mock.side_effect = self._returnImageCallback

    def tearDown(self):
        self.get_img_patcher.stop()
        self.redis.stop()
        
        super(TestURL2ThumbIndex, self).tearDown()

    def _returnImageCallback(self, request, callback=None):
        image = self._returnValidImage(request)
        if callback:
            self.io_loop.add_callback(callback, image)
        else:
            return image

    def _returnValidImage(self, request):
        response = tornado.httpclient.HTTPResponse(request, 200,
                                                   buffer=StringIO())
        self.url2img[request.url].save(response.buffer, 'JPEG')
        response.buffer.seek(0)
        return response

    def _get_phash_of_jpegimage(self, image):
        # needed because random images don't JPEG compress well
        buf = StringIO()
        image.save(buf, format='JPEG')
        buf.seek(0)

        cimage = PIL.Image.open(buf)
        return URL2ThumbnailIndex().hash_index.hash_pil_image(cimage)

    def setURLImageMapping(self, mapping):
        '''Define the mapping that will be mocked out from url to images.

        mapping is a dictionary from url -> PIL image data (use self.images
        '''
        self.url2img = mapping

    def test_all_known_image_urls(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3]})
        index = URL2ThumbnailIndex()

        self.assertEqual(index.get_thumbnail_info('one.jpg').key, 't1')
        self.assertEqual(index.get_thumbnail_info('one_cmp.jpg').key,
                         't1')
        self.assertEqual(index.get_thumbnail_info('two.jpg').key, 't2')
        self.assertEqual(index.get_thumbnail_info('three.jpg').key, 't3')
        self.assertEqual(index.get_thumbnail_info('four.jpg').key, 't4')

    def test_external_process_changes_thumb_info(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3]})
        index = URL2ThumbnailIndex()

        orig_thumb = index.get_thumbnail_info('one.jpg')
        self.assertTrue(orig_thumb.enabled)
        self.assertEqual(orig_thumb.key, 't1')

        to_mod = neondata.ThumbnailMetadata.get('t1')
        to_mod.enabled = False
        to_mod.save()

        mod_thumb = index.get_thumbnail_info('one.jpg')
        self.assertFalse(mod_thumb.enabled)
        self.assertEqual(mod_thumb.key, 't1')
        

    def test_async_ability(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3],
                                 'unknown.jpg' : self.images[1]})
        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        index.get_thumbnail_info('three.jpg', callback=self.stop)
        self.assertEqual(self.wait().key, 't3')
        index.get_thumbnail_info('four.jpg', callback=self.stop)
        self.assertEqual(self.wait().key, 't4')
        index.get_thumbnail_info('unknown.jpg', callback=self.stop)
        thumb_info = self.wait()
        self.assertEqual(thumb_info.key, 't2')
        self.assertIn('unknown.jpg', thumb_info.urls)

        # Make sure that the new url was recorded
        self.assertIn('unknown.jpg', neondata.ThumbnailMetadata.get('t2').urls)


    def test_unknown_url_of_same_image(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3],
                                 'unknown.jpg' : self.images[1]})
        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()


        thumb_info = index.get_thumbnail_info('unknown.jpg')
        self.assertEqual(thumb_info.key, 't2')
        self.assertIn('unknown.jpg', thumb_info.urls)

        # Make sure that the new url was recorded
        self.assertIn('unknown.jpg', neondata.ThumbnailMetadata.get('t2').urls)

    def test_unknown_url_of_unknown_image(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3],
                                 'unknown.jpg' : self.images[4]})
        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        self.assertIsNone(index.get_thumbnail_info('unknown.jpg'))

    def test_add_known_thumbnail(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3]})
        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        db_thumb = neondata.ThumbnailMetadata.get('t2')
        self.assertIsNotNone(db_thumb.phash)
        index.add_thumbnail_to_index(db_thumb)

        thumb_info = index.get_thumbnail_info('two.jpg')
        self.assertEqual(thumb_info.key, 't2')
        self.assertEqual(thumb_info.phash, db_thumb.phash)

    def test_add_new_thumbnail(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3],
                                 'five.jpg' : self.images[4],
                                 'five_cmp.jpg' : self.images[4]})
        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        v3 = neondata.ThumbnailMetadata.get(
            neondata.InternalVideoID.generate('api2', 'v3'))
        t5 = neondata.ThumbnailMetadata(
             't5', v3.key, ['five.jpg'],
             None, None, None, None, None, None)
        index.add_thumbnail_to_index(t5)
        index.add_thumbnail_to_index(t5)
        v2 = neondata.ThumbnailMetadata.get(
            neondata.InternalVideoID.generate('api1', 'v2'))
        t6 = neondata.ThumbnailMetadata(
             't6', v2.key, ['five_cmp.jpg'],
             None, None, None, None, None, None)
        t6.phash = self._get_phash_of_jpegimage(self.images[4])
        t6.save()
        index.add_thumbnail_to_index(t6)

        # The same image is used in different videos, so we don't know
        # which one to choose.
        self.assertIsNone(index.get_thumbnail_info('five.jpg'))
        self.assertIsNone(neondata.ThumbnailURLMapper.get_id('five.jpg'))
        self.assertIsNone(neondata.ThumbnailURLMapper.get_id('five_cmp.jpg'))

        # Now we limit to the specific video so that we can find it
        self.assertEqual('t5', 
                         index.get_thumbnail_info('five.jpg',
                                                  internal_video_id=v3.key).key)
        self.assertEqual(neondata.ThumbnailURLMapper.get_id('five.jpg'), 't5')
        self.assertIsNone(neondata.ThumbnailURLMapper.get_id('five_cmp.jpg'))
        
        self.assertEqual('t6', 
                         index.get_thumbnail_info('five_cmp.jpg',
                                                  internal_video_id=v2.key).key)
        self.assertIsNotNone(neondata.ThumbnailMetadata.get('t6').phash)
        self.assertEqual(neondata.ThumbnailURLMapper.get_id('five.jpg'), 't5')
        self.assertEqual(neondata.ThumbnailURLMapper.get_id('five_cmp.jpg'),
                         't6')

    def test_same_image_in_different_account(self):
        v3 = neondata.ThumbnailMetadata.get(
            neondata.InternalVideoID.generate('api2', 'v3'))
        v3.thumbnail_ids.append('t5')
        v3.save()
        t5 = neondata.ThumbnailMetadata(
             't5', v3.key, ['five.jpg'],
             None, None, None, None, None, None)
        t5.save()
        
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3],
                                 'five.jpg' : self.images[0]})

        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        self.assertIsNone(index.get_thumbnail_info('five.jpg'))
        self.assertEqual(index.get_thumbnail_info('five.jpg',
                                                  account_api_key='api2').key,
                                                  't5')
        index.get_thumbnail_info('five.jpg', callback=self.stop,
                                 account_api_key='api1')
        self.assertIsNone(self.wait())
        self.assertEqual(
            index.get_thumbnail_info('one.jpg', account_api_key='api1').key,
            't1')
        self.assertIsNone(
            index.get_thumbnail_info(
            'five.jpg',
            internal_video_id=neondata.InternalVideoID.generate('api1',
                                                                'v1')))
        self.assertEqual(
            't1',
            index.get_thumbnail_info(
            'one.jpg',
            internal_video_id=neondata.InternalVideoID.generate('api1',
                                                                'v1')).key)
        self.assertEqual(
            't5',
            index.get_thumbnail_info('five.jpg',
                                     internal_video_id=v3.key).key)

    def test_same_image_in_different_video(self):
        v2 = neondata.ThumbnailMetadata.get(
            neondata.InternalVideoID.generate('api1', 'v2'))
        v2.thumbnail_ids.append('t5')
        v2.save()
        t5 = neondata.ThumbnailMetadata(
             't5', v2.key, ['five.jpg'],
             None, None, None, None, None, None)
        t5.save()

        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3],
                                 'five.jpg' : self.images[0]})

        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        self.assertIsNone(index.get_thumbnail_info('five.jpg'))
        self.assertIsNone(index.get_thumbnail_info('five.jpg',
                                                   account_api_key='api1'))
        self.assertEqual(
            't5',
            index.get_thumbnail_info('five.jpg', internal_video_id=v2.key).key)
        self.assertIsNone(
            index.get_thumbnail_info(
                'five.jpg',
                internal_video_id=neondata.InternalVideoID.generate(
                    'api1', 'v1')))
        self.assertEqual(
            't1',
            index.get_thumbnail_info(
                'one.jpg',
                internal_video_id=neondata.InternalVideoID.generate(
                    'api1', 'v1')).key)
    def test_thumbnail_metadata_missing(self):
        neondata.ThumbnailURLMapper('unknown.jpg', 'no_tid').save()
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3]})
        
        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        with self.assertLogExists(
                logging.ERROR,
                'Could not find thumbnail information for id: no_tid'):
            index.get_thumbnail_info('unknown.jpg')

    def test_image_not_available(self):
        self.setURLImageMapping({'one.jpg' : self.images[0],
                                 'one_cmp.jpg' : self.images[0],
                                 'two.jpg' : self.images[1],
                                 'three.jpg' : self.images[2],
                                 'four.jpg' : self.images[3]})
        index = URL2ThumbnailIndex()
        index.build_index_from_neondata()

        def return_error(x, callback=None):
            callback(tornado.httpclient.HTTPResponse(
                x, 200, error=tornado.httpclient.HTTPError(404)))

        self.get_img_mock.side_effect = return_error

        with self.assertLogExists(
                logging.ERROR,
                'Error retrieving image from: unknown.jpg'):
            index.get_thumbnail_info('unknown.jpg')
        

    

if __name__ == '__main__':
    test_utils.neontest.main()
