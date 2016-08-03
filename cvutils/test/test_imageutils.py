#!/usr/bin/env python

import os.path
import sys
sys.path.insert(0,  os.path.abspath(
    os.path.join(os.path.dirname(__file__),  '..',  '..')))

import logging
from mock import patch, MagicMock
from PIL import Image
import random
from StringIO import StringIO
import test_utils.neontest
import tornado.ioloop
import tornado.httpclient
import unittest
from cvutils import imageutils

class TestPILImageUtils(unittest.TestCase):
    def setUp(self):
        random.seed(214)
        self.im = imageutils.PILImageUtils.create_random_image(360, 480)

    def _generate_aspect_ratio_tuples(self, ar):
        ''' Generate w,h given aspect ratio  '''
        n = 10
        wmax = 1280
        sizes = []
        for _ in range(n):
            w = random.randrange(0, wmax, 2)
            h = int(float(ar[1])/ar[0] * w) 
            sizes.append((w, h))
        return sizes 

    def test_resize(self):
        '''test common aspect ratio resizes'''

        sizes = [ (270, 480), (480, 640), (90, 120), (100, 100), 
                (480, 360), (120, 90), (1280, 720), (120, 67) ]
        sizes.extend(self._generate_aspect_ratio_tuples((16, 9)))
        sizes.extend(self._generate_aspect_ratio_tuples((4, 3)))
        sizes.extend(self._generate_aspect_ratio_tuples((3, 2)))
        
        for size in sizes:
            im2 = imageutils.PILImageUtils.resize(self.im, size[1], size[0])
            self.assertTrue(im2.size[0], size[0])
            self.assertEqual(im2.size[1], size[1]) 

    def test_resize_by_width(self):
        im2 = imageutils.PILImageUtils.resize(self.im, im_w=640)
        self.assertEqual(im2.size[0], 640)
        self.assertEqual(im2.size[1], 480)
        
    def test_resize_by_height(self):
        im2 = imageutils.PILImageUtils.resize(self.im, im_h=90)
        self.assertEqual(im2.size[0], 120)
        self.assertEqual(im2.size[1], 90)

    def test_resize_no_op(self):
        im2 = imageutils.PILImageUtils.resize(self.im, None, None)
        self.assertEqual(im2.size, (480, 360))
        self.assertEqual(self.im, im2)

    def test_rgba_to_cv(self):
        rgba_im = Image.open(os.path.join(os.path.dirname(__file__),
                                          'alpha_image.png'))
        cv_im = imageutils.PILImageUtils.to_cv(rgba_im)
        #imageutils.PILImageUtils.from_cv(cv_im).show()

class TestDownloadImage(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        random.seed(1654984)
        super(TestDownloadImage, self).setUp()
        self.image = imageutils.PILImageUtils.create_random_image(360, 480)

        self.get_img_patcher = patch('utils.http.send_request')
        self.get_img_mock = self._future_wrap_mock(
            self.get_img_patcher.start())
        self.get_img_mock.side_effect = self._returnValidImage

    def tearDown(self):
        self.get_img_patcher.stop()
        
        super(TestDownloadImage, self).tearDown()

    def _returnValidImage(self, request, **kw):
        response = tornado.httpclient.HTTPResponse(request, 200,
                                                   buffer=StringIO())
        self.image.save(response.buffer, 'JPEG')
        response.buffer.seek(0)
        return response

    def test_get_valid_image(self):

        image = imageutils.PILImageUtils.download_image('url')

        self.assertEqual(self.image.size, image.size)

    def test_connection_error(self):
        self.get_img_mock.side_effect = (
            lambda x, **kw: 
                tornado.httpclient.HTTPResponse(
                    x,
                    200,
                    error = tornado.httpclient.HTTPError(404)))

        with self.assertLogExists(logging.ERROR, 'Error retrieving image'):
            with self.assertRaises(tornado.httpclient.HTTPError):
                imageutils.PILImageUtils.download_image('url')

    @patch('cvutils.imageutils.Image')
    def test_image_ioerror(self, pil_mock):
        pil_mock.open.side_effect = [IOError()]

        
        with self.assertLogExists(logging.ERROR, 'Invalid image at url'):
            with self.assertRaises(IOError):
                imageutils.PILImageUtils.download_image('url')

    
    @patch('cvutils.imageutils.Image')
    def test_image_valueerror(self, pil_mock):
        pil_mock.open.side_effect = [ValueError()]

        with self.assertLogExists(logging.ERROR, 'Invalid image at url'):
            with self.assertRaises(IOError):
                imageutils.PILImageUtils.download_image('url')

    @patch('cvutils.imageutils.Image')
    def test_image_typeerror(self, pil_mock):
        pil_mock.open.side_effect = [TypeError()]

        with self.assertLogExists(logging.ERROR, 'Invalid image at url'):
            with self.assertRaises(IOError):
                imageutils.PILImageUtils.download_image('url')
                
if __name__ == '__main__':
    unittest.main()
