#!/usr/bin/env python

import os.path
import sys
sys.path.insert(0,  os.path.abspath(
    os.path.join(os.path.dirname(__file__),  '..',  '..')))

import Image
import random
import unittest
from utils import imageutils

random.seed(214)
class TestImageUtils(unittest.TestCase):
    def setUp(self):
        self.im = self._create_random_image(480,  360)

    def _create_random_image(self, h, w):
        ''' Create a random image '''

        pixels = [(0, 0, 0) for _w in range(h*w)] 
        r = random.randrange(0, 255)
        g = random.randrange(0, 255)
        b = random.randrange(0, 255)
        pixels[0] = (r, g, b)
        im = Image.new("RGB", (h, w))
        im.putdata(pixels)
        return im

    def test_crop_and_resize(self):
        ''' Crop and resize test '''
        sizes = [ (270, 480), (480, 640), (90, 120), (100, 100)] #(w, h) 
        
        for size in sizes:
            im2 = imageutils.ImageUtils.resize(self.im, size[0], size[1])
            self.assertTrue(im2.size, (size[1], size[0]))

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
        ''' #test common aspect ratios'''

        sizes = [ (270, 480), (480, 640), (90, 120), (100, 100), 
                (480, 360), (120, 90), (1280, 720), (120, 67) ]
        sizes.extend(self._generate_aspect_ratio_tuples((16, 9)))
        sizes.extend(self._generate_aspect_ratio_tuples((4, 3)))
        sizes.extend(self._generate_aspect_ratio_tuples((3, 2)))
        
        for size in sizes:
            im2 = imageutils.ImageUtils.resize(self.im, size[1], size[0])
            self.assertTrue(im2.size[0], size[0])
            self.assertTrue(im2.size[1], size[1])                    

        #empty height
        im2 = imageutils.ImageUtils.resize(self.im, None, 640)
        self.assertTrue(im2.size[0], 640)
        self.assertTrue(im2.size[1], 480)
        
        #empty width
        im2 = imageutils.ImageUtils.resize(self.im, 90, None)
        self.assertTrue(im2.size[0], 120)
        self.assertTrue(im2.size[1], 90)
        
        #empty width
        im2 = imageutils.ImageUtils.resize(self.im, None, None)
        self.assertEqual(self.im, im2)

if __name__ == '__main__':
    unittest.main()
