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
class TestPILImageUtils(unittest.TestCase):
    def setUp(self):
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

if __name__ == '__main__':
    unittest.main()
