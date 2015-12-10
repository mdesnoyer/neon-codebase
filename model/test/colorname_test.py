#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import unittest
import numpy as np
import sys
import random
from model.colorname import JSD
from model.colorname import ColorName
from model.video_searcher import VideoSearcher

class TestColorName(unittest.TestCase):
    def setUp(self):
        random.seed(32452)
        

#    def tearDown(self):

    def testJSD(self):
    	print "here"
        zero_array_compare = JSD([0, 0, 0], [0, 0, 0])
        self.assertEquals(zero_array_compare, 0)

        equal_array_compare = JSD([0.1, 0.2, 0.7], [0.1, 0.2, 0.7])
        self.assertEquals(equal_array_compare, 0)

        significant_difference = JSD([1, 0, 0], [0, 1, 0])
        some_difference = JSD([0.5, 0.5, 0], [0, 0.5, 0.5])
        self.assertGreater(significant_difference, some_difference)

    def test_image_to_colorname_color(self):
    	image = np.array([[
    						[0, 0, 0],
    						[255, 0, 0],
    						[63, 102, 127],
    						[127, 127, 127],
    						[0, 255, 0],
    						[0, 19, 255],
    						[255, 127, 255],
    						[255, 0, 255],
    						[0, 0, 255],
    						[255, 255, 255],
    						[0, 255, 255]
    					 ]], dtype = np.uint8)
    	colorname_image = ColorName(image)
    	colorname_name = colorname_image._image_to_colorname()
    	self.assertItemsEqual(colorname_name[0], np.array(range(11)))

    	cn_hist = colorname_image.get_colorname_histogram()
    	self.assertItemsEqual(cn_hist, np.ones(11) * 1.0 / 11.0)

    def test_get_distance(self):
    	image_1 = np.array([[
    						[0, 0, 0],
    						[255, 0, 0],
    						[63, 102, 127],
    						[127, 127, 127],
    						[0, 255, 0],
    						[0, 19, 255],
    						[255, 127, 255],
    						[255, 0, 255],
    						[0, 0, 255],
    						[255, 255, 255],
    						[0, 255, 255]
    					 ]], dtype = np.uint8)

    	# different order
    	image_2 = np.array([[
    						[255, 0, 0],
    						[0, 0, 0],
    						[63, 102, 127],
    						[127, 127, 127],
    						[0, 255, 0],
    						[0, 19, 255],
    						[255, 127, 255],
    						[255, 0, 255],
    						[0, 0, 255],
    						[255, 255, 255],
    						[0, 255, 255]
    					 ]], dtype = np.uint8)
    	self.assertEquals(ColorName.get_distance(image_1, image_2), 0)

    	# different image
    	image_3 = np.array([[
    						[255, 0, 0],
    						[255, 0, 0],
    						[63, 102, 127],
    						[127, 127, 127],
    						[0, 255, 0],
    						[0, 19, 255],
    						[255, 127, 255],
    						[255, 0, 255],
    						[0, 0, 255],
    						[255, 255, 255],
    						[0, 255, 255]
    					 ]], dtype = np.uint8)
    	self.assertGreater(ColorName.get_distance(image_1, image_3), 0)

class TestDedup(unittest.TestCase):
    def test_dedup(self):
        video_searcher = VideoSearcher([])
        image_s1 = cv2.imread(os.path.join(os.path.dirname(__file__),
                                           'test_similar_images/s1.jpg'))
        image_s2 = cv2.imread(os.path.join(os.path.dirname(__file__),
                                          'test_similar_images/s2.jpg'))
        image_d1 = cv2.imread(os.path.join(os.path.dirname(__file__),
                                           'test_similar_images/d1.jpg'))
        self.assertTrue(video_searcher.is_duplicate(image_s1, image_s2))
        self.assertFalse(video_searcher.is_duplicate(image_s1, image_d1))
        self.assertFalse(video_searcher.is_duplicate(image_s2, image_d1))

if __name__ == '__main__':
    unittest.main()
