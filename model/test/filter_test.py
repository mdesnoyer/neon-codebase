#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import unittest
import model.filters
import numpy as np
import cv2

IMG_DIR = os.path.join(os.path.dirname(__file__), 'test_filter_images')

class TestUniformColorFilter(unittest.TestCase):
    def setUp(self):
        self.filter = model.filters.UniformColorFilter(30, 0.95)

    def test_hard_images_pass(self):
        '''Make sure that difficult real images pass.'''
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'black_face.jpg'))))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'soccer.jpg'))))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, '200m.jpg'))))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'pens_buins.jpg'))))

    def test_white_filters(self):
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'white_logo.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'white_middle_garbage.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'ice_surface.jpg'))))

    def test_black_filters(self):
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'black.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'black_fading_title.jpg'))))

    def test_green_screen(self):
        green = np.zeros((200, 300, 3), dtype=np.uint8)
        green[:,:,1] = 255
        self.assertFalse(self.filter.accept(green))

    def test_pink_screen(self):
        pink = np.zeros((200, 300, 3), dtype=np.uint8)
        pink[:,:,0] = 255
        pink[:,:,2] = 255
        self.assertFalse(self.filter.accept(pink))

class TestTextFilter(unittest.TestCase):
    def setUp(self):
        self.filter = model.filters.TextFilter(0.025)

    def test_natural_images_no_text(self):
        #self.assertTrue(self.filter.accept(
        #    cv2.imread("/data/neon/imdb/staging/images/8pMJhXpcqFoxeEh5djLYFc.jpg")))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, '200m.jpg'))))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'pens_buins.jpg'))))
        #self.assertTrue(self.filter.accept(
        #    cv2.imread(os.path.join(IMG_DIR, 'black_face.jpg'))))

    def test_natural_images_with_text(self):
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'ctv_scroller.png'))))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'has_ticker.jpg'))))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'podium.jpg'))))
        self.assertTrue(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'cbs_newsroom.jpg'))))
        #self.assertTrue(self.filter.accept(
        #    cv2.imread(os.path.join(IMG_DIR, 'title_in_background.jpg'))))

    #@unittest.skip("Skipping because these examples are too hard for the algorithm now.")
    def test_title_cards(self):
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'stack_title.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'stack_title2.jpg'))))
        #self.assertFalse(self.filter.accept(
        #    cv2.imread(os.path.join(IMG_DIR, 'stack_title4.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'splash_title.jpg'))))
        #self.assertFalse(self.filter.accept(
        #    cv2.imread(os.path.join(IMG_DIR, 'small_white_text.jpg'))))
        #self.assertFalse(self.filter.accept(
        #    cv2.imread(os.path.join(IMG_DIR, 'angled_title_card.jpg'))))
        #self.assertFalse(self.filter.accept(
        #    cv2.imread(os.path.join(IMG_DIR, 'white_text_faded_background.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'slide.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'transition_slide.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'text_and_pics.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'pink_back_white_text.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'sim_city_title_card.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'blue_text_white_back.jpg'))))
        #self.assertFalse(self.filter.accept(
        #    cv2.imread(os.path.join(IMG_DIR, 'hgtv.jpg'))))
        self.assertFalse(self.filter.accept(
            cv2.imread(os.path.join(IMG_DIR, 'text_and_pics.jpg'))))
        
if __name__ == '__main__':
    unittest.main()
