import unittest
from BadImageFilter import BadImageFilter
from PIL import Image
import numpy as np

class TestBadImageFilter(unittest.TestCase):
    def setUp(self):
        self.filter = BadImageFilter(30, 0.95)

    def test_hard_images_pass(self):
        '''Make sure that difficult real images pass.'''
        self.assertFalse(self.filter.should_filter(
            Image.open('test_images/black_face.jpg')))
        self.assertFalse(self.filter.should_filter(
            Image.open('test_images/soccer.jpg')))
        self.assertFalse(self.filter.should_filter(
            Image.open('test_images/200m.jpg')))
        self.assertFalse(self.filter.should_filter(
            Image.open('test_images/pens_buins.jpg')))

    def test_white_filters(self):
        self.assertTrue(self.filter.should_filter(
            Image.open('test_images/white_logo.jpg')))
        self.assertTrue(self.filter.should_filter(
            Image.open('test_images/white_middle_garbage.jpg')))
        self.assertTrue(self.filter.should_filter(
            Image.open('test_images/ice_surface.jpg')))

    def test_black_filters(self):
        self.assertTrue(self.filter.should_filter(
            Image.open('test_images/black.jpg')))
        self.assertTrue(self.filter.should_filter(
            Image.open('test_images/black_fading_title.jpg')))

    def test_green_screen(self):
        green = np.zeros((200, 300, 3), dtype=np.uint8)
        green[:,:,1] = 255
        self.assertTrue(self.filter.should_filter(green))

    def test_pink_screen(self):
        pink = np.zeros((200, 300, 3), dtype=np.uint8)
        pink[:,:,0] = 255
        pink[:,:,2] = 255
        self.assertTrue(self.filter.should_filter(pink))
if __name__ == '__main__':
    unittest.main()
