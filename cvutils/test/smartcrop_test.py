#!/usr/bin/env python
''' Unit testing for smart cropping
'''
import cv2
import os.path
import numpy as np
import sys
sys.path.insert(0,  os.path.abspath(
    os.path.join(os.path.dirname(__file__),  '..',  '..')))

import unittest
from cvutils import smartcrop
from cvutils import imageutils
# from model import features
# from model.colorname import JSD

class TestSmartCrop(unittest.TestCase):
    def setUp(self):
        self.im = np.zeros((360, 480, 3), dtype=np.uint8)

    def test_crop_text(self):
        ''' Generate a image with text in the bottom 1/3 of the image.
        Test if the image is cropped correctly with text removed.
        '''
        # text_im here is generated, with current text detection, it doesn't
        # work as well. So instead we will use a pregenerated one.
        text_im = cv2.putText(self.im,
                              'This is a test',
                              (50, 330),
                              cv2.FONT_HERSHEY_PLAIN,
                              3, (255, 255, 255),
                              3, 8)
        
        # text_im = cv2.imread(os.path.join(os.path.dirname(__file__),
        #                                   'test_crop_images/lower_text.jpg'))
        smart_crop = smartcrop.SmartCrop(text_im)
        cropped_im = smart_crop.text_crop(0, 0, text_im.shape[1], text_im.shape[0])
        # Assert to make sure there are less residual pixels. It is allowed to
        # have some.
        self.assertLess(cropped_im.shape[0], text_im.shape[0])
        self.assertLess(np.sum(cropped_im > 128), 10)
    
    def test_face_crop(self):
        face_im = cv2.imread(os.path.join(os.path.dirname(__file__),
                                          'test_crop_images/right_face.jpg'))
        smart_crop = smartcrop.SmartCrop(face_im)
        # Crop horizontally
        cropped_im = smart_crop.crop_and_resize(280, 300)
        faces = smart_crop.detect_faces()
        self.assertEqual(len(faces), 1)
        # Crop vertically
        cropped_im = smart_crop.crop_and_resize(130, 300)
        faces = smart_crop.detect_faces()
        self.assertEqual(len(faces), 1)

    def test_saliency_crop(self):
        saliency_im = cv2.imread(os.path.join(os.path.dirname(__file__),
                                          'test_crop_images/pens_buins.jpg'))
        smart_crop = smartcrop.SmartCrop(saliency_im)
        cropped_im = smart_crop.crop_and_resize(300, 300)
        expected_crop = saliency_im[0:saliency_im.shape[0], \
                                    saliency_im.shape[1] - saliency_im.shape[0] + 1:saliency_im.shape[1]]
        expected_resize = cv2.resize(expected_crop, (300, 300))
                
        gist = features.MemCachedFeatures.create_shared_cache(
                        features.GistGenerator())
        gist_cropped = gist.generate(cropped_im)
        gist_expected = gist.generate(expected_resize)
        self.assertLess(JSD(gist_cropped, gist_expected), 0.01)

    def test_sliding_window_saliency(self):
        ''' Test the saliency cropping algorithm.
        '''
        # create the image asymetrical
        test_im = np.zeros((360, 600, 3), dtype=np.uint8)
        saliency_line = np.append(np.zeros(27), np.append(range(1, 256), np.arange(255, 1, -0.8))).astype(np.uint8)
        saliency_im = np.repeat(np.array([saliency_line]), 360, axis=0)
        smart_crop = smartcrop.SmartCrop(test_im, with_text_detection=False, with_face_detection=False)
        smart_crop._saliency_map = saliency_im
        smart_crop._text_boxes = []
        cropped_im = smart_crop.sliding_window_crop_and_resize(300, 300)
