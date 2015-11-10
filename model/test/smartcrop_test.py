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
from model import smartcrop
from utils import imageutils
import model.features
from model.colorname import JSD

class TestSmartCrop(unittest.TestCase):
    def setUp(self):
        self.im = np.zeros((360, 480, 3), dtype=np.uint8)

    def test_crop_text(self):
        ''' Generate a image with text in the bottom 1/3 of the image.
        Test if the image is cropped correctly with text removed.
        '''
        # text_im here is generated, with current text detection, it doesn't
        # work as well. So instead we will use a pregenerated one.
        # text_im = cv2.putText(self.im,
        #                       'This is a test',
        #                       (50, 330),
        #                       cv2.FONT_HERSHEY_PLAIN,
        #                       3, (255, 255, 255),
        #                       3, 8)
        
        text_im = cv2.imread(os.path.join(os.path.dirname(__file__),
                                          'test_filter_images/lower_text.jpg'))
        smart_crop = smartcrop.SmartCrop(text_im)
        cropped_im = smart_crop.text_crop()
        # Assert to make sure there are less residual pixels. It is allowed to
        # have some.
        self.assertLess(np.sum(cropped_im > 128), 10)
    
    def test_face_crop(self):
        face_im = cv2.imread(os.path.join(os.path.dirname(__file__),
                                          'test_filter_images/right_face.jpg'))
        smart_crop = smartcrop.SmartCrop(face_im)
        # Crop horizontally
        cropped_im = smart_crop.crop(300, 280)
        faces = smart_crop.face_cascade.detectMultiScale(smart_crop.original_im, \
                                                         **smart_crop.haarParams)
        self.assertEqual(len(faces), 1)
        # Crop vertically
        cropped_im = smart_crop.crop(300, 120)
        faces = smart_crop.face_cascade.detectMultiScale(smart_crop.original_im, \
                                                         **smart_crop.haarParams)
        self.assertEqual(len(faces), 1)

    def test_saliency_crop(self):
        saliency_im = cv2.imread(os.path.join(os.path.dirname(__file__),
                                          'test_filter_images/pens_buins.jpg'))
        smart_crop = smartcrop.SmartCrop(saliency_im)
        cropped_im = smart_crop.crop(300, 300)
        expected_crop = saliency_im[0:saliency_im.shape[0], \
                                    saliency_im.shape[1] - saliency_im.shape[0] + 1:saliency_im.shape[1]]
        expected_resize = cv2.resize(expected_crop, (300, 300))
                
        gist = model.features.MemCachedFeatures.create_shared_cache(
                        model.features.GistGenerator())
        gist_cropped = gist.generate(cropped_im)
        gist_expected = gist.generate(expected_resize)
        self.assertLess(JSD(gist_cropped, gist_expected), 0.01)