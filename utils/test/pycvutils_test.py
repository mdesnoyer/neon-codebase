#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import unittest
import numpy as np
import cv2
from glob import glob

from utils.pycvutils import ImagePrep
from PIL import Image

dimtol = 1.     # dimension tolerance
areatol = 1.    # area tolerance
asptol = 1.     # aspect ratio tolerance

TEST_IMAGE = os.path.join(os.path.dirname(__file__), 'im480x360.jpg')

def _is_CV(image):
    return type(image).__module__ == np.__name__

def _is_PIL(image):
    return not _is_CV(image)

def _get_array(image):
    '''Returns a copy of the image as an array''' 
    if _is_PIL(image):
        return image.toarray()
    else:
        return np.copy(image)

def _get_dims(image):
    if _is_PIL(image):
        h = image.size[1]
        w = image.size[0]
    else:
        h, w = image.shape[:2]
    return h, w

def _check_dim(image, dims, at_most=False):
    '''Checks the dimensions of an image. dims is an array of type [h, w].
    If h or w is <= 0, then it ignores that dimension. If at_most is true, 
    then it checks that the dimensions are at most h and w.'''
    oh, ow = _get_dims(image)
    h, w = dims
    hdiff = (h - oh) * (h > 0)
    wdiff = (w - ow) * (w > 0)
    if at_most:
        return (((hdiff + dimtol) >= 0) and ((wdiff + dimtol) >= 0))
    else:
        return ((abs(hdiff) <= dimtol) and (abs(wdiff) <= dimtol))

def _compute_aspect_ratio(image):
    '''Computes the aspect ratio'''
    oh, ow = _get_dims(image)
    return float(ow) / float(oh)

def _compute_aspect_ratio_range(image):
    oh, ow = _get_dims(image)
    max_asp = float(ow + asptol) / float(oh - asptol)
    min_asp = float(ow - asptol) / float(oh + asptol)
    return (min_asp, max_asp)

def _compute_area(image):
    oh, ow = _get_dims(image)
    return oh * ow

def _compute_area_range(image):
    oh, ow = _get_dims(image)
    max_area = (ow + asptol) * (oh + asptol)
    min_area = (ow - asptol) * (oh - asptol)
    return (min_area, max_area)

def check_width_is(image, x):
    return _check_dim(image, [0, x])

def check_width_at_most(image, x):
    return _check_dim(image, [0, x], True)

def check_height_is(image, x):
    return _check_dim(image, [x, 0])

def check_height_at_most(image, x):
    return _check_dim(image, [x, 0], True)

def check_dims_is(image, x):
    return _check_dim(image, x)

def check_dims_at_most(image, x):
    return _check_dim(image, x, True)

def check_aspect_ratio_is(image, min_asp, max_asp):
    '''Verifies that the aspect ratio is within allowable range'''
    obs_asp = _compute_aspect_ratio(image)
    return ((obs_asp >= min_asp) and (obs_asp <= max_asp))

def check_area_is(image, min_area, max_area):
    '''Verifies that the area is within allowable range'''
    obs_area = _compute_area(image)
    return ((obs_area >= min_area) and (obs_area <= max_area))

def check_is_grayscale(image, x):
    if len(image.shape) <= 2:
        return True
    if image.shape[2] <= 1:
        return True
    return False

class TestImagePrep(unittest.TestCase):
    def setUp(self):
        self._image_cv = cv2.imread(TEST_IMAGE)
        self._image_pil = Image.open(TEST_IMAGE)

    @property
    def image_cv(self):
        return np.copy(self._image_cv)

    @property
    def image_pil(self):
        return self._image_pil.copy()

    def test_max_height_cv(self):
        # first: make sure that a very large desired height does not
        # change anything.
        image = self.image_cv
        oh, ow = _get_dims(image)
        asp = _compute_aspect_ratio(image)
        max_height = oh * 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))
        # now check a small height
        max_height = oh / 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_height_at_most(new_image, max_height))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))

    def test_max_height_pil(self):
        # first: make sure that a very large desired height does not
        # change anything.
        image = self.image_pil
        oh, ow = _get_dims(image)
        asp = _compute_aspect_ratio(image)
        max_height = oh * 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))
        # now check a small height
        max_height = oh / 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_height_at_most(new_image, max_height))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))

    def test_max_width_cv(self):
        # first: make sure that a very large desired height does not
        # change anything.
        image = self.image_cv
        oh, ow = _get_dims(image)
        asp = _compute_aspect_ratio(image)
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))
        # now check a small height
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_width_at_most(new_image, max_width))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))

    def test_max_width_pil(self):
        # first: make sure that a very large desired height does not
        # change anything.
        image = self.image_pil
        oh, ow = _get_dims(image)
        asp = _compute_aspect_ratio(image)
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))
        # now check a small height
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_width_at_most(new_image, max_width))
        self.assertTrue(check_aspect_ratio_is(new_image, asp))

    def test_max_side_cv(self):
        image = self.image_cv
        oh, ow = _get_dims(image)
        asp = _compute_aspect_ratio(image)
        max_side = np.min([oh, ow])
        ip = ImagePrep(max_side=max_side)
        new_image = ip(image)
        self.assertTrue(max(_get_dims(new_image)) == max_side)

    def test_max_side_pil(self):
        image = self.image_pil
        oh, ow = _get_dims(image)
        asp = _compute_aspect_ratio(image)
        max_side = np.min([oh, ow])
        ip = ImagePrep(max_side=max_side)
        new_image = ip(image)
        self.assertTrue(max(_get_dims(new_image)) == max_side)