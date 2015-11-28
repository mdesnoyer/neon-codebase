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

def check_aspect_ratio_equal(image1, image2):
    '''Verifies that the aspect ratio of image2 is within the range of
    image1'''
    min_asp, max_asp = _compute_aspect_ratio_range(image1)
    obs_asp = _compute_aspect_ratio(image2)
    return ((obs_asp >= min_asp) and (obs_asp <= max_asp))

def check_is_grayscale(image):
    if len(image.shape) <= 2:
        return True
    if image.shape[2] <= 1:
        return True
    return False

def _subarray_in_array(a, suba):
    targ = suba.flatten()[0]
    if not targ in a:
        return None
    srchrgn = a
    for n, z in enumerate(suba.shape[:-1]):
        srchrgn = srchrgn.take(np.arange(0,a.shape[n]-z), axis=n)
    fnds = np.nonzero(srchrgn[...,0] == targ)
    for nn, fnd in enumerate(zip(*fnds)):
        coords = [np.arange(z1, z1+z2) for z1, z2 in zip(fnd, suba.shape)]
        obt = a
        for n, z in enumerate(coords):
            obt = obt.take(z, axis=n)
        if np.array_equiv(suba, obt):
            x1 = fnd[0]
            y1 = fnd[1]
            x2 = fnd[0] + suba.shape[0]
            y2 = fnd[1] + suba.shape[1]
            return (x1, y1, x2, y2)
    return None

class TestImagePrep(unittest.TestCase):
    '''
    Tests the atomic operations of ImagePrep, to ensure that they are
    valid. Once we know the atomic operations are valid, we can check
    ensembles of operations by performing them sequentially.
    '''
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
        max_height = oh * 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))
        # now check a small height
        max_height = oh / 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_height_at_most(new_image, max_height))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_max_height_pil(self):
        # first: make sure that a very large desired height does not
        # change anything.
        image = self.image_pil
        oh, ow = _get_dims(image)
        max_height = oh * 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))
        # now check a small height
        max_height = oh / 2
        ip = ImagePrep(max_height=max_height)
        new_image = ip(image)
        self.assertTrue(check_height_at_most(new_image, max_height))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_max_width_cv(self):
        # first: make sure that a very large desired height does not
        # change anything.
        image = self.image_cv
        oh, ow = _get_dims(image)
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))
        # now check a small height
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_width_at_most(new_image, max_width))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_max_width_pil(self):
        # first: make sure that a very large desired height does not
        # change anything.
        image = self.image_pil
        oh, ow = _get_dims(image)
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_dims_is(new_image, [oh, ow]))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))
        # now check a small height
        max_width = ow * 2
        ip = ImagePrep(max_width=max_width)
        new_image = ip(image)
        self.assertTrue(check_width_at_most(new_image, max_width))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_max_side_cv(self):
        image = self.image_cv
        oh, ow = _get_dims(image)
        max_side = np.min([oh, ow])
        ip = ImagePrep(max_side=max_side)
        new_image = ip(image)
        self.assertTrue(max(_get_dims(new_image)) == max_side)
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_max_side_pil(self):
        image = self.image_pil
        oh, ow = _get_dims(image)
        max_side = np.min([oh, ow])
        ip = ImagePrep(max_side=max_side)
        new_image = ip(image)
        self.assertTrue(max(_get_dims(new_image)) == max_side)
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_scale_height_cv(self):
        image = self.image_cv
        oh, ow = _get_dims(image)
        new_height = np.random.randint(oh/2, oh*2)
        ip = ImagePrep(scale_height=new_height)
        new_image = ip(image)
        self.assertTrue(check_height_is(new_image, new_height))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_scale_height_pil(self):
        image = self.image_pil
        oh, ow = _get_dims(image)
        new_height = np.random.randint(oh/2, oh*2)
        ip = ImagePrep(scale_height=new_height)
        new_image = ip(image)
        self.assertTrue(check_height_is(new_image, new_height))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_scale_width_cv(self):
        image = self.image_cv
        oh, ow = _get_dims(image)
        new_width = np.random.randint(ow/2, ow*2)
        ip = ImagePrep(scale_width=new_width)
        new_image = ip(image)
        self.assertTrue(check_width_is(new_image, new_width))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    def test_scale_width_pil(self):
        image = self.image_pil
        oh, ow = _get_dims(image)
        new_width = np.random.randint(ow/2, ow*2)
        ip = ImagePrep(scale_width=new_width)
        new_image = ip(image)
        self.assertTrue(check_width_is(new_image, new_width))
        self.assertTrue(check_aspect_ratio_equal(image, new_image))

    # if all of these have passed, we assume that the CV --> PIL
    # conversion is working properly, and only use CV images for the
    # remainder of the format-invariant tests.
    def test_image_size(self):
        image = self.image_cv
        oh, ow = _get_dims(image)
        new_height = np.random.randint(oh/2, oh*2)
        new_width = np.random.randint(ow/2, ow*2)
        ip = ImagePrep(image_size=[new_height, new_width])
        new_image = ip(image)
        self.assertTrue(check_width_is(new_image, new_width))
        self.assertTrue(check_height_is(new_image, new_height))

    def test_image_area(self):
        image = self.image_cv
        area = _compute_area(image)
        new_area = np.random.randint(area/2, area*2)
        ip = ImagePrep(image_area=new_area)
        new_image = ip(image)
        min_area, max_area = _compute_area_range(new_image)
        self.assertTrue(new_area >= min_area)
        self.assertTrue(new_area <= max_area)

    def test_convert_to_gray(self):
        image = self.image_cv
        oh, ow = _get_dims(image)
        ip = ImagePrep(convert_to_gray=True)
        new_image = ip(image)
        self.assertTrue(check_is_grayscale(new_image))
        self.assertTrue(check_width_is(new_image, ow))
        self.assertTrue(check_height_is(new_image, oh))

    def test_cropping(self):
        _log.debug('Testing cropping')
        '''Attempts to validate the cropping.'''
        image = self.image_cv
        oh, ow = _get_dims(image)
        def _get_frac(orig, new1, new2):
            return (float(abs((new2-dimtol)-(new1+dimtol)))/orig,
                    float(abs((new2+dimtol)-(new1-dimtol)))/orig)
        # test crop type one
        crop_frac = np.random.rand()*.5+.5 #(crop no more than 50%)
        ip = ImagePrep(crop_frac=crop_frac)
        new_image = ip(image)
        fnd = _subarray_in_array(image, new_image)
        self.assertTrue(fnd)
        x1, y1, x2, y2 = fnd
        min_frac, max_frac = _get_frac(oh, x1, x2)
        self.assertTrue((crop_frac >= min_frac) and (crop_frac <= max_frac))
        min_frac, max_frac = _get_frac(ow, y1, y2)
        self.assertTrue((crop_frac >= min_frac) and (crop_frac <= max_frac))
        # test crop type 2
        crop_frac = [np.random.rand()*.5+.5, np.random.rand()*.5+.5]
        ip = ImagePrep(crop_frac=crop_frac)
        new_image = ip(image)
        fnd = _subarray_in_array(image, new_image)
        self.assertTrue(fnd)
        x1, y1, x2, y2 = fnd
        min_frac, max_frac = _get_frac(oh, x1, x2)
        self.assertTrue((crop_frac[0] >= min_frac) and
                        (crop_frac[0] <= max_frac))
        min_frac, max_frac = _get_frac(ow, y1, y2)
        self.assertTrue((crop_frac[1] >= min_frac) and
                        (crop_frac[1] <= max_frac))
        # test crop type 3
        crop_frac = [np.random.rand()*.5, np.random.rand()*.5,
                     np.random.rand()*.5, np.random.rand()*.5]
        ip = ImagePrep(crop_frac=crop_frac)
        new_image = ip(image)
        fnd = _subarray_in_array(image, new_image)
        self.assertTrue(fnd)
        x1, y1, x2, y2 = fnd
        min_frac, max_frac = _get_frac(oh, x1, oh)
        self.assertTrue((1.-crop_frac[0] >= min_frac) and
                        (1.-crop_frac[0] <= max_frac))
        min_frac, max_frac = _get_frac(ow, 0, y2)
        self.assertTrue((1.-crop_frac[1] >= min_frac) and
                        (1.-crop_frac[1] <= max_frac))
        min_frac, max_frac = _get_frac(oh, 0, x2)
        self.assertTrue((1.-crop_frac[2] >= min_frac) and
                        (1.-crop_frac[2] <= max_frac))
        min_frac, max_frac = _get_frac(ow, y1, ow)
        self.assertTrue((1.-crop_frac[3] >= min_frac) and
                        (1.-crop_frac[3] <= max_frac))















