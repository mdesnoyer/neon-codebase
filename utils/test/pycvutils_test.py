#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import unittest
import numpy as np
import cv2
from glob import glob

from utils.pycvutils import ImagePrep
from utils import pycvutils
from PIL import Image

dimtol = 1.      # dimension:        tolerance = dimtol
areatol = .01    # area:             tolerance = areatol * max(area1, area2)
asptol = .01     # aspect ratio:     tolerance = asptol * max(asp1, asp2)

TEST_IMAGE = os.path.join(os.path.dirname(__file__), 'im480x360.jpg')
TEST_GRAY_IMAGE = os.path.join(os.path.dirname(__file__), 'grayscale.jpg')

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

def _compute_aspect_ratio(image):
    '''Computes the aspect ratio'''
    oh, ow = _get_dims(image)
    return float(ow) / float(oh)

def _compute_area(image):
    oh, ow = _get_dims(image)
    return oh * ow

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

def produce_rand_config(image, base_prob=0.5):
    '''
    Produces a random configuration for ImagePrep.

    base_prob = the probability that any parameter is defined
    '''
    true_height, true_width = image.shape[:2]
    h = [true_height / 2, true_height * 2]  # width interval
    w = [true_width / 2, true_width * 2]    # height interval
    s_range = [min(h[0], w[0]), max(h[1], w[1])]
    a_range = [h[0]*w[0], h[1]*w[1]]
    pd = dict()
    # max height
    pd['max_height'] = np.random.randint(h[0], h[1])
    pd['max_width'] = np.random.randint(w[0], w[1])
    pd['max_side'] = np.random.randint(s_range[0], s_range[1])
    pd['scale_height'] = np.random.randint(h[0], h[1])
    pd['scale_width'] = np.random.randint(w[0], w[1])
    pd['image_size'] = [np.random.randint(h[0], h[1]),
                        np.random.randint(w[0], w[1])]
    pd['image_area'] = np.random.randint(a_range[0], a_range[1])
    cc_type = np.random.rand()
    grc = lambda: np.random.rand() / 2
    if cc_type < 1./3:
        cf = grc() * 2
    elif cc_type < 2./3:
        cf = [grc(), grc()]
    else:
        cf = [grc(), grc(), grc(), grc()]
    pd['crop_frac'] = cf
    for key in pd.keys():
        if base_prob > np.random.rand():
            pd[key] = None
    if base_prob < np.random.rand():
        pd['convert_to_gray'] = True
    else:
        pd['convert_to_gray'] = False
    if base_prob < np.random.rand():
        pd['convert_to_color'] = True
    else:
        pd['convert_to_color'] = False
    return pd

def run_imageprep_seq(image, config):
    '''
    Runs ImagePrep sequentially by evaluating the arguments in-order
    '''
    op_order = ['convert_to_color', 'convert_to_gray', 'max_height', 'max_width',
                'max_side', 'scale_height', 'scale_width', 'image_size',
                'image_area', 'crop_frac']
    for i in op_order:
        # convert_to_color, if defaulted, will break this test
        # if convert_to_gray is also true (i.e., it will run last 
        # and override convert to gray).
        args = {'convert_to_color': False, i:config.get(i, None)}
        ip = ImagePrep(**args)
        image = ip(image)

    return image

class TestImagePrep(unittest.TestCase):
    '''
    Tests the atomic operations of ImagePrep, to ensure that they are
    valid. Once we know the atomic operations are valid, we can check
    ensembles of operations by performing them sequentially.
    '''
    def setUp(self):
        self._image_cv = cv2.imread(TEST_IMAGE)
        self._image_pil = Image.open(TEST_IMAGE)
        np.random.seed(42)

    @property
    def image_cv(self):
        return np.copy(self._image_cv)

    @property
    def image_pil(self):
        return self._image_pil.copy()

    def test_inputs(self):
        singletons = ['max_height', 'max_width', 'max_side', 'scale_height',
                      'scale_width', 'image_area']
        for i in singletons:
            args = {i:'a'}
            self.assertRaises(ValueError, ImagePrep, **args)
            args = {i:0}
            self.assertRaises(ValueError, ImagePrep, **args)
        # wrong arg types
        self.assertRaises(ValueError, ImagePrep, max_width='a')
        self.assertRaises(ValueError, ImagePrep, crop_frac=['a','b'])
        # too large
        self.assertRaises(ValueError, ImagePrep, crop_frac=1.1)
        self.assertRaises(ValueError, ImagePrep, crop_frac=[1.1,0.5])
        # too small
        self.assertRaises(ValueError, ImagePrep, crop_frac=-0.1)
        self.assertRaises(ValueError, ImagePrep, crop_frac=[-1.1,0.5])
        # wrong length
        self.assertRaises(ValueError, ImagePrep, crop_frac=[0.5, 0.5, 0.5])

    def assertSideIs(self, s1, s2):
        '''Asserts s1 == s2 to a tolerance of dimtol'''
        self.assertAlmostEqual(s1, s2, delta=dimtol)

    def assertSideNotMoreThan(self, s1, s2):
        '''Asserts that s1 <= s2 to a tolerance of dimtol'''
        self.assertLessEqual(s1, s2 + dimtol)

    def assertAspMatch(self, im1, im2):
        '''Asserts that the aspect ratio of two images are equal to a
        tolerance calculated from asptol'''
        asp1 = _compute_aspect_ratio(im1)
        asp2 = _compute_aspect_ratio(im2)
        tolerance = asptol * max(asp1, asp2)
        self.assertAlmostEqual(asp1, asp2, delta=tolerance)

    def assertAspIs(self, im1, asp2):
        '''Asserts that the aspect ratio an image is equal to asp2'''
        asp1 = _compute_aspect_ratio(im1)
        tolerance = asptol * max(asp1, asp2)
        self.assertAlmostEqual(asp1, asp2, delta=tolerance)

    def assertDimMatch(self, im1, dims):
        '''Asserts the dimensions of im1 matches dims to within a tolerance'''
        oh, ow = _get_dims(im1)
        h, w = dims
        self.assertSideIs(oh, h)
        self.assertSideIs(ow, w)

    def assertAreaIs(self, im1, area2):
        '''Asserts that the area of im1 is equal to area2 to within
        tolerance'''
        area1 = _compute_area(im1)
        tolerance = areatol * max(area1, area2)
        self.assertAlmostEqual(area1, area2, delta=tolerance)

    def _test_max_height(self, image, direction='down'):
        oh, ow = _get_dims(image)
        if direction == 'down':
            max_height = oh / 2
        else:
            max_height = oh * 2
        ip = ImagePrep(max_height = max_height)
        new_image = ip(image)
        nh, nw = _get_dims(new_image)
        self.assertSideNotMoreThan(nh, max_height)
        self.assertAspMatch(image, new_image)

    def _test_max_width(self, image, direction='down'):
        oh, ow = _get_dims(image)
        if direction == 'down':
            max_width = ow / 2
        else:
            max_width = ow * 2
        ip = ImagePrep(max_width = max_width)
        new_image = ip(image)
        nh, nw = _get_dims(new_image)
        self.assertSideNotMoreThan(nw, max_width)
        self.assertAspMatch(image, new_image)

    def _test_max_side(self, image, direction='down'):
        oh, ow = _get_dims(image)
        mxs = max(oh, ow)
        if direction == 'down':
            max_side = mxs / 2
        else:
            max_side = mxs * 2
        ip = ImagePrep(max_side = max_side)
        new_image = ip(image)
        nh, nw = _get_dims(new_image)
        nmxs = max(nh, nw)
        self.assertSideNotMoreThan(nmxs, max_side)
        self.assertAspMatch(image, new_image)

    def _test_scale_height(self, image):
        oh, ow = _get_dims(image)
        new_height = np.random.randint(oh/2, oh*2)
        ip = ImagePrep(scale_height=new_height)
        new_image = ip(image)
        nh, nw = _get_dims(new_image)
        self.assertSideIs(nh, new_height)

    def _test_scale_width(self, image):
        oh, ow = _get_dims(image)
        new_width = np.random.randint(ow/2, ow*2)
        ip = ImagePrep(scale_width=new_width)
        new_image = ip(image)
        nh, nw = _get_dims(new_image)
        self.assertSideIs(nw, new_width)

    def _test_image_size(self, image):
        oh, ow = _get_dims(image)
        new_height = np.random.randint(oh/2, oh*2)
        new_width = np.random.randint(ow/2, ow*2)
        ip = ImagePrep(image_size=[new_height, new_width])
        new_image = ip(image)
        nh, nw = _get_dims(new_image)
        self.assertSideIs(nh, new_height)
        self.assertSideIs(nw, new_width)

    def _test_image_area(self, image):
        area = _compute_area(image)
        new_area = np.random.randint(area/2, area*2)
        ip = ImagePrep(image_area=new_area)
        new_image = ip(image)
        self.assertAreaIs(new_image, new_area)
        self.assertAspMatch(new_image, image)

    def _test_convert_to_gray(self, image):
        oh, ow = _get_dims(image)
        ip = ImagePrep(convert_to_gray=True)
        new_image = ip(image)
        nh, nw = _get_dims(new_image)
        self.assertEqual(len(new_image.shape), 2)
        self.assertEqual(oh, nh)
        self.assertEqual(ow, nw)

    def test_max_height(self):
        image = self.image_cv
        self._test_max_height(image, direction='down')
        self._test_max_height(image, direction='up')
        image = self.image_pil
        self._test_max_height(image, direction='down')
        self._test_max_height(image, direction='up')

    def test_max_width(self):
        image = self.image_cv
        self._test_max_width(image, direction='down')
        self._test_max_width(image, direction='up')
        image = self.image_pil
        self._test_max_width(image, direction='down')
        self._test_max_width(image, direction='up')

    def test_max_side(self):
        image = self.image_cv
        self._test_max_side(image, direction='down')
        self._test_max_side(image, direction='up')
        image = self.image_pil
        self._test_max_side(image, direction='down')
        self._test_max_side(image, direction='up')

    def test_scale_height(self):
        image = self.image_cv
        self._test_scale_height(image)
        image = self.image_pil
        self._test_scale_height(image)

    def test_scale_width(self):
        image = self.image_cv
        self._test_scale_width(image)
        image = self.image_pil
        self._test_scale_width(image)

    def test_image_size(self):
        image = self.image_cv
        self._test_image_size(image)
        image = self.image_pil
        self._test_image_size(image)

    def test_image_area(self):
        image = self.image_cv
        self._test_image_area(image)
        image = self.image_pil
        self._test_image_area(image)

    def test_convert_to_gray(self):
        image = self.image_cv
        self._test_convert_to_gray(image)
        image = self.image_pil
        self._test_convert_to_gray(image)

    @unittest.skip("fails for unknown reasosn on video_server machines - NPD 1/22/16")
    def test_cropping(self):
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
        self.assertGreaterEqual(crop_frac, min_frac)
        self.assertGreaterEqual(max_frac, crop_frac)
        min_frac, max_frac = _get_frac(ow, y1, y2)
        self.assertGreaterEqual(crop_frac, min_frac)
        self.assertGreaterEqual(max_frac, crop_frac)

        # test crop type 2
        crop_frac = [np.random.rand()*.5+.5, np.random.rand()*.5+.5]
        ip = ImagePrep(crop_frac=crop_frac)
        new_image = ip(image)
        fnd = _subarray_in_array(image, new_image)
        self.assertTrue(fnd)
        x1, y1, x2, y2 = fnd
        min_frac, max_frac = _get_frac(oh, x1, x2)
        self.assertGreaterEqual(crop_frac[0], min_frac)
        self.assertGreaterEqual(max_frac, crop_frac[0])
        min_frac, max_frac = _get_frac(ow, y1, y2)
        self.assertGreaterEqual(crop_frac[1], min_frac)
        self.assertGreaterEqual(max_frac, crop_frac[1])
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
        self.assertGreaterEqual(1-crop_frac[0], min_frac)
        self.assertGreaterEqual(max_frac, 1-crop_frac[0])
        min_frac, max_frac = _get_frac(ow, 0, y2)
        self.assertGreaterEqual(1-crop_frac[1], min_frac)
        self.assertGreaterEqual(max_frac, 1-crop_frac[1])
        min_frac, max_frac = _get_frac(oh, 0, x2)
        self.assertGreaterEqual(1-crop_frac[2], min_frac)
        self.assertGreaterEqual(max_frac, 1-crop_frac[2])
        min_frac, max_frac = _get_frac(ow, y1, ow)
        self.assertGreaterEqual(1-crop_frac[3], min_frac)
        self.assertGreaterEqual(max_frac, 1-crop_frac[3])

    def test_returnTypes(self):
        pil_img = self.image_pil
        cv_img = self.image_cv
        no_conv_arg = ImagePrep()
        return_same = ImagePrep(return_same=True)
        return_pil = ImagePrep(return_pil=True)
        # no converstion arguments, should return CV
        new_pil = no_conv_arg(pil_img)
        new_cv = no_conv_arg(cv_img)
        self.assertTrue(_is_CV(new_pil))
        self.assertTrue(_is_CV(new_cv))
        # return same image type
        new_pil = return_same(pil_img)
        new_cv = return_same(cv_img)
        self.assertTrue(_is_PIL(new_pil))
        self.assertTrue(_is_CV(new_cv))
        # return PIL images
        new_pil = return_pil(pil_img)
        new_cv = return_pil(cv_img)
        self.assertTrue(_is_PIL(new_pil))
        self.assertTrue(_is_PIL(new_cv))

    '''This completes all atomic tests of ImagePrep. Now, since we know that
    the atomic operations are functional, we can apply an ensemble of
    operations sequentially'''
    def test_ensembleConfig(self):
        np.random.seed(42)
        for i in range(100):
            # run 100 random tests
            config = produce_rand_config(self.image_cv)
            ip = ImagePrep(**config)
            imageSeq = run_imageprep_seq(self.image_cv, config)
            imageEns = ip(self.image_cv)
            self.assertTrue(np.array_equiv(imageSeq, imageEns))

    def test_gray_to_bgr(self):
        image_pil = Image.open(TEST_GRAY_IMAGE)
        ip = ImagePrep()
        image_cv = ip(image_pil)
        self.assertEqual(520, len(image_cv))
        self.assertEqual(400, len(image_cv[0]))
        self.assertEqual(3, len(image_cv[0][0]))
        ip = ImagePrep(convert_to_color=False)
        image_cv = ip(image_pil)
        self.assertEqual(520, len(image_cv))
        self.assertEqual(400, len(image_cv[0]))
        self.assertEqual(33, image_cv[0][0])

class TestResizeAndCrop(unittest.TestCase):
    '''
    Tests the atomic operations of ImagePrep, to ensure that they are
    valid. Once we know the atomic operations are valid, we can check
    ensembles of operations by performing them sequentially.
    '''
    def setUp(self):
        super(TestResizeAndCrop, self).setUp()
        # Test image is 480x360
        self._image_cv = cv2.imread(TEST_IMAGE)
        self._image_pil = Image.open(TEST_IMAGE)
        np.random.seed(42)

    def test_resize_and_crop_preseve_aspect_ratio(self):
        self.assertEquals(
            pycvutils.resize_and_crop(self._image_cv, w=240).shape,
            (180,240,3))
        self.assertEquals(
            pycvutils.resize_and_crop(self._image_cv, h=120).shape,
            (120,160,3))

if __name__=='__main__':
    unittest.main()
