#!/usr/bin/env python
'''
Testing the imhash_index module

Copyright: 2014 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import glob
from cv.imhash_index import ImHashIndex
import math
import numpy as np
import PIL.Image
from StringIO import StringIO
import unittest
import utils.neon

class TestImHashIndex(unittest.TestCase):
    def setUp(self):
        self.index = ImHashIndex(min_flann_size=16)

    def tearDown(self):
        pass

    def _get_test_image_files(self):
        return glob.glob('%s/images/*.jpg' % os.path.dirname(__file__))

    def test_hash_size(self):
        image = PIL.Image.open(self._get_test_image_files()[0])
        # Make sure it's 64 bit
        self.assertEqual(1<<int(math.ceil(math.log(
            long.bit_length(self.index.hash_pil_image(image)))/math.log(2))),
            64)

    def test_find_same_images(self):
        # Load images into index
        hashes = {}
        for image_fn in self._get_test_image_files():
            full_hash = self.index.hash_pil_image(PIL.Image.open(image_fn))
            hashes[image_fn] = full_hash
            self.index.add_hash(full_hash)

        # Make sure we find them again
        for image_fn in self._get_test_image_files():
            results = self.index.pil_image_radius_search(
                PIL.Image.open(image_fn),
                radius=1)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], hashes[image_fn])
            self.assertEqual(results[0][1], 0)

    def test_build_index_at_once(self):
        hashes = {}
        for image_fn in self._get_test_image_files():
            hashes[image_fn] = self.index.hash_pil_image(
                PIL.Image.open(image_fn))

        self.index.build_index(hashes.itervalues())

        # Make sure we find the images again
        for image_fn in self._get_test_image_files():
            results = self.index.pil_image_radius_search(
                PIL.Image.open(image_fn),
                radius=1)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], hashes[image_fn])
            self.assertEqual(results[0][1], 0)

    def test_jpeg_compression_same(self):
        '''Test that applying different JPEG compression gives the same image.
        '''
        
        # Load up a bunch of images into an index
        hashes = {}
        for image_fn in self._get_test_image_files():
            full_hash = self.index.hash_pil_image(PIL.Image.open(image_fn))
            hashes[image_fn] = full_hash
            self.index.add_hash(full_hash)

        # Now go through them, change the compression ratio and
        # make sure that there is only one entry in the index for
        # each.
        for image_fn in self._get_test_image_files():
            buf = StringIO()
            PIL.Image.open(image_fn).save(buf, format='JPEG', quality=50)
            buf.seek(0)

            cimage = PIL.Image.open(buf)
            results = self.index.pil_image_radius_search(cimage)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], hashes[image_fn])
            self.assertLess(results[0][1], 5)

    def test_hash_conversions(self):
        self.assertEqual(self.index.int_to_binary_array(0x0).size, 64)
        self.assertEqual(0x36ded345346aed00,
                         ImHashIndex.binary_array_to_int(
                             self.index.int_to_binary_array(
                                 0x36ded345346aed00)))
        # Only does the hash length in self.index
        self.assertEqual(0xd345346aed003467,
                         ImHashIndex.binary_array_to_int(
                             self.index.int_to_binary_array(
                                 0x36ded345346aed003467)))

        np.testing.assert_array_equal(
            np.array([[0xf3, 0x62],
                      [0x78, 0xa6]], np.uint8),
            ImHashIndex.binary_array_to_uint8_array(
                np.array([[1,1,1,1,0,0,1,1,0,1,1,0,0,0,1,0],
                          [0,1,1,1,1,0,0,0,1,0,1,0,0,1,1,0]],
                          np.bool)))

        # Make sure the array is flattened
        self.assertEqual(ImHashIndex.binary_array_to_int(
            np.array([[False, True], [True, True]], np.bool)),
            0b0111)

        self.assertEqual(0x36ded345346aed00,
                         ImHashIndex.uint8_row_to_int(ImHashIndex.binary_array_to_uint8_array(np.array([self.index.int_to_binary_array(0x36ded345346aed00)], np.bool))[0]))
        


if __name__ == '__main__':
    utils.neon.InitNeonTest()
    unittest.main()
