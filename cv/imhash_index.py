'''
An index of image hashes that can be used to identify duplicates.

A good radius for finding duplicates is 5

Copyright: 2014 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import imagehash
import logging
import math
import numpy as np
import pyflann

_log = logging.getLogger(__name__)

class ImHashIndex:
    def __init__(self, hashtype='dhash', hash_size=64, rebuild_threshold=1.5,
                 min_flann_size=256):
        '''
        Inputs:
        hashtype - Hash algorithm to use. One of 'ahash', 'dhash', 'phash'
        hash_size - Size of the hash value in bits
        rebuild_threshold - How often to rebuild the index. 
                            2 is when the size doubles.
        min_flann_size - Minimum number of entries before using flann to be
                         fast
        '''
        self.flann = pyflann.FLANN(
            algorithm='lsh',
            table_number_=12,
            key_size_=20,
            multi_probe_level_=2)
        self.flann.set_distance_type("hamming")
        self.params = None
        hashfuncs = {
            'ahash' : imagehash.average_hash,
            'phash' : imagehash.phash,
            'dhash' : imagehash.dhash
            }

        self.hashfunc = hashfuncs[hashtype]
        self.hash_size = 64 if hashtype == 'phash' else hash_size
        self.rebuild_threshold = rebuild_threshold
        self.min_flann_size = min_flann_size

        # Matrix of byte rows in the flann index
        self.flann_index = None

        # Matrix of boolean vectors that we do the radius search on directly.
        # Has not been incorporated into the flann index yet.
        self.overflow_hash_index = None

        

    def add_pil_image(self, image):
        '''Add a PIL image to the index.'''
        self.add_hash(self.hash_pil_image(image))

    def build_index(self, hashvals):
        '''Build an index from a sequence of hash values.

        This is more efficient than just calling add_hash many
        times. Anything that was entered in this index before it was
        built is thrown out.

        Input:
        hashvals - list or generator of hash integers
        '''
        self.flann_index = self.binary_array_to_uint8_array(np.array(
            [self.int_to_binary_array(h) for h in hashvals],
            np.bool))

        self.params = self.flann.build_index(self.flann_index)        
        

    def add_hash(self, hashval):
        '''Add an image hash integer to the index.'''

        if self.overflow_hash_index is None:
            self.overflow_hash_index = np.array(
                [self.int_to_binary_array(hashval)], np.bool)
        else:
            self.overflow_hash_index = \
              np.vstack((self.overflow_hash_index,
                         [self.int_to_binary_array(hashval)]))

        if self.overflow_hash_index.shape[0] < self.min_flann_size:
            return

        if self.flann_index is not None and (
                (float(self.overflow_hash_index.shape[0]) /
                 self.flann_index.shape[0]) <
                (self.rebuild_threshold - 1)):
            return

        # Build the new flann index
        if self.flann_index is None:
            self.flann_index = \
              self.binary_array_to_uint8_array(self.overflow_hash_index)
        else:
            self.flann_index = np.vstack((
                self.flann_index,
                self.binary_array_to_uint8_array(self.overflow_hash_index)))
        self.params = self.flann.build_index(
            self.flann_index)
        self.overflow_hash_index = None
                

    def radius_search(self, hashval, radius=5):
        '''Return all the (hash, dist) < radius from the hashval.'''
        query = self.int_to_binary_array(hashval)

        results = {} # hash -> dist

        # Find entries in the overflow index
        if self.overflow_hash_index is not None:
            # Calculate the hamming distance
            o_dists = np.sum(np.bitwise_xor(self.overflow_hash_index, query),
                             axis=1)
            idx = np.nonzero(o_dists < radius)[0]
            dists = o_dists[idx]
            hashes = [
                self.binary_array_to_int(self.overflow_hash_index[i,:]) for
                i in idx]
            results = dict(zip(hashes, dists))
            

        # Find entries in the flann index
        if self.params is not None:
            # Normally, we'd do a radius search, but the flann radius
            # search looks broken for lsh indicies. So we're going to
            # do a nn search and check the resulting distances.
            idx, f_dists = self.flann.nn_index(
                self.binary_array_to_uint8_array(query),
                min(5, self.flann_index.shape[0]))
            valid_idx = np.nonzero(f_dists < radius)
            idx = idx[valid_idx]
            dists = f_dists[valid_idx]
            hashes = [
                self.uint8_row_to_int(self.flann_index[i,:]) for
                i in idx]
            results.update(dict(zip(hashes, dists)))
            
        return sorted(results.iteritems(), key=lambda x: x[1])

    def pil_image_radius_search(self, image, radius=5):
        '''Returns all the (hash, dist) < radius from the PIL image.'''
        return self.radius_search(self.hash_pil_image(image), radius)

    def hash_pil_image(self, image):
        '''Returns the hash integer of a PIL image.'''
        return self.binary_array_to_int(
            self.hashfunc(image,
                          hash_size=int(math.sqrt(self.hash_size))).hash)

    def int_to_binary_array(self, val):
        '''Converts an integer to a binary numpy array.

        Note, the bit order is actually reversed.
        '''
        return np.array([val & (1L<<i) for i in 
                         range(self.hash_size-1, -1, -1)],
                        dtype=np.bool)

    @classmethod
    def binary_array_to_int(cls, arr):
        '''Converts a binary numpy array to int.

        Note, the bit order is actually reversed.
        '''
        return sum([1L<<i for i,v in enumerate(arr.flatten()[::-1]) if v])

    @classmethod
    def binary_array_to_uint8_array(cls, arr):
        '''Converts a binary 2D array to a unit8 one.

        In the unit8 one, each row is a list of uint8 values.
        '''
        return np.packbits(arr.astype(np.int), axis=-1)

    @classmethod
    def uint8_row_to_int(cls, arr):
        '''Converts a uint8 row to an int.'''
        return sum([long(v)<<(8*i) for i,v in enumerate(arr.flatten()[::-1])])

    
        
