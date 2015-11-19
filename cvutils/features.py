'''Feature generators


Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import hashlib
import leargist
import logging
import numpy as np
import os
import os.path
import utils.obj
from cvutils import pycvutils
from cvutils.colorname import ColorName

_log = logging.getLogger(__name__)

class FeatureGenerator(object):
    '''Abstract class for the feature generator.'''
    def __init__(self):
        self.__version__ = 2

    def __str__(self):
        return utils.obj.full_object_str(self)

    def reset(self):
        pass

    def generate(self, image):
        '''Creates a feature vector for an image.

        Input:
        image - image to generate features for as a numpy array in BGR
                format (aka OpenCV)

        Returns: 1D numpy feature vector
        '''
        raise NotImplementedError()

    def hash_type(self, hashobj):
        '''Updates a hash object with data about the type.'''
        hashobj.update(self.__class__.__name__)

class PredictedFeatureGenerator(FeatureGenerator):
    '''Wrapper around a Predictor so that it looks like a feature generator.'''
    def __init__(self, predictor):
        super(PredictedFeatureGenerator, self).__init__()
        self.predictor = predictor

    def reset(self):
        self.predictor.reset()

    def generate(self, image):
        return self.predictor.predict(image)

    def hash_type(self, hashobj):
        hashobj.update(self.__class__.__name__)
        self.predictor.hash_type(hashobj)

class GistGenerator(FeatureGenerator):
    '''Class that generates GIST features.'''
    def __init__(self, image_size=(144,256)):
        super(GistGenerator, self).__init__()
        self.image_size = image_size

    def __cmp__(self, other):
        typediff = cmp(self.__class__.__name__, other.__class__.__name__)
        if typediff <> 0:
            return typediff
        return cmp(self.image_size, other.image_size)

    def __hash__(self):
        return hash(self.image_size)

    def generate(self, image):
        # leargist needs a PIL image in RGB format
        rimage = pycvutils.resize_and_crop(image, self.image_size[0],
                                                 self.image_size[1])
        pimage = pycvutils.to_pil(rimage)
        return leargist.color_gist(pimage)

class ColorNameGenerator(FeatureGenerator):
    '''Class that generates ColorName features.'''
    def __init__(self, max_height = 480 ):
        super(ColorNameGenerator, self).__init__()
        self.max_height = max_height

    def __cmp__(self, other):
        typediff = cmp(self.__class__.__name__, other.__class__.__name__)
        if typediff <> 0:
            return typediff
        return cmp(self.max_height, other.max_height)

    def __hash__(self):
        return hash(self.max_height)

    def generate(self, image):
        # leargist needs a PIL image in RGB format
        image_size = (int(2*round(float(image.shape[1]) * 
                                self.max_height / image.shape[0] /2)),
                                self.max_height)
        image_resized = cv2.resize(image, image_size)
        cn = ColorName(image_resized)
        return cn.get_colorname_histogram()


class MemCachedFeatures(FeatureGenerator):
    '''Wrapper for a feature generator that caches the features in memory'''
    _shared_instances = {}
    
    def __init__(self, feature_generator):
        super(MemCachedFeatures, self).__init__()
        self.feature_generator = feature_generator
        self.cache = {}
        self._shared = False

    def __str__(self):
        return utils.obj.full_object_str(self, exclude=['cache'])

    def reset(self):
        self.feature_generator.reset()
        self.cache = {}

    def generate(self, image):
        key = hash(image.tostring())

        try:
            return self.cache[key]
        except KeyError:
            features = self.feature_generator.generate(image)
            self.cache[key] = features
            return features

    def __setstate__(self, state):
        '''If this is a shared cache, register it when unpickling.'''
        self.__dict__.update(state)
        if self._shared:
            MemCachedFeatures._shared_instances[self.feature_generator] = self

    @classmethod
    def create_shared_cache(cls, feature_generator):
        '''Factory function to create an in memory cached that can be shared.

        The shared caches are those that have the same feature_generator
        '''
        try:
            return cls._shared_instances[feature_generator]
        except KeyError:
            instance = MemCachedFeatures(feature_generator)
            instance._shared = True
            cls._shared_instances[feature_generator] = instance
            return instance

        return None
        
class DiskCachedFeatures(FeatureGenerator):
    '''Wrapper for a feature generator that caches the features for images on the disk.

    Images are keyed by their md5 hash.
    '''
    def __init__(self, feature_generator, cache_dir=None):
        '''Create the cached generator.
Inputs:
        feature_generator - the generator to cache features for
        cache_dir - Directory to store the cached features in.
                    If None, becomes an in-memory shared cache.
        
        '''
        super(DiskCachedFeatures, self).__init__()
        self.feature_generator = feature_generator

        if cache_dir is not None and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        self.cache_dir = cache_dir

    @property
    def cache_dir(self):
        return self._cache_dir

    @cache_dir.setter
    def cache_dir(self, cache_dir):
        # When setting cache dir to None, we revert to an in-memory
        # shared class.
        self._cache_dir = cache_dir
        if self._cache_dir == None:
            _log.warning('Using an in memory cache instead of a disk cache.')
            mem_cache = MemCachedFeatures.create_shared_cache(
                self.feature_generator)
            self.feature_generator = mem_cache

    def reset(self):
        self.feature_generator.reset()

    def generate(self, image):
        if self.cache_dir is not None:
            hashobj = hashlib.md5()
            hashobj.update(image.view(np.uint8))
            hashobj.update(str(self.__version__))
            self.feature_generator.hash_type(hashobj)
            hashhex = hashobj.hexdigest()
            cache_file = os.path.join(self.cache_dir, '%s.npy' % hashhex)

            if os.path.exists(cache_file):
                return np.load(cache_file)
            
        features = self.feature_generator.generate(image)

        if self.cache_dir is not None:
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)
            np.save(cache_file, features)

        return features
            

    def __setstate__(self, state):
        '''Extra handling for when this is unpickled.'''
        self.__dict__.update(state)

        # If the cache directory doesn't exist, then turn off caching
        if self.cache_dir is not None and not os.path.exists(self.cache_dir):
            _log.warning('Cache directory %s not found.' % self.cache_dir)
            self.cache_dir = None
