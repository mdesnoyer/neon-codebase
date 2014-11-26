'''Feature generators

We try to fit to the scikit-learn framework, except that there is a
fundamental problem with that library for image problems. It assumes
that your input data is all available in raw form in a gigantic
array. For images, this is unrealistic for reasonable sized
datasets. You need to generate your features one image at a time and
then concatenate. So, the feature generators only generate for one
image at a time. You need to concanate the results yourself (or use
the ImageFileTransformer)

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
from . import error
import hashlib
import leargist
import logging
import numpy as np
import os
import os.path
import sklearn.base
from sklearn.cross_validation import check_cv
from sklearn.utils import check_arrays
from . import TextDetectionPy
import utils.obj
import utils.pycvutils

_log = logging.getLogger(__name__)

class FeatureGenerator(sklearn.base.BaseEstimator):
    '''Abstract class for the feature generator.

    Only generates for one image at a time.
    '''
    def __init__(self):
        super(FeatureGenerator, self).__init__()
        self.__version__ = 2

    def __key(self):
        return (type(self), self.__version__)

    def __cmp__(self, other):
        return cmp(self.__key(), other.__key())

    def __str__(self):
        return utils.obj.full_object_str(self)

    def __hash__(self):
        return hash(self.__key())

    def reset(self):
        '''Reset any state in the generator.'''
        pass

    def fit(self, X, y=None):
        '''Do nothing

        Used to make the class compatible with sklearn's API
        '''
        return self

    def transform(self, image):
        '''Transforms the image into a feature vector.

        Input:
        image - image to generate features for as a numpy array in BGR
                format (aka OpenCV)

        Returns: 1D numpy feature vector
        '''
        raise NotImplementedError()

class ImageFileTransformer(sklearn.base.BaseEstimator,
                           sklearn.TransformerMixin):
    '''Converts a list of image file names into a feature matrix.'''
    def __init__(self, generator):
        super(ImageFileTransformer, self).__init__()
        self.generator = generator

    def __key(self):
        return (super(ImageFileTransformer, self).__key(), self.genrator)

    def transform_iterator(self, image_list):
        for image_fn in image_list:
            cur_image = cv2.imread(image_fn)
            if cur_image is None:
                _log.error('Could not open %s' % image_fn)
                continue
            yield self.generator.transform(cur_image)

    def transform(self, image_list):
        '''Transforms an iterable image list into a feature matrix

        Inputs:
        image_list - Iterator of image filenames

        Ouputs:
        Matrix of (nImages, nFeatures) 
        '''
        return np.vstack((x for x in self.transform_iterator(image_list)))    

class GistGenerator(FeatureGenerator):
    '''Class that generates GIST features.'''
    def __init__(self, image_size=(144,256)):
        super(GistGenerator, self).__init__()
        self.image_size = image_size

    def __key(self):
        return (super(GistGenerator, self).__key(), self.image_size)

    def transform(self, image):
        # leargist needs a PIL image in RGB format
        rimage = utils.pycvutils.resize_and_crop(image, self.image_size[0],
                                                 self.image_size[1])
        pimage = utils.pycvutils.to_pil(rimage)
        return leargist.color_gist(pimage)

class PredictedFeatureGenerator(FeatureGenerator):
    '''Wrapper around an sklearn predictor so that it looks like a
    feature generator.

    Inputs:
    predictor - An object that implements predict()
    feature_generator - A feature generator
    name - Name for this generator so we can distinguish it
    '''
    def __init__(self, predictor, feature_generator, name=''):
        super(PredictedFeatureGenerator, self).__init__()
        self.predictor = predictor
        self.feature_generator = feature_generator
        self.name = name

    def __key(self):
        return (super(PredictedFeatureGenerator, self).__key(),
                type(self.predictor),
                self.feature_generator,
                self.name)

    def reset(self):
        if hasattr(self.predictor, 'reset'):
            self.predictor.reset()
        self.feature_generator.reset()

    def fit(self, X, y=None):
        '''We can't fit this data because raw images won't be available.'''
        raise NotImplementedError('Please train the predictor separately')

    def transform(self, image, exclusion_key=None):
        return self.predictor.predict(self.feature_generator.transform(image),
                                      exclusion_key)

class UnionFeatures(FeatureGenerator):
    '''A union of feature vectors appended into a single large vector.'''
    def __init__(self, generators):
        super(UnionFeatures, self).__init__()
        self.generators = generators
         # An array equal to the large vector size where each entry is
         # equal to the index of which generator created it.
        self.owners = None

    def __key(self):
        return (super(UnionFeatures, self).__key(), self.generators)
    
    def transform(self, image):
        vecs = [x.transform(image) for x in self.generators]

        # Figure out which entries correspond to which generator
        if self.owners is None:            
            owners = [np.zeros(vec[idx].shape) + idx
                      for idx in range(len(vecs))]
            self.owners = np.vstack(owners)

        return np.vstack(vecs)
        

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

    def __key(self):
        return (super(MemCachedFeatures, self).__key(),
                self.feature_generator)

    def reset(self):
        self.feature_generator.reset()
        self.cache = {}

    def transform(self, image):
        key = hash(image.tostring())

        try:
            return self.cache[key]
        except KeyError:
            features = self.feature_generator.transform(image)
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

        The shared caches are those that have the same
        feature_generator. The feature generator must implement the
        __hash__ and __cmp__ functions.
        
        '''
        try:
            return cls._shared_instances[feature_generator]
        except KeyError:
            instance = MemCachedFeatures(feature_generator)
            instance._shared = True
            cls._shared_instances[feature_generator] = instance
            return instance

        return None

class SingleCacheFeature(FeatureGenerator):
    '''Wrapper for a feature generator that caches the most recent result.

    Useful if you're going to use the same features in multiple
    predictors/classifiers because the feature will only be calculated
    once per image.

    To use properly, use the create_shared_cache function.
    '''
    _shared_instaces = {}

    def __init__(self, feature_generator):
        super(SingleCacheFeature, self).__init__()
        self.feature_generator = feature_generator
        self.last_key = None
        self.last_result = None
        self._shared = False

    def __str__(self):
        return utils.full_object_str(self, exclude=['last_key', 'last_result'])

    def __key(self):
        return (super(SingleCacheFeature, self).__key(),
                self.feature_generator)

    def reset(self):
        self.feature_generator.reset()
        self.last_key = None
        self.last_result = None

    def transform(self, image):
        key = hash(image.tostring())
        if key == self.last_key:
            return self.last_result
        self.last_result = self.feature_generator.transform(image)
        self.last_key = key
        return key

    def __setstate__(self, state):
        '''If this is a shared cache, register it when unpickling.'''
        self.__dict__.update(state)
        if self._shared:
            SingleCacheFeature._shared_instances[self.feature_generator] = self

    @classmethod
    def create_shared_cache(cls, feature_generator):
        '''Factory function to create an in memory cached that can be shared.

        The shared caches are those that have the same
        feature_generator. The feature generator must implement the
        __hash__ and __cmp__ functions.
        
        '''
        try:
            return cls._shared_instances[feature_generator]
        except KeyError:
            instance = SingleCacheFeature(feature_generator)
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

    def __key(self):
        return (super(DiskCachedFeatures, self).__key(),
                self.feature_generator)

    def reset(self):
        self.feature_generator.reset()

    def transform(self, image):
        if self.cache_dir is not None:
            hashobj = hashlib.md5()
            hashobj.update(image.view(np.uint8))
            hashobj.update(str(self.__version__))
            hashobj.update(str(hash(self.feature_generator)))
            hashhex = hashobj.hexdigest()
            cache_file = os.path.join(self.cache_dir, '%s.npy' % hashhex)

            if os.path.exists(cache_file):
                return np.load(cache_file)
            
        features = self.feature_generator.transform(image)

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



class GeneratorSelectionCV(FeatureGenerator)
    '''These routines filter generators (not features).

    Needs a special approach because each generator produces a
    potentially large feature vector and we want to avoid calculating
    the entire feature vector.

    Done using cross validation and recursive feature elimination.

    Inputs:
    generators - List of FeatureGenerators
    estimator - An estimator that is being trained
    score_func - Scoring function that takes f(estimator, X, Y) (high is good)
    importance_func - Function of the form f(estimator) that returns a
                      vector of the imporance of each feature
                      (once estimator is trained)
    cv - Cross validation object. See sklearn.cross_validation.
         Defaults to 3-fold.
    step - Number of generators to take out at each step
    '''

    def __init__(self, generators,
                 estimator,
                 score_func,
                 importance_func=lambda x: x.coefs_
                 cv=None,
                 step=1):
        super(UnionFeatureSelection, self).__init__()
        self.generators = generators
        self.estimator = estimator
        self.score_func = score_func
        self.cv = cv
        self.importance_func = importance_func
        self.step=step

        # subclasses should generate a UnionFeatures object after fitting
        self.union_features = None 

    def __key(self):
        return (super(GeneratorSelection, self).__key(),
                self.generators,
                type(self.estimator),
                type(self.score_func),
                type(self.importance_func),
                type(self.cv))

    def fit(self, X, Y):
        '''Choose the feature generators based on the data.

        X - Image iteratrable
        Y - Labels for each image
        '''
        X, Y = check_arrays(X, Y)
        cv = check_cv(self.cv, X, Y, is_classifier(self.estimator))

        scores = (np.ones((len(self.generators)), dtype=np.float32) * 
                  float('-inf'))

        for n, (train, test) in enumerate(cv):
            X_train, X_test = X[train], X[test]
            Y_train, Y_test = Y[train], Y[test]

            cur_estimator = sklearn.base.clone(self.estimator)
            cur_estimator.fit(X_train, Y_train)

            #TODO(mdesnoyer) Finish this

    def transform(self, image):
        '''Converts an image into the smaller feature vector.'''
        if self.union_features is None:
            raise error.NotTrainedError()

        return self.union_features.transform(image)

    def predict(self, image):
        '''Predicts the value for a single image.'''        
        return self.estimator.predict(self.transform(image))

class TreeGeneratorSelection(GeneratorSelection):
    '''Selects the generators for tree based predictors.

    A tree based predictor is one that has the feature_importances_ property.
    '''

    def __init__(self, generators, estimator):
        super(TreeGeneratorSelection).__init__(generators,
                                               estimator)

    def fit(self, X, Y):
        X, Y = check_arrays(X, Y)
