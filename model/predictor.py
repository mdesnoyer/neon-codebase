'''Predictory classes for the model

Note that as the code version changes, code may need to be added to
deal with backwards compatibility when pickling/unpickling. See the
Python pickling docs about those issues.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import hashlib
import logging
import numpy as np
import os
import pyflann
import sklearn.base
from sklearn.utils import check_arrays, atleast2d_or_csr
import tempfile
import utils.obj
from . import error

_log = logging.getLogger(__name__)

def save_predictor(predictor, filename):
    '''Save the model to a file.'''
    with open(filename, 'wb') as f:
        pickle.dump(predictor, f, 2)

def load_predictor(filename):
    '''Loads a model from a file.

    Inputs:
    filename - The model to load

    '''
    with open(filename, 'rb') as f:
        return pickle.load(f)

class KFlannPredictor(sklearn.base.BaseEstimator,
                      sklearn.base.RegressorMixin):
    '''Approximate k nearest neighbour using flann.'''
    
    def __init__(self, k=3, target_precision=0.95,
                 seed=802374):
        super(KFlannPredictor, self).__init__()

        self.k = k
        self.target_precision = target_precision
        self.seed = seed

        self.reset()

    def reset(self):
        self.data = None
        self.scores = None
        self.metadata = None
        self.flann = pyflann.FLANN()
        self.params = None

    def __key(self):
        return (super(KFlannPredictor, self).__key(), self.k)

    def __str__(self):
        return utils.obj.full_object_str(self,
                                     exclude=['data', 'scores', 'metadata'])

    def fit(X, y, metadata=None):
        self.data, self.scores = self.data = check_arrays((X, y))
        self.metadata = metadata

        n_examples = self.data.shape[0]
        
        _log.info('Training predictor with %i examples.' % n_examples)

        sample_fraction = 0.20
        if n_examples > 10000:
            sample_fraction=0.05
        self.params = self.flann.build_index(
            self.data,
            algorithm='autotuned',
            target_precision=self.target_precision,
            build_weight=0.01,
            memory_weight=0.5,
            sample_fraction=sample_fraction,
            random_seed=self.seed,
            log_level='warn')
        _log.info('Built index with parameters: %s' % self.params)

    def predict(self, X, exclusion_key=None):
        '''Predict the score for each datapoint.

        Inputs:
        X - A n_example x n_features array
        exclusion_key - If set, the prediction won't include entries where
                        the metadata is equal to the exclusion key

        Returns:
        An generator for the scores.
        '''
        if self.data is None:
            raise error.NotTrainedError()

        X = atleast2d_or_csr(X)

        return (self._predict_one(row, exclusion_key=exclusion_key)
                for row in X)

    def _predict_one(self, x, exclusion_key=None):
        '''Predict the score for a single entry.'''

        if exclusion_key is None or self.metadata is None:
            return self.score_neighbors(self.get_neighbours(x, k=self.k))

        # If we don't want to include images from the same
        # video, we need to ask for extra neighbours. This
        # should only happen in training a higher order predictor/classifier.
        valid_neighbours = []
        k = self.k
        while len(valid_neighbours) < self.k:
            # We didn't get enough entries last time, so exponentially increase
            valid_neighbours = []
            k *= 2

            neighbours = self.get_neighbours(x, k=k)
            valid_neighbours = []
            for neighbour in neighbours:
                if len(valid_neighbours) == self.k:
                    break
            
                if neighbour[2] <> exclusion_key:
                    valid_neighbours.append(neighbour)
                    
        return self.predictor.score_neighbors(valid_neighbours)
        

    def score_neighbours(self, neighbours):
        '''Returns the score for k neighbours.'''
        if neighbours[0][1] < 1e-4:
            # We have an extry that is almost identical, so just use that label
            return neighbours[0][0]
        scores = [x[0] for x in neighbours]
        dists = np.array([1.0/x[1] for x in neighbours])
        return np.dot(scores, dists) / np.sum(dists)

    def get_neighbours(self, x, k=3):
        '''Retrieve N neighbours of an image.

        Inputs:
        x - feature vector for an image
        n - number of neighbours to return

        Outputs:
        Returns a list of [(score, dist, metadata)]

        '''
        idx, dists = self.flann.nn_index(x, k,
                                         checks=self.params['checks'])
        if k == 1:
            # When k=1, the dimensions get squeezed
            return [(self.scores[idx[0]], dists[0],
                     None if self.metadata is None else self.metadata[idx[0]])]
        
        return [(self.scores[i],
                 dist,
                 None if self.metadata is None else self.metadata[i])
                for i, dist in zip(idx[0], dists[0])]

    def __getstate__(self):
        # The flann model can't be pickled directly. the save_model()
        # function must be called which has to write to a file. So, we
        # write to a temporary file and then read that file back and
        # pickle the strings. sigh.
        state = self.__dict__.copy()
        tfile,tfilename = tempfile.mkstemp()
        flann_string = ''
        try:
            os.close(tfile)
            state['flann'].save_index(tfilename)
            with open(tfilename, 'rb') as f:
                flann_string = f.read()
        finally:
                os.unlink(tfilename)
        state['flann'] = flann_string
        return state

    def __setstate__(self, state):
        # Rebuild the flann index using the load_index function
        tfile,tfilename = tempfile.mkstemp()
        new_flann = pyflann.FLANN()
        try:
            os.close(tfile)
            with open(tfilename, 'w+b') as f:
                f.write(state['flann'])
            if len(state['data']) > 0:
                new_flann.load_index(tfilename, np.array(state['data']))
        finally:
            os.unlink(tfilename)
        state['flann'] = new_flann

        self.__dict__ = state
