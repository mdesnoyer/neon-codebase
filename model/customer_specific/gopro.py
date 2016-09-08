'''GoPro's custom predictor

Copyright: 2016 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import logging
import numpy as np
import sklearn.mixture

_log = logging.getLogger(__name__)

class GoProPredictor:
    '''A predictor of GoPro likeness.

    We are looking for an estimate of the likelihood that an image is
    a GoPro like profession image.

    '''
    def __init__(self):
        self._gmm = sklearn.mixture.DPGMM()

    def predict(self, X):
        '''Predict the likelihood that the samples in X are GoPro like.

        Inputs:
        X- numpy array, shape = [n_samples, n_features]

        Outputs:
        Y - numpy array n_samples long
        '''
        if X.shape[0] == 1024 and len(X.shape) == 2:
            X = X.T
        elif len(X.shape) == 1:
            X = X.reshape(1, -1)
        probs = self._gmm.predict_proba(X)

        return probs.max(axis=1)
