'''
Implements a local binary pattern approach to
finding and detecting closed eyes.
'''

import numpy as np
from glob import glob
import cv2
from skimage import feature
import sklearn
import warnings

def _get_n_bins(n_circ_sym):
    return 2**n_circ_sym

def _get_split_r(arr, n_splt=2, axis=None):
    '''
    Splits an array n_splt times along
    all but the last axis. (Recursive)
    '''
    splts = []
    if axis is None:
        axis = 0
    psplt = np.array_split(arr, 2, axis=axis)
    if axis == (arr.shape[-1] - 1):
        return psplt
    for i in psplt:
        splts += _get_split_r(i, n_splt, axis+1)
    return splts

def _get_split(arr, n_splt=2):
    '''
    Splits an image into n_splt along the first 2 
    dimensions.
    '''
    q = np.array_split(arr, n_splt, axis=0)
    q = [np.array_split(x, n_splt, axis=1) for x in q]
    return q[0] + q[1]

def get_hists(b, bins):
    if type(b) != list:
        b = [b]
    histos = []
    for bs in b:
        histos.append(_get_one_hist(bs, bins))
    return np.array(histos).flatten()

def _get_one_hist(b, bins):
    chist = np.bincount(b.flatten(), minlength=bins)
    return chist

class ScoreEyes(object):
    def __init__(self, clf):
        '''
        Classifies and scores eyes based
        on whether they are open or closed.

        INPUTS:
            clf - a classifier
            the following are defined by the clf:
                n_size - a float, the size of the LBP
                n_circ_sym - number of the LBP divisions to use
                n_splt - how many times to symmetrically
                         subdivide the image
                trim - the number of pixels to trim off the
                         the borders of the returned LBP image.
                scaler - a standard scaler
        '''
        self.clf = clf
        self._n_size = clf.n_size
        self._n_circ_sym = clf.n_circ_sym
        self._n_split = clf.n_splits
        self._trim = clf.trim
        self._resize = clf.img_size
        self.scaler = clf.scaler
        self.f_lbp = lambda x: feature.local_binary_pattern(x, 
                                    self._n_circ_sym, 
                                    self._n_size).astype(int)

    def _get_x_vec(self, img):
        img = cv2.resize(img, (self._resize, self._resize))
        b = self.f_lbp(img)
        b = b[self._trim:-self._trim, self._trim:-self._trim]
        bins = _get_n_bins(self._n_circ_sym)
        q = _get_split(b, n_splt=self._n_split)
        return get_hists(q, bins)

    def classifyScore(self, imgs):
        '''
        Returns the classification and the score
        for an image. A classification of 1 means
        open eyes.
        '''
        if type(imgs) != list:
            imgs = [imgs]
        X = []
        if not len(imgs):
            return [], []
        for img in imgs:
            X.append(self._get_x_vec(img))
        X = np.array(X)
        with warnings.catch_warnings(sklearn.utils.validation.DataConversionWarning):
              X = self.scaler.transform(X)
        scores = self.clf.decision_function(X)
        classif = [x > 0 for x in scores]
        return classif, scores
