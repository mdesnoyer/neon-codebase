'''
Implements a local binary pattern approach to
finding and detecting closed eyes.
'''

import numpy as np
from glob import glob
import cv2
import os
from skimage import feature
from sklearn import linear_model
import dlib
import cPickle

def _get_n_bins(n_circ_sym):
    return 2**n_circ_sym

def _get_split_r(arr, n_splt=2, axis=None):
    '''
    Splits an array n_splt times along
    all but the last axis. (Recursive)
    '''
    splts = []
    if axis == None:
        axis = 0
    psplt = np.array_split(arr, 2, axis=axis)
    if axis == (arr.shape[-1] - 1):
        return psplt
    for i in psplt:
        splts += _get_split_r(i, n_splt, axis+1)

def _get_split(arr, n_splt=2):
    '''
    Splits an image into n_splt along the first 2 
    dimensions.
    '''
    q = np.array_split(b, n_splt, axis=0)
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

class ScoreEyes(object):
    def __init__(self, clf, scaler, n_size=3., 
                 n_circ_sym=3, n_splt=2, trim=4):
        '''
        Classifies and scores eyes based
        on whether they are open or closed.

        INPUTS:
            clf - a classifier
            n_size - a float, the size of the LBP
            n_circ_sym - number of the LBP divisions to use
            n_splt - how many times to symmetrically
                     subdivide the image
            trim - the number of pixels to trim off the
                     the borders of the returned LBP image.
        '''
        self.n_size = n_size
        self.n_circ_sym = n_circ_sym
        self.clf = clf
        self.scaler = scaler
        self.n_splt = n_splt
        self.trim = trim
        self.f_lbp = lambda x: feature.local_binary_pattern(x, 
                                    n_circ_sym, n_size).astype(int)

    def _get_x_vec(self, img):
        img = cv2.resize(img, (32, 32))
        b = self.f_lbp(img)
        b = b[self.trim:-self.trim, self.trim:-self.trim]
        bins = _get_n_bins(self.n_circ_sym)
        q = get_splits(b)
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
        for img in imgs:
            X.append(self._get_x_vec(img))
        X = p.array(X)
        X = self.scaler.transform(X)
        scores = self.clf.decision_function(X)
        classif = [x > 0 for x in scores]
        return zip(classif, scores)
