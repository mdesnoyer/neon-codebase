'''
Exports an intelligent search method using Monte-Carlo
Metropolis-Hastings search.
'''
import logging
import os
import sys
import time
import traceback
import numpy as np
from bisect import *

_log = logging.getLogger(__name__)

class ThumbnailResultObject(object):
    '''
    A thumbnail result object, which stores
    - its position in the video (frameno)
    - its score
    - whether or not it was filtered
    - its filter scores (as a dict)
    '''
    def __init__(self, frameno, score=None):
        self.frameno = frameno
        self._score = score

    def set_score(self, score):
        self._score = score

    def get_score(self):
        return self._score

    def __cmp__(self, other):
        if hasattr(other, 'frameno'):
            # i.e., you are comparing against
            # another result object
            other = other.frameno
        return cmp(self.frameno, other)

class MonteCarloMetropolisHastings(object):
    '''
    A generic searching algorithm that
    samples from the distribution of the
    scores in accordance with the algorithm's
    belief in the viability of that region.

    Supports asynchronous updating (i.e., it
    is possible to draw sequential samples without
    updating the object's knowledge about the 
    distribution)

    Note that this will has a min_dist for sampling,
    such that if the next largest sampled thumbnail
    by frameno is closer than min_dist, it will not
    draw that sample. 
    '''

    def __init__(self, min_dist=64,
                 base_sample_prob=0.3):
        '''
        min_dist : the minimum proximity that will
                   trigger a search
        base_sample_prob : the base probability that
                   a thumbnail will be searched.
        '''
        self.N = 0
        self.results = []
        self.max_score = 0.
        self.n_samples = 0
        self.tot_score = 0.
        self.mean = 1.
        self.min_dist = min_dist
        self.base_sample_prob = base_sample_prob
        self.max_interval = (self.N, 0, self.N)

    def start(self, elements):
        '''
        Re-initializes the search. 

        elements : the maximum number of elements
                   over which we will search.
        '''
        self.N = elements
        self.results = []
        self.max_score = 0.
        self.n_samples = 0
        self.tot_score = 0.
        self.mean = 1.

    def update(self, frameno, score):
        '''
        Updates the knowledge of the algorithm.
        '''
        res_obj = self._get_result(frameno)
        if res_obj == None:
            # then we're updating a frame that does
            # not contain a result object, so instantiate
            # one
            res_obj = ThumbnailResultObject(frameno)
            self._insert_result(res_obj)
        res_obj.set_score(score)
        self.max_score = max(self.max_score, score)
        self.tot_score += score
        self.n_samples += 1
        self.mean = self.tot_score / self.n_samples

    def get(self):
        '''Gets the next sample'''
        # ensure that we haven't already taken every sample
        # possible
        resPerFrame = self.n_samples * 1./ self.N
        maxResPerFrame = (self.N / (1. * self.min_dist)) - 2
        if resPerFrame >= maxResPerFrame:
            return None
        attempts = 0
        while attempts < 200:
            sample = np.random.choice(self.N)
            if self._get_result(sample) == sample:
                # reject if it's already been sampled
                continue
            attempts += 1
            acc = self._accept_sample(sample)
            if acc:
                res_obj = ThumbnailResultObject(frameno)
                self._insert_result(res_obj)
                return sample
        res_obj = ThumbnailResultObject(frameno)
        self._insert_result(res_obj)
        return sample

    def _compute_max_interval(self):
        fframeno = 0
        max_interval = 0
        for r in self.results:
            int_size = r.frameno - (fframeno + 1)
            if int_size > max_interval:
                max_interval = int_size
                start = fframeno
                stop = r.frameno
            fframeno = r.frameno
        self.max_interval = (max_interval, start, stop - 1)

    def _update_max_interval(self, sample):
        '''
        Determine if it's necessary to recompute
        the maximum interval, and then recomputes it
        as (interval, start, stop). A sample falls
        within the maximum interval if it's between
        the start and stop indices. 

        Note: sample must be a frameno!
        '''
        if sample < self.max_interval[1]:
            return
        if sample >= self.max_interval[2]:
            return
        self._compute_max_interval()


    def _get_result(self, x):
        '''
        Obtains a result value given its frameno.
        '''
        i = bisect_left(self.results, x)
        if i != len(self.results) and self.results[i] == x:
            return self.results[i]
        return None

    def _create_result(self, frameno):
        res_obj = ThumbnailResultObject(frameno)
        self._insert_result(res_obj)

    def _insert_result(self, result):
        if not hasattr(result, 'frameno'):
            raise ValueError('Not a result object!')
        insort(self.results, result)
        self._update_max_interval(result.frameno)

    def _find_lt(self, x):
        'Find rightmost value less than x'
        i = bisect_left(self.results, x)
        if i:
            return self.results[i-1]
        return ThumbnailResultObject(-1, score=self.mean)

    def _find_gt(self, x):
        'Find leftmost value greater than x'
        i = bisect_right(self.results, x)
        if i != len(self.results):
            return self.results[i]
        return ThumbnailResultObject(self.N, score=self.mean)

    def _bounds(self, target):
        '''
        returns the immediate neighbors of target
        '''
        lt = self._find_lt(target)
        gt = self._find_gt(target)
        return (lt, gt)
        
    def _accept_sample(self, sample):
        '''
        Returns true or false if the sample
        is to be accepted.
        '''
        if not self.max_score:
            # accept if the max score is unknown
            return True
        lt, gt = self._bounds(sample)
        pred_inp = [[lt.frameno, lt.get_score()],
                    [gt.frameno, gt.get_score()]]
        pred_score = self._predict_score(
                        pred_inp, sample)
        score_prob = (pred_score / self.max_score)**0.5
        rdiff = gt.frameno - sample
        dist_thresh = self.min_dist
        if self.max_interval[0] <= self.min_dist:
            # wow, you've been going for a while! start
            # sampling more aggressively, and stop caring
            # about if it's too far away.
            dist_thresh = self.max_interval[0] / 2
        if rdiff < dist_thresh:
            # the next largest sample is too close
            return False
        if lt.frameno == -1:
            ldiff = np.inf
        else:
            ldiff = sample - lt.frameno
        min_dist = min(ldiff, rdiff)
        dist_prob = (min_dist * 1. / dist_thresh)**0.5
        thresh = self.base_sample_prob + dist_prob * score_prob
        accept = np.random.rand() < thresh
        return accept
        
    def _predict_score(self, neighbs, sample):
        '''
        Predicts the score of a sample given
        its neighbors. Currently only supports
        nearest neighbor on both sides.
        '''
        [x1, y1], [x2, y2] = neighbs
        x3 = sample
        m = float(y2 - y1) / float(x2 - x1)
        return m * (x3 - x1) + y1
