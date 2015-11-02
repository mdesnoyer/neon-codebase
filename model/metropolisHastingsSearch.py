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
    - its position in the search frame sequence
    - its score (if defined)
    - a pointer to the next frame (if defined)
    - a pointer to the prev frame (if defined)
    - a pointer to the next gap frame before
      a gap in the analysis
    '''
    def __init__(self, frameno, score=None,
                 next_frame=None, prev_frame=None,
                 dummy_frame=False, next_gap=None):
        self.frameno = frameno
        self.score = score
        # next frame won't induce a memory leak, right?
        if next_gap == None:
            next_gap = self
        self.next_frame = next_frame
        self.prev_frame = prev_frame
        self.dummy_frame = dummy_frame
        self.next_gap = next_gap
        # indicates whether the interval starting with
        # this frame has been searched.
        self.lead_int_srchd = False

    def _can_search(self):
        '''
        Checks to see if ether the forward or backward
        interval can be searched. If the forward interval
        can be searched, it is assumed that it *is* searched,
        and sets lead_int_srchd to True. If the backward
        interval can be searched, it does the same but for
        the previous frame. 

        If either can be searched, it returns 
        (startFrame, endFrame, startScore, endScore) else
        it returns False.
        '''
        fwd = self._check_fwd()
        if fwd:
            return fwd
        bck = self._check_bck()
        if bck:
            return bck

    def _check_fwd(self):
        '''
        Replicates can_search, but for the foward interval
        '''
        sf = self.frameno
        ef = self.next_frame.frameno
        if abs(sf - ef) != 1:
            return False
        if self.score == None:
            return False
        if self.lead_int_srchd:
            return False
        if self.next_frame.score == None:
            return False
        if self.next_frame.dummy_frame:
            return False
        sfs = self.score
        efs = self.next_frame.score
        self.lead_int_srchd = True
        return (sf, ef, sfs, efs)

    def _check_bck(self):
        '''
        _check_fwd, only for the 'backward' interval
        '''
        ef = self.frameno
        sf = self.prev_frame.frameno
        if abs(sf - ef) != 1:
            return False
        if self.score == None:
            return False
        if self.prev_frame.lead_int_srchd:
            return False
        if self.prev_frame.score == None:
            return False
        if self.prev_frame.dummy_frame:
            return False
        efs = self.score
        sfs = self.prev_frame.score
        self.prev_frame.lead_int_srchd = True
        return (sf, ef, sfs, efs)

    def __cmp__(self, other):
        if hasattr(other, 'frameno'):
            # i.e., you are comparing against
            # another result object
            other = other.frameno
        return cmp(self.frameno, other)

    def __repr__(self):
        if self.prev_frame:
            pf = self.prev_frame.frameno
        else:
            pf = -(np.inf)
        if self.next_frame:
            nf = self.next_frame.frameno
        else:
            nf = np.inf
        pf = str(pf)
        nf = str(nf)
        return ("Result Object at %i "
                "between %s and %s")%(
                self.frameno, pf, nf)

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

    def __init__(self, search_interval=64,
                 base_sample_prob=0.1, explore_coef=0.):
        '''
        min_dist : the search interval. This won't actually
                   sample from the frames individually, but
                   rather from 'search frames', which are
                   equally spaced frames, each separated by
                   precisely min_dist frames.
        base_sample_prob : the base probability that
                   a thumbnail will be searched.
        explore_coef : a value between 0 and 1, the degree
                   to which the algorithm will favor exploration
                   over exploitation.
        '''
        self.N = None
        self.tot = None
        self.buffer = None
        self.results = []
        self.max_score = 0.
        self.n_samples = 0
        self.tot_score = 0.
        self.mean = 1.
        self.search_interval = search_interval
        self.base_sample_prob = base_sample_prob
        self.max_interval = (self.N, 0, self.N)
        explore_coef = max(0., explore_coef)
        explore_coef = min(1., explore_coef)
        self._ex_co = explore_coef

    def start(self, elements):
        '''
        Re-initializes the search. 

        elements : the maximum number of elements
                   over which we will search.
        '''
        self.tot = elements
        self.N = int(elements) / self.search_interval
        self.results = []
        self.max_score = 0.
        self.n_samples = 0
        self.tot_score = 0.
        self.mean = 1.
        # compute the buffer
        self.buffer = ((elements 
                        % self.search_interval)
                       / 2)
        self._l_bound = ThumbnailResultObject(
                            -1, score=self.mean,
                            dummy_frame=True)
        self._r_bound = ThumbnailResultObject(
                            self.N, score=self.mean,
                            dummy_frame=True)
        self._l_bound.next_frame = self._r_bound
        self._r_bound.prev_frame = self._l_bound

    def _is_invalid_frameno(self, frameno):
        '''
        Returns true if frameno is not a search
        frame.
        '''
        return (frameno - self.buffer) % self.search_interval

    def _insert_result_at(self, frameno, score=None):
        '''
        Creates a new result object at frameno, and
        updates it. THIS ASSUMES THE RESULT OBJECT
        IS NOT ALREADY DEFINED AT THAT FRAMENO
        '''
        prev_frame, next_frame = self._bounds(frameno)
        # if the next frame is immediately adjacent
        # to the current one, then set the _next_gap
        # appropriately
        if next_frame.frameno == (frameno+1):
            next_gap = next_frame.next_gap
        else:
            next_gap = None
        res_obj = ThumbnailResultObject(frameno, score=score,
                                    next_frame=next_frame,
                                    prev_frame=prev_frame,
                                    next_gap=next_gap)
        if frameno == (prev_frame.frameno+1):
            prev_frame.next_gap = res_obj.next_gap
        prev_frame.next_frame = res_obj
        next_frame.prev_frame = res_obj
        insort(self.results, res_obj)
        return res_obj

    def update(self, frameno, score):
        '''
        Updates the knowledge of the algorithm.
        '''
        if self._is_invalid_frameno(frameno):
            # ensure that this is actually a search
            # frame
            return
        # compute the frameno
        frameno = (frameno - self.buffer) / self.search_interval
        res_obj = self._get_result(frameno)
        if res_obj == None:
            res_obj = self._insert_result_at(frameno, score)
        else:
            res_obj.score = score
        self.max_score = max(self.max_score, score)
        self.tot_score += score
        self.mean = self.tot_score / self.n_samples

    def _get_nth_empty(self, n):
        '''
        Returns the nth undefined slot. (with the first being 0)
        '''
        # import ipdb
        # ipdb.set_trace()
        # first, check if there's space at the beginning
        if not len(self.results):
            return n
        res = self.results[0]
        if res.frameno > n:
            return n
        else:
            n -= res.frameno
        while True:
            # jump to the next gap
            res = res.next_gap
            cframeno = res.frameno
            # get gap size
            gap_size = res.next_frame.frameno - cframeno - 1
            if gap_size > n:
                return cframeno + n + 1
            n -= gap_size
            res = res.next_frame
        raise ValueError('n is larger than the number of empty spaces!')

    def get(self):
        '''Gets the next sample'''
        if self.n_samples == self.N:
            # have we taken every sample?
            print 'Returning none for some reason?'
            return None
        # modification so that the search doesn't
        # take too long.
        rng = self.N - (self.n_samples+1)
        while True:
            if rng == 0:
                sample = 0
            else:
                sample = np.random.choice(rng)
            frameno = self._get_nth_empty(sample)
            acc = self._accept_sample(frameno)
            if acc:
                res_obj = self._insert_result_at(frameno)
                self.n_samples += 1
                return frameno*self.search_interval + self.buffer

    def can_search(self, frameno):
        '''
        Returns a search interval if the frameno participates
        in a searchable interval. Note, this does not check
        that the frameno is valid as a search frame, it simply
        assumes it is. 

        If it's valid, it returns (start, end, start_score, end_score)
        otherwise it returns False
        '''
        frameno = self._frameno_to_search_frame(frameno)
        res_obj = self._get_result(frameno)
        if res_obj == None:
            return False
        can_srch = res_obj._can_search()
        if can_srch:
            sf, ef, sfs, efs = can_srch
            sf = self._search_frame_to_frameno(sf)
            ef = self._search_frame_to_frameno(ef)
            return (sf, ef, sfs, efs)
        return False

    def _frameno_to_search_frame(self, frameno):
        '''
        Converts a frameno into a search frame.
        '''
        return (frameno - self.buffer) / self.search_interval

    def _search_frame_to_frameno(self, frameno):
        '''
        Converts a search frame into a frameno.
        '''
        return (frameno * self.search_interval) + self.buffer

    def get_nearest(self, frameno):
        '''
        Returns the search frame nearest to the location
        sampled. 
        '''
        nframe = self._frameno_to_search_frame(self, frameno)
        return self._search_frame_to_frameno(self, nframe)

    def _get_result(self, frameno):
        '''
        Obtains a result value given its frameno.
        '''
        i = bisect_left(self.results, frameno)
        if i != len(self.results) and self.results[i] == frameno:
            return self.results[i]
        return None

    def _find_lt(self, x):
        'Find rightmost value less than x'
        i = bisect_left(self.results, x)
        if i:
            return self.results[i-1]
        return self._l_bound

    def _find_gt(self, x):
        'Find leftmost value greater than x'
        i = bisect_right(self.results, x)
        if i != len(self.results):
            return self.results[i]
        return self._r_bound

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
        pred_inp = [[lt.frameno, lt.score],
                    [gt.frameno, gt.score]]
        pred_score = self._predict_score(
                        pred_inp, sample)
        score_prob = (pred_score / self.max_score)**0.5
        rdiff = gt.frameno - sample
        if lt.frameno == -1:
            ldiff = np.inf
        else:
            ldiff = sample - lt.frameno
        min_dist = min(ldiff, rdiff)
        dist_prob = min_dist**0.5
        comb_prob = ((dist_prob * self._ex_co) 
                     *(score_prob * (1 - self._ex_co)))
        thresh = self.base_sample_prob + comb_prob
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
