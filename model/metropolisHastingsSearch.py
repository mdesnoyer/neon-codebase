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
    - a pointer to the next gap frame before a gap in the analysis
    - bad indicates whether or not there was some kind of issue processing
      this thumbnail, and causes this thumbnail to not be searched.
    '''
    def __init__(self, frameno, score=None, next_frame=None, prev_frame=None,
                 dummy_frame=False, next_gap=None, bad=False):
        self.frameno = frameno
        self.score = score
        # next frame won't induce a memory leak, right?
        if next_gap == None:
            next_gap = self
        self.next_frame = next_frame
        self.prev_frame = prev_frame
        self.dummy_frame = dummy_frame
        self.next_gap = next_gap
        self.bad = bad
        # indicates whether the interval starting with
        # this frame has been searched.
        self.lead_int_srchd = False

    def _can_search(self):
        '''
        Checks to see if ether the forward or backward interval can be
        searched. If the forward interval can be searched, it is assumed that
        it *is* searched, and sets lead_int_srchd to True. If the backward
        interval can be searched, it does the same but for the previous frame.

        If either can be searched, it returns (startFrame, endFrame,
        startScore, endScore) else it returns False.
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

    def __str__(self):
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
    A generic searching algorithm that samples from the distribution of the
    scores in accordance with the algorithm's belief in the viability of that
    region.

    Supports asynchronous updating (i.e., it is possible to draw sequential
    samples without updating the object's knowledge about the distribution)

    Note that this will has a min_dist for sampling, such that if the next
    largest sampled thumbnail by frameno is closer than min_dist, it will not
    draw that sample.
    '''

    def __init__(self, search_interval=64,
                 base_sample_prob=0.1, explore_coef=0.1):
        '''
        min_dist : the search interval. This won't actually sample from the
                   frames individually, but rather from 'search frames', which
                   are equally spaced frames, each separated by precisely
                   min_dist frames.
        base_sample_prob : the base probability that a thumbnail will be
                   sampled (or seached)
        explore_coef : a value between 0 and 1, the degree to which the
                   algorithm will favor exploration over exploitation.
        '''
        self._reset()
        self.search_interval = search_interval
        self.base_sample_prob = base_sample_prob
        explore_coef = max(0., explore_coef)
        explore_coef = min(1., explore_coef)
        self._ex_co = explore_coef

    def _reset(self):
        self.N = None
        self.tot = None
        self.buffer = None
        self.results = []
        self.max_score = 0.
        self.n_samples = 0
        self.tot_score = 0.
        self.mean = 1.
        self.max_interval = (self.N, 0, self.N)
        self._l_bound = None
        self._r_bound = None

    def start(self, elements):
        '''
        Re-initializes the search.

        elements : the maximum number of elements over which we will search.
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
        Returns true if frameno is not a search frame.
        '''
        return (frameno - self.buffer) % self.search_interval

    def _insert_result_at(self, frameno, score=None, bad=False):
        '''
        Creates a new result object at frameno, and updates it. THIS ASSUMES
        THE RESULT OBJECT IS NOT ALREADY DEFINED AT THAT FRAMENO
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
                                    next_gap=next_gap, bad=bad)
        if frameno == (prev_frame.frameno+1):
            prev_frame.next_gap = res_obj.next_gap
        prev_frame.next_frame = res_obj
        next_frame.prev_frame = res_obj
        insort(self.results, res_obj)
        return res_obj

    def update(self, frameno, score=None, bad=False):
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
            if score == None:
                score = self.mean
            res_obj = self._insert_result_at(frameno, score, bad)
            # update the number of samples because we now have one extra
            self.n_samples += 1
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
        Returns a search interval if the frameno participates in a searchable
        interval. Note, this does not check that the frameno is valid as a
        search frame, it simply assumes it is.

        If it's valid, it returns (start, end, start_score, end_score)
        otherwise it returns False
        '''
        frameno = self._frameno_to_search_frame(frameno)
        res_obj = self._get_result(frameno)
        if res_obj == None:
            return False
        can_srch = res_obj._can_search()
        if can_srch:
            if res_obj.bad:
                return False
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
        nframe = self._frameno_to_search_frame(frameno)
        return self._search_frame_to_frameno(nframe)

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
        Predicts the score of a sample given its neighbors. Currently only
        supports nearest neighbor on both sides, but may (in the future) be
        extended to something a bit more elaborate.
        '''
        [x1, y1], [x2, y2] = neighbs
        x3 = sample
        m = float(y2 - y1) / float(x2 - x1)
        return m * (x3 - x1) + y1

    def __getstate__(self):
        '''
        While using dill drastically improves our ability to pickle the
        objects involved in prediction, we need to reset the result object
        when it attempts to serialize itself, otherwise we'll have a situation
        in which the maximum recursion depth is exceeded due to the mutual
        pointers between elements of the results list.
        '''
        self._reset()

        return self.__dict__.copy()

class MCMH_rpl(MonteCarloMetropolisHastings):
    '''
    Works just like the original Monte Carlo Metropolis Hastings, but each
    time it takes a sample it returns the following: (str, meta). 'str' is
    either 'sample' in which case the score must be obtained or is 'search'
    in which case the FORWARD region can be searched. meta is a frameno in the
    case of sample, and a search tuple in the case of search (which is
    start_frame, start_score, end_frame, end_score).

    The _rpl is for 'replacement' even though this isn't really accurate.
    Instead, what it's going to do is replace samples and check if they can be
    sampled versus searched.
    '''
    def __init__(self, search_interval=64,
                 base_sample_prob=0.1, explore_coef=0.):
        # the search parameters are the same as for the
        # vanilla MCMH.
        super(MCMH_rpl, self).__init__(search_interval,
                                       base_sample_prob,
                                       explore_coef)
        self.searched = 0

    def start(self, elements):
        super(MCMH_rpl, self).start(elements)
        self.searched = 0

    def _reset(self):
        '''
        Removes knowledge about the video being processed.
        '''
        super(MCMH_rpl, self)._reset()
        self.searched = 0

    def get(self):
        while True:
            if self.searched == self.N - 1:
                # log this
                return None
                # add log to note that searching is incomplete
            obt = self._get()
            if obt:
                break
        action, meta = obt
        if action == 'search':
            # convert the metadata framenos
            sf, ef, sfs, efs = meta
            sf = self._search_frame_to_frameno(sf)
            ef = self._search_frame_to_frameno(ef)
            meta = (sf, ef, sfs, efs)
            ret = (action, meta)
        else:
            meta = self._search_frame_to_frameno(meta)
        return (action, meta)

    def _get(self):
        '''
        Attempts to acquire a sample. If it fails, returns False. Otherwise,
        it returns a tuple of the form (index, string). See the documentation
        under the class declaration.
        '''
        frameno = np.random.choice(self.N)
        result = self._get_result(frameno)
        if result == None:
            if not len(self.results):
                # handle the case when the first sample is taken
                self.n_samples += 1
                res_obj = self._insert_result_at(frameno)
                return ('sample', frameno)
            acc = self._accept_sample(frameno)
            if acc:
                self.n_samples += 1
                res_obj = self._insert_result_at(frameno)
                return ('sample', frameno)
            else:
                return False
        else:
            srch = result._check_fwd()
            if srch:
                self.searched += 1
                # ensure that search intervals are not 'bad' frames.
                if result.bad:
                    return False
                if result.next_frame.bad:
                    return False
                return ('search', srch)
            else:
                return False

    def __getstate__(self):
        self._reset()
        return self.__dict__.copy()












