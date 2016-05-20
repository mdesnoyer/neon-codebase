'''
New MCMH script that hopefully drastically simplifies things.
Additionally, this version of MCMH is thread-safe and, once 
initialized, may be safely distributed among threads.

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
import threading
from Queue import PriorityQueue, Empty
from itertools import cycle

_log = logging.getLogger(__name__)

class MCMH(object):
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
    def __init__(self, elements, search_interval, clip=None):
        '''
        elements: the number of elements to search over.
        search_interval: The number of frames between search frames plus the
                         start frame.
        clip: how much of the bookends of the region to ignore, as a fraction.

        NOTES:
            Search interval is the number of frames between the search frames.
            In this diagram, we have search interval of 4, and search frame j, 
            and search step (not surfaced to mcmh) of 2. 
            ...   j-1   j     j+1   j+2   j+3   j+4   j+5   j+6   ...
                        ^                        ^                  search frames
                        ^            ^                              search step frames
                        |----search interval-----|
            
            * indicate frames that will be processed during the conducting
            of a local search.

        '''
        self.search_interval = search_interval
        self.clip = clip
        self.elements = elements
        self._lock = threading.Lock()
        self._setup()

    def _setup(self):
        '''
        Allocates all the required memory and things.
        '''
        N = self.elements
        c = self.clip
        intr = self.search_interval - 1

        start = int(c * N)
        stop = int(N - (c * N))
        search_frames = np.arange(start, stop, intr + 1).astype(int)

        self._tot = 0.  # sum of scores
        self.n_samples = 0.
        self._n = 0.  # total scored

        self._first = search_frames[0]
        self._last = search_frames[-1]
        # search frame to frame number dictionary
        self._sf2fno = {n: v for n, v in enumerate(search_frames)}
        # frame number to search frame dictionary
        self._fno2sf = {v: k for k, v in self._sf2fno.iteritems()}
        self._scores = []  # list of frames and scores, sorted by frameno.
        self._scored = [False] * len(search_frames)  # whether or not frame has been scored
        self._srt_scores = []  # list of scores, sorted by the score
        self._search_queue = PriorityQueue()
        self._sample_queue = range(len(search_frames))
        self.max_samps = len(search_frames)
        self._up_next = None  # for ensuring search intervals are produced.

    @property
    def _mean(self):
        return self._tot / max(self._n, 1.)

    def update(self, frameno, score):
        with self._lock:
            self._update(frameno, score)

    def _update(self, frameno, score):
        '''
        Updates the knowledge of the algorithm. A score of 'None'
        indicates there was a problem with this search frame.
        '''
        sf = self._fno2sf.get(frameno, None)
        if sf is None:
            # That is not a valid search frame.
            _log.warn('Invalid search frame.')
            return
        if self._scored[sf]:
            # you've already sampled this frame.
            _log.debug('Sample has already been scored.')
            return 
        insort(self._scores, (sf, score))
        # we have to keep track of which scores were actually
        # updated in case we get a score of 0. 
        self._scored[sf] = True
        if frameno < self._last:
            if self._scored[sf + 1]:
                # then you can search it!
                # add it to the search queue
                est = (self._get_score(sf) + self._get_score(sf + 1)) * 0.5
                self._search_queue.put((-est, sf))
        if frameno > self._first:
            if self._scored[sf - 1]:
                # then you can search it!
                # add it to the search queue
                est = (self._get_score(sf - 1) + self._get_score(sf)) * 0.5
                self._search_queue.put((-est, sf - 1))
        insort(self._srt_scores, score)
        self._tot += score
        self._n += 1
        self.n_samples += 1
        _log.debug('Sampling %.1f%% complete', self.n_samples * 100. / self.max_samps)

    def get_search(self):
        '''
        Returns an interval to search.
        '''
        try:
            item = self._search_queue.get_nowait()
        except Empty:
            return
        sf = item[1]
        f1 = self._sf2fno[sf]
        f2 = self._sf2fno[sf + 1]
        s1 = self._get_score(sf)
        s2 = self._get_score(sf + 1)
        return (f1, s1, f2, s2)

    def get_sample(self):
        with self._lock:
            return self._get_sample()
    
    def _get_sample(self):
        '''
        Returns a frame to search.
        '''
        if self._up_next is not None:
            # then return that to complete a local search interval
            sample = self._up_next
            self._up_next = None
            return self._sf2fno[sample]
        if not len(self._sample_queue):
            _log.debug('Sampling complete.')
            return None  # there is nothing left to sample.
        while True:
            sf = int(np.random.choice(self._sample_queue))
            isc = self._interp_score(sf)
            rnk = (1+float(bisect_left(self._srt_scores, isc))) / (1+len(self._srt_scores))
            if np.random.rand() < rnk:
                # then take the sample
                break
        self._sample_queue.remove(sf)
        if (sf + 1) in self._sample_queue:
            self._up_next = sf + 1
            self._sample_queue.remove(sf + 1)
        return self._sf2fno[sf]

    def _find_lt(self, sf):
        'Find the closest earlier frameno to sf'
        i = bisect_left(self._scores, (sf, 0.))
        if i:
            return self._scores[i-1]
        return (-1, self._mean)

    def _find_gt(self, sf):
        'Find closest later frameno to sf'
        i = bisect_right(self._scores, (sf, 0))
        if i != len(self._scores):
            return self._scores[i]
        return (len(self._scored), self._mean)

    def _get_score(self, sf):
        'Locate the leftmost value exactly equal to x'
        i = bisect_left(self._scores, (sf, -np.inf))
        if i != len(self._scores) and self._scores[i][0] == sf:
            sf, score = self._scores[i]
            return score
        _log.exception('Could not locate score for search frame %i' % sf)
        raise ValueError('Could not locate the score for %i' % sf)

    def _interp_score(self, sf):
        '''
        Returns the interpolated score for a search frame.
        '''
        x1, y1 = self._find_lt(sf)
        x2, y2 = self._find_gt(sf)
        x3 = sf
        m = float(y2 - y1) / float(x2 - x1)
        return m * (x3 - x1) + y1




