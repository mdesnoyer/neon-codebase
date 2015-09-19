import logging
from bisect import bisect_left as bidx
from bisect import insort_left as bput

import numpy as np

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
                    )
'''
MonteCarloMetropolisHastings is a searching method
where frames are sampled according to the probability,
given their neighbors, that they have a high score. 
Assuming that the scores of sequential frames are not
completely independent, then this will eventually (and
efficiently) converge to the correct distribution of 
scores over frames.
'''

class MonteCarloMetropolisHastings(object):
    '''
    A generic searching algorithm that
    samples from the distribution of the
    scores in accordance with the algorithm's
    belief in the viability of that region.
    '''
    def __init__(self, elements):
        self.N = elements
        self.samples = []
        self.results = dict()
        self.max_score = 0.
        self.n_samples = 0
        self.tot_score = 0.
        self.mean = 0.
        self.rejected = set()

    def __call__(self, result=None):
        if result:
            self._update(result)
        else:
            return self._get()

    def _update(self, update):
        '''
        Updates the algorithm's current knoweldge
        state. 

        'update' is a list of tuples (x, y) where
        x - integer - the location of the sample
        y - float - the score of the sample
        '''
        if update[1] = None:
            # the image was rejected
            self.rejected.add(update[0])
        else:
            bput(self.samples, update[0])
            self.results[update[0]] = update[1]
            self.max_score = max(self.max_score, 
                                 update[1])
            self.tot_score += update[1]
        self.n_samples += 1
        # a rejected image causes the mean score
        # to be reduced -- this is sensible since
        # the more rejections we get the less likely
        # we should be to search unexplored regions.
        self.mean = self.tot_score / self.n_samples

    def _find_n_neighbors(self, target, N):
        '''
        Given a sorted list, returns the 
        N next smallest and the N next
        largest. Uses a bisection search.

        Returns a tuple of lists:
        ((smallest), (largest))
        '''
        v = bidx(self.samples, target)
        # Make sure to check for all those stupid
        # edge conditions
        si = max(0, v - N)
        ei = min(len(slist), v + N)
        nsvs = slist[si:v]
        if not nsvs:
            nsvs = 0
        nlvs = slist[v:ei]
        if not nlvs:
            nlvs = self.N
        return (nsvs, nlvs)

    def _bounds(self, target):
        '''
        Simpler version of find_n_neighbors,
        which only returns the left and right
        neighbors for now. 
        '''
        v = bidx(self.samples, target)
        if not v:
            # there are no lower samples
            xL = 0
            yL = self.mean
        else:
            xL = self.samples[v-1]
            yL = self.results[xL]
        if v == self.n_samples:
            # there are no higher samples
            xH = self.N
            yH = self.mean
        else:
            xH = self.samples[v]
            yH = self.results[xH]
        return [(xL, yL), (xH, yH)]

    def _accept_sample(self, sample):
        '''
        Returns true or false if the sample
        is to be accepted.
        '''
        if not self.n_samples:
            return True
        if sample in self.results:
            return False
        if sample in self.rejected:
            return False
        neighbs = self._bounds(sample)
        pred_score = self._predict_score(
                        neighbs, sample)
        criterion = pred_score / self.max_score
        return np.random.rand() < criterion

    def _get(self):
        '''
        Returns a sample
        '''
        while self.n_samples < self.N:
            sample = np.random.choice(self.N)
            if self._accept_sample(sample):
                return sample

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
