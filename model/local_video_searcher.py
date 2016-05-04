"""
==============================================================================
Local Video Searcher

Nick Dufour
12/15/2015
==============================================================================
OVERVIEW......................................................................
    
    The local searcher is the next iteration of the video search, which makes
a number of changes in addition to "local search." This is a brief outline of
how the search proceeds:

(1) The video is partitioned into "search frames"
(2) Samples from the video are obtained via the Metropolis-Hastings Monte
    Carlo search. 
(3) The function either executes a sampling or a local search.
    (3a)    Sampling occurs if a local search cannot be conducted. The frame
            is extracted, and the features are added to the set of statistics
            that consistute the algorithms knowledge of the video so far.
    (3b)    A local search can be conducted, at which point some number of
            equispaced samples are taken. The features are extracted, and then
            the best is valence scored. It is then potentially added to the
            results list.

Updates:
    Local searcher is now updated to work with asynchronous prediction (e.g.,
    from a deepnet running on a different server). This is done by having two
    distinct threads, performing sampling and local searches simultaneoulsy
    by extracting them from priority queues.
==============================================================================
LOCAL SEARCH..................................................................

    In local search, a region of the video bounded on either side by a search
frame is partitioned into equispaced samples. For each sample, the features
are extracted and combined according to the combiner function or object
specified into a combined score. The sample frame with the maximal combined
score is then assessed in terms of valence. If the valence is sufficiently
high, it is submitted to the results object to determine if it can replace
any of the current top thumbnails.

    Regions are locally searched if (a) both sides of the region (which are
search frames) have been sampled and (b) if the estimated score of the region
is sufficiently high (by averaging the score of the search frames). 

==============================================================================
FEATURE EXTRACTION............................................................

    Feature extraction is performed by regional feature generators. Some
features, like the SAD ("Sum of absolute differences") generator, require
multiple frames to compute. 

==============================================================================
COMBINER......................................................................

    The combiner accepts and arbitrary feature vector and returns a combined
score. The nature of this combination is either multiplicative or additive,
depending on the combiner used. In the multiplicative case, transfer functions
are used to move the feature vector to the combined score. 

    Feature scores are not directly taken from the output of the feature
generators. Some frames, dictacted by the arguments to the local search, are
added to running statistics. Then, for a given frame, the feature scores are
given by applying a transfer function to that frame's rank (expressed as a
ratio from 0 to 1, where 1 is the best) in the list of observed statistics.

    Thus the combiner initially accepts a vector of raw feature scores and
converts it to a vector of ranked feature values. Alternatively, the combiner
may instead return a vector of anonymous functions that return these ranks
when evaluated. This is useful as the rank score may not be the same as more
'knowledge' is added during processing. 

    The scores are further modulated either by transfer functions or by
feature weights. Further, a weight valence is specified indicating which rank
is the best one. There are many of these, which are described at the beginning
of the script. For example, one might be "MAXIMIZE," in which case the closer
a frame is to the top frame as ranked by a particular feature, the better. 

    In the multiplicative setting, the combined feature score varies from 0
to 1, and multiplies the final valence score (thereby attenuating it by some
amount). In the additive setting, the combined feature score over a greater
domain and is simply added to the valence score after being multiplied by the
combined score weight.

    Finally, in the multiplicative setting, there is a chance that some
features are undefined or irrelevant to a particular frame. For instance, if
a frame has no faces, then its closed eye score will necessarily be zero. Thus
the combiner may be provided with a dependencies dictionary, which is a 
dictionary of feature names to [feature_name, lambda] pairs. Given two
features x and y, and dependencies[x] = [y, lambda_func] the value of x only 
affects the combined score if lambda_func(y_val) == True.

==============================================================================
TRANSFER FUNCTIONS............................................................

    In the multiplicative setting, transfer functions are used to map the
feature value ranks to an appropriate score. They are lambda functions that 
accept a value x in [0, 1] and map it to a logistic curve. The logistic curve
can be modulated by specifying a max penalty, whereby the curve is logistic
and we have:
                    f(0) = 1 - max penalty
                    f(1) = 1

    Each feature has its own transfer function. Suppose there are N features
and feature i value v_i has transfer function f_i and max penalty 0.2. Then
the final combined score of an arbitrary image x with valence score v will be

    final score = v * f_1(v_1) * ... * f_i(v_i) * ... * f_N(v_N)

if v_i = 0, then this effectively becomes 

    final score = v * f_1(v_1) * ... * (1 - 0.2) * ... * f_N(v_N)
                = v * f_1(v_1) * ... * (    0.8) * ... * f_N(v_N)

hence this feature can reduce the combined score by a factor of (at most) 0.8.
It follows that larger max penalties mean that this feature has greater
importance, since the final score is penalized more if a given frame is ranked
poorly in terms of that feature. 

==============================================================================
ADDITITIONAL NOTES............................................................

    Replicate images are prevented in a piecewise manner from entering the top
N thumbnails if the fail certain tests using they are not sufficiently 'far'
away from the other thumbnails (excluding the one the thumbnail would replace)
where 'far' is the pairwise Jensen-Shannon divergence of the two ColorName
histograms.
    
    Frames are sampled randomly from a distribution governed by the knowledge
of the searcher over the video. This is performed by Metropolis-Hastings
search, where frames are more likely to be sampled if they are between other
high scoring frames. This is not strictly a 'true' metropolist-hastings
search, I just adopted the methodology of sampling from an uncomputable 
distribution.  

==============================================================================
NOTES:
This no longer inherits from the VideoSearcher() object, I'm not
sure if we want to change how this works in the future.

While this initially used Statistics() objects to calculate running
statistics, in principle even with a small search interval (32 frames)
and a very long video (2 hours), we'd only have about 5,000 values to
store, which we can easily manage. Thus we will hand-roll our own Statistics
objects.

Some filters introduced in this iteration of the video searcher implements
one filter, scene-change, that requires temporal information to be preserved
and as such we shouldn't be filtering out frames before this can occur. What
we're going to do when and if we have more than one temporal filter is an
open question.
==============================================================================
"""

import heapq
import logging
import os
import sys
from time import time
from itertools import permutations
from collections import OrderedDict as odict
from collections import defaultdict as ddict
from random import getrandbits
import shutil
import threading
from Queue import Queue

__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import model.errors
import numpy as np
from model.colorname import ColorName
from utils import statemon
from utils import pycvutils
from utils.options import define, options
from model.mcmh import MCMH  # change me
from grpc.framework.interfaces.face.face import ExpirationError

_log = logging.getLogger(__name__)

statemon.define('all_frames_filtered', int)
statemon.define('cv_video_read_error', int)
statemon.define('video_processing_error', int)
statemon.define('low_number_of_frames_seen', int)

define("text_model_path", 
       default=os.path.join(__base_path__, 'cvutils', 'data'), 
       help="The location of the text detector models")

MINIMIZE = -1  # flag for statistics where better = smaller
NORMALIZE = 0  # flag for statistics where better = closer to mean
MAXIMIZE = 1  # flag for statistics where better = larger
PEN_LOW_HALF = -2  # flag for statistics where better > median
PEN_HIGH_HALF = 2  # flag for statistics where better < median
PEN_ZERO = 3  # flag for statistics as (x > 0)

TESTING = False
TESTING_DIR = None
CUR_TESTING_DIR = None


def get_feat_score_transfer_func(max_penalty, median=0.3):
    """
    Returns a function that maps a feature score to another score that will be
    multiplicatively combined with the score and the other transferred feature
    scores. Thus, max_penalty is the maximum amount the final score may be
    reduced by having the worst feature score for this particular feature
    possible.

    In other words, if you wish the blurriest image in a video to have its
    final score reduced by at least 80%, irrespective of its other feature
    scores, then you would obtain its transfer func with

    get_feat_score_transfer_func(0.8)

    The median is the score for which the penalty for the final score for that
    image is (at least, depending on the other feature scores) equidistant
    between the max penalty and no penalty. So if it's value is 0.4, then an
    image in the 40th percentile of the rankings is penalized with half the
    maximum penalty
    """
    k = 7.  # this is the slope of the transfer function.
    c = max_penalty
    x0 = median

    def calcL(k, x0, c):
        numer = (c - 1) * np.exp(-k * x0) * (np.exp(k * x0) + 1) * (np.exp(k * x0) + np.exp(k))
        return -(numer / (np.exp(k) - 1))

    def calcZ(k, x0, c):
        numer = -c * (np.exp(k * x0) + 1) + np.exp(k * (x0 - 1)) + 1
        return numer / (np.exp(k * (x0 - 1)) - np.exp(k * x0))

    L = calcL(k, x0, c)
    Z = calcZ(k, x0, c)
    return lambda x: Z + (L / (1 + np.exp(-k * (x - x0))))


class Statistics(object):
    """
    Replicates (to a degree) the functionality of the true running statistics
    objects (which are in the utils folder under runningstat). This is because
    it is unlikely that we will ever need to maintain a very large number of
    measurements.

    If init is not None, it initializes with the values provided.
    """

    def __init__(self, max_size=5000, init=None):
        """
        Parameters:
            max_size = the maximum size of the value array
            init = an initial set of values to instantiate it
        """
        self._count = 0
        self._max_size = max_size
        self._vals = np.zeros(max_size)
        self._update_var = False
        self._p_var = None
        self._update_mean = False
        self._p_mean = None
        self._update_median = False
        self._p_median = None
        if init is not None:
            self.push(init)

    def push(self, x):
        """
        pushes a value onto x
        """
        self._update_var = True
        self._update_mean = True
        self._update_median = True
        if type(x) == list:
            for ix in x:
                self.push(ix)
        if self._count == self._max_size:
            # randomly replace one
            idx = np.random.choice(self._max_size)
            self._vals[idx] = x
        else:
            self._vals[self._count] = x
            self._count += 1  # increment count

    @property
    def var(self):
        if not self._count:
            return 0
        if self._update_var:
            self._p_var = np.var(self._vals[:self._count])
            self._update_var = False
        return self._p_var

    @property
    def mean(self):
        if not self._count:
            return 0
        if self._update_mean:
            self._p_mean = np.mean(self._vals[:self._count])
            self._update_mean = False
        return self._p_mean

    @property
    def median(self):
        if not self._count:
            return 0
        if self._update_median:
            self._p_median = np.median(self._vals[:self._count])
            self._update_median = False
        return self._p_median

    def rank(self, x):
        """Returns the rank of x"""
        quant = np.sum(self._vals[:self._count] < x)
        return quant * 1. / max(1, self._count)

    def percentile(self, x):
        """Returns the xth percentile of the stored elements. x may be a float
        between 0 and 100, inclusive. This is fairly computationally
        expensive, however it's only being used when new thumbnails are being
        added to the top thumbnails list."""
        return np.percentile(self._vals[:self._count], x)


class ColorStatistics(object):
    """
    Similar to the Statistics object (defined below), but in lieu of storing
    numeric values, it stores color histograms. This will allow us to
    establish a lower bound on the allowable distances--by empirically
    establishing the mean and standard deviation of the pairwise distances
    between frames in the video.

    IMPORTANTLY, this flavor of the running statistic does NOT support
    initialization with a set of ColorName objects.
    """

    def __init__(self, max_size=150):
        """
        Parameters:
            max_size = the maximum number of color histograms to store.
        """
        self._ColObjs = []
        self._max_size = max_size
        self._count = 0
        self._dists = Statistics()
        self._prep = pycvutils.ImagePrep(max_side=480)

    def push(self, img):
        """
        Add an image into the statistics object. Unlike the vanilla Statistics
        object, this does *not* support pushing multiple items simultaneously.
        """
        cn = ColorName(self._prep(img))
        for pcn in self._ColObjs:
            self._dists.push(pcn.dist(cn))
        if self._count == self._max_size:
            # randomly replace one
            idx = np.random.choice(self._max_size)
            self._ColObjs[idx] = cn
        else:
            self._ColObjs.append(cn)
            self._count += 1

    @property
    def var(self):
        return self._dists.var

    @property
    def mean(self):
        return self._dists.mean

    def percentile(self, x):
        """
        The notion of Rank doesnt have much meaning in this sense, since
        we are measuring how different the thumbnails are from each other
        in the aggregate.
        """
        return self._dists.percentile(x)


class MultiplicativeCombiner(object):
    '''
    Multiplicatively combines feature scores
    '''

    def __init__(self, penalties=ddict(lambda: 0.999), weight_valence=dict(),
                 combine=lambda x: np.prod(x),
                 dependencies=ddict(lambda: [])):
        '''
        penalties is a dict of feature names --> maximum penalties, see
            get_feat_score_transfer_func for an explanation.
        weight_valence is a dictionary of {'stat name': valence} encoding,
            which indicates whether 'better' is higher, lower, or maximally
            typical.
        combine is an anonymous function to combine scored statistics; combine
            must have a single argument and be able to operate on lists of
            floats.
        dependencies: a dictionary of feature names to [feature_name, lambda]
            pairs. Given two features x and y, and
                dependencies[x] = [y, lambda_func]
            then the value of x only affects the combined score if
            lambda_func(y_val) == True.
        Note: if a statistic has an entry in both the stats and weights dict,
            then weights dict takes precedence.
        '''
        # self.weight_dict = weight_dict
        self.weight_valence = weight_valence
        # compute the transfer functions
        self._trans_funcs = dict()
        self.name = 'Multiplicative combiner'
        for feat in weight_valence:
            max_pen = 1 - penalties[feat]
            self._trans_funcs[feat] = get_feat_score_transfer_func(max_pen)
        self._combine = combine
        # the combiner exports a combination function for use in the results
        # objects, it accepts model score (ms), feature score (fs) and
        # feature score weight (w)
        self.result_combine = lambda ms, fs, w: ms * fs
        self.dependencies = dependencies

    def _set_stats_dict(self, stats_dict):
        '''
        Sets the statistics dictionary given a video searcher object.

        Should only be called by the object that has the stats dictionary.

        stats_dict is a dictionary of {'stat name': Statistics()}
        '''
        self._stats_dict = stats_dict

    def _compute_stat_score(self, feat_name, feat_vec):
        '''
        Computes the statistics score for a feature vector. If it has a
        defined weight, then we simply return the product of this weight with
        the value of the feature.
        '''

        if self._stats_dict.has_key(feat_name):
            vals = []
            if self.weight_valence.has_key(feat_name):
                valence = self.weight_valence[feat_name]
            else:
                _log.error('No valence defined for feature %s' % (feat_name))
                raise
            for v in feat_vec:
                if valence == PEN_ZERO:
                    rank = int(v > 0)
                else:
                    rank = self._stats_dict[feat_name].rank(v)
                if valence == MINIMIZE:
                    rank = 1. - rank
                if valence == NORMALIZE:
                    rank = 1. - abs(0.5 - rank) * 2
                if valence == PEN_LOW_HALF:
                    rank = 1. + min(0, rank - 0.5) * 2
                if valence == PEN_HIGH_HALF:
                    rank = 1. - max(0, rank - 0.5) * 2
                vals.append(self._trans_funcs[feat_name](rank))
            return vals
        return feat_vec

    def _compute_stats_score_func(self, feat_name, feat_val):
        '''For an images feature name / feature value pair, returns a lambda
        function that allows you to evaluate it lazily / dynamically, so that
        early thumbnails are not given a "free pass"'''
        if self._stats_dict.has_key(feat_name):
            stat_obj = self._stats_dict[feat_name]
            if self.weight_valence.has_key(feat_name):
                valence = self.weight_valence[feat_name]
            else:
                _log.error('No valence defined for feature %s' % (feat_name))
                raise
            if valence == MINIMIZE:
                rankfunc = lambda x: 1. - stat_obj.rank(x)
            elif valence == NORMALIZE:
                rankfunc = lambda x: 1. - abs(0.5 - stat_obj.rank(x)) * 2
            elif valence == PEN_LOW_HALF:
                rankfunc = lambda x: 1. + min(0, stat_obj.rank(x) - 0.5) * 2
            elif valence == PEN_HIGH_HALF:
                rankfunc = lambda x: 1. - max(0, stat_obj.rank(x) - 0.5) * 2
            elif valence == MAXIMIZE:
                rankfunc = lambda x: stat_obj.rank(x)
            elif valence == PEN_ZERO:
                rankfunc = lambda x: x > 0
            return lambda: self._trans_funcs[feat_name](rankfunc(feat_val))
        else:
            return lambda: feat_val

    def combine_scores_func(self, feat_dict):
        '''This will return a list of functions that can be evaluated lazily,
        which permit the thumbnail scores to be updated--especially in the
        event that some thumbnails would not actually be accepted if the order
        of analysis was changed.'''
        funcs = []
        for feat_name, feat_val in feat_dict.iteritems():
            incl = True
            if len(self.dependencies[feat_name]):
                for dep, lamb in self.dependencies[feat_name]:
                    if not feat_dict.has_key(dep):
                        incl = False
                        break
                    dep_val = feat_dict[dep]
                    if not lamb(dep_val):
                        incl = False
                        break
            if incl:
                funcs.append(self._compute_stats_score_func(
                        feat_name, feat_val))
        return lambda: self._combine([x() for x in funcs])

    def get_indy_funcs(self, feat_dict):
        '''Testing function that returns individual transfer functions, so
        that we can evaluate each transfer function's value independently'''
        funcs_dict = {}
        for feat_name, feat_val in feat_dict.iteritems():
            incl = True
            if len(self.dependencies[feat_name]):
                for dep, lamb in self.dependencies[feat_name]:
                    if not feat_dict.has_key(dep):
                        incl = False
                        break
                    dep_val = feat_dict[dep]
                    if not lamb(dep_val):
                        incl = False
                        break
            if incl:
                funcs_dict[feat_name] = self._compute_stats_score_func(
                        feat_name, feat_val)
        return funcs_dict

    def combine_scores(self, feat_dict):
        '''
        Returns the scores for the thumbnails given a feat_dict, which is a
        dictionary {'feature name': feature_vector}

        This has to be changed from the original implementation (see
        AdditiveCombiner) since it has dependencies. So what is done instead
        is that we deal them to individual dictionaries, and evaluate the
        combine_scores_func.
        '''

        stat_scores = []
        max_def = max([len(x) for x in feat_dict.itervalues()])
        indi_dicts = [dict() for x in range(max_def)]
        for k, v in feat_dict.iteritems():
            for n, kval in enumerate(v):
                indi_dicts[n][k] = kval
        comb_scores = []
        for feat_dict in indi_dicts:
            comb_scores.append(self.combine_scores_func(feat_dict)())
        return comb_scores


class AdditiveCombiner(object):
    '''
    Combines arbitrary feature vectors according to either (1) predefined
    weights or (2) attempts to deduce the weight given the global statistics
    object.
    '''

    def __init__(self, weight_dict=ddict(lambda: 1.), weight_valence=dict(),
                 combine=lambda x: np.sum(x)):
        '''
        weight_dict is a dictionary of {'stat name': weight} which yields
            absolute weights.
        weight_valence is a dictionary of {'stat name': valence} encoding,
            which indicates whether 'better' is higher, lower, or maximally
            typical.
        combine is an anonymous function to combine scored statistics; combine
            must have a single argument and be able to operate on lists of
            floats.
        Note: if a statistic has an entry in both the stats and weights dict,
            then weights dict takes precedence.
        '''
        self.weight_dict = weight_dict
        self.weight_valence = weight_valence
        self._combine = combine
        self.name = 'Additive Combiner'
        # the combiner exports a combination function for use in the results
        # objects, it accepts model score (ms), feature score (fs) and
        # feature score weight (w)
        self.result_combine = lambda ms, fs, w: ms + w * fs

    def _set_stats_dict(self, stats_dict):
        '''
        Sets the statistics dictionary given a video searcher object.

        Should only be called by the object that has the stats dictionary.

        stats_dict is a dictionary of {'stat name': Statistics()}
        '''
        self._stats_dict = stats_dict

    def _compute_stat_score(self, feat_name, feat_vec):
        '''
        Computes the statistics score for a feature vector. If it has a
        defined weight, then we simply return the product of this weight with
        the value of the feature.
        '''

        if self._stats_dict.has_key(feat_name):
            vals = []
            if self.weight_valence.has_key(feat_name):
                valence = self.weight_valence[feat_name]
            else:
                valence = MINIMIZE  # assume you are trying to maximize it
            for v in feat_vec:
                if valence == PEN_NONZERO:
                    rank = int(v > 0)
                else:
                    rank = self._stats_dict[feat_name].rank(v)
                if valence == MINIMIZE:
                    rank = 1. - rank
                if valence == NORMALIZE:
                    rank = 1. - abs(0.5 - rank) * 2
                if valence == PEN_LOW_HALF:
                    rank = 1. + min(0, rank - 0.5) * 2
                if valence == PEN_HIGH_HALF:
                    rank = 1. - max(0, rank - 0.5) * 2
                vals.append(rank * self.weight_dict[feat_name])
            return vals

        else:
            return [(x * self.weight_dict[feat_name]) ** 2 for x in feat_vec]
        return feat_vec

    def _compute_stats_score_func(self, feat_name, feat_val):
        '''For an images feature name / feature value pair, returns a lambda
        function that allows you to evaluate it lazily / dynamically, so that
        early thumbnails are not given a "free pass"'''
        if self._stats_dict.has_key(feat_name):
            stat_obj = self._stats_dict[feat_name]
            if self.weight_valence.has_key(feat_name):
                valence = self.weight_valence[feat_name]
            else:
                valence = MINIMIZE
            if valence == MINIMIZE:
                rankfunc = lambda x: 1. - stat_obj.rank(x)
            elif valence == NORMALIZE:
                rankfunc = lambda x: 1. - abs(0.5 - stat_obj.rank(x)) * 2
            elif valence == PEN_LOW_HALF:
                rankfunc = lambda x: 1. + min(0, stat_obj.rank(x) - 0.5) * 2
            elif valence == PEN_HIGH_HALF:
                rankfunc = lambda x: 1. - max(0, stat_obj.rank(x) - 0.5) * 2
            elif valence == MAXIMIZE:
                rankfunc = lambda x: stat_obj.rank(x)
            elif valence == PEN_NONZERO:
                rankfunc = lambda x: x > 0
            return lambda: rankfunc(feat_val) ** 2 * self.weight_dict[feat_name]
        else:
            return lambda: (feat_val * self.weight_dict[feat_name]) ** 2

    def combine_scores_func(self, feat_dict):
        '''This will return a list of functions that can be evaluated lazily,
        which permit the thumbnail scores to be updated--especially in the
        event that some thumbnails would not actually be accepted if the order
        of analysis was changed.'''
        funcs = []
        tot_pos = float(np.sum(
                [self.weight_dict[x] for x in feat_dict.keys()]))
        for feat_name, feat_val in feat_dict.iteritems():
            funcs.append(self._compute_stats_score_func(feat_name, feat_val))
        return lambda: sum([x() for x in funcs]) / tot_pos

    def get_indy_funcs(self, feat_dict):
        '''Testing function that returns individual transfer functions, so
        that we can evaluate each transfer function's value independently'''
        funcs_dict = {}
        for feat_name, feat_val in feat_dict.iteritems():
            cur_func = self._compute_stats_score_func(feat_name, feat_val)
            funcs_dict[feat_name] = cur_func
        return funcs_dict

    def combine_scores(self, feat_dict):
        '''
        Returns the scores for the thumbnails given a feat_dict, which is a
        dictionary {'feature name': feature_vector}
        '''

        stat_scores = []
        tot_pos = float(np.sum(
                [self.weight_dict[x] for x in feat_dict.keys()]))
        for k, v in feat_dict.iteritems():
            stat_scores.append(self._compute_stat_score(k, v))
        comb_scores = []
        for x in zip(*stat_scores):
            comb_score = self._combine(x)
            comb_score /= tot_pos  # normalize
            comb_scores.append(comb_score)
        return comb_scores


class _Result(object):
    '''
    Private class to be used by the ResultsList object. Represents an
    invidiual top-result image in the top results list.

    Note: comb_score is not the score output by the combiner, but rather
    the combination of the feature score (feat_score, which is output by
    the combiner) and the score, as a function of the feat_score_weight)

    The combination function is exported by the combiner, and must accept
    three arguments: model score (ms), feature score (fs), and feature
    score weight (w) and return a float.
    '''

    def __init__(self, frameno=None, score=-np.inf, image=None,
                 feat_score=None, meta=None,
                 feat_score_weight=None, feat_score_func=None,
                 combination_function=None):
        self._defined = False
        if score and frameno:
            self._defined = True
            _log.debug(('Instantiating result object at frame %i with'
                        ' score %.3f') % (frameno, score))
        self.score = score
        self.frameno = frameno
        self._feat_score = feat_score
        self._feat_score_func = feat_score_func
        self._hash = getrandbits(128)
        self.image = image
        if combination_function is None:
            combination_function = lambda ms, fs, w: ms + fs * w
        self._combination_function = combination_function
        if self.image is not None:
            self._color_name = ColorName(self.image)
        else:
            self._color_name = None
        self.meta = meta
        self._feat_score_weight = feat_score_weight

    @property
    def feat_score(self):
        if self._feat_score_func is None:
            return self._feat_score
        return self._feat_score_func()

    @property
    def comb_score(self):
        '''Property that computes the combination score. Note that it no
        longer checks to see if the feature score weight is defined; thus
        if you use a function that requires the feature score weight, it
        must be defined'''
        if self.feat_score is None:
            return self.score
        return self._combination_function(self.score, self.feat_score,
                                          self._feat_score_weight)

    def __cmp__(self, other):
        if type(self) is not type(other):
            return cmp(self.score, other)
        # undefined result objects are always 'lower' than defined
        # result objects.
        if not self._defined:
            return -1
        if not other._defined:
            return 1
        return cmp(self.score, other.score)

    def __str__(self):
        if not self._defined:
            return 'Undefined Top Result object'
        if self.feat_score:
            return 'Result fr:%i sc:%.2f feat sc:%.2f comb sc:%.2f' % (
                self.frameno, self.score, self.feat_score,
                self.comb_score)
        else:
            return 'Result fr:%i sc:%.2f feat sc:N/A comb sc:%.2f' % (
                self.frameno, self.score, self.comb_score)

    def dist(self, other):
        if type(self) is not type(other):
            raise ValueError('Must get distance relative to other Result obj')
        if not self._defined:
            if not other._defined:
                return 0
            else:
                return np.inf
        if not other._defined:
            return np.inf
        if self._hash == other._hash:
            return np.inf  # the same object is infinitely different from itself
        return self._color_name.dist(other._color_name)


class ResultsList(object):
    '''
    The ResultsList class represents the sorted list of current best results.
    This also handles the updating of the results list.

    If the 'max_variety' parameter is set to true (default), inserting a new
    result is not guaranteed to kick out the lowest current scoring result;
    instead, it's also designed to maximize variety as represented by the
    histogram of the colorname. Thus, new results added to the pile will not
    be added if the minimium pairwise distance between all results is
    decreased.

    There are two parameters that dictate whether or not an image should be
    accepted or rejected based on its similarity.
        min_acceptable: a paramter that ensures that a thumbnail is *not*
            accepted unless it is as least this different from the extant
            pool.
        max_rejectable: if the thumbnail is at least this different from the
            other thumbnails, then it's not necessary to ensure that it still
            increases the 'representativeness' of the top thumbnail.

        *** Both min_acceptable and max_rejectable may be functions.
    '''

    def __init__(self, n_thumbs=5, max_variety=True, min_acceptable=0.02,
                 max_rejectable=0.2, feat_score_weight=0.,
                 adapt_improve=False, combination_function=None):
        self._max_variety = max_variety
        self.n_thumbs = n_thumbs
        self.reset()
        self._min_acceptable = min_acceptable
        self._max_rejectable = max_rejectable
        self._feat_score_weight = feat_score_weight
        self._considered_thumbs = 0
        self._adapt_improve = adapt_improve
        self._combination_function = combination_function
        if adapt_improve:
            self._clipLimit = 1.0
            self._tileGridSize = (8, 8)
            self.clahe = cv2.createCLAHE(clipLimit=self._clipLimit,
                                         tileGridSize=self._tileGridSize)

    @property
    def min_acceptable(self):
        try:
            return self._min_acceptable()
        except TypeError:
            return self._min_acceptable

    @property
    def max_rejectable(self):
        try:
            return self._max_rejectable()
        except TypeError:
            return self._max_rejectable

    def _write_testing_frame(self, res, reason=None, idx=None):
        '''
        Creates a filename for the current result object. Reason
        is either 'accept' or 'reject'. If accept, must provide
        an index for the position in the results list.
        '''
        if not TESTING:
            return
        if TESTING_DIR is None:
            return
        cur_sc_str = ' '.join(['%.3f' % x.feat_score for x in self.results
                               if x._defined])
        cur_comb_str = ' '.join(['%.3f' % x.comb_score for x in self.results
                                 if x._defined])
        _log.debug('Current feat scores for top thumbs: ' + cur_sc_str)
        _log.debug('Current comb scores for top thumbs: ' + cur_comb_str)
        _log.debug('TESTING ENABLED: saving %s' % res)
        fname = '%04i_' % (self._considered_thumbs)
        fname += '%05i_' % (res.frameno)
        fname += '%04.2f_' % (res.score)
        if res._feat_score is None:
            fname += 'NA_'
        else:
            fname += '%04.2f_' % (res.feat_score)
        fname += '%04.2f_' % (res.comb_score)
        if reason == 'accept':
            fname += 'accepted_replacing_%i.jpg' % (idx)
        elif reason is not None:
            fname += '%s.jpg' % (reason)
        else:
            fname += 'rejected.jpg'
        fname = os.path.join(CUR_TESTING_DIR, fname)
        cv2.imwrite(fname, res.image)

    def reset(self, n_thumbs=None):
        _log.debug('Result object of size %i resetting' % (self.n_thumbs))
        if n_thumbs is not None:
            self.n_thumbs = n_thumbs
        self.results = [_Result() for x in range(self.n_thumbs)]
        self.min = self.results[0].score
        self.dists = np.zeros((self.n_thumbs, self.n_thumbs))

    def _update_dists(self, entry_idx):
        for idx in range(len(self.results)):
            dst = self.results[idx].dist(self.results[entry_idx])
            self.dists[idx, entry_idx] = dst
            self.dists[entry_idx, idx] = dst

    def accept_replace(self, frameno, score, image=None, feat_score=None,
                       meta=None, feat_score_func=None):
        '''
        Attempts to insert a result into the results list. If it does not
        qualify, it returns False, otherwise returns True
        '''
        res = _Result(frameno=frameno, score=score, image=image,
                      feat_score=feat_score, meta=meta,
                      feat_score_weight=self._feat_score_weight,
                      feat_score_func=feat_score_func,
                      combination_function=self._combination_function)
        self._considered_thumbs += 1
        if score < self.min:
            _log.debug('Frame %i [%.3f] rejected due to score' % (frameno,
                                                                  score))
            self._write_testing_frame(res, 'score_too_low')
            return False
        if not self._max_variety:
            return self._push_over_lowest(res)
        else:
            return self._maxvar_replace(res)

    def _compute_new_dist(self, res):
        '''
        Returns the distance of the new result object to all result objects
        currently in the list of result objects.
        '''
        dists = []
        for rres in self.results:
            dists.append(res.dist(rres))
        return np.array(dists)

    def _push_over_lowest(self, res):
        '''
        Replaces the current lowest-scoring result with whatever res is. Note:
        this does not check that res's score > the min score. It's assumed
        that this is done in accept_replace.
        '''
        sco_by_idx = np.argsort([x.comb_score for x in self.results])
        return self._replace(sco_by_idx[0], res)

    def _replace(self, idx, res):
        '''
        The thumbnail at index idx is replaced by the thumbnail res.
        '''
        old = self.results[idx]
        self.results[idx] = res
        _log.info('%s is replacing %s' % (res, old))
        self._update_dists(idx)
        self._update_min()
        self._write_testing_frame(res, 'accept', idx)
        return True

    def _improve_img(self, res):
        '''auto-improves the main image of a result object via
        _improve_raw_img'''
        if self._adapt_improve:
            _log.debug('Adaptively improving %s' % (res))
            res.image = self._improve_raw_img(res.image)

    def _improve_raw_img(self, image):
        '''auto-improves an image'''
        if self._adapt_improve:
            if len(image.shape) < 3:
                image = self.clahe.apply(image)
                return image
            else:
                # convert to HSV, apply to last channel
                img = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
                img[:, :, 2] = self.clahe.apply(img[:, :, 2])
                return cv2.cvtColor(img, cv2.COLOR_HSV2BGR)
        else:
            return image

    def _maxvar_replace(self, res):
        '''
        Replaces the lowest scoring result possible while maximizing variance,
        up to self.max_rejectable. Results whose similarity distance to the
        'closest' thumbnail is less than self.min_acceptable are automatically
        rejected.
        '''
        repl_idx = [x for x in range(len(self.results)) if
                    self.results[x] < res]
        # get dists as they are now
        mdists = np.min(self.dists[repl_idx], 1)
        # get the distances of the candidate to the current results
        dists = self._compute_new_dist(res)
        arg_srt_idx = np.argsort(dists)
        # if you are 'sufficiently different' then replace the lowest
        # scoring one.
        if dists[arg_srt_idx[0]] > self.max_rejectable:
            _log.debug(('%s thumbnail is sufficiently different from the '
                        'other thumbnails given the variety seen in the '
                        'video to be accepted') % (res))
            return self._push_over_lowest(res)

        if dists[arg_srt_idx[0]] < self.min_acceptable:
            # it's too close to the other thumbnails.
            if dists[arg_srt_idx[1]] > self.min_acceptable:
                # so maybe you can remove the closest one--i.e., if the
                # candidate frame is different enough from all but one of
                # the other thumbs and its score is higher than the least
                # different thumbs.
                if (self.results[arg_srt_idx[0]].comb_score <
                        res.comb_score):
                    # replace the closest one.
                    return self._replace(arg_srt_idx[0], res)
                else:
                    _log.debug('Most similar thumb is better than candidate')
                    self._write_testing_frame(res, ('too_similar_to_all_but_'
                                                    'one_but_most_similar_has_higher_'
                                                    'score'))
                    return False
            else:

                # i.e., if the new thumbnail will be below the minimum
                # acceptable distance AND it will not increase the global
                # minimum distance
                _log.debug(('%s is insufficiently different given the variety'
                            ' seen in the video so far.') % (res))
                self._write_testing_frame(res, 'below_sim_threshold')
                return False
        # if there are any undefined thumbnails, replace them.
        undef_thumbs = filter(lambda x: x.score == -np.inf, self.results)
        if len(undef_thumbs):
            return self._push_over_lowest(res)
        # otherwise, iterate over the lowest scoring ones and replace the
        # lowest one that is 'less different' than you are from the
        # remaining thumbnails
        sco_by_idx = np.argsort([x.comb_score for x in self.results])
        for idx in sco_by_idx:
            if self.results[idx].comb_score > res.comb_score:
                # none of the current thumbnails can be replaced
                _log.debug('There are no low-scoring less-varied thumbnails for %s' % (res))
                self._write_testing_frame(res, 'none_replaceable')
                return False
            # see if you can replace it
            if idx == arg_srt_idx[0]:
                c_min_dist = dists[arg_srt_idx[1]]
            else:
                c_min_dist = dists[arg_srt_idx[0]]
            # if the resulting minimum distance is >= the results minimum
            # distance, you may replace it.
            if c_min_dist >= np.min(self.dists[idx]):
                break
        # replace the idx
        return self._replace(idx, res)

    def _update_min(self):
        '''
        Updates current minimum score.
        '''
        new_min = np.inf
        for res in self.results:
            if res.score < new_min:
                new_min = res.score
        _log.debug('New minimum score is %.3f' % (new_min))
        self.min = new_min

    def get_results(self):
        '''
        Returns the results in sorted order, sorted by score. Returns them
        as (image, score, frameno)
        '''
        _log.debug('Dumping results')
        sco_by_idx = np.argsort([x.score for x in self.results])[::-1]
        res = []
        for idx in sco_by_idx:
            res_obj = self.results[idx]
            if not res_obj._defined:
                continue
            image = self._improve_raw_img(res_obj.image)
            res.append([image, res_obj.score, res_obj.frameno])
        return res


class LocalSearcher(object):
    def __init__(self, predictor,
                 processing_time_ratio=1.0,
                 local_search_width=32,
                 local_search_step=4,
                 n_thumbs=5,
                 feat_score_weight=0.,
                 mixing_samples=40,
                 search_algo=MCMH,
                 max_variety=True,
                 feature_generators=None,
                 feats_to_cache=None,
                 combiner=None,
                 filters=None,
                 startend_clip=0.1,
                 adapt_improve=False,
                 queue_unsearched=False,
                 use_all_data=False,
                 use_best_data=False,
                 testing=False,
                 testing_dir=None,
                 filter_text=True,
                 text_filter_params=None,
                 filter_text_thresh=0.04):
        '''
        Inputs:
            predictor:
                computes the score for a given image
            local_search_width:
                The number of frames to search forward.
            local_search_step:
                The step size between adjacent frames.
                ===> for instance, if local_search_width = 6 and
                     local_search_step = 2, then it will obtain 6 frames
                     across 12 frames (about 0.5 sec)
            n_thumbs:
                The number of top images to store.
            feat_score_weight:
                The degree to which the combined feature score should effect
                the rank of the frames. New frames are added to results
                according to score + comb_score * feat_score_weight. Note that
                the values of feat_score_weight can be fairly high, as it it
                is exponentially weighted to more greatly penalize
                poor-performing samples (akin to an L-2 norm). Additionally,
                because of the normalization, the feature score is always
                constrained to be between 0.0 and 1.0. Thus the maximum the
                feat_score can add to the score is feat_score_weight
            mixing_samples:
                The number of samples to draw to establish baseline
                statistics.
            search_algo:
                Selects the thumbnails to try; accepts the number of elements
                over which to search. Should support asynchronous result
                updating, so it is easy to switch the predictor between
                sequential (CPU-based) and non-sequential (GPU-based)
                predictor methods. Further, it must be able to accept an
                indication that the frame search request was BAD (i.e., it
                couldn't be read).
            max_variety:
                If True, the local searcher will maximize the variety of the
                images.
            feature_generators:
                A list of feature generators. Note that this have to be of the
                RegionFeatureGenerator type. The features (those that are not
                required by the filter, that is) are extracted in the order
                specified. This is due to some of the features requiring
                sequential processing. Thus, an ordered dict is used.
            feats_to_cache:
                The name of all features to save as running statistics.
                (features are only cached during sampling). This also dictates
                which features contribute to the combined score, since feature
                scores individually are their ranks in the population sample.
            combiner:
                Combines the feature scores. See class definition above. This
                replaces the notion of a list of criteria objects, which
                proved too abstract to implement.
            filters:
                A list of filters which accept feature vectors and return
                which frames need to be filtered. Each filter should surface
                the name of the feature generator whose vector is to be used,
                via an attribute that is simply named "feature."
                Filters are applied in-order, and only non-filtered frames
                have their features extracted per-filter.
            startend_clip:
                The fraction of the start and the end of the video to clip
                off. A value of 0.1 means that 10% of the start and 10% of the
                end of the video are removed.
            adapt_improve:
                Adaptively improves the brightness / contrast / etc of an
                image via the CLAHE algorithm.
            queue_unsearched:
                If the interval should not be searched initially, then it will
                be placed into a priority queue based on the mean score.
            use_all_data:
                If True, will use all feature data from any analyzed thumb,
                not just those from the search intervals.
            use_best_data:
                If True, local search will add the data from the best
                thumbnail found to its knowledge about the feature score
                distributions. Note that this option is irrelevant if
                use_all_data is enabled.
            testing:
                If true, saves the sequence of considered thumbnails to the
                directory specified by testing_dir as
                testing_dir/<video name>/<image> with <image> specified as
                <number>_<frame>_<score>_<feat_score>_<comb_score>_<reason>
            testing_dir:
                Specifies where to save the images, if testing is enabled.
            filter_text:
                Whether or not to remove text from the frames.
            text_filter_params:
                The parameters used to instantiate the text filter. This is a
                list of 9 individual parameters:
                    classifier xml 1 
                        - (str) The first level classifier filename. Must be
                        located in options.text_model_path
                    classifier xml 2 
                        - (str) The second level classifier filename. Must be
                        located in options.text_model_path
                    threshold delta [def: 16]
                        - (int) the number of steps for MSER 
                    min area [def: 0.00015]
                        - (float) minimum ratio of the detection area to the
                        total area of the image for acceptance as a text region.
                    max area [def: 0.003]
                        - (float) maximum ratio of the detection area to the
                        total area of the image for acceptance as a text region.
                    min probability, step 1 [def: 0.8]
                        - (float) minimum probability for step 1 to proceed.
                    non max suppression [def: True]
                        - (bool) whether or not to use non max suppression.
                    min probability difference [def: 0.5]
                        - (float) minimum probability difference for 
                        classification to proceed.
                    min probability, step 2 [def: 0.9]
                        - (float) minimum probability for step 2 to proceed.
            filter_text_thresh: [def: 0.04]
                The fraction of text that occupies the image in order to
                filter it out.

        '''
        self.predictor = predictor
        if self.predictor.async:
            # you're using an asynchronous predictor, so create a lock
            self._result_lock = threading.Lock()
        self.processing_time_ratio = processing_time_ratio
        self._orig_local_search_width = local_search_width
        self._orig_local_search_step = local_search_step
        self.n_thumbs = n_thumbs
        self._feat_score_weight = feat_score_weight
        self.mixing_samples = mixing_samples
        self._search_algo = search_algo
        self.generators = odict()
        self.feats_to_cache = odict()
        self.combiner = combiner
        self.startend_clip = startend_clip
        self.filters = filters
        self.max_variety = max_variety
        self.use_all_data = use_all_data
        self.use_best_data = use_best_data
        self.filter_text_thresh = filter_text_thresh
        # the explore coefficient relates the probability of
        # sampling vs. searching.
        self.explore_coef = 0.9
        # the number of workers to use -- set it to the maximum number of
        # requests the predictor is allowed to issue.
        self.num_workers = self.predictor.concurrency
        self.done_sampling = False
        self.done_searching = False

        if adapt_improve:
            _log.warn(('WARNING: adaptive improvement is enabled, but is '
                       'an experimental feature'))
        self.adapt_improve = adapt_improve
        if testing:
            global TESTING
            TESTING = True
            global TESTING_DIR
            TESTING_DIR = testing_dir
        self._reset()
        self.filter_text = filter_text
        if text_filter_params is None:
            tcnm1 = os.path.join(options.text_model_path,
                                 'trained_classifierNM1.xml')
            tcnm2 = os.path.join(options.text_model_path,
                                 'trained_classifierNM2.xml')
            text_filter_params = [tcnm1, tcnm2, 16, 0.00015, 0.003, 0.8, 
                                  True, 0.5, 0.9]
        else:
            text_filter_params[0] = os.path.join(options.text_model_path, 
                                                 text_filter_params[0])
            text_filter_params[1] = os.path.join(options.text_model_path, 
                                                 text_filter_params[1])
        self.text_filter_params = text_filter_params
        # this, if necessary at all, will be set by update_processing_strategy
        self.analysis_crop = None 
        # determine the generators to cache.
        for f in feature_generators:
            gen_name = f.get_feat_name()
            self.generators[gen_name] = f
            if gen_name in feats_to_cache:
                self.feats_to_cache[gen_name] = f

        # create a processing lock, that will be used by the sampling and
        # the local search threads. 
        self._proc_lock = threading.Condition()

    def _reset(self):
        self.cur_frame = None
        self.video = None
        self.video_name = None
        self.results = None
        self.stats = dict()
        self.fps = None
        self.col_stat = None
        self.num_frames = None
        self._queue = []
        self._searched = 0
        self.done_sampling = False
        self.done_searching = False
        # it's not necessary to reset the search algo, since it will be reset
        # internally when the self.__getstate__() method is called.

    def update_processing_strategy(self, processing_strategy):
        '''
        Changes the state of the video client based on the processing
        strategy. See the ProcessingStrategy object in cmsdb/neondata.py
        '''
        self._reset()
        # handle the text filter parameters
        text_filter_params = processing_strategy.text_filter_params
        text_filter_params[0] = os.path.join(options.text_model_path, 
                                             text_filter_params[0])
        text_filter_params[1] = os.path.join(options.text_model_path, 
                                             text_filter_params[1])
        self.processing_time_ratio = processing_strategy.processing_time_ratio
        self._orig_local_search_width = processing_strategy.local_search_width
        self._orig_local_search_step = processing_strategy.local_search_step
        self.n_thumbs = processing_strategy.n_thumbs
        self._feat_score_weight = processing_strategy.feat_score_weight
        self.mixing_samples = processing_strategy.mixing_samples
        self.max_variety = processing_strategy.max_variety
        self.startend_clip = processing_strategy.startend_clip
        self.adapt_improve = processing_strategy.adapt_improve
        self.analysis_crop = processing_strategy.analysis_crop
        self.filter_text = processing_strategy.filter_text
        self.text_filter_params = text_filter_params
        self.filter_text_thresh = processing_strategy.filter_text_thresh

    @property
    def min_score(self):
        return self.results.min

    def choose_thumbnails(self, video, n=None, video_name='',):
        self._reset()
        rand_seed = int(1000*time()) % 2 ** 32
        _log.info('Beginning thumbnail selection for video %s, random seed '
                  'for this run is %i', video_name, rand_seed)
        np.random.seed(rand_seed)
        thumbs = self.choose_thumbnails_impl(video, n, video_name)
        return thumbs

    def _set_up_testing(self):
        vname = self.video_name
        if vname is None:
            return
        else:
            vdir = os.path.join(TESTING_DIR, vname)
        if os.path.exists(vdir):
            shutil.rmtree(vdir)
        try:
            os.mkdir(vdir)
        except:
            pass
        if os.path.exists(vdir):
            global CUR_TESTING_DIR
            CUR_TESTING_DIR = vdir
        else:
            raise Exception("Could not create testing dir!")

    def choose_thumbnails_impl(self, video, n=None, video_name=''):
        # start up the threads
        self._inq = Queue(maxsize=2)
        threads = [threading.Thread(target=self._worker, args=(n,)) 
                   for n in range(self.num_workers)]
        for t in threads:
            t.start()
        # instantiate the statistics objects required
        # for computing the running stats.
        for gen_name in self.feats_to_cache.keys():
            self.stats[gen_name] = Statistics()
        if n is not None:
            self.n_thumbs = n
        # create a prep object for analysis crops
        self._prep = pycvutils.ImagePrep(crop_frac=self.analysis_crop)
        self.stats['score'] = Statistics()
        self.col_stat = ColorStatistics()
        # instantiate the combiner
        self.combiner._set_stats_dict(self.stats)
        # define the variation measures and requirements
        f_min_var_acc = 0.015
        f_max_var_rej = lambda: min(0.1,
                                    self.col_stat.percentile(
                                            100. / self.n_thumbs))
        self.results = ResultsList(n_thumbs=n, min_acceptable=f_min_var_acc,
                           max_rejectable=f_max_var_rej,
                           feat_score_weight=self._feat_score_weight,
                           adapt_improve=self.adapt_improve,
                           max_variety=self.max_variety,
                           combination_function=self.combiner.result_combine)
        # maintain results as:
        # (score, rtuple, frameno, colorHist)
        #
        # where rtuple is the value to be returned.
        self.video = video
        self.video_name = video_name
        if TESTING:
            self._set_up_testing()
        fps = video.get(cv2.CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
        self.num_frames = num_frames
        # account for the case where the video is very short
        search_divisor = (self._orig_local_search_width /
                          self._orig_local_search_step)
        self.local_search_width = min(self._orig_local_search_width,
                                      max(search_divisor,
                                          self.num_frames / self.n_thumbs))
        self.local_search_step = max(1, self.local_search_width /
                                     search_divisor)
        _log.info('Search width: %i' % (self.local_search_width))
        _log.info('Search step: %i' % (self.local_search_step))
        video_time = float(num_frames) / fps
        self.search_algo = self._search_algo(num_frames, self.local_search_width, 
                                             self.startend_clip)
        start_time = time()
        max_processing_time = self.processing_time_ratio * video_time
        _log.info('Starting search of %s with %i frames, for %s seconds' % (
            video_name, num_frames, max_processing_time))
        self._mix()
        while (time() - start_time) < max_processing_time:
            if self.done_sampling and self.done_searching:
                break
            self._step()
        _log.info('Halting worker threads')
        for t_n in range(len(threads)):
            self._inq.put(None)
        for t in threads:
            t.join()
        raw_results = self.results.get_results()
        # format it into the expected format
        try:
            perc_samp = self.search_algo.n_samples * 100. / self.search_algo.N
            _log.info('%.2f%% of video sampled' % perc_samp)
        except:
            _log.info('Unknown percentage of video sampled')
        try:
            perc_srch = self._searched * 100. / (self.search_algo.N - 1)
            _log.info('%.2f%% of video searched' % perc_srch)
        except:
            _log.info('Unknown percentage of video searched')
        results = []
        if not len(raw_results):
            _log.debug('No suitable frames have been found for video %s!'
                      ' Will uniformly select frames', video_name)
            # increment the statemon
            statemon.state.increment('all_frames_filtered')
            # select which frames to use
            frames = np.linspace(self.search_algo.buffer, 
                                 self.num_frames - self.search_algo.buffer, 
                                 self.n_thumbs).astype(int)
            rframes = [self._get_frame(x) for x in frames]
            for frame, frameno in zip(rframes, frames):
                formatted_result = (frame, 1.0, frameno, 
                                    frameno / float(fps))
                results.append(formatted_result)
        else:
            _log.debug('%i thumbs found', len(raw_results))
            for rr in raw_results:
                formatted_result = (rr[0], rr[1], rr[2], rr[2] / float(fps),
                                    '')
                results.append(formatted_result)
        return results

    def _worker(self, workerno=None):
        '''
        The worker function, which dequeues requests from the input
        queue and issues requests to either the sampler or the local
        searcher.

        The items in the input queue `inq` should consist either of
        None or a tuple of the form (request_type, args) where
        `request_type` is either 'samp' for sample or 'srch' for 
        local search. The args are provided directly to the corresponding
        functions.
        '''
        if workerno is None:
            workerno = 'N/A'
        else:
            workerno = str(workerno)
        _log.debug('Worker %s starting', workerno)
        while True:
            item = self._inq.get()
            if item is None:
                # terminate
                _log.debug('Worker %s', workerno)
                return
            req_type, args = item
            if req_type == 'samp':
                _log.debug('Worker %s taking sample', workerno)
                try:
                    self._take_sample(*(args,))
                except Exception, e:
                    _log.warn('Problem sampling frame %i: %s', args, e.message)
            elif req_type == 'srch':
                _log.debug('Worker %s performing search', workerno)
                try:
                    self._conduct_local_search(*args)
                except Exception, e:
                    start = args[0]
                    stop = args[2]
                    _log.warn('Problem local searching %i <---> %i: %s', 
                        start, stop, e.message)

    def _get_score(self, frame, frameno=None, numretry=1, timeout=10.):
        '''
        Acquires the score for a frame.

        frame: The frame to process, as an openCV-style array.
        frameno: The frame number (if provided)
        numretry: The maximum number of times to retry processing the frame.
        timeout: How long to wait for the frame to be returned.
        '''
        cv = threading.Condition()
        inference_result = []
        def done(result_future):
            '''
            Callback handler.
            '''
            with cv:
                exception = result_future.exception()
                if exception:
                    result_status['error'] = True
                else:
                    result = result_future.result()
                    result_status.append(result.valence[0])
                result_status['done'] = True
                result_status['active'] = False
                cv.notify()
        rem_try = numretry
        if numretry is None:
            rem_try = -1
        while rem_try != 0:
            with cv:
                result_status = {'active': True, 'error': False, 'done': False}
                result_future = self.predictor.predict(frame, timeout=timeout)
                result_future.add_done_callback(
                    lambda result_future: done(result_future))
                while result_status['active']:
                    cv.wait()
                if result_status['done']:
                    return inference_result[0]
                elif result_status['error']:
                    # one attempt has been made, but there was an issue.
                    rem_try -= 1
        if frameno is None:
            fno = 'N/A'
        else:
            fno = str(frameno)
        _log.warn('Frame #%s has exceeded the maximum number of retries (%i).', fno, numretry)
        return None
            
    def _conduct_local_search(self, start_frame, start_score,
                              end_frame, end_score):
        '''
        Given the frames that are already the best, determine whether it makes
        sense to proceed with local search.
        '''
        _log.debug('Local search of %i [%.3f] <---> %i [%.3f]' % (
            start_frame, start_score, end_frame, end_score))
        with self._proc_lock:
            gold, framenos = self.get_search_frame(start_frame)
            if gold is None:
                _log.error('Could not obtain search interval %i <---> %i', 
                            start_frame, end_frame)
                return
            self._searched += 1
            frames = self._prep(gold)
            frame_feats = dict()
            allowed_frames = np.ones(len(frames)).astype(bool)
            # obtain the features required for the filter.

            for f in self.filters:
                fgen = self.generators[f.feature]
                feats = fgen.generate_many(frames)
                if f.feature in self.feats_to_cache:
                    frame_feats[f.feature] = feats
                accepted = f.filter(feats)
                n_rej = np.sum(np.logical_not(accepted))
                n_acc = np.sum(accepted)
                _log.debug(('Filter for feature %s has '
                            'has rejected %i frames, %i remain' % (
                                f.feature, n_rej, n_acc)))
                if not np.any(accepted):
                    _log.debug('No frames accepted by filters')
                    return
                # filter the current features across all feature
                # dicts, as well as the framenos
                acc_idxs = list(np.nonzero(accepted)[0])
                for k in frame_feats.keys():
                    frame_feats[k] = [frame_feats[k][x] for x in acc_idxs]
                framenos = [framenos[x] for x in acc_idxs]
                frames = [frames[x] for x in acc_idxs]
                gold = [gold[x] for x in acc_idxs]
            # ---------- START OF TEXT PROCESSING
            if self.filter_text:
                lower_crop_frac = 0.2  # how much of the lower portion of the
                # image to crop out
                text_d = []
                for cframe in frames:
                    # Cut out the bottom 20% of the image because it often has 
                    # tickers
                    text_det_out = cv2.text.textDetect(
                        cframe[0:int(cframe.shape[0]*.82), :, :],
                        *self.text_filter_params)
                    text_d.append(text_det_out)
                masks = [x[1] for x in text_d]
                # accept only those where tet occupies a sufficiently small amount 
                # of the image.
                accepted = [(np.sum(x > 0) * 1./ x.size) < self.filter_text_thresh 
                            for x in masks]
                n_rej = np.sum(np.logical_not(accepted))
                n_acc = np.sum(accepted)
                _log.debug(('Filter for feature %s has '
                            'has rejected %i frames, %i remain' % (
                                'fancy text detect', n_rej, n_acc)))
                if not np.any(accepted):
                    _log.debug('No frames accepted by filters')
                    return
                # filter the current features across all feature
                # dicts, as well as the framenos
                acc_idxs = list(np.nonzero(accepted)[0])
                for k in frame_feats.keys():
                    frame_feats[k] = [frame_feats[k][x] for x in acc_idxs]
                framenos = [framenos[x] for x in acc_idxs]
                frames = [frames[x] for x in acc_idxs]
                gold = [gold[x] for x in acc_idxs]
            # ---------- END OF TEXT PROCESSING
            for k, f in self.generators.iteritems():
                if k in frame_feats:
                    continue
                if k in self.feats_to_cache:
                    feats = f.generate_many(frames)
                    frame_feats[k] = feats
            # get the combined scores
            comb = self.combiner.combine_scores(frame_feats)
            comb = np.array(comb)
            best_frameno = framenos[np.argmax(comb)]
            best_frame = frames[np.argmax(comb)]
            best_gold = gold[np.argmax(comb)]
            best_feat_dict = {x: frame_feats[x][np.argmax(comb)] for x in
                              frame_feats.keys()}
            feat_score_func = self.combiner.combine_scores_func(best_feat_dict)
            if self.use_all_data:
                # save the data from the analysis, for frames that were not
                # filtered out.
                for featName, cfeats in frame_feats.iteritems():
                    if featName not in self.feats_to_cache:
                        continue
                    for cfidx, fval in enumerate(cfeats):
                        if ((framenos[cfidx] == start_frame) and
                                (framenos[cfidx] == end_frame)):
                            # then it's already been measured
                            continue
                        self.stats[featName].push(fval)
            elif (self.use_best_data and
                      (best_frameno != start_frame) and
                      (best_frameno != end_frame)):
                # save the data from the best identified thumb
                for featName, featVal in best_feat_dict.iteritems():
                    self.stats[featName].push(featVal)
            if TESTING:
                meta = [best_feat_dict,
                        self.combiner.get_indy_funcs(best_feat_dict)]
            else:
                meta = None
            self._proc_lock.notify()
        if self.predictor.async:
            indi_framescore = self._get_score(best_frame, 
                                              frameno=best_frameno)
        else:
            indi_framescore = self.predictor.predict(best_frame)
        with self._proc_lock:
            inter_framescore = (start_score + end_score) / 2
            # interpolate the framescore
            flambda = (best_frameno - start_frame) * 1. / (start_frame - end_frame)
            inter_framescore = (1 - flambda) * start_score + flambda * end_score
            framescore = (indi_framescore + inter_framescore) / 2
            _log.debug(('Best frame from interval %i [%.3f] <---> %i [%.3f]'
                        ' is %i with interp score %.3f and with feature score '
                        '%.3f') % (start_frame,
                                   start_score, end_frame, end_score, best_frameno,
                                   framescore, np.max(comb)))
            self.results.accept_replace(best_frameno, framescore, best_gold,
                                        np.max(comb), meta=meta,
                                        feat_score_func=feat_score_func)
            # IMPORTANT: Exiting a "with" block does *not* wake up other threads
            self._proc_lock.notify()

    def _take_sample(self, frameno):
        '''
        Takes a sample, updating the estimates of mean score, mean image
        variance, mean frame xdiff, etc.
        '''
        with self._proc_lock:
            frames = self.get_seq_frames(
                    [frameno, frameno + self.local_search_step])
            if frames is None:
                # uh-oh, something went wrong! Update the knowledge state of the
                # search algo with the knowledge that the frame is bad.
                self.search_algo.update(frameno, bad=True)
                return
            frames = self._prep(frames)
            # get the score the image (asynchronously).
            self._proc_lock.notify()
        if self.predictor.async:
            # get the score the image (asynchronously).
            frame_score = self._get_score(frames[0], frameno)
        else:
            frame_score = self.predictor.predict(frames[0])
        with self._proc_lock:
            self.stats['score'].push(frame_score)
            _log.debug('Took sample at %i, score is %.3f' % (frameno, frame_score))
            self.search_algo.update(frameno, frame_score)
            # extract all the features we want to cache
            for n, f in self.feats_to_cache.iteritems():
                vals = f.generate_many(frames, fonly=True)
                self.stats[n].push(vals[0])
            # update the knowledge about its variance
            self.col_stat.push(frames[0])
            self._proc_lock.notify()

    def _step(self, force_sample=False):
        '''
        Takes a single step in the search. If force_sample is
        True, then it will force it to take a sample.
        '''

        if force_sample: 
            _log.debug('Taking sample [Forced]')
            if not self.done_sampling:
                frameno = self.search_algo.get_sample()
                if frameno is not None:
                    # then there are still samples to be taken
                    _log.debug('Acquired sample %i', frameno)
                    self._inq.put(('samp', frameno))
                    _log.debug('Sample %i placed in queue', frameno)
                else:
                    self.done_sampling = True
                    _log.info('Finished sampling')
            return
        if ((not self.done_sampling) and 
            (np.random.rand() < self.explore_coef)):
            _log.debug('Taking sample.')
            frameno = self.search_algo.get_sample()
            if frameno is not None:
                # then there are still samples to be taken
                self._inq.put(('samp', frameno))
                return
            else:
                self.done_sampling = True
                _log.info('Finished sampling')
        # okay, let's get a search frame instead.
        _log.debug('Performing search')
        srch_info = self.search_algo.get_search()
        if srch_info is None:
            if self.done_sampling:
                self.done_searching = True
                _log.info('Finished searching')
            return
        self._inq.put(('srch', srch_info))

    def _update_color_stats(self, images):
        '''
        Computes a color similarities for all pairwise combinations of images.
        '''
        colorObjs = [ColorName(img) for img in images]
        dists = []
        for i, j in permutations(range(len(images))):
            dists.append(i.dist(j))
        self._tot_colorname_val[0] = np.sum(dists)
        self._tot_colorname_val[1] = len(dists)
        self._colorname_stat = (self._tot_colorname_val[0] * 1. /
                                self._tot_colorname_val[1])

    def _mix(self):
        '''
        'mix' takes a number of samples from the video. This is
        inspired from the notion of mixing for a Markov chain.
        '''
        _log.info('Taking %i initial samples' % (self.mixing_samples))
        for i in range(self.mixing_samples):
            self._step(force_sample=True)

    # -------------------------------------------------------------------------
    # OBTAINING FRAMES FROM THE VIDEO
    def _get_frame(self, f):
        try:
            more_data, self.cur_frame = pycvutils.seek_video(
                    self.video, f,
                    cur_frame=self.cur_frame)
            if not more_data:
                if self.cur_frame is None:
                    raise model.errors.VideoReadError(
                            "Could not read the video")
            more_data, frame = self.video.read()
        except model.errors.VideoReadError:
            statemon.state.increment('cv_video_read_error')
            frame = None
        except Exception as e:
            _log.exception("Unexpected error when searching through video %s" %
                           self.video_name)
            statemon.state.increment('video_processing_error')
            frame = None
        return frame

    def get_seq_frames(self, framenos):
        '''
        Acquires a series of frames, in sorted order.

        NOTE: This does not ensure that you will not seek off the video. It is
        up to the caller to ensure this is the case.
        '''
        if not type(framenos) == list:
            framenos = [framenos]
        frames = []
        for frameno in framenos:
            frame = self._get_frame(frameno)
            if frame is None:
                return None
            frames.append(frame)
        return frames

    def get_region_frames(self, start, num=1,
                          step=1):
        '''
        Obtains a region from the video.
        '''
        frame_idxs = [start]
        for i in range(num - 1):
            frame_idxs.append(frame_idxs[-1] + step)
        frames = self.get_seq_frames(frame_idxs)
        return frames

    def get_search_frame(self, start_frame):
        '''
        Obtains a search region from the video.
        '''
        num = (self.local_search_width /
               self.local_search_step)
        frames = self.get_region_frames(start_frame, num,
                                        self.local_search_step)
        frameno = range(start_frame,
                        start_frame + self.local_search_width + 1,
                        self.local_search_step)
        return frames, frameno

    # END OBTAINING FRAMES FROM THE VIDEO
    # -------------------------------------------------------------------------

    def __getstate__(self):
        self._reset()
        return self.__dict__.copy()

    def get_name(self):
        return 'LocalSearcher'
