'''
==============================================================================
New video searcher. Implements:
    - local search (to circumvent closed-eyes, blurry, etc...)
    - Metropolis-Hastings sampling
    - Inverse filter / score order (3x speedup)
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
'''

import hashlib
import heapq
import logging
import os
import sys
import threading
from time import time
import traceback
from Queue import Queue
from itertools import permutations
from collections import OrderedDict as odict
from collections import defaultdict as ddict
from random import getrandbits

__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import ffvideo
import model.errors
import model.features as feat
import numpy as np
import utils.obj
from model.colorname import ColorName
from utils import pycvutils, statemon
from utils.pycvutils import seek_video
from model.metropolisHastingsSearch import ThumbnailResultObject
from model.metropolisHastingsSearch import MCMH_rpl

_log = logging.getLogger(__name__)

statemon.define('all_frames_filtered', int)
statemon.define('cv_video_read_error', int)
statemon.define('video_processing_error', int)
statemon.define('low_number_of_frames_seen', int)

MINIMIZE = -1     # flag for statistics where better = smaller
NORMALIZE = 0     # flag for statistics where better = closer to mean
MAXIMIZE = 1      # flag for statistics where better = larger
PEN_LOW_HALF = -2 # flag for statistics where better > median
PEN_HIGH_HALF = 2 # flag for statistics where better < median

class Statistics(object):
    '''
    Replicates (to a degree) the functionality of the true running statistics
    objects (which are in the utils folder under runningstat). This is because
    it is unlikely that we will ever need to maintain a very large number of
    measurements.

    If init is not None, it initializes with the values provided.
    '''
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
        '''
        pushes a value onto x
        '''
        self._update_var = True
        self._update_mean = True
        if type(x) == list:
            for ix in x:
                self.push(ix)
        if self._count == self._max_size:
            # randomly replace one
            idx = np.random.choice(self._max_size)
            self._vals[idx] = x
        else:
            self._vals[self._count] = x
            self._count += 1 # increment count

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
            self._p_median = np.median(self._dists)
            self._update_median = False
        return self._p_median

    def rank(self, x):
        '''Returns the rank of x'''
        quant = np.sum(self._vals[:self._count] < x)
        return quant * 1./max(1, self._count)

    def percentile(self, x):
        '''Returns the xth percentile of the stored elements. x may be a float
        between 0 and 100, inclusive. This is fairly computationally
        expensive, however it's only being used when new thumbnails are being
        added to the top thumbnails list.'''
        return np.percentile(self._vals[:self._count], x)

class ColorStatistics(object):
    '''
    Similar to the Statistics object (defined below), but in lieu of storing
    numeric values, it stores color histograms. This will allow us to
    establish a lower bound on the allowable distances--by empirically
    establishing the mean and standard deviation of the pairwise distances
    between frames in the video.

    IMPORTANTLY, this flavor of the running statistic does NOT support
    initialization with a set of ColorName objects.
    '''
    def __init__(self, max_size=150):
        '''
        Parameters:
            max_size = the maximum number of color histograms to store.
        '''
        self._ColObjs = []
        self._max_size = max_size
        self._count = 0
        self._dists = Statistics()

    def push(self, img):
        '''
        Add an image into the statistics object. Unlike the vanilla Statistics
        object, this does *not* support pushing multiple items simultaneously.
        '''
        cn = ColorName(img)
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
        '''
        The notion of Rank doesnt have much meaning in this sense, since
        we are measuring how different the thumbnails are from each other
        in the aggregate.
        '''
        return self._dists.percentile(x)

class Combiner(object):
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
                valence = MINIMIZE # assume you are trying to maximize it
            for v in feat_vec:
                rank = self._stats_dict[feat_name].rank(v)
                if valence == MINIMIZE:
                    rank = 1. - rank
                if valence == NORMALIZE:
                    rank = 1. - abs(0.5 - rank)*2
                if valence == PEN_LOW_HALF:
                    rank = 1. + min(0, rank - 0.5) * 2
                if valence == PEN_HIGH_HALF:
                    rank = 1. - max(0, rank - 0.5) * 2
                vals.append(rank * self.weight_dict[feat_name])
            return vals

        else:
            return [x * self.weight_dict[feat_name] for x in feat_vec]
        return feat_vec

    def combine_scores(self, feat_dict):
        '''
        Returns the scores for the thumbnails given a feat_dict, which is a
        dictionary {'feature name': feature_vector}
        '''
        stat_scores = []
        for k, v in feat_dict.iteritems():
            stat_scores.append(self._compute_stat_score(k, v))
        comb_scores = []
        for x in zip(*stat_scores):
            comb_score = self._combine(x)
            comb_score /= (1. * len(x)) # normalize
            comb_scores.append(self._combine(x))
        return comb_scores

class _Result(object):
    '''
    Private class to be used by the ResultsList object. Represents an
    invidiual top-result image in the top results list.

    Note: comb_score is not the score output by the combiner, but rather
    the combination of the feature score (feat_score, which is output by
    the combiner) and the score, as a function of the comb_score_weight)
    '''
    def __init__(self, frameno=None, score=-np.inf, image=None,
                 feat_score=None, meta=None, comb_score=None,
                 comb_score_weight=None):
        self._defined = False
        if score and frameno:
            self._defined = True
            _log.debug(('Instantiating result object at frame %i with'
                           ' score %.3f')%(frameno, score))
        self.score = score
        self.frameno = frameno
        self._feat_score = feat_score
        self._hash = getrandbits(128)
        self.image = image
        if self.image != None:
            self._color_name = ColorName(image)
        else:
            self._color_name = None
        self.meta = meta
        self._comp_comb_score(comb_score_weight)

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
        if self._feat_score:
            return 'Result fr:%i sc:%.2f feat sc:%.2f comb sc:%.2f'%(
                        self.frameno, self.score, self._feat_score,
                        self.comb_score)
        else:
            return 'Result fr:%i sc:%.2f feat sc:N/A comb sc:%.2f'%(
                        self.frameno, self.score, self.comb_score)

    def _comp_comb_score(self, comb_score_weight):
        if comb_score_weight == None:
            self.comb_score = self.score
            return
        if self._feat_score == None:
            self.comb_score = self.score
            return
        self.comb_score = self.score + self._feat_score * comb_score_weight

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
            return np.inf # the same object is infinitely different from itself
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
                 max_rejectable=0.2, comb_score_weight=0.):
        self._max_variety = max_variety
        self.n_thumbs = n_thumbs
        self.reset()
        self._min_acceptable = min_acceptable
        self._max_rejectable = max_rejectable
        self._comb_score_weight = comb_score_weight
        self._testing = False
        self._testing_dir = None
        self._considered_thumbs = 0

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

    def _set_testing(self, testing_dir):
        '''
        Sets up the testing directory.
        '''
        self._testing = True
        self._testing_dir = testing_dir

    def _write_testing_frame(self, res, reason=None, idx=None):
        '''
        Creates a filename for the current result object. Reason
        is either 'accept' or 'reject'. If accept, must provide
        an index for the position in the results list.
        '''
        if not self._testing:
            return
        _log.debug('TESTING ENABLED: saving %s'%res)
        fname = '%04i_'%(self._considered_thumbs)
        fname += '%05i_'%(res.frameno)
        fname += '%04.2f_'%(res.score)
        if res._feat_score == None:
            fname += 'NA_'
        else:
            fname += '%04.2f'%(res._feat_score)
        fname += '%04.2f_'%(res.comb_score)
        if reason == 'accept':
            fname += 'accepted_replacing_%i.jpg'%(idx)
        else:
            fname += 'rejected.jpg'
        fname = os.path.join(self._testing_dir, fname)
        cv2.imwrite(fname, res.image)

    def reset(self, n_thumbs=None):
        _log.debug('Result object of size %i resetting'%(self.n_thumbs))
        self.results = [_Result() for x in range(self.n_thumbs)]
        self.min = 0
        if n_thumbs != None:
            self.n_thumbs = n_thumbs
        self.dists = np.zeros((self.n_thumbs, self.n_thumbs))

    def _update_dists(self, entry_idx):
        for idx in range(len(self.results)):
            dst = self.results[idx].dist(self.results[entry_idx])
            self.dists[idx, entry_idx] = dst
            self.dists[entry_idx, idx] = dst

    def accept_replace(self, frameno, score, image=None, feat_score=None,
                       meta=None):
        '''
        Attempts to insert a result into the results list. If it does not
        qualify, it returns False, otherwise returns True
        '''
        res = _Result(frameno, score, image, feat_score, meta,
                      self._comb_score_weight)
        self._considered_thumbs += 1
        if score < self.min:
            _log.warn('Frame %i [%.3f] rejected due to score'%(frameno,
                                                                   score))
            self._write_testing_frame(res)
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
        old = self.results[sco_by_idx[0]]
        _log.info('%s is replacing %s'%(res, old))
        self._update_dists(idx)
        if old.score == self.min:
            self._update_min()
        self.results[idx] = res
        self._write_testing_frame(res, 'accept', idx)
        return True

    def _maxvar_replace(self, res):
        '''
        Replaces the lowest scoring result possible while maximizing variance.
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
                        'video to be accepted')%(res))
            return self._push_over_lowest(res)

        if dists[arg_srt_idx[0]] < self.min_acceptable:
            _log.debug(('%s is insufficiently different given the variety '
                        'seen in the video so far.')%(res))
            self._write_testing_frame(res)
            return False
        # otherwise, iterate over the lowest scoring ones and replace the
        # lowest one that is 'less different' than you are from the
        # remaining thumbnails
        sco_by_idx = np.argsort([x.comb_score for x in self.results])
        for idx in sco_by_idx:
            if self.results[idx].comb_score > res.comb_score:
                # none of the current thumbnails can be replaced
                _log.debug('There are no low-scoring less-varied thumbnails for %s'%(res))
                self._write_testing_frame(res)
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
        _log.debug('New minimum score is %.3f'%(new_min))
        self.min = new_min

    def get_results(self):
        '''
        Returns the results in sorted order, sorted by score. Returns them
        as (image, score, frameno)
        '''
        _log.debug('Dumping results')
        sco_by_idx = np.argsort([x.score for x in self.results])
        res = []
        for idx in sco_by_idx:
            res_obj = self.results[idx]
            if not res_obj._defined:
                statemon.state.increment('low_number_of_frames_seen')
                break
            res.append([res_obj.image, res_obj.score, res_obj.frameno])
        return res

class LocalSearcher(object):
    def __init__(self, predictor, face_finder,
                 eye_classifier,
                 processing_time_ratio=1.0,
                 local_search_width=32,
                 local_search_step=4,
                 n_thumbs=5,
                 comb_score_weight=0.5,
                 mixing_samples=10,
                 search_algo=MCMH_rpl,
                 max_variety=True,
                 feature_generators=None,
                 feats_to_cache=None,
                 combiner=None,
                 filters=None,
                 testing=False,
                 testing_dir='/tmp'):
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
            comb_score_weight:
                The degree to which the combined feature score should effect
                the rank of the frames. New frames are added to results
                according to score + comb_score * comb_score_weight
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
                (features are only cached during sampling)
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
            testing:
                If true, saves the sequence of considered thumbnails to the
                directory specified by testing_dir as
                testing_dir/<video name>/<image> with <image> specified as
                <number>_<frame>_<score>_<feat_score>_<comb_score>_<reason>
            testing_dir:
                Specifies where to save the images, if testing is enabled.

        '''
        self.predictor = predictor
        self.processing_time_ratio = processing_time_ratio
        self.local_search_width = local_search_width
        self.local_search_step = local_search_step
        self.n_thumbs = n_thumbs
        self._comb_score_weight = comb_score_weight
        self.mixing_samples = mixing_samples
        self.search_algo = search_algo(local_search_width)
        self.generators = odict()
        self.feats_to_cache = odict()
        self.combiner = combiner
        self.filters = filters
        self._testing = testing
        self._testing_dir = testing_dir
        self._reset()

        # determine the generators to cache.
        for f in feature_generators:
            gen_name = f.get_feat_name()
            self.generators[gen_name] = f
            if gen_name in feats_to_cache:
                self.feats_to_cache[gen_name] = f

    def _reset(self):
        self.cur_frame = None
        self.video = None
        self.video_name = None
        self.results = None
        self.stats = dict()
        self.fps = None
        self.col_stat = None
        # it's not necessary to reset the search algo, since it will be reset
        # internally when the self.__getstate__() method is called.

    @property
    def min_score(self):
        return self.results.min

    def choose_thumbnails(self, video, n=1, video_name=''):
        thumbs = self.choose_thumbnails_impl(video, n, video_name)
        return thumbs

    def _set_up_testing(self):
        vname = self.video_name
        if vname == None:
            vdir = os.path.join(self._testing_dir, 'default')
        else:
            vdir = os.path.join(self._testing_dir, vname)
        try:
            os.mkdir(vdir)
        except:
            pass
        if os.path.exists(vdir):
            self.results._set_testing(vdir)
            return vdir
        else:
            raise Exception("Could not create testing dir!")

    def choose_thumbnails_impl(self, video, n=1, video_name=''):
        # instantiate the statistics objects required
        # for computing the running stats.
        for gen_name in self.feats_to_cache.keys():
            self.stats[gen_name] = Statistics()
        self.stats['score'] = Statistics()
        self.col_stat = ColorStatistics()
        # instantiate the combiner
        self.combiner._set_stats_dict(self.stats)
        # define the variation measures and requirements
        f_min_var_acc = lambda: min(0.015,
                                        self.col_stat.percentile(
                                            1./self.n_thumbs))
        f_max_var_rej = lambda: max(0.25, self.col_stat.percentile(40.))
        self.n_thumbs = n
        self.results = ResultsList(n_thumbs=n, min_acceptable=f_min_var_acc,
                                   max_rejectable=f_max_var_rej,
                                   comb_score_weight=self._comb_score_weight)
        # maintain results as:
        # (score, rtuple, frameno, colorHist)
        #
        # where rtuple is the value to be returned.
        self.video = video
        self.video_name = video_name
        if self._testing:
            self._set_up_testing()
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        video_time = float(num_frames) / fps
        self.search_algo.start(num_frames)
        start_time = time()
        max_processing_time = self.processing_time_ratio * video_time
        _log.info('Starting search of %s with %i frames, for %s seconds'%(
                        video_name, num_frames, max_processing_time))
        while (time() - start_time) < max_processing_time:
            r = self._step()
            if r == False:
                _log.info('Searched whole video')
                # you've searched as much as possible
                break
        raw_results = self.results.get_results()
        # format it into the expected format
        results = []
        for rr in raw_results:
            formatted_result = (rr[0], rr[1], rr[2], rr[2] / float(fps),
                                '')
            results.append(formatted_result)
        return results

    def _conduct_local_search(self, start_frame, end_frame,
                              start_score, end_score):
        '''
        Given the frames that are already the best, determine whether it makes
        sense to proceed with local search.
        '''
        _log.debug('Local search of %i [%.3f] <---> %i [%.3f]'%(
                    start_frame, start_score, end_frame, end_score))
        if not self._should_search(start_score, end_score):
            return
        frames, framenos = self.get_search_frame(start_frame)
        if frames == None:
            # uh-oh, something went wrong! In this case, the search region
            # will not be searched again, and so we don't have to worry about
            # updating the knowledge of the search algo.
            return
        frame_feats = dict()
        allowed_frames = np.ones(len(frames)).astype(bool)
        # obtain the features required for the filter.

        for f in self.filters:
            fgen = self.generators[f.feature]
            feats = fgen.generate_many(frames)
            frame_feats[f.feature] = feats
            accepted = f.filter(feats)
            n_rej = np.sum(np.logical_not(accepted))
            n_acc = np.sum(accepted)
            _log.debug(('Filter for feature %s has '
                        'has rejected %i frames, %i remain'%(
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
            #     frame_feats[k] = [x for n, x in enumerate(frame_feats[k])
            #                         if accepted[n]]
            # framenos = [x for n, x in enumerate(frames) if accepted[n]]
            # frames = [x for n, x in enumerate(frames) if accepted[n]]
        for k, f in self.generators.iteritems():
            if k in frame_feats:
                continue
            frame_feats[k] = f.generate_many(frames)
        # get the combined scores
        comb = self.combiner.combine_scores(frame_feats)
        comb = np.array(comb)
        best_frameno = framenos[np.argmax(comb)]
        best_frame = frames[np.argmax(comb)]
        _log.debug(('Best frame from interval %i [%.3f] <---> %i [%.3f]'
                    ' is %i with feature score %.3f')%(start_frame,
                            start_score, end_frame, end_score, best_frameno,
                            np.max(comb)))
        # the selected frame (whatever it may be) will be assigned
        # the score equal to mean of its boundary frames.
        framescore = (start_score + end_score) / 2
        # push the frame into the results object.
        self.results.accept_replace(best_frameno, framescore, best_frame,
                                    np.max(comb), meta=frame_feats)

    def _take_sample(self, frameno):
        '''
        Takes a sample, updating the estimates of mean score, mean image
        variance, mean frame xdiff, etc.
        '''
        frames = self.get_seq_frames(
                    [frameno, frameno + self.local_search_step])
        if frames == None:
            # uh-oh, something went wrong! Update the knowledge state of the
            # search algo with the knowledge that the frame is bad.
            self.search_algo.update(frameno, bad=True)
            return
        # get the score the image.
        frame_score = self.predictor.predict(frames[0])
        # extract all the features we want to cache
        for n, f in self.feats_to_cache.iteritems():
            vals = f.generate_many(frames, fonly=True)
            self.stats[n].push(vals[0])
        # update the knowledge about its variance
        self.col_stat.push(frames[0])
        self.stats['score'].push(frame_score)
        _log.debug('Took sample at %i, score is %.3f'%(frameno, frame_score))
        # update the search algo's knowledge
        self.search_algo.update(frameno, frame_score)

    def _should_search(self, start_score, end_score):
        '''
        Accepts a start frame score and the end frame score and returns True /
        False indicating if this region should be searched.

        Regions are searched if and only if:
            - the mean of their score exceeds the min of the results.
            - mean of their scores exceeds the mean score observed so far.
        '''
        mean_score = (start_score + end_score) / 2.
        if mean_score > self.min_score:
            if mean_score > self.stats['score'].median:
                _log.debug('Interval should be searched')
                return True
            else:
                _log.debug('Interval can be in results but does not exceed'
                           ' mean observed score data')
        else:
            _log.debug('Interval cannot be admitted to results')
        return False

    def _step(self):
        r = self.search_algo.get()
        if r == None:
            return False
        action, meta = r
        if action == 'sample':
            self._take_sample(meta)
        else:
            self._conduct_local_search(*meta)
        return True

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
        self._colorname_stat = (self._tot_colorname_val[0] * 1./
                                self._tot_colorname_val[1])

    def _mix(self):
        '''
        'mix' takes a number of equispaced samples from the video. This is
        inspired from the notion of mixing for a Markov chain.
        '''
        _log.info('Mixing before search begins for %i frames'%(
                                                    self.mixing_samples))
        num_frames = self.mixing_samples
        samples = np.linspace(0, num_frames,
                              self.mixing_samples+2).astype(int)
        samples = [self.search_algo.get_nearest(x) for x in samples]
        samples = list(np.unique(samples))
        # we need to be able to compute the SAD, so we need to
        # also insert local search steps
        for frameno in samples:
            self._take_sample(framemno)

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
            if frame == None:
                return None
            frames.append(frame)
        return frames

    def get_region_frames(self, start, num=1,
                          step=1):
        '''
        Obtains a region from the video.
        '''
        frame_idxs = [start]
        for i in range(num-1):
            frame_idxs.append(frame_idxs[-1]+step)
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

    def __getstate__(self):
        self._reset()
        return self.__dict__.copy()

    def get_name(self):
        return 'LocalSearcher'
