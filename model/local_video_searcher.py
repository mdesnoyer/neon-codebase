'''
New video searcher. Implements:
    - local search (to circumvent closed-eyes, blurry, etc...)
    - Metropolis-Hastings sampling
    - Inverse filter / score order (3x speedup)

NOTE:
This no longer inherits from the VideoSearcher() object, I'm not
sure if we want to change how this works in the future.

NOTE:
While this initially used Statistics() objects to calculate running
statistics, in principle even with a small search interval (32 frames)
and a very long video (2 hours), we'd only have about 5,000 values to
store, which we can easily manage. Thus we will hand-roll our own Statistics
objects. 
'''

import hashlib
import heapq
import logging
import os
import sys
import threading
import time
import traceback
from Queue import Queue
from itertools import permutations

import cv2
import ffvideo
import model.errors
import model.features as feat
import numpy as np
import utils.obj
from model import colorname
from model.video_searcher import VideoSearcher
from utils import pycvutils, statemon
#from utils.runningstat import Statistics
from utils.pycvutils import seek_video
from model.metropolisHastingsSearch import ThumbnailResultObject, MonteCarloMetropolisHastings

_log = logging.getLogger(__name__)

statemon.define('all_frames_filtered', int)
statemon.define('cv_video_read_error', int)
statemon.define('video_processing_error', int)
statemon.define('low_number_of_frames_seen', int)

MINIMIZE = -1   # flag for statistics where better = smaller
NORMALIZE = 0   # flag for statistics where better = closer to mean
MAXIMIZE = 1    # flag for statistics where better = larger

# __base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# if sys.path[0] != __base_path__:
#     sys.path.insert(0, __base_path__)

class Statistics(object):
    '''
    Replicates (to a degree) the functionality of the
    true running statistics objects (which are in the
    utils folder under runningstat). This is because
    it is unlikely that we will ever need to maintain
    a very large number of measurements. 

    If init is not None, it initializes with the values
    provided.
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
        if init is not None:
            self.push(init)

    def push(self, x):
        '''
        pushes a value onto x
        '''
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

    def var(self):
        return np.var(self._vals[:self._count])

    def mean(self):
        return np.mean(self._vals[:self._count])

    def rank(self, x):
        '''Returns the rank of x'''
        quant = np.sum(self._vals[:self._count] < x)
        return quant * 1./self._count

class Combiner(object):
    '''
    Combines arbitrary feature vectors according
    to either (1) predefined weights or (2) attempts
    to deduce the weight given the global statistics
    object.
    '''
    def __init__(self, stats_dict, weight_dict=None,
                 weight_valence=None, combine=lambda x: np.sum(x)):
        '''
        stats_dict is a dictionary of {'stat name': Statistics()}
        weight_dict is a dictionary of {'stat name': weight} which
            yields absolute weights.
        weight_valence is a dictionary of {'stat name': valence} 
            encoding, which indicates whether 'better' is higher,
            lower, or maximally typical.
        combine is an anonymous function to combine scored statistics;
            combine must have a single argument and be able to operate
            on lists of floats.
        Note: if a statistic has an entry in both the stats and
            weights dict, then weights dict takes precedence.
        '''
        self._stats_dict = stats_dict
        self.weight_dict = weight_dict
        self.weight_valence = weight_valence
        self._combine = combine

    def _compute_stat_score(self, feat_name, feat_vec):
        '''
        Computes the statistics score for a feature vector.
        If it has a defined weight, then we simply return the
        product of this weight with the 
        '''
        if self.weight_dict.has_key(feat_name):
            return [x * self.weight_dict[feat_name] for x in feat_vec]
        
        if self._stats_dict.has_key(feat_name):
            vals = []
            if self.weight_valence.has_key(feat_name):
                valence = self.weight_valence[feat_name]
            else:
                valence = MINIMIZE # assume you are trying to maximize it
            for v in feat_vec:
                rank = self._stats_dict[feat_name].rank()
                if valence == MINIMIZE:
                    rank = 1. - rank
                if valence == NORMALIZE:
                    rank = 1. - abs(0.5 - rank)*2
                vals.append(rank)
            return vals

        return feat_vec

    def combine_scores(self, feat_dict):
        '''
        Returns the scores for the thumbnails given a feat_dict,
        which is a dictionary {'feature name': feature_vector}
        '''
        stat_scores = []
        for k, v in feat_dict.iteritems():
            stat_score.append(self._compute_stat_score(k, v))
        comb_scores = []
        for x in zip(*stat_score):
            comb_scores.append(self._combine(x))

class LocalSearcher(object):
    def __init__(self, predictor, face_finder,
                 eye_classifier, 
                 processing_time_ratio=1.0,
                 local_search_width=64,
                 local_search_step=8,
                 n_thumbs=5,
                 mixing_samples=10,
                 search_algo=MCMH_rpl,
                 feature_generators=None,
                 feats_to_cache=None,
                 combiner=None,
                 filters=None,
                 criteria=None):
        '''
        Inputs: 
            predictor:
                computes the score for a given image
            local_search_width:
                The number of frames to search forward.
            local_search_step:
                The step size between adjacent frames.
                ===> for instance, if local_search_width = 6 
                     and local_search_step = 2, then it will
                     obtain 6 frames across 12 frames (about 0.5 sec) 
            n_thumbs:
                The number of top images to store.
            mixing_samples:
                The number of samples to draw to establish baseline
                statistics.
            search_algo:
                Selects the thumbnails to try; accepts the number of elements
                over which to search. Should support asynchronous result updating,
                so it is easy to switch the predictor between sequential (CPU-based)
                and non-sequential (GPU-based) predictor methods.
            feature_generators:
                A list of feature generators. Note that this have to be of the
                RegionFeatureGenerator type.
            feats_to_cache:
                The name of all features to save as running statistics.
                (features are only cached during sampling)
            combiner:
                Combines the feature scores. See class definition above.
            filters:
                A lists tuples or lists of tuples of (feature_name, filter). If we have:
                [[(feature1, filter1), (feature2, filter2)], [(feature3, filter3)]]

                Then the first two features are extracted and the first two filters
                applied. The third feature is obtained only for images which pass
                the first 
        '''
        self.predictor = predictor
        self.local_search_width = local_search_width
        self.local_search_step = local_search_step
        self.n_thumbs = n_thumbs
        self.mixing_samples = mixing_samples
        self.search_algo = search_algo(local_search_width)
        self.generators = feature_generators
        self.generator_names = []
        self.feats_to_cache = []
        self.feats_to_cache_name = []
        self.criteria = criteria
        self.cur_frame = None
        self.video = None
        self.results = None
        self.stats = dict()

        # determine the generators to cache.
        for f in self.feature_generators:
            gen_name = f.get_feat_name()
            self.generator_names.append(gen_name)
            if gen_name in self.feats_to_cache:
                self.feats_to_cache.append(f)
                self.feats_to_cache_name.append(gen_name)

    def choose_thumbnails(self, video, n=1, video_name=''):
        thumbs = self.choose_thumbnails_impl(video, n, video_name)
        return thumbs

    def choose_thumbnails_impl(self, video, n=1, video_name=''):
        # instantiate the statistics objects required
        # for computing the running stats.
        for gen_name in self.feats_to_cache_name:
            self.stats[gen_name] = Statistics()
        self.stats['score'] = Statistics()
        self.stats['colorname'] = Statistics()

        self._samples = []
        self.results = []
        # maintain results as:
        # (score, rtuple, frameno, colorHist)
        #
        # where rtuple is the value to be returned.
        self.video = video
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        video_time = float(num_frames) / fps
        self.search_algo.start(num_frames)

    def _conduct_local_search(self, start_frame, end_frame, 
                              start_score, end_score):
        '''
        Given the frames that are already the best, determine
        whether it makes sense to proceed with local search. 
        '''
        # open question: how do we choose how to combine
        # blur, text and face data?
        # 
        # further, in the case of multiple faces, what is
        # an appropriate action to take?

    def _take_sample(self, frameno):
        '''
        Takes a sample, updating the estimates of mean score,
        mean image variance, mean frame xdiff, etc.
        '''
        imgs = self.get_seq_frames(self.video,
                    [frameno, frameno + self.local_search_step])
        # get the score the image.
        frame_score = self.predictor.predict(imgs[0])
        # extract all the features we want to cache
        for n, f in zip(self.feats_to_cache_name, 
                        self.feats_to_cache):
            vals = f.generate(imgs, fonly=True)
            self.stats[n].push(vals[0])
        self.stats['score'].push(frame_score)
        # update the search algo's knowledge
        self.search_algo.update(frameno, frame_score)

    def _should_search(self, srchTupl):
        '''
        Accepts a tuple about the search interval
        and returns True / False based on whether
        this region should be searched given the
        searching criteria.
        '''
        # the search criteria so far is:
        # - the search interval should be (a) above 
        #   the average of the mean score and (b) at
        #   least one frame should be higher scoring
        #   than any of the ones searched so far.

    def _step(self):
        r = self.search_algo.get()
        if r == None:
            return False
        action, meta = r
        if action == 'sample':
            self._take_sample(meta)
        else:
            self._conduct_local_search(*meta)

    def _update_color_stats(self, images):
        '''
        Computes a color similarities for all
        pairwise combinations of images.
        '''
        colorObjs = [ColorNameCache(img) for img in images]
        dists = []
        for i, j in permutations(range(len(images))):
            dists.append(i.dist(j))
        self._tot_colorname_val[0] = np.sum(dists)
        self._tot_colorname_val[1] = len(dists)
        self._colorname_stat = (self._tot_colorname_val[0] * 1./
                                self._tot_colorname_val[1])
            
    def _mix(self, num_frames):
        '''
        'mix' takes a number of equispaced samples from
        the video. 
        '''
        samples = np.linspace(0, num_frames, 
                              self.mixing_samples+2).astype(int)
        samples = [self.search_algo.get_nearest(x) for x in samples]
        samples = list(np.unique(samples))
        # we need to be able to compute the SAD, so we need to
        # also insert local search steps
        for frameno in samples:
            framenos = [frameno, frameno + self.local_search_step]
            imgs = self.get_seq_frames(self.video, framenos)

            SAD = self.compute_SAD(imgs)
            self._SAD_stat.push(SAD[0])

            pix_val = np.max(np.var(np.var(imgs[0],0),0))
            self._pixel_stat.push(pix_val)

            frame_score = self.predictor.predict(imgs[0])
            self.search_algo.update(frameno, frame_score)
            self._score_stat.push(frame_score) 

    def _get_frame(self, f):
        more_data, self.cur_frame = pycvutils.seek_video(
                                    self.video, f, 
                                    cur_frame=self.cur_frame)
        if not more_data:
            if self.cur_frame is None:
                raise model.errors.VideoReadError(
                    "Could not read the video")
        more_data, frame = self.video.read() 
        return frame

    def get_seq_frames(self, framenos):
        '''
        Acquires a series of frames, in sorted
        order.

        NOTE: This does not ensure that you will not
        seek off the video. It is up to the caller
        to ensure this is the case.
        '''
        if not type(framenos) == list:
            framenos = [framenos]
        frames = []
        for frameno in framenos:
            frame = self._get_frame(frameno)
            frames.append(frame)
        return frames

    def get_region_frames(self, start, num=1,
                          step=0):
        '''
        Obtains a region from the video.
        '''
        frame_idxs = [start]
        for i in range(num-1):
            frame_idxs.append(frame_idxs[-1]+step)
        frames = get_seq_frames(framenos)
        return frames
