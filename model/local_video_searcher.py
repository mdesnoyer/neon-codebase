'''
New video searcher. Implements:
    - local search (to circumvent closed-eyes, blurry, etc...)
    - Metropolis-Hastings sampling
    - Inverse filter / score order (3x speedup)

NOTE:
This no longer inherits from the VideoSearcher() object, I'm not
sure if we want to change how this works in the future.

NOTE:
It's not clear how passing the filters themselves will work.

NOTE:
Because of how local_video_searcher works, it currently searches
at least every other interval but is not guaranteed to search them
all given enough time (i.e., if a new search frame is between two
of them that have already been searched). 
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
from utils.runningstat import Statistics
from utils.pycvutils import seek_video
from model.metropolisHastingsSearch import ThumbnailResultObject, MonteCarloMetropolisHastings

_log = logging.getLogger(__name__)

statemon.define('all_frames_filtered', int)
statemon.define('cv_video_read_error', int)
statemon.define('video_processing_error', int)
statemon.define('low_number_of_frames_seen', int)



# __base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# if sys.path[0] != __base_path__:
#     sys.path.insert(0, __base_path__)


# TODO: Determine how we will provide filters to the local searcher
#       Right now, we're assuming that we will have a text filter and
#       a closed eye filter.

# class ThumbVariation(object):
#     '''
#     This class keeps track of the variation
#     of the top thumbnails, and also indicates
#     whether or not a new thumbnail is permissible
#     to add to the top thumbnails. 

#     A new thumbnails is appropriate to add to the
#     top thumbnail list if the distance to the
#     'closest' thumbnail is greater than closest
#     thumbnail pair thus found.
#     '''
#     def __init__(self, n_thumbs):
#         # instantiate a similarity matrix
#         self.sim_matrix = np.zeros((n_thumbs, n_thumbs))
#         self._tot_thumbs = 0
#         # instantiate a dictionary that will map
#         # thumbnail ids to their index in the
#         # similarity matrix.
#         self._id_to_idx = dict()

class Criterion(object):
    '''
    A class that represents a filtering
    criterion. Criteria come in several flavors,
    but all are initialized with any of the following:
        - a Statistics() object
        - threshold values
        - lambda functions of these

    Each criteria may be of the following
    types:
        - judge interval
            Either accepts or rejects an entire interval. If
            this is the case, it accepts just the search frames
            (i.e., the start and the end) statistics.
        - filter interval
            Returns a subset (or none at all) of the frames
            in an interval that are acceptable.
        - winner selector
            Returns a single winning frame
        - ranking criteria
            Returns the frame numbers in ranked-order
            according to some ranking function
    '''


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
            criteria:
                A list of criterion for conducting the local search based on
                the values of the features. TODO: what the hell is this gonna
                look like?
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
