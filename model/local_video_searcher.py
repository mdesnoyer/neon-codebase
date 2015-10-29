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
import model.features
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

class ColorNameCached(ColorName):
    '''
    Replicates the function of ColorName class, but
    computes the color histogram immediately, and
    caches it
    '''
    def __init__(self, image):
        super(ColorNameCached, self).__init__(image)
        self.hist = self.get_colorname_histogram()

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



class LocalSearcher(VideoSearcher):
    def __init__(self, predictor,
                 startend_buffer=0.1,
                 max_startend_buffer=5.0,
                 processing_time_ratio=1.0,
                 local_search_width=64,
                 local_search_step=8,
                 retain_scores=True,
                 soft_filtering=False,
                 max_im_var=True,
                 n_thumbs=5,
                 text_std_mult=1,
                 scene_diff_std_mult=2,
                 text_filt=None,
                 face_filt=None,
                 mixing_time=10,
                 search_algo=MonteCarloMetropolisHastings):
        '''
        Inputs: (those distinct from abstract class)
            local_search_width:
                The number of frames to search forward.
            local_search_step:
                The step size between adjacent frames.
                ===> for instance, if local_search_width = 6 
                     and local_search_step = 2, then it will
                     obtain 6 frames across 12 frames (about 0.5 sec) 
            retain_scores:
                Save the obtained filtering scores.
            soft_filtering:
                Always run all the filters, retaining
                scores. Use filter scores to rank, not
                to exclude.
            max_im_var:
                Only add an image to the heap when it increases
                the variety of the images.
            n_thumbs:
                The number of top images to store.
            scene_diff_mult:
                The frames must be >= <mean_SAD> - scene_diff_std_mult * <std_SAD>
            text_filt:
                The text filter
            face_filt:
                The closed eye / face filter
            mixing_samples:
                The number of samples to draw to establish baseline
                statistics.
            search_algo:
                Selects the thumbnails to try; accepts the number of elements
                over which to search. Should support asynchronous result updating,
                so it is easy to switch the predictor between sequential (CPU-based)
                and non-sequential (GPU-based) predictor methods.
        '''
        if soft_filtering:
            retain_scores = True
            raise NotImplementedError('Soft filtering not implemented yet.')
        self.startend_buffer = startend_buffer
        self.max_startend_buffer = max_startend_buffer
        self.processing_time_ratio = processing_time_ratio
        self.predictor = predictor
        self.local_search_width = local_search_width
        self.local_search_step = local_search_step
        self.retain_scores = retain_scores
        self.soft_filtering = soft_filtering
        self.max_im_var = max_im_var
        self.text_std_mult = text_std_mult
        self.scene_diff_mult = scene_diff_mult
        self.text_filt = text_filt
        self.face_filt = face_filt
        self.mixing_samples = mixing_samples
        self.n_thumbs = n_thumbs
        self.search_algo = search_algo(local_search_width)
        self.cur_frame = None


    def choose_thumbnails(self, video, n=1, video_name=''):
        self.gist.reset()
        thumbs = self.choose_thumbnails_impl(video, n, video_name)
        return thumbs

    def _conduct_local_search(self, score, results):
        '''
        Given the frames that are already the best, determine
        whether it makes sense to proceed with local search. 
        '''
        # open question: how do we choose how to combine
        # blur, text and face data?
        # 
        # further, in the case of multiple faces, what is
        # an appropriate action to take?

    def _compute_colorsim_mtx(self, images):
        '''
        Computes a color similarities for all
        pairwise combinations of images.
        '''
        colorObjs = [ColorNameCache(img) for img in images]
        for i, j in permutations(range(len(images))):
            

    def _mix(self, video, num_frames):
        '''
        'mix' takes a number of equispaced samples from
        the video. 
        '''
        samples = np.linspace(0, num_frames, 
                              self.mixing_samples+2).astype(int)
        # we need to be able to compute the SAD, so we need to
        # also insert local search steps
        for frameno in samples:
            framenos = [frameno, frameno + self.local_search_step]
            imgs = self.get_seq_frames(video, framenos)
            SAD = self.compute_SAD(imgs)
            self._SAD_stat.push(SAD)
            pix_val = np.max(np.var(np.var(imgs[0],0),0))
            self._tot_pixel_val[0] += pix_val
            frame_score = self.predictor.predict(imgs[0])
            self._tot_score_val[0] += frame_score

        self._tot_pixel_val[1] += len(samples)
        self._tot_score_val[1] += len(samples) 

    def choose_thumbnails_impl(self, video, n=1, video_name=''):
        # instantiate the statistics objects required
        # for computing the running stats.
        self._pixel_stat = None # max channelwise pixel variance
        self._colorname_stat = None # mean colorname distance
        self._score_stat = None # mean score
        # the _tot_x_val objects are of the form
        # (stat_sum, tot_measurements), and are used
        # to compute the means.
        self._tot_pixel_val = [0, 0]
        self._tot_colorname_val = [0, 0]
        self._tot_score_val = [0, 0]
        self._SAD_stat = Statistics()
        self.search_algo.start()
        
        self.results = []
        # maintain results as:
        # (score, rtuple, frameno, colorHist)
        #
        # where rtuple is the value to be returned.
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        video_time = float(num_frames) / fps

    def compute_SAD(self, imgs):
        '''
        Computes the sum of absolute difference between
        a list of frames. 
        '''
        SAD_vals = []
        prev_img = imgs[0]
        for next_img in imgs[1:]:
            sad = np.sum(cv2.absdiff(prev_img, next_img))
            SAD_vals.append(sad)
            prev_img = next_img
        return SAD_vals

    def compute_blur(self, img):
        '''
        Computes the average blurriness of an image by
        computing the variance of the laplacian.
        '''
        if type(img) == list:
            return [self.compute_blur(x) for x in img]
        image = self._center_crop(img, 0.5)
        return cv2.Laplacian(image, cv2.CV_32F).var()

    def _center_crop(self, img, crop_frac):
        '''
        Takes the center <crop_frac> of an image
        and returns it.
        '''
        if len(img.shape) == 3:
            x, y, w = img.shape
        else:
            x, y = img.shape
        xlim = int(x * (1. - crop_frac)/2)
        ylim = int(y * (1. - crop_frac)/2)
        if len(img.shape) == 3:
            return img[xlim:-xlim, ylim:-ylim, :]
        else:
            return img[xlim:-xlim, ylim:-ylim]

    def _get_frame(self, video, f):
        more_data, self.cur_frame = pycvutils.seek_video(
                                    video, f, 
                                    cur_frame=self.cur_frame)
        if not more_data:
            if self.cur_frame is None:
                raise model.errors.VideoReadError(
                    "Could not read the video")
        more_data, frame = video.read() 
        return frame

    def get_seq_frames(self, video, framenos):
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
            frame = self._get_frame(video, frameno)
            frames.append(frame)
        return frames

    def get_region_frames(self, video, start, num=1,
                          step=0):
        '''
        Obtains a region from the video.
        '''
        frame_idxs = [start]
        for i in range(num-1):
            frame_idxs.append(frame_idxs[-1]+step)
        frames = get_seq_frames(video, framenos)
        return frames
