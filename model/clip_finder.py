'''A tool to identify the best clips in a video

Copyright: 2016 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
        Nick Dufour (dufour@neon-lab.com)
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from collections import defaultdict
from collections import Counter
import cv2
import logging
import numpy as np
import matplotlib.pyplot as plt
import model.features
import random
from scipy import stats
import Queue
from threading import Thread
from threading import Lock
from threading import Event
import time
from utils.options import options, define
from utils import pycvutils

define('workers', default=2, help='Number of worker threads')

_log = logging.getLogger(__name__)


class ClipFinder(object):
    def __init__(self, predictor, scene_detector, action_estimator,
                 valence_weight=1.0, action_weight=0.25, custom_weight=0.5,
                 processing_time_ratio=0.7, startend_clip=0.1,
                 cross_scene_boundary=True,
                 min_scene_piece=15):
        # A model.predictor.Predictor object
        self.predictor = predictor
        # A scenedetect.SceneDetector object
        self.scene_detector = scene_detector
        self.scene_cut_generator = model.features.SceneCutGenerator(
            self.scene_detector)
        # A model.features.FeatureGenerator that estimates action 
        self.action_estimator = action_estimator
        self.weight_dict = {'valence': valence_weight,
                            'action' : action_weight,
                            'custom' : custom_weight}
        self.processing_time_ratio = processing_time_ratio
        self.startend_clip = startend_clip
        self.cross_scene_boundary = cross_scene_boundary
        self.min_scene_piece = min_scene_piece
        self.custom_predictor = None

    def update_processing_strategy(self, processing_strategy):
        '''
        Changes the state of the video client based on the processing
        strategy. See the ProcessingStrategy object in cmsdb/neondata.py
        '''
        self.startend_clip = processing_strategy.startend_clip
        self.processing_time_ratio = \
          processing_strategy.clip_processing_time_ratio
        self.cross_scene_boundary = \
          processing_strategy.clip_cross_scene_boundary
        self.min_scene_piece = processing_strategy.min_scene_piece
        self.scene_detector.threshold = processing_strategy.scene_threshold
        if processing_strategy.custom_predictor:
            self.custom_predictor = model.load_custom_predictor(
                processing_strategy.custom_predictor)
        else:
            self.custom_predictor = None
        self.weight_dict['custom'] = \
          processing_strategy.custom_predictor_weight

    def reset(self):
        self.scene_cut_generator.reset()
        self.action_estimator.reset()

    def find_clips(self, mov, n=1, max_len=None, min_len=None):
        '''Finds a set of clips in the movie.

        Inputs:
        mov - OpenCV VideoCapture object
        max_len - Maximum length of a clip in seconds
        min_len - Minimum length of a clip in seconds

        Returns:
        List of model.VideoClip objects sorted by score descending
        '''
        num_frames = int(mov.get(cv2.CAP_PROP_FRAME_COUNT))
        end_scene = min(int(num_frames * (1-self.startend_clip)),
                        num_frames-1)
        fps = mov.get(cv2.CAP_PROP_FPS) or 30.0
        duration = num_frames / fps
        analysis_budget = duration * self.processing_time_ratio

        start_time = time.time()

        # First, do a pass through the video to find scenes and detect action
        mov_feature_generator = model.features.MovieMultipleFeatureGenerator(
            [self.scene_cut_generator,
             self.action_estimator],
             max_height=360,
             frame_step=2,
             startend_buffer = self.startend_clip
             )
        mov_features = mov_feature_generator.generate(mov)

        # Get the list of scenes
        scene_list = [k for k,v in mov_features[
            model.features.SceneCutGenerator].iteritems() if v]
        scene_list.append(end_scene)
        _log.debug('Found %i scenes: %s' % (len(scene_list), scene_list))

        # Initialize the object that keeps track of feature values
        score_obj = RegionScore(weights=self.weight_dict,
                                regions=scene_list)
        for frameno, val in mov_features[self.action_estimator.__class__].iteritems():
            score_obj.update('action', frameno, val)

        time_spent = (time.time() - start_time)
        _log.info('Used %3.2fs of %3.2fs (%.1f%%) finding scenes' %
                  (time_spent, analysis_budget,
                   time_spent/analysis_budget*100.0))
        analysis_budget -= time_spent
        self._score_scenes(mov, scene_list, score_obj, analysis_budget)

        clips = self._build_clips(
            score_obj,
            n, 
            None if max_len is None else int(max_len*fps),
            None if min_len is None else int(min_len*fps),
            num_frames)

        return clips

    def _score_scenes(self, mov, scene_list, score_obj, analysis_budget):
        '''Get all the scenes scored.

        Results stored in score_obj
        '''
        frame_q = Queue.Queue(maxsize=100)
        halter = Event()

        threads = [
            Thread(target=self._frame_extractor,
                   args=(mov, scene_list, frame_q, halter))]
        for i in range(options.workers):
            threads.append(Thread(target=self._worker,
                                  args=(score_obj, frame_q, halter)))

        for t in threads:
            t.daemon = True
            t.start()

        # Run the frame scoring until we are out of time
        if not halter.wait(analysis_budget):
            _log.info('Out of time sampling frames')
            halter.set()

        for t in threads:
            t.join()

        _log.info('Finished scoring scenes')
        

    def _build_clips(self, score_obj, n_clips, max_len, min_len, vid_len):
        '''From a list of scenes and scores for scenes, put together clips.

        scene_list - List of scene starts (except the last one,
                     which is the last frame)
        score_obj - RegionScore object
        n_clips - Number of clips to find
        max_len - Maximum length of a clip in frames
        min_len - Maximum length of a clip in frames
        vid_len - Length of the video in frames

        Returns:
        List of VideoClip objects sorted by score descending
        '''
        if min_len is None and max_len is None:
            # Just grab the whole scenes in priority order
            scenes = sorted(score_obj.get_results(), key=lambda x:x[4],
                            reverse=True)
            return [model.VideoClip(x[1], x[2], x[4]) 
                    for x in scenes[0:n_clips]]
        elif min_len is None:
            min_len = max_len
        elif max_len is None:
            max_len = min_len

        # Map of scene # -> (start, end, nframes, score, prop_dict
        scenes = {x[0] : x[1:] for x in score_obj.get_results()}

        # An array the length of the video where each value is the
        # scene number for that frame
        scene_arr = np.zeros(vid_len, np.int16)

        # An array the length of the video where each value is the
        # score of that scene for that frame.
        scores = np.zeros(vid_len)

        for scene_num, val in scenes.iteritems():
            start, end, nframes, score, prop_dict = val
            scene_arr[start:end] = scene_num
            scores[start:end] = score

        # Now convolve the scores with indices for the min and max
        # video length to get an average score for a clip starting at
        # each location.
        clip_score_small = np.convolve(scores, np.ones(min_len)/min_len,
                                       'valid')
        clip_score_big = np.convolve(scores, np.ones(max_len)/max_len,
                                     'valid')

        # Estimate the action through the video
        # TODO(mdesnoyer): Do this using something other than the scene
        # detector because that's just HSV diffs.
        action_samples = [(k+int(vid_len*self.startend_clip),
                           v['delta_hsv_avg']) for k, v in 
                          self.scene_cut_generator.frame_metrics.iteritems()]
        action = np.zeros(vid_len)
        last_frame, last_val = 0, 0
        for frameno, val in sorted(action_samples, key=lambda x:x[0]):
            action[last_frame:frameno] = last_val
            last_frame, last_val = frameno, val
            

        rv = []
        while np.max(clip_score_small) > 0 and len(rv) < n_clips:
            # Pick the best clip left
            best_small = np.nanmax(clip_score_small)
            best_big = np.nanmax(clip_score_big)
            if best_small > best_big:
                target_len = min_len
                clip_start = np.argmax(clip_score_small)
            else:
                target_len = max_len
                clip_start = np.argmax(clip_score_big)

            # Now, grab the scene where this clip starts
            scene_num = scene_arr[clip_start]
            start, end, scene_len, score, prop_dict = scenes[scene_num]
            if scene_len > target_len:
                # TODO(mdesnoyer) Use a better motion analysis from my thesis
                _log.debug('Scene is bigger than clip size, so grabbing '
                           'looking for the best action in %i frames' % 
                           scene_len)
                #rv.append(model.VideoClip(
                #    int((start + end - target_len) / 2),
                #    int((start + end + target_len) / 2),
                #    score))

                # Look for a pattern in motion that follows a plot arc
                # (building up to a crescendo and then dropping off at
                # the end. We do this by convolving the action
                # measurements with a vector of the length of the
                # desired clip that peaks 80% of the way through. The
                # vector needs to be flipped to do the convolution
                # properly.
                denoumement_len = int(0.2*target_len)
                plot_arc = np.concatenate(
                    (np.linspace(0.8, 1.0, denoumement_len),
                    np.linspace(1.0, 0.2, target_len-denoumement_len)))
                plot_fit = np.convolve(action[start:end], plot_arc, 'valid')
                clip_start = np.argmax(plot_fit)
                rv.append(model.VideoClip(
                    start + clip_start,
                    start + clip_start + target_len,
                    score))
                
                # Remove the scene from future consideration
                clip_score_small[max(0,(start-target_len)):end] = 0
                clip_score_big[max(0,(start-target_len)):end] = 0
            else:
                if not self.cross_scene_boundary:
                    # This scene is too small, so remove it
                    clip_score_small[start:end] = 0
                    clip_score_big[start:end] = 0
                    continue
                # The scene is smaller than the requested size, so
                # just keep going
                rv.append(model.VideoClip(
                    clip_start,
                    clip_start + target_len,
                    np.mean(scores[clip_start:(clip_start+target_len)])))
                
                # The scene is too small, so we need to grab multiple
                # scenes and zero them out
                scene_list = set(scene_arr[clip_start:(clip_start+target_len)])
                _log.debug('Scene is smaller than clip size, so grabbing '
                           'the %i scenes' % len(scene_list))
                for scene_i in scene_list:
                    cur_scene = scenes[scene_i]
                    clip_score_small[cur_scene[0]:cur_scene[1]] = 0
                    clip_score_big[cur_scene[0]:cur_scene[1]] = 0

                # We don't want any clips that bleed into this scene
                clip_score_small[max(0,(clip_start-target_len)):clip_start] = 0
                clip_score_big[max(0, (clip_start-target_len)):clip_start] = 0
                continue
        return sorted(rv, key=lambda x: x.score, reverse=True)

    def _frame_yielder(self, scene_list):
        '''Yields frame numbers to sample. 

        For efficiency, tries to sample in passes of the video.
        '''
        # Build up a list of scenes. Each scene is a randomized
        # list of frame numbers to pull from
        scenes2frames = [range(x,y) for x, y in zip(scene_list[:-1],
                                                    scene_list[1:])]
        for sc in scenes2frames:
            random.shuffle(sc)

        # Now that we have the list of frames in scenes to choose
        # from, select them in passes by building a qeue
        queue = []
        while len(scenes2frames) > 0 or len(queue) > 0:
            if len(queue) == 0:
                _log.debug('Adding new frames to yielding queue')
                # Fill up the queue
                for i in range(100):
                    if len(scenes2frames) == 0:
                        break
                    scene_idx = random.randrange(len(scenes2frames))
                    scene = scenes2frames[scene_idx]
                    queue.append(scene.pop())
                    if len(scene) == 0:
                        scenes2frames.pop(scene_idx)
                queue.sort(reverse=True)
            else:
                yield queue.pop()
        

    def _frame_extractor(self, video, scene_list, queue, halt_event):
        """ populates a queue with frames. Each element of the queue is a
        tuple of the form (frameno, image)"""
        cur_frame = None
        first_move = True
        enqueue_count = 0
        for frameno in self._frame_yielder(scene_list):
            # Get the frame
            seek_success, cur_frame = pycvutils.seek_video(
                video, frameno, do_log=first_move, cur_frame=cur_frame)
            if not seek_success:
                if cur_frame is None:
                    raise model.errors.VideoReadError("Error reading video")
                break
            first_move = False

            read_success, image = video.read()
            if not read_success:
                break

            # Add the frame to the queue
            while True:
                if halt_event.is_set():
                    _log.debug('Halt is set. Extractor terminating')
                    return
                try:
                    queue.put((frameno, image), True, 2.0)
                    enqueue_count += 1
                    if enqueue_count % 100 == 0:
                        _log.debug('Enqueued %i frames' % enqueue_count)
                    break
                except Queue.Full:
                    pass
        _log.debug('Out of frames after %i. Halting' % enqueue_count)
        halt_event.set()

    def _worker(self, score_obj, queue, halt_event):
        while not halt_event.is_set():
            try:
                while True:
                    if halt_event.is_set():
                        _log.debug('Halting. Worker terminating')
                        return
                    try:
                        frameno, image = queue.get(True, 1.0)
                        break
                    except Queue.Empty:
                        pass

                score, features, version = self.predictor.predict(image)
                score_obj.update('valence', frameno, score)
                if self.custom_predictor is not None and features is not None:
                    score_obj.update('custom', frameno,
                                     self.custom_predictor.predict(features))

                if score_obj.n_scored['valence'] % 100 == 0:
                    _log.debug('%i images scored so far' %
                              score_obj.n_scored['valence'])
            except model.errors.PredictionError as e:
                _log.error('Working error predicting. Shutting down: %s'
                           % e)
                halt_event.set()


class RegionScore(object):
    """
    Defines a class that performs scoring of arbitrary regions
    on a set of arbitrary parameters. This is intended to be
    used with video, so regions are referred to as 'scenes'
    which are quantized into 'frames'
    """
    def __init__(self, weights=None, regions=None, filters=None):
        """
        weights: a dict, mapping property_name -> importance
                 where importance is a float value in the
                 domain [0, 1]
        regions: a list of region boundaries as integers. The 
                 first value is either zero of the start of the
                 second region.
        filters: a list of functions that accepts a property 
                 dictionary and returns True or False

        Both of these may be set after the fact.
        """
        self.set_weights(weights)
        self.set_regions(regions)
        self.set_filters(filters)
        # self._props is a dict of dicts as:
        # self._props[property][frame] = raw_score
        self._props = defaultdict(lambda: dict())
        self.n_scored = Counter()
        self._is_finalized = False
        self._rprops = dict()
        self._mt_lock = Lock()

    def set_weights(self, weights):
        if weights:
            try:
                self.property_names = weights.keys()
            except AttributeError, e:
                raise AttributeError('weights must be a dict mapping property '
                                     'to importance')
            self._weights = weights
        else:
            self._weights = None

    def set_regions(self, regions):
        if regions:
            if regions[0] == 0:
                self._regions = regions[1:]
            else:
                self._regions = regions[:]
        else:
            self._regions = None

    def set_filters(self, filters):
        self.filters = filters or []
            

    def update(self, property_name, frame, raw_score):
        """ updates the scoring objects knowledge, can be multithreaded """
        with self._mt_lock:
            self._props[property_name][frame] = raw_score
            self._is_finalized = False
            self.n_scored[property_name] += 1

    def _finalize(self):
        """
        computes the ranking ... 
        should it be the mean of ranks or the rank of a mean?

        the mean of ranks is probably more sensitive.
        """
        if self._is_finalized:
            return
        for cur_prop in self.property_names:
            if not len(self._props[cur_prop]):
                continue
            keys, values = zip(*self._props[cur_prop].iteritems())
            ranks = stats.rankdata(values)
            self._rprops[cur_prop] = {k:(v/len(ranks)) for k, v in zip(keys, ranks)}
        self.is_finalized = True

    def get_results(self):
        """ 
        returns a list of regions and
        includes, in order,
        [region_num, start, end, n frames, combined_score, mean_rank_dict]

        where mean_rank_dict is a dictionary from properties to a
        tuple: (mean_rank, frac_frames_scored)
        """
        self._finalize()
        results = []
        for region_n in range(len(self._regions)):
            start, end, prop_dict = self._get_region_score(region_n)
            comb_score = self._combine(prop_dict)
            nframes = end - start
            results.append([region_n, start, end, nframes, comb_score, prop_dict])
        return results

    def _combine(self, prop_dict):
        """
        accepts a property dictionary of the form returned by 
        _get_region_score and combines them according to 
        self._weights
        """
        cscore = 0
        wsum = 0
        if not self._check_filters_passed(prop_dict):
            return 0.
        for k, w in self._weights.iteritems():
            mean_rank, nsamp = prop_dict.get(k, (0., 0.))
            if not nsamp:
                continue
            cscore += mean_rank * w
            wsum += w
        if wsum == 0:
            return 0
        return cscore / wsum

    def _check_filters_passed(self, prop_dict):
        try:
            for cfilter in self._filters:
                if not cfilter(prop_dict):
                    return False
        except AttributeError:
            pass
        return True

    def _get_region_score(self, region_n):
        """
        returns the start and end of a region as well as a 
        dictionary for region_n that maps property
        names to a tuple (mean_rank, n_frames)
        """
        if region_n == 0:
            start = 0
        else:
            start = self._regions[region_n - 1]
        end = self._regions[region_n]
        cur_prop_dict = dict()
        for cur_prop in self.property_names:
            if not len(self._props[cur_prop]):
                continue
            cur_prop_sum = 0
            cur_prop_count = 0
            for i in range(start, end):
                if i in self._rprops[cur_prop]:
                    cur_prop_sum += self._rprops[cur_prop][i]
                    cur_prop_count += 1
            cur_prop_dict[cur_prop] = (float(cur_prop_sum) / max(1, cur_prop_count), 
                                       float(cur_prop_count) / (end - start))
        return start, end, cur_prop_dict
