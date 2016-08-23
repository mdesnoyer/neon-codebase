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

define('workers', default=4, help='Number of worker threads')

_log = logging.getLogger(__name__)



class ClipFinder(object):
    def __init__(self, predictor, scene_detector, action_estimator,
                 valence_weight=1.0, action_weight=0.25,
                 processing_time_ratio=0.7, startend_clip=0.1,
                 cross_scene_boundary=True,
                 min_scene_piece=15):
        # A model.predictor.Predictor object
        self.predictor = predictor
        # A scenedetect.SceneDetector object
        self.scene_detector = scene_detector
        # A model.features.FeatureGenerator that estimates action 
        self.action_estimator = action_estimator
        self.weight_dict = {'valence': valence_weight,
                            'action' : action_weight}
        self.processing_time_ratio = processing_time_ratio
        self.startend_clip = startend_clip
        self.cross_scene_boundary = cross_scene_boundary
        self.min_scene_piece = min_scene_piece

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
            [model.features.SceneCutGenerator(self.scene_detector),
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
            None if min_len is None else int(min_len*fps))

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
        

    def _build_clips(self, score_obj, n_clips, max_len, min_len):
        '''From a list of scenes and scores for scenes, put together clips.

        scene_list - List of scene starts (except the last one,
                     which is the last frame)
        score_obj - RegionScore object
        n_clips - Number of clips to find
        max_len - Maximum length of a clip in frames
        min_len - Maximum length of a clip in frames

        Returns:
        List of VideoClip objects sorted by score descending
        '''
        # Sort the possible scenes by score
        scenes_left = sorted(score_obj.get_results(), key=lambda x:x[4])
        rv = []
        while scenes_left and len(rv) < n_clips:
            scene_n, start, end, nframes, score, prop_dict = scenes_left.pop()

            if max_len is not None and nframes > max_len:
                # Grab part of the scene starting in the middle.
                # TODO(mdesnoyer) Search for the right action in the
                # scene using motion analysis.
                rv.append(model.VideoClip(
                    int((start + end - max_len) / 2),
                    int((start + end + max_len) / 2),
                    score))
            elif min_len is not None and nframes < min_len:
                # The scene is too small, so we need to grab into the
                # next scene
                if not self.cross_scene_boundary:
                    # Not allowed so we throw out this scene
                    continue

                # Stich together scenes until we have enough to reach min_len
                cur_scene_end = end
                cur_scene_idx = None
                while (cur_scene_end - start) < min_len:
                    next_scene = [x for x in enumerate(scenes_left) if
                                  x[1][1] == cur_scene_end]
                    if len(next_scene) == 0:
                        # We're at the end and cannot use this scene
                        continue
                    cur_scene_idx, cur_scene = next_scene[0]

                    if (max_len is not None and 
                        (cur_scene[2] - start) > max_len):
                        # Take a chunk of this scene
                        cur_scene_end = start + max_len
                    else:
                        # Take the whole scene
                        cur_scene_end = cur_scene[2]

                    # Remove the scene from consideration
                    del scenes_left[cur_scene_idx]
                rv.append(model.VideoClip(start, cur_scene_end, score))
            else:
                # Take the whole scene
                rv.append(model.VideoClip(start, end, score))
                
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
        self._weights = None
        self._regions = regions
        self._filters = []
        if weights:
            self.set_weights(weights)
        if regions:
            self.set_regions(regions)
        if filters:
            self.set_filters(filters)
        # self._props is a dict of dicts as:
        # self._props[property][frame] = raw_score
        self._props = defaultdict(lambda: dict())
        self.n_scored = Counter()
        self._is_finalized = False
        self._rprops = dict()
        self._mt_lock = Lock()

    def set_weights(self, weights):
        try:
            self.property_names = weights.keys()
        except AttributeError, e:
            raise AttributeError('weights must be a dict mapping property to importance')
        self._weights = weights

    def set_regions(self, regions):
        if regions[0] == 0:
            self._regions = regions[1:]
        else:
            self._regions = regions[:]

    def set_filters(self, filters):
        self.filters = filters

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
        for cfilter in self._filters:
            if not cfilter(prop_dict):
                return False
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
            cur_prop_sum = 0
            cur_prop_count = 0
            for i in range(start, end):
                if i in self._rprops[cur_prop]:
                    cur_prop_sum += self._rprops[cur_prop][i]
                    cur_prop_count += 1
            cur_prop_dict[cur_prop] = (float(cur_prop_sum) / max(1, cur_prop_count), 
                                       float(cur_prop_count) / (end - start))
        return start, end, cur_prop_dict
