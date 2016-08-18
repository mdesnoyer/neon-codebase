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

from collections import defaultdict as ddict
from collections import Counter
import cv2
import logging
import numpy as npfrom Queue import Queue
import scenedetect
from scipy import stats
from threading import Thread
from threading import Lock
from threading import Event
from utils.options import options, define

define('workers', default=4, help='Number of worker threads')

_log = logging.getLogger(__name__)

class ClipFinder(object):
    def __init__(self, predictor, scene_detector, action_calculator,
                 valence_weight=1.0, action_weight=0.25,
                 processing_time_ratio=0.7, startend_clip=0.1,
                 cross_scene_boundary=True,
                 min_scene_piece=15):
        self.predictor = predictor
        self.scene_detector = scene_detector
        self.action_calculator = action_calculator
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

    def find_clips(self, mov, n=1, max_len=None, min_len=None):
        pass

    def _build_clips(self, scene_list, score_obj, n_clips, max_len, min_len)
        # TODO(have options about how to fit/select the clip from the list of scenes. Do we go in the middle of a scene? Only grab scenes about the correct length? etc.
