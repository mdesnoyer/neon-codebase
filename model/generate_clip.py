#!/usr/bin/env python
'''A script that generates clips from a video.

Copyright: 2016 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import imageio
import logging
import matplotlib.pyplot as plt
import model
import model.predictor
import numpy as np
import time
import utils.autoscale
from utils.options import options, define
from utils import pycvutils
import utils.neon

define('input', default=None, help='Input video file')
define('output', default=None, help='Output video file')
define('model', default=None, help='File that contains the model to use')
define('custom_predictor', default=None, 
       help='Name of the custom predictor to use')
define('aq_groups', default='AquilaOnDemandTest,AquilaTestSpot',
       help=('Comma separated list of autoscaling groups to talk to for '
             'aquilla'))
define('len', default=None, type=float, help='Desired clip length in seconds')
define('n', default=1, help='Number of clips to extract')

_log = logging.getLogger(__name__)

def main():
    conn = utils.autoscale.MultipleAutoScaleGroups(
        options.aq_groups.split(','))
    predictor = model.predictor.DeepnetPredictor(aquila_connection=conn)
    
    _log.info('Searching for clips in %s' % options.input)
    mov = cv2.VideoCapture(options.input)
    
    _log.info('Opening model %s' % options.model)
    mod = model.generate_model(options.model, predictor)
    if options.custom_predictor is not None:
        mod.clip_finder.custom_predictor = model.load_custom_predictor(
            options.custom_predictor)
    mod.clip_finder.scene_detector.threshold = 30.0
    mod.clip_finder.scene_detector.min_scene_len = 30
    mod.clip_finder.weight_dict['custom'] = 1.0
    mod.clip_finder.weight_dict['valence'] = 1.0
    

    clips = []
    try:
        predictor.connect()

        clips = mod.find_clips(mov, options.n, max_len=options.len,
                               min_len=options.len)

    finally:
        predictor.shutdown()

    clip_i = 0
    for clip in clips:
        out_splits = options.output.rpartition('.')
        out_fn = '%s_%i.%s' % (out_splits[0], clip_i, out_splits[2])
        _log.info('Output clip %i with score %f to %s' %
                  (clip_i, clip.score, out_fn))
        clip_i += 1

        writer = imageio.get_writer(out_fn, 'FFMPEG', fps=30.0)
        try:
            for frame in pycvutils.iterate_video(mov, clip.start, clip.end):
                writer.append_data(frame[:,:,::-1])
        finally:
            writer.close()
    

if __name__ == '__main__':
    utils.neon.InitNeon()
    logging.getLogger('boto').propagate = False
    main()
