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
import tempfile
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
from shutil import copyfile

def main():

    mov = cv2.VideoCapture(options.input)
    clip = model.VideoClip(0, 200, 55)

    ffmpeg_params = ['-vf', 'scale=320:240']

    with tempfile.NamedTemporaryFile(suffix='.mp4') as target:
         with imageio.get_writer(target.name, 'FFMPEG', fps=29.97, ffmpeg_params=ffmpeg_params) as writer:

            _log.info('Output clip with score %f to %s' %
                      (clip.score, target.name))

            for frame in pycvutils.iterate_video(mov, clip.start, clip.end):
                writer.append_data(frame[:,:,::-1])
         copyfile(target.name, options.output)

if __name__ == '__main__':
    utils.neon.InitNeon()
    logging.getLogger('boto').propagate = False
    main()
