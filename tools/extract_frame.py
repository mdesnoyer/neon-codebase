#!/usr/bin/env python
'''A script that extracts frame N from a video.

Copyright: 2015 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cProfile as profile
import cv2
import logging
import matplotlib.pyplot as plt
import numpy as np
import time
from utils import pycvutils
from optparse import OptionParser

_log = logging.getLogger(__name__)

def run_one_video(video_file, n, output_file):
    _log.info('Opening %s' % video_file)
    video = cv2.VideoCapture(video_file)

    _log.info('Video is %fs long' % (
        video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT) /
        video.get(cv2.cv.CV_CAP_PROP_FPS)))

    cur_frame = 0
    while cur_frame < n:
        frame = video.grab()
        cur_frame += 1

    status, frame = video.retrieve()

    if output_file is not None:
        cv2.imwrite(output_file, frame)

    # Plot the frame
    cv2.imshow('', frame)
    cv2.waitKey(60000)

def main(options):
    run_one_video(options.video, options.n, options.output)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('--video', default=None,
                      help='The video to process')
    parser.add_option('-n', default=65, type='int',
                      help='Frame number to extract')
    parser.add_option('--output', '-o', default=None,
                      help='Filename to output to')
    
    options, args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    main(options)
