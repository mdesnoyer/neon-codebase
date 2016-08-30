#!/usr/bin/env python

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import collections
import concurrent.futures
import cPickle as pickle
import cv2
import logging
import itertools
import model.predictor
import numpy as np
import pandas
import threading
import utils.autoscale
from utils import pycvutils
import utils.neon
from utils.options import options, define

define('input', default=None, help='Input video')
define('output', default=None, help='Output pickle file')
define('step', default=15, help='Sample every N frames')
define('threads', default=10, help='Max number of threads')

_log = logging.getLogger(__name__)

class VideoStepper(collections.Iterator):
    def __init__(self, video_filename, step_size):
        self._mov = cv2.VideoCapture(video_filename)
        self._step = step_size
        self._lock = threading.RLock()
        self._cur_frame = 0

    def __iter__(self):
        return self

    def next(self):
        with self._lock:
            seek_success, self._cur_frame = pycvutils.seek_video(
                self._mov,
                self._cur_frame,
                cur_frame=self._cur_frame)
            if not seek_success:
                raise StopIteration

            read_success, image = self._mov.read()
            if not read_success:
                raise StopIteration
            rv = (image, self._cur_frame)
            
            self._cur_frame += self._step
            
            return rv
            

def main():
    conn = utils.autoscale.MultipleAutoScaleGroups(['AquilaOnDemandTest'])
    predictor = model.predictor.DeepnetPredictor(aquila_connection=conn)
    pool = concurrent.futures.ThreadPoolExecutor(options.threads)

    _log.info('Opening %s' % options.input)
    mov = VideoStepper(options.input, options.step)

    try:
        predictor.connect()
        _log.info('Connected')

        feature_list = list(pool.map(
            lambda x: (int(x[1]), predictor.predict(x[0])[1]), 
            mov))

    finally:
        predictor.shutdown()

    outputfn = options.output
    if not outputfn:
        splits = options.input.split('.')
        splits[-1] = 'pkl'
        outputfn = '.'.join(splits)
    _log.info('Done scoring images, outputting to %s' % outputfn)

    df = pandas.DataFrame(dict(feature_list))
    df.to_pickle(outputfn)

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
