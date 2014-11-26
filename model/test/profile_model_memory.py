#!/usr/bin/env python
'''A scrip that profiles the model processing a video.

The output file can be examined using the pstats module.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
if __name__ == '__main__':
    import sys
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if sys.path[0] <> base_path:
        sys.path.insert(0,base_path)

import cv2
import gc
import logging
import model
import numpy as np
import objgraph
from optparse import OptionParser

_log = logging.getLogger(__name__)

def choose_thumbnails(video):
    video = cv2.VideoCapture(options.video)
    mod.choose_thumbnails(video, n=5)

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--model', default=None,
                      help='File containing the model')
    parser.add_option('--video', default=None,
                      help='The video to process')
    
    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    _log.info('Loading model')
    mod = model.load_model(options.model)

    gc.collect()
    objgraph.show_growth()
    for i in range(10):
        choose_thumbnails(options.video)
    gc.collect()
    objgraph.show_growth()

    _log.info('Done')
