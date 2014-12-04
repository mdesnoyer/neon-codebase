#!/usr/bin/env python
'''A scrip that profiles the model processing a video.

The output file can be examined using the pstats module.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import os.path
if __name__ == '__main__':
    import sys
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if sys.path[0] <> base_path:
        sys.path.insert(0,base_path)

import cProfile as profile
import ffvideo
import logging
import model
import numpy as np
from optparse import OptionParser

_log = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--model', default=None,
                      help='File containing the model')
    parser.add_option('--video', default=None,
                      help='The video to process')
    parser.add_option('--output', '-o', default='model.prof',
                      help='Output file for profiling information')
    
    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    _log.info('Loading model')
    mod = model.load_model(options.model)

    _log.info('Opening %s' % options.video)
    video = ffvideo.VideoStream(options.video)

    _log.info('Video is %fs long' % video.duration)
    profile.run("mod.choose_thumbnails(video, n=5)", options.output)

    _log.info('Saved profile to %s' % options.output)
    
