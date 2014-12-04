#!/usr/bin/env python
'''A script that shows the top N thumbnails.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import cProfile as profile
import cv2
import logging
import matplotlib.pyplot as plt
import model
import numpy as np
import time
from utils import pycvutils
from optparse import OptionParser

_log = logging.getLogger(__name__)

def run_one_video(mod, video_file, n, output_file, batch):
    _log.info('Opening %s' % video_file)
    video = cv2.VideoCapture(video_file)

    _log.info('Video is %fs long' % (
        video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT) /
        video.get(cv2.cv.CV_CAP_PROP_FPS)))
    
    startTime = time.time()
    thumbs, endTime = mod.choose_thumbnails(video, n=n,
                                            start_time=5.0,
                                            end_buffer_time=5.0,
                                            processing_time_ratio=1.2)
    _log.info('Processing time: %fs' % (time.time() - startTime))

    # Plot the examples
    plt.figure(figsize=(16, 4), dpi=80)
    curThumb = 0
    for image, score, frame_no, timecode, attribute in thumbs:
        # Output the image
        if output_file is not None:
            cv2.imwrite(output_file % curThumb, image)


        frame = plt.subplot(1, len(thumbs), curThumb+1)
        frame.axes.get_xaxis().set_ticks([])
        frame.axes.get_yaxis().set_visible(False)
        plt.imshow(image[:,:,::-1])
        plt.xlabel('s: %3.2f' % score)
        curThumb += 1

    if not batch:
        plt.show()

def main(options):     
    _log.info('Loading model')
    mod = model.load_model(options.model)

    if options.video is not None:
        run_one_video(mod, options.video, options.n, options.output,
                      options.batch)
    elif options.video_list is not None:
        for line in open(options.video_list):
            run_one_video(mod, line.strip(), options.n, None,
                          options.batch)

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--model', default=None,
                      help='File containing the model')
    parser.add_option('--video', default=None,
                      help='The video to process')
    parser.add_option('--video_list', default=None,
                      help='The list of videos to process')
    parser.add_option('-n', default=5, type='int',
                      help='Number of thumbnails to extract')
    parser.add_option('--output', '-o', default=None,
                      help='String template to output the thumbnails to. Eg. thumb_%i.jpg')
    parser.add_option('--batch', default=False, action='store_true',
                      help='If true, does not show images')
    
    options, args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    main(options)
