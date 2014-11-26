#!/usr/bin/env python
'''Script that lets me experiment with processing on every frame.

Copyright: 2014 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0,__base_path__)

import cv2
import ffvideo
import matplotlib.pyplot as plt
import numpy as np
from optparse import OptionParser
import time
import model.filters

class BlurMeasure:
    def __init__(self):
        self.overlay_frame_count = 30
        self.frame_buffer = []
        self.frame_sum = 0
        self.frame_sum_sq = 0

    def score(self, image):
        mask = self.get_overlay_mask(image)

    def get_overlay_mask(self, image):
        grey_im = np.array(cv2.cvtColor(image,cv2.COLOR_BGR2GRAY),
                           dtype=np.float)
        
        if len(self.frame_buffer) >= self.overlay_frame_count:
            self.frame_sum = self.frame_sum - self.frame_buffer[0]
            self.frame_sum_sq -= np.square(self.frame_buffer[0])
            self.frame_buffer = self.frame_buffer[1:]

        self.frame_buffer.append(grey_im)
        self.frame_sum += grey_im
        self.frame_sum_sq += np.square(grey_im)

        e_x = self.frame_sum / len(self.frame_buffer)
        e_x2 = self.frame_sum_sq / len(self.frame_buffer)
        stddev = e_x2 - np.square(e_x)
        return stddev / np.max(stddev) * 255.0

def main(options):
    video = ffvideo.VideoStream(options.video, frame_size=(None, 256))

    print 'Video name: %s' % options.video
    print 'Video length is %fs' % video.duration
    print "Frame size: %dx%d" % (video.frame_width, video.frame_height)
    print 'Codec: %s' % video.codec_name 

    outputVideo = None

    filt = model.filters.BlurryFilter()
    blur = BlurMeasure()

    startTime = time.time()
    laplace_scores = []
    diff_scores = []
    lastFrame = None
    for frame in video:
    #for sec in np.arange(0.5, video.duration-0.5, 0.15):
    #    frame = video.get_frame_at_sec(sec)
        curFrame = frame.ndarray()[:,:,::-1]

        grey_im = np.array(cv2.cvtColor(curFrame, cv2.COLOR_BGR2GRAY),
                           dtype=np.float)
        laplace_im = np.abs(cv2.Laplacian(grey_im, cv2.CV_64F))

        if outputVideo is None:
            outputVideo = cv2.VideoWriter(
                'overlay.avi',
                cv2.cv.CV_FOURCC('d','i','v','x'),
                30.0,
                (curFrame.shape[1], curFrame.shape[0]))
        grey_im = np.array(blur.get_overlay_mask(curFrame),
                                   dtype=np.uint8)
        outputVideo.write(np.repeat(np.atleast_3d(np.array(laplace_im, np.uint8)), 3, axis=2))
        
        if lastFrame is not None:
            laplace_scores.append(np.sum(laplace_im)/
                                  (laplace_im.shape[0]*laplace_im.shape[1]))
            diff = np.abs(curFrame - lastFrame)
            diff_scores.append(np.sum(diff))
        lastFrame = curFrame

    print 'Processing time is: %fs' % (time.time() - startTime)

    timestamps = np.arange(len(laplace_scores)) / 30.0
    laplace_scores = np.array(laplace_scores)

    plt.figure(1)
    plt.plot(timestamps, laplace_scores)

    plt.figure(2)
    plt.plot(timestamps, diff_scores)

    plt.figure(3)
    plt.plot(timestamps[1:], np.abs(laplace_scores[1:]-laplace_scores[:-1]))

    plt.show()

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--video', '-i', default=None,
                      help='Video to process')
    
    options, args = parser.parse_args()
    main(options)
