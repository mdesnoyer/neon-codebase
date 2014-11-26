#!/usr/bin/env python
'''Script that lets me experiment with processing on every frame.

Copyright: 2014 Neon Labs
Author: P. Michael Furlong (furlong@gmail.com)
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
from sampling_strategies import *


	
def load_video(video_filename):
    video_file = None
    video_file = cv2.VideoCapture(video_filename)

    if not video_file.isOpened():
        print 'Error opening: %s' % video_filename
        exit(1)
    ### end if not moreData

    fps = video_file.get(cv2.cv.CV_CAP_PROP_FPS)
    num_frames = int(video_file.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
    return video_file,num_frames,fps
### end load_video

def main(name):
    video,num_frames,framerate= load_video(name) 

    duration = int(framerate*num_frames)

    stdFilt = model.filters.DeltaStdDevFilter()
    crossFilt = model.filters.CrossFadeFilter(dframe=2)

    pixel_std = []

    cur_frame = 0
    while video.grab():
        moreData, frame = video.retrieve()
        #pixel_std.append(np.std(np.sum(frame,axis=2)))
        #stdFilt.accept(frame, cur_frame, video)
        #pixel_std.append(stdFilt.score())
        pixel_std.append(crossFilt.score(frame, cur_frame, video))
        cur_frame += 1
    ### end while 

    plt.plot(np.array(pixel_std))
    plt.show()

### end main

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--video', '-i', default=None,
                      help='Video to process')
    
    options, args = parser.parse_args()
    main(os.path.abspath(os.path.expanduser(options.video.strip())))
