#!/usr/bin/env python
'''Script that lets me experiment with processing on every frame.

Copyright: 2014 Neon Labs
Author: P. Michael Furlong (furlong@gmail.com)
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
import random
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
    #use_ffmpeg = False
    video_file = None
    #if use_ffmpeg:
    #    video_file = ffvideo.VideoStream(name, frame_size=(None, 256))
    #else:
    video_file = cv2.VideoCapture(video_filename)

    if not video_file.isOpened():
        print 'Error opening: %s' % video_filename
        exit(1)
    ### end if not moreData

    fps = video_file.get(cv2.cv.CV_CAP_PROP_FPS)
    num_frames = int(video_file.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
    return video_file,num_frames,fps
### end load_video

def main(name,model_name):

    random.seed(10641283287)

    video,num_frames,framerate= load_video(name) 
    duration = int(framerate*num_frames)

    random_frames = random.sample(range(num_frames-4),min(num_frames-4,30))
    mod = model.load_model(model_name)

    selected_frames = [None,None,None]
    model_times = []
    std_times = []
    try:
        for f in random_frames:
           video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES,f)
           moreData,frame = video.read()

           model_start_time = time.clock()
           mod.score(frame)
           model_times.append(time.clock()-model_start_time)
     
           std_start_time = time.clock()
           selected_frames[0] = np.var(np.sum(frame,axis=2))
           moreData,frame = video.read()
           selected_frames[1] = np.var(np.sum(frame,axis=2))
           moreData,frame = video.read()
           selected_frames[2] = np.var(np.sum(frame,axis=2))
           dd_std_val = np.diff(selected_frames,n=2)
           std_times.append(time.clock() - std_start_time)
        ### end for in random_frames

        print np.min(model_times),np.max(model_times),np.mean(model_times),len(random_frames)
        print np.min(std_times),np.max(std_times),np.mean(std_times),len(random_frames)
        print np.mean(std_times)/np.mean(model_times)

    except IOError:
        print name 
    ### end try


if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--model', '-m', default=None,
                      help='The name of the model to use')
    parser.add_option('--video', '-i', default=None,
                      help='Video to process')
    
    options, args = parser.parse_args()
    if options.model is None:
        print 'Need model'
        exit()
    if options.video is not None:
        main(options.video,options.model)
    else:
        i = 0
        for name in sys.stdin.readlines():
	        main(os.path.abspath(os.path.expanduser(name.strip())),options.model)
