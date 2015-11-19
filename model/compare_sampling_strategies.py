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

from utils import pycvutils
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

def main(name,model):
    video,num_frames,framerate= load_video(name) 

    duration = int(framerate*num_frames)

    #filt = model.filters.BlurryFilter()
    #blur = BlurMeasure()

    startTime = time.time()
    laplace_scores = []
    diff_scores = []
    lastFrame = None

    dt = 1.0 # sample every second
    #strategy = EveryFrameSampling(model) 
    #strategy = UniformSampling(int(framerate*dt),model) 
    #strategy = AdaptiveSampling(4*int(framerate*dt),int(framerate*dt/4.),4.06,model) # 4.06 is approximately the mean plus 3 sigma
    strategy = BisectionSampling(model)
    #strategy = BisectionFadeSampling(model)
    valence_scores = []
    try:
        valence_scores = strategy.process(video)
        result_str = str(valence_scores).strip('[]').replace('(','').replace(')','')
        sys.stdout.write(str(num_frames)+','+result_str+'\n')
        #print num_frames,',',

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
    if options.model == None:
        print 'Need model'
        exit()
    if options.video != None:
        main(options.video,options.model)
    else:
        i = 0
        for name in sys.stdin.readlines():
	        main(os.path.abspath(os.path.expanduser(name.strip())),options.model)
