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
import numpy as np
import math

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

def main(name):
    video,num_frames,framerate= load_video(name)

    moreData = False
    pos = num_frames
    while not moreData:
        pos = pos - 1
        video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES,pos)
        moreData,frame = video.read()
    ### end while
    sys.stdout.write(str(num_frames)+','+str(pos)+','+str(framerate)+','+str(math.fabs(num_frames-pos))+'\n')

if __name__ == '__main__':

    for name in sys.stdin.readlines():
        main(os.path.abspath(os.path.expanduser(name.strip())))
