# @author: P. Michael Furlong
# @date: 11.04.2014
#
# The objective of this test is to determine if seeking on a video frame produces different frames or not.
# The test objective is to seek to position i, then seek to position i+1, and determine the L2 norm of the video frames.
# if the video frame is NoneType then.....put -inf value?

import os.path
import sys
import cv2
import ffvideo
import numpy as np
import random



def main(video_name):
	video,num_frames,fps = load_video(video_name)
	frames_to_process = random.sample(range(num_frames),5)
	frames_to_process.append(num_frames-2)
	scores = [diff(video,f,f+1) for f in frames_to_process]	
	sys.stdout.write(str(scores).strip('[]')+'\n')	
### end main

def diff(video,f1,f2):
	frame1 = seek(video,f1)
	frame2 = seek(video,f2)
	if not(frame1 == None or frame2 == None):
		df = frame1-frame2
		return np.sum(df*df)	
	return float('-inf')

### end diff	
	
def seek(video,f):
        video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES,f)
        moreData,frame = video.read() 
	return frame
### end seek

def load_video(video_filename):
	#use_ffmpeg = False
	video_file = None
	#if use_ffmpeg:
	#    video_file = ffvideo.VideoStream(name, frame_size=(None, 256))
	#else:
	video_file = cv2.VideoCapture(video_filename)
	moreData,frame = video_file.read()
	assert(moreData)
	
	if not video_file.isOpened():
		print 'Error opening: %s' % video_filename
		exit(1)
	### end if not moreData
	
	fps = video_file.get(cv2.cv.CV_CAP_PROP_FPS)
	num_frames = int(video_file.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
	return video_file,num_frames,fps
### end load_video

	


if __name__=='__main__':
	random.seed(1238471)
        for name in sys.stdin.readlines():
		main(os.path.abspath(os.path.expanduser(name.strip())))
