import os.path
import sys
import numpy as np
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0,__base_path__)

import model
import heapq 
import cv2

class UniformSampling:
    def __init__(self,rate,model_name):
        self._rate = rate
        ## TOOD: construct this outside of the main loop so we 
        ## don't need to make a new one each time.
        self._mod = model.load_model(model_name)
    #####################

    def __call__(self,frame,frameNo):
        result = None
        if (frameNo % self._rate) == 0:
                curFrame = frame
		valence = self._mod.score(curFrame) 
		result = (frameNo,valence[0])
        # end if (frame.frameno % self._rate) == 0:
        return result
     ####################
    def process(self,video):
        results = []
        frameNo = 0
        ### using grab and retrieve should speed up processing.
	while video.grab():
            if (frameNo % self._rate) == 0:
                moreData,frame = video.retrieve()
                if not moreData:
                    print 'error retrieving data'
                    exit()
                ### end if
                valence = self._mod.score(frame)
                results.append((frameNo,valence[0]))
            ### end if
            frameNo += 1
        ### end while 
        return results
    #####################
### end class ####

class AdaptiveSampling:
    def __init__(self,rate1,rate2,threshold,model_name):
        self._rate1 = rate1
        self._rate2 = rate2
        self._rate = self._rate1
	self._mod = model.load_model(model_name)
	self._fast_mode = 0 
        self._threshold = threshold
	self._countdown = 5
    ### end __init__

    def __call__(self,frame,frameNo):
        result = None
        if (frameNo % self._rate) == 0:
            curFrame = frame
            valence = self._mod.score(curFrame)
            result = (frameNo,valence[0])
            if valence[0] > self._threshold:
		self._fast_mode = self._countdown 
		self._rate = self._rate2
            ### end if
            if self._fast_mode == 0:
                self._rate = self._rate1
            ### end if
            self._fast_mode -= 1
        ### end if
	return result
    ### end __call__
    
    def process(self,video):
        results = []
        frameNo = 0
        while video.grab():
            if (frameNo % self._rate) == 0:
                moreData,frame = video.retrieve() 
                if not moreData: 
                    print 'error retrieving video frame'
                    exit()
                ### end if not moreData
                valence = self._mod.score(frame)
                results.append((frameNo,valence[0]))
                # Do state machine house keeping.
                if valence[0] > self._threshold:
	            self._fast_mode = self._countdown 
	            self._rate = self._rate2
                ### end if
                if self._fast_mode == 0:
                    self._rate = self._rate1
                ### end if
                self._fast_mode -= 1
            ### end if (frameNo % self._rate)
            frameNo += 1
        ### end while
        return results
    ### end process
### end class

class EveryFrameSampling:
    def __init__(self,model_name):
        self._mod = model.load_model(model_name) 
    ##########

    def __call__(self,frame,frameNo):
        curFrame = frame#.ndarray()[:,:,::-1]
        valence = self._mod.score(curFrame) 
        return (frameNo,valence[0]) 
    ##########
    def process(self,video):
        results = []
        frameNo = 0
        moreData,frame = video.read()
        while moreData:
           valence = self._mod.score(frame)
           results.append((frameNo,valence[0]))
           frameNo += 1
           moreData,frame = video.read() 
        ### end while moreData
        return results
    ### end process
### end class ####


class BisectionSampling:
    def __init__(self,model_name):
        self._brackets = []#Queue.PriorityQueue()
        self._scores = {} # maps frame number to valence score.
        self._executing = True
        self._frames_processed = 0
        self._mod = model.load_model(model_name) 
    ##########
    ### end __init__
    
    def process(self,video):
        '''
        Process takes an OpenCV2 video object and seeks through different
        brackets.  Searches the bracket with the highest score first.
        '''
        self._executing = True
        self._frames_processed = 0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        self._threshold = int(num_frames*0.1) # only want to process ten percent of video frames.

        b0 = (0,num_frames-1)
        score0 = self.score_bracket(video,b0)
        heapq.heappush(self._brackets,(score0,b0))
        while self.still_executing():
            # take the top bracket
            b = heapq.heappop(self._brackets)
            # split it in half,
            b1 = (b[1][0],int(-0.5+(b[1][0]+b[1][1])/2))
            b2 = (int(0.5+(b[1][0]+b[1][1])/2),b[1][1])
            # rescore each half, put them back in.
            s1 = self.score_bracket(video,b1)
            s2 = self.score_bracket(video,b2)

            heapq.heappush(self._brackets,(s1,b1))
            heapq.heappush(self._brackets,(s2,b2))
        ### end while self._executing
        results = [(x,self._scores[x]) for x in self._scores.keys()]
        return results
    ### end process

    def still_executing(self):
        #return len(self._scores.keys()) < self._threshold
        return len(self._brackets) < self._threshold
    ### end still_executing
    
    def score_bracket(self,video,bracket):
        def score_frame(f):
            if not f in self._scores.keys():
                self._frames_processed += 1
                frame = self.get_frame(video,f)
                if frame is not None:
                    s = self._mod.score(frame,f,video)
                    #print f,s[0],s[1]
                    self._scores[f] = s[0]
                else:
                    self._scores[f] = float('-inf')
                    #print 'Error: frame %d was null' % f
            ### end if
            return self._scores[f]
        ### end score_frame 

        score = [score_frame(b) for b in bracket] 
        return -sum(score)
    ### end def score_bracket

    def get_frame(self,video,f):
        last_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
        video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES,f)
        moreData,frame = video.read() 
        #if not moreData:
            #print 'Error reading frame %d of %d' % (f,video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
            #self._executing = False
        ### end if not moreData
        return frame
    ### end get_frame

### end class BisectionSampling   

class BisectionFadeSampling:
    def __init__(self,model_name):
        self._brackets = []#Queue.PriorityQueue()
        self._scores = {} # maps frame number to valence score.
        self._pixel_variance = {}
        self._pixel_variance_rate = {}
        self._executing = True
        self._frames_processed = 0
        self._mod = model.load_model(model_name) 
    ##########
    ### end __init__
    
    def process(self,video):
        '''
        Process takes an OpenCV2 video object and seeks through different
        brackets.  Searches the bracket with the highest score first.
        '''
        self._executing = True
        self._frames_processed = 0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        self._threshold = int(num_frames*0.1) # only want to process ten percent of video frames.

        b0 = (0,num_frames-2)
        score0 = self.score_bracket(video,b0)
        heapq.heappush(self._brackets,(score0,b0))
        while self.still_executing():
            # take the top bracket
            b = heapq.heappop(self._brackets)
            # split it in half,
            b1 = (b[1][0],int(-0.5+(b[1][0]+b[1][1])/2))
            b2 = (int(0.5+(b[1][0]+b[1][1])/2),b[1][1])
            # rescore each half, put them back in.
            s1 = self.score_bracket(video,b1)
            s2 = self.score_bracket(video,b2)

            heapq.heappush(self._brackets,(s1,b1))  # here is where we want f(s1,v1) to prioritise.
            heapq.heappush(self._brackets,(s2,b2))
        ### end while self._executing
        results = [(x,self._scores[x],self._pixel_variance_rate[x]) for x in self._scores.keys()]
        return results
    ### end process

    def still_executing(self):
        #return len(self._scores.keys()) < self._threshold
        return len(self._brackets) < self._threshold
    ### end still_executing
    
    def score_bracket(self,video,bracket):
        def fade_score_frame(f):
            video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES,f-1)
            for idx in [f-1,f,f+1]:
                moreData,frame = video.read() # read automatically advances the video.
                if not moreData:
                    print 'error reading frame ', idx 
                    self._pixel_variance[idx] = 0
                    self._scores[f] = float('-inf')
                ### end if
                if idx not in self._pixel_variance.keys():
                    self._pixel_variance[idx] = np.std(np.sum(frame,axis=2))
                if idx == f and f not in self._scores.keys():
                    s = self._mod.score(frame,f,video)
                    self._scores[f] = s[0]
                ### end if idx not in self._pixel_variance.keys()
            ### end for idx in [f-1,f]
            d_var1 = self._pixel_variance[f]-self._pixel_variance[f-1]
            d_var2 = self._pixel_variance[f+1]-self._pixel_variance[f]
            self._pixel_variance_rate[f] = d_var2-d_var1 # Large negative values inidicate badness.
            return self._scores[f]
        ### end fade_score_frame
        score = [fade_score_frame(b) for b in bracket] 
        return -sum(score)
    ### end def score_bracket

    def get_frame(self,video,f):
        last_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
        video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES,f)
        moreData,frame = video.read() 
        #if not moreData:
            #print 'Error reading frame %d of %d' % (f,video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
            #self._executing = False
        ### end if not moreData
        return frame
    ### end get_frame

### end class BisectionFadeSampling   


