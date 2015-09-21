import heapq
import logging
import threading
from bisect import bisect_left as bidx
from bisect import insort_left as bput

import numpy as np

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
                    )

'''
=======================================================================
                            UTILITY FUNCTIONS
=======================================================================
'''
def seek_video(video, frame_no, cur_frame=None):
    '''Seeks an OpenCV video to a given frame number.

    After calling this function, the next read() will give you that frame.

    This is necessary because the normal way of seeking in OpenCV
    (setting the CV_CAP_PROP_POS_FRAMES doesn't always work. It might
    only go to the previous keyframe, or it might not be possible to
    get the current frame number).

    Inputs:
    video - An opencv VideoCapture object
    frame_no - The frame number to seek to
    do_log - True if logging should happen on errors
    cur_frame - If you know the frame number that the video should be at, 
                put it here. It helps to identify error cases.

    Outputs:
    Returns (sucess, cur_frame)
    '''

    grab_sucess = True
    if (cur_frame is not None and cur_frame > 0 and 
        video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES) == 0):
        while grab_sucess and cur_frame < frame_no:
            grab_sucess = video.grab()
            cur_frame += 1

    else:
        if (cur_frame is None or not (
                (frame_no - cur_frame) < 4 and (frame_no - cur_frame) >= 0) ):
            # Seeking to a place in the video that's a ways away, so JUMP
            video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES, frame_no)
            
        cur_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
        while grab_sucess and cur_frame < frame_no:
            grab_sucess = video.grab()
            cur_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
            if cur_frame == 0:
                return False, None

    return grab_sucess, cur_frame

'''
=======================================================================
                            REAL STUFF
=======================================================================
'''
class GPUVideoSearch(object):
    '''
    Abstract GPU Video Searcher, preliminary
    implementation: designed for testing 
    _gpuPredictor and MonteCarloMetropolisHastings.

    Note: in contrast to the orthodox video searchers,
    this simply gets a video file.
    '''
    def __init__(self, predictor, search_algo):
        '''
        predictor : object for performing the prediction
        search_algo : a class that returns the next frame
                      to search.
        '''
        self._predictor = predictor
        self._algo = search_algo
        self._chooser = None
        self._kill_switch = Threading.Event()
        self._complete = Threading.Event()
        self.result = []

    def choose_thumbnails(self, video_file, n=1):
        '''
        Selects thumbnails based on the asynchronous
        activity of the GPU. choose_thumbnails spawns
        a thread (_choose_thumbnails), which runs
        continuously in the background until it is 
        instructed to stop.
        '''
        logging.debug('Beginning to choose thumbnails...')
        self._complete.clear()
        self._kill_switch.clear() 
        self._chooser = threading.Thread(
            target=self._choose_thumbnails,
            args=(video_file, n))
        self._choose.start()
        return True

    def _choose_thumbnails(self, video_file, n):
        logging.debug('Starting')
        seek_loc = [None]
        results = []
        self._predictor.reset()
        # get the number of frames
        video = cv2.VideoCapture(video_file)
        nframes = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
        selector = self._algo(nframes)

        def get_frame(video, f):
            '''
            Obtains a specific frame from the video
            '''
            more_data, cur_frame = seek_vide(vide, f,
                                cur_frame=seek_loc[0])
            seek_loc[0] = cur_frame
            if not more_data:
                if cur_frame is None:
                    raise RuntimeError('Couldnt read video')
            more_data, frame = video.read()
            return frame

        '''
        COMMENCE SEARCHHHH
        '''
        while True:
            if self._kill_switch.is_set():
                break
            for i in self._predictor.get():
                # i is (id, score)
                # id will be: <VIDEO_FILE>_<FRAME>
                frame = int(i[0].split('_')[-1])
                score = i[1]
                logging.debug('Fetched %s'%i[0])
                selector((frame, score))
                if len(results) < n:
                    heapq.heappush(results,
                                   (score, frame))
                else:
                    heapq.heappushpop(results,
                                      (score, frame))
            for i in range(10):
                f = selector()
                jid = 'MOVIE_' + str(f)
                img = get_frame(video, f)
                logging.debug('Request %s'%(jid))
                self._predictor(img, jid)



    def stop(self):
        '''
        Stops the chooser
        '''
        logging.debug('Stopping')
        self._kill_switch.set()
        if self._chooser != None:
            self._chooser.join()

    def kill(self):
        '''
        Stops the chooser and everything else.
        '''
        logging.debug('Killing everything')
        self._kill_switch.set()
        self._predictor.stop()
        if self._chooser != None:
            self._chooser.join()

'''
MonteCarloMetropolisHastings is a searching method
where frames are sampled according to the probability,
given their neighbors, that they have a high score. 
Assuming that the scores of sequential frames are not
completely independent, then this will eventually (and
efficiently) converge to the correct distribution of 
scores over frames.
'''

class MonteCarloMetropolisHastings(object):
    '''
    A generic searching algorithm that
    samples from the distribution of the
    scores in accordance with the algorithm's
    belief in the viability of that region.
    '''
    def __init__(self, elements):
        '''
        elements : the maximum number of elements
                   over which we will search.
        '''
        self.N = elements
        self.samples = []
        self.results = dict()
        self.max_score = 0.
        self.n_samples = 0
        self.tot_score = 0.
        self.mean = 0.
        self.rejected = set()

    def __call__(self, result=None):
        if result:
            self._update(result)
        else:
            return self._get()

    def _update(self, update):
        '''
        Updates the algorithm's current knoweldge
        state. 

        'update' is a list of tuples (x, y) where
        x - integer - the location of the sample
        y - float - the score of the sample
        '''
        if update[1] = None:
            # the image was rejected
            self.rejected.add(update[0])
        else:
            bput(self.samples, update[0])
            self.results[update[0]] = update[1]
            self.max_score = max(self.max_score, 
                                 update[1])
            self.tot_score += update[1]
        self.n_samples += 1
        # a rejected image causes the mean score
        # to be reduced -- this is sensible since
        # the more rejections we get the less likely
        # we should be to search unexplored regions.
        self.mean = self.tot_score / self.n_samples

    def _find_n_neighbors(self, target, N):
        '''
        Given a sorted list, returns the 
        N next smallest and the N next
        largest. Uses a bisection search.

        Returns a tuple of lists:
        ((smallest), (largest))
        '''
        v = bidx(self.samples, target)
        # Make sure to check for all those stupid
        # edge conditions
        si = max(0, v - N)
        ei = min(len(slist), v + N)
        nsvs = slist[si:v]
        if not nsvs:
            nsvs = 0
        nlvs = slist[v:ei]
        if not nlvs:
            nlvs = self.N
        return (nsvs, nlvs)

    def _bounds(self, target):
        '''
        Simpler version of find_n_neighbors,
        which only returns the left and right
        neighbors for now. 
        '''
        v = bidx(self.samples, target)
        if not v:
            # there are no lower samples
            xL = 0
            yL = self.mean
        else:
            xL = self.samples[v-1]
            yL = self.results[xL]
        if v == self.n_samples:
            # there are no higher samples
            xH = self.N
            yH = self.mean
        else:
            xH = self.samples[v]
            yH = self.results[xH]
        return [(xL, yL), (xH, yH)]

    def _accept_sample(self, sample):
        '''
        Returns true or false if the sample
        is to be accepted.
        '''
        if not self.n_samples:
            return True
        if sample in self.results:
            return False
        if sample in self.rejected:
            return False
        neighbs = self._bounds(sample)
        pred_score = self._predict_score(
                        neighbs, sample)
        criterion = pred_score / self.max_score
        return np.random.rand() < criterion

    def _get(self):
        '''
        Returns a sample
        '''
        while self.n_samples < self.N:
            sample = np.random.choice(self.N)
            if self._accept_sample(sample):
                return sample

    def _predict_score(self, neighbs, sample):
        '''
        Predicts the score of a sample given
        its neighbors. Currently only supports
        nearest neighbor on both sides.
        '''
        [x1, y1], [x2, y2] = neighbs
        x3 = sample
        m = float(y2 - y1) / float(x2 - x1)
        return m * (x3 - x1) + y1
