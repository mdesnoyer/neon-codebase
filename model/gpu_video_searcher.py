import heapq
import logging
import sys
import threading
from Queue import Queue
from bisect import bisect_left as bidx
from bisect import insort_left as bput
import traceback

import cv2
import numpy as np

'''
=======================================================================
                            LOGGING
=======================================================================
'''

class _AnsiColorizer(object):
    """
    A colorizer is an object that loosely wraps around a stream, allowing
    callers to write text to the stream in a particular color.

    Colorizer classes must implement C{supported()} and C{write(text, color)}.
    """
    _colors = dict(black=30, red=31, green=32, yellow=33,
                   blue=34, magenta=35, cyan=36, white=37)

    def __init__(self, stream):
        self.stream = stream

    @classmethod
    def supported(cls, stream=sys.stdout):
        """
        A class method that returns True if the current platform supports
        coloring terminal output using this method. Returns False otherwise.
        """
        if not stream.isatty():
            return False  # auto color only on TTYs
        try:
            import curses
        except ImportError:
            return False
        else:
            try:
                try:
                    return curses.tigetnum("colors") > 2
                except curses.error:
                    curses.setupterm()
                    return curses.tigetnum("colors") > 2
            except:
                raise
                # guess false in case of error
                return False

    def write(self, text, color):
        """
        Write the given text to the stream in the given color.

        @param text: Text to be written to the stream.

        @param color: A string label for a color. e.g. 'red', 'white'.
        """
        color = self._colors[color]
        self.stream.write('\x1b[%s;1m%s\x1b[0m' % (color, text))


class ColorHandler(logging.StreamHandler):
    def __init__(self, stream=sys.stderr):
        super(ColorHandler, self).__init__(_AnsiColorizer(stream))

    def emit(self, record):
        msg_colors = {
            logging.DEBUG: "green",
            logging.INFO: "blue",
            logging.WARNING: "yellow",
            logging.ERROR: "red"
        }
        # import ipdb
        # ipdb.set_trace()
        color = msg_colors.get(record.levelno, "blue")
        msg = self.format(record)
        self.stream.write(msg + "\n", color)

#logging.getLogger().addHandler(ColorHandler())

ch = ColorHandler()
ch.setFormatter(logging.Formatter('[%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s'))
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(ch)
# logging.info('test info')
# logging.debug('test debug')
# logging.warning('test warning')
# logging.error('test error')
# logging.basicConfig(level=logging.INFO,
#                     format='[%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
#                     handlers=[ColorHandler()])

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
    Begins a search of a video, using an asynchronous searching
    technique that termed (somewhat erroneously) Monte Carlo
    Metropolis Hastings.

    While search is asynchronous (scoring requests are
    submitted to a job manager) as is results being obtaiend (it is
    run as a separate thread), a sequence of locks ensures that
    results are dispatched and obtained appropriately and not
    clobbered by subsequent searches.

    The sequence works like this:
    - The instance of this class running in the main thread
    is instructed to fetch the results. The function sets
    the "_result_request" event, which indicates that someone
    wants to see what the results are so far. It then waits
    on _finalized.
    - The separate thread that handles video searches sees the
    result request, and stops submitting new requests.
    - The predictor is requested to yield all available results
    that are still in the queue.
    - Results in hand, the video search thread indicates that the
    video is done, and sets _finalized, and obtains the next video
    for analysis. It then waits on _results_received
    - The requester sees _finalized is set, and fetches the results,
    and sets the _results_received event indicating that the results
    are safely obtained.
    - The video searcher sees that _results_received has been set,
    and continues with the next analysis.

    NOTE: If the video search completes before the results are
    requested, the video searcher itself sets _result_request to true.
    This means, essentially, that results have to be fetched before
    a new video search can begin.
    '''
    def __init__(self, predictor, search_algo):
        '''
        predictor : object for performing the prediction
        search_algo : a class that returns the next frame
                      to search.
        '''
        self._predictor = predictor
        self._algo = search_algo
        self._chooser_thread = None
        self._kill_switch = threading.Event()
        self._complete = threading.Event()
        self._video_queue = Queue()
        self._finalized = threading.Event()
        self._result_obtained = threading.Event()
        self._result_request = threading.Event()
        self._start_thread()
        self._cur_video_id = None
        self._cur_selector = None
        logging.debug('_chooser_thread status is: ' 
                      + str(self._chooser_thread.is_alive()))
        self.result = []

    def _start_thread(self):
        '''
        Starts the thread.
        '''
        logging.debug('Starting searcher thread')
        #self._result_obtained.set()
        self._chooser_thread = threading.Thread(
            target=self._chooser,
            name='thumbnail selector')
        self._chooser_thread.start()

    def _chooser(self):
        '''
        Runs _choose_thumbnails until a new
        video enters the queue.
        '''
        while True:
            logging.debug('Waiting on item from _video_queue.')
            item = self._video_queue.get()
            if item == None:
                logging.debug('Chooser is shutting down!')
                return
            if self._kill_switch.is_set():
                logging.debug('Server is shutting down!')
                return
            video_file, n = item
            self._choose_thumbnails(video_file, n)
            # # perhaps the below isn't necessary -- it only needs to
            # # wait for the result to be OBTAINED.
            # logging.debug('Waiting on result request')
            # self._result_request.wait()
            # logging.debug('Result request obtained!')
            logging.debug('Waiting until last result is obtained')
            self._result_obtained.wait()
            logging.debug('Last result is obtained, resetting flag '\
                'and starting search.')
            self._result_obtained.clear()

    def _choose_thumbnails(self, video_file, n):
        logging.debug('Starting')
        seek_loc = [None]
        results = []
        # get the number of frames
        video = cv2.VideoCapture(video_file)
        nframes = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        self._cur_video_id = id(video)
        logging.info('Instantiating algo with %i frames'%(nframes))
        self._cur_selector = self._algo(nframes)

        def get_frame(video, f):
            '''
            Obtains a specific frame from the video
            '''
            more_data, cur_frame = seek_video(video, f,
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
                logging.debug('Received termination request!')
                logging.info('Searched %i frames'%len(self._cur_selector.results.keys()))
                self.results = results
                break
            if self._result_request.is_set():
                logging.debug('Received result request, halting search.')
                logging.info('Searched %i frames'%len(self._cur_selector.results.keys()))
                self.results = results
                self._finalized.set()
                self._result_request.clear()
                break
            # obtain a frame request from the selector
            nframe = self._cur_selector.get()
            if nframe == None:
                # then you've completed searching the video
                logging.debug('Video search is complete!')
                logging.debug('Waiting for result request')
                self._result_request.wait()
                logging.debug('Result request recieved after video is complete...finally')
            else:
                # obtain that frame
                bgr_img = get_frame(video, nframe)
                if nframe == 54:
                    cv2.imwrite('/home/nick/Desktop/gpu_vide_searcher.jpg', bgr_img)
                # submit that frame request to the job manager
                logging.info('Requested score for frame %s'%(nframe))
                self._predictor.predict(bgr_img, self._cur_video_id, nframe)
                # obtain the extant results from the predictor
            logging.debug('Updating results')
            self._update_results(results, n)
            logging.info('Results is now %i elements'%(len(results)))

    def _update_results(self, results, n, fetchallrem=False):
        '''
        Fetches results from the predictor.
        '''
        for result in self._predictor.results(fetchallrem):
            cvid, frame, score = result
            if not cvid == self._cur_video_id:
                logging.warning('Returned VID is invalid')
            logging.info('Score for frame %s obtained'%result[1])
            self._cur_selector.update((frame, score))
            if len(results) < n:
                heapq.heappush(results,
                               (score, frame))
            else:
                heapq.heappushpop(results,
                                  (score, frame))

    def get_result(self):
        logging.debug('Issueing request for results')
        self._result_request.set()
        logging.debug('Waiting for finalization')
        self._finalized.wait()
        logging.debug('Fetching results')
        cur_res = self.results
        logging.debug('Clearing finalization')
        self._finalized.clear()
        logging.debug('Notifying that results have been obtained')
        self._result_obtained.set()
        return cur_res

    def choose_thumbnails(self, video_file, n=1):
        '''
        Selects thumbnails based on the asynchronous
        activity of the GPU. choose_thumbnails spawns
        a thread (_choose_thumbnails), which runs
        continuously in the background until it is
        instructed to stop.
        '''
        logging.debug('Beginning to choose thumbnails...')
        self._video_queue.put((video_file, n))
        return True

    def stop(self):
        '''
        Stops the chooser
        '''
        logging.debug('Stopping')
        try:
            logging.debug('Inserting poison pill')
            self._video_queue.put(None)
        except:
            logging.warning('Poison pill insertion has failed')
            logging.warning('Failure could be due to irregular thread termination')
        logging.debug('Stopping predictor')
        self._predictor.stop()
        try:
            logging.debug('Setting kill switch')
            self._kill_switch.set()
        except:
            logging.warning('Kill Switch set has failed for unknown reasons.')
        if self._chooser_thread != None:
            logging.debug('Joining chooser')
            self._chooser_thread.join(5)
    
    def __del__(self):
        self.stop()

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

    def update(self, result):
        self._update(result)

    def get(self):
        return self._get()

    def _update(self, update):
        '''
        Updates the algorithm's current knoweldge
        state.

        'update' is a list of tuples (x, y) where
        x - integer - the location of the sample
        y - float - the score of the sample
        '''
        if update[1] == None:
            # the image was rejected
            self.rejected.add(update[0])
        else:
            self.results[update[0]] = update[1]
            self.max_score = max(self.max_score,
                                 update[1])
            self.tot_score += update[1]
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
            if not self.results.has_key(xL):
                yL = self.mean
            else:
                yL = self.results[xL]
        if v == self.n_samples:
            # there are no higher samples
            xH = self.N
            yH = self.mean
        else:
            xH = self.samples[v]
            if not self.results.has_key(xH):
                yH = self.mean
            else:
                yH = self.results[xH]
        return [(xL, yL), (xH, yH)]

    def _accept_sample(self, sample):
        '''
        Returns true or false if the sample
        is to be accepted.
        '''
        if sample in self.results:
            return False
        if sample in self.rejected:
            return False
        v = bidx(self.samples, sample)
        if v < len(self.samples):
            if self.samples[v] == sample:
                # do not resample!
                return False
        if not self.mean:
            logging.debug('Mean is undefined! Accepting sample')
            return True
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
                # increment n_samples to indicate that another
                # sample has been 'taken'
                bput(self.samples, sample)
                self.pending.add(sample)
                self.n_samples += 1
                logging.info('Selected frame %i'%(sample))
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
