'''
~ Simplified Version ~

This is the core of the model when using GPU Video Clients.
It is a python wrapper for the GPU implementation for our 
new model, which is based off Google's 'Inception' architecture.

NOTE:
All of this is contingent on using BGR images in the openCV style!
'''

import logging
import multiprocessing
import os
import threading
from glob import glob
from Queue import Queue
from time import time

import cv2
import numpy as np

# this will prevent caffe from printing thousands 
# of lines to stderr
if 'GLOG_minloglevel' not in os.environ:
    # Hide INFO and WARNING, show ERROR and FATAL
    os.environ['GLOG_minloglevel'] = '2'
    _unset_glog_level = True
else:
    _unset_glog_level = False
import caffe
if _unset_glog_level:
    del os.environ['GLOG_minloglevel']


logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s]'\
                    '[%(funcName)s] %(message)s',)
# temporarily, since we're *only* using debug
logging.basicConfig(level=logging.DEBUG,
                    format='[%(process)-10s][%(threadName)-10s][%(funcName)s]' \
                    ' %(message)s',)

caffe.set_mode_gpu()

class _GPUMgr(caffe.Net):
    '''
    Manages the GPU. This is the last layer of CPU-based
    processing that occurs before computation is handed
    off to the GPU

    model_file : the deploy prototext
    pretrained_file : the *.caffemodel file with 
                      pretrained weights.
    '''
    def __init__(self, model_file, pretrained_file):
        '''
        model_file : the caffe model to use
        pretrained_file : array of model weights
        '''
        caffe.Net.__init__(self, model_file, pretrained_file, caffe.TEST)
        in_ = self.inputs[0]
        self.image_dims = np.array(self.blobs[in_].data.shape[1:])
        logging.debug('Instantiated.')

    def __call__(self, data_array):
        '''
        Actually executes the prediction, provided with an 
        N x 3 x H x W array of N images that have already
        been preprocessed and resized.
        '''
        logging.debug('Running chunk of %i images on GPU' % (
            data_array.shape[0]))
        if type(data_array).__module__ != np.__name__:
            raise TypeError("data_array type is %s, must be %s" % (
                str(type(data_array)), str(np.__name__)))
        if data_array.dtype != np.dtype('float32'):
            raise ValueError("data_array must be float32")
        if np.any(data_array.shape[1:] != self.image_dims):
            raise ValueError(
                "data_array must have shape N x %i x %i x %i" % (
                self.image_dims[0], self.image_dims[1], self.image_dims[2]))
        out = self.forward_all(**{self.inputs[0]: data_array})
        predictions = np.exp(out[self.outputs[0]])
        return list(predictions[:,0])

class _Preprocess(object):
    ''' 
    preprocesses images so they are appropriate to be
    fed into the GPU, and reorients their dimensions
    as necessary. Note that images are either read in
    in the openCV fashion (B, G, R order) or are provided
    as such.
    '''
    def __init__(self, image_dims, image_mean=[104, 117, 123]):
        '''
        image_dims = W x H array / list / tuple
        image_mean = triple of channel mean values 
        '''
        self.image_dims = image_dims
        if image_mean == None:
            image_mean = [0, 0, 0] 
        self.image_mean = image_mean

    def _read(self, imgfn):
        '''
        If img is provided as a filename, this will
        read it in. 
        '''
        bgr_img = cv2.imread(imgfn)
        if bgr_img == None:
            raise ValueError("Could not find image %s" % (imgfn))
        return bgr_img

    def __call__(self, bgr_img):
        '''
        Actually performs the preprocessing
        '''
        logging.debug('Preprocessing image')
        if type(img) == str:
            bgr_img = self._read(bgr_img)
        if not type(bgr_img).__module__ == np.__name__:
            raise TypeError("Image must be a numpy array (or str filename)")
        if bgr_img.dtype != np.float32: 
            bgr_img = bgr_img.astype(np.float32)
        bgr_img = cv2.resize(img, (self.image_dims[0], self.image_dims[1]))
        if bgr_img.shape[2] == 1:
            # it's a black and white image, we have to colorize it
            bgr_img = cv2.cvtColor(bgr_img, cv2.COLOR_GRAY2BGR)
        elif img.shape[2] != 3:
            raise ValueError("Image has the incorrect number of channels")
        # subtract the channelwise image means
        bgr_img -= self.image_mean
        bgr_img = bgr_img.transpose(2, 0, 1)
        return bgr_img

class _Predictor(object):
    '''
    The Predictor class, which is handed off the video clients.
    You should never need to call this yourself, instead they are
    instantiated within JobManager.
    '''
    def __init__(self, putQ, getQ, cmax, prep,
                 clients, dead_clients, valid_videos,
                 client_has_died, terminate):
        '''
        putQ = the MP queue to put jobs into
        getQ = the MP queue to get jobs from
        max = the maximum number of jobs to allow
        cid = the ID of this client
        prep = the preprocessing function
        clients = the global list of valid clients
        dead_clients = the global list of dead clients
        valid_videos = a list of valid videos
        client_has_died = a condition that incidates that 
                          this client has died.
        terminate = an event that terminates all child
                    processes of the JobManager.
        '''
        self._putQ = putQ
        self._getQ = getQ
        self._prep = prep
        self.max = cmax
        self.cid = id(self)
        self._clients = clients
        self._dead_clients = dead_clients
        self._valid_videos = valid_videos
        self._client_has_died = client_has_died
        self._terminate = terminate
        self._cur_video = self.cid # initialize to the client ID
        self._results = []
        self._get_result_thread = None
        self._fully_init = False
        self._total_jobs = 0

    def _start_result(self):
        '''
        Locally initialize the results thread
        '''
        logging.debug('Starting the threads locally')
        self._submit_allow = threading.Semaphore(self.max)
        self._get_result_thread = threading.Thread(
                target=self._get_result,
                name='Result Fetch')
        self._get_result_thread.daemon = True
        self._get_result_thread.start()
        self._fully_init = True

    def predict(self, bgr_img, vid=None, jid=None):
        '''
        Asynchronously predicts scores. if vid, the video
        id, is None, then it uses the client id. If jid, 
        the job ID, is None, then it uses the total jobs
        submitted so far.

        Jobs in the putQ have the form:
        (ID, bgr_img)

        where ID is 
        (cid, vid, jid)
        '''
        if not self._fully_init:
            logging.debug('Locally starting threads')
            self._start_result()
        # acquire permission to submit job
        logging.debug('Awaiting permission to submit job')
        proceed = self._submit_allow.acquire(blocking=False)
        if not proceed:
            logging.debug('TOO MANY JOBS SUBMITTED. WAITING.')
            self._submit_allow.acquire()
        logging.debug('Permission acquired')
        if vid == None:
            logging.debug('video id is undefined, assigning it to %i' % (
                self._cur_video))
            vid = self._cur_video
        if jid == None:
            logging.debug('job id is currently undefined, assigning to %i' % (
                self._total_jobs))
            jid = self._total_jobs
        self._check_vid(vid)
        self._putQ.put(((self.cid, vid, jid), 
                              self._prep(bgr_img)))
        self._total_jobs += 1
        logging.debug('%ith job submitted' % (self._total_jobs))

    def _check_vid(self, vid):
        '''
        verifies that the video ID is the current one,
        if not, retires the old one and makes appropriate
        changes
        '''
        if self._cur_video != vid:
            logging.debug('New video seen: %i vs. %i' % (vid,
                self._cur_video))
            try:
                logging.debug('Attempting to remove from valid videos')
                self._valid_videos.remove(self.cur_video)
            except:
                logging.debug('Invalid video id, although this may not be a ' \
                    'problem')
            self._valid_videos.append(vid)
            self._cur_video = vid
            # remove pending results
            logging.debug('Purging %i results awaiting integration' % (
                len(self._results)))
            self._results = []

    def _get_result(self):
        '''
        A synchronously fetches results from the 
        get Queue, which is populated by a thread
        under the control of the video manager. 
        Because processing is done asynchronously,
        it's possible that it will return results
        that are no longer applicable, in which
        case they will be discarded.
        '''
        logging.debug('Results fetcher started')
        while True:
            item = self._getQ.get()
            if self._terminate.is_set():
                logging.debug('Termination order recieved!')
                return
            if item == None:
                # this video client is terminating
                logging.debug('This video client terminating')
                return
            else:
                (cid, vid, jid), score = item
                logging.debug('Job %i obtained, score: %.3f' % (
                    jid, score))
                if vid != self._cur_video:
                    logging.debug('Obtained result corresponds to finished ' \
                        'video')
                    continue
                self._results.append((vid, jid, score))
            self._submit_allow.release()


    def results(self):
        '''
        Returns results
        '''
        cur_res = self._results[:]
        self._results = [] 
        return self._results

    def stop(self):
        '''
        Locally terminate the results.
        '''
        logging.debug('Stopping...')
        logging.debug('Enqueueing null result')
        self._getQ.put(None)
        if not self._terminate.is_set():
            logging.debug('Removing self from active clients')
            self._clients.remove(self.cid)
            logging.debug('Adding self to dead clients list')
            self._dead_clients.append(self.cid)
            logging.debug('Notifying JobManager')
            with self._client_has_died:
                self._client_has_died.notify_all()
        else:
            logging.debug('JobManager is already dead.')
        logging.debug('Joining results thread')
        self._get_result_thread.join()

    def __del__(self):
        self.stop()


class JobManager(object):
    '''
    Simplified JobManager, which maintains several items:
    >> *Note: MMP = Manager Multiprocessing
        - MMP Input queue
        - Output Dict:
            - keys are _Predictor IDs
            - values are MMP Queues
        - MP List of valid video IDs
        - MP List of valid client IDs
        - MP List of dead client IDs

    Note that this JobManager is designed to run in the
    main process, and further manages all GPU clients 
    from within Main(). However, since it uses Managed
    lists, this requires another process to take over as
    the Manager server.

    FOR NOW, this will only work with 1 GPU. 
    '''
    def __init__(self, model_file, pretrained_file,
                 batchSize = 32, image_dims = [224, 224, 3],
                 image_mean = [104, 117, 123], N = None):
        logging.debug('Initializing')
        self._manager = multiprocessing.Manager()
        self._clients = self._manager.list()
        self._dead_clients = self._manager.list()
        self._valid_videos = self._manager.list()
        self._terminate = self._manager.Event()
        # _client_has_died wakes up the garbage collector thread
        self._client_has_died = self._manager.Condition()
        self._output_queues = dict()  # client ID --> output pipeline (queue)
        self._pretrained = pretrained_file
        self._model = model_file
        self._batchSize = batchSize
        self._image_dims = image_dims
        self._image_mean = image_mean
        self._gpu2mgr = Queue()
        if N == None:
            N = np.inf
        self._max = N
        self._client2mgr = multiprocessing.Queue()
        self._submit_thread = None
        self._garbage_collect_thread = None
        self._start_threads()
        logging.debug('Starting GPU manager')
        self._gpu_mgr = _GPUMgr(self._model, self._pretrained)
        self._prep = _Preprocess(self._image_dims, 
                                 self._image_mean)

    def _start_threads(self):
        '''
        Initializes the allocator, garbage collector, and
        input handling threads.
        '''
        self._submit_thread = threading.Thread(
            target=self._submit,
            name='Submission and Allocation')
        self._submit_thread.daemon = True
        self._garbage_collect_thread = threading.Thread(
            target=self._garbage_collect,
            name='Garbage Collector')
        self._garbage_collect_thread.daemon = True
        logging.debug('Starting garbage collector daemon')
        self._garbage_collect_thread.start()
        logging.debug('Starting submission / allocation daemon')
        self._submit_thread.start()

    def register_client(self, cmax=None, cid=None):
        '''
        Register and returns a new _Predictor object
        '''
        # instantiate a new queue
        if cmax == None:
            cmax = self._max
        mgr2client = self._manager.Queue()
        new_client = _Predictor(self._client2mgr, mgr2client,
            cmax, self._prep, self._clients,
            self._dead_clients, self._valid_videos,
            self._client_has_died, self._terminate)
        cid = id(new_client)
        logging.debug('Predictor %i instantiated' % (cid))
        self._output_queues[cid] = mgr2client 
        self._clients.append(cid)
        return new_client

    def _garbage_collect(self):
        '''
        Destroys the dead client's output queue.
        '''
        self._client_has_died.acquire()
        while True:
            self._client_has_died.wait()
            logging.debug('Notified of client death!')
            if self._terminate.is_set():
                logging.debug('Server is shutting down!')
                return # you've been ordered to die
            while self._dead_clients:
                dcq = self._dead_clients.pop()
                logging.debug('Deregistering client %i' % (dcq))
                dead_queue = self._output_queues.pop(dcq)
                dead_queue.join()

    def _submit(self):
        '''
        Submits data to the GPU, and allocates
        the results back to the appopriate clients.
        '''
        logging.debug('Instantiating contiguous GPU data array')
        gpuArray = np.ascontiguousarray(
            np.zeros((self._batchSize, 
                     self._image_dims[2],
                     self._image_dims[0],
                     self._image_dims[1])
            ).astype(np.float32))
        
        def _grab(is_first=False):
            if is_first:
                item = self._client2mgr.get()
                if self._terminate.is_set():
                    return
                return item
            try:
                item = self._client2mgr.get(timeout=1)
                return item
            except:
                pass

        def _grab_data():
            '''
            Gracefully grabs all available to data
            that are in need of analysis
            '''
            pending = [] 
            # grab the first job
            while True:
                logging.debug('Waiting on first job')
                item = _grab(True)
                if self._terminate.is_set():
                    logging.debug('Received termination order!')
                    return
                ID, bgr_img = item
                client, video, jid = ID
                if client not in self._clients:
                    logging.debug('Job request from dead/invalid client')
                    continue
                if video not in self._valid_videos:
                    logging.debug('Job request for invalid / completed video')
                    continue
                pending.append(ID)
                gpuArray[len(pending)-1,:,:,:] = bgr_img
                break
            # grab remaining data
            while True:
                if len(pending) >= self._batchSize:
                    logging.debug('Batch is ready')
                    return pending
                item = _grab()
                if self._terminate.is_set():
                    logging.debug('Received termination order!')
                    return
                if item == None:
                    logging.debug('Reached end of waiting jobs')
                    break
                if client not in self._clients:
                    logging.debug('Job request from dead/invalid client')
                    continue
                if video not in self._valid_videos:
                    logging.debug('Job request for invalid / completed video')
                    continue
                ID, bgr_img = item
                pending.append(ID)
                gpuArray[len(pending)-1,:,:,:] = bgr_img
            return pending

        while True:
            pending = _grab_data()
            if self._terminate.is_set():
                logging.debug('Received termination order!')
                return
            scores = self._gpu_mgr(gpuArray[:len(pending), :, :, :])
            for sid, score in zip(pending, scores):
                cid, vid, jid = sid
                logging.debug('Enqueueing job %i on video %i for client %i' % (
                    jid, vid, cid))
                self._output_queues[cid].put((sid, score))

    def stop(self):
        '''
        Stops the associated threads, as well
        as all child predictors.
        '''
        try:
            if self._terminate.is_set():
                logging.debug('Already terminated.')
                return
        except:
            logging.debug('Already terminated.')
            return 
        logging.debug('Termination request initiated')
        logging.debug('Bringing down job server')
        self._terminate.set()
        # terminate garbage collect
        logging.debug('Notifying job collector')
        with self._client_has_died:
            self._client_has_died.notify_all()
        # terminate submit -- in case it's waiting
        # on a job.
        logging.debug('Notifying submittor / allocator')
        self._client2mgr.put(None)
        logging.debug('Joining garbage collector')
        self._garbage_collect_thread.join()
        logging.debug('Joining submittor / allocator')
        self._submit_thread.join()
        logging.debug('Shutting down threaded queue')
        with self._gpu2mgr.all_tasks_done:
            self._gpu2mgr.all_tasks_done.notify_all()
        logging.debug('Joining threaded queue')
        self._gpu2mgr.join()

    def __del__(self):
        self.stop()