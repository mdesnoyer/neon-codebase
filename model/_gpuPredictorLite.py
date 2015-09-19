'''
~ Simplified Version ~

This is the core of the model when using GPU Video Clients.
It is a python wrapper for the GPU implementation for our 
new model, which is based off Google's 'Inception' architecture.
'''

import multiprocessing
import os
import threading
import logging
from glob import glob
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
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
                    )

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
        logging.debug('instantiated.')

    def __call__(self, data_array):
        '''
        Actually executes the prediction, provided with an 
        N x 3 x H x W array of N images that have already
        been preprocessed and resized.
        '''
        logging.debug('Running chunk of %i images on GPU'%(data_array.shape[0]))
        if type(data_array).__module__ != np.__name__:
            raise TypeError("data_array type is %s, must be %s"%(
                str(type(data_array)), str(np.__name__)))
        if data_array.dtype != np.dtype('float32'):
            raise ValueError("data_array must be float32")
        if np.any(data_array.shape[1:] != self.image_dims):
            raise ValueError(
                "data_array must have shape N x %i x %i x %i"%(
                self.image_dims[0], self.image_dims[1], self.image_dims[2]))
        out = self.forward_all(**{self.inputs[0]: data_array})
        predictions = np.exp(out[self.outputs[0]])
        return list(predictions[:,0])

class JobManager(multiprocessing.Process):
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
    '''
    def __init__(self, model_file, pretrained_file,
                 batchSize=32, image_dims=[224,224,3],
                 image_mean=[104,117,123], nGPUs=2,
                 N=None):
        self._manager = multiprocess.Manager()
        self._clients = self._manager.list()
        self._dead_clients = self._manager.list()
        self._valid_videos = self._manager.list()
        self._terminate_self = self._manager.Event()
        self._client_has_died = self._manager.Condition() # wakes up the garbage collector thread
        self._output_queues = dict()  # client ID --> output pipeline (queue)
        self._pending_counts = dict() # client ID --> # of jobs pending
        self._max = N
        self._mgr2gpu = Queue()
        self._client2mgr = self._manager.Queue()
        self._QM = _QueueManager(model_file, pretrained_file,
                                self._inQ, batchSize, 
                                image_dims, image_mean,
                                timeout, nGPUs)
        self._QM.start()
        self._input = None
        self._allocator = None
        self._garbage_collector = None
        self._start_threads()

    def _start_threads(self):
        '''
        Initializes the allocator, garbage collector, and
        input handling threads.
        '''

    def register_client(self):
        '''
        Register and returns a new _Predictor object
        '''

    def _prune_dead_clients(self):
        '''
        Destroys the dead client's output queue.
        '''
        self._client_has_died.acquire()
        while True:
            self._client_has_died.wait()
            if self._terminate_self.is_set():
                return # you've been ordered to die
            while self._dead_clients:
                dcq = self._dead_clients.pop()
                _ = self._output_queues.pop(dcq)

    def _submit(self):
        '''
        Submits data into the qeueue
        '''
        self._job_was_submitted.acquire()
        while True:
            self._job_was_submitted.wait()
            if self._terminate_self.is_set():
                return
            ID, data = self._client2mgr.get_nowait()
            client, video, jid = ID
            if client not in self._valid_clients:
                continue
            if video not in self._valid_videos:
                continue
                

    def stop(self):
        '''
        Stops the associated threads, as well
        as all child predictors.
        '''
        self._terminate_self.set()
        with self._client_has_died:
            self._client_has_died.notify_all()
        with self._job_has_finished:
            self._job_has_finished.notify_all()
        with self._job_was_submitted:
            self._job_was_submitted.notify_all()
        self._QM.join()
        self._input.join()
        self._allocator.join()
        self._garbage_collector.join()

