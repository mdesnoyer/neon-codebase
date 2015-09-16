'''
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

class _PredictorGPU(caffe.Net):
    def __init__(self, model_file, pretrained_file):
        '''
        model_file = the deploy prototext
        pretrained_file = the *.caffemodel file with 
            pretrained weights.
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
        img = cv2.imread(imgfn)
        if img == None:
            raise ValueError("Could not find image %s"%(imgfn))
        return img

    def __call__(self, img):
        logging.debug('Preprocessing')
        if type(img) == str:
            img = self._read(img)
        if not type(img).__module__ == np.__name__:
            raise TypeError("Image must be a numpy array (or str filename)")
        img = img.astype(np.float32)
        img = cv2.resize(img, (self.image_dims[0], self.image_dims[1]))
        if img.shape[2] == 1:
            # it's a black and white image, we have to colorize it
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
        elif img.shape[2] != 3:
            raise ValueError("Image has the incorrect number of channels")
        # subtract the channelwise image means
        img -= self.image_mean
        img = img.transpose(2, 0, 1)
        return img

class _Predictor(multiprocessing.Process):
    def __init__(self, model_file, pretrained_file, inQ, 
                 batchSize, image_dims, image_mean, 
                 timeout=None, nGPUs=None):
        '''
        The new predictor. This spawns some number of GPU predictors.
        Its input is performed via inQ, which receives input as tuples:
        
        (image, Value, condition)

        Any time a Value is updated, the corresponding condition is set,
        so that the requester knows that their job is completed.
        '''
        super(_Predictor, self).__init__()
        self.model = model_file
        self.pretrained = pretrained_file
        self.inQ = inQ
        self.batchSize = batchSize
        self.prep = _Preprocess(image_dims, image_mean)
        if nGPUs == None:
            nGPUs = 1 # or max GPUs
        if timeout == None:
            timeout = np.inf
        self.timeout = timeout
        self.nGPUs = nGPUs
        self.image_dims = image_dims
        self.kill_received = multiprocessing.Event()
        logging.debug('Initialization complete')

    def _grab(self, first=False):
        start = time()
        while True:
            if not first:
                logging.debug('Time remaining: %.2f'%(self.timeout - time() + start))
                if ((time() - start) > self.timeout):
                    logging.debug('Timed out -- running')
                    return (None, None, None)
            try:
                #logging.debug('Fetching')
                item = self.inQ.get(timeout=2)
                return item
            except:
                #logging.debug('Timed out!')
                if self.kill_received.is_set():
                    logging.debug('Received kill signal!')
                    return (None, None, None)

    def run(self):
        logging.debug('Starting')
        self.threads = []
        for gpuid in range(self.nGPUs):
            t = threading.Thread(name="GPU Worker %i"%(gpuid), 
                                 target=self._run, 
                                 args=(gpuid,))
            self.threads.append(t)
            t.start()
        self.kill_received.wait()
        logging.debug('joining threads')
        for t in self.threads:
            t.join()

    def _run(self, gpuid):
        predict = _PredictorGPU(self.model, self.pretrained)
        logging.debug('Predictor %i is starting'%(gpuid))
        array = np.ascontiguousarray(np.zeros((
            self.batchSize, 3, self.image_dims[0], self.image_dims[1]
            )).astype(np.float32))
        logging.debug('Array instantiated')
        while True:
            # wait to get first job
            logging.debug('Waiting for first job')
            image, value, condition = self._grab(True)
            if self.kill_received.is_set():
                logging.debug('Received kill signal!')
                return
            logging.debug('First job acquired')
            start = time()
            curBatchSize = 1
            values = []
            conditions = set()
            values.append(value)
            conditions.add(condition)
            image = self.prep(image)
            array[curBatchSize-1,:,:,:] = image
            #while (((time() - start) < self.timeout) and 
            #    (curBatchSize < batchSize)):
            while True:
                logging.debug('Time remaining: %.2f'%(self.timeout - time() + start))
                if (curBatchSize == self.batchSize):
                    logging.debug('Batch size met -- running')
                    break
                image, value, condition = self._grab()
                if ((time() - start) > self.timeout):
                    logging.debug('Timed out -- running')
                    break
                if self.kill_received.is_set():
                    logging.debug('Received kill signal!')
                    return
                curBatchSize += 1
                image = self.prep(image)
                array[curBatchSize-1,:,:,:] = image
                values.append(value)
                conditions.add(condition)
            scores = predict(array[:curBatchSize, :, :, :])
            logging.debug('Scores obtained, setting values')
            for v, s in zip(values, scores):
                v.set(s)
            logging.debug('Notifying conditions')
            for c in conditions:
                with c.acquire():
                    c.notifyAll()

    def stop(self):
        logging.debug('sending kill signal')
        self.kill_received.set()

class Predictor(object):
    '''
    The predictor class that the video client will
    interact with. Implements predict(), which returns
    predicted values ASYNCHRONOUSLY. The true value is
    only returned after-the-fact.
    '''
    def __init__(self, model_file, pretrained_file, 
                 batchSize=32, image_dims=[224,224,3], 
                 image_mean=[104, 117, 123], 
                 timeout=None, nGPUs=None):
        self._manager = multiprocessing.Manager()
        self._inQ = self._manager.Queue()
        self._pred = _Predictor(model_file, pretrained_file,
                                self._inQ, batchSize, 
                                image_dims, image_mean,
                                timeout, nGPUs)
        self._pred.start()


    def predict(self, image, condition, value):
        self._inQ.put((image, condition, value))

    def stop(self):
        self._pred.stop()

