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
        '''
        Actually performs the preprocessing
        '''
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

#class _QueueManager(multiprocessing.Process):
class _QueueManager(threading.Thread):
    '''
    This is the Queue Manager, of which there
    is only one instance but as a separate process
    together with the GPU instances. 

    It accepts new job requests via inQ, which 
    consist of tuples of (image, job), where
    image is an 3 x N x N numpy array.

    Preprocessing happens upstream, in the
    JobManager.

    Job is an instance of a class defined in this
    module, which is designed to partially emulate
    the concurrent.futures.Future class.
    '''
    def __init__(self, model_file, pretrained_file, inQ, 
                 batchSize, image_dims, image_mean, 
                 nGPUs=None):
        '''
        model_file : the caffe model to use
        pretrained_file : array of model weights
        inQ : multiprocessing queue for job submission
        batchSize : the number of jobs to handle at once
        image_dims : image dimensions
        image_mean : channelwise image means
        nGPUs : the number of GPUs to use (default: Max)
        '''
        super(_QueueManager, self).__init__()
        self.model = model_file
        self.pretrained = pretrained_file
        self.inQ = inQ
        self.batchSize = batchSize
        if nGPUs == None:
            nGPUs = 1 # or max GPUs
        self.nGPUs = nGPUs
        self.image_dims = image_dims
        self.kill_received = multiprocessing.Event()
        logging.debug('Initialization complete')

    def _grab(self, first=False):
        '''
        Fetches a job.
        '''
        if first:
            while True:
                try:
                    item = self.inQ.get(timeout=2)
                    return item
                except:
                    if self.kill_received.is_set():
                        logging.debug('Received kill signal!')
                        return (None, None)
        else:
            try:
                item = self.inQ.get_nowait
                return item
            except:
                logging.debug('Queue is empty')
                return (None, None)

    def run(self):
        '''
        Starts itself.
        '''
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
        '''
        *Actually* starts itself
        '''
        predict = _GPUMgr(self.model, self.pretrained)
        logging.debug('Predictor %i is starting'%(gpuid))
        array = np.ascontiguousarray(np.zeros((
            self.batchSize, 3, self.image_dims[0], self.image_dims[1]
            )).astype(np.float32))
        logging.debug('Array instantiated')
        while True:
            # wait to get first job
            logging.debug('Waiting for first job')
            item = self._grab(True)
            if self.kill_received.is_set():
                logging.debug('Received kill signal!')
                return
            logging.debug('First job acquired')
            cjobs = []
            cjobs.append(item)
            while len(cjobs) < self.batchSize:
                item = self._grab()
                if self.kill_received.is_set():
                    logging.debug('Received kill signal!')
                    return
                if item[0] == None:
                    break
                cjobs.append(item)
            logging.debug('%i jobs found'%len(cjobs))
            logging.debug('Checking for cancelled jobs')
            cjobs = filter(lambda x: not x[1].is_cancelled(), cjobs)
            logging.debug('%i jobs remain'%len(cjobs))
            for n, (image, job) in enumerate(cjobs):
                array[n,:,:,:] = image
                job.set_running()
            scores = predict(array[len(cjobs), :, :, :])
            for j, s in zip(scores, cjobs):
                j[1].set_result(s)

    def stop(self):
        logging.debug('sending kill signal')
        self.kill_received.set()

class Job(object):
    '''
    The Job object is designed to emulate, as much as
    possible, the Future class from concurrent.futures.
    It enables requesters to cancel jobs that are in 
    the pipeline, and manages signals back and forth
    from various processes.

    You should never have to invoke or use this yourself,
    instead its operation takes place entirely behind-the-
    scenes.
    '''
    PENDING = 0
    RUNNING = 1
    CANCELLED = 2
    FINISHED = 3
    def __init__(self, cond, result, state, 
                 fsibs=None, finished=None,
                 jid=None):
        if jid = None:
            jid = id(self)
        self.id = jid
        self._cond = cond
        self._fsibs = fsibs # finished siblings
        self._finished = finished
        self._result = result
        self._result.set(0.)
        self._state = state
        self._state.set(Job.PENDING)

    def set_running(self):
        with self._cond:
            if self._state.get() == Job.PENDING:
                self._state.set(Job.RUNNING)

    def cancel(self):
        with self._cond:
            if self._state.get() == Job.PENDING:
                self._state.set(Job.CANCELLED)
                # let the parents know so they can prune it
                self._inc_fsibs()
        self._cond.notify_all()

    def _inc_fsibs(self):
        self._fsibs.release()

    def set_result(self, result):
        if type(result) != float:
            raise ValueError('Result must be set to a float')
        with self._cond:
            if self._state.get() in [Job.RUNNING, Job.PENDING]:
                self._state.set(Job.FINISHED)
                self._result.set(result)
                if self._finished != None:
                    self._finished.append((self.id, result))
                if self._parent != None:
                    # ping the parent
                    self._inc_fsibs()
        self._cond.notify_all()

    def result(self):
        with self._cond:
            if self._state.get() == Job.FINISHED:
                return self._result.get()

    def get_result(self):
        '''
        Blocks until the result is ready.
        '''
        with self._cond:
            self._cond.wait()
            return self.result()

    def is_cancelled(self):
        '''
        Returns True if the job request
        has been cancelled
        '''
        with self._cond:
            return self._state.get() == Job.CANCELLED

    def is_finished(self):
        '''
        Returns True if the job is finished.
        '''
        with self._cond:
            return self._state.get() == Job.FINISHED

class _Predictor(object):
    '''
    The predictor class, which manages all prediction
    (evaluated asynchronously) as well as maintaining
    a heap of scores, sorted from highest to lowest.

    These shouldn't be instantiated on their own, but
    are instead initialized by the JobManager instance.

    Additionally, this handles all the preprocessing of
    the images, to minimize the amount of data that has
    to be sent between processes.
    '''
    def __init__(self, submit, complete, 
                 fkids, image_dims, image_mean,
                 kill_switch, N=None):
        '''
        submit : a submit function, provided by JobManager()
        complete : a process-safe list of completed jobs
        fkids : a list of finished child jobs ('kids')
        image_dims : image dimensions
        image_mean : channelwise image mean
        kill_switch : multiprocessing event for termination
        N : maximum number of pending jobs allowed
        '''
        self._submit = submit
        self._complete = complete
        self._fkids = fkids # finished kids
        self._initialized = False
        self._pending = []
        self._total = 0
        self._kill = kill_switch
        self._prep = _Preprocess(image_dims, image_mean)
        if N == None:
            N = np.inf
        self._max = N

    def __call__(self, image, jid=None):
        '''
        Submits a job. If the number of pending
        jobs is too large, then blocks until some
        complete.
        '''
        if len(self._pending) >= self._max:
            # then you have too many pending jobs.
            # wait for a semaphore to be released,
            # and call _filter_jobs
            logging.debug('Too many pending jobs, waiting for jobs to complete')
            self._fkids.acquire()
            self._fkids.release()
            self._filter_jobs()
            image = self._prep(image)
        logging.debug('Submitting '+str(jid))
        job = self._submit(image, jid, 
                           self._pending,
                           self._complete,
                           self._kids)
        with self._pending_lock:
            self._pending.append(job)

    def stop(self):
        '''
        Halts the process and kills the thread.
        '''
        logging.debug('Stopping')
        self._kill.set()
        for job in self._pending:
            job.cancel()

    def reset(self):
        '''
        Removes pending jobs, and empties out
        the results and pending lists.
        '''
        logging.debug('Resetting self')
        for job in self._pending:
            job.cancel()

        while self._complete or self._pending:
            self._filter_jobs()

    def get(self):
        '''
        Returns results as tuples:
        id, score
        '''
        self._filter_jobs()
        while len(self._complete):
            results.append(self._complete.pop())
            self._total+=1
        logging.debug('%i jobs finished [%i total]'%(len(results), self._total))
        return results
    
    def _filter_jobs(self):
        '''
        Filters pending jobs in-place
        '''
        logging.debug('Filtering jobs in-place')
        index = 0
        while index < len(self._pending):
            cont = self._fkids.acquire(False)
            if not cont:
                break
            b1 = self._pending[index].is_cancelled()
            b2 = self._pending[index].is_finished()
            if b1 or b2:
                j = self._pending.pop(index)
                if b1:
                    logging.debug('Filtering job %s because it was cancelled'%(j.id))
                if b2:
                    logging.debug('Filtering job %s because it is done'%(j.id))
            else:
                index += 1

class JobManager(object):
    '''
    Manager Job creation and submission, as well
    as creates the manager as well. It also yields
    predictor objects.
    '''
    def __init__(self, model_file, pretrained_file, 
                 batchSize=32, image_dims=[224,224,3], 
                 image_mean=[104, 117, 123], 
                 nGPUs=None, N=None):
        '''
        model_file : the deploy prototext
        pretrained_file : the *.caffemodel file with 
                          pretrained weights.
        batchSize : the number of jobs to handle at once
        image_dims : image dimensions
        image_mean : channelwise image means
        nGPUs : the number of GPUs to use (default: Max)
        N : maximum number of pending jobs allowed
        '''
        self._manager = multiprocessing.Manager()
        #self._inQ = self._manager.Queue()
        # managers cannot put their own objects into their
        # own queues!
        self._kill_switches = []
        self._inQ = Queue()
        self._QM = _QueueManager(model_file, pretrained_file,
                                self._inQ, batchSize, 
                                image_dims, image_mean,
                                timeout, nGPUs)
        self._QM.start()


    def yieldPred(self):
        '''
        Returns a predictor (i.e., a requester)
        '''
        pending = self._manager.list()
        finished = self._manager.list()
        fkids = self._manager.Semaphore()
        kill_switch = self._manager.Event()
        self._kill_switches.append(kill_switch)
        return _Predictor(self, pending, finished, 
                          fkids, image_dims,
                          image_mean, kill_switch, N)

    def submit(self, image, jid, pending, finished, fsibs):
        '''
        Submits a job.
        '''
        scond = self._manager.Condition()
        result = m.Value('f', 0.)
        state = m.Value('i', 0)
        job = Job(scond, result, state, fsibs, 
                  finished, jid)
        self._inQ.put((image, job))
        return job

    def stop(self):
        '''
        Halts the JobManager, and everything
        spawned from it.
        '''
        for kill_switch in self._kill_switches:
            kill_switch.set()
        self._QM.stop()
        self._QM.join()