'''
Predictor classes for the model

Note that as the code version changes, code may need to be added to
deal with backwards compatibility when pickling/unpickling. See the
Python pickling docs about those issues.

Copyright: 2016 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
Author: Nick Dufour
'''
import hashlib
import logging
import numpy as np
from PIL import Image
import os
import pyflann
import tempfile
import utils.obj
from utils import statemon
import threading
from grpc.beta import implementations
import aquila_inference_pb2  # TODO: make sure this is correct.
from grpc.beta.interfaces import ChannelConnectivity

_log = logging.getLogger(__name__)

statemon.define('lost_server_connection', int)  # lost the server connection
statemon.define('unable_to_connect', int)  # could not connect to server in the first place

def _resize_to(img, w=None, h=None):
  '''
  Resizes the image to a desired width and height. If either is undefined,
  it resizes such that the defined argument is satisfied and preserves aspect
  ratio. If both are defined, resizes to satisfy both arguments without
  preserving aspect ratio.

  Args:
    img: A PIL image.
    w: The desired width.
    h: The desired height.
  '''
  ow, oh = img.size
  asp = float(ow) / oh
  if w is None and h is None:
    # do nothing
    return img
  elif w is None:
    # set the width
    w = int(h * asp)
  elif h is None:
    h = int(w / asp)
  return img.resize((w, h), Image.BILINEAR)


def _center_crop_to(img, w, h):
  '''
  Center crops image to desired size. If either dimension of the image is
  already smaller than the desired dimensions, the image is not cropped.

  Args:
    img: A PIL image.
    w: The width desired.
    h: The height desired.
  '''
  ow, oh = img.size
  if ow < w or oh < h:
    return img
  upper = (oh - h) / 2
  lower = upper + h
  left = (ow - w) / 2
  right = left + w
  return img.crop((left, upper, right, lower))


def _pad_to_asp(img, asp):
  '''
  Symmetrically pads an image to have the desired aspect ratio.

  Args:
    img: A PIL image.
    asp: The aspect ratio, a float, as w / h
  '''
  ow, oh = img.size
  oasp = float(ow) / oh
  if asp > oasp:
    # the image is too narrow. Pad out width.
    nw = int(oh * asp)
    left = (nw - ow) / 2
    upper = 0
    newsize = (nw, oh)
  elif asp < oasp:
    # the image is too short. Pad out height.
    nh = int(ow / asp)
    left = 0
    upper = (nh - oh) / 2
    newsize = (ow, nh)
  else:
    return img
  nimg = Image.new(img.mode, newsize)
  nimg.paste(img, box=(left, upper))
  return nimg


def _aquila_prep(image):
    '''
    Preprocesses an image so that it is appropriate
    for input into Aquila. Aquila was trained on
    images in RGB order, padded to an aspect ratio of
    16:9 and then resized to 299 x 299. We will replicate
    this here. For now, we assume the image provided has
    been obtained from OpenCV (and so is BGR) and will use
    PIL to prep the image.
    '''
    img = Image.fromarray(image[:,:,::-1])
    img = _pad_to_asp(img, 16./9)
    # resize the image to 299 x 299
    img = _resize_to(img, w=314, h=314)
    img = _center_crop_to(img, w=299, h=299)
    return np.array(img)



class Predictor(object):
    '''An abstract valence predictor.

    This class should be specialized for specific models
    '''
    def __init__(self, feature_generator = None):
        self.feature_generator = feature_generator
        self.__version__ = 3
        self.async = False
        self.concurrency = 1

    def __str__(self):
        return utils.obj.full_object_str(self)

    def add_feature_vector(self, features, score, metadata=None):
        '''Adds a veature vector to train on.

        Inputs:
        features - a 1D numpy vector of the feature vector
        score - score of this example.
        metadata - metadata to attach to this example
        '''
        raise NotImplementedError()

    def add_image(self, image, score, metadata=None):
        '''Add an image to train on.

        Inputs:
        image - numpy array of the image in BGR format (aka OpenCV)
        score - floating point valence score
        metadata - metadata to attach to this example
        '''
        self.add_feature_vector(self.feature_generator.generate(image),
                                score,
                                metadata=metadata)

    def add_images(self, data):
        '''Adds multiple images to the model.

        Input:
        data - iteration of (image, score) tuples
        '''
        for image, score in data:
            self.add_image(image, score)

    def train(self):
        '''Train on any images that were previously added to the predictor.'''
        raise NotImplementedError()


    def predict(self, image, *args, **kwargs):
        '''Wrapper for image valence prediction functions'''
        if self.async:
            return self._predictasync(image, *args, **kwargs)
        else:
            return self._predict(image, *args, **kwargs)

    def _predict(self, image, *args, **kwargs):
        '''Predicts the valence score of an image synchronously.

        Inputs:
        image - numpy array of the image
        
        Returns: predicted valence score

        Raises: NotTrainedError if it has been called before train() has.
        '''
        raise NotImplementedError()

    def _predictasync(self, image, *args, **kwargs):
        '''
        Asynchronous prediction using the deepnet Aquila's
        server. 

        Inputs:
        image - numpy array of the image, as an N x M x 3
        array of integers.

        Returns: A prediction future.
        '''
        raise NotImplementedError()

    def reset(self):
        '''Resets the predictor by removing all the data/model.'''
        raise NotImplementedError()

    def hash_type(self, hashobj):
        '''Updates a hash object with data about the type.'''
        hashobj.update(self.__class__.__name__)
        self.feature_generator.hash_type(hashobj)

    def complete(self):
        '''
        Returns True when all requests are complete.
        '''
        if not self.async:
            # you are running synchronously, so it's fine.
            return True
        else:
            raise NotImplementedError()

    def shutdown(self):
        '''
        There is currently a bug in the gRPC garbage collection,
        so this function is useful to disable stubs and channels
        that are not created within the frame of a function (i.e.,
        if the stub / channel is created in __main__, or as the
        attribute of a class, as in DeepnetPredictor).
        '''
        pass


class DeepnetPredictor(Predictor):
    '''Prediction using the deepnet Aquila (or an arbitrary predictor). 
    Note, this does not require you provision a feature generator for 
    the predictor.

    The connection to the server is maintained internally, using the
    _check_conn method. This is called at the outset and invokes
    _connect, which creates the channel and the stub, and adds
    _check_conn as a callback. _check_conn will ensure that the
    state of the ready event is set appropriately as the state of
    the gRPC channel changes.'''

    def __init__(self, concurrency=10, port=9000,
                 aquila_connection=None):
        '''
        concurrency - The maximum number of simultaneous requests to 
        submit.
        port - the port on which to establish the connection.
        aquila_connection - An instance (or singleton) of an object
        that supplies the get_ip method, which returns an IP address
        of an Aquila server as a string.
        '''
        super(DeepnetPredictor, self).__init__()
        self.concurrency = concurrency
        self.aq_conn = aquila_connection
        self.port = port
        self._cv = threading.Condition()
        self._ready = threading.Event()
        self.active = 0
        self.done = 0
        self.async = True
        self._connect(force_refresh=False)

    def _connect(self, force_refresh):
        '''
        Establishes a connection to the server.
        '''
        host = self.aq_conn.get_ip(force_refresh=force_refresh)
        _log.debug('Establishing connection on %s' % host)
        # open question: do we have to destroy old channels?
        # open question: what happens to futures that derive from destroyed
        #   channels?
        self.channel = implementations.insecure_channel(host, self.port)
        # register callback
        self.channel.subscribe(self._check_conn, try_to_connect=True)
        self.stub = aquila_inference_pb2.beta_create_AquilaService_stub(
            self.channel)

    def _init_check(self, status=None):
        '''
        Serves as the callback to a channel when it is under
        construction / connecting. Once the channel reaches a
        ready state for the first time, it unsubscribes itself
        and subscribes the regular check. It is necessary to
        verify that a server can be connected to. 

        This function is the _only_ function that can authorize
        requests to begin, which it does by setting the _ready
        event.
        '''
        if status is ChannelConnectivity.READY:
            _log.debug('Server has been reached')
            self.channel.unsubscribe(self._init_check)
            self.channel.subscribe(self._check_conn)
            self._ready.set()
        elif (status is ChannelConnectivity.TRANSIENT_FAILURE or
            status is ChannelConnectivity.FATAL_FAILURE):
            # attempt to connect again
            statemon.state.increment('unable_to_connect')
            _log.debug('Error connecting to server, trying new server')
            self._connect(force_refresh=True)

    def _check_conn(self, status=None):
        '''
        Checks the connection, as a callback.
        '''
        if (status is ChannelConnectivity.TRANSIENT_FAILURE or
            status is ChannelConnectivity.FATAL_FAILURE):
            # the connection has been lost
            self._ready.clear()
            statemon.state.increment('lost_server_connection')
            _log.warn('Lost connection to server, trying another')
            self._connect(force_refresh=True)
        else:
            _log.debug('Connection status is %s' % str(status))

    def _predictasync(self, image, timeout=10.0):
        '''
        image: The image to be scored, as a OpenCV-style numpy array.
        timeout: How long the request lasts for before expiring. 
        '''
        conn_est = self._ready.wait()  # TODO: do we want a timeout?
        if not conn_est:
            _log.error('Connection not established in time.')
            # what do we do here? 
            return
        image = _aquila_prep(image)
        request = aquila_inference_pb2.AquilaRequest()
        request.image_data.extend(image.flatten().tolist())
        with self._cv:
            while self.active == self.concurrency:
                self._cv.wait()
        self.active += 1
        result_future = self.stub.Regress.future(request, timeout)  # 10 second timeout
        result_future.add_done_callback(
            lambda result_future: self.async_cb_hand(result_future))
        return result_future

    def async_cb_hand(self, result_future):
        '''
        Housekeeping handler for predictor to monitor the number of active
        inference requests.

        NOTE: This does not handle any errors, this is up to the true callback
        function to handle.
        '''
        with self._cv:
            self.done += 1
            self.active -= 1
            self._cv.notify()

    def complete(self):
        '''
        Returns True when it is safe to terminate.
        '''
        with self.cv:
            return self.active == 0

    def __del__(self):
        del self.channel
        del self.stub
        super(DeepnetPredictor, self).__del__()


class KFlannPredictor(Predictor):
    '''Approximate k nearest neighbour using flann.'''
    
    def __init__(self, feature_generator, k=3, target_precision=0.95,
                 seed=802374, max_images_per_video=6):
        super(KFlannPredictor, self).__init__(feature_generator)

        self.k = k
        self.target_precision = target_precision
        self.seed = seed
        self.max_images_per_video = max_images_per_video

        self.reset()

    def reset(self):
        self.is_trained = False

        self.data = []
        self.scores = []
        self.metadata = []
        self.video_ids = []
        self.flann = pyflann.FLANN()
        self.params = None

    def __str__(self):
        return utils.obj.full_object_str(self,
                                         exclude=['data', 'scores', 'metadata',
                                                  'video_ids'])

    def add_feature_vector(self, features, score, metadata=None,
                           video_id=None):
        if self.is_trained:
            raise AlreadyTrainedError()
        self.scores.append(score)
        self.data.append(features)
        self.metadata.append(metadata)
        self.video_ids.append(hash(video_id))

    def train(self):
        _log.info('Training predictor with %i examples.' % len(self.data))

        sample_fraction = 0.20
        if len(self.data) > 10000:
            sample_fraction=0.05
        self.params = self.flann.build_index(
            np.array(self.data),
            algorithm='autotuned',
            target_precision=self.target_precision,
            build_weight=0.01,
            memory_weight=0.5,
            sample_fraction=sample_fraction,
            random_seed=self.seed,
            log_level='info')
        _log.info('Built index with parameters: %s' % self.params)
        self.is_trained = True

    def _predict(self, image, video_id=None):
        if not self.is_trained:
            raise NotTrainedError()

        if video_id is None:
            return self.score_neighbours(self.get_neighbours(image, k=self.k))

        # If we don't want to include images from the same
        # video, we need to ask for extra neighbours. This
        # should only happen in training a higher order predictor/classifier.
        k = self.k + self.max_images_per_video
        video_hash = hash(video_id)

        neighbours = self.get_neighbours(image, k=k)
        valid_neighbours = []
        for neighbour in neighbours:
            if len(valid_neighbours) == self.k:
                break
            
            if neighbour[3] <> video_hash:
                valid_neighbours.append(neighbour)
        return self.score_neighbours(valid_neighbours)

    def score_neighbours(self, neighbours):
        '''Returns the score for k neighbours.'''
        if neighbours[0][1] < 1e-4:
            # We have an extry that is almost identical, so just use that label
            return neighbours[0][0]
        scores = [x[0] for x in neighbours]
        dists = np.array([1.0/x[1] for x in neighbours])
        return np.dot(scores, dists) / np.sum(dists)

    def get_neighbours(self, image, k=3):
        '''Retrieve N neighbours of an image.

        Inputs:
        image - numpy array of the image in opencv format
        n - number of neighbours to return

        Outputs:
        Returns a list of [(score, dist, metadata, video_id)]

        '''
        features = self.feature_generator.generate(image)
        idx, dists = self.flann.nn_index(features, k,
                                         checks=self.params['checks'])
        if k == 1:
            # When k=1, the dimensions get squeezed
            return [(self.scores[idx[0]], dists[0], self.metadata[idx[0]])]
        
        return [(self.scores[i], dist, self.metadata[i])
                for i, dist in zip(idx[0], dists[0])]

    def __getstate__(self):
        # The flann model can't be pickled directly. the save_model()
        # function must be called which has to write to a file. So, we
        # write to a temporary file and then read that file back and
        # pickle the strings. sigh.
        state = self.__dict__.copy()
        tfile,tfilename = tempfile.mkstemp()
        flann_string = ''
        try:
            os.close(tfile)
            state['flann'].save_index(tfilename)
            with open(tfilename, 'rb') as f:
                flann_string = f.read()
        finally:
                os.unlink(tfilename)
        state['flann'] = flann_string
        return state

    def __setstate__(self, state):
        # Rebuild the flann index using the load_index function
        tfile,tfilename = tempfile.mkstemp()
        new_flann = pyflann.FLANN()
        try:
            os.close(tfile)
            with open(tfilename, 'w+b') as f:
                f.write(state['flann'])
            if len(state['data']) > 0:
                new_flann.load_index(tfilename, np.array(state['data']))
        finally:
            os.unlink(tfilename)
        state['flann'] = new_flann

        self.__dict__ = state

# -------------- Start Exception Definitions --------------#

class Error(Exception):
    '''Base class for exceptions in this module.'''
    pass

class NotTrainedError(Error):
    def __init__(self, message = ''):
        Error.__init__(self, "The model isn't trained yet: %s" % message)

class AlreadyTrainedError(Error):
    def __init__(self, message = ''):
        Error.__init__(self, "The model is already trained: %s" % message)
