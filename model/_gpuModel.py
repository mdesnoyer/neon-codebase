'''
This is the core of the model when using GPU Video Clients.
It is a python wrapper for the GPU implementation for our 
new model, which is based off Google's 'Inception' architecture.
'''

import os
from glob import glob

import caffe
import cv2
import numpy as np

caffe.set_mode_gpu()

class predictor(caffe.Net):
    def __init__(self, model_file, pretrained_file):
        '''
        model_file = the deploy prototext
        pretrained_file = the *.caffemodel file with 
            pretrained weights.
        '''
        caffe.Net.__init__(self, model_file, pretrained_file, caffe.TEST)
        in_ = self.inputs[0]
        self.image_dims = np.array(self.blobs[in_].data.shape[1:])

    def __call__(self, data_array):
        '''
        Actually executes the prediction, provided with an 
        N x 3 x H x W array of N images that have already
        been preprocessed and resized.
        '''
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
        predictions = out[self.outputs[0]]
        return list(predictions[:,0])

class preprocess(object):
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
