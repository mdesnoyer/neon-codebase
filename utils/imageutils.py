'''
Image utility methods for PIL images.

For OpenCV images see utils/pycvutils.py

Author: Sunil Mallya (mallya@neon-lab.com)
Copyright 2013 Neon Labs
'''

import logging
import numpy as np
from PIL import Image
import random

_log = logging.getLogger(__name__)

class PILImageUtils(object):
    '''Utilities for dealing with PIL images.'''

    def __init__(self):
        pass

    @classmethod
    def resize(cls, im, im_h=None, im_w=None):
        ''' Resize image.

        If either the desired height or the desired width are not
        specified, the aspect ratio is preserved.
        '''

        #TODO: instance check, PIL.Image check is ambigous, 
        #type is JpegImagePlugin?

        if im_h is None and im_w is None:
            return im

        ar = im.size
        #resize on either dimension
        if im_h is None:
            im_h = int(float(ar[1])/ar[0] * im_w) 

        elif im_w is None:
            im_w = int(float(ar[0])/ar[1] * im_h) 

        image_size = (im_w, im_h)
        image = im.resize(image_size, Image.ANTIALIAS)
        return image

    @classmethod
    def create_random_image(cls, h, w):
        ''' return a random image '''
        return Image.new("RGB", (w, h), None)

    @classmethod
    def to_cv(cls, im):
        '''Convert a PIL image to an OpenCV one in BGR format.'''
        if im.mode == 'RGB':
            return np.array(im)[:,:,::-1]
        elif im.mode in ['L', 'I', 'F']:
            return np.array(im)
        
        raise NotImplementedError(
            'Conversion for mode %s is not implemented' % im.mode)

    @classmethod
    def from_cv(cls, im):
        '''Converts an OpenCV BGR image into a PIL image.'''
        if len(im.shape, 3):
            return Image.fromarray(im[:,:,::-1])
        else:
            return Image.fromarray(im)
            
