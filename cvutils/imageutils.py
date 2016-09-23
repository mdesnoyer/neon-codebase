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
import tornado.httpclient
import tornado.gen
import utils.http
import utils.sync

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
        return Image.fromarray(np.array(
            np.random.random_integers(0, 255, (h, w, 3)),
            np.uint8))

    @classmethod
    def create_random_gray_image(cls, h, w):
        ''' Return a random grayscale image '''
        ''' return a random image '''
        return Image.fromarray(np.array(
            np.random.random_integers(0, 255, (h, w)),
            np.uint8))

    @classmethod
    def to_cv(cls, im):
        '''Convert a PIL image to an OpenCV one in BGR format.'''

        # Pass images of these modes through.
        if im.mode in ['L', 'I', 'F']:
            return np.array(im)
        # All other image formats get conversion to mode=RGB.
        im = PILImageUtils.convert_to_rgb(im)
        return np.array(im)[:,:,::-1]

    @classmethod
    def from_cv(cls, im):
        '''Converts an OpenCV BGR image into a PIL image.'''
        if len(im.shape) == 3:
            return Image.fromarray(im[:,:,::-1])
        else:
            return Image.fromarray(im)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def download_image(cls, url):
        '''Downloads an image from a given url.

        Returns the image in PIL format.
        '''
        response = yield utils.http.send_request(
            tornado.httpclient.HTTPRequest(url,
                                           request_timeout=60.0,
                                           connect_timeout=10.0),
            async=True)
        if response.error:
            _log.error('Error retrieving image from: %s. %s' % 
                       (url, response.error))
            raise response.error

        try:
            image = Image.open(response.buffer)
        except IOError as e:
            _log.error('Invalid image at %s: %s' % (url, e))
            raise
        except ValueError as e:
            _log.error('Invalid image at %s: %s' % (url, e))
            raise IOError('Invalid image at %s: %s' % (url, e))
        except TypeError as e:
            _log.error('Invalid image at %s: %s' % (url, e))
            raise IOError('Invalid image at %s: %s' % (url, e))
        except Exception as e:
            _log.exception('Uncaught exception %s' % e)
            raise

        raise tornado.gen.Return(image)

    @classmethod
    def convert_to_rgb(cls, image):
        '''Convert the image to RGB if it is not.'''
        
        if image.mode == "RGBA":
            # Composite the image to a white background
            new_image = Image.new("RGB", image.size, (255,255,255))
            new_image.paste(image, mask=image)
            image = new_image
        elif image.mode != "RGB":
            image = image.convert("RGB")

        return image

    
            
