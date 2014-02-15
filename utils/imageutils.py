'''
Image utility methods
'''

from PIL import Image
import numpy as np
import random

class ImageUtils(object):

    def __init__(self):
        pass

    #@classmethod
    #def crop_and_resize(cls, im, im_h, im_w):
    #    ''' crop and resize image '''
    #    image_size = (im_h, im_w)
    #    image = np.array(im)
    #
    #    '''Returns a version of the image resized & cropped to the target size.'''
    #    scaling = max(float(image_size[0]) / image.shape[0],
    #                  float(image_size[1]) / image.shape[1])
    #
    #    newsize = np.round(np.array([image.shape[0], image.shape[1]])*scaling)
    #    big_image = cv2.resize(image, (int(newsize[1]), int(newsize[0])))
    #
    #    sr = np.floor((newsize[0] - image_size[0])/2)
    #    sc = np.floor((newsize[1] - image_size[1])/2)
    #
    #    im_array = big_image[sr:sr + image_size[0],
    #                     sc:sc + image_size[1],
    #                     :]
    #
    #    return Image.fromarray(im_array)

    @classmethod
    def resize(cls, im, im_h=None, im_w=None):
        ''' Resize image '''

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
        pixels = [(0, 0, 0) for _w in range(h*w)] 
        r = random.randrange(0, 255)
        g = random.randrange(0, 255)
        b = random.randrange(0, 255)
        pixels[0] = (r, g, b)
        im = Image.new("RGB",(h, w))
        im.putdata(pixels)
        return im
