'''
Image utility methods
'''

import cv2
from PIL import Image
import numpy as np

class ImageUtils(object):

    def __init__(self):
        pass

    @classmethod
    def crop_and_resize(cls,im,im_h,im_w):
        image_size = (im_h,im_w)
        image = np.array(im)

        '''Returns a version of the image resized & cropped to the target size.'''
        scaling = max(float(image_size[0]) / image.shape[0],
                      float(image_size[1]) / image.shape[1])

        newsize = np.round(np.array([image.shape[0], image.shape[1]])*scaling)
        big_image = cv2.resize(image, (int(newsize[1]), int(newsize[0])))

        sr = np.floor((newsize[0] - image_size[0])/2)
        sc = np.floor((newsize[1] - image_size[1])/2)

        im_array = big_image[sr:sr + image_size[0],
                         sc:sc + image_size[1],
                         :]

        return Image.fromarray(im_array)

    @classmethod
    def resize(cls,im,im_h,im_w):
        image_size = (im_w,im_h)
        image = im.resize(image_size,Image.ANTIALIAS)
        return image
