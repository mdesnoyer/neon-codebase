'''
Image utility methods for OpenCV images.

OpenCV image are stored in BGR format.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''

import cv2
import logging
import numpy as np
from cvutils import imageutils
from cvutils import smartcrop

_log = logging.getLogger(__name__)

def resize_and_crop(image, h, w, interpolation=cv2.INTER_AREA):
    '''Resizes the image and then crops to a new size.

    The resize preserves the aspect ratio and then the crop forces the
    image into the desired size.

    Inputs:
    image - Image to resize. Image in OpenCV format
    h - desired height
    w - desired width

    Returns: The resized and cropped image.
    '''
    sc = smartcrop.SmartCrop.get_cropper()
    cropped_im = sc.crop_and_resize(image, w, h)
    return cropped_im

def to_pil(im):
    '''Converts an OpenCV image to a PIL image.'''
    return imageutils.PILImageUtils.from_cv(im)

def from_pil(im):
    '''Converts an PIL image to an OpenCV BGR format.'''
    return imageutils.PILImageUtils.to_cv(im)

def seek_video(video, frame_no, do_log=True, cur_frame=None):
    '''Seeks an OpenCV video to a given frame number.

    After calling this function, the next read() will give you that frame.

    This is necessary because the normal way of seeking in OpenCV
    (setting the CV_CAP_PROP_POS_FRAMES doesn't always work. It might
    only go to the previous keyframe, or it might not be possible to
    get the current frame number).

    Inputs:
    video - An opencv VideoCapture object
    frame_no - The frame number to seek to
    do_log - True if logging should happen on errors
    cur_frame - If you know the frame number that the video should be at, 
                put it here. It helps to identify error cases.

    Outputs:
    Returns (sucess, cur_frame)
    '''

    grab_sucess = True
    if (cur_frame is not None and cur_frame > 0 and 
        video.get(cv2.CAP_PROP_POS_FRAMES) == 0):
        if do_log:
            _log.warn('Cannot read the current frame location.'
                      'Resorting to manual advancing')

        while grab_sucess and cur_frame < frame_no:
            grab_sucess = video.grab()
            cur_frame += 1

    else:
        if (cur_frame is None or not (
                (frame_no - cur_frame) < 4 and (frame_no - cur_frame) >= 0) ):
            # Seeking to a place in the video that's a ways away, so JUMP
            video.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
            
        cur_frame = video.get(cv2.CAP_PROP_POS_FRAMES)
        while grab_sucess and cur_frame < frame_no:
            grab_sucess = video.grab()
            cur_frame = video.get(cv2.CAP_PROP_POS_FRAMES)
            if cur_frame == 0:
                _log.error('Cannot read the current frame location. '
                           'This probably means that we cannot walk '
                           'through the video properly, so we are '
                           'stopping.')
                return False, None

    return grab_sucess, cur_frame

def _ensure_CV(image):
    '''
    Ensures that the image is an Numpy array
    in OpenCV format.
    '''
    if not type(image).__module__ == np.__name__:
        return imageutils.PILImageUtils.to_cv(im)
    return image

def _get_area(image):
    # computes the area of an image
    return image.shape[0] * image.shape[1]

def _convert_to_gray(image):
    # returns the grayscale version of an image
    if len(image.shape) == 3 and image.shape[2] > 1:
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return image

class ImagePrep(object):
    '''
    Exports a class that preprocesses images
    in a varity of configurable ways. This
    accepts either a PIL or OpenCV image, but always
    returns an OpenCV image.
    '''
    def __init__(self, max_height=None, max_width=None,
                 max_side=None, scale_height=None, 
                 scale_width=None, image_size=None,
                 crop_image_size=None, image_area=None,
                 crop_frac=None, convert_to_gray=False):
        '''
        If any of the inputs are None or False, then
        that input does not trigger any preprocessing.

        Inputs are defined by the actions they trigger. In order
        of application:
            - resize such that height is not more than max_height
            - resize such that width is not more than max_width
            - resize such that no side is more than max_side
            - resize such that height is exactly scale_height
            - resize such that width is exactly scale_width
            - force image size to image_size
            - resize and crop image to crop_image_size
            - resize an image such that its area is image_area
            - center crop image to crop_frac
                - this may either be a float, a 2-element list,
                  or a 4-element list. Which either specify the 
                  overall, top+bottom and left+right crop frac, or 
                  the top / right / bottom / left crop frac.

                  IMPORTANT:
                    If crop_frac is a 4-element list, then it refers to the 
                    percentage from FROM that edge. I.e., 
                    crop_frac = [.6, .7] = retain 60% of the horizontal and 
                        70% of the vertical
                    crop_frac = [.2, .3, .1, .0] = cut 20% off the top, 30%
                        off the right, 10% off the bottom, and 0% off the
                        left.
            - convert image to grayscale.
        '''
        self.max_height = max_height
        self.max_width = max_width
        self.max_side = max_side
        self.scale_height = scale_height
        self.scale_width = scale_width
        self.image_size = image_size
        self.crop_image_size = crop_image_size
        self.image_area = image_area
        self.crop_frac = crop_frac
        self.convert_to_gray = convert_to_gray

    def __call__(self, image):
        image = _ensure_CV(image)
        # import ipdb
        # ipdb.set_trace()
        if self.convert_to_gray:
            image = _convert_to_gray(image)
        if self.max_height != None:
            image = self._resize_to_max(image, 0)
        if self.max_width != None:
            image = self._resize_to_max(image, 1)
        if self.max_side != None:
            image = self._resize_to_max(image, None)
        if self.scale_height != None:
            image = self._resize_side(image, 0)
        if self.scale_width != None:
            image = self._resize_side(image, 1)
        if self.image_size != None:
            image = cv2.resize(image,
                               (self.image_size[1],
                               self.image_size[0]))
        if self.crop_image_size != None:
            #image = self._resize_and_crop(image, self.crop_image_size)
            image = self._resize_and_crop(image)
        if self.image_area != None:
            image = self._resize_to_area(image)
        if self.crop_frac != None:
            image = self._center_crop(image)
        return image

    def _center_crop(self, image):
        '''
        Takes the center self.crop_frac of an image
        and returns it. If this is None, does nothing.

        self.crop_frac may be a list of either 2 or 4
        elements, which either specify the top+bottom and
        left+right crop frac, or the top / right / bottom 
        / left crop frac.

        IMPORTANT: SEE NOTE
        '''
        # import ipdb
        # ipdb.set_trace()
        if self.crop_frac == None:
            return image
        if len(image.shape) == 3:
            x, y, w = image.shape
        else:
            x, y = image.shape
        if type(self.crop_frac) == list:
            '''
            Then they've specified either the
            crop fraction per side (in order
            top-right-bottom-left) or top/bottom
            left/right.
            '''
            if len(self.crop_frac) == 2:
                # top/bottom
                xlim1 = int(x * (1. - self.crop_frac[0])/2)
                xlim2 = xlim1
                # left/right
                ylim1 = int(y * (1. - self.crop_frac[1])/2)
                ylim2 = ylim1
            elif len(self.crop_frac) == 4:
                # top - right - bottom - left
                xlim1 = int(x * self.crop_frac[0])
                ylim2 = int(y * self.crop_frac[1])
                xlim2 = int(x * self.crop_frac[2])
                ylim1 = int(y * self.crop_frac[3])
            else:
                raise ValueError('Improper crop frac specification')
        elif type(self.crop_frac)==float:
            xlim1 = int(x * (1. - self.crop_frac)/2)
            ylim1 = int(y * (1. - self.crop_frac)/2)
            xlim2 = xlim1
            ylim2 = ylim1
        else:
            raise ValueError('Improper crop frac specification')
        if xlim2 != 0:
            timage = image[xlim1:-xlim2]
        else:
            timage = image[xlim1:]
        if ylim2 != 0:
            timage = image[:, ylim1:-ylim2]
        else:
            timage = image[:, ylim1:]
        return timage

    def _resize_to_max(self, image, dim=None):
        '''
        Resizes an image such that the dimension specified by dim
        does not exceed max_size. If dim is None, the constraint
        is applied across all dimensions.
        '''
        if dim == None:
            max_size = self.max_side
        if dim == 0:
            max_size = self.max_height
        if dim == 1:
            max_size = self.max_width
        if dim != None:
            if image.shape[dim] > max_size:
                scaleF = max_size * 1./image.shape[dim]
                x, y = image.shape[:2]
                newsz = (int(x*scaleF), int(y*scaleF))
                return cv2.resize(image, newsz[::-1])
            else:
                return image
        if np.max(image.shape[:2]) > max_size:
            scaleF = max_size * 1./np.max(image.shape[:2])
            x, y = image.shape[:2]
            newsz = (int(x*scaleF), int(y*scaleF))
            return cv2.resize(image, newsz[::-1])
        else:
            return image

    def _resize_side(self, image, dim):
        if dim == 0:
            max_size = self.scale_height
        if dim == 1:
            max_size = self.scale_width
        '''
        Resizes an image side dim to a precise size.
        '''
        scaleF = max_size * 1./image.shape[dim]
        x, y = image.shape[:2]
        newsz = (int(x*scaleF), int(y*scaleF))
        return cv2.resize(image, newsz[::-1])

    def _resize_to_area(self, image):
        '''
        Resizes an image to a given area.
        '''
        carea = _get_area(image)
        sfactor = np.sqrt(self.image_area * 1./carea)
        newx = image.shape[0] * sfactor
        newy = image.shape[1] * sfactor
        newx = int(newx)
        newy = int(newy)
        image = cv2.resize(image, (newy, newx))
        return image

    def _resize_and_crop(self, image):
        h, w = self.crop_image_size
        return resize_and_crop(image, h, w)
