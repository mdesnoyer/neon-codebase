'''
Image utility methods for OpenCV images.

OpenCV image are stored in BGR format.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''

import cv2
import numpy as np
from . import imageutils

def resize_and_crop(image, h, w):
    '''Resizes the image and then crops to a new size.

    The resize preserves the aspect ratio and then the crop forces the
    image into the desired size.

    Inputs:
    image - Image to resize.
    h - desired height
    w - desired width

    Returns: The resized and cropped image.
    '''
    scaling = max(float(h) / image.shape[0],
                  float(w) / image.shape[1])

    newsize = np.round(np.array([image.shape[0], image.shape[1]])*scaling)
    big_image = cv2.resize(image, (int(newsize[1]), int(newsize[0])))

    sr = np.floor((newsize[0] - h)/2)
    sc = np.floor((newsize[1] - w)/2)

    return big_image[sr:sr + h, sc:sc + w, :]

def hsv_image_difference(imA, imB, w_h=0.25, w_s=0.25, w_v=0.5):
    '''Calculates the normalized difference between image A and B.

    The difference is normalized per pixel and ranges from 0 -> sum(w_*)

    Inputs:
    imA, imB - The two images to compare. OpenCV in BGR format.
    w_* - Weights for the difference in (h)ue (s)aturation and (v)alue
    '''
    if imA.shape != imB.shape:
        return float('inf')
    
    hsvA = np.array(cv2.cvtColor(imA, cv2.COLOR_BGR2HSV), np.float)
    hsvB = np.array(cv2.cvtColor(imB, cv2.COLOR_BGR2HSV), np.float) 

    diff = np.abs(hsvA - hsvB)
    if imA.dtype == np.uint8:
        diff = np.array(diff, np.float) / 255.

    return (np.sum(w_h*diff[0] + w_s*diff[1] + w_v*diff[2]) / 
            (imA.shape[0] * imA.shape[1]))
    
    

def to_pil(im):
    '''Converts an OpenCV image to a PIL image.'''
    return imageutils.PILImageUtils.from_cv(im)

def from_pil(im):
    '''Converts an PIL image to an OpenCV BGR format.'''
    return imageutils.PILImageUtils.to_cv(im)
