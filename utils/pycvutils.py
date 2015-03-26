'''
Image utility methods for OpenCV images.

OpenCV image are stored in BGR format.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''

import cv2
import logging
import numpy as np
from . import imageutils

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
    scaling = max(float(h) / image.shape[0],
                  float(w) / image.shape[1])

    newsize = np.round(np.array([image.shape[0], image.shape[1]])*scaling)
    big_image = cv2.resize(image, (int(newsize[1]), int(newsize[0])),
                           interpolation=interpolation)

    sr = np.floor((newsize[0] - h)/2)
    sc = np.floor((newsize[1] - w)/2)

    return big_image[sr:sr + h, sc:sc + w, :]

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
        video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES) == 0):
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
            video.set(cv2.cv.CV_CAP_PROP_POS_FRAMES, frame_no)
            
        cur_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
        while grab_sucess and cur_frame < frame_no:
            grab_sucess = video.grab()
            cur_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
            if cur_frame == 0:
                _log.error('Cannot read the current frame location. '
                           'This probably means that we cannot walk '
                           'through the video properly, so we are '
                           'stopping.')
                return False, None

    return grab_sucess, cur_frame
