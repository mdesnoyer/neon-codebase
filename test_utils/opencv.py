'''
Mocking tools to mock out OpenCV function

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs
'''
import cv2
import logging
from mock import MagicMock
import numpy as np

_log = logging.getLogger(__name__)

def create_random_image(h, w):
    return np.array(np.random.random_integers(0, 255, (h, w, 3)), np.uint8)

class VideoCaptureMock(object):
    '''Mocks out a cv2.VideoCapture object.

    Will always return a random image.
    '''
    def __init__(self, fps=29.97, frame_count=1024, h=480,
                 w=640):
        self.fps = fps
        self.frame_count = frame_count
        self.h = h
        self.w = w
        self.cur_frame = 0

    def get(self, prop):
        if prop == cv2.CAP_PROP_POS_FRAMES:
            return self.cur_frame
        elif prop == cv2.CAP_PROP_FRAME_COUNT:
            return self.frame_count
        elif prop == cv2.CAP_PROP_FPS:
            return self.fps
        elif prop == cv2.CAP_PROP_FRAME_WIDTH:
            return self.w
        elif prop == cv2.CAP_PROP_FRAME_HEIGHT:
            return self.h
        raise ValueError('Unhandled property %s' % prop)

    def set(self, prop, value):
        if prop == cv2.CAP_PROP_POS_FRAMES:
            # We are seeking
            self.cur_frame = int(value)
        else:
            raise ValueError('Unhandled property %s' % prop)

    def grab(self):
        self.cur_frame += 1
        return True

    def retrieve(self):
        return True, create_random_image(self.h, self.w)

    def read(self):
        self.grab()
        return self.retrieve()

    def release(self):
        pass
        
