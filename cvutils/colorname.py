import os.path
import sys
import numpy as np
from scipy.stats import entropy
from numpy.linalg import norm

__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

w2c_data = np.load(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                'data/w2c.dat')))
w2c_data = w2c_data[0:, 3:]
w2c_max = np.argmax(w2c_data, axis=1)

import cv2

# Jensen-Shannon divergence
# https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence
def JSD(P, Q):
    epsilon = 2e-10
    P = np.array(P, dtype=float)
    Q = np.array(Q, dtype=float)
    new_P = P[P + Q > 0]
    new_Q = Q[P + Q > 0]
    if len(new_P) == 0 or len(new_Q) == 0:
        return 0
    new_P += epsilon
    new_Q += epsilon
    _P = new_P / norm(new_P, ord=1)
    _Q = new_Q / norm(new_Q, ord=1)
    _M = 0.5 * (_P + _Q)
    return 0.5 * (entropy(_P, _M) + entropy(_Q, _M))

class ColorName(object):
    '''For a given image, returns the colorname histogram.'''    

    def __init__(self, image):
    	self.image = image
        self._hist = self.get_colorname_histogram()

    def _image_to_colorname(self):
        BB = self.image[0:, 0:, 0].astype(int) / 8
        GG = self.image[0:, 0:, 1].astype(int) / 8
        RR = self.image[0:, 0:, 2].astype(int) / 8
        index_im = RR + 32 * GG + 32 * 32 * BB 
        colorname_image = np.array(w2c_max[index_im])
        # colorname_image = w2c_max[index_im]
        return colorname_image

    def get_colorname_histogram(self):
        colorname_image = self._image_to_colorname()
        hist_result = np.histogram(colorname_image, bins=11)[0]
        normalized_hist = hist_result.astype(float)/sum(hist_result)
        return normalized_hist

    @classmethod
    def get_distance(cls, image_1, image_2):
        cn_1 = cls(image_1)
        cn_2 = cls(image_2)
        hist_1 = cn_1.get_colorname_histogram()
        hist_2 = cn_2.get_colorname_histogram()
        return JSD(hist_1, hist_2)

    def dist(self, other):
        '''
        Computes the distance to another ColorName object or
        image sa the Jensen-Shannon divergence.
        '''
        if not hasattr(other, '_hist'):
            if type(other).__module__ == np.__name__:
                # it's an image, create colorname object
                other = ColorName(other)
            else:
                raise ValueError("Object of comparison must "
                                 "be a ColorName object or a "
                                 "image as a numpy array")
        return JSD(self._hist, other._hist)
