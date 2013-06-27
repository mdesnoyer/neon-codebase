import numpy as np
from pylab import *

class BadImageFilter:
    '''Class that is used to filter out bad image frames.

    For now, the only bad image frames are those that are a uniform color.
    '''
    def __init__(self, dthresh=30, frac_pixels=0.95):
        '''Create the filter with some parameters.

        dthresh - Distance (L1) in RGB space for a pixel to be considered the
                  same color
        frac_pixels - Fraction of pixels that must be the same color to reject
        '''
        self.dthresh = dthresh
        self.frac_pixels = frac_pixels

    def should_filter(self, image):
        '''Returns True if we should filter the image.

        image - the image to check
        '''
        n_image = np.array(image, dtype=np.float)

        npixels = float(n_image.shape[0]*n_image.shape[1])

        # Find the average color of the image
        avg_color = np.sum(np.sum(n_image, 1),0) / npixels

        # Calculate the L1 distance of each pixel to the average color
        dist = np.abs(n_image -
                      np.squeeze(np.tile(avg_color, (n_image.shape[0],
                                                     n_image.shape[1],
                                                     1))))
        if len(dist.shape) > 2:
            dist = np.sum(dist, 2)
        else:
            dist *= 3; # There are three colors so the distance needs
                       # to increase

        # Calculate the fraction of pixels more than dthresh from the average
        frac_same = np.count_nonzero(dist < self.dthresh) / npixels

        return frac_same > self.frac_pixels

        
        
