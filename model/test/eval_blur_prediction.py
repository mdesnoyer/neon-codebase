#!/usr/bin/python
'''Script that does a rough, manual evaluation of the blur predictor
'''
__test__ = False

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
        
import cv2
import os.path
from optparse import OptionParser
from matplotlib.pyplot import *
import numpy as np
import glob
import model.filters

def get_file_list(d):
    return glob.glob(os.path.join(d, '*.jpg'))

def get_stats(d):
    stats = []
    #filt995 = model.filters.BlurryFilter(80, 0.999)
    filt995 = model.filters.InterlaceFilter()
    filtMax = model.filtersBlurryFilter(80, 1.0)
    for f in get_file_list(d):
        print 'Processing %s' % f
        image = cv2.imread(f)
        
        stats.append((filtMax.score(image),
                      filt995.score(image)))
    return stats

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--pos',
      default='/Users/mdesnoyer/Dropbox/Neon/Research/blur_test_images/testset_blur/',
      help='Directory with the examples of blurred images')
    parser.add_option('--neg',
      default='/Users/mdesnoyer/Dropbox/Neon/Research/blur_test_images/non_blur/',
      help='Directory with the examples of non-blurred images')

    options, args = parser.parse_args()

    pos = get_stats(options.pos)
    
    neg = get_stats(options.neg)

    figure(1)
    scatter([x[0] for x in pos], [x[1] for x in pos],
            marker='x', c='b', hold=True)
    scatter([x[0] for x in neg], [x[1] for x in neg],
            marker='o', c='g', hold=True)
    xlabel('Maximum laplacian')
    ylabel('99.5th percentile laplacian')
    legend(['Blurry', 'Not Blurry'])

    figure(2)
    hist([x[1] for x in pos], normed=1, color='r', hold=True)
    hist([x[1] for x in neg], normed=1, color='b', hold=True, alpha=0.5)
    legend(['Blurry', 'Not Blurry'])
    title('995 Filter')

    figure(3)
    hist([x[0] for x in pos], normed=1, color='r', hold=True)
    hist([x[0] for x in neg], normed=1, color='b', hold=True, alpha=0.5)
    legend(['Blurry', 'Not Blurry'])
    title('Max Filter')
           

    show()
