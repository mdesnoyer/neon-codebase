#!/usr/bin/env python
'''Quick script that calculates the distance in GIST space between two images.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
USAGE='%prog [options] <imageA> <imageB>'

import cv2
from . import features
import numpy as np
from optparse import OptionParser
import scipy.spatial.distance

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)
    
    options, args = parser.parse_args()

    gist = features.GistGenerator()

    featuresA = gist.transform(cv2.imread(args[0]))
    featuresB = gist.transform(cv2.imread(args[1]))

    dist = scipy.spatial.distance.cdist([featuresA], [featuresB])

    print 'Euclidian squared distance is: %f' % dist
    
