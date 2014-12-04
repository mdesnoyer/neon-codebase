#!/usr/bin/python
'''Tools for validating the model.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import copy
import csv
import cv2
import logging
import model
import numpy as np
from optparse import OptionParser
import os.path
import time


_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--model', default=None,
                      help='File containing the model')
    parser.add_option('--image_files', default=None,
                      help='File containing an image file on each line')
    
    options, args = parser.parse_args()

    _log.info('Loading images')
    image_files = [cv2.imread(x.strip()) for x in open(options.image_files)]

    filt = model.load_model(options.model).filt

    start_time = time.time()
    for image in image_files:
        filt.accept(image)
    end_time = time.time()

    _log.info('Average processing time for %s is: %f s' %
              (filt.__class__.__name__,
               (end_time - start_time) / len(image_files)))
