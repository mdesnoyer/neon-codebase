#!/usr/bin/env python
'''
Script that scores images using the text detector

Takes as input a text file with one filename per line.

Outputs a pandas pickle where each column is a file and each row is a feature vector value.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs Inc.
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import logging
import pandas as pd
import re
import utils.neon
from utils.options import options, define

_log = logging.getLogger(__name__)
define('input', default=None,
       help='Input file with image filenames, one per line')
define('output', default=None,
       help='Output file, which will be a pandas pickle')
define('prefix', default=None, type=str,
       help='Prefix to remove from the filename in the index')

def get_features(filename, text_classifier1, text_classifier2):
    image = cv2.imread(filename)

    boxes, mask = cv2.text.textDetect(
        image,
        text_classifier1,
        text_classifier2,
        16, # thresholdDelta, steps for MSER
        0.00015, # min area, ratio to the total area
        0.003, # max area, ratio to the total area
        0.8, # min probablity for step 1
        True, # bool nonMaxSuppression
        0.5, # min probability differernce
        0.9 # min probability for step 2
        )

    name = filename
    if options.prefix:
        name = re.sub(options.prefix, '', filename)

    return pd.Series(
        {'text_frac': float((mask>0).sum()) / (mask.shape[0]*mask.shape[1]),
         'box_count': len(boxes)},
         name=name)

def image_file_iterator():
    _log.info('Reading image files from %s' % options.input)
    with open(options.input, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                yield line

def main():
    text_classifier1 = os.path.join(os.path.dirname(__file__),
                                    '..', 'cvutils', 'data',
                                    'trained_classifierNM1.xml')
    text_classifier2 = os.path.join(os.path.dirname(__file__),
                                    '..', 'cvutils', 'data',
                                    'trained_classifierNM2.xml')

    success_count = 0
    fail_count = 0
    vecs = []
    for image_file in image_file_iterator():
        try:
            result = get_features(image_file, text_classifier1,
                                  text_classifier2)
            success_count += 1
            vecs.append(result)
            if success_count % 100 == 0:
                _log.info('Processed %i successfully, %i failed' %
                        (success_count, fail_count))
        except Exception as e:
            _log.error('Error processing image %s' % e)
            fail_count += 1

    df = pd.concat(vecs, axis=1)
    df.to_pickle(options.output)
    _log.info('Output file to %s' % options.output)

if __name__ == '__main__':
    utils.neon.InitNeon()
    logging.getLogger('boto').propagate = False
    main()
