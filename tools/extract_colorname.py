#!/usr/bin/env python
'''
Script that extracts the color histogram for a bunch of images

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

import concurrent.futures
import cv2
import logging
import model.colorname
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

def get_features(filename):
    image = cv2.imread(filename)
    image = cv2.resize(image, dsize=(0,0), fx=1./8, fy=1./8,
                       interpolation=cv2.INTER_AREA)

    features = model.colorname.ColorName(image).get_colorname_histogram()

    name = filename
    if options.prefix:
        name = re.sub(options.prefix, '', filename)

    return pd.Series(features, name=name)

def image_file_iterator():
    _log.info('Reading image files from %s' % options.input)
    with open(options.input, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                yield line

def main():
    success_count = 0
    fail_count = 0
    vecs = []
    with concurrent.futures.ProcessPoolExecutor(3) as executor:
        for fut in concurrent.futures.as_completed(
                    [executor.submit(get_features, image_file)
                     for image_file in image_file_iterator()]):
            try:
                vecs.append(fut.result())
                success_count += 1
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
