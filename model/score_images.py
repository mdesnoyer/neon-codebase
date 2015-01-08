#!/usr/bin/env python
'''A script that scores a number of images.

Usage: ./score_images.py <image_list.txt>

Input image list should be one image file per line.

Output is a csv of <image_file>,<score>, sorted by score.

Copyright: 2015 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import logging
import model
import sys
import utils.neon
from utils.options import define, options

define('model_file', default=None, help='File that contains the model')
define('output', default=None, help='File to output to. Defaults to stdout')

_log = logging.getLogger(__name__)

def score_images(image_files, outstream):
    _log.info('Opening model %s' % options.model_file)
    mod = model.load_model(options.model_file)

    results = [] # list of (filename, score)
    for imfile in image_files:
        imfile = imfile.strip()
        if imfile == '':
            continue

        image = cv2.imread(imfile)
        if image is None:
            _log.warn('Could not open %s' % imfile)
            continue
        score, attr = mod.score(image, do_filtering=False)
        results.append((imfile, score))

        if len(results) % 1000 == 0:
            _log.info('Scored %i images' % len(results))

    results = sorted(results, key=lambda x: -x[1])

    outstream.write('\n'.join(['%s,%.5f' % x for x in results]))

if __name__ == '__main__':
    args = utils.neon.InitNeon('%prog [options] <image_list>')

    outstream = sys.stdout
    if options.output is not None:
        _log.info('Writing scores to %s' % options.output)
        outstream = open(options.output, 'w')

    _log.info('Reading image file list from %s' % args[0])
    score_images(open(args[0]), outstream)
