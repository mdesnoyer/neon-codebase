#!/usr/bin/env python
'''
Script that generates a map of important regions of an image.

This is done by running a grey box over the image and getting the scores

Outputs a pandas Panel where the outer index (gender, age) and the
index index are the image arrays.

The image is a single channel specifying the importance of that pixel

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs Inc.
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import collections
import concurrent.futures
import cv2
import logging
import hashlib
import math
import model.predictor
import numpy as np
import pandas as pd
import cPickle as pickle
import re
import utils.autoscale
import utils.neon
from utils.options import options, define

_log = logging.getLogger(__name__)
define('input', default=None,
       help='Input image to analyze')
define('output', default=None,
       help='Output file, which will be a pickled dicationary')
define('cache_dir', default=None,
       help='Directory to store cached results')
define('aq_groups', default='AquilaOnDemandTest,AquilaTestSpot',
       help=('Comma separated list of autoscaling groups to talk to for '
             'aquilla'))
define('n_workers', default=4, 
       help='Number of parallel workers to use')
define('box_size', default=32,
       help='Size of the occluding box')
define('box_step', default=8,
       help='Size of the occluding box')

def score_occluded_image(image, x, y, predictor):

    # Put a grey box on the image
    image = image.copy()
    sz = options.box_size
    image[y:(y+sz), x:(x+sz) :] = 128

    # Get the features
    score, features, model_name = predictor.predict(image)

    signatures = model.predictor.DemographicSignatures(model_name)
    scores = signatures.get_scores_for_all_demos(features)

    return x, y, scores.T[0]

def box_location_iterator(image):
    for x in range(0, image.shape[1], options.box_step):
        for y in range(0, image.shape[0], options.box_step):
            yield x, y

def main():
    conn = utils.autoscale.MultipleAutoScaleGroups(options.aq_groups.split(','))
    _log.info('Opening %s' % options.input)
    im = cv2.imread(options.input)
    im = model.predictor._aquila_prep(im)
    predictor = model.predictor.DeepnetPredictor(aquila_connection=conn)

    try:
        predictor.connect()

        res = collections.defaultdict(dict)
        success_count = 0
        fail_count = 0
        with concurrent.futures.ThreadPoolExecutor(options.n_workers) as executor:
            for fut in concurrent.futures.as_completed(
                    [executor.submit(score_occluded_image, im, a, b, predictor)
                     for a, b in box_location_iterator(im)]):
                try:
                    x, y, scores = fut.result()
                    res[int(y/options.box_step)][int(x/options.box_step)] = \
                      scores
                    success_count += 1
                    if success_count % 100 == 0:
                        _log.info('Processed %i successfully, %i failed' %
                                  (success_count, fail_count))
                except Exception as e:
                    _log.error('Error processing image %s' % e)
                    fail_count += 1
    finally:
        predictor.shutdown()

    panel = pd.Panel(res).transpose(1, 0, 2)
    panel.to_pickle(options.output)
    _log.info('Output file to %s' % options.output)

if __name__ == '__main__':
    utils.neon.InitNeon()
    logging.getLogger('boto').propagate = False
    main()
