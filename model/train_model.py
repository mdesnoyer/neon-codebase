#!/usr/bin/python
'''Trains a model using some data.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import csv
import cv2
import logging
import model
from optparse import OptionParser
import random
import re

_log = logging.getLogger(__name__)

def parse_image_scores(score_file, source_file, img_id_regex, image_dir):
    '''Parses the image scores.

    Returns ([image_files], [scores])

    '''

    # Create a id->source lookup table
    source_lut = {}
    with open(source_file) as f:
        for fields in csv.reader(f, delimiter=' '):
            if fields[0] == '':
                continue
            source_lut[fields[0]] = fields[1]


    image_files = []
    scores = []
    with open(score_file) as f:
        for fields in csv.reader(f, delimiter=','):
            if fields[0] == '':
                continue
            image_files.append(os.path.join(image_dir, fields[0]))
            scores.append(float(fields[1]))

    # Sort based on the source
    def _get_source(image_file, source_lut, img_id_regex):
        img_id = img_id_regex.search(image_file)
        if img_id:
            try:
                return source_lut[img_id.groups()[0]]
            except KeyError:
                pass
            
        return ''

    retval = sorted(zip(image_files, scores),
                    key = lambda x: _get_source(x[0], source_lut,
                                                img_id_regex))

    return zip(*retval)

def train_model(model, img_files, scores):
    '''Trains a model.

    Inputs:
    model - The model to train
    img_files - List of image filenames to train on
    scores - Scores for each of those filenames

    Returns:
    the trained model
    '''
    model.reset()

    _log.info('Starting to load images')
    for img_file, score in zip(img_files, scores):
        cur_image = cv2.imread(img_file)
        if cur_image is None:
            _log.error('Could not find image %s. skipping.' % img_file)
            continue
        model.predictor.add_image(cur_image, score,
                                  os.path.basename(img_file))

    _log.info('Starting to train')
    model.predictor.train()

    return model

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--input', '-i', default=None,
                      help='File containing the model to train.')
    parser.add_option('--output', '-o', default=None,
                      help='File that will contain the trained model.')
    parser.add_option('--scores', default=None,
                      help='File containing "<filename> <score>" on each line')
    parser.add_option('--image_source', default=None,
                      help='File containing "<img_id> <url>" on each line')
    parser.add_option('--img_id_regex', default='([a-zA-Z0-9_-]+)\.jpg',
                      help='Regex for extracting the image id from a filename')
    parser.add_option('--image_dir', default=None,
                      help='Directory with the images')
    parser.add_option('--seed', type='int', default=16984,
                      help='Random seed')
    
    options, args = parser.parse_args()
    random.seed(options.seed)

    logging.basicConfig(level=logging.INFO)

    _log.info('Opening model %s' % options.input)
    mod = model.load_model(options.input)

    img_files, scores = parse_image_scores(options.scores,
                                           options.image_source,
                                           re.compile(options.img_id_regex),
                                           options.image_dir)

    mod = train_model(mod, img_files, scores)

    _log.info('Saving model to %s' % options.output)
    model.save_model(mod, options.output)
    
