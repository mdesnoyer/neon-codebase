#!/usr/bin/env python
'''
Generates example urls for each cluster in the image space based on
a list of images. 

Copyright: 2013 Neon Labs
Author: Sunil Mallya (mallaya@neon-lab.com)
        Mark Desnoyer (desnoyer@neon-lab.com)
'''
USAGE = '%prog [options]'

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
import model

import cv2
import cPickle as pickle
import logging
import numpy as np
from optparse import OptionParser
import random
import scipy.spatial.distance
import utils.logs

_log = logging.getLogger(__name__)

def load_gist_generator(cache_dir=None):
    generator = model.GistGenerator()
    if cache_dir is not None:
        generator = model.DiskCachedFeatures(generator, cache_dir)

    return generator

def parse_image_db(imdb_file, image_dir):
    _log.info('Loading the image database file: %s' % imdb_file)
    retval = []
    with open(imdb_file) as f:
        for line in f:
            fields = line.split()
            retval.append((os.path.join(image_dir, '%s.jpg' % fields[0]),
                           fields[1]))

    return retval

def choose_cluster(codebook, image_fn, generator):
    image = cv2.imread(image_fn)
    features = generator.generate(image)

    dists = scipy.spatial.distance.cdist([features], codebook)
    return np.argmin(dists)

def main(options):
    generator = load_gist_generator(options.cache_dir)
    image_db = parse_image_db(options.image_db, options.image_dir)

    with open(options.codebook, 'rb') as f:
        example_urls, codebook, white_vector = pickle.load(f)

    if options.n_examples is not None:
        random.seed(options.seed)
        random.shuffle(image_db)
        image_db = image_db[0:options.n_examples]

    n_processed = 0
    for image_fn, url in image_db:
        cluster = choose_cluster(codebook, image_fn, generator)
        example_urls[cluster].append(url)

        n_processed += 1

        if n_processed % 50 == 0:
            _log.info('Processed %i images' % n_processed)

    with open(options.output, 'wb') as f:
        pickle.dump(example_urls, f, 2)
    

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)
    
    parser.add_option('-i', '--image_db', default=None,
                      help='Image database file')
    parser.add_option('--image_dir', default=None,
                      help='Image directory')
    parser.add_option('--codebook', default=None,
                      help=('File containing the codebook definition. '
                            'Created using the divide_visual_space.py script.'))
    parser.add_option('--seed', type='int', default=1984968,
                      help='Random seed')
    parser.add_option('--n_examples', type='int', default=None,
                      help='Number of examples to add to the url list')
    
    parser.add_option('--output', '-o', default='examples.urls')

    parser.add_option('--cache_dir', default=None,
                      help='Directory for cached feature files.')
    parser.add_option('--log', default=None,
                      help='Log file. If none, dumps to stdout')

    options, args = parser.parse_args()

    if options.log is None:
        utils.logs.StreamLogger(None)
    else:
        utils.logs.FileLogger(None, options.log)

    main(options)
