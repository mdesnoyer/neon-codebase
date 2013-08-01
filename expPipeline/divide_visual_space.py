#!/usr/bin/env python
'''This script takes a number of GIST database files and partitions the visual space.

The database files contain list of (url, feature_vector) for
images. These feature vectors are then clustered to find the cluster
centers. Finally, we write out the cluster centers into an output
file. 

The output file is a numpy pickle of:

(example_urls, centers, whitening_vector)

example_urls -> [[example urls for cluster i]]
centers -> numpy array each row is a cluster center
whitening_vector -> numpy vector to divide your data by before calculate the distance to the centers

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import cv2
import logging
import numpy as np
from optparse import OptionParser
import cPickle as pickle
import random
import scipy.cluster.vq
import sys

_log = logging.getLogger(__name__)

def load_features(file_stream):
    features = []
    urls = []
    for line in file_stream:
        try:
            with open(line.strip(), 'rb') as db_file:
                entries = pickle.load(db_file)
                for url, feature in entries:
                    if np.max(np.abs(feature)) > 4.0:
                        _log.error('The GIST features from %s are too large. '
                                   'skipping' % url)
                        continue
                    features.append(feature)
                    urls.append(url)
            _log.info('Loaded %i images' % len(features))
        except IOError as e:
            _log.error('Error loading %s: %s' % (line.strip(), e))
            
    return np.array(features), urls

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('-o', '--output', default='gist.db',
                      help='Output filename')
    parser.add_option('-i', '--input', default=None,
                      help='Input file, database file per line. Otherwise, uses stdin')
    parser.add_option('-k', default=108, type='int',
                      help='Number of clusters to create')
    parser.add_option('--attempts', default=1, type='int',
                      help='Number of attempts for running kmeans')
    parser.add_option('--n_examples', default=20, type='int',
                      help='Number of example urls to output per cluster')

    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    inStream = sys.stdin
    if options.input is not None:
        inStream = open(options.input, 'r')

    features, urls = load_features(inStream)

    _log.info('Whitening data')
    #whitening_vector = np.std(features, axis=0)
    whitening_vector = np.ones(features.shape[1], dtype=np.float32)
    wh_data = features / whitening_vector

    _log.info('Starting to cluster data')
    compactness, best_labels, centers = cv2.kmeans(
        wh_data,
        options.k,
        (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 20, 1e-6),
        options.attempts,
        cv2.KMEANS_PP_CENTERS)
    best_labels = np.squeeze(best_labels)

    _log.info('Clusting of %i clusters done. The compactness is: %f'
              % (centers.shape[0], compactness)) 

    # Build the list of example urls
    example_urls = []
    for i in range(centers.shape[0]):
        cur = [urls[j] for j in range(len(urls)) if best_labels[j] == i]
        
        example_urls.append(random.sample(cur, min(options.n_examples,
                                                   len(cur))))

    _log.info('Writing cluster centers to: %s' % options.output)
    with open(options.output, 'wb') as f:
        pickle.dump((example_urls, centers, whitening_vector), f, 2)
