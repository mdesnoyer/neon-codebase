#!/usr/bin/env python
'''Plots a scatter plot of filter scores vs. valence score

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import os.path
if __name__ == '__main__':
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                    '..', '..')))

import cv2
import logging
import matplotlib.pyplot as plt
import model
import numpy as np
from optparse import OptionParser
from model.train_model import parse_image_scores
import random
import re

_log = logging.getLogger(__name__)

def get_images(options):
    img_id_regex = re.compile(options.img_id_regex)
    img_files, scores = parse_image_scores(options.scores,
                                           options.image_source,
                                           img_id_regex,
                                           options.image_dir)
    idx = range(len(img_files))
    random.shuffle(idx)
    idx = idx[0:options.n_sample]

    img_files = [img_files[i] for i in idx]
    scores = [scores[i] for i in idx]

    return img_files, scores
    
def main(options):
    filt = eval(options.filter)
    
    image_files, valence = get_images(options)

    filter_scores = [filt.score(cv2.imread(x)) for x in image_files]

    if options.plot_density:
        plt.hexbin(filter_scores, valence, bins='log', cmap=plt.cm.Blues,
                   gridsize=50)
    else:
        plt.scatter(filter_scores, valence)

    plt.xlabel('Filter scores')
    plt.ylabel('Valence')
    plt.show()

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--filter', default=None,
                      help='Python code to execute to create the filter object')
    parser.add_option('--scores', default=None,
                      help='File containing "<filename> <score>" on each line')
    parser.add_option('--image_source', default=None,
                      help='File containing "<img_id> <url>" on each line')
    parser.add_option('--img_id_regex', default='([a-zA-Z0-9_-]+)\.jpg',
                      help='Regex for extracting the image id from a filename')
    parser.add_option('--image_dir', default=None,
                      help='Directory with the images')
    parser.add_option('--n_sample', type='int', default=5000,
                      help='Number of images to sample')
    parser.add_option('--seed', type='int', default=16981,
                      help='Random seed')
    parser.add_option('--plot_density', action='store_true', default=False, 
                      help='Instead of a scatter plot, do a density one')
    
    options, args = parser.parse_args()
    random.seed(options.seed)
    logging.basicConfig(level=logging.INFO)

    main(options)
