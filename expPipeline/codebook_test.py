#!/usr/bin/env python
'''Script that validates a codebook.

We make sure that the images specified in the codebook centers actually belong
to those clusters. Also, we can check what cluster an image is in.

'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
import model

import cv2
import logging
import numpy as np
import cPickle as pickle
from PIL import Image
import random
import scipy.spatial.distance
from cStringIO import StringIO
import urllib2
import utils.logs
import utils.neon
from utils.options import options, define

define('input', help='Input codebook file')
define('image', default=None, help='Image to determine what cluster it is in')

_log = logging.getLogger(__name__)

def assign_cluster(codebook, image, white_vector):
    gist = model.GistGenerator()
    features = gist.generate(image)
    dists = scipy.spatial.distance.cdist([features / white_vector], codebook)
    return np.argmin(dists, axis=1)

def get_image(url):
    '''Gets the image from a given url'''
    # First retrieve the image
    url_stream = urllib2.urlopen(url)
    im_stream = StringIO(url_stream.read())
    image = Image.open(im_stream)

    return np.array(image)[:,:,::-1]

if __name__ == '__main__':
    utils.neon.InitNeon()

    # Open the codebook
    with open(options.input, 'rb') as f:
        example_urls, codebook, wh_vec = pickle.load(f)

    if options.image is not None:
        image = cv2.imread(options.image)
        if image is None:
            _log.error('Error opening %s' % options.image)
            exit(1)
        cluster = assign_cluster(codebook, image, wh_vec)
        _log.info('The cluster for %s is: %i' % (options.image, cluster))
        exit(0)

    # Grab some images in the codebook and make sure they are in the
    # right cluster. There may be some minor mistakes because the
    # clustering was approximate
    random.seed(34590)
    errors = 0
    nsamples = 0
    for cluster_id in range(len(example_urls)):
        for url in random.sample(example_urls[cluster_id], 3):
            try:
                sel_cluster = assign_cluster(codebook, get_image(url),
                                             wh_vec)
                if sel_cluster <> cluster_id:
                    _log.warn('For image %s, codebook cluster %i, assigned: %i'
                              % (url, cluster_id, sel_cluster))
                    errors += 1
                nsamples += 1
            except IOError as e:
                _log.error('Error retrieving %s: %s' % (url, e))

    _log.info('There were %i/%i (%f%%) errors.' %
              (errors, nsamples, errors*100.0/nsamples))
