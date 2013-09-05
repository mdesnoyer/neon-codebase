#!/usr/bin/env python
'''This script takes a list of images and stores a database of their GIST

The GIST features of the images are calculated and an output database
is created that contains the GIST descriptor and the url of image.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
if __name__ == '__main__':
    sys.path.insert(0,os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
import model

import cPickle as pickle
import logging
from optparse import OptionParser
from PIL import Image
import numpy as np
from cStringIO import StringIO
import urllib2

_log = logging.getLogger(__name__)

def get_gist_features(url, cache_dir=None):
    generator = model.GistGenerator()
    if cache_dir is not None:
        generator = model.DiskCachedFeatures(generator, cache_dir)

    url_stream = urllib2.urlopen(url)
    im_stream = StringIO(url_stream.read())
    image = np.array(Image.open(im_stream))

    return generator.generate(image[:,:,::-1])

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('-o', '--output', default='gist.db',
                      help='Output filename')
    parser.add_option('-i', '--input', default=None,
                      help='Input file, one url per line. Otherwise, uses stdin')
    parser.add_option('--cache_dir', default=None,
                      help='Directory of the cached GIST features')

    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)


    inStream = sys.stdin
    if options.input is not None:
        inStream = open(options.input, 'r')

    results = []
    i = 0
    for line in inStream:
        try:
            url = line.strip()
            results.append((url, get_gist_features(url)))
        except IOError as e:
            _log.error('Could not get features for %s: %s' %
                       (url, e))

        if i % 100 == 0:
            _log.info('Processed %i images' % i)
        i += 1

    _log.info('Writing database to: %s' % options.output)
    with open(options.output, 'wb') as f:
        pickle.dump(results, f, 2)

    
