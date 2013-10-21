#!/usr/bin/env python
'''This script scrapes Flikr for images that are returned by quries.

The GIST features of the images are calculated and an output database
is created that contains the GIST descriptor and the url of the
original image.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
USAGE = '%prog [options]'

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
import model

import cPickle as pickle
from flickrapi import FlickrAPI, shorturl
import numpy as np
from optparse import OptionParser
from PIL import Image
from cStringIO import StringIO
import time
import urllib2
import re
import utils.logs
import youtube_video_id_scraper as youtube


API_KEY = '3c8ecd709d9b6b933e679b6aa5128727'
API_SECRET = '80b54fa4f78056fb'

_log = None

def streamurl2imgurl(stream_url):
    '''Converts the connonical url to the one that has the image.'''
    img_url_re = re.compile(r'<link rel="image_src" href="([\S]+)"')
    url_stream = urllib2.urlopen(stream_url)
    small_url = img_url_re.search(url_stream.read()).group(1)

    # Convert to the larger image url
    small_re = re.compile(r'([\S]+)_m\.([\S]+)$')
    search = small_re.match(small_url)
    return '%s.%s' % (search.group(1), search.group(2))

def get_features(url, generator):
    '''Gets the features of an image at a given url.'''
    # First retrieve the image
    url_stream = urllib2.urlopen(url)
    im_stream = StringIO(url_stream.read())
    image = Image.open(im_stream)

    return generator.generate(np.array(image)[:,:,::-1])

def output_results(results, out_file):
    if len(results) > 0:
        _log.info('Writing %i gist features to: %s' %
                  (len(results), out_file))
        with open(out_file, 'wb') as f:
            pickle.dump(results, f, 2)
    

def process_one_query(query, n_images, out_file, generator, qpm=55):
    MAX_PER_QUERY = 500 # Flickr limitation
    flickr = FlickrAPI(API_KEY)

    _log.info('Finding %i images with query: %s' % (n_images, query))
    results = []
    query_results = flickr.walk(
        api_key=API_KEY,
        text=query,
        per_page=min(MAX_PER_QUERY, n_images),
        media='photos')

    try:
        cur_image = 0
        for data in query_results:
            time.sleep(60.0 / qpm)
            url = shorturl.url(data.get('id'))
            try:
                features = get_features(
                    streamurl2imgurl('%s/sizes/z/in/photostream/' % url),
                    generator)
            except IOError as e:
                _log.error('Error getting image %s: %s' % (url, e))
                continue
            results.append((url, features))            

            if cur_image >= n_images:
                break
            cur_image += 1

            if cur_image % 50 == 0:
                _log.info('Processed %i images' % cur_image)

    finally:
        output_results(results, out_file)
        
def process_youtube_query(query, n_images, out_file, generator, qpm=55):
    _log.info('Finding %i youtube thumbnails with query: %s' %
              (n_images, query))

    results = []
    cur_image = 0
    try:
        for video_id in youtube.generate_video_ids(query, n_images, 3600):
            time.sleep(60.0 / qpm)
            url = 'http://i.ytimg.com/vi/%s/hqdefault.jpg' % video_id
            try: 
                features = get_features(url, generator)
            except IOError as e:
                _log.error('Error getting image %s: %s' % (url, e))
                continue
            results.append((url, features))

            cur_image += 1

            if cur_image % 50 == 0:
                _log.info('Processed %i images' % cur_image)
            
    finally:
        output_results(results, out_file)

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)

    parser.add_option('-o', '--output', default='flickrdb_%i.db',
                      help='Output filename format string. Must be able to insert an integer')
    parser.add_option('-i', '--input', default=None,
                      help='Input file, one query per line. Otherwise, uses stdin')
    parser.add_option('-n', type='int', default=25,
                      help='Number of images to retrieve per query')
    parser.add_option('--qpm', type='int', default=55,
                      help='Number of queries per minute to hit Flikr with')
    parser.add_option('--log', default='flickr.log',
                      help='Log file')
    parser.add_option('--start_index', type='int', default=0,
                      help='Query to start with')
    parser.add_option('--youtube', action='store_true', default=False,
                      help='Get youtube thumbnails instead of flickr ones?')

    options, args = parser.parse_args()

    _log = utils.logs.FileLogger(__name__, options.log)

    in_stream = sys.stdin
    if options.input is not None:
        in_stream = open(options.input, 'r')

    generator = model.GistGenerator()

    i = -1
    for line in in_stream:
        query = line.strip()
        i += 1
        if i < options.start_index:
            continue
        try:
            process_func = process_one_query
            if options.youtube:
                process_func = process_youtube_query

            output_fn = options.output % i
            if os.path.exists(output_fn):
                _log.info('File %s already exists. Skipping.' % output_fn)
                continue
                
            process_func(query,
                         options.n,
                         output_fn,
                         generator,
                         options.qpm)
        except Exception as e:
            _log.error('Error getting results for query: %s: %s' % (query,
                                                                    e))
            continue
