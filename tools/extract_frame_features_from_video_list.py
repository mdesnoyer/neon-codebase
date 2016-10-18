#!/usr/bin/env python
'''
Script that extracts the features for frames from videos in a list

Outputs a number of pandas pickle where each column is a (url, frame
number) and each row is a feature vector value. Files are stored in a
S3 location.

This script can be run simultaneously from multiple machines with
minimal extra work being done.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs Inc.
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.s3
import concurrent.futures
import hashlib
import logging
import multiprocessing
import random
import re
import tools.extract_frame_features_from_video
import tempfile
import utils.sync
from utils.options import options, define

_log = logging.getLogger(__name__)
define('input', default=None,
       help='List of URLs for videos to analyze, one per line.')
define('output', default='s3://neon-test/fbvideos',
       help='S3 location to store the output files')

def urlhash(url):
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()

def process_video(video_url):
    '''Process a single video url.'''
    _log.info('Processing %s' % video_url)
    s3conn = boto.s3.connect_to_region('us-east-1')
    s3re = re.compile('s3://([a-zA-Z0-9\-]+)/(.*)/?')
    s3match = s3re.match(options.output)
    bucket_name, prefix = s3match.groups()
    if prefix[-1] != '/':
        prefix = prefix + '/'

    # Look for the result already being on S3
    bucket = s3conn.get_bucket(bucket_name)
    key_name = prefix + urlhash(video_url)
    key_exists = bucket.get_key(key_name)
    if key_exists:
        _log.info('Video %s was already processed.' % video_url)
        return
    
    key = bucket.new_key(key_name)

    # Process the actual video
    with tempfile.NamedTemporaryFile() as tfile:
        tools.extract_frame_features_from_video.main(video_url, tfile.name)

        key.set_contents_from_filename(tfile.name)
        _log.info('Uploaded results from video %s to s3://%s/%s' % 
                  (video_url, bucket_name, key_name))
                                                            

def main():
    n_workers = multiprocessing.cpu_count()
    
    video_urls = [x.strip() for x in open(options.input) if x]
    random.shuffle(video_urls)

    _log.info('Starting to process %i videos' % len(video_urls))
    
    success_count = 0
    fail_count = 0
    with concurrent.futures.ProcessPoolExecutor(n_workers) as executor:
        for fut in concurrent.futures.as_completed([
                        executor.submit(process_video, url)
                        for url in video_urls]):
            try:
                fut.result()
                success_count += 1
                if success_count % 10 == 0:
                    _log.info('Processed %i successfully, %i failed' %
                              (success_count, fail_count))
            except Exception as e:
                _log.error('Error processing image %s' % e)
                fail_count += 1
                

if __name__ == '__main__':
    utils.neon.InitNeon()
    logging.getLogger('boto').propagate = False
    main()
