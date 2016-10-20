#!/usr/bin/env python

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
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.s3
import caffe
import concurrent.futures
import cv2
import hashlib
import logging
import multiprocessing
import random
import re
import pandas as pd
import scipy.io
import tornado.gen
import tempfile
import utils.neon
import utils.pycvutils
import utils.sync
import utils.video_download
from utils.options import options, define

_log = logging.getLogger(__name__)
define('input', default=None,
       help='List of URLs for videos to analyze, one per line.')
define('output', default='s3://neon-test/fbvideos_memnet',
       help='S3 location to store the output files')
define('model_def', default='/home/ubuntu/caffe/models/memnet/deploy.prototxt',
       help='Model definition file')
define('pretrained_model',
       default='/home/ubuntu/caffe/models/memnet/memnet.caffemodel',
       help='Trained model weights file')
define('mean',
       default='/home/ubuntu/caffe/models/memnet/mean.mat',
       help='The mean image file')
define('gpu', type=int, default=1,
       help='1 if a GPU should be used')
define('image_dims',  default='227,227',
       help='Cannonical image size')
define('frame_step', default=10, 
       help='Number of frames to step between samples')

def urlhash(url):
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()

@utils.sync.optional_sync
@tornado.gen.coroutine
def download_video(video_url):
    vid_downloader = utils.video_download.VideoDownloader(video_url)
    yield vid_downloader.download_video_file()
    raise tornado.gen.Return(vid_downloader)

def process_video(video_url, predictor):
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
        vid_downloader = download_video(video_url)
        vid = cv2.VideoCapture(vid_downloader.get_local_filename())

        framebuf = []
        data = []
        framenos = []
        frameno = 0
        try:
            for frame in utils.pycvutils.iterate_video(
                    vid, step=options.frame_step):
                framebuf.append(frame)
                framenos.append(frameno)
                frameno += options.frame_step
                if len(framebuf) >= 10:
                    scores = predictor.predict(framebuf, False)
                    data.append(pd.Series(dict(zip(framenos, scores[:,0])),
                                          name=video_url))

                    framebuf = []
                    framenos = []

            if len(framebuf) > 0:
                scores = predictor.predict(framebuf, False)
                data.append(pd.Series(dict(zip(framenos, scores[:,0])),
                                      name=video_url))
        finally:
            vid.release()
            vid_downloader.close()
        data = pd.concat(data, axis=0)
        data.to_pickle(tfile.name)        

        key.set_contents_from_filename(tfile.name)
        _log.info('Uploaded results from video %s to s3://%s/%s' % 
                  (video_url, bucket_name, key_name))

def main():
    n_workers = 2

    if options.gpu:
        caffe.set_mode_gpu()
        print("GPU mode")
    else:
        caffe.set_mode_cpu()
        print("CPU mode")

    mean = scipy.io.loadmat(options.mean)
    mean = mean['image_mean'][:,:,::-1]

    # Load up the predictor
    image_dims = [int(s) for s in options.image_dims.split(',')]
    mean = cv2.resize(mean, image_dims)
    mean = mean.transpose((2,0,1))
    predictor = caffe.Classifier(options.model_def, options.pretrained_model,
            image_dims=image_dims, mean=mean, raw_scale=1.0)
    
    video_urls = [x.strip() for x in open(options.input) if x]
    random.shuffle(video_urls)

    _log.info('Starting to process %i videos' % len(video_urls))

    success_count = 0
    fail_count = 0
    process_video(video_urls[0], predictor)
    with concurrent.futures.ThreadPoolExecutor(n_workers) as executor:
        for fut in concurrent.futures.as_completed([
                        executor.submit(process_video, url, predictor)
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
