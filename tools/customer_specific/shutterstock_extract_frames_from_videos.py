#!/usr/bin/env python
'''Script to extract frames from a number of videos using ffmpeg

Uses a json of the form:
{ 'results' : 
  [
    { 
      'video_id' :
      'model' :
      'thumbnails' : 
      [
        { 
          'rank' :
          'score' :
          'url' :
          'frameno' : 
        }
      ]
    }
 ]
}
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.s3
from cmsdb import neondata
import datetime
import dateutil.parser
import json
import logging
import multiprocessing
import os
import re
import subprocess
import tempfile
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

define('input', default=None, help='Input json')
define('temp_dir', default=None)

def process_single_video(video_info):
    _log.info('Processing video %s' % video_info['video_id'])
    s3re = re.compile('http://s3.amazonaws.com/([a-zA-Z0-9\-_\.]+)/(.+/([a-zA-Z0-9]+\.jpg))')
    
    try:
        s3conn = boto.s3.connect_to_region('us-east-1')

        # Get the video
        with tempfile.NamedTemporaryFile('w+b', dir=options.temp_dir) as \
          vid_file:
            bucket = s3conn.get_bucket('neon-shutterstock')
            key = bucket.get_key('videos/%s.mov' % video_info['video_id'])
            key.get_contents_to_file(vid_file)
            vid_file.flush()

            for thumb in video_info['thumbnails']:
                # Extract the thumbnail
                thumb_filename = os.path.join(options.temp_dir,
                                              '%s_%s.jpg' % (
                                                  video_info['video_id'],
                                                  thumb['frameno']))
                proc_args = [
                      'ffmpeg',
                      '-i',
                      vid_file.name,
                      '-vf',
                      '"select=gte(n\,%d)"' % thumb['frameno'],
                      '-vframes',
                      '1',
                      '-q:v',
                      '1',
                      thumb_filename]
                retval = subprocess.call(' '.join(proc_args), shell=True)
                if retval != 0:
                    _log.error('Error Extracting frame %d from video %s' %
                               (thumb['frameno'], video_info['video_id']))
                    return


                # Upload the image to s3
                img_bucket_name, img_key_name, img_fn = \
                s3re.search(thumb['url']).groups()

                img_bucket = s3conn.get_bucket(img_bucket_name)
                img_key = img_bucket.get_key(img_key_name)
                if img_key is None:
                    img_key = img_bucket.new_key(img_key_name)
                else:
                    mod_date = dateutil.parser.parse(img_key.last_modified)
                    if mod_date > dateutil.parser.parse('2015-02-11T12:00:00 GMT'):
                        continue
                img_key.set_contents_from_filename(
                    thumb_filename,
                    replace=True,
                    headers={
                        'Content-Type' : 'image/jpeg'
                        },
                    policy='private')
                os.remove(thumb_filename)
        _log.info('Sucessfully processed video %s' % video_info['video_id'])
    except Exception as e:
        _log.exception('Error processing video %s' % video_info['video_id'])
        

def main():
    # Start some extra subprocesses for processing
    pool = multiprocessing.Pool(processes=(multiprocessing.cpu_count()+16))
    
    # Open the input file
    with open(options.input) as stream:
        json_data = json.load(stream)['results']

    # Start the jobs
    #process_single_video(json_data[0])
    pool.map(process_single_video, json_data)

    

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
