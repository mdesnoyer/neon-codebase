#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import utils.neon
from video_processor.client import VideoClient, VideoProcessor

if __name__ == "__main__":
    args = utils.neon.InitNeon()

    processor = VideoProcessor({
        'video_url': args[0],
        'job_id' : 'job1',
        'api_key' : 'apikey',
        'video_id' : 'vid1',
        }, None, None, None)

    try:
        processor.download_video_file()

    finally:
        print os.stat(processor.tempfile.name).st_size / (1024.*1024)
          
