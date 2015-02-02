#!/usr/bin/env python
'''
Script to reprocess video

Warning: This script will change the request state of the object and
submit it to the video processing server unit

To reprocess multiple videos when you have video ids in a file do 
for vid in `cat videoidsfile`;do ./reprocess_videos.py -c CONF_FILE --video_id\
    $vid; done

'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
from cmsdb import neondata
import urllib2
import utils.neon

from utils.options import define, options
define('host', default='localhost',
       help='Host where the video processing server is')
define('port', default=8081, type=int,
       help='Port where the video processing server is')
define('video_id', default=None, type=str, help='Internal VID to reprocess')

_log = logging.getLogger(__name__)

def main():
    if options.video_id is None:
        _log.error('Video id is None, Please give video to reprocess')
        return

    i_vid = options.video_id.rstrip("\n")
    request = neondata.VideoMetadata.get_video_request(i_vid)
    if request:
        request.state = neondata.RequestState.REPROCESS
        request.save()
        url = 'http://%s:%s/requeue' % (options.host, options.port)
        response = urllib2.urlopen(url, request.to_json())
        if response.code != 200:
            _log.error('Could not requeue %s' % request.__dict__)
    else:
        _log.error("Request for video id not found, or invalid video id")

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
