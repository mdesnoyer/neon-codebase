#!/usr/bin/env python

'''
Convert a given Neon hosted s3 url of an image 
USAGE: ./script --url "http://image.url"
'''

import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)
    
from supportServices.neondata import NeonApiRequest, ThumbnailURLMapper,\
        VideoMetadata, ThumbnailMetadata
import utils
from utils.options import define, options

define("url", default=None, help="URL of the image", type=str)


def main():
    if options.url is None:
        print "Enter a Neon image url"
        sys.exit(0)

    im_url = options.url
    tid = ThumbnailURLMapper.get_id(im_url)
    if not tid:
        print "Image URL not found in DB"

    vid = ThumbnailMetadata.get_video_id(tid)
    if not vid:
        print "ThumbnailID %s not found"%tid

    req = VideoMetadata.get_video_request(vid)
    if not req:
        print "Request not found for video %s"%vid

    print vid, req.video_url

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
