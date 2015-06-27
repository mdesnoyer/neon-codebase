#!/usr/bin/env python

'''
Script to move images around in the CDNs so that their base url is
the same for a given thumbnail.

This script should not be necessary beyond June 2015

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from boto.s3.connection import S3Connection
import cmsdb.cdnhosting
from cmsdb import neondata
import logging
from PIL import Image
import re
from StringIO import StringIO
import tornado.httpclient
from utils.options import define, options

_log = logging.getLogger(__name__)

def download_image(url):
    req = tornado.httpclient.HTTPRequest(url)
    response = utils.http.send_request(req)
    if not response.error:
        return Image.open(StringIO(response.body))

urlRe = re.compile('(http[s]?://(.+)/(.+))/.+')

def main():
    cdn_metadata = None
    vid_meta = None
    
    for serving_urls in ThumbnailServingURLs.get_all():
        # parse the existing urls
        url_parsing = [urlRe.match(x).groups() for x in serving_urls.size_map.itervalues()]

        # If the base is the same for all, we're done
        if len(set((x[2] for x in url_parsing))) <= 1:
            continue

        # Grab the video, thumbnail and hosting metadata objects for
        # this thumb
        thumb_meta = ThumbnailMetadata.get(serving_urls.get_id())
        if vid_meta is None or vid_meta.key != thumb_meta.video_id):
            vid_meta = VideoMetadata.get(thumb_meta.video_id)
        api_key = thumb_meta.get_account()
        if cdn_metadata is None or cdn_metadata.get_id() != api_key:
            cdn_metadata = neondata.CDNHostingMetadataList.get(
                api_key, vid_meta.integration_id)

        # If the only difference is the hostname and it is a valid cdn
        # prefix, then just change the prefix and we're done.
    
if __name__ == "__main__":
    utils.neon.InitNeon()

    main()
