#!/usr/bin/env python

'''
Script to backfill images in to the CDN

Use this script when a new image rendition is added for a customer or
the cdn parameters change.

'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import cdnhosting 
from cmsdb import neondata
from PIL import Image
from utils.imageutils import PILImageUtils
from utils import pycvutils
from StringIO import StringIO
import tornado.httpclient
import utils.neon 
import utils.http

from utils.options import define, options
define('api_key', default=None, help='api key to backfill')
define('integration_id', default='0', help='integration id')

import logging
_log = logging.getLogger(__name__)

def download_image(url):
    req = tornado.httpclient.HTTPRequest(url)
    response = utils.http.send_request(req)
    if not response.error:
        return Image.open(StringIO(response.body))

def backfill(api_key, i_id):
    ba = neondata.BrightcovePlatform.get(api_key, i_id)
    vids = ba.get_videos()
    
    # Get cdn metadatalist
    cdn_key = neondata.CDNHostingMetadataList.create_key(api_key, i_id)
    clist = neondata.CDNHostingMetadataList.get(cdn_key)
    for cdn_metadata in clist.cdns:
        hoster = cdnhosting.CDNHosting.create(cdn_metadata)

        expected_sizes = cdn_metadata.rendition_sizes
        n_processed = 0
        for vid in vids:
            if n_processed % 50 == 0:
                _log.info('Processing %i of %i videos' % (n_processed,
                                                          len(vids)))
            n_processed += 1
            
            i_vid = neondata.InternalVideoID.generate(api_key, vid)
            vm = neondata.VideoMetadata.get(i_vid)
            if vm:
                for tid in vm.thumbnail_ids:
                    # Get thumbnail serving urls
                    s_urls = neondata.ThumbnailServingURLs.get(tid)
                    missing_sizes = []
                    new_serving_thumbs = []
                    for esz in expected_sizes:
                        try:
                            c_url = s_urls.get_serving_url(esz[0], esz[1])
                        except KeyError:
                            missing_sizes.append(esz)
                   
                    image = None
                    if len(missing_sizes) >0 :
                        # get original thumbnail url
                        td = neondata.ThumbnailMetadata.get(tid)
                        im_url = td.urls[0]
                        image = download_image(im_url)
                        if not image:
                            _log.info("failed to download image %s" % im_url)
                            continue
                    
                    # Missing the URL, hence create image of that size 
                    for sz in missing_sizes:
                        cv_im = pycvutils.from_pil(image)
                        cv_im_r = pycvutils.resize_and_crop(cv_im, sz[1],
                                                            sz[0])
                        im = pycvutils.to_pil(cv_im_r)
                        cdn_url = hoster._upload_impl(im, tid)
                        if cdn_url:
                            new_serving_thumbs.append((cdn_url, sz[0], sz[1]))

                    if (hoster.update_serving_urls and 
                        len(new_serving_thumbs)>0):
                        def add_serving_urls(obj):
                            for params in new_serving_thumbs:
                                obj.add_serving_url(*params)

                        neondata.ThumbnailServingURLs.modify(
                            tid,
                            add_serving_urls)

if __name__ == "__main__":
    utils.neon.InitNeon()

    if options.api_key:
        backfill(options.api_key, options.integration_id)
