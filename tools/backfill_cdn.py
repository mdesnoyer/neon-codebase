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

import atexit
from cmsdb import cdnhosting 
from cmsdb import neondata
import multiprocessing
from PIL import Image
from cvutils.imageutils import PILImageUtils
from utils import pycvutils
import signal
from StringIO import StringIO
import utils.http
from cvutils.imageutils import PILImageUtils
import utils.neon 
import utils.ps
import utils.sync

from utils.options import define, options
define('api_key', default=None, help='api key to backfill')
define('integration_id', default='0', help='integration id to processes.')
define('worker_multiplier', default=1.0, 
       help=('Multiplier by the number of cores to figure out how many '
             'workers to use'))
define('overwrite', default=0)

import logging
_log = logging.getLogger(__name__)

def process_thumb(thumb, hoster):
    try:
        # Download the full sized image
        image = None
        for url in thumb.urls:
            try:
                image = PILImageUtils.download_image(url)
                break
            except Exception as e:
                pass

        if image is None:
            _log.error('Could not download image for thumbnail %s from %s'
                       % (thumb.key, thumb.urls))
            return False

        # Now upload to the cdn
        overwrite = options.overwrite > 0
        hoster.upload(image, thumb.key, overwrite=overwrite,
                      servingurl_overwrite=True)

    except Exception as e:
        _log.error('Error uploading %s: %s' % (thumb.key, e))
        return False

    return True

def process_one_video(internal_video_id):
    _log.info('Processing video %s' % internal_video_id)
    vidmeta = neondata.VideoMetadata.get(internal_video_id)
    if vidmeta is None:
        _log.error('Could not find video %s' % internal_video_id)
        return False
    
    # Get cdn metadatalist
    cdn_key = neondata.CDNHostingMetadataList.create_key(
        vidmeta.get_account_id(), vidmeta.integration_id)
    clist = neondata.CDNHostingMetadataList.get(cdn_key)
    
    for cdn_metadata in clist.cdns:
        hoster = cdnhosting.CDNHosting.create(cdn_metadata)

        # Process each thumbnail
        thumbs = neondata.ThumbnailMetadata.get_many(vidmeta.thumbnail_ids)

        for thumb in thumbs:
            result = process_thumb(thumb, hoster)
            if not result:
                return False

    return True

def process_account(api_key, pool):
    account = neondata.NeonUserAccount.get(api_key)

    platform_types = dict((x.get_ovp(), x) for x in 
                          [neondata.NeonPlatform,
                           neondata.BrightcovePlatform,
                           neondata.YoutubePlatform,
                           neondata.OoyalaPlatform])
    integration_id = options.integration_id
    _log.info('Processing integration %s for account %s' %
              (integration_id, api_key))
    plattype = platform_types[account.integrations[options.integration_id]]
    plat = plattype.get(api_key, integration_id)
    
    if plat is None:
        _log.error('Could not get platform %s %s' % 
                   (api_key, integration_id))
        return

    # Submit each video
    results = pool.imap_unordered(process_one_video,
                                  plat.get_internal_video_ids(),
                                  30)

    n_success = 0
    n_failure = 0
    for result in results:
        if result:
            n_success += 1 
        else:
            n_failure += 1

        if (n_success + n_failure) % 10 == 0:
            _log.info('Processed %i sucesses and %i failures' % (
                n_success, n_failure))


def init_worker():
    '''Have the worker ignore SIGINT so that it doesn't get stuck.'''
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def main():
    have_write_permissions = False

    # Check for write permissions
    if not neondata.ThumbnailServingURLs('some_test_thumb').save():
        raise Exeception('Do not have write permission')

    proc_slots = max(multiprocessing.cpu_count() * options.worker_multiplier,
                     1)
    pool = multiprocessing.Pool(int(proc_slots), init_worker,
                                maxtasksperchild=50)

    try:
        process_account(options.api_key, pool)
    finally:
        pool.terminate()
        _log.info('Waiting for workers to finish')
        pool.join()


if __name__ == "__main__":
    utils.neon.InitNeon()

    process_one_video('9xmw08l4ln1rk8uhv3txwbg1_6ad8ed60780cecdc58f845686155fbdc')
    exit(1)

    # Register a function that will shutdown the workers
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    
    main()
