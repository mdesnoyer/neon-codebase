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

import atexit
import cmsdb.cdnhosting
from cmsdb import neondata
import logging
from PIL import Image
import multiprocessing
import random
import re
import signal
from StringIO import StringIO
import tornado.httpclient
import urlparse
import utils.neon
from utils.options import define, options
import utils.ps

_log = logging.getLogger(__name__)

def download_image(url):
    req = tornado.httpclient.HTTPRequest(url)
    response = utils.http.send_request(req)
    if not response.error:
        return Image.open(StringIO(response.body))
    return None

urlRe = re.compile('(http[s]?://.+)/.+')

def process_one_thumb(serving_urls):
    cdn_list = None
    vid_meta = None
    hoster = None
    
    thumb_id = serving_urls.get_id()
    try:

        # parse the existing urls
        bases = [urlRe.match(x).group(1)
                 for x in serving_urls.size_map.itervalues()]

        # If the base is the same for all, we're done
        if len(set(bases)) <= 1:
            return True

        _log.info('Making changes to serving urls for thumb %s' %
                  thumb_id)

        # Check for IGN because in this case, we just need to
        # change the host name to be consistent for every size.
        if thumb_id.startswith('9xmw08l4ln1rk8uhv3txwbg1'):
            prefixes = ['assets.ign.com', 'assets2.ignimgs.com']
            rng = random.Random(thumb_id)
            new_host = rng.choice(prefixes)
            def _change_host(x):
                for size, url in x.size_map.items():
                    new_url = re.sub(
                        '(assets.ign.com)|(assets2.ignimgs.com)',
                        new_host,
                        url)
                    x.add_serving_url(new_url, size[0], size[1])
            neondata.ThumbnailServingURLs.modify(thumb_id, _change_host)
            return True


        # Grab the video, thumbnail and hosting metadata objects for
        # this thumb
        thumb_meta = neondata.ThumbnailMetadata.get(thumb_id)
        if vid_meta is None or vid_meta.key != thumb_meta.video_id:
            vid_meta = neondata.VideoMetadata.get(thumb_meta.video_id)
        api_key = thumb_meta.get_account_id()
        if cdn_list is None or cdn_list.key != api_key:
            cdn_list = neondata.CDNHostingMetadataList.get(
                 neondata.CDNHostingMetadataList.create_key(
                     api_key, vid_meta.integration_id))
            cdns = [x for x in cdn_list.cdns if x.update_serving_urls]
            if len(cdns) != 1:
                _log.error('Cannot find cdnmetadata for thumb %s' %
                           serving_urls.get_id())
                return False
            cdn_metadata = cdns[0]
            hoster = cmsdb.cdnhosting.CDNHosting.create(cdn_metadata)

        # Grab the original image
        image = None
        for im_url in thumb_meta.urls:
            image = download_image(im_url)
            if image is not None:
                break
        if image is None:
            # Grab the image of the same size as the original
            image = download_image(serving_urls.get_serving_url(
                thumb_meta.width, thumb_meta.height))
        if image is None:
            _log.error('Could not get the original image for thumb %s' %
                       thumb_id)
            return False

        # Upload new images
        try:
            hoster.upload(image, thumb_id, overwrite=False)
        except Exception as e:
            _log.error('Error uploading thumbnail %s: %s' %
                       (thumb_id, e))
            return False

        # Grab the new serving urls and delete any images we don't
        # need anymore
        new_serving_urls = neondata.ThumbnailServingURLs.get(thumb_id)
        remove_serving_urls = []
        for size, old_url in serving_urls.size_map.iteritems():
            if list(size) not in cdn_metadata.rendition_sizes:
                # This rendition isn't valid anymore
                try:
                    if (old_url.startswith('http://n3.neon-images.com')
                        and ('n3.neon-images.com' not in
                             cdn_metadata.cdn_prefixes)):
                        tmp_hoster = cmsdb.cdnhosting.CDNHosting.create(
                            neondata.NeonCDNHostingMetadata())
                        tmp_hoster.delete(old_url)
                    else:
                        hoster.delete(old_url)
                    remove_serving_urls.append(size)
                except NotImplementedError as e:
                    pass
                except Exception as e:
                    _log.error('Error deleting %s: %s' % (old_url, e))
                continue

            new_url = new_serving_urls.get_serving_url(*size)
            if (urlparse.urlparse(old_url).path !=
                urlparse.urlparse(new_url).path):
                try:
                    if (old_url.startswith('http://n3.neon-images.com')
                        and ('n3.neon-images.com' not in
                             cdn_metadata.cdn_prefixes)):
                        tmp_hoster = cmsdb.cdnhosting.CDNHosting.create(
                            neondata.NeonCDNHostingMetadata())
                        tmp_hoster.delete(old_url)
                    elif cdn_metadata.cdn_prefixes[0].startswith(
                            'www.gannett-cdn.com'):
                        # We don't have permission to delete on usatoday
                        continue
                    else:
                        hoster.delete(old_url)
                except NotImplementedError as e:
                    pass
                except Exception as e:
                    _log.error('Error deleting %s: %s' % (old_url, e))

        def _delete_old_urls(x):
            for size in remove_serving_urls:
                del x.size_map[size]

        if len(remove_serving_urls) > 0:
            neondata.ThumbnailServingURLs.modify(thumb_id,
                                                 _delete_old_urls)
    except Exception as e:
        _log.exception('Unexpected exception when processing thumb %s' %
                       thumb_id)
        return False

    return True

def main():
    have_write_permissions = False

    # Check for write permissions
    if not neondata.ThumbnailServingURLs('some_test_thumb').save():
        raise Exeception('Do not have write permission')

    proc_slots = max(multiprocessing.cpu_count() * 2, 1)
    #proc_slots = 1
    pool = multiprocessing.Pool(proc_slots, maxtasksperchild=500)

    n_success = 0
    n_failure = 0
    results = pool.imap_unordered(process_one_thumb,
                                  neondata.ThumbnailServingURLs.iterate_all(),
                                  10)
    for success in results:
        if success:
            n_success += 1
        else:
            n_failure += 1
        if (n_success + n_failure) % 1000 == 0:
            _log.info('Processed %i sucesses and %i failures' % (
                n_success, n_failure))

    pool.close()
        
    
if __name__ == "__main__":
    utils.neon.InitNeon()

    # Register a function that will shutdown the workers
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    main()
