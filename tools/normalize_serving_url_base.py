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

import cmsdb.cdnhosting
from cmsdb import neondata
import logging
from PIL import Image
import random
import re
from StringIO import StringIO
import tornado.httpclient
import urlparse
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

def download_image(url):
    req = tornado.httpclient.HTTPRequest(url)
    response = utils.http.send_request(req)
    if not response.error:
        return Image.open(StringIO(response.body))
    return None

urlRe = re.compile('(http[s]?://.+)/.+')

def main():
    cdn_list = None
    vid_meta = None
    hoster = None
    have_write_permissions = False
    
    for serving_urls in neondata.ThumbnailServingURLs.iterate_all():
        thumb_id = serving_urls.get_id()

        if not have_write_permissions:
            serving_urls.save()
            have_write_permissions = True

        try:
        
            # parse the existing urls
            bases = [urlRe.match(x).group(1)
                           for x in serving_urls.size_map.itervalues()]

            # If the base is the same for all, we're done
            if len(set(bases)) <= 1:
                continue

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
                continue
                

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
                    continue
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
                continue

            # Upload new images
            try:
                hoster.upload(image, thumb_id, overwrite=False)
            except Exception as e:
                _log.error('Error uploading thumbnail %s: %s' %
                           (thumb_id, e))
                continue

            # Grab the new serving urls and delete any images we don't
            # need anymore
            new_serving_urls = neondata.ThumbnailServingURLs.get(thumb_id)
            remove_serving_urls = []
            for size, old_url in serving_urls.size_map.iteritems():
                if list(size) not in cdn_metadata.rendition_sizes:
                    # This rendition isn't valid anymore
                    try:
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
        
    
if __name__ == "__main__":
    utils.neon.InitNeon()

    main()
