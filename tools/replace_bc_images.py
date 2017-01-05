#!/usr/bin/env python
'''
Script that replaces all images in a Brightcove account so that they
are served by Brightcove. 

Takes the best performing image or the highest ranked Neon one.

Authors: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from api import brightcove_api
from cmsdb import neondata
from utils.imageutils import PILImageUtils
import integrations.brightcove
import integrations.brightcove_pusher
import logging
from PIL import Image
import tornado.gen
import urlparse
import utils.http
import utils.neon
from utils.options import define, options

define("api_key", default=None, help="Account api key where the videos are",
       type=str)
define("integration_id", default=None, type=str, 
       help="Integration id of the Brightcove account")

_log = logging.getLogger(__name__)

def is_neon_url(image_info):
    if image_info is None:
        return False
    if not image_info['remote']:
        return False
    url = image_info.get('src', '')
    return (('neon-images.com' in url) or
            ('neon-lab.com' in url) or
            ('amazonaws.com' in url))

@tornado.gen.coroutine
def replace_one_image(ingestapi, cmsapi, bc_vid,
                      image_info, image_type, thumb_meta,
                      thumb_urls):
    cur_image = image_info.get(image_type, {})
    #if not is_neon_url(cur_image):
    #    return
    #width, height = yield find_image_size(cur_image)
    if image_type == 'poster':
        width, height = (960,540)
    elif image_type == 'thumbnail':
        width, height = (640, 360)
    if width is None or height is None:
        _log.warn('Could not determine size for video %s from url %s' %
                  (thumb_meta.video_id, cur_image.get('src')))
        return

    try:
        src_url = thumb_urls.get_serving_url(width, height)
    except KeyError as e:
        _log.error('Could not find an image of size (%i,%i) for thumb %s' %
                   (width, height, thumb_meta.key))
        return

    kwargs = {
        '%s_url' % image_type : src_url,
        '%s_size' % image_type : (width, height)
        }

    yield ingestapi.ingest_image(bc_vid, **kwargs)

    # Wait until we see the change
    while cur_image.get('remote'):
        info = yield cmsapi.get_video_images(bc_vid)
        cur_image = info.get(image_type, {})
        if not cur_image.get('remote'):
            return
        yield tornado.gen.sleep(0.5)

@tornado.gen.coroutine
def choose_replacement_thumb(video):
    '''Get the replace thumb to put back in Brightcove

    Returns (ThumbnailMetadata, ThumbnailServingURLs)
    '''
    # See if there is a winner thumbnail
    vstatus = yield neondata.VideoStatus.get(video.key, async=True)
    best_tid = None
    if vstatus.winner_tid is None:
        # Get the thumbnail with the best CTR so far
        tstatus = yield neondata.ThumbnailStatus.get_many(
            video.thumbnail_ids, async=True)
        tstatus = [x for x in tstatus if x.imp > 0]
        tstatus = sorted(tstatus, key=lambda x:x.ctr, reverse=True)
        if len(tstatus) > 0:
            best_tid = tstatus[0].get_id()
    else:
        best_tid = vstatus.winner_tid

    best_thumb = None
    if best_tid is not None:
        best_thumb = yield ThumbnailMetadata.get(best_tid, async=True)
    else:
        # Take the highest ranked thumbnail
        thumbs = yield neondata.ThumbnailMetadata.get_many(video.thumbnail_ids,
                                                           async=True)
        valid_thumbs = sorted([x for x in thumbs if 
                              x.type == neondata.ThumbnailType.NEON and
                              x.enabled],
                              key=lambda x: x.rank)
        if len(valid_thumbs) == 0:
            valid_thumbs = sorted([x for x in thumbs if 
                                     x.type == neondata.ThumbnailType.DEFAULT],
                                     key=lambda x: x.rank)
            if len(valid_thumbs) == 0:
                valid_thumbs = thumbs
                if len(valid_thumbs) == 0:
                    _log.warn('No thumbnail found for video %s' % video.key)
        best_thumb = valid_thumbs[0]

    turls = yield neondata.ThumbnailServingURLs.get(best_thumb.key, async=True)
    if turls is None:
        raise tornado.web.HTTPError(
            500, reason='No thumbnail serving URLs known')
    raise tornado.gen.Return((best_thumb, turls))

@tornado.gen.coroutine
def find_image_size(image_response):
    '''Returns (width, height) of the image in a Brightcove Response.'''
    width = None
    height = None

    if len(image_response) == 0:
        raise tornado.gen.Return((None, None))

    if image_response['remote']:
        # Try to get the image size from the URL
        query_params = dict(urlparse.parse_qsl(
            urlparse.urlparse(image_response['src']).query))
        try:
            height = int(query_params.get('height') or 
                         query_params.get('h'))
        except TypeError:
            pass
        try:
            width = int(query_params.get('width') or 
                        query_params.get('w'))
        except TypeError:
            pass
    else:
        # The height and width reported by Brightcove is incorrect
        # sometimes, so we are going to ignore it for now and go get the
        # actual image to figure out its size.
        # TODO: renable once Brightcove fixes this bug
        #width = max([x.get('width') for x in image_response['sources']] or
        #            [None])
        #height = max([x.get('height') for x in image_response['sources']]
        #             or [None])
        pass

    if (width is None or height is None):
        width, height = yield get_size_from_existing_image(
            image_response)

    if (height == 1080 and width == 1920) or (height==262 and width==480):
        width, height = 960, 540

    raise tornado.gen.Return((width, height))

@tornado.gen.coroutine
def get_size_from_existing_image(image_response):
    '''Downloads the image in the account in order to get the image size.
    '''
    neonServingRe = re.compile('neon-images.com/v1/client')
    if ('src' not in image_response or 
        image_response['src'] is None or
        neonServingRe.search(image_response['src']) is not None):
        raise tornado.gen.Return((None, None))

    try:
        img = yield PILImageUtils.download_image(image_response['src'],
                                                 async=True)
    except Exception as e:
        _log.warn('Error downloading image %s: %s' %
                   (image_response['src'], e))
        raise tornado.gen.Return((None, None))

    raise tornado.gen.Return(img.size)
    

@tornado.gen.coroutine
def main():
    acct = yield neondata.NeonUserAccount.get(options.api_key, async=True)
    integration = yield neondata.BrightcoveIntegration.get(
        options.integration_id,
        async=True)
    media_api = integration.get_api()
    cmsapi = brightcove_api.CMSAPI(integration.publisher_id,
                                   integration.application_client_id,
                                   integration.application_client_secret)
    ingestapi = brightcove_api.IngestAPI(integration.publisher_id,
                                         integration.application_client_id,
                                         integration.application_client_secret)
    cmsapi_integ = integrations.brightcove.CMSAPIIntegration(options.api_key,
                                                             integration)

    for video in acct.iterate_all_videos():
        if video.integration_id != options.integration_id:
            continue

        bc_vid = neondata.InternalVideoID.to_external(video.key)
        try:
            image_info = yield cmsapi.get_video_images(bc_vid)
        except brightcove_api.BrightcoveApiClientError as e:
            if e.errno == 404:
                _log.info('Video %s no longer exists in Brightcove' % 
                          video.key)
                continue
            raise
        #if not (is_neon_url(image_info.get('thumbnail')) or 
        #        is_neon_url(image_info.get('poster'))):
        #    continue

        tmeta, turls = yield choose_replacement_thumb(video)

        _log.info('Replacing the images for video %s' % bc_vid)

        yield replace_one_image(ingestapi, cmsapi, bc_vid,
                                image_info, 'thumbnail',
                                tmeta, turls)
        yield replace_one_image(ingestapi, cmsapi, bc_vid, 
                                image_info, 'poster',
                                tmeta, turls)

if __name__ == "__main__":
    utils.neon.InitNeon()

    tornado.ioloop.IOLoop.current().run_sync(lambda: main())
