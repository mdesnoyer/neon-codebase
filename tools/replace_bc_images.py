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
import utils.http
import utils.neon
from utils.options import define, options

define("api_key", default=None, help="Account api key where the videos are",
       type=str)
define("integration_id", default=None, type=str, 
       help="Integration id of the Brightcove account")

_log = logging.getLogger(__name__)

def is_neon_url(url):
    return (('neon-images.com' in url) or
            ('neon-lab.com' in url) or
            ('amazonaws.com' in url))

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
    cmsapi_integ = integrations.brightcove.CMSAPIIntegration(options.api_key,
                                                             integration)
    handler = integrations.brightcove_pusher.ServingURLHandler()

    for video in acct.iterate_all_videos():
        if video.intergration_id != options.integration_id:
            continue

        bc_vid = neondata.InternalVideoID.to_external(video.key)
        bc_videos = yield cmsapi_integ.lookup_videos([bc_vid])
        thumb_info = cmsapi_integ.get_video_thumbnail_info(bc_videos[0])

        if not is_neon_url(thumb_info['thumb_url']):
            continue

        _log.info('Replacing the images for video %s' % bc_vid)

        tmeta, turls = yield handler._choose_replacement_thumb(
            {'video_id' : bc_vid},
            integration)

        image = yield PILImageUtils.download_image(tmeta.urls[0], async=True)

        # TODO: Upload the images to brightcove

if __name__ == "__main__":
    utils.neon.InitNeon()

    tornado.ioloop.IOLoop.current().run_sync(lambda: main(input_file))
