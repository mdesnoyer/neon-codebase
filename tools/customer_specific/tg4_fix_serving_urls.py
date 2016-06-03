#!/usr/bin/env python

'''
Reset the serving urls in TG4's Brightcove account

Author: Mark Desnoyer(desnoyer@neon-lab.com)
Copyright 2016
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
from cmsdb import neondata
import logging
import tornado.gen
import tornado.ioloop
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def process_one_video(bcapi, video_id):
    _log.info('Processing video: %s' % video_id)
    video = yield neondata.VideoMetadata.get(video_id, async=True)

    new_serving_url = yield video.get_serving_url(async=True)
    new_serving_url += '?height=262&width=480'

    bc_vid = neondata.InternalVideoID.to_external(video_id)
    video_data = yield bcapi.get_video_images(bc_vid)

    poster = video_data['poster']
    if 'neon-images.com' in poster['src']:
        yield bcapi.update_poster(bc_vid, poster['asset_id'],
                                  new_serving_url)

@tornado.gen.coroutine
def main():
    acct = yield neondata.NeonUserAccount.get('4hgq4m5npm304ndewfejkk1w',
                                              async=True)
    video_ids = yield acct.get_internal_video_ids(async=True)

    integ = yield neondata.BrightcoveIntegration.get(
        'fef5ef35a5dd473f8f80b735f5fa49f4', async=True)

    bcapi = api.brightcove_api.CMSAPI(integ.publisher_id,
                                      integ.application_client_id,
                                      integ.application_client_secret)

    for vid in video_ids:
        try:
            yield process_one_video(bcapi, vid)
        except Exception as e:
            _log.error('Error processing video %s: %s' % (vid, e))

if __name__ == "__main__":
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop().run_sync(main)
