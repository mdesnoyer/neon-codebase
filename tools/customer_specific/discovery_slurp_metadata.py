#!/usr/bin/env python
'''
Script that submits a lot of Discovery jobs for backprocessing

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
from cmsdb import neondata
from integrations.brightcove import BrightcoveIntegration
import logging
import time
import tornado.ioloop
import utils.http
import utils.neon

from utils.options import define, options

_log = logging.getLogger(__name__)

API_KEY = 'gvs3vytvg20ozp78rolqmdfa'
INTEGRATION_ID = '71'
GROUP_SIZE = 20

@tornado.gen.coroutine
def update_many_videos(videos):
    # Create dictionary of internal video id -> bc video object
    videos = dict([(neondata.InternalVideoID.generate(
        API_KEY, x['customFields']['newmediapaid']), x) for x in videos])

    def _update_custom_fields(obj_dict):
        for key, obj in obj_dict.iteritems():
            if obj is not None:
                try:
                    obj.custom_data.update(videos[key]['customFields'])
                except KeyError as e:
                    pass

    yield tornado.gen.Task(neondata.VideoMetadata.modify_many,
                           videos.keys(),
                           _update_custom_fields)

@tornado.gen.coroutine
def main():    
    plat = neondata.BrightcovePlatform.get(API_KEY, INTEGRATION_ID)
    integration = BrightcoveIntegration('314', plat)

    bc_api = plat.get_api()

    video_fields = BrightcoveIntegration.get_submit_video_fields()
    video_fields.append('customFields')

    n_processed = 0
    cur_videos = []
    bc_iter = bc_api.search_videos_iter(_all=[('network', 'dsc')],
                                        exact=True,
                                        sort_by='CREATION_DATE:DESC',
                                        video_fields=video_fields)
    for video in bc_iter:
        cur_videos.append(video)
        if len(cur_videos) == GROUP_SIZE:
            try:
                yield update_many_videos(cur_videos)
            except Exception as e:
                _log.exception('Unxpected exception processing videos')
            n_processed += len(cur_videos)
            cur_videos = []

        if n_processed % 100 == 0:
            _log.info('Processed %d videos' % n_processed)

    yield update_many_videos(cur_videos)

if __name__ == '__main__':
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop.current().run_sync(main)
