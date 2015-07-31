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
define('max_submit_rate', default=60.0,
       help='Maximum number of jobs to submit per hour')

_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def main():
    plat = neondata.BrightcovePlatform.get('gvs3vytvg20ozp78rolqmdfa', '71')

    bc_api = plat.get_api()

    video_fields = BrightcoveIntegration.get_submit_video_fields()
    video_fields.append('customFields')

    videos = yield bc_api.search_videos(
        _all=[('network', 'dsc')],
        exact=True,
        video_fields=video_fields,
        sort_by='REFERENCE_ID:ASC',
        async=True)

    videos = [x for x in videos if x['customFields']['newmediapaid'] 
              not in plat.videos]

    _log.info('Found %i videos to submit' % len(videos))

    integration = BrightcoveIntegration('gvs3vytvg20ozp78rolqmdfa', plat)

    n_processed = 0
    for video in videos:
        video['id'] = video['customFields']['newmediapaid']

        yield integration.submit_one_video_object(video)

        n_processed += 1
        if n_processed % 100 == 0:
            _log.info('Processed %i videos' % n_processed)

        time.sleep(3600.0/options.max_submit_rate)

if __name__ == '__main__':
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop.current().run_sync(main)
