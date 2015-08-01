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
    integration = BrightcoveIntegration('gvs3vytvg20ozp78rolqmdfa', plat)

    bc_api = plat.get_api()

    video_fields = BrightcoveIntegration.get_submit_video_fields()
    video_fields.append('customFields')
    
    cur_page = 0
    n_processed = 0
    while True:

        videos = yield bc_api.search_videos(
            _all=[('network', 'dsc')],
            exact=True,
            video_fields=video_fields,
            sort_by='REFERENCE_ID:ASC',
            page=cur_page,
            async=True)
        if len(videos) == 0:
            break

        videos = [x for x in videos if 
                  'newmediapaid' in x['customFields'] and 
                  x['customFields']['newmediapaid'] 
                  not in plat.videos]

        _log.info('Found %i videos to submit on this page' % len(videos))

    
        for video in videos:
            video['id'] = video['customFields']['newmediapaid']

            try:
                job_id = yield integration.submit_one_video_object(video)
            except Exception as e:
                _log.exception('Unexpected error submitting video')
                continue

            n_processed += 1
            if n_processed % 100 == 0:
                _log.info('Processed %i videos. Last job %s' % 
                          (n_processed, job_id))

            time.sleep(3600.0/options.max_submit_rate)

        cur_page += 1

if __name__ == '__main__':
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop.current().run_sync(main)
