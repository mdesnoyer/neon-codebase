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
    integration = BrightcoveIntegration('314', plat)

    bc_api = plat.get_api()

    video_fields = BrightcoveIntegration.get_submit_video_fields()
    video_fields.append('customFields')
    
    cur_page = 0
    n_processed = 0
    n_errors = 0
    while True:

        videos = yield bc_api.search_videos(
            _all=[('network', 'dsc')],
            exact=True,
            video_fields=video_fields,
            sort_by='CREATION_DATE:DESC',
            page=cur_page,
            async=True)
        videos = [x for x in videos if 
                  'newmediapaid' in x['customFields'] and 
                  x['customFields']['newmediapaid'] 
                  not in plat.videos]
                    
        if len(videos) == 0:
            break

        _log.info('Found %i videos to submit on this page' % len(videos))

        results = yield integration.submit_many_videos(
            videos, grab_new_thumbs=False, continue_on_error=True)

        n_errors += len([x for x in results.values() 
                         if isinstance(x, Exception)])
        n_processed += len(videos)

        job_id = None
        if len(results) > 0:
            job_id = results.values()[0]

        _log.info('Processed %i videos. %i failed. Recent job %s' % 
                  (n_processed, n_errors, job_id))

        time.sleep(3600.0 / options.max_submit_rate * len(videos))

        cur_page += 1

if __name__ == '__main__':
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop.current().run_sync(main)
