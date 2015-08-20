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
define('clear_account', default=0,
       help='If 1, the videos in the account are deleted first')

_log = logging.getLogger(__name__)

API_KEY = 'gvs3vytvg20ozp78rolqmdfa'
INTEGRATION_ID = '71'

@tornado.gen.coroutine
def delete_all_videos():
    _log.warn('All videos are being deleted in the account')
    raw_input('Press ENTER to continue. Ctrl-C to cancel')

    # Get the video keys to delete
    vid_map = {}
    def _delete_vids(plat):
        vid_map.update(plat.videos)
        plat.videos = {}
    plat = neondata.BrightcovePlatform.modify(API_KEY, INTEGRATION_ID,
                                              _delete_vids)

    db_connection = neondata.DBConnection.get(neondata.NeonUserAccount)

    # Get the video and thumbnail keys to delete
    cur_keys = db_connection.fetch_keys_from_db('%s_*' % API_KEY)
    neondata.StoredObject.delete_many(cur_keys)

    # Get the serving url keys to delete
    cur_keys = db_connection.fetch_keys_from_db('thumbnailservingurls_%s_*' %
                                                API_KEY)
    neondata.StoredObject.delete_many(cur_keys)

    # Get the api requests to delete
    cur_keys = db_connection.fetch_keys_from_db('request_%s_*' %
                                                API_KEY)
    neondata.StoredObject.delete_many(cur_keys)

@tornado.gen.coroutine
def main():
    if options.clear_account:
        yield delete_all_videos()
    
    plat = neondata.BrightcovePlatform.get(API_KEY, INTEGRATION_ID)
    integration = BrightcoveIntegration('314', plat)

    bc_api = plat.get_api()

    video_fields = BrightcoveIntegration.get_submit_video_fields()
    video_fields.append('customFields')
    
    cur_page = 0
    n_processed = 0
    n_errors = 0
    while True:

        try:

            videos = yield bc_api.search_videos(
                _all=[('network', 'dsc')],
                exact=True,
                video_fields=video_fields,
                sort_by='CREATION_DATE:DESC',
                page=cur_page,
                async=True)
            cur_page += 1
                    
            if len(videos) == 0:
                break

            videos = [x for x in videos if 
                      'newmediapaid' in x['customFields'] and 
                      x['customFields']['newmediapaid'] 
                      not in plat.videos]

            if len(videos) == 0:
                continue

            _log.info('Found %i videos to submit on this page' % len(videos))

            #futures = [integration.submit_one_video_object(x, grab_new_thumb=False)
            #           for x in videos]
            #
            #results = []
            #try:
            #    results = yield futures
            #except Exception as e:
            #    _log.error('Error submitting video: %s' % e)

            results = yield integration.submit_many_videos(
                videos, grab_new_thumb=False,
                continue_on_error=True)

            n_errors += len([x for x in results
                             if isinstance(x, Exception)])
            n_processed += len(videos)

            job_id = None
            #if len(results) > 0:
            #    job_id = results[0]

            _log.info('Processed %i videos. %i failed. Recent job %s' % 
                      (n_processed, n_errors, job_id))

            time.sleep(3600.0 / options.max_submit_rate * len(videos))

        except Exception as e:
            _log.exception('Unexpected exception. Waiting 5 minutes to try again')
            time.sleep(300.0)
            _log.info('Continuing')

if __name__ == '__main__':
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop.current().run_sync(main)
