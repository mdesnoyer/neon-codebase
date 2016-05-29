#!/usr/bin/env python

'''
Script used to reprocess videos from an account.

One of the parameters defines a function that determines if a video
should be redone. It takes two object, the VideoMetadata object and
the NeonApiRequest object. For example, if you want to only do those
videos that have one thumbnail and are currently in the serving state,
you would do

--test_video_func "len(vid.thumbnail_ids) == 1 and job.state == 'serving'"

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import cmsapiv2.client
import dateutil.parser
import logging
import time
import tornado.gen
import tornado.httpclient
import utils.http
import utils.sync
import utils.neon

from utils.options import define, options
define('api_key', default=None, help='api key of the account to backfill')
define('test_video_func', default=None,
       help=('python function that will be run to decide if a video should be '
             'reprocessed. If the function returns true, the video will be '
             'reprocessed. It will be of the form lambda vid, job: '
             '<some expression>. This option replaces <some expression>'))
define('max_submit_rate', default=60.0,
       help='Maximum number of jobs to submit per hour')
define('username', default=None, type=str, help='Username to talk to cmsapiv2')
define('password', default=None, type=str, help='Password to talk to cmsapiv2')

_log = logging.getLogger(__name__)

@utils.sync.optional_sync
@tornado.gen.coroutine
def send_reprocess_request(client, job):
    _log.info('Reprocessing job %s, video %s for account %s' % (
        job.job_id, job.video_id, job.api_key))
    
    request = tornado.httpclient.HTTPRequest(
        '/api/v2/{account_id}/videos?external_video_ref={video_id}&reprocess=1'.format(account_id=job.api_key, video_id=job.video_id),
        method='POST',
        allow_nonstandard_methods=True)
    response = yield client.send_request(request)

    if response.error:
        if response.code == 409:
            # Request is already in the queue
            raise tornado.gen.Return(True)
        _log.error('Error submitting job %s: %s' % (job.key, response.error))
        raise tornado.gen.Return(False)

    time.sleep(3600.0/options.max_submit_rate)
    raise tornado.gen.Return(True)

def main():
    if options.test_video_func is None:
        raise ValueError('Need to specify test_video_func')
    exec('test_video_func = lambda vid, job: %s' % options.test_video_func)

    n_reprocessed = 0
    n_failures = 0

    if options.username is None or options.password is None:
        raise Exception('Missing username or password')
    client = cmsapiv2.client.Client(options.username, options.password)
    
    account = neondata.NeonUserAccount.get(options.api_key)
    vids_processed = 0
    for vid_obj in account.iterate_all_videos():
        job_obj = neondata.NeonApiRequest.get(vid_obj.job_id, options.api_key)
        if vid_obj and job_obj and test_video_func(vid_obj, job_obj):
            if not send_reprocess_request(client, job_obj):
                n_failures += 1
            n_reprocessed += 1

        vids_processed += 1
        if vids_processed % 50 == 0:
            _log.info('Processed %i videos' %
                      (vids_processed))

    _log.info('Submitted %i jobs. %i failures' % (n_reprocessed, n_failures))

if __name__ == "__main__":
    utils.neon.InitNeon()

    main()
