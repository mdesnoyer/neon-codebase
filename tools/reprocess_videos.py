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
import logging
import time
import tornado.httpclient
import utils.http
import utils.neon

from utils.options import define, options
define('api_key', default=None, help='api key of the account to backfill')
define('video_server_ip', default=None, 
       help='IP address of the video server to submit the jobs to')
define('test_video_func', default=None,
       help=('python function that will be run to decide if a video should be '
             'reprocessed. If the function returns true, the video will be '
             'reprocessed. It will be of the form lambda vid, job: '
             '<some expression>. This option replaces <some expression>'))
define('max_submite_rate', default=60.0,
       help='Maximum number of jobs to submit per hour')

_log = logging.getLogger(__name__)

def send_reprocess_request(job):
    _log.info('Reprocessing job %s for account %s' % (job.job_id,
                                                      job.api_key))
    
    request = tornado.httpclient.HTTPRequest(
        'http://%s/reprocess' % options.video_server_ip,
        method='POST',
        body=job.to_json())
    response = utils.http.send_request(request)

    if response.error:
        if response.code == 409:
            # Request is already in the queue
            return True
        _log.error('Error submitting job %s: %s' % (job.key, response.error))
        return False

    time.sleep(3600.0/options.max_submite_rate)
        
    return True

def main():
    if options.test_video_func is None:
        raise ValueError('Need to specify test_video_func')
    exec('test_video_func = lambda vid, job: %s' % options.test_video_func)

    n_reprocessed = 0
    n_failures = 0
    
    account = neondata.NeonUserAccount.get(options.api_key)
    for plat in account.get_platforms():
        _log.info('Processing platform %s which has %i videos' %
                  (plat.get_id(), len(plat.videos)))
        vids_processed = 0

        for external_vid, job_id in plat.videos.iteritems():
            vid_obj = neondata.VideoMetadata.get(
                neondata.InternalVideoID.generate(options.api_key,
                                                  external_vid))
            job_obj = neondata.NeonApiRequest.get(job_id, options.api_key)
            if vid_obj and job_obj and test_video_func(vid_obj, job_obj):
                if not send_reprocess_request(job_obj):
                    n_failures += 1
                n_reprocessed += 1

            vids_processed += 1
            if vids_processed % 50 == 0:
                _log.info('Processed %i of %i videos in this platform' %
                          (vids_processed, len(plat.videos)))

    _log.info('Submitted %i jobs. %i failures' % (n_reprocessed, n_failures))

if __name__ == "__main__":
    utils.neon.InitNeon()

    main()
