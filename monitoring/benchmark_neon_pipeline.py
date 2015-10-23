#!/usr/bin/env python
'''
Monitor the time it take for a video to go through the Neon pipeline
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
from cmsdb import neondata
import json
import re
import redis
import signal
import time
import tornado
import utils.http
import utils.neon
import utils.ps
from utils import statemon
import utils.sync

from utils.options import define, options
define("cmsapi_host", default="services.neon-lab.com", help="cmsapi server", type=str)
define("isp_host", default="i1.neon-images.com", help="host where the isp is")
define("account", default="159", help="account id", type=str)
define("api_key", default="3yd7b8vmrj67b99f7a8o1n30", help="api key", type=str)
define("sleep", default=1800, type=float,
       help="sleep time between inserting new jobs in seconds")
define("serving_timeout", default=2000.0, type=float,
       help="Timeout to get to serving state in seconds")
define("isp_timeout", default=500.0, type=float,
       help='Timeout to see the video being served by isp in seconds')
define("test_video", default="https://neon-test.s3.amazonaws.com/output.mp4",
       help='Video to test with')

# counters
statemon.define('total_time_to_isp', float)
statemon.define('time_to_serving', float)
statemon.define('time_to_finished', float)
statemon.define('mastermind_to_isp', float)
statemon.define('job_submission_error', int)
statemon.define('job_not_serving', int)
statemon.define('job_failed', int)
statemon.define('request_not_in_db', int)
statemon.define('not_available_in_isp', int)
statemon.define('unexpected_exception_thrown', int)
statemon.define('jobs_created', int)

import logging
_log = logging.getLogger(__name__)

class JobError(Exception): pass
class SubmissionError(JobError): pass
class RunningTooLongError(JobError): pass
class JobFailed(JobError): pass


@utils.sync.optional_sync
@tornado.gen.coroutine
def create_neon_api_request(account_id, api_key, video_id=None):
    '''
    create random video processing request to Neon
    '''
    
    video_api_formater = "http://%s/api/v1/accounts/%s/neon_integrations/0/create_thumbnail_api_request"
    headers = {"X-Neon-API-Key" : api_key, "Content-Type" : "application/json"}
    request_url = video_api_formater % (options.cmsapi_host, account_id)
    v = int(time.time())
    video_id = video_id or "test%d" % v
    video_title = "monitoring"
    video_url = "%s?x=%s" % (options.test_video, video_id)

    data =     { 
        "video_id": video_id,
        "video_url": video_url, 
        "video_title": video_title
    }
    req = tornado.httpclient.HTTPRequest(request_url, method='POST', 
                                         headers=headers,
                                         body=json.dumps(data))
    try:
        http_client = tornado.httpclient.AsyncHTTPClient()
        res = yield http_client.fetch(req)
    except tornado.httpclient.HTTPError as e:
        _log.error('Error submitting job: %s' % e)
        statemon.state.job_submission_error = 1
        raise SubmissionError
    api_resp = json.loads(res.buffer)
    raise tornado.gen.Return((video_id, api_resp["job_id"]))

@utils.sync.optional_sync
@tornado.gen.coroutine
def monitor_neon_pipeline(video_id=None):    
    start_request = time.time()

    # Create a video request for test account
    statemon.state.increment('jobs_created')
    video_id, job_id = yield create_neon_api_request(options.account,
                                                     options.api_key,
                                                     video_id=video_id, 
                                                     async=True)

    _log.info('created video request vid %s job %s api %s' % (video_id, job_id, 
                                                              options.api_key))
    try:

        # Poll the API for job request completion
        job_serving = False
        job_finished = False
        while not job_serving:
            request = neondata.NeonApiRequest.get(job_id, options.api_key)
            if request:
                _log.info_n("current request state is %s" % request.state,
                            10)
                if (not job_finished and 
                    request.state == neondata.RequestState.FINISHED):
                    statemon.state.time_to_finished = \
                      time.time() - start_request
                    job_finished = True
                elif request.state == neondata.RequestState.SERVING:
                    job_serving = True
                    break
                elif request.state in [
                    neondata.RequestState.FAILED,
                    neondata.RequestState.INT_ERROR,
                    neondata.RequestState.CUSTOMER_ERROR]:
                    statemon.state.job_failed = 1
                    _log.error('Job failed with response: %s' %
                               request.response)
                    raise JobFailed
            else:
                _log.warn("request data not found in db")
                statemon.state.request_not_in_db = 1
                # Should we attempt to cleanup in this case ?

            if time.time() > (start_request + options.serving_timeout):
                statemon.state.job_not_serving = 1
                _log.error('Job took too long to reach serving state')
                raise RunningTooLongError
            time.sleep(5.0)

        if not job_finished:
            statemon.state.time_to_finished = time.time() - start_request
        # time to video serving
        video_serving = time.time() - start_request
        statemon.state.time_to_serving = video_serving
        _log.info("video is in serving state, took %s" % video_serving)

        # Query ISP to get the IMG
        isp_start = time.time()
        isp_ready = False
        vid_obj = yield tornado.gen.Task(
            neondata.VideoMetadata.get,
            neondata.InternalVideoID.generate(options.api_key, video_id))
        while not isp_ready:
            ready = yield vid_obj.image_available_in_isp(async=True)
            if ready:    
                isp_ready = True
                break

            if time.time() > (isp_start + options.isp_timeout):
                _log.error('Too long for image to appear in ISP')
                statemon.state.not_available_in_isp = 1
                raise RunningTooLongError
            time.sleep(1.0)
        
        isp_serving = time.time() - isp_start 
        statemon.state.mastermind_to_isp = isp_serving
        _log.info("video is in ISP, took %s s from mastermind to ISP" %
                  isp_serving)
        
        # Now you can delete the video from the database; Write an Internal API
        # delete video; request; thumbnails; serving thumbs
        
        total_time = time.time() - start_request
        statemon.state.total_time_to_isp = total_time
        _log.info("total pipeline time %s" % total_time)

        # Clear the error states
        statemon.state.job_submission_error = 0
        statemon.state.job_failed = 0
        statemon.state.request_not_in_db = 0
        statemon.state.job_not_serving = 0
        statemon.state.not_available_in_isp = 0

    finally:
        # cleanup
        np = yield tornado.gen.Task(neondata.NeonPlatform.get,
                                    options.api_key, '0')
        yield np.delete_all_video_related_data(video_id,
                                               really_delete_keys=True,
                                               async=True)

def main():
    utils.neon.InitNeon()

    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    
    while True:
        try:
            yield monitor_neon_pipeline(async=True)
            statemon.state.unexpected_exception_thrown = 0
        except JobError as e:
            # Logging already done
            pass
        except Exception as e:
            _log.exception('Exception when monitoring')
            statemon.state.unexpected_exception_thrown = 1
        time.sleep(options.sleep)

if __name__ == "__main__":
    main()
