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
import functools
import json
import re
import redis
import signal
import time
import tornado
import tornado.web
import utils.http
import utils.neon
import utils.ps
from utils import statemon
import utils.sync

from utils.options import define, options
define("cmsapi_host", default="services.neon-lab.com", help="cmsapi server", type=str)
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
define("callback_port", default=8080,
       help='Port to listen with the callback receiver on')

# counters
statemon.define('total_time_to_isp', float)
statemon.define('time_to_callback', float)
statemon.define('time_to_serving', float)
statemon.define('time_to_finished', float)
statemon.define('mastermind_to_isp', float)
statemon.define('time_to_callback', float)
statemon.define('job_submission_error', int)
statemon.define('job_not_serving', int)
statemon.define('job_failed', int)
statemon.define('request_not_in_db', int)
statemon.define('not_available_in_isp', int)
statemon.define('unexpected_exception_thrown', int)
statemon.define('jobs_created', int)
statemon.define('incorrect_callback', int)
statemon.define('no_callback', int)

import logging
_log = logging.getLogger(__name__)

class JobError(Exception): pass
class SubmissionError(JobError): pass
class RunningTooLongError(JobError): pass
class JobFailed(JobError): pass


class JobManager(object):
    def __init__(self, cb_collector):
        self.cb_collector = cb_collector
        self.video_id = None
        self.job_id = None
        self.start_time = None

        self._stopped = False # Has a request been received to stop

    def __del__(self):
        self.cleanup()

    def stop(self):
        self._stopped = True

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def cleanup(self):
        if self.job_id is not None:
            np = yield tornado.gen.Task(neondata.NeonPlatform.get,
                                        options.api_key, '0')
            yield np.delete_all_video_related_data(video_id,
                                                   really_delete_keys=True,
                                                   async=True)
            self.job_id = None
            self.video_id = None
            self.start_time = None
            self._stopped = False

    @tornado.gen.coroutine
    def run_test_job(self, video_id=None):
        if self.job_id is not None:
            _log.error('A job is already running')
            return
        try:
            yield self._run_test_job(video_id=video_id)
            statemon.state.unexpected_exception_thrown = 0
        except JobError as e:
            # Logging already done
            pass
        except Exception as e:
            _log.exception('Exception when monitoring')
            statemon.state.unexpected_exception_thrown = 1
        finally:
            yield self.cleanup(async=True)

    @tornado.gen.coroutine
    def _run_test_job(self, video_id=None):
        self.start_time =  time.time()

        # Create a video request for test account
        yield self.create_neon_api_request(options.account,
                                           options.api_key,
                                           video_id=video_id)

        yield [self.wait_for_job_serving(), self.wait_for_callback()]
        
        yield self.wait_for_isp_image()

        if self._stopped:
            return


        total_time = time.time() - self.start_time
        statemon.state.total_time_to_isp = total_time
        _log.info("total pipeline time %s" % total_time)

        # Clear the error states
        statemon.state.job_submission_error = 0
        statemon.state.job_failed = 0
        statemon.state.request_not_in_db = 0
        statemon.state.job_not_serving = 0
        statemon.state.not_available_in_isp = 0
        statemon.state.no_callback = 0

    @tornado.gen.coroutine
    def create_neon_api_request(self, account_id, api_key, video_id=None):
        '''
        create random video processing request to Neon
        '''

        video_api_formater = "http://%s/api/v2/%s/videos"
        headers = {"X-Neon-API-Key" : api_key,
                   "Content-Type" : "application/json"}
        request_url = video_api_formater % (options.cmsapi_host, account_id)
        v = int(time.time())
        self.video_id = video_id or ("test%d" % v)
        video_title = "monitoring"
        video_url = "%s?x=%s" % (options.test_video, self.video_id)

        data =     { 
            "external_video_ref": self.video_id,
            "video_url": video_url, 
            "video_title": video_title,
            "callback_url": "http://%s:%s/callback" % (cur_host, 
                                                       options.callback_port)
        }
        req = tornado.httpclient.HTTPRequest(request_url, method='POST', 
                                             headers=headers,
                                             body=json.dumps(data))
        try:
            res = yield utils.http.send_request(req, ntries=1)
        except tornado.httpclient.HTTPError as e:
            _log.error('Error submitting job: %s' % e)
            statemon.state.job_submission_error = 1
            raise SubmissionError
        api_resp = json.loads(res.buffer)
        self.job_id = api_resp['job_id']
        
        statemon.state.increment('jobs_created')
        _log.info('created video request vid %s job %s api %s' % (
            self.video_id, self.job_id, options.api_key))

    @tornado.gen.coroutine
    def wait_for_job_serving(self):

        # Poll the API for job request completion
        job_serving = False
        job_finished = False
        while not job_serving and not self._stopped:
            request = yield tornado.gen.Task(neondata.NeonApiRequest.get,
                                             job_id, options.api_key)
            if request:
                if (not job_finished and 
                    request.state == neondata.RequestState.FINISHED):
                    statemon.state.time_to_finished = \
                      time.time() - self.start_time
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

            if time.time() > (self.start_time + options.serving_timeout):
                statemon.state.job_not_serving = 1
                _log.error('Job took too long to reach serving state')
                raise RunningTooLongError
            yield tornado.gen.sleep(1.0)

        if not job_finished:
            statemon.state.time_to_finished = time.time() - self.start_time
        # time to video serving
        video_serving = time.time() - self.start_time
        statemon.state.time_to_serving = video_serving
        _log.info("video is in serving state, took %s" % video_serving)

    @tornado.gen.coroutine
    def wait_for_callback(self):
        callback_seen = False

        while not callback_seen and not self._stopped:
            cb = self.cb_collector.get_callback(self.video_id)
            if cb is not None:
                # Verify the callback
                if cb['processing_state'] == neondata.RequestState.SERVING:
                    callback_seen = True
                else:
                    _log.warn('Unexpected callback received: %s' %
                              cb)
                    statemon.state.increment('incorrect_callback')
            elif time.time() > (self.start_time + options.serving_timeout):
                statemon.state.no_callcallback = 1
                _log.error('Timeout waiting for the callback')
                raise RunningTooLongError

            if not callback_seen:
                yield tornado.gen.sleep(0.1)

        callback_time = time.time() - self.start_time
        statemon.state.time_to_callback = callback_time
        _log.info('Callback received after %s seconds' % callback_time)

    @tornado.gen.coroutine
    def wait_for_isp_image(self):
        # Query ISP to get the IMG
        isp_start = time.time()
        isp_ready = False
        vid_obj = yield tornado.gen.Task(
            neondata.VideoMetadata.get,
            neondata.InternalVideoID.generate(options.api_key, video_id))
        while not isp_ready and not self._stopped:
            isp_ready = yield vid_obj.image_available_in_isp(async=True)

            if time.time() > (isp_start + options.isp_timeout):
                _log.error('Too long for image to appear in ISP')
                statemon.state.not_available_in_isp = 1
                raise RunningTooLongError
            if not isp_ready:
                yield tornado.gen.sleep(1.0)

        isp_serving = time.time() - isp_start 
        statemon.state.mastermind_to_isp = isp_serving
        _log.info("video is in ISP, took %s s from mastermind to ISP" %
                  isp_serving)

class CallbackCollector(object):
    def __init__(self):
        self._callbacks = {} # Callbacks that have been received

    def add_callback(self, callback_obj):
        self._callbacks[callback_obj.video_id] = callback_obj

    def get_callback(self, video_id):
        '''Retrieves a callback object that we received.

        Returns None if the callback hasn't been seen yet
        '''
        try:
            obj = self._callbacks[video_id]
            del self._callbacks[video_id]
            return obj
        except KeyError:
            pass
        return None

class CallbackHandler(tornado.web.RequestHandler):
    '''Handler to receive the callbacks with'''
    def initialize(self, collector):
        super(CallbackHandler, self).initialize()
        self.collector = collector

    @tornado.gen.coroutine
    def put(self, *args, **kwargs):
        try:
            self.collector.add_callback(json.loads(self.request.body))
        except Exception as e:
            _log.error('Bad callback received: %s %s' % (e, self.request.body))
            statemon.state.increment('incorrect_callback')
            self.set_status(400)
        self.set_status(200)
        self.finish()

class Benchmarker(object):
    def __init__(self):
        self.cb_collector = CallbackCollector()
        self.job_manager = JobManager(self.cb_collector)
        self.timer = utils.sync.PeriodicCoroutineTimer(
            self.job_manager.run_test_job,
            options.sleep)
        self.application = tornado.web.Application([
            (r'/callback', CallbackHandler,
             dict(collector=self.cb_collector))])
        self.server = tornado.httpserver.HTTPServer(self.application)

    @tornado.gen.coroutine
    def shutdown(self, stop_loop=True):
        '''Cleanly shuts down the benchmarker.'''
        _log.info('Shutting down')
        self.job_manager.stop()
        self.timer.stop()
        yield self.timer.wait_for_running_func()
        if stop_loop:
            tornado.ioloop.IOLoop.current().stop()

    def start(self):
        self.server.listen(options.callback_port)
        self.timer.start()

def main():
    utils.neon.InitNeon()

    io_loop = tornado.ioloop.IOLoop.current()
    benchmarker = Benchmarker()
    
    signal.signal(signal.SIGTERM, lambda sig, y: 
                  io_loop.add_callback_from_signal(io_loop.spawn_callback,
                                                   Benchmarker.shutdown))

    Benchmarker.start()
    io_loop.start()

if __name__ == "__main__":
    main()
