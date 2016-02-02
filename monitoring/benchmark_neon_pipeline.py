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
import cmsapiv2.client
from cmsdb import neondata
import datetime
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
import utils.net
import utils.ps
from utils import statemon
import utils.sync

from utils.options import define, options
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
define("cmsapi_user", default=None, help='User to submit jobs with')
define("cmsapi_pass", default=None, help='Password for the cmsapi')
define("result_endpoint",
       default="http://10.0.13.60:9200/result_index/videojob/",
       help=("External endpoint to send the result objects to. It could "
             "be an elasticsearch cluster"))

# counters
statemon.define('total_time_to_isp', float)
statemon.define('time_to_processing', float)
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
statemon.define('result_submission_error', int)

import logging
_log = logging.getLogger(__name__)

class JobError(Exception): 
    def __init__(self, msg=None):
        self.msg = msg
        
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.msg)
    
class SubmissionError(JobError): pass
class RunningTooLongError(JobError): pass
class JobFailed(JobError): pass

class BenchmarkVideoJobResult:
    '''Stores the metrics for running a benchmark video job.

    Used for benchmarking the system. Keyed by job start time.
    '''
    def __init__(self, start_time,
                 error_type=None,
                 error_msg=None,
                 total_time=None,
                 time_to_processing=None,
                 time_to_finished=None,
                 time_to_callback=None,
                 time_to_serving=None
                 ):
        # The start time of the job in iso format
        self.start_time = start_time

        # String specifying the type of error that occurred
        self.error_type = error_type

        # More detailed string describing the error
        self.error_msg = error_msg

        # Time from job submission to images being available in the isp
        self.total_time = total_time

        # Time from submission to when it starts to be processed.
        self.time_to_processing = time_to_processing

        # Time from submission to when it is finished processing.
        self.time_to_finished = time_to_finished

        # Time from submission to when it is flagged as serving in the db.
        self.time_to_serving = time_to_serving

        # Time from submission to when the callback is received
        self.time_to_callback = time_to_callback

    @tornado.gen.coroutine
    def send(self):
        request = tornado.httpclient.HTTPRequest(
            options.result_endpoint,
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps(self.__dict__))

        response = yield utils.http.send_request(request, async=True)

        if response.error:
            _log.error('Error submitting job information: %s' %
                       response.error)
            statemon.state.increment('result_submission_error')

class JobManager(object):
    def __init__(self, cb_collector):
        self.cb_collector = cb_collector
        self.video_id = None
        self.job_id = None
        self.start_time = None
        self.result = None
        self.cur_state = None

        self._stopped = False # Has a request been received to stop

    def __del__(self):
        self.cleanup()

    def stop(self):
        self._stopped = True

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def cleanup(self):
        if self.result is not None:
            if not self._stopped:
                # Don't send the result if an external entity stopped
                # the process because it wasn't the system that got
                # stuck part way.
                yield self.result.send()
            self.result = None
        
        if self.job_id is not None:
            np = yield tornado.gen.Task(neondata.NeonPlatform.get,
                                        options.api_key, '0')
            yield np.delete_all_video_related_data(self.video_id,
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
            self.result.error_type = e.__class__.__name__
            self.result.error_msg = e.msg
            pass
        except Exception as e:
            _log.exception('Exception when monitoring')
            statemon.state.unexpected_exception_thrown = 1
        finally:
            yield self.cleanup(async=True)

    @tornado.gen.coroutine
    def _run_test_job(self, video_id=None):
        self.start_time =  time.time()
        self.result = BenchmarkVideoJobResult(
           datetime.datetime.now().isoformat())

        # Create a video request for test account
        yield self.create_neon_api_request(options.account,
                                           options.api_key,
                                           video_id=video_id)

        # Wait for processing
        yield self.wait_for_job_state([
            neondata.RequestState.PROCESSING,
            neondata.RequestState.FINISHED,
            neondata.RequestState.SERVING])
        self.result.time_to_processing = time.time() - self.start_time
        statemon.state.time_to_processing = self.result.time_to_processing

        # Wait for finished
        yield self.wait_for_job_state([
            neondata.RequestState.FINISHED,
            neondata.RequestState.SERVING])
        self.result.time_to_finished = time.time() - self.start_time
        statemon.state.time_to_finished = self.result.time_to_finished

        # Wait for serving and the callback
        yield tornado.gen.multi_future([self.wait_for_callback(),
                                        self.wait_for_job_serving()])
        
        yield self.wait_for_isp_image()

        if self._stopped:
            return


        self.result.total_time = time.time() - self.start_time
        statemon.state.total_time_to_isp = self.result.total_time
        _log.info("total pipeline time %s" % self.result.total_time)

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
        headers = {"X-Neon-API-Key" : api_key,
                   "Content-Type" : "application/json"}
        request_url = '/api/v2/%s/videos' % account_id
        v = int(time.time())
        self.video_id = video_id or ("test%d" % v)
        video_title = "monitoring"
        video_url = "%s?x=%s" % (options.test_video, self.video_id)

        data =     { 
            "external_video_ref": self.video_id,
            "video_url": video_url, 
            "video_title": video_title,
            "callback_url": "http://%s:%s/callback" %(utils.net.get_local_ip(),
                                                      options.callback_port)
        }
        req = tornado.httpclient.HTTPRequest(request_url, method='POST', 
                                             headers=headers,
                                             body=json.dumps(data))
        client = cmsapiv2.client.Client(options.cmsapi_user,
                                        options.cmsapi_pass)
        try:
            res = yield client.send_request(req, ntries=1)
            if res.error:
                _log.error('Error submitting job: %s' % res.error)
                statemon.state.job_submission_error = 1
                raise SubmissionError(str(res.error))
        except tornado.httpclient.HTTPError as e:
            _log.error('Error submitting job: %s' % e)
            statemon.state.job_submission_error = 1
            raise SubmissionError(str(e))
        api_resp = json.loads(res.buffer)
        self.job_id = api_resp['job_id']
        
        statemon.state.increment('jobs_created')
        _log.info('created video request vid %s job %s api %s' % (
            self.video_id, self.job_id, options.api_key))

    @tornado.gen.coroutine
    def wait_for_job_state(self, valid_states=[]):
        '''Poll the api to wait for the request to be one of the valid_states.
        '''

        if self.cur_state and self.cur_state in valid_states:
            return
        
        while not self._stopped:
            request = yield tornado.gen.Task(neondata.NeonApiRequest.get,
                                             self.job_id, options.api_key)
            if request:
                self.cur_state = request.state
                if request.state in valid_states:
                    return
                elif request.state in [
                    neondata.RequestState.FAILED,
                    neondata.RequestState.INT_ERROR,
                    neondata.RequestState.CUSTOMER_ERROR]:
                    statemon.state.job_failed = 1
                    _log.error('Job failed with state %s with response: %s' %
                               (request.state, request.response))
                    raise JobFailed(request.response['error'])
            else:
                _log.warn("request data not found in db")
                statemon.state.request_not_in_db = 1
                # Should we attempt to cleanup in this case ?

            if time.time() > (self.start_time + options.serving_timeout):
                statemon.state.job_not_serving = 1
                _log.error('Job took too long to reach serving state')
                raise RunningTooLongError('Too long to reach serving')
            yield tornado.gen.sleep(1.0)

    @tornado.gen.coroutine
    def wait_for_job_serving(self):
        yield self.wait_for_job_state([
            neondata.RequestState.SERVING])
        self.result.time_to_serving = time.time() - self.start_time
        statemon.state.time_to_serving = self.result.time_to_serving

        _log.info("video is in serving state, took %s" % 
                  self.result.time_to_serving)

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
                    _log.warn('Incorrect callback received: %s' %
                              cb)
                    statemon.state.increment('incorrect_callback')
            elif time.time() > (self.start_time + options.serving_timeout):
                statemon.state.no_callback = 1
                _log.error('Timeout waiting for the callback')
                raise RunningTooLongError('Too long to receive callback')

            if not callback_seen:
                yield tornado.gen.sleep(0.1)

        self.result.time_to_callback = time.time() - self.start_time
        statemon.state.time_to_callback = self.result.time_to_callback
        _log.info('Callback received after %s seconds' % 
                  self.result.time_to_callback)

    @tornado.gen.coroutine
    def wait_for_isp_image(self):
        # Query ISP to get the IMG
        isp_start = time.time()
        isp_ready = False
        vid_obj = yield tornado.gen.Task(
            neondata.VideoMetadata.get,
            neondata.InternalVideoID.generate(options.api_key, self.video_id))
        while not isp_ready and not self._stopped:
            isp_ready = yield vid_obj.image_available_in_isp(async=True)

            if not isp_ready:
                if time.time() > (isp_start + options.isp_timeout):
                    _log.error('Too long for image to appear in ISP')
                    statemon.state.not_available_in_isp = 1
                    raise RunningTooLongError('Too long waiting for ISP')
                yield tornado.gen.sleep(1.0)

        isp_serving = time.time() - isp_start 
        statemon.state.mastermind_to_isp = isp_serving
        _log.info("video is in ISP, took %s s from mastermind to ISP" %
                  isp_serving)

class CallbackCollector(object):
    def __init__(self):
        self._callbacks = {} # Callbacks that have been received

    def add_callback(self, callback_obj):
        self._callbacks[callback_obj['video_id']] = callback_obj

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
            self.set_status(200)
        except Exception as e:
            _log.error('Bad callback received: %s %s' % (e, self.request.body))
            statemon.state.increment('incorrect_callback')
            self.set_status(400)
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
                                                   benchmarker.shutdown))

    benchmarker.start()
    io_loop.start()

if __name__ == "__main__":
    main()
