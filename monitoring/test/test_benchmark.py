#!/usr/bin/env python
'''
Test the benchmark code
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import json
from mock import MagicMock, patch 
from monitoring import benchmark_neon_pipeline
from StringIO import StringIO
import test_utils.neontest
import test_utils.redis
import tornado
import tornado.httpclient
import tornado.testing
import unittest
import utils.neon
from utils.options import define, options
from utils import statemon

import logging
_log = logging.getLogger(__name__)

class BenchmarkTest(test_utils.neontest.AsyncHTTPTestCase):
    def setUp(self):
        self.benchmarker = benchmark_neon_pipeline.Benchmarker()
        
        super(BenchmarkTest, self).setUp()

        # Mock out the http calls for creating the job and checking the isp
        self.create_job_patcher = patch('monitoring.benchmark_neon_pipeline.cmsapiv2.client')
        self.create_job_mock = self._future_wrap_mock(
            self.create_job_patcher.start().Client().send_request)
        self.create_job_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, 200, buffer='{"job_id": "myjobid"}')

        # Mock out the isp request
        self.isp_call_mock = MagicMock()
        self.isp_call_mock.side_effect = \
          lambda x: tornado.httpclient.HTTPResponse(
              x, code=200, effective_url="http://www.where.com/neontntid.jpg")

        # Mock out the result submission
        self.result_mock = MagicMock()
        self.result_mock.side_effect = \
          lambda x: tornado.httpclient.HTTPResponse(x, code=200)

        # Mock out the http requests
        self.send_request_patcher = patch('utils.http.send_request')
        self.send_request_mock = self._future_wrap_mock(
            self.send_request_patcher.start(), require_async_kw=True)
        def _handle_http_request(req, **kw):
            if req.url == options.get('monitoring.benchmark_neon_pipeline.result_endpoint'):
                return self.result_mock(req)
            else:
                return self.isp_call_mock(req)
        self.send_request_mock.side_effect = _handle_http_request
        

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.api_key = 'apikey'
        self.old_api_key = options.get(
            'monitoring.benchmark_neon_pipeline.api_key')
        options._set('monitoring.benchmark_neon_pipeline.api_key',
                     self.api_key)
        options._set('monitoring.benchmark_neon_pipeline.serving_timeout', 0)
        options._set('monitoring.benchmark_neon_pipeline.isp_timeout', 0)

        acct = neondata.NeonUserAccount('a1', self.api_key)
        plat = neondata.NeonPlatform.modify(
            self.api_key, '0',
            lambda x: x.add_video('vid1', 'myjobid'),
            create_missing=True)
        acct.add_platform(plat)
        acct.save()

        self.request = neondata.NeonApiRequest('myjobid', self.api_key, 'vid1')
        self.request.save()

        video = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(self.api_key, 'vid1'))
        video.save()
        video.get_serving_url()

    def get_app(self):
        return self.benchmarker.application
        
    def tearDown(self):
        self.benchmarker.job_manager.stop()
        self.benchmarker.timer.stop()
        self.send_request_patcher.stop()
        self.create_job_patcher.stop()
        statemon.state._reset_values()
        options._set('monitoring.benchmark_neon_pipeline.api_key',
                     self.old_api_key)
        self.redis.stop()
        super(BenchmarkTest, self).tearDown()

    def _check_request_cleanup(self):
        '''Checks that the request object is cleaned up in the database.'''
        self.assertIsNone(neondata.NeonApiRequest.get('myjobid', self.api_key))
        self.assertNotIn('vid1', 
                         neondata.NeonPlatform.get(self.api_key, '0').videos)

        
    @tornado.gen.coroutine
    def _send_callback(self, callback_obj):
        response = yield self.http_client.fetch(
            self.get_url('/callback'), method='PUT',
            body=json.dumps(callback_obj))

        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def _get_job_result(self):
        cargs, kwargs = self.result_mock.call_args
        raise tornado.gen.Return(json.loads(cargs[0].body))

    @tornado.testing.gen_test
    def test_video_serving(self):
        self.request.state = neondata.RequestState.SERVING
        self.request.save()
        cb_response = yield self._send_callback({
            'video_id': 'vid1',
            'processing_state': neondata.RequestState.SERVING})
        self.assertEquals(cb_response.code, 200)

        with self.assertLogExists(logging.INFO, 'total pipeline time'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self.create_job_mock.assertCalled()

        self._check_request_cleanup()

        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_finished'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_serving'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.total_time_to_isp'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.mastermind_to_isp'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_callback'),
            0)

        result = yield self._get_job_result()
        self.assertIsNone(result['error_type'])
        self.assertIsNone(result['error_msg'])
        self.assertGreater(result['total_time'], 0)
        self.assertGreater(result['time_to_processing'], 0)
        self.assertGreater(result['time_to_finished'], 0)
        self.assertGreater(result['time_to_serving'], 0)
        self.assertGreater(result['time_to_callback'], 0)

    @tornado.testing.gen_test
    def test_bad_callback_received(self):
        with self.assertRaisesRegexp(tornado.httpclient.HTTPError, 'HTTP 400'):
            yield self._send_callback({
                'thumb_id': 'tid1',
                'processing_state': neondata.RequestState.SERVING})
            
    @tornado.testing.gen_test
    def test_job_still_submitting(self):
        with self.assertLogExists(logging.ERROR,
                                  'Job took too long to reach serving state'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self._check_request_cleanup()

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_not_serving'), 1)

        result = yield self._get_job_result()
        self.assertEquals(result['error_type'], 'RunningTooLongError')
        self.assertRegexpMatches(result['error_msg'], 'Too long to .*')
        self.assertIsNone(result['total_time'])
        self.assertIsNone(result['time_to_processing'])
        self.assertIsNone(result['time_to_finished'])
        self.assertIsNone(result['time_to_serving'])
        self.assertIsNone(result['time_to_callback'])

    @tornado.testing.gen_test
    def test_job_processing(self):
        self.request.state = neondata.RequestState.PROCESSING
        self.request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Job took too long to reach serving state'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self._check_request_cleanup()

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_not_serving'), 1)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_processing'),
            0)

        result = yield self._get_job_result()
        self.assertEquals(result['error_type'], 'RunningTooLongError')
        self.assertRegexpMatches(result['error_msg'], 'Too long to .*')
        self.assertIsNone(result['total_time'])
        self.assertGreater(result['time_to_processing'], 0)
        self.assertIsNone(result['time_to_finished'])
        self.assertIsNone(result['time_to_serving'])
        self.assertIsNone(result['time_to_callback'])

    @tornado.testing.gen_test
    def test_job_finished_but_not_serving(self):
        self.request.state = neondata.RequestState.FINISHED
        self.request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Job took too long to reach serving state'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self._check_request_cleanup()

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_not_serving'), 1)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_finished'),
            0)
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.no_callback'), 1)

        result = yield self._get_job_result()
        self.assertEquals(result['error_type'], 'RunningTooLongError')
        self.assertRegexpMatches(result['error_msg'], 'Too long to .*')
        self.assertIsNone(result['total_time'])
        self.assertGreater(result['time_to_processing'], 0)
        self.assertGreater(result['time_to_finished'], 0)
        self.assertIsNone(result['time_to_serving'])
        self.assertIsNone(result['time_to_callback'])

    @tornado.testing.gen_test
    def test_job_failed(self):
        self.request.state = neondata.RequestState.FAILED
        self.request.response = {'error' : 'some_error'}
        self.request.save()

        # The callback should be received and handled correctly
        cb_response = yield self._send_callback({
            'video_id': 'vid1',
            'error' : 'some_error',
            'processing_state': neondata.RequestState.FAILED})
        self.assertEquals(cb_response.code, 200)

        with self.assertLogExists(logging.ERROR,
                                  'Job failed with state'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self._check_request_cleanup()

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_failed'), 1)

        result = yield self._get_job_result()
        self.assertEquals(result['error_type'], 'JobFailed')
        self.assertRegexpMatches(result['error_msg'], '.*some_error.*')
        self.assertIsNone(result['total_time'])
        self.assertIsNone(result['time_to_processing'])
        self.assertIsNone(result['time_to_finished'])
        self.assertIsNone(result['time_to_serving'])
        self.assertIsNone(result['time_to_callback'])

    @tornado.testing.gen_test
    def test_error_submitting_job(self):
        self.create_job_mock.side_effect = [
            tornado.httpclient.HTTPError(400, 'Cannot submit'),
            tornado.httpclient.HTTPResponse(
                tornado.httpclient.HTTPRequest('http://server'),
                code=401,
                error=tornado.httpclient.HTTPError(401))]

        with self.assertLogExists(logging.ERROR, 'Error submitting job'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_submission_error'), 1)

        with self.assertLogExists(logging.ERROR, 'Error submitting job'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_submission_error'), 1)

        result = yield self._get_job_result()
        self.assertEquals(result['error_type'], 'SubmissionError')
        self.assertIsNone(result['total_time'])
        self.assertIsNone(result['time_to_processing'])
        self.assertIsNone(result['time_to_finished'])
        self.assertIsNone(result['time_to_serving'])
        self.assertIsNone(result['time_to_callback'])


    @tornado.testing.gen_test
    def test_isp_timeout(self):
        self.request.state = neondata.RequestState.SERVING
        self.request.save()
        cb_response = yield self._send_callback({
            'video_id': 'vid1',
            'processing_state': neondata.RequestState.SERVING})
        self.assertEquals(cb_response.code, 200)
        self.isp_call_mock.side_effect = \
          lambda x: tornado.httpclient.HTTPResponse(
              x, code=204)

        with self.assertLogExists(logging.ERROR,
                                  'Too long for image to appear in ISP'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self._check_request_cleanup()

        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.not_available_in_isp'), 1)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_finished'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_serving'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_callback'),
            0)

        result = yield self._get_job_result()
        self.assertEquals(result['error_type'], 'RunningTooLongError')
        self.assertRegexpMatches(result['error_msg'], '.*waiting for ISP.*')
        self.assertIsNone(result['total_time'])
        self.assertGreater(result['time_to_processing'], 0)
        self.assertGreater(result['time_to_finished'], 0)
        self.assertGreater(result['time_to_serving'], 0)
        self.assertGreater(result['time_to_callback'], 0)

    @tornado.testing.gen_test
    def test_timeout_waiting_for_callback(self):
        self.request.state = neondata.RequestState.SERVING
        self.request.save()

        with self.assertLogExists(logging.ERROR, 
                                  'Timeout waiting for the callback'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self._check_request_cleanup()
        
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.no_callback'), 1)

        result = yield self._get_job_result()
        self.assertEquals(result['error_type'], 'RunningTooLongError')
        self.assertIsNone(result['total_time'])
        self.assertGreater(result['time_to_processing'], 0)
        self.assertGreater(result['time_to_finished'], 0)
        self.assertGreater(result['time_to_serving'], 0)
        self.assertIsNone(result['time_to_callback'], 0)

    @tornado.testing.gen_test
    def test_fail_send_result_info(self):
        self.result_mock.side_effect = \
          lambda x: tornado.httpclient.HTTPResponse(
              x, code=404, error=tornado.httpclient.HTTPError(404))

        self.request.state = neondata.RequestState.SERVING
        self.request.save()
        cb_response = yield self._send_callback({
            'video_id': 'vid1',
            'processing_state': neondata.RequestState.SERVING})
        self.assertEquals(cb_response.code, 200)

        with self.assertLogExists(logging.ERROR, 'Error submitting job info'):
            yield self.benchmarker.job_manager.run_test_job('vid1')

        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.result_submission_error'), 1)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
