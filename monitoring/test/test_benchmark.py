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
import unittest
import utils.neon
from utils.options import define, options
from utils import statemon

import logging
_log = logging.getLogger(__name__)

class BenchmarkTest(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(BenchmarkTest, self).setUp()


        self.http_patcher = patch('tornado.httpclient.AsyncHTTPClient.fetch')
        self.http_mock = self._future_wrap_mock(self.http_patcher.start())
        self.headers = {"X-Neon-API-Key" : 'apikey', "Content-Type" : "application/json"}
        self.mock_request = tornado.httpclient.HTTPRequest('http://nope.com',
                                                            headers=self.headers)

        self.http_mock.reset_mock()
        self.http_mock.side_effect = [
            tornado.httpclient.HTTPResponse(self.mock_request, 200,
                                            buffer='{"job_id": "myjobid", '\
                                            '"Location": "location"}')]

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
        
    def tearDown(self):
        self.http_patcher.stop()
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

    def side_effect(self, req):
        if req.method == 'POST':
            return tornado.httpclient.HTTPResponse(self.mock_request, 200,
                                            buffer='{"job_id": "myjobid", '\
                                            '"Location": "location"}')
        else:
            return tornado.httpclient.HTTPResponse(self.mock_request, 204,
                                            buffer='{"job_id": "myjobid", '\
                                            '"Location": "location"}')

    def test_video_serving(self):
        self.request.state = neondata.RequestState.SERVING
        self.request.save()

        self.http_mock.side_effect = [
            tornado.httpclient.HTTPResponse(self.mock_request, 200,
                                            buffer='{"job_id": "myjobid", '\
                                            '"Location": "location"}'),
            tornado.httpclient.HTTPResponse(self.mock_request, 200,
                                            buffer='{"job_id": "myjobid", '\
                                            '"Location": "location"}')]

        with self.assertLogExists(logging.INFO, 'total pipeline time'):
            benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        self.http_mock.assertCalled()

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

    def test_job_not_serving(self):
        with self.assertLogExists(logging.ERROR,
                                  'Job took too long to reach serving state'):
            with self.assertRaises(benchmark_neon_pipeline.RunningTooLongError):
                benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        self._check_request_cleanup()

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_not_serving'), 1)

    def test_job_finished_but_not_serving(self):
        self.request.state = neondata.RequestState.FINISHED
        self.request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Job took too long to reach serving state'):
            with self.assertRaises(benchmark_neon_pipeline.RunningTooLongError):
                benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        self._check_request_cleanup()

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_not_serving'), 1)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_finished'),
            0)

    def test_job_failed(self):
        self.request.state = neondata.RequestState.FAILED
        self.request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Job failed with response'):
            with self.assertRaises(benchmark_neon_pipeline.JobFailed):
                benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        self._check_request_cleanup()

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_failed'), 1)

    def test_error_submitting_job(self):
        self.http_mock.side_effect = [
            tornado.httpclient.HTTPError(400, 'Cannot submit')]

        with self.assertLogExists(logging.ERROR, 'Error submitting job'):
            with self.assertRaises(benchmark_neon_pipeline.SubmissionError):
                benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        # Make sure that statemon is set correctly
        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.job_submission_error'), 1)

    def test_isp_timeout_with_default(self):
        self.request.state = neondata.RequestState.SERVING
        self.request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Too long for image to appear in ISP'):
            with self.assertRaises(benchmark_neon_pipeline.RunningTooLongError):
                benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        self._check_request_cleanup()

        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.not_available_in_isp'), 1)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_finished'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_serving'),
            0)

    def test_isp_timeout(self):
        self.request.state = neondata.RequestState.SERVING
        self.request.save()

        with self.assertLogExists(logging.ERROR,
                                  'Too long for image to appear in ISP'):
            with self.assertRaises(benchmark_neon_pipeline.RunningTooLongError):
                benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        self._check_request_cleanup()

        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.not_available_in_isp'), 1)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_finished'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_serving'),
            0)

    def test_isp_connection_error(self):
        options._set('monitoring.benchmark_neon_pipeline.isp_timeout', 1.0)
        self.request.state = neondata.RequestState.SERVING
        self.request.save()
        self.http_mock.side_effect = self.side_effect

        with self.assertLogExists(logging.ERROR, 'Cannot find ISP'):
            with self.assertRaises(benchmark_neon_pipeline.RunningTooLongError):
                benchmark_neon_pipeline.monitor_neon_pipeline('vid1')

        self.http_mock.assertCalled()

        self._check_request_cleanup()

        self.assertEquals(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.not_available_in_isp'), 1)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_finished'),
            0)
        self.assertGreater(statemon.state.get(
            'monitoring.benchmark_neon_pipeline.time_to_serving'),
            0)
        
        

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
