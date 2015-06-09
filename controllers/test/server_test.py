#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import controllers.server
import test_utils.neontest
import test_utils.redis
import unittest
import utils.neon
import boto.exception
import json
import gzip
import socket
import tornado.gen
import tornado.testing
import concurrent.futures
from utils.options import options
from mock import MagicMock, patch, call, ANY
from cmsdb import neondata
from datetime import datetime, timedelta
from StringIO import StringIO
_log = logging.getLogger(__name__)


class MockS3Connection(object):
    class MockItem(object):
        def __init__(self, name, last_modified, data):
            self.name = name
            self.last_modified = last_modified
            self.data = data

        def get_contents_as_string(self):
            return self.data

    def __init__(self, _keys):
        self.keys = {}
        for k in _keys:
            self.keys[k['name']] = \
                MockS3Connection.MockItem(
                    k['name'], k['last_modified'], k['data'])

    def get_bucket(self, name):
        return self

    def list(self, prefix=None):
        if prefix is None:
            return [self.keys[k] for k in self.keys.iterkeys()]

        return [
            self.keys[k] for k in self.keys.iterkeys()
            if k.startswith(prefix)]

    def get_key(self, key):
        return self.keys[key]


class TestS3DirectiveWatcher(test_utils.neontest.AsyncTestCase):
    ''' Controller Results Retriever Test '''
    def setUp(self):
        # Mock for redis database
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.directive_watcher = controllers.server.S3DirectiveWatcher()

        self.i_id = "0"
        self.controller_type = neondata.ControllerType.OPTIMIZELY
        self.ecmd1 = self.create_default_experiment_controller_meta_data(
            a_id='1', exp_id='1', video_id='1', goal_id='1')
        self.ecmd2 = self.create_default_experiment_controller_meta_data(
            a_id='2', exp_id='2', video_id='2', goal_id='2',
            state=neondata.ControllerExperimentState.INPROGRESS)
        self.ecmd3 = self.create_default_experiment_controller_meta_data(
            a_id='3', exp_id='3', video_id='3', goal_id='3',
            state=neondata.ControllerExperimentState.INPROGRESS)

        self.item_now = {
            'name': '%s.%s' % (
                datetime.utcnow().strftime('%Y%m%d%H%M%S'), 'mastermind'),
            'last_modified': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'data': self.generate_gzipstring_with_directive(
                self.ecmd1, [0.2, 0.7, 0.1])
        }
        self.item_one_hour_ago = {
            'name': '%s.%s' % (
                (datetime.utcnow() - timedelta(hours=1)).strftime('%Y%m%d%H%M%S'), 'mastermind'),
            'last_modified': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'data': self.generate_gzipstring_with_directive(
                self.ecmd2, [0.1, 0.7, 0.2])
        }
        self.item_two_hours_ago = {
            'name': '%s.%s' % (
                (datetime.utcnow() - timedelta(hours=2)).strftime('%Y%m%d%H%M%S'), 'mastermind'),
            'last_modified': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'data': self.generate_gzipstring_with_directive(
                self.ecmd2, [0.1, 0.7, 0.2])
        }

        # Mock for S3
        self.bucket_name = 'neon-image-serving-directives-test'
        self.s3conn = MockS3Connection([
            self.item_now, self.item_one_hour_ago, self.item_two_hours_ago])
        self.directive_watcher.S3Connection = \
            MagicMock(return_value=self.s3conn)

        super(TestS3DirectiveWatcher, self).setUp()

    def tearDown(self):
        self.redis.stop()
        super(TestS3DirectiveWatcher, self).tearDown()

    def _future_wrap_mock(self, outer_mock):
        inner_mock = MagicMock()
        def _build_future(*args, **kwargs):
            future = concurrent.futures.Future()
            try:
                future.set_result(inner_mock(*args, **kwargs))
            except Exception as e:
                future.set_exception(e)
            return future
        outer_mock.side_effect = _build_future
        return inner_mock

    def create_default_experiment_controller_meta_data(self, a_id, exp_id,
                                                       video_id, goal_id,
                                                       state=None):
        ecmd = neondata.ExperimentControllerMetaData(
            a_id, self.i_id, self.controller_type,
            exp_id, video_id,
            {'goal_id': str(goal_id), 'ovid_to_tid': {}},
            0)

        if state is not None:
            ecmd.controllers[0]['state'] = state

        ecmd.save()
        return ecmd

    def generate_gzipstring_with_directive(self, ecmd, thumbs_pct=[]):
        directive_data = {
            "type": "dir",
            "aid": ecmd.get_api_key(),
            "vid": "%s_%s" % (
                ecmd.get_api_key(), ecmd.controllers[0]['video_id']),
            "sla": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fractions": []
        }

        for i in range(len(thumbs_pct)):
            fraction = {
                "pct": thumbs_pct[i],
                "tid": "thumb%s" % i,
                "default_url": "http://neon/thumb%s_480_640.jpg" % i,
                "imgs": [{
                    "h": 480,
                    "w": 640,
                    "url": "http://neon/thumb%s_480_640.jpg" % i
                }, {
                    "h": 600,
                    "w": 800,
                    "url": "http://neon/thumb%s_600_800.jpg" % i
                }]
            }
            directive_data['fractions'].append(fraction)

        out = StringIO()
        with gzip.GzipFile(fileobj=out, mode="w") as f:
            f.write('expiry=2015-06-03T13:49:46Z\n')
            f.write(json.dumps(directive_data))
            f.write('\nend')
        return out.getvalue()

    def unzip_file(self, file_content):
        gz = gzip.GzipFile(fileobj=StringIO(file_content), mode='rb')
        lines = gz.read().split('\n')

        directives = {}
        for line in lines[1:]:
            if len(line.strip()) == 0:
                # It's an empty line
                continue
            if line == 'end':
                break
            data = json.loads(line)
            if data['type'] == 'dir':
                directives[data['vid']] = data

        return directives

    def test_s3_bucket_missing(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = boto.exception.BotoServerError(
            'Missing bucket', '')
        self.assertEqual(
            self.directive_watcher.get_bucket(self.bucket_name), None)

    def test_s3_bucket_permission_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = boto.exception.S3PermissionsError('Ooops')
        self.assertEqual(
            self.directive_watcher.get_bucket(self.bucket_name), None)

    def test_s3_connection_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = socket.gaierror('Unknown name')
        self.assertEqual(
            self.directive_watcher.get_bucket(self.bucket_name), None)

    @patch('controllers.server.S3DirectiveWatcher.compare_dates')
    def test_filter_and_get_files_from_s3_with_last_updated_now(
                                            self, mock_compare_dates):
        last_update_time = \
            self.directive_watcher.last_update_time + timedelta(hours=0)
        self.directive_watcher.filter_and_get_files_from_s3(
            self.s3conn.get_bucket(self.bucket_name), last_update_time)

        mock_compare_dates.assert_called_with(
            self.item_now['last_modified'], last_update_time)
        self.assertEqual(mock_compare_dates.call_count, 1)

    @patch('controllers.server.S3DirectiveWatcher.compare_dates')
    def test_filter_and_get_files_from_s3_with_one_hour_ago(
                                            self, mock_compare_dates):
        self.directive_watcher.last_update_time = \
            self.directive_watcher.last_update_time - timedelta(hours=1)

        self.directive_watcher.filter_and_get_files_from_s3(
            self.s3conn.get_bucket(self.bucket_name),
            self.directive_watcher.last_update_time)

        calls = [
            call(
                self.item_one_hour_ago['last_modified'],
                self.directive_watcher.last_update_time),
            call(
                self.item_now['last_modified'],
                self.directive_watcher.last_update_time)]

        mock_compare_dates.assert_has_calls(calls, any_order=True)
        self.assertEqual(mock_compare_dates.call_count, 2)

    @tornado.testing.gen_test
    def test_run_one_cycle_call_read_directives(self):
        read_directives_mocker = patch(
            'controllers.server.S3DirectiveWatcher.read_directives')
        read_directives_mock = self._future_wrap_mock(
            read_directives_mocker.start())
        read_directives_mock.return_value = None

        yield self.directive_watcher.run_one_cycle()
        read_directives_mock.assert_called_with(
            self.s3conn, self.s3conn.get_key(self.item_now['name']))
        self.assertEqual(read_directives_mock.call_count, 1)

    @tornado.testing.gen_test
    def test_run_one_cycle_call_read_directives_for_one_hour_ago(self):
        read_directives_mocker = patch(
            'controllers.server.S3DirectiveWatcher.read_directives')
        read_directives_mock = self._future_wrap_mock(
            read_directives_mocker.start())
        read_directives_mock.return_value = None

        self.directive_watcher.last_update_time = \
            self.directive_watcher.last_update_time - timedelta(hours=1)

        yield self.directive_watcher.run_one_cycle()
        calls = [
            call(self.s3conn, self.s3conn.get_key(self.item_now['name'])),
            call(self.s3conn, self.s3conn.get_key(
                self.item_one_hour_ago['name']))
        ]
        read_directives_mock.assert_has_calls(calls, any_order=True)
        self.assertEqual(read_directives_mock.call_count, 2)

    @patch('controllers.server.S3DirectiveWatcher.parse_directive_file')
    @tornado.testing.gen_test
    def test_read_directives_with_parse_directive_file(self, mock_parse):
        yield self.directive_watcher.read_directives(
            self.s3conn, self.s3conn.get_key(self.item_now['name']))
        mock_parse.assert_called_with(self.item_now['data'])
        self.assertEqual(mock_parse.call_count, 1)

    @patch('cmsdb.neondata.ExperimentControllerMetaData.get')
    @tornado.testing.gen_test
    def test_read_directives_check_get_metadata(self, mock_get):
        self.directive_watcher.read_directives(
            self.s3conn, self.s3conn.get_key(self.item_now['name']))
        mock_get.assert_called_with('1', '1', callback=ANY)
        self.assertEqual(mock_get.call_count, 1)

    @patch('cmsdb.neondata.Controller.get')
    @tornado.testing.gen_test
    def test_read_directives_check_metadata_state(self, mock_get):
        self.ecmd1.controllers[0]['state'] = \
            neondata.ControllerExperimentState.COMPLETE
        self.ecmd1.save()

        yield self.directive_watcher.read_directives(
            self.s3conn, self.s3conn.get_key(self.item_now['name']))
        self.assertEqual(mock_get.call_count, 0)

    @patch('cmsdb.neondata.Controller.get')
    @tornado.testing.gen_test
    def test_read_directives_check_metadata_last_process_date(self, mock_get):
        last_modified_epoch = (
            (datetime.utcnow() + timedelta(hours=1)) -
            datetime(1970, 1, 1)).total_seconds()
        self.ecmd1.controllers[0]['last_process_date'] = last_modified_epoch
        self.ecmd1.save()

        yield self.directive_watcher.read_directives(
            self.s3conn, self.s3conn.get_key(self.item_now['name']))
        self.assertEqual(mock_get.call_count, 0)

    @tornado.testing.gen_test
    def test_read_directives_call_update_experiment(self):
        verify_account_mocker = patch(
            'cmsdb.neondata.OptimizelyController.verify_account')
        verify_account_mock = self._future_wrap_mock(
            verify_account_mocker.start())
        verify_account_mock.return_value = None

        yield neondata.Controller.create(
                neondata.ControllerType.OPTIMIZELY, '1', '0', 'x')

        upd_directive_mocker = patch(
            'cmsdb.neondata.OptimizelyController.update_experiment_with_directives')
        upd_directive_mock = self._future_wrap_mock(
            upd_directive_mocker.start())
        upd_directive_mock.return_value = \
            neondata.ControllerExperimentState.INPROGRESS

        last_modified_epoch = (
            datetime.utcnow() -
            datetime(1970, 1, 1)).total_seconds()

        yield self.directive_watcher.read_directives(
            self.s3conn,
            self.s3conn.get_key(self.item_now['name']))

        ecmd = yield tornado.gen.Task(
            neondata.ExperimentControllerMetaData.get, '1', '1')

        ecmd_ctr = ecmd.controllers[0]
        self.assertEqual(upd_directive_mock.call_count, 1)
        upd_directive_mock.assert_called_with(
            ecmd_ctr,
            self.unzip_file(self.item_now['data'])['1_1'])
        self.assertGreater(last_modified_epoch, ecmd_ctr['last_process_date'])
        self.assertEqual(
            neondata.ControllerExperimentState.INPROGRESS, ecmd_ctr['state'])

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
