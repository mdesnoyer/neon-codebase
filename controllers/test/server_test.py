import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import controllers.server
import test_utils.mock_boto_s3
import test_utils.neontest
import test_utils.redis
import redis
import unittest
import utils.neon
import boto.exception
import json
import tempfile
import gzip
import socket
from StringIO import StringIO
import tornado.gen
import controllers.neon_controller as neon_controller
from utils.options import options
from mock import MagicMock, patch, call, ANY
from cmsdb import neondata
from contextlib import closing
from datetime import datetime, timedelta
from controllers.neon_controller import ControllerType, ControllerExperimentState
from tornado.concurrent import Future
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
        if name != "neon-image-serving-directives-test":
            raise boto.exception.BotoServerError("Missing bucket", "")
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
        super(TestS3DirectiveWatcher, self).setUp()

        # Mock for redis database
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.directive_watcher = controllers.server.S3DirectiveWatcher()

        self.i_id = "0"
        self.controller_type = ControllerType.OPTIMIZELY
        self.vcmd1 = self.create_default_video_controller_meta_data(
            a_id='1', exp_id='1', video_id='1', goal_id='1')
        self.vcmd2 = self.create_default_video_controller_meta_data(
            a_id='2', exp_id='2', video_id='2', goal_id='2',
            state=ControllerExperimentState.INPROGRESS)
        self.vcmd3 = self.create_default_video_controller_meta_data(
            a_id='3', exp_id='3', video_id='3', goal_id='3',
            state=ControllerExperimentState.INPROGRESS)

        self.item_now = {
            'name': '%s.%s' % (
                datetime.utcnow().strftime('%Y%m%d%H%M%S'), 'mastermind'),
            'last_modified': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'data': self.generate_gzipstring_with_directive(
                self.vcmd1, [0.2, 0.7, 0.1])
        }
        self.item_one_hour_ago = {
            'name': '%s.%s' % (
                (datetime.utcnow() - timedelta(hours=1)).strftime('%Y%m%d%H%M%S'), 'mastermind'),
            'last_modified': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'data': self.generate_gzipstring_with_directive(
                self.vcmd2, [0.1, 0.7, 0.2])
        }
        self.item_two_hours_ago = {
            'name': '%s.%s' % (
                (datetime.utcnow() - timedelta(hours=2)).strftime('%Y%m%d%H%M%S'), 'mastermind'),
            'last_modified': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'data': self.generate_gzipstring_with_directive(
                self.vcmd2, [0.1, 0.7, 0.2])
        }

        self.s3conn = MockS3Connection([
            self.item_now, self.item_one_hour_ago, self.item_two_hours_ago])
        self.directive_watcher.S3Connection = \
            MagicMock(return_value=self.s3conn)

    def tearDown(self):
        self.redis.stop()
        super(TestS3DirectiveWatcher, self).tearDown()

    def create_default_video_controller_meta_data(self, a_id, exp_id, video_id,
                                                  goal_id, state=None):
        vcmd = neondata.VideoControllerMetaData(
            a_id, self.i_id, self.controller_type,
            exp_id, video_id,
            {'goal_id': str(goal_id), 'ovid_to_tid': {}},
            0)

        if state is not None:
            vcmd.controllers[0]['state'] = state

        vcmd.save()
        return vcmd

    def generate_gzipstring_with_directive(self, vcmd, thumbs_pct=[]):
        directive_data = {
            "type": "dir",
            "aid": vcmd.get_api_key(),
            "vid": "%s_%s" % (
                vcmd.get_api_key(), vcmd.controllers[0]['video_id']),
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

    def test_s3_bucket_permission_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = boto.exception.S3PermissionsError('Ooops')

        with self.assertLogExists(logging.ERROR, 'Could not get bucket'):
            self.directive_watcher.read_directives()

    def test_s3_connection_error(self):
        bucket_mock = MagicMock()
        self.s3conn.get_bucket = bucket_mock
        bucket_mock.side_effect = socket.gaierror('Unknown name')

        with self.assertLogExists(logging.ERROR, 'Error connecting to S3'):
            self.directive_watcher.read_directives()

    @patch('controllers.server.S3DirectiveWatcher.compare_dates')
    def test_read_directives_last_updated_now(self, mock_compare_dates):
        last_update_time = self.directive_watcher.last_update_time \
            + timedelta(hours=0)
        self.directive_watcher.read_directives()

        mock_compare_dates.assert_called_with(
            self.item_now['last_modified'],
            last_update_time)
        self.assertEqual(mock_compare_dates.call_count, 1)

    @patch('controllers.server.S3DirectiveWatcher.compare_dates')
    def test_read_directives_one_hour_ago(self, mock_compare_dates):
        self.directive_watcher.last_update_time = \
            self.directive_watcher.last_update_time \
            - timedelta(hours=1)
        last_update_time = self.directive_watcher.last_update_time \
            + timedelta(hours=0)
        self.directive_watcher.read_directives()

        calls = [
            call(self.item_one_hour_ago['last_modified'], last_update_time),
            call(self.item_now['last_modified'], last_update_time)]
        mock_compare_dates.assert_has_calls(calls, any_order=True)
        self.assertEqual(mock_compare_dates.call_count, 2)

    @patch('cmsdb.neondata.VideoControllerMetaData.get')
    def test_read_directives_check_get_metadata(self, mock_get):
        self.directive_watcher.read_directives()
        mock_get.assert_called_with('1', '1', callback=ANY)
        self.assertEqual(mock_get.call_count, 1)

    @patch('controllers.neon_controller.Controller.get')
    @tornado.testing.gen_test
    def test_read_directives_check_metadata_state(self, mock_get):
        self.vcmd1.controllers[0]['state'] = \
            ControllerExperimentState.COMPLETE
        self.vcmd1.save()

        yield self.directive_watcher.read_directives()
        self.assertEqual(mock_get.call_count, 0)

    @patch('controllers.neon_controller.Controller.get')
    @tornado.testing.gen_test
    def test_read_directives_check_metadata_last_process_date(self, mock_get):
        last_modified_epoch = (
            (datetime.utcnow() + timedelta(hours=1)) -
            datetime(1970, 1, 1)).total_seconds()
        self.vcmd1.controllers[0]['last_process_date'] = \
            last_modified_epoch
        self.vcmd1.save()

        yield self.directive_watcher.read_directives()
        self.assertEqual(mock_get.call_count, 0)

    @patch('controllers.neon_controller.OptimizelyController.update_experiment_with_directives')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_read_directives_call_update_experiment(self, mock_verify_account,
                                                    mock_update):
        mock_verify_account.return_value = None
        yield neon_controller.Controller.create(
                ControllerType.OPTIMIZELY, '1', '0', 'x')

        state_future = Future()
        state_future.set_result(ControllerExperimentState.INPROGRESS)
        mock_update.return_value = state_future

        yield self.directive_watcher.read_directives()
        mock_update.assert_called_with(
            self.vcmd1.controllers[0], self.unzip_file(self.item_now['data'])['1_1'], callback=ANY
        )

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
