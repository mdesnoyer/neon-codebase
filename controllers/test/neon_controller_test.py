#!/usr/bin/env python
'''
Neon Controller Test
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import json
import logging
import unittest
import utils.neon
import test_utils.neontest
import test_utils.redis
import redis
import controllers.neon_controller as neon_controller
import test_utils.neon_controller_aux as neon_controller_aux
import tornado.gen
import datetime
from cmsdb import neondata
from mock import patch, MagicMock
from tornado.httpclient import HTTPRequest, HTTPResponse
from StringIO import StringIO
from concurrent.futures import Future
_log = logging.getLogger(__name__)

# Constants
CONTROLLER_TYPE = neon_controller.ControllerType.OPTIMIZELY
OPTMIZELY_ENDPOINT = "https://test.optimizely.com"
ACCESS_TOKEN = "5851ee8c6358f0d46850dd60fe3d17e5:aff12cb2"
ACCOUNT_ID = "77trcufrh25ztyru4gx7eq95"
VIDEO_ID = ACCOUNT_ID + "_99987212"
PLATFORM_ID = "0"
PROJECT_NAME = "neon-lab"
EXPERIMENT_ID = 2889571145
GOAL_ID = 2930210622


class TestControllerOptimizely(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        # Optimizely Auxiliator and Responses data pre created
        self.optimizely_api_aux = neon_controller_aux.OptimizelyApiAux()
        self.generate_data_for_optimizely()

        # Default Video Controller MetaData to use
        self.vcmd_default = self.create_default_video_controller_meta_data()

        # S3 fractions file
        self.fractions_s3_file = self.generate_fractions_s3_file()
        super(TestControllerOptimizely, self).setUp()

    def tearDown(self):
        self.redis.stop()
        super(TestControllerOptimizely, self).tearDown()

    def get_value_in_list(self, _list, key, value):
        new_list = [t for t in _list if t[key] == value]
        if len(new_list) > 0:
            return new_list[0]
        return None

    def generate_data_for_optimizely(self):
        pj = self.optimizely_api_aux.response_project_create(
            project_name=PROJECT_NAME)
        exp = self.optimizely_api_aux.response_experiment_create(
            experiment_id=EXPERIMENT_ID, project_id=pj['id'],
            description="thumb_id",
            edit_url="https://www.neon-lab.com/videos/")
        goal = self.optimizely_api_aux.response_goal_create(
            goal_id=GOAL_ID, project_id=pj['id'], title="Video Image Clicks",
            goal_type=0, selector="div.video > img",
            target_to_experiments=True, experiment_ids=[exp['id']])
        goal_type2 = self.optimizely_api_aux.response_goal_create(
            project_id=pj['id'], title="Engagement",
            goal_type=2, selector="", target_to_experiments=True,
            experiment_ids=[exp['id']])
        for idx, var in enumerate([5000, 5000, 0]):
            # is_paused = True if (v == 0) else False,
            self.optimizely_api_aux.response_variation_create(
                project_id=pj['id'], experiment_id=exp['id'],
                description="Variation #%s" % idx, is_paused=False,
                js_component='', weight=var)

    def create_default_video_controller_meta_data(self):
        vcmd = neondata.VideoControllerMetaData(
            ACCOUNT_ID,
            PLATFORM_ID,
            neon_controller.ControllerType.OPTIMIZELY,
            EXPERIMENT_ID,
            VIDEO_ID,
            {
                'goal_id': GOAL_ID,
                'element_id': '#1',
                'js_component': "$(\"img#1\").attr(\"src\", \"THUMB_URL\")",
                'ovid_to_tid': {}
            },
            0)
        vcmd.save()
        return vcmd

    def generate_fractions_s3_file(self):
        return {
            "type": "dir",
            "aid": ACCOUNT_ID,
            "vid": VIDEO_ID,
            "sla": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fractions": [{
                "pct": 0.8,
                "tid": "thumb1",
                "default_url": "http://neon/thumb1_480_640.jpg",
                "imgs": [{
                    "h": 480,
                    "w": 640,
                    "url": "http://neon/thumb1_480_640.jpg"
                }, {
                    "h": 600,
                    "w": 800,
                    "url": "http://neon/thumb1_600_800.jpg"
                }]
              },
              {
                "pct": 0.2,
                "tid": "thumb2",
                "default_url": "http://neon/thumb2_480_640.jpg",
                "imgs": [{
                    "h": 480,
                    "w": 640,
                    "url": "http://neon/thumb2_480_640.jpg"
                }, {
                    "h": 600,
                    "w": 800,
                    "url": "http://neon/thumb2_600_800.jpg"
                }]
            }]
        }

    ###########################################################################
    # Update Experiment With Directives
    ###########################################################################
    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_experiment_with_get_variations_bad_request(
                                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 400,
            buffer=StringIO(json.dumps('timeout')))

        with self.assertRaises(ValueError) as e:
            yield controller.update_experiment_with_directives(
                self.vcmd_default.controllers[0], self.fractions_s3_file)
        self.assertTrue("code: 400" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_experiment_with_variations_to_disable(
                                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        def _side_effect(request, callback=None, *args, **kwargs):
            id = int(request.url.rsplit('/', 1)[1])
            if "variations" in request.url and request.method == 'GET':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.response_variation_list())))
            elif "variations" in request.url and request.method == 'PUT':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.response_variation_update(
                            variation_id=id, weight=0, is_paused=True))))
            elif "experiments" in request.url and request.method == 'PUT':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.response_experiment_update(
                            experiment_id=id, percentage_included=10000,
                            status="Running"))))

        mock_http.side_effect = _side_effect
        yield controller.update_experiment_with_directives(
                self.vcmd_default.controllers[0], {'fractions': None})

    ###########################################################################
    # GET and CREATE
    ###########################################################################
    @patch('controllers.neon_controller.utils.http.send_request')
    @tornado.testing.gen_test
    def test_create_controller_with_invalid_access_token(self, mock_http):
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 401,
            buffer=StringIO(json.dumps('Authentication failed')))

        with self.assertRaises(ValueError) as e:
            yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)
        self.assertTrue("code: 401" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_controller_already_exist(self, mock_verify_account,
                                             mock_http):
        mock_verify_account.return_value = None
        yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # call again
        with self.assertRaises(ValueError) as e:
            yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)
        self.assertEquals("Integration already exists", str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_controller_success(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        self.assertEquals("OptimizelyController", type(controller).__name__)
        self.assertEquals(controller.platform_id, PLATFORM_ID)
        self.assertEquals(controller.api_key, ACCOUNT_ID)
        self.assertEquals(controller.access_token, ACCESS_TOKEN)
        self.assertEquals(controller.key, "optimizely_%s_%s" % (
            ACCOUNT_ID, PLATFORM_ID))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_get_controller_success(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        controller = neon_controller.Controller.get(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID)
        self.assertEquals("OptimizelyController", type(controller).__name__)
        self.assertEquals(controller.platform_id, PLATFORM_ID)
        self.assertEquals(controller.api_key, ACCOUNT_ID)
        self.assertEquals(controller.access_token, ACCESS_TOKEN)
        self.assertEquals(controller.key, "optimizely_%s_%s" % (
            ACCOUNT_ID, PLATFORM_ID))

    ###########################################################################
    # Verify Experiment
    ###########################################################################
    def _side_effect_to_verity_experiment(self, request, callback=None, *args, **kwargs):
        id = int(request.url.rsplit('/', 1)[1])
        if "experiments" in request.url:
            return HTTPResponse(
                HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                buffer=StringIO(json.dumps(
                    self.optimizely_api_aux.response_experiment_read(
                        experiment_id=id))))
        elif "goals" in request.url:
            return HTTPResponse(
                HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                buffer=StringIO(json.dumps(
                    self.optimizely_api_aux.response_goal_read(
                        goal_id=GOAL_ID))))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_experiment_not_found(
                                        self, mock_verify_account, mock_http):

        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 400,
            buffer=StringIO(json.dumps("not found")))

        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                PLATFORM_ID, EXPERIMENT_ID, VIDEO_ID)
        self.assertTrue("code: 400" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_goal_not_found(self, mock_verify_account,
                                                   mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        def _side_effect(request, callback=None, *args, **kwargs):
            if "experiments" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.response_experiment_read(
                            EXPERIMENT_ID))))
            elif "goals" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 400,
                    buffer=StringIO(json.dumps("not found")))

        mock_http.side_effect = _side_effect
        extras = {'goal_id': None}
        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                PLATFORM_ID, EXPERIMENT_ID, VIDEO_ID, extras)
        self.assertTrue("code: 400" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_goal_type_wrong(self, mock_verify_account,
                                                    mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        def _side_effect(request, callback=None, *args, **kwargs):
            if "experiments" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.response_experiment_read(
                            EXPERIMENT_ID))))
            elif "goals" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                    buffer=StringIO(json.dumps(self.get_value_in_list(
                        self.optimizely_api_aux.response_goal_list(),
                        'goal_type', 2))))

        mock_http.side_effect = _side_effect
        extras = {'goal_id': None}
        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                PLATFORM_ID, EXPERIMENT_ID, VIDEO_ID, extras)
        self.assertTrue("Invalid goal_id" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_success(
                    self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        experiment_id = EXPERIMENT_ID + 1
        self.optimizely_api_aux.response_experiment_create(
            experiment_id=experiment_id, project_id=1,
            description="thumb_id",
            edit_url="https://www.neon-lab.com/videos/")

        mock_http.side_effect = self._side_effect_to_verity_experiment
        extras = {
            'goal_id': GOAL_ID,
            'element_id': '#11',
            'js_component': '$(\"#11\").attr(\"src\": \"http://fake/1.png\");'}

        response = yield controller.verify_experiment(
            PLATFORM_ID, experiment_id, VIDEO_ID, extras)
        self.assertEquals(response['goal_id'], extras['goal_id'])
        self.assertEquals(response['element_id'], extras['element_id'])
        self.assertEquals(response['js_component'], extras['js_component'])
        self.assertEquals(response['experiment_id'], experiment_id)
        self.assertEquals(response['goal_id'], GOAL_ID)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_already_exist(
                    self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        mock_http.side_effect = self._side_effect_to_verity_experiment
        extras = {'goal_id': GOAL_ID, 'element_id': '', 'js_component': ''}

        # call again
        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                PLATFORM_ID, EXPERIMENT_ID, VIDEO_ID, extras)
        self.assertTrue("Experiment already exists" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_append_same_video(
                    self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        extras = {'goal_id': GOAL_ID, 'element_id': '', 'js_component': ''}
        vd = neondata.VideoControllerMetaData(
            ACCOUNT_ID, PLATFORM_ID, neon_controller.ControllerType.OPTIMIZELY,
            '123456', VIDEO_ID, extras, 0)
        vd.save()

        mock_http.side_effect = self._side_effect_to_verity_experiment
        yield controller.verify_experiment(
            PLATFORM_ID, EXPERIMENT_ID, VIDEO_ID, extras)

        vmd = yield tornado.gen.Task(
            neondata.VideoControllerMetaData.get,
            ACCOUNT_ID, VIDEO_ID)
        self.assertEquals(len(vmd.controllers), 2)


class TestControllerOptimizelyApi(test_utils.neontest.AsyncTestCase):
    # Test cases that do requests to optimizely API

    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.project_name = PROJECT_NAME
        self.optimizely_api_aux = neon_controller_aux.OptimizelyApiAux()
        super(TestControllerOptimizelyApi, self).setUp()

    def tearDown(self):
        self.redis.stop()
        super(TestControllerOptimizelyApi, self).tearDown()

    ###########################################################################
    # Projects
    ###########################################################################
    def create_new_project_response(self):
        return self.optimizely_api_aux.response_project_create(
            project_name=self.project_name)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_project(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # create project
        project_create = self.create_new_project_response()
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 201,
            buffer=StringIO(json.dumps(project_create)))

        response = controller.create_project(
            project_name=project_create['project_name'])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_name"], project_create['project_name'])
        self.assertEqual(response_data["project_status"], "Active")

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_read_project(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # read project
        project_create = self.create_new_project_response()
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(project_create)))

        response = controller.read_project(
            project_id=project_create['id'])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], project_create['id'])
        self.assertEqual(
            response_data["project_name"], project_create['project_name'])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_project(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # update project
        project_create = self.create_new_project_response()
        project_update = self.optimizely_api_aux.response_project_update(
            project_id=project_create['id'],
            project_name="neonlabupdate",
            ip_filter="0.0.0.0")

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 202,
            buffer=StringIO(json.dumps(project_update)))

        response = controller.update_project(
            project_id=project_update["id"],
            project_name=project_update["project_name"],
            ip_filter=project_update["ip_filter"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_name"], project_update["project_name"])
        self.assertEqual(
            response_data["ip_filter"], project_update["ip_filter"])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_get_projects(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # get list projects
        for x in range(0, 3):
            self.create_new_project_response()
        project_list = self.optimizely_api_aux.response_project_list()
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(project_list)))

        response = controller.get_projects()
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

    ##########################################################################
    # Experiments
    ##########################################################################
    def create_new_experiment_response(self, project_id):
        return self.optimizely_api_aux.response_experiment_create(
            project_id=project_id,
            description="video_id_video_title",
            edit_url="https://www.neon-lab.com/videos/")

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_experiment(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        project_id = 1
        # create experiment
        experiment_create = self.create_new_experiment_response(project_id)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 201,
            buffer=StringIO(json.dumps(experiment_create)))

        response = controller.create_experiment(
            project_id=project_id,
            description=experiment_create["description"],
            edit_url=experiment_create["edit_url"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_id"], experiment_create["project_id"])
        self.assertEqual(
            response_data["description"], experiment_create["description"])
        # // ... (other fields omitted)
        url_conditions = response_data["url_conditions"][0]
        self.assertEqual(len(response_data["variation_ids"]), 2)
        self.assertEqual(response_data["status"], "Not started")
        self.assertEqual(url_conditions["match_type"], "simple")
        self.assertEqual(
            url_conditions["value"], experiment_create["edit_url"])
        self.assertEqual(
            response_data["edit_url"], experiment_create["edit_url"])
        self.assertEqual(response_data["percentage_included"], 10000)
        self.assertEqual(response_data["activation_mode"], "immediate")
        self.assertEqual(response_data["experiment_type"], "ab")

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_read_experiment(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # read experiment
        experiment_create = self.create_new_experiment_response(1)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(experiment_create)))

        response = controller.read_experiment(
            experiment_id=experiment_create["id"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], experiment_create["id"])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_experiment(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # update experiment
        experiment_create = self.create_new_experiment_response(1)
        experiment_update = self.optimizely_api_aux.response_experiment_update(
            experiment_id=experiment_create["id"],
            status="Paused")

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 202,
            buffer=StringIO(json.dumps(experiment_update)))

        response = controller.update_experiment(
            experiment_id=experiment_update["id"],
            status=experiment_update["status"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["status"], experiment_update["status"])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_delete_experiment(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # delete experiment
        experiment_create = self.create_new_experiment_response(1)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 204, buffer=None)

        response = controller.delete_experiment(
            experiment_id=experiment_create["id"])
        self.assertEqual(response["status_code"], 204)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_get_experiments(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        project_id = 1
        # get list experiments
        for x in range(0, 3):
            self.create_new_experiment_response(project_id)
        experiment_list = self.optimizely_api_aux.response_experiment_list()
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(experiment_list)))

        response = controller.get_experiments(
            project_id=project_id)
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_get_experiment_status(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # get list experiment status
        exp_status = self.optimizely_api_aux.response_experiment_status()
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(exp_status)))

        response = controller.get_experiments_status(
            experiment_id=1)
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

    ##########################################################################
    # Variations
    ##########################################################################
    def create_new_variation_response(self, experiment_id):
        return self.optimizely_api_aux.response_variation_create(
            project_id=1,
            experiment_id=experiment_id,
            description="Variation #2",
            js_component="$(\".headline\").text(\"New headline\");",
            weight=3333)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_variation(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        experiment_id = 1
        # create variation
        variation_create = self.create_new_variation_response(experiment_id)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 201,
            buffer=StringIO(json.dumps(variation_create)))

        response = controller.create_variation(
            experiment_id=variation_create["experiment_id"],
            description=variation_create["description"],
            js_component=variation_create["js_component"],
            weight=variation_create["weight"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["experiment_id"], variation_create["experiment_id"])
        self.assertEqual(
            response_data["description"], variation_create["description"])
        self.assertEqual(
            response_data["js_component"], variation_create["js_component"])
        self.assertEqual(response_data["weight"], variation_create["weight"])
        self.assertEqual(response_data["is_paused"], False)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_read_variation(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # read variation
        variation_create = self.create_new_variation_response(1)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(variation_create)))

        response = controller.read_variation(
            variation_id=variation_create["id"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], variation_create["id"])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_variation(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # update variation
        variation_create = self.create_new_variation_response(1)
        variation_update = self.optimizely_api_aux.response_variation_update(
            variation_id=variation_create["id"],
            description="Change name variation #2",
            weight=7777,
            is_paused=True)

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 202,
            buffer=StringIO(json.dumps(variation_update)))

        response = controller.update_variation(
            variation_id=variation_update["id"],
            description=variation_update["description"],
            weight=variation_update["weight"],
            is_paused=variation_update["is_paused"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["description"], variation_update["description"])
        self.assertEqual(
            response_data["weight"], variation_update["weight"])
        self.assertEqual(
            response_data["is_paused"], variation_update["is_paused"])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_delete_variation(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # delete variation
        variation_create = self.create_new_variation_response(1)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 204, buffer=None)

        response = controller.delete_variation(
            variation_id=variation_create["id"])
        self.assertEqual(response["status_code"], 204)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_get_variations(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        experiment_id = 1
        # get list variations
        for x in range(0, 3):
            self.create_new_variation_response(experiment_id)
        variation_list = self.optimizely_api_aux.response_variation_list()
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(variation_list)))

        response = controller.get_variations(
            experiment_id=experiment_id)
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

    ##########################################################################
    # Goals
    ##########################################################################
    def create_new_goal_response(self, project_id):
        return self.optimizely_api_aux.response_goal_create(
            project_id=project_id,
            title="Add to images clicks",
            goal_type=0,
            selector="div.video > img",
            target_to_experiments=True,
            experiment_ids=[11231])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_goal(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        project_id = 1
        # create goal
        goal_create = self.create_new_goal_response(project_id)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 201,
            buffer=StringIO(json.dumps(goal_create)))

        response = controller.create_goal(
            project_id=goal_create["project_id"],
            title=goal_create["title"],
            goal_type=goal_create["goal_type"],
            selector=goal_create["selector"],
            target_to_experiments=goal_create["target_to_experiments"],
            experiment_ids=goal_create["experiment_ids"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_id"], goal_create["project_id"])
        self.assertEqual(response_data["title"], goal_create["title"])
        self.assertEqual(response_data["goal_type"], goal_create["goal_type"])
        self.assertEqual(response_data["selector"], goal_create["selector"])
        self.assertEqual(
            response_data["target_to_experiments"],
            goal_create["target_to_experiments"])
        self.assertEqual(
            response_data["experiment_ids"][0],
            goal_create["experiment_ids"][0])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_read_goal(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # read goal
        goal_create = self.create_new_goal_response(1)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(goal_create)))

        response = controller.read_goal(
            goal_id=goal_create["id"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], goal_create["id"])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_goal(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # update goal
        goal_create = self.create_new_goal_response(1)
        goal_update = self.optimizely_api_aux.response_goal_update(
            goal_id=goal_create["id"],
            title="Change - Add to images clicks",
            goal_type=1,
            selector="#imageDivid",
            target_to_experiments=False,
            experiment_ids=[])

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 202,
            buffer=StringIO(json.dumps(goal_update)))

        response = controller.update_goal(
            goal_id=goal_update["id"],
            title=goal_update["title"],
            goal_type=goal_update["goal_type"],
            selector=goal_update["selector"],
            target_to_experiments=goal_update["target_to_experiments"],
            experiment_ids=goal_update["experiment_ids"])
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["title"], goal_update["title"])
        self.assertEqual(response_data["goal_type"], goal_update["goal_type"])
        self.assertEqual(response_data["selector"], goal_update["selector"])
        self.assertEqual(
            response_data["experiment_ids"], goal_update["experiment_ids"])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_delete_goal(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        # delete goal
        goal_create = self.create_new_goal_response(1)
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 204, buffer=None)

        response = controller.delete_goal(
            goal_id=goal_create["id"])
        self.assertEqual(response["status_code"], 204)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_get_goals(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                CONTROLLER_TYPE, ACCOUNT_ID, PLATFORM_ID, ACCESS_TOKEN)

        project_id = 1
        # get list goals
        for x in range(0, 3):
            self.create_new_goal_response(project_id)
        goal_list = self.optimizely_api_aux.response_goal_list()
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(goal_list)))

        response = controller.get_goals(
            project_id=project_id)
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

if __name__ == "__main__":
    utils.neon.InitNeon()
    unittest.main()
