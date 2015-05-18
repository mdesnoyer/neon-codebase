#!/usr/bin/env python
'''
Optimizely API Test
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
import api.optimizely_api
from mock import patch
from test_utils.optimizely_aux import OptimizelyAux
import test_utils.neontest
from tornado.httpclient import HTTPRequest, HTTPResponse
from StringIO import StringIO

_log = logging.getLogger(__name__)

# Constants
ACCESS_TOKEN = "5851ee8c6358f0d46850dd60fe3d17e5:aff12cb2"
PROJECT_NAME = "neon-lab"


class TestOptimizelyApi(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestOptimizelyApi, self).setUp()
        self.optimizely_aux = OptimizelyAux()
        self.optimizely_api = api.optimizely_api.OptimizelyApi(ACCESS_TOKEN)
        self.project_name = PROJECT_NAME

    def tearDown(self):
        super(TestOptimizelyApi, self).tearDown()

##############################################################################
# Projects
##############################################################################
    @patch('api.optimizely_api.utils.http.send_request')
    def test_create_project(self, utils_http):
        # create project
        project_create = self.optimizely_aux.response_project_create(
            project_name=self.project_name
        )
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            201,
            buffer=StringIO(json.dumps(project_create))
        )

        response = self.optimizely_api.create_project(
            project_name=project_create['project_name']
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_name"],
            project_create['project_name']
        )
        self.assertEqual(response_data["project_status"], "Active")

    @patch('api.optimizely_api.utils.http.send_request')
    def test_read_project(self, utils_http):
        # create project
        project_create = self.optimizely_aux.response_project_create(
            project_name=self.project_name
        )

        # read project
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(project_create))
        )

        response = self.optimizely_api.read_project(
            project_id=project_create['id']
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], project_create['id'])
        self.assertEqual(
            response_data["project_name"],
            project_create['project_name']
        )

    @patch('api.optimizely_api.utils.http.send_request')
    def test_update_project(self, utils_http):
        # create project
        project_create = self.optimizely_aux.response_project_create(
            project_name=self.project_name
        )

        # update project
        project_update = self.optimizely_aux.response_project_update(
            project_id=project_create['id'],
            project_name="neonlab",
            ip_filter="0.0.0.0"
        )
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            202,
            buffer=StringIO(json.dumps(project_update))
        )

        response = self.optimizely_api.update_project(
            project_id=project_update["id"],
            project_name=project_update["project_name"],
            ip_filter=project_update["ip_filter"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_name"],
            project_update["project_name"]
        )
        self.assertEqual(
            response_data["ip_filter"],
            project_update["ip_filter"]
        )

    @patch('api.optimizely_api.utils.http.send_request')
    def test_get_projects(self, utils_http):
        # create projects
        for x in range(0, 3):
            self.optimizely_aux.response_project_create(
                project_name=self.project_name
            )

        # get list projects
        project_list = self.optimizely_aux.response_project_list()
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(project_list))
        )

        response = self.optimizely_api.get_projects()
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

##############################################################################
# Experiments
##############################################################################

    @patch('api.optimizely_api.utils.http.send_request')
    def test_create_experiment(self, utils_http):
        project_id = 1
        # create experiment
        experiment_create = self.optimizely_aux.response_experiment_create(
            project_id=1,
            description="video_id_video_title",
            edit_url="https://www.neon-lab.com/videos/"
        )
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            201,
            buffer=StringIO(json.dumps(experiment_create))
        )

        response = self.optimizely_api.create_experiment(
            project_id=project_id,
            description=experiment_create["description"],
            edit_url=experiment_create["edit_url"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_id"],
            experiment_create["project_id"]
        )
        self.assertEqual(
            response_data["description"],
            experiment_create["description"]
        )
        # // ... (other fields omitted)
        url_conditions = response_data["url_conditions"][0]
        self.assertEqual(len(response_data["variation_ids"]), 2)
        self.assertEqual(response_data["status"], "Not started")
        self.assertEqual(url_conditions["match_type"], "simple")
        self.assertEqual(
            url_conditions["value"],
            experiment_create["edit_url"]
        )
        self.assertEqual(
            response_data["edit_url"],
            experiment_create["edit_url"]
        )
        self.assertEqual(response_data["percentage_included"], 10000)
        self.assertEqual(response_data["activation_mode"], "immediate")
        self.assertEqual(response_data["experiment_type"], "ab")

    @patch('api.optimizely_api.utils.http.send_request')
    def test_read_experiment(self, utils_http):
        # create experiment
        experiment_create = self.optimizely_aux.response_experiment_create(
            project_id=1,
            description="video_id_video_title",
            edit_url="https://www.neon-lab.com/videos/"
        )

        # read experiment
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(experiment_create))
        )

        response = self.optimizely_api.read_experiment(
            experiment_id=experiment_create["id"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], experiment_create["id"])

    @patch('api.optimizely_api.utils.http.send_request')
    def test_update_experiment(self, utils_http):
        # create experiment
        experiment_create = self.optimizely_aux.response_experiment_create(
            project_id=1,
            description="video_id_video_title",
            edit_url="https://www.neon-lab.com/videos/"
        )

        # update experiment
        experiment_update = self.optimizely_aux.response_experiment_update(
            experiment_id=experiment_create["id"],
            status="Paused"
        )

        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            202,
            buffer=StringIO(json.dumps(experiment_update))
        )

        response = self.optimizely_api.update_experiment(
            experiment_id=experiment_update["id"],
            status=experiment_update["status"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["status"], experiment_update["status"])

    @patch('api.optimizely_api.utils.http.send_request')
    def test_delete_experiment(self, utils_http):
        # create experiment
        experiment_create = self.optimizely_aux.response_experiment_create(
            project_id=1,
            description="video_id_video_title",
            edit_url="https://www.neon-lab.com/videos/"
        )

        # delete experiment
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            204,
            buffer=None
        )

        response = self.optimizely_api.delete_experiment(
            experiment_id=experiment_create["id"]
        )
        self.assertEqual(response["status_code"], 204)

    @patch('api.optimizely_api.utils.http.send_request')
    def test_get_experiments(self, utils_http):
        # create experiments
        project_id = 1
        for x in range(0, 3):
            self.optimizely_aux.response_experiment_create(
                project_id=project_id,
                description="video_id_video_title",
                edit_url="https://www.neon-lab.com/videos/"
            )

        # get list experiments
        experiment_list = self.optimizely_aux.response_experiment_list()
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(experiment_list))
        )

        response = self.optimizely_api.get_experiments(
            project_id=project_id
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

    @patch('api.optimizely_api.utils.http.send_request')
    def test_get_experiment_status(self, utils_http):
        # get list experiment status
        experiment_status = self.optimizely_aux.response_experiment_status()
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(experiment_status))
        )

        response = self.optimizely_api.get_experiments_status(
            experiment_id=1
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

##############################################################################
# Variations
##############################################################################

    @patch('api.optimizely_api.utils.http.send_request')
    def test_create_variation(self, utils_http):
        experiment_id = 1
        # create variation
        variation_create = self.optimizely_aux.response_variation_create(
            project_id=1,
            experiment_id=experiment_id,
            description="Variation #2",
            js_component="$(\".headline\").text(\"New headline\");",
            weight=3333
        )
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            201,
            buffer=StringIO(json.dumps(variation_create))
        )

        response = self.optimizely_api.create_variation(
            experiment_id=variation_create["experiment_id"],
            description=variation_create["description"],
            js_component=variation_create["js_component"],
            weight=variation_create["weight"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["experiment_id"],
            variation_create["experiment_id"]
        )
        self.assertEqual(
            response_data["description"],
            variation_create["description"]
        )
        self.assertEqual(
            response_data["js_component"],
            variation_create["js_component"]
        )
        self.assertEqual(response_data["weight"], variation_create["weight"])
        self.assertEqual(response_data["is_paused"], False)

    @patch('api.optimizely_api.utils.http.send_request')
    def test_read_variation(self, utils_http):
        experiment_id = 1
        # create variation
        variation_create = self.optimizely_aux.response_variation_create(
            project_id=1,
            experiment_id=experiment_id,
            description="Variation #2",
            js_component="$(\".headline\").text(\"New headline\");",
            weight=3333
        )

        # read variation
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(variation_create))
        )

        response = self.optimizely_api.read_variation(
            variation_id=variation_create["id"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], variation_create["id"])

    @patch('api.optimizely_api.utils.http.send_request')
    def test_update_variation(self, utils_http):
        experiment_id = 1
        # create variation
        variation_create = self.optimizely_aux.response_variation_create(
            project_id=1,
            experiment_id=experiment_id,
            description="Variation #2",
            js_component="$(\".headline\").text(\"New headline\");",
            weight=3333
        )

        # update variation
        variation_update = self.optimizely_aux.response_variation_update(
            variation_id=variation_create["id"],
            description="Change name variation #2",
            weight=7777,
            is_paused=True
        )

        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            202,
            buffer=StringIO(json.dumps(variation_update))
        )

        response = self.optimizely_api.update_variation(
            variation_id=variation_update["id"],
            description=variation_update["description"],
            weight=variation_update["weight"],
            is_paused=variation_update["is_paused"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["description"],
            variation_update["description"]
        )
        self.assertEqual(
            response_data["weight"],
            variation_update["weight"]
        )
        self.assertEqual(
            response_data["is_paused"],
            variation_update["is_paused"]
        )

    @patch('api.optimizely_api.utils.http.send_request')
    def test_delete_variation(self, utils_http):
        experiment_id = 1
        # create variation
        variation_create = self.optimizely_aux.response_variation_create(
            project_id=1,
            experiment_id=experiment_id,
            description="Variation #2",
            js_component="$(\".headline\").text(\"New headline\");",
            weight=3333
        )

        # delete variation
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            204,
            buffer=None
        )

        response = self.optimizely_api.delete_variation(
            variation_id=variation_create["id"]
        )
        self.assertEqual(response["status_code"], 204)

    @patch('api.optimizely_api.utils.http.send_request')
    def test_get_variations(self, utils_http):
        # create variations
        experiment_id = 1
        for x in range(0, 3):
            self.optimizely_aux.response_variation_create(
                project_id=1,
                experiment_id=experiment_id,
                description="Variation #%s" % x
            )

        # get list variations
        variation_list = self.optimizely_aux.response_variation_list()
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(variation_list))
        )

        response = self.optimizely_api.get_variations(
            experiment_id=experiment_id
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

##############################################################################
# Goals
##############################################################################

    @patch('api.optimizely_api.utils.http.send_request')
    def test_create_goal(self, utils_http):
        project_id = 1
        # create goal
        goal_create = self.optimizely_aux.response_goal_create(
            project_id=project_id,
            title="Add to images clicks",
            goal_type=0,
            selector="div.video > img",
            target_to_experiments=True,
            experiment_ids=[11231]
        )
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            201,
            buffer=StringIO(json.dumps(goal_create))
        )

        response = self.optimizely_api.create_goal(
            project_id=goal_create["project_id"],
            title=goal_create["title"],
            goal_type=goal_create["goal_type"],
            selector=goal_create["selector"],
            target_to_experiments=goal_create["target_to_experiments"],
            experiment_ids=goal_create["experiment_ids"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 201)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(
            response_data["project_id"],
            goal_create["project_id"]
        )
        self.assertEqual(response_data["title"], goal_create["title"])
        self.assertEqual(response_data["goal_type"], goal_create["goal_type"])
        self.assertEqual(response_data["selector"], goal_create["selector"])
        self.assertEqual(
            response_data["target_to_experiments"],
            goal_create["target_to_experiments"]
        )
        self.assertEqual(
            response_data["experiment_ids"][0],
            goal_create["experiment_ids"][0]
        )

    @patch('api.optimizely_api.utils.http.send_request')
    def test_read_goal(self, utils_http):
        project_id = 1
        # create goal
        goal_create = self.optimizely_aux.response_goal_create(
            project_id=project_id,
            title="Add to images clicks",
            goal_type=0,
            selector="div.video > img",
            target_to_experiments=True,
            experiment_ids=[11231]
        )

        # read goal
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(goal_create))
        )

        response = self.optimizely_api.read_goal(
            goal_id=goal_create["id"]
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["id"], goal_create["id"])

    @patch('api.optimizely_api.utils.http.send_request')
    def test_update_goal(self, utils_http):
        project_id = 1
        # create goal
        goal_create = self.optimizely_aux.response_goal_create(
            project_id=project_id,
            title="Add to images clicks",
            goal_type=0,
            selector="div.video > img",
            target_to_experiments=True,
            experiment_ids=[11231]
        )

        # update goal
        goal_update = self.optimizely_aux.response_goal_update(
            goal_id=goal_create["id"],
            title="Change - Add to images clicks",
            goal_type=1,
            selector="#imageDivid",
            target_to_experiments=False,
            experiment_ids=[]
        )

        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            202,
            buffer=StringIO(json.dumps(goal_update))
        )

        response = self.optimizely_api.update_goal(
            goal_id=goal_update["id"],
            title=goal_update["title"],
            goal_type=goal_update["goal_type"],
            selector=goal_update["selector"],
            target_to_experiments=goal_update["target_to_experiments"],
            experiment_ids=goal_update["experiment_ids"],
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 202)
        self.assertEqual(response["status_string"], "OK")
        self.assertEqual(response_data["title"], goal_update["title"])
        self.assertEqual(response_data["goal_type"], goal_update["goal_type"])
        self.assertEqual(response_data["selector"], goal_update["selector"])
        self.assertEqual(
            response_data["experiment_ids"],
            goal_update["experiment_ids"]
        )

    @patch('api.optimizely_api.utils.http.send_request')
    def test_delete_goal(self, utils_http):
        project_id = 1
        # create goal
        goal_create = self.optimizely_aux.response_goal_create(
            project_id=project_id,
            title="Add to images clicks",
            goal_type=0,
            selector="div.video > img",
            target_to_experiments=True,
            experiment_ids=[11231]
        )

        # delete goal
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            204,
            buffer=None
        )

        response = self.optimizely_api.delete_goal(
            goal_id=goal_create["id"]
        )
        self.assertEqual(response["status_code"], 204)

    @patch('api.optimizely_api.utils.http.send_request')
    def test_get_goals(self, utils_http):
        # create goal
        project_id = 1
        for x in range(0, 3):
            self.optimizely_aux.response_goal_create(
                project_id=project_id,
                title="Add to images clicks %s" % x,
                goal_type=0,
                selector="div.video > img",
                target_to_experiments=True,
                experiment_ids=[11231]
            )

        # get list goals
        goal_list = self.optimizely_aux.response_goal_list()
        utils_http.return_value = HTTPResponse(
            HTTPRequest("https://optimizely"),
            200,
            buffer=StringIO(json.dumps(goal_list))
        )

        response = self.optimizely_api.get_goals(
            project_id=project_id
        )
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

if __name__ == "__main__":
    utils.neon.InitNeon()
    unittest.main()
