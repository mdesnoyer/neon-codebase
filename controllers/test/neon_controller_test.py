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
import tornado.gen
import datetime
import controllers.neon_controller as neon_controller
import test_utils.neon_controller_aux as neon_controller_aux
from cmsdb import neondata
from mock import patch
from tornado.httpclient import HTTPRequest, HTTPResponse
from StringIO import StringIO
_log = logging.getLogger(__name__)

# Global Constants
OPTMIZELY_ENDPOINT = "https://test.optimizely.com"


class TestControllerOptimizely(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.a_id = "77trcufrh25ztyru4gx7eq95"
        self.i_id = "0"
        self.access_token = "5851ee8c6358f0d46850dd60fe3d17e5:aff12cb2"

        self.controller_type = neon_controller.ControllerType.OPTIMIZELY
        self.project_name = "neon-lab"
        self.project_id = 101010
        self.experiment_id = 2889571145
        self.goal_id = 2930210622
        self.video_id = self.a_id + "_99987212"

        # Optimizely Auxiliator and Responses data pre created
        self.optimizely_api_aux = neon_controller_aux.OptimizelyApiAux()
        self.generate_data_for_optimizely()

        # variable of update_success's control
        self.n_var_update_calls = 0
        self.n_var_update_success_calls = 0

        super(TestControllerOptimizely, self).setUp()

    def tearDown(self):
        self.redis.stop()
        super(TestControllerOptimizely, self).tearDown()

    def generate_data_for_optimizely(self):
        pj = self.optimizely_api_aux.project_create(
            project_id=self.project_id, project_name=self.project_name)
        exp = self.optimizely_api_aux.experiment_create(
            experiment_id=self.experiment_id, project_id=pj['id'],
            description="thumb_id",
            edit_url="https://www.neon-lab.com/videos/")
        self.optimizely_api_aux.goal_create(
            goal_id=self.goal_id, project_id=pj['id'],
            title="Video Image Clicks",
            goal_type=0, selector="div.video > img",
            target_to_experiments=True, experiment_ids=[exp['id']])
        self.optimizely_api_aux.goal_create(
            project_id=pj['id'], title="Engagement",
            goal_type=2, selector="", target_to_experiments=True,
            experiment_ids=[exp['id']])

    def create_default_video_controller_meta_data(self):
        vcmd = neondata.VideoControllerMetaData(
            self.a_id,
            self.i_id,
            neon_controller.ControllerType.OPTIMIZELY,
            self.experiment_id,
            self.video_id,
            {
                'goal_id': str(self.goal_id),
                'element_id': '#1',
                'js_component': "$(\"img#1\").attr(\"src\", \"THUMB_URL\")",
                'ovid_to_tid': {}
            },
            0)
        vcmd.save()
        return vcmd

    def generate_fractions_s3_file(self, thumbs_pct=[]):
        directive = {
            "type": "dir",
            "aid": self.a_id,
            "vid": self.video_id,
            "sla": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
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
            directive['fractions'].append(fraction)

        return directive

    def get_item_in_list(self, _list, key, value):
        new_list = [i for i in _list if i[key] == value]
        if len(new_list) == 1:
            return new_list[0]
        elif len(new_list) > 1:
            return new_list
        return None

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
                self.controller_type, self.a_id, self.i_id, self.access_token)
        self.assertTrue("code: 401" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_controller_already_exist(self, mock_verify_account,
                                             mock_http):
        mock_verify_account.return_value = None
        yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # call again
        with self.assertRaises(ValueError) as e:
            yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)
        self.assertEquals("Integration already exists", str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_controller_success(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        self.assertEquals("OptimizelyController", type(controller).__name__)
        self.assertEquals(controller.platform_id, self.i_id)
        self.assertEquals(controller.api_key, self.a_id)
        self.assertEquals(controller.access_token, self.access_token)
        self.assertEquals(controller.key, "optimizely_%s_%s" % (
            self.a_id, self.i_id))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_get_controller_success(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        controller = neon_controller.Controller.get(
                self.controller_type, self.a_id, self.i_id)
        self.assertEquals("OptimizelyController", type(controller).__name__)
        self.assertEquals(controller.platform_id, self.i_id)
        self.assertEquals(controller.api_key, self.a_id)
        self.assertEquals(controller.access_token, self.access_token)
        self.assertEquals(controller.key, "optimizely_%s_%s" % (
            self.a_id, self.i_id))

    ###########################################################################
    # Verify Experiment
    ###########################################################################
    def _side_effect_to_verity_experiment(
                                self, request, callback=None, *args, **kwargs):
        if "experiments" in request.url:
            id = int(request.url.rsplit('/', 1)[1])
            return HTTPResponse(
                HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                buffer=StringIO(json.dumps(
                    self.optimizely_api_aux.experiment_read(
                        experiment_id=id))))
        elif "goals" in request.url:
            id = int(request.url.rsplit('/', 1)[1])
            return HTTPResponse(
                HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                buffer=StringIO(json.dumps(
                    self.optimizely_api_aux.goal_read(
                        goal_id=self.goal_id))))
        elif "www.neon-lab.com" in request.url:
            return HTTPResponse(
                HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                buffer=StringIO(self.html_aux.get_html()))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_experiment_not_found(
                                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 400,
            buffer=StringIO(json.dumps("not found")))

        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                self.i_id, self.experiment_id, self.video_id)
        self.assertTrue("code: 400" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_goal_not_found(self, mock_verify_account,
                                                   mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        def _side_effect(request, callback=None, *args, **kwargs):
            if "experiments" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.experiment_read(
                            self.experiment_id))))
            elif "goals" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 400,
                    buffer=StringIO(json.dumps("not found")))

        mock_http.side_effect = _side_effect
        extras = {'goal_id': None}
        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                self.i_id, self.experiment_id, self.video_id, extras)
        self.assertTrue("code: 400" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_goal_type_wrong(self, mock_verify_account,
                                                    mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        def _side_effect(request, callback=None, *args, **kwargs):
            if "experiments" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.experiment_read(
                            self.experiment_id))))
            elif "goals" in request.url:
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 200,
                    buffer=StringIO(json.dumps(self.get_item_in_list(
                        self.optimizely_api_aux.goal_list(),
                        'goal_type', 2))))

        mock_http.side_effect = _side_effect
        extras = {'goal_id': None}
        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                self.i_id, self.experiment_id, self.video_id, extras)
        self.assertTrue("Invalid goal_id" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_success(
                    self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        experiment_id = self.experiment_id + 1
        self.optimizely_api_aux.experiment_create(
            experiment_id=experiment_id, project_id=self.project_id,
            description="thumb_id",
            edit_url="https://www.neon-lab.com/videos/")

        mock_http.side_effect = self._side_effect_to_verity_experiment
        element_name = "element_id"
        extras = {
            'goal_id': self.goal_id,
            'element_id': '.%s' % element_name,
            'js_component': None}

        self.html_aux = neon_controller_aux.HTMLAux(
            "class_img", element_name)
        response = yield controller.verify_experiment(
            self.i_id, experiment_id, self.video_id, dict(extras))
        self.assertEquals(response['goal_id'], self.goal_id)
        self.assertEquals(response['goal_id'], extras['goal_id'])
        self.assertEquals(response['element_id'], extras['element_id'])
        self.assertEquals(response['experiment_id'], experiment_id)
        self.assertIsNotNone(response['js_component'])

        vcmd = yield tornado.gen.Task(
            neondata.VideoControllerMetaData.get,
            self.a_id, self.video_id)
        vcmd_ctr = vcmd.controllers[0]
        vcmd_ctr_extras = vcmd_ctr['extras']

        self.assertEquals(len(vcmd.controllers), 1)
        self.assertEquals(
            vcmd_ctr['controller_type'],
            neon_controller.ControllerType.OPTIMIZELY)
        self.assertEquals(vcmd_ctr['platform_id'], self.i_id)
        self.assertEquals(vcmd_ctr['video_id'], self.video_id)
        self.assertEquals(vcmd_ctr['last_process_date'], 0)
        self.assertEquals(
            vcmd_ctr['state'],
            neon_controller.ControllerExperimentState.PENDING)
        self.assertEquals(vcmd_ctr['experiment_id'], experiment_id)
        self.assertEquals(vcmd_ctr_extras['goal_id'], extras['goal_id'])
        self.assertEquals(vcmd_ctr_extras['element_id'], extras['element_id'])
        self.assertIsNotNone(vcmd_ctr_extras['js_component'])

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_already_exist(
                    self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        mock_http.side_effect = self._side_effect_to_verity_experiment
        extras = \
            {'goal_id': self.goal_id, 'element_id': '#11', 'js_component': ''}

        # call again
        self.create_default_video_controller_meta_data()
        with self.assertRaises(ValueError) as e:
            yield controller.verify_experiment(
                self.i_id, self.experiment_id, self.video_id, extras)
        self.assertTrue("Experiment already exists" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_verify_experiment_with_append_same_video(
                    self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        extras = \
            {'goal_id': self.goal_id, 'element_id': '#11', 'js_component': ''}
        self.create_default_video_controller_meta_data()
        vd = neondata.VideoControllerMetaData(
            self.a_id, self.i_id, neon_controller.ControllerType.OPTIMIZELY,
            '123456', self.video_id, extras, 0)
        vd.save()

        mock_http.side_effect = self._side_effect_to_verity_experiment
        yield controller.verify_experiment(
            self.i_id, self.experiment_id, self.video_id, extras)

        vmd = yield tornado.gen.Task(
            neondata.VideoControllerMetaData.get,
            self.a_id, self.video_id)
        self.assertEquals(len(vmd.controllers), 2)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_check_element_id_on_page_with_success(self, mock_verify_account,
                                                   mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        mock_http.side_effect = self._side_effect_to_verity_experiment
        url = "https://www.neon-lab.com/videos/"
        element_name = "element_id"

        # 1 - With Element Tag - Image
        element_id = "#%s" % element_name
        self.html_aux = neon_controller_aux.HTMLAux("id_img", element_name)
        _js_component = \
            "$.each($(\"imgSELECTOR\"), function(element) {" \
            "    $(this).attr(\"src\", \"THUMB_URL\");" \
            "}); ".replace('SELECTOR', element_id)

        response = controller.check_element_id_on_page(url, element_id)
        self.assertEquals(response, _js_component)

        # 2 - With Element Tag - Video
        element_id = "#%s" % element_name
        self.html_aux = neon_controller_aux.HTMLAux("id_video", element_name)
        _js_component = \
            "$.each($(\"videoSELECTOR\"), function(element) {" \
            "    $(this).attr(\"poster\", \"THUMB_URL\");" \
            "}); ".replace('SELECTOR', element_id)

        response = controller.check_element_id_on_page(url, element_id)
        self.assertEquals(response, _js_component)

        # 3 - With Element Tag - Div
        element_id = ".%s" % element_name
        self.html_aux = neon_controller_aux.HTMLAux("class_div", element_name)
        _js_component = \
            "$.each($(\"divSELECTOR\"), function(element) {" \
            "    $(this).css({\"background-image\": \"url(THUMB_URL)\"});"\
            "}); ".replace('SELECTOR', element_id)

        response = controller.check_element_id_on_page(url, element_id)
        self.assertEquals(response, _js_component)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_check_element_id_on_page_with_errors(self, mock_verify_account,
                                                  mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        mock_http.side_effect = self._side_effect_to_verity_experiment
        url = "https://www.neon-lab.com/videos/"
        element_name = "element_id"

        # 1 - With Element Not Found
        element_id = "#%s" % element_name
        self.html_aux = neon_controller_aux.HTMLAux("id_img", "id_img")
        with self.assertRaises(ValueError) as e:
            controller.check_element_id_on_page(url, element_id)
        self.assertTrue("could not found the element" in str(e.exception))

        # 2 - Wih Element ID Duplicated
        element_id = "#%s" % element_name
        self.html_aux = neon_controller_aux.HTMLAux(
            "id_duplicate", element_name)
        with self.assertRaises(ValueError) as e:
            controller.check_element_id_on_page(url, element_id)
        self.assertTrue("duplicated" in str(e.exception))

        # 3 - With Tag Not Supported by ID
        element_id = "#%s" % element_name
        self.html_aux = neon_controller_aux.HTMLAux(
            "id_not_supported", element_name)
        with self.assertRaises(ValueError) as e:
            controller.check_element_id_on_page(url, element_id)
        self.assertTrue("tag not supported by element" in str(e.exception))

        # 4 - With Tag Not Supported by CLASS
        element_id = ".%s" % element_name
        self.html_aux = neon_controller_aux.HTMLAux(
            "class_not_supported", element_name)
        with self.assertRaises(ValueError) as e:
            controller.check_element_id_on_page(url, element_id)
        self.assertTrue("tag not supported by element" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_check_element_id_on_page_with_bad_url(self, mock_verify_account,
                                                   mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 400,
            buffer=StringIO("error"))

        url = "https://www.neon-lab.com/videos/"
        element_id = "#12345"

        with self.assertRaises(ValueError) as e:
            controller.check_element_id_on_page(url, element_id)
        self.assertTrue(
            "could not verify optimizely experiment URL. code: 400" in
            str(e.exception))

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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT, method='GET'), 400,
            buffer=StringIO(json.dumps('timeout')))

        vcmd = self.create_default_video_controller_meta_data()
        with self.assertRaises(ValueError) as e:
            yield controller.update_experiment_with_directives(
                vcmd.controllers[0],
                self.generate_fractions_s3_file)
        self.assertTrue("code: 400" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_experiment_with_variations_to_disable(
                                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # 1-With Update, 2-With Update, 3-NoChange - 4-With 404
        for idx, var in enumerate([5000, 4000, 0, 1000]):
            is_paused = True if var == 0 else False
            self.optimizely_api_aux.variation_create(
                project_id=self.project_id, experiment_id=self.experiment_id,
                description="Variation #%s" % idx, is_paused=is_paused,
                js_component='', weight=var)

        def _side_effect(request, callback=None, *args, **kwargs):
            id = 0
            try:
                id = int(request.url.rsplit('/', 1)[1])
            except:
                id = int(request.url.rsplit('/', 2)[1])

            if "variations" in request.url and request.method == 'GET':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.variation_list(
                            experiment_id=id))))
            elif "variations" in request.url and request.method == 'PUT':
                _code = 202
                _buffer = None
                if self.n_var_update_success_calls < 2:
                    self.n_var_update_success_calls += 1
                    rb = json.loads(request.body)
                    _buffer = StringIO(json.dumps(
                        self.optimizely_api_aux.variation_update(
                            variation_id=id, weight=rb['weight'],
                            is_paused=rb['is_paused'])))
                else:
                    _code = 404
                    _buffer = StringIO(json.dumps('not found'))
                    self.optimizely_api_aux.variation_remove(
                        variation_id=id)
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), _code,
                    buffer=_buffer)
            elif "experiments" in request.url and request.method == 'PUT':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 202,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.experiment_update(
                            experiment_id=id, percentage_included=10000,
                            status="Running"))))

        mock_http.side_effect = _side_effect
        vcmd = self.create_default_video_controller_meta_data()
        yield controller.update_experiment_with_directives(
            vcmd.controllers[0], {'fractions': []})

        self.assertEquals(self.n_var_update_success_calls, 2)
        lv = self.optimizely_api_aux.variation_list(
            experiment_id=self.experiment_id)
        for x in lv:
            self.assertEquals(x['weight'], 0)
            self.assertEquals(x['is_paused'], True)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_experiment_with_update_create_and_delete(
                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # 1-With Update, 2-NotChange, 3-Remove,
        # 4-Delete with 404(And Create), 5-Create

        vcmd = self.create_default_video_controller_meta_data()
        vcmd_ovid_to_tid = vcmd.controllers[0]['extras']['ovid_to_tid']
        for idx, var in enumerate([2000, 2000, 1000, 5000]):
            is_paused = True if var == 0 else False
            v = self.optimizely_api_aux.variation_create(
                project_id=self.project_id, experiment_id=self.experiment_id,
                description="Variation #%s" % idx, is_paused=is_paused,
                js_component='', weight=var)
            variation_id = str(v['id'])
            vcmd_ovid_to_tid[variation_id] = "thumb%s" % idx

            if idx == 2:
                self.optimizely_api_aux.variation_remove(idx+1)
        vcmd.save()
        before_variations = len(vcmd_ovid_to_tid)

        # Create S3 fractions file based on variation list by experiment
        fractions_s3_file = self.generate_fractions_s3_file(
            [0.1, 0.2, 0.5, 0.2])

        def _side_effect(request, callback=None, *args, **kwargs):
            id = 0
            try:
                id = int(request.url.rsplit('/', 1)[1])
            except:
                id = int(request.url.rsplit('/', 2)[1])

            if "variations" in request.url and request.method == 'GET':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.variation_list(
                            experiment_id=id))))
            elif "variations" in request.url and request.method == 'PUT':
                _code = 202
                _buffer = None
                self.n_var_update_calls += 1
                if self.n_var_update_calls >= 2:
                    _code = 404
                    _buffer = StringIO(json.dumps('not found'))
                else:
                    self.n_var_update_success_calls += 1
                    rb = json.loads(request.body)
                    _buffer = StringIO(json.dumps(
                        self.optimizely_api_aux.variation_update(
                            variation_id=id, weight=rb['weight'],
                            is_paused=rb['is_paused'])))
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), _code,
                    buffer=_buffer)
            elif "variations" in request.url and request.method == 'POST':
                rb = json.loads(request.body)
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='POST'), 201,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.variation_create(
                            experiment_id=id,
                            weight=rb['weight'], is_paused=rb['is_paused'],
                            js_component=rb['js_component'],
                            description=rb['description']))))
            elif "experiments" in request.url and request.method == 'PUT':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 202,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.experiment_update(
                            experiment_id=id, percentage_included=10000,
                            status="Running"))))

        mock_http.side_effect = _side_effect
        response = yield controller.update_experiment_with_directives(
            vcmd.controllers[0], fractions_s3_file)

        variation_list = self.optimizely_api_aux.variation_list(
            experiment_id=self.experiment_id)
        for v in variation_list:
            variation_id = v['id']
            has_key = str(variation_id) in vcmd_ovid_to_tid

            if variation_id == 1:
                self.assertEquals(v['weight'], 1000)
                self.assertEquals(v['is_paused'], False)
                self.assertEquals(has_key, True)
            elif variation_id == 2:
                self.assertEquals(v['weight'], 2000)
                self.assertEquals(v['is_paused'], False)
                self.assertEquals(has_key, True)
            elif variation_id == 3:
                pass
            elif variation_id == 4:
                self.assertEquals(has_key, False)
            elif variation_id == 5:
                self.assertEquals(v['weight'], 5000)
                self.assertEquals(v['is_paused'], False)
                self.assertEquals(v['description'], 'thumb2')
                self.assertEquals(has_key, True)
            elif variation_id == 6:
                self.assertEquals(v['weight'], 2000)
                self.assertEquals(v['is_paused'], False)
                self.assertEquals(v['description'], 'thumb3')
                self.assertEquals(has_key, True)
        self.assertTrue(str(3) not in vcmd_ovid_to_tid)

        after_variations = len(vcmd_ovid_to_tid)
        self.assertEquals(before_variations, 4)  # before calls
        self.assertEquals(after_variations, 4)  # 2 deletes and 2 inserts
        self.assertEquals(self.n_var_update_success_calls, 1)  # one update
        self.assertEquals(
            response, neon_controller.ControllerExperimentState.INPROGRESS)

        experiment_read = self.optimizely_api_aux.experiment_read(
            experiment_id=self.experiment_id)
        self.assertEquals(experiment_read['percentage_included'], 10000)
        self.assertEquals(experiment_read['status'], 'Running')

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_update_experiment_with_change_state(
                                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        vcmd = self.create_default_video_controller_meta_data()
        vcmd_ovid_to_tid = vcmd.controllers[0]['extras']['ovid_to_tid']
        for idx, var in enumerate([5000, 4000, 0, 1000]):
            is_paused = True if var == 0 else False
            v = self.optimizely_api_aux.variation_create(
                project_id=self.project_id, experiment_id=self.experiment_id,
                description="Variation #%s" % idx, is_paused=is_paused,
                js_component='', weight=var)
            variation_id = str(v['id'])
            vcmd_ovid_to_tid[variation_id] = "thumb%s" % idx
        vcmd.save()

        # Create S3 fractions file based on variation list by experiment
        fractions_s3_file = self.generate_fractions_s3_file(
            [0.0, 1.0, 0.0, 0.0])

        def _side_effect(request, callback=None, *args, **kwargs):
            id = 0
            try:
                id = int(request.url.rsplit('/', 1)[1])
            except:
                id = int(request.url.rsplit('/', 2)[1])

            if "variations" in request.url and request.method == 'GET':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 200,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.variation_list(
                            experiment_id=id))))
            elif "variations" in request.url and request.method == 'PUT':
                rb = json.loads(request.body)
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 202,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.variation_update(
                            variation_id=id, weight=rb['weight'],
                            is_paused=rb['is_paused']))))
            elif "experiments" in request.url and request.method == 'PUT':
                return HTTPResponse(
                    HTTPRequest(OPTMIZELY_ENDPOINT, method='PUT'), 202,
                    buffer=StringIO(json.dumps(
                        self.optimizely_api_aux.experiment_update(
                            experiment_id=id, percentage_included=10000,
                            status="Running"))))

        mock_http.side_effect = _side_effect
        response = yield controller.update_experiment_with_directives(
            vcmd.controllers[0], fractions_s3_file)

        self.assertEquals(
            response, neon_controller.ControllerExperimentState.COMPLETE)

    ###########################################################################
    # Retrieve Experiment Results
    ###########################################################################
    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_retrieve_experiment_results_with_experiment_not_found(
                                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 400,
            buffer=StringIO(json.dumps("not found")))

        vcmd = self.create_default_video_controller_meta_data()
        with self.assertRaises(ValueError) as e:
            controller.retrieve_experiment_results(vcmd.controllers[0])
        self.assertTrue("code: 400" in str(e.exception))

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_retrieve_experiment_results_okay(
                                        self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        vcmd = self.create_default_video_controller_meta_data()
        vcmd_ovid_to_tid = vcmd.controllers[0]['extras']['ovid_to_tid']
        for idx, var in enumerate([5000, 4000, 0, 1000]):
            is_paused = True if var == 0 else False
            v = self.optimizely_api_aux.variation_create(
                project_id=self.project_id, experiment_id=self.experiment_id,
                description="Variation #%s" % idx, is_paused=is_paused,
                js_component='', weight=var)
            if idx != 0:
                variation_id = str(v['id'])
                vcmd_ovid_to_tid[variation_id] = "thumb%s" % idx
        vcmd.save()

        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(
                self.optimizely_api_aux.experiment_status(
                    experiment_id=self.experiment_id))))

        response = controller.retrieve_experiment_results(vcmd.controllers[0])
        self.assertEquals(len(response), 3)

        for s in response:
            has_thumb = False
            for key, value in vcmd_ovid_to_tid.iteritems():
                if value == s['thumb_id']:
                    has_thumb = True
            self.assertEquals(has_thumb, True)
            self.assertGreater(s['visitors'], 0)
            self.assertGreater(s['conversions'], 0)
            self.assertGreater(s['conversion_rate'], 0)

class TestControllerOptimizelyApi(test_utils.neontest.AsyncTestCase):
    # Test cases that do requests to optimizely API
    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.a_id = "77trcufrh25ztyru4gx7eq95"
        self.i_id = "0"
        self.access_token = "5851ee8c6358f0d46850dd60fe3d17e5:aff12cb2"

        self.controller_type = neon_controller.ControllerType.OPTIMIZELY
        self.project_name = "neon-lab"
        self.project_id = 101010
        self.experiment_id = 2889571145

        self.optimizely_api_aux = neon_controller_aux.OptimizelyApiAux()
        super(TestControllerOptimizelyApi, self).setUp()

    def tearDown(self):
        self.redis.stop()
        super(TestControllerOptimizelyApi, self).tearDown()

    ###########################################################################
    # Projects
    ###########################################################################
    def create_new_project_response(self):
        return self.optimizely_api_aux.project_create(
            project_id=self.project_id, project_name=self.project_name)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_project(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

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
                self.controller_type, self.a_id, self.i_id, self.access_token)

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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # update project
        project_create = self.create_new_project_response()
        project_update = self.optimizely_api_aux.project_update(
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # get list projects
        for x in range(0, 3):
            self.create_new_project_response()
        project_list = self.optimizely_api_aux.project_list()
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
        return self.optimizely_api_aux.experiment_create(
            project_id=project_id,
            description="video_id_video_title",
            edit_url="https://www.neon-lab.com/videos/")

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_experiment(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        project_id = self.project_id
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # read experiment
        experiment_create = \
            self.create_new_experiment_response(self.project_id)
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # update experiment
        experiment_create = \
            self.create_new_experiment_response(self.project_id)
        experiment_update = self.optimizely_api_aux.experiment_update(
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # delete experiment
        experiment_create = \
            self.create_new_experiment_response(self.project_id)
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        project_id = self.project_id
        # get list experiments
        for x in range(0, 3):
            self.create_new_experiment_response(project_id)
        experiment_list = self.optimizely_api_aux.experiment_list()
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        pj = self.optimizely_api_aux.project_create(
            project_name=self.project_name)
        exp = self.optimizely_api_aux.experiment_create(
            experiment_id=self.experiment_id, project_id=pj['id'],
            description="thumb_id",
            edit_url="https://www.neon-lab.com/videos/")
        self.optimizely_api_aux.goal_create(
            project_id=pj['id'], title="Video Image Clicks",
            goal_type=0, selector="div.video > img",
            target_to_experiments=True, experiment_ids=[exp['id']])
        for idx, var in enumerate([5000, 4000, 1000]):
            self.optimizely_api_aux.variation_create(
                project_id=pj['id'], experiment_id=self.experiment_id,
                description="Variation #%s" % idx, is_paused=False,
                js_component='', weight=var)

        # get list experiment status
        mock_http.return_value = HTTPResponse(
            HTTPRequest(OPTMIZELY_ENDPOINT), 200,
            buffer=StringIO(json.dumps(
                self.optimizely_api_aux.experiment_status(
                    experiment_id=self.experiment_id))))

        response = controller.get_experiments_status(
            experiment_id=self.experiment_id)
        response_data = response["data"]
        self.assertEqual(response["status_code"], 200)
        self.assertEqual(response["status_string"], "OK")
        assert len(response_data) == 3

    ##########################################################################
    # Variations
    ##########################################################################
    def create_new_variation_response(self, experiment_id):
        return self.optimizely_api_aux.variation_create(
            project_id=self.project_id,
            experiment_id=experiment_id,
            description="Variation #X",
            js_component="$(\".headline\").text(\"New headline\");",
            weight=3333)

    @patch('controllers.neon_controller.utils.http.send_request')
    @patch('controllers.neon_controller.OptimizelyController.verify_account')
    @tornado.testing.gen_test
    def test_create_variation(self, mock_verify_account, mock_http):
        mock_verify_account.return_value = None
        controller = yield neon_controller.Controller.create(
                self.controller_type, self.a_id, self.i_id, self.access_token)

        experiment_id = self.experiment_id
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # read variation
        variation_create = \
            self.create_new_variation_response(self.experiment_id)
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # update variation
        variation_create = \
            self.create_new_variation_response(self.experiment_id)
        variation_update = self.optimizely_api_aux.variation_update(
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # delete variation
        variation_create = \
            self.create_new_variation_response(self.experiment_id)
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        experiment_id = self.experiment_id
        # get list variations
        for x in range(0, 3):
            self.create_new_variation_response(experiment_id)
        variation_list = self.optimizely_api_aux.variation_list()
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
        return self.optimizely_api_aux.goal_create(
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        project_id = self.project_id
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # read goal
        goal_create = self.create_new_goal_response(self.project_id)
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # update goal
        goal_create = self.create_new_goal_response(self.project_id)
        goal_update = self.optimizely_api_aux.goal_update(
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        # delete goal
        goal_create = self.create_new_goal_response(self.project_id)
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
                self.controller_type, self.a_id, self.i_id, self.access_token)

        project_id = self.project_id
        # get list goals
        for x in range(0, 3):
            self.create_new_goal_response(project_id)
        goal_list = self.optimizely_api_aux.goal_list()
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
