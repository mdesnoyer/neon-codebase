import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import abc
import json
import utils.http
from utils.http import RequestPool
import tornado.gen
import tornado.httpclient
import logging
from utils import statemon
from cmsdb import neondata
from cmsdb.neondata import NamespacedStoredObject
_log = logging.getLogger(__name__)

# Monitoring
statemon.define('optimizely_response_failed', int)


class ControllerType(object):
    ''' Controller type enumeration '''
    OPTIMIZELY = "optimizely"


class ControllerExperimentState(object):
    ''' Controller Experiment state enumeration '''
    PENDING = 'pending'
    INPROGRESS = "inprogress"
    COMPLETE = 'complete'


class ControllerBase(NamespacedStoredObject):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def verify_account(self):
        return

    @abc.abstractmethod
    def verify_experiment(self, experiment_id, video_id, extras):
        return

    @abc.abstractmethod
    def update_experiment_with_directives(self, vcmd, directive):
        return

    @abc.abstractmethod
    def retrieve_experiment_results(self, vcmd, experiment_id):
        return


class OptimizelyController(ControllerBase):
    base_url = 'https://www.optimizelyapis.com/experiment/v1/'
    http_request_pool = RequestPool(5, 5)

    def __init__(self, api_key, platform_id='', access_token=''):
        super(OptimizelyController, self).__init__(
            self._generate_subkey(api_key, platform_id))
        self.api_key = api_key
        self.platform_id = platform_id
        self.access_token = access_token

    @classmethod
    def _generate_subkey(self, api_key, platform_id):
        return '_'.join([api_key, platform_id])

    @classmethod
    def get(self, api_key, platform_id, callback=None):
        return super(OptimizelyController, self).get(
            self._generate_subkey(api_key, platform_id), callback=callback)

    @classmethod
    def _baseclass_name(self):
        return self.get_ovp()

    @classmethod
    def get_ovp(self):
        return ControllerType.OPTIMIZELY

    ###########################################################################
    # Abstract Methods
    ###########################################################################

    def verify_account(self):
        # check token
        token_response = self.get_projects()
        if token_response["status_code"] != 200:
            return token_response
        return None

    @tornado.gen.coroutine
    def verify_experiment(self, platform_id, experiment_id, video_id,
                          extras={}):

        # Check if experiment exist in Optimizely
        exp_response = self.read_experiment(experiment_id=experiment_id)
        if exp_response["status_code"] != 200:
            raise ValueError("could not verify %s experiment. code: %s" % (
                self.get_ovp(), exp_response["status_code"]))

        # get primary goal for experiment
        if extras['goal_id'] is None:
            extras['goal_id'] = exp_response['data']['primary_goal_id']

        # Check if goal exist in Optimizely
        goal_response = self.read_goal(goal_id=extras['goal_id'])
        if goal_response["status_code"] != 200:
            raise ValueError("could not verify %s goal. code: %s" % (
                self.get_ovp(), goal_response["status_code"]))

        if goal_response['data']['goal_type'] == 2:
            raise ValueError(
                "Invalid goal_id. goal cannot be of "
                "type engagement. set primary goal in optimizely "
                "or provide a different goal_id")

        # TODO: New implementation (Check element_id in the page)
        # Do a request to URL of experiment and parse HTML to find the element_id
        # Generating the correct js_component to put in variation
        # Raise exception if element in page not found
        edit_url = exp_response['data']['edit_url']

        # formatter js_component
        if extras['js_component'] is None:
            extras['js_component'] = (
                "$.each($(\"imgSELECTOR\"), function(element) {"
                "    $(this).attr(\"src\", \"THUMB_URL\");"
                "}); "
                "$.each($(\"videoSELECTOR\"), function(element) {"
                "    $(this).attr(\"poster\", \"THUMB_URL\");"
                "}); "
                "$.each($(\"divSELECTOR\"), function(element) {"
                "    $(this).css({\"background-image\": \"url(THUMB_URL)\"});"
                "}); "
            ).replace('SELECTOR', extras['element_id'])

        # create or append new experiment
        vd = yield tornado.gen.Task(
            neondata.VideoControllerMetaData.get,
            self.api_key, video_id)

        if vd:
            e_list = [
                i for i in vd.controllers
                if i['experiment_id'] == experiment_id
            ]
            if len(e_list) > 0:
                raise ValueError("Experiment already exists")
                return
            else:
                vd.append_controller(self.get_ovp(), platform_id,
                                     experiment_id, video_id, extras, 0)
        else:
            vd = neondata.VideoControllerMetaData(
                self.api_key, self.platform_id,
                self.get_ovp(), experiment_id, video_id, extras, 0)

        yield tornado.gen.Task(vd.save)

        # return data
        data = {
            'experiment_id': experiment_id,
            'video_id': video_id,
            'goal_id': extras['goal_id'],
            'element_id': extras['element_id'],
            'js_component': extras['js_component']
        }
        raise tornado.gen.Return(data)

    @tornado.gen.coroutine
    def update_experiment_with_directives(self, vcmd, directive):
        state = ControllerExperimentState.INPROGRESS
        experiment_id = vcmd['experiment_id']

        # retrieve list of variations in experiment optimizely
        v_list_resp = self.get_variations(experiment_id=experiment_id)
        if v_list_resp["status_code"] != 200:
            raise ValueError("could not verify %s variation. code: %s" % (
                self.get_ovp(), v_list_resp["status_code"]))

        # iterate optimizely variations to "disable" variation not used
        for var in v_list_resp['data']:
            if str(var['id']) in vcmd['extras']['ovid_to_tid']:
                continue

            # Variation exists in optimizely BUT
            # there is not the thumbnail of the video on neon
            if var['weight'] == 0 and var['is_paused'] == True:
                continue

            # Update the variation to set weight = 0
            response = self.update_variation(variation_id=var['id'], weight=0,
                                             is_paused=True)
            if response["status_code"] == 404:
                continue
            elif response["status_code"] != 202:
                raise ValueError(
                    "could not update %s variation. code: %s" % (
                        self.get_ovp(), response["status_code"]))

        # iterate fractions to create/update variation in optimizely
        for thumb in directive['fractions']:
            var = None
            variation_id = None
            frac_calc = int(10000 * thumb['pct'])

            # Get the variation key by thumbnail id
            for key, value in vcmd['extras']['ovid_to_tid'].iteritems():
                if value == thumb['tid']:
                    variation_id = int(key)

            # Update or Remove a variation
            if variation_id is not None:
                var = self.get_value_list(v_list_resp['data'], 'id',
                                          variation_id)

                # Variation not found. Assuming Delete
                if var is None:
                    vcmd['extras']['ovid_to_tid'].pop(str(variation_id), None)
                else:
                    # Update variation
                    if var['weight'] == frac_calc:
                        continue
                    response = self.update_variation(
                        variation_id=var['id'], weight=frac_calc,
                        is_paused=False)

                    if response["status_code"] == 404:
                        # Optimizely BUG: If a variation has been deleted.
                        # I still get the variation when call the variations list.
                        # Any access to variation by ID. I receive 404.
                        vcmd['extras']['ovid_to_tid'].pop(
                            str(variation_id), None)
                        var = None
                    elif response["status_code"] != 202:
                        raise ValueError(
                            "could not update %s variation. code: %s" % (
                                self.get_ovp(), response["status_code"]))

            if var is None:
                # Create new variation
                js_component = vcmd['extras']['js_component']
                vcmd['extras']['js_component'] = js_component.replace(
                    "THUMB_URL", thumb['imgs'][0]['url'])

                response = self.create_variation(
                    experiment_id=experiment_id, weight=frac_calc,
                    js_component=vcmd['extras']['js_component'],
                    is_paused=False, description=thumb['tid'])

                if response["status_code"] != 201:
                    raise ValueError(
                        "could not create %s variation. code: %s" % (
                            self.get_ovp(), response["status_code"]))

                variation_id = response['data']['id']
                vcmd['extras']['ovid_to_tid'][variation_id] = thumb['tid']

        # Update experiment - Start
        exp_resp = self.update_experiment(experiment_id=experiment_id,
                                          percentage_included=10000,
                                          status="Running")
        if exp_resp["status_code"] != 202:
            raise ValueError(
                "could not update %s experiment. code: %s" % (
                    self.get_ovp(), exp_resp["status_code"]))

        # check experiment is completed
        is_done = self.get_value_list(directive['fractions'], 'pct', '1.0')
        if is_done is not None:
            state = ControllerExperimentState.COMPLETE

        raise tornado.gen.Return(state)

    def retrieve_experiment_results(self, vcmd):
        experiment_id = vcmd['experiment_id']
        exp_status = self.get_experiments_status(experiment_id=experiment_id)
        if exp_status["status_code"] != 200:
            raise ValueError(
                "could not verify %s experiment status. code: %s" % (
                    self.get_ovp(), exp_status["status_code"]))

        data = []
        for exp in exp_status['data']:
            variation_id = str(exp['variation_id'])
            goal_id = str(exp['goal_id'])

            # I do not know this variation
            if variation_id not in vcmd['extras']['ovid_to_tid']:
                continue
            # I do not want this goal
            if goal_id != vcmd['extras']['goal_id']:
                continue

            item = {
                'thumb_id': vcmd['extras']['ovid_to_tid'][variation_id],
                'visitors': exp['visitors'],
                'conversions': exp['conversions'],
                'conversion_rate': exp['conversion_rate']
            }
            data.append(item)
        return data

    ###########################################################################
    # API Methods
    # Documentation from: http://developers.optimizely.com/rest/
    ###########################################################################

    def create_response(self, code=200, string="", data={}):
        response_data = {
            u"status_code": code,
            u"status_string": string,
            u"data": data
        }
        return response_data

    def send_request(self, relative_path, http_method='GET', body=None):
        '''
        Make request to Optimizely RESP API
        '''
        client_url = self.base_url + relative_path
        headers = tornado.httputil.HTTPHeaders({
            "Token": self.access_token,
            "Content-Type": "application/json"
        })
        request_body = tornado.escape.json_encode(body)
        if (http_method == 'GET') or (http_method == 'DELETE'):
            request_body = None

        request = tornado.httpclient.HTTPRequest(
            url=client_url,
            method=http_method,
            headers=headers,
            body=request_body,
            request_timeout=60.0,
            connect_timeout=5.0
        )

        response = self.http_request_pool.send_request(request)
        response_data = self.create_response(code=response.code)

        if response.error:
            _log.error("Failed to access Optimizely API: %s" % response.error)
            response_data["status_string"] = response.body
            statemon.state.increment('optimizely_response_failed')
        else:
            try:
                response_body = tornado.escape.json_decode(response.body)
                response_data["data"] = response_body
            except Exception:
                pass
            response_data["status_string"] = "OK"

        return response_data

    def get_projects(self):
        '''
        Get a list of all the projects in your acct, with associated metadata.
        '''
        return self.send_request("projects")

    def create_project(self, project_name=None,
                       project_status=None, include_jquery=None,
                       project_javascript=None, enable_force_variation=None,
                       exclude_disabled_experiments=None, exclude_names=None,
                       ip_anonymization=None, ip_filter=None):
        '''
        Create a new project in your account.
        The project_name is required in the request.
        '''
        request_body = {
            "project_name": project_name,
            "project_status": project_status,
            "include_jquery": include_jquery,
            "project_javascript": project_javascript,
            "enable_force_variation": enable_force_variation,
            "exclude_disabled_experiments": exclude_disabled_experiments,
            "exclude_names": exclude_names,
            "ip_anonymization": ip_anonymization,
            "ip_filter": ip_filter
        }
        request_body = self.remove_none_values(request_body)
        return self.send_request("projects", "POST", request_body)

    def update_project(self, project_id=None, project_name=None,
                       project_status=None, include_jquery=None,
                       project_javascript=None, enable_force_variation=None,
                       exclude_disabled_experiments=None, exclude_names=None,
                       ip_anonymization=None, ip_filter=None):
        '''
        Update an project.
        '''
        request_body = {
            "project_name": project_name,
            "project_status": project_status,
            "include_jquery": include_jquery,
            "project_javascript": project_javascript,
            "enable_force_variation": enable_force_variation,
            "exclude_disabled_experiments": exclude_disabled_experiments,
            "exclude_names": exclude_names,
            "ip_anonymization": ip_anonymization,
            "ip_filter": ip_filter
        }
        request_body = self.remove_none_values(request_body)
        relative_path = "projects/%s" % project_id
        return self.send_request(relative_path, "PUT", request_body)

    def read_project(self, project_id):
        '''
        Get metadata for a single project.
        '''
        relative_path = "projects/%s" % project_id
        return self.send_request(relative_path)

    def delete_project(self, project_id):
        raise NotImplementedError("Deleting projects is not supported.")

    def get_experiments(self, project_id):
        '''
        Get a list of all the experiments in a project.
        '''
        relative_path = "projects/%s/experiments" % project_id
        return self.send_request(relative_path)

    def create_experiment(self, project_id=None, audience_ids=None,
                          activation_mode=None, conditional_code=None,
                          description=None, edit_url=None, status=None,
                          custom_css=None, custom_js=None,
                          percentage_included=None, url_conditions=None):
        '''
        Create an experiment.
        The project_id is required in the URL, and the description
        and edit_url are required in the body.
        '''
        request_body = {
            "description": description,
            "edit_url": edit_url,
            "audience_ids": audience_ids,
            "activation_mode": activation_mode,
            "conditional_code": conditional_code,
            "status": status,
            "custom_css": custom_css,
            "custom_js": custom_js,
            "percentage_included": percentage_included,
            "url_conditions": url_conditions
        }
        request_body = self.remove_none_values(request_body)
        relative_path = "projects/%s/experiments" % project_id
        return self.send_request(relative_path, "POST", request_body)

    def update_experiment(self, experiment_id=None, audience_ids=None,
                          activation_mode=None, conditional_code=None,
                          description=None, edit_url=None, status=None,
                          custom_css=None, custom_js=None,
                          percentage_included=None, url_conditions=None):
        '''
        Update an experiment.
        '''
        request_body = {
            "description": description,
            "edit_url": edit_url,
            "audience_ids": audience_ids,
            "activation_mode": activation_mode,
            "conditional_code": conditional_code,
            "status": status,
            "custom_css": custom_css,
            "custom_js": custom_js,
            "percentage_included": percentage_included,
            "url_conditions": url_conditions
        }
        request_body = self.remove_none_values(request_body)
        relative_path = "experiments/%s" % experiment_id
        return self.send_request(relative_path, "PUT", request_body)

    def read_experiment(self, experiment_id):
        '''
        Get metadata for a single experiment.
        '''
        relative_path = "experiments/%s" % experiment_id
        return self.send_request(relative_path)

    def delete_experiment(self, experiment_id):
        '''
        Delete an experiment.
        '''
        relative_path = "experiments/%s" % experiment_id
        return self.send_request(relative_path, "DELETE")

    def get_experiments_status(self, experiment_id):
        '''
        Use the results endpoint to get the top-level results of an experiment,
        including number of visitors, number of conversions,
        and chance to beat baseline for each variation
        '''
        relative_path = "experiments/%s/stats" % experiment_id
        return self.send_request(relative_path)

    def get_variations(self, experiment_id):
        '''
        List all variations associated with the experiment.
        '''
        relative_path = "experiments/%s/variations" % experiment_id
        return self.send_request(relative_path)

    def create_variation(self, experiment_id=None, description=None,
                         is_paused=None, js_component=None,
                         weight=None):
        '''
        Create a new variation.
        The experiment_id is required in the URL,
        and the description is required in the body.
        Most variations will also want to include js_component,
        but an Original can use the default value of an empty string.
        '''
        request_body = {
            "description": description,
            "is_paused": is_paused,
            "js_component": js_component,
            "weight": weight
        }
        request_body = self.remove_none_values(request_body)
        relative_path = "experiments/%s/variations" % experiment_id
        return self.send_request(relative_path, "POST", request_body)

    def update_variation(self, variation_id=None, description=None,
                         is_paused=None, js_component=None,
                         weight=None):
        '''
        Update a variation.
        '''
        request_body = {
            "description": description,
            "is_paused": is_paused,
            "js_component": js_component,
            "weight": weight
        }
        request_body = self.remove_none_values(request_body)
        relative_path = "variations/%s" % variation_id
        return self.send_request(relative_path, "PUT", request_body)

    def read_variation(self, variation_id):
        '''
        Get metadata for a single variation.
        '''
        relative_path = "variations/%s" % variation_id
        return self.send_request(relative_path)

    def delete_variation(self, variation_id):
        '''
        Delete a variation.
        '''
        relative_path = "variations/%s" % variation_id
        return self.send_request(relative_path, "DELETE")

    def get_goals(self, project_id):
        '''
        Get a list of all the goals in a project.
        '''
        relative_path = "projects/%s/goals" % project_id
        return self.send_request(relative_path)

    def create_goal(self, project_id=None, title=None, goal_type=None,
                    archived=None, description=None, experiment_ids=None,
                    selector=None, target_to_experiments=None,
                    target_urls=None, target_url_match_types=None,
                    urls=None, url_match_types=None, is_editable=None):
        '''
        Create a new goal.
        For all goals, the title and goal_type are required.
        For each goal type, other fields are required, see documentation
        goal_type = [
            "0": "Click", "1": "Custom event", "2": "Engagement",
            "3": "Pageviews", "4": "Revenue"
        ]
        '''
        request_body = {
            "title": title,
            "goal_type": goal_type,
            "archived": archived,
            "description": description,
            "experiment_ids": experiment_ids,
            "selector": selector,
            "target_to_experiments": target_to_experiments,
            "target_urls": target_urls,
            "target_url_match_types": target_url_match_types,
            "urls": urls,
            "url_match_types": url_match_types,
            "is_editable": is_editable
        }
        request_body = self.remove_none_values(request_body)
        relative_path = "projects/%s/goals" % project_id
        return self.send_request(relative_path, "POST", request_body)

    def update_goal(self, goal_id=None, title=None, goal_type=None,
                    archived=None, description=None, experiment_ids=None,
                    selector=None, target_to_experiments=None,
                    target_urls=None, target_url_match_types=None,
                    urls=None, url_match_types=None, is_editable=None):
        '''
        Update a goals.
        '''
        request_body = {
            "title": title,
            "goal_type": goal_type,
            "archived": archived,
            "description": description,
            "experiment_ids": experiment_ids,
            "selector": selector,
            "target_to_experiments": target_to_experiments,
            "target_urls": target_urls,
            "target_url_match_types": target_url_match_types,
            "urls": urls,
            "url_match_types": url_match_types,
            "is_editable": is_editable
        }
        request_body = self.remove_none_values(request_body)
        relative_path = "goals/%s" % goal_id
        return self.send_request(relative_path, "PUT", request_body)

    def read_goal(self, goal_id):
        '''
        Get metadata for a single goal.
        '''
        relative_path = "goals/%s" % goal_id
        return self.send_request(relative_path)

    def delete_goal(self, goal_id):
        '''
        Delete a goal.
        '''
        relative_path = "goals/%s" % goal_id
        return self.send_request(relative_path, "DELETE")

    ###########################################################################
    # Helper Methods
    ###########################################################################

    def remove_none_values(self, _dict):
        return dict((k, v) for k, v in _dict.iteritems() if v is not None)

    def get_value_list(self, _list, key, value):
        new_list = [t for t in _list if t[key] == value]
        if len(new_list) > 0:
            return new_list[0]
        return None


class Controller(object):
    controllers = {ControllerType.OPTIMIZELY: OptimizelyController}

    @classmethod
    def get(cls, c_type, api_key, platform_id):
        controller = Controller(c_type, api_key, platform_id)
        return controller.get(api_key, platform_id)

    @classmethod
    @tornado.gen.coroutine
    def create(cls, c_type, api_key, platform_id, access_key):
        controller = Controller.get(c_type, api_key, platform_id)
        if controller:
            raise ValueError("Integration already exists")

        controller = Controller(c_type, api_key, platform_id, access_key)
        token_response = controller.verify_account()
        if token_response is not None:
            raise ValueError("could not verify %s access. code: %s" % (
                c_type, token_response["status_code"]))

        controller.save()
        raise tornado.gen.Return(controller)

    def __new__(cls, c_type, api_key, platform_id, access_key=''):
        return Controller.controllers[c_type](api_key, platform_id, access_key)
