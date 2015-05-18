'''
Optimizely API Interface class
Documentation from: http://developers.optimizely.com/rest/
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import json
import utils.http
from utils.http import RequestPool
import tornado.httpclient
import logging
from utils import statemon
_log = logging.getLogger(__name__)

# Monitoring
statemon.define('optimizely_response_failed', int)

# Constants
DEFAULT_BASE_URL = 'https://www.optimizelyapis.com/experiment/v1/'


class OptimizelyApi(object):
    '''
    This class provide methods to use a Optimizely API
    '''

    def __init__(self, access_token, base_url=DEFAULT_BASE_URL):
        self.access_token = access_token
        self.base_url = base_url
        self.http_request_pool = RequestPool(5, 5)

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
        # _log.warn("Make request to Optimizely RESP API")

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

        # _log.warn(response_data)
        return response_data

    def remove_none_values(self, _dict):
        return dict((k, v) for k, v in _dict.iteritems() if v is not None)

##############################################################################
# Projects
##############################################################################

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

##############################################################################
# Experiments
##############################################################################

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

##############################################################################
# Variations
##############################################################################

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

##############################################################################
# Goals
##############################################################################

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
