'''
Neon Controller Helper Test
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
from datetime import datetime, timedelta
from random import randint
_log = logging.getLogger(__name__)


class HTMLAux:
    def __init__(self, tag, value):
        self.html = self.generate_html().replace(tag, value)

    def generate_html(self):
        return """
        <!DOCTYPE html>
        <html>
        <head><title>The Http Html Parser</title></head>
        <body>
            <div id="page">
                <!-- ID Duplicate -->
                <img id="id_duplicate" src="http://neon-lab.com/images/tmp.png" width="200" height="200"></img>
                <img id="id_duplicate" src="http://neon-lab.com/images/tmp.png" width="200" height="200"></img>

                <!-- TAG Not Supported -->
                <span id="id_not_supported"><p>tag not supported</p></span>
                <div>
                    <img class="class_not_supported" src="http://neon-lab.com/images/tmp.png" width="200" height="200"></img>
                    <input class="class_not_supported" type="text" name="text1">
                </div>

                <!-- Image Tags -->
                <img id="id_img" src="http://neon-lab.com/images/tmp.png" width=200 height="200"></img>
                <img class="class_img" src="http://neon-lab.com/images/tmp.png" width=200 height="200"></img>
                <img class="class_img" src="http://neon-lab.com/images/tmp.png" width=200 height="200"></img>

                <!-- Video Tags -->
                <video id="id_video" controls poster="http://neon-lab.com/images/tmp.png" width="320" height="240">
                   <source src="movie.mp4" type="video/mp4">
                   <source src="movie.ogg" type="video/ogg">
                   Your browser does not support the video tag.
                </video>
                <video class="class_video" controls poster="http://neon-lab.com/images/tmp.png" width="320" height="240">
                   <source src="movie.mp4" type="video/mp4">
                </video>
                <video class="class_video" controls poster="http://neon-lab.com/images/tmp.png" width="320" height="240">
                   <source src="movie.mp4" type="video/mp4">
                </video>

                <!-- Div Tags -->
                <div id="id_div" style="background-image: url('http://neon-lab.com/images/tmp.png'); width: 200px; height: 200px;"></div>
                <div class="class_div" style="background-image: url('http://neon-lab.com/images/tmp.png'); width: 200px; height: 200px;"></div>
                <div class="class_div" style="background-image: url('http://neon-lab.com/images/tmp.png'); width: 200px; height: 200px;"></div>
            </div>
        </body>
        </html>
        """

    def get_html(self):
        return self.html


class OptimizelyApiAux:
    def __init__(self):
        self.project_id = 0
        self.experiment_id = 0
        self.variation_id = 0
        self.goal_id = 0
        self.projects = []
        self.experiments = []
        self.variations = []
        self.goals = []

    def increment_id(self, variable):
        variable += 1
        return variable

    def remove_none_values(self, _dict):
        return dict((k, v) for k, v in _dict.iteritems() if v is not None)

    def get_item_in_list(self, _list, key, value, asArray=None):
        new_list = [i for i in _list if i[key] == value]
        if len(new_list) == 1 and asArray is None:
            return new_list[0]
        elif len(new_list) >= 1:
            return new_list
        return None

    def project_create(
            self, project_id=None, project_name=None,
            project_status=None, include_jquery=None,
            project_javascript=None, enable_force_variation=None,
            exclude_disabled_experiments=None, exclude_names=None,
            ip_anonymization=None, ip_filter=None):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        if project_id is None:
            project_id = self.increment_id(self.project_id)
            self.project_id = project_id

        data = {
            'id': project_id,
            'code_revision': 1,
            'socket_token': 'AAM7hIkAze5LQo32UCpky8VKG9zJPxLn~%s' % project_id,
            'account_id': 2828420149,
            'project_name': project_name,
            'exclude_disabled_experiments': exclude_disabled_experiments if exclude_disabled_experiments is not None else False,
            'ip_filter': ip_filter if ip_filter is not None else "",
            'ip_anonymization': ip_anonymization if ip_filter is not None else False,
            'enable_force_variation': enable_force_variation if enable_force_variation is not None else False,
            'project_status': project_status if project_status is not None else "Active",
            'exclude_names': exclude_names,
            'include_jquery': include_jquery,
            'library': 'jquery-1.6.4-trim',
            'js_file_size': 0,
            'project_javascript': project_javascript,
            'created': now,
            'last_modified': now,
        }

        self.projects.append(data)
        return data

    def project_update(
            self, project_id=None, project_name=None,
            project_status=None, include_jquery=None,
            project_javascript=None, enable_force_variation=None,
            exclude_disabled_experiments=None, exclude_names=None,
            ip_anonymization=None, ip_filter=None):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        data = {
            'project_name': project_name,
            'project_status': project_status,
            'include_jquery': include_jquery,
            'project_javascript': project_javascript,
            'enable_force_variation': enable_force_variation,
            'exclude_disabled_experiments': exclude_disabled_experiments,
            'exclude_names': exclude_names,
            'ip_anonymization': ip_anonymization,
            'ip_filter': ip_filter,
            'last_modified': now
        }
        data = self.remove_none_values(data)

        project = self.get_item_in_list(self.projects, 'id', project_id)
        project.update(data)
        return project

    def project_read(self, project_id):
        return self.get_item_in_list(self.projects, 'id', project_id)

    def project_list(self):
        return self.projects

    def experiment_create(
            self, experiment_id=None, project_id=None, audience_ids=None,
            activation_mode=None, conditional_code=None,
            description=None, edit_url=None, status=None,
            custom_css=None, custom_js=None,
            percentage_included=None, url_conditions=None):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        if experiment_id is None:
            experiment_id = self.increment_id(self.experiment_id)
            self.experiment_id = experiment_id

        data = {
            'id': experiment_id,
            'project_id': project_id,
            'percentage_included': percentage_included if percentage_included is not None else 10000,
            'is_multivariate': False,
            'variation_ids': [2898660203, 2898660204],
            'status': status if status is not None else 'Not started',
            'display_goal_order_lst': ['2911340090'],
            'shareable_results_link': 'https://app.optimizely.com/results?token=AAKr42QAQQik8Yz2IJS839D1Wmr3qIwY&experiment_id=%s' % experiment_id,
            'conditional_code': conditional_code,
            'primary_goal_id': 2911340090,
            'details': '',
            'url_conditions': [
                {'negate': False, 'match_type': 'simple', 'value': edit_url}
            ],
            'description': description,
            'activation_mode': activation_mode if activation_mode is not None else 'immediate',
            'custom_js': custom_js if custom_js is not None else '',
            'custom_css': custom_css if custom_css is not None else '',
            'auto_allocated': False,
            'experiment_type': 'ab',
            'edit_url': edit_url,
            'audience_ids': audience_ids if audience_ids is not None else [],
            'last_modified': now,
            'created': now
        }

        self.experiments.append(data)
        return data

    def experiment_update(
            self, experiment_id=None, audience_ids=None,
            activation_mode=None, conditional_code=None,
            description=None, edit_url=None, status=None,
            custom_css=None, custom_js=None,
            percentage_included=None, url_conditions=None):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        data = {
            'description': description,
            'edit_url': edit_url,
            'audience_ids': audience_ids,
            'activation_mode': activation_mode,
            'conditional_code': conditional_code,
            'status': status,
            'custom_css': custom_css,
            'custom_js': custom_js,
            'percentage_included': percentage_included,
            'url_conditions': url_conditions,
            'last_modified': now
        }
        data = self.remove_none_values(data)

        experiment = self.get_item_in_list(
            self.experiments, 'id', experiment_id)
        experiment.update(data)
        return experiment

    def experiment_read(self, experiment_id):
        return self.get_item_in_list(self.experiments, 'id', experiment_id)

    def experiment_list(self, project_id=None):
        if project_id is not None:
            return self.get_item_in_list(
                self.experiments, 'project_id', project_id, True)
        return self.experiments

    def experiment_status(self, experiment_id):
        project_id = self.experiment_read(
            experiment_id=experiment_id)['project_id']

        data = []
        g_list = self.goal_list(project_id=project_id)
        v_list = self.variation_list(experiment_id=experiment_id)
        begin = datetime.now() - timedelta(days=1)
        end = datetime.now() + timedelta(days=1)
        for variation in v_list:
            for goal in g_list:
                visitors = randint(100, 5000)
                conversions = randint(50, 3000)
                data.append({
                    'variation_id': str(variation['id']),
                    'variation_name': variation['description'],
                    'goal_id': goal['id'],
                    'goal_name': goal['title'],
                    'baseline_id': str(variation['id']),
                    'begin_time': begin.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    'end_time': end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    'visitors': visitors,
                    'conversions': conversions,
                    'conversion_rate': round((float(conversions) / visitors), 2),
                    'status': 'baseline',
                    'improvement': 0,
                    'statistical_significance': 0,
                    'difference': 0,
                    'difference_confidence_interval_min': 0,
                    'difference_confidence_interval_max': 0,
                    'visitors_until_significance': 100,
                    'is_revenue': False
                })
        return data

    def variation_create(
            self, variation_id=None, project_id=None, experiment_id=None,
            description=None, is_paused=None, js_component=None,
            weight=None):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        if variation_id is None:
            variation_id = self.increment_id(self.variation_id)
            self.variation_id = variation_id
        data = {
            'id': variation_id,
            'experiment_id': experiment_id,
            'project_id': project_id,
            'description': description if description is not None else '',
            'weight': weight if weight is not None else 10000,
            'section_id': None,
            'js_component': js_component if js_component is not None else '',
            'is_paused': is_paused if is_paused is not None else False,
            'created': now
        }
        self.variations.append(data)
        return data

    def variation_update(
            self, variation_id=None, description=None,
            is_paused=None, js_component=None,
            weight=None):
        data = {
            "description": description,
            "is_paused": is_paused,
            "js_component": js_component,
            "weight": weight
        }
        data = self.remove_none_values(data)
        variation = self.get_item_in_list(self.variations, 'id', variation_id)
        variation.update(data)
        return variation

    def variation_remove(self, variation_id):
        self.variations[:] = [
            v for v in self.variations
            if v.get('id') != variation_id
        ]
        return True

    def variation_read(self, variation_id):
        return self.get_item_in_list(self.variations, 'id', variation_id)

    def variation_list(self, experiment_id=None):
        if experiment_id is not None:
            return self.get_item_in_list(
                self.variations, 'experiment_id', experiment_id, True)
        return self.variations

    def goal_create(
            self, goal_id=None, project_id=None, title=None, goal_type=None,
            archived=None, description=None, experiment_ids=None,
            selector=None, target_to_experiments=None,
            target_urls=None, target_url_match_types=None,
            urls=None, url_match_types=None, is_editable=None):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        if goal_id is None:
            goal_id = self.increment_id(self.goal_id)
            self.goal_id = goal_id

        data = {
            'id': goal_id,
            'project_id': project_id,
            'goal_type': goal_type,
            'title': title,
            'selector': selector if selector is not None else '',
            'archived': archived if archived is not None else False,
            'target_urls': target_urls if target_urls is not None else [],
            'description': description if description is not None else '',
            'experiment_ids': experiment_ids if experiment_ids is not None else [],
            'event': None,
            'is_editable': is_editable if is_editable is not None else False,
            'target_to_experiments': target_to_experiments if target_to_experiments is not None else True,
            'urls': [],
            'url_match_types': [],
            'target_url_match_types': [],
            'last_modified': now,
            'created': now
        }

        self.goals.append(data)
        return data

    def goal_update(
            self, goal_id=None, title=None, goal_type=None,
            archived=None, description=None, experiment_ids=None,
            selector=None, target_to_experiments=None,
            target_urls=None, target_url_match_types=None,
            urls=None, url_match_types=None, is_editable=None):
        data = {
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
        data = self.remove_none_values(data)

        goal = self.get_item_in_list(self.goals, 'id', goal_id)
        goal.update(data)
        return goal

    def goal_read(self, goal_id):
        return self.get_item_in_list(self.goals, 'id', goal_id)

    def goal_list(self, project_id=None):
        if project_id is not None:
            return self.get_item_in_list(
                self.goals, 'project_id', project_id, True)
        return self.goals
