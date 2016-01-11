'''
Abstract definitions for an OVP integration.

Authors: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
from integrations.exceptions import IntegrationError
import json
import logging
import tornado.gen
import utils.http
from utils.options import define, options
from utils import statemon
import utils.sync

statemon.define('job_submission_error', int)
statemon.define('new_job_submitted', int)

define('cmsapi_host', default='services.neon-lab.com',
       help='Host where the cmsapi is')
define('cmsapi_port', default=80, type=int, help='Port where the cmsapi is')

_log = logging.getLogger(__name__)

class OVPError(IntegrationError): pass
class OVPRefIDError(OVPError): pass
class OVPCustomRefIDError(OVPError): pass
class CMSAPIError(IntegrationError): pass

class OVPIntegration(object):
    def __init__(self, account_id, platform):
        self.account_id = account_id # Neon account id
        self.platform = platform # An AbstractPlatform object

    @tornado.gen.coroutine
    def submit_video(self, video_id, video_url,
                     callback_url=None,
                     video_title=None,
                     default_thumbnail=None,
                     external_thumbnail_id=None,
                     custom_data=None,
                     duration=None,
                     publish_date=None):
        '''Submits a single video for processing to the CMSAPI.

        Parameters:
        Same as the video post parameters from the CMSAPI

        Returns:
        json returned by the CMSAPI
        '''
        body = {
            'video_id' : video_id,
            'video_url': video_url,
            'video_title': video_title,
            'default_thumbnail': default_thumbnail,
            'external_thumbnail_id': external_thumbnail_id,
            'callback_url': callback_url,
            'custom_data': custom_data,
            'duration': duration,
            'publish_date': publish_date
            }
        headers = {"X-Neon-API-Key" : self.platform.neon_api_key,
                   "Content-Type" : "application/json"}
        url = ('http://%s:%s/api/v1/accounts/%s/neon_integrations/%s/'
               'create_thumbnail_api_request') % (
                   options.cmsapi_host,
                   options.cmsapi_port,
                   self.account_id,
                   self.platform.integration_id)
        request = tornado.httpclient.HTTPRequest(
            url=url,
            method='POST',
            headers=headers,
            body=json.dumps(body),
            request_timeout=300.0,
            connect_timeout=30.0)

        response = yield tornado.gen.Task(utils.http.send_request, request,
                                          base_delay=4.0, ntries=2)

        if response.code == 409:
            _log.warn('Video %s for account %s already exists' % 
                      (video_id, self.platform.neon_api_key))
            raise tornado.gen.Return(json.loads(response.body))
        elif response.error is not None:
            statemon.state.increment('job_submission_error')
            _log.error('Error submitting video %s: %s' % (video_id,
                                                          response.error))
            raise CMSAPIError('Error submitting video: %s' % response.error)

        _log.info('New video was submitted for account %s video id %s'
                  % (self.platform.neon_api_key, video_id))
        statemon.state.increment('new_job_submitted')
        raise tornado.gen.Return(json.loads(response.body))

    @tornado.gen.coroutine
    def lookup_and_submit_videos(self, ovp_video_ids):
        '''Lookup information about videos and submit them as jobs.

        Returns a dictionary of video_id->job_id/Exception
        '''
        raise NotImplementedError()

    @tornado.gen.coroutine
    def process_publisher_stream(self):
        '''Look at the stream of newest videos in the account and submit
        them if necessary.
        '''
        raise NotImplementedError()

    @tornado.gen.coroutine
    def set_thumbnail(self, internal_video_id, thumb_metadata):
        '''Sets the thumbnail that is returned from the OVP's apis.

        TODO: Scope this function better once it is used. For now,
        it's a placeholder.

        Inputs:
        internal_video_id - The internal video id of the video to modify
        thumb_metadata - The thumbnail metadata of the thumbnail to show
        '''
        raise NotImplementedError()
