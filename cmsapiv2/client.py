'''A client to talk to the api v2.

Deals with authentication etc automatically.

Copyright: 2015 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from cmsapiv2.apiv2 import ResponseCode
import logging
import simplejson as json
import tornado.gen
import tornado.httpclient
import urlparse
import utils.http
from utils.options import define, options

define("auth_host", default="auth.neon-lab.com", type=str, 
       help="Authentication hostname")
define("api_host", default="services.neon-lab.com", type=str, 
       help="Api hostname")

_log = logging.getLogger(__name__)

class Client(object):
    '''Use this client to send requests to the CMSAPI V2.

    Handles authentication behind the scenes so you don't have to.
    '''
    def __init__(self, username, password):
        '''Create the client that will connect with the given user/pass.'''
        self.username = username
        self.password = password
        
        self.access_token = None
        self.refresh_token = None

    @tornado.gen.coroutine
    def _authenticate(self):
        while self.access_token is None:
            if self.refresh_token is None:
                request = tornado.httpclient.HTTPRequest(
                    'https://%s/api/v2/authenticate' % options.auth_host,
                    method='POST',
                    headers={'Content-Type' : 'application/json'},
                    body=json.dumps({'username' : self.username,
                                     'password' : self.password}))
            else:
                request = tornado.httpclient.HTTPRequest(
                    'https://%s/api/v2/authenticate' % options.auth_host,
                    method='POST',
                    headers={ 'Authorization' : 
                              'Bearer %s' % self.refresh_token},
                    body='')
            response = yield utils.http.send_request(
                request,
                no_retry_codes=[ResponseCode.HTTP_UNAUTHORIZED],
                async=True)
            if response.error:
                if self.refresh_token is None:
                    # Could not authenticate
                    raise response.error
                else:
                    # Refresh token could be old
                    self.refresh_token = None
                    continue
                
            # Parse the access and refresh tokens
            data = json.loads(response.body)
            self.access_token = data['access_token']
            self.refresh_token = data['refresh_token']
                

    @tornado.gen.coroutine
    def send_request(self, request, cur_try=0, **send_kwargs):
        '''Sends a request to the APIv2

        Inputs:
        request - A tornado.httpclient.HTTPRequest object. Url can either be 
                  complete, or relative to the host (i.e. 
                  /api/v2/dfsdfe/integrations)
        send_kwargs - Same arguments as utils.http.send_request
        '''
        try:
            yield self._authenticate()
        except tornado.httpclient.HTTPError as e:
            response = tornado.httpclient.HTTPResponse(request, e.code,
                                                       error=e)
            raise tornado.gen.Return(response)

        no_retry_codes = send_kwargs.get('no_retry_codes', [])
        no_retry_codes.append(ResponseCode.HTTP_UNAUTHORIZED)
        send_kwargs['no_retry_codes'] = no_retry_codes

        # Adjust the request
        request.headers['Authorization'] = 'Bearer %s' % self.access_token
        parse = urlparse.urlsplit(request.url)
        if not parse.scheme:
            parse = [x for x in parse]
            parse[0] = 'https'
            parse = urlparse.urlsplit(urlparse.urlunsplit(parse))
            parse = [x for x in parse]
            parse[1] = options.api_host
        request.url = urlparse.urlunsplit(parse)
        
        response = yield utils.http.send_request(request, async=True,
                                                 **send_kwargs)
        if response.error:
            if response.error.code == 401 and cur_try == 0:
                # Our token probably timed out
                self.access_token = None
                response = yield self.send_request(request, cur_try=cur_try+1,
                                                   **send_kwargs)

        raise tornado.gen.Return(response)
