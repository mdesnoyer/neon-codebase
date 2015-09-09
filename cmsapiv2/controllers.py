#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import traceback
import datetime
import json
import hashlib
import logging
import os
import signal
import time
import ast

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient

import utils.neon
import utils.logs
import utils.http

import dateutil.parser

from cmsdb import neondata
from datetime import datetime
from functools import wraps 
import utils.sync
from utils.options import define, options
from voluptuous import Schema, Required, All, Length, Range, MultipleInvalid, Invalid, Coerce, Any

import logging
_log = logging.getLogger(__name__)
import uuid

define("port", default=8084, help="run on the given port", type=int)
define("video_server", default="50.19.216.114", help="thumbnails.neon api", type=str)
define("video_server_port", default=8081, help="what port the video server is running on", type=int)
define("cmsapiv1_port", default=8083, help="what port apiv1 is running on", type=int)

from utils import statemon
statemon.define('post_account_oks', int) 
statemon.define('put_account_oks', int) 
statemon.define('get_account_oks', int) 

statemon.define('post_ooyala_oks', int)  
statemon.define('put_ooyala_oks', int)  
statemon.define('get_ooyala_oks', int)  

statemon.define('post_brightcove_oks', int)  
statemon.define('put_brightcove_oks', int)  
statemon.define('get_brightcove_oks', int)  

statemon.define('post_thumbnail_oks', int) 
statemon.define('put_thumbnail_oks', int) 
statemon.define('get_thumbnail_oks', int) 

statemon.define('post_video_oks', int) 
statemon.define('put_video_oks', int) 
statemon.define('get_video_oks', int)
_get_video_oks_ref = statemon.state.get_ref('get_video_oks')

statemon.define('invalid_input_errors', int)
_invalid_input_errors_ref = statemon.state.get_ref('invalid_input_errors')
statemon.define('unauthorized_errors', int) 
_unauthorized_errors_ref = statemon.state.get_ref('unauthorized_errors')
statemon.define('not_found_errors', int) 
_not_found_errors_ref = statemon.state.get_ref('not_found_errors')
statemon.define('not_implemented_errors', int)
_not_implemented_errors_ref = statemon.state.get_ref('not_implemented_errors')
statemon.define('already_exists_errors', int)
_already_exists_errors_ref = statemon.state.get_ref('already_exists_errors')

statemon.define('internal_server_errors', int)  
_internal_server_errors_ref = statemon.state.get_ref('internal_server_errors')

class ResponseCode(object): 
    HTTP_OK = 200
    HTTP_ACCEPTED = 202 
    HTTP_BAD_REQUEST = 400
    HTTP_UNAUTHORIZED = 401 
    HTTP_NOT_FOUND = 404 
    HTTP_CONFLICT = 409 
    HTTP_INTERNAL_SERVER_ERROR = 500
    HTTP_NOT_IMPLEMENTED = 501

class APIV2Sender(object): 
    def success(self, data, code=ResponseCode.HTTP_OK):
        self.set_status(code) 
        self.write(data) 
        self.finish()

    def error(self, message, extra_data=None, code=None):
        error_json = {} 
        error_json['message'] = message
        if code: 
            error_json['code'] = code 
        if extra_data: 
            error_json['data'] = extra_data 
        self.write(error_json)
        self.finish() 

class apiv2(object):
    @staticmethod
    def api_key_required(func): 
        def _decorator(request, *args, **kwargs):
            request.set_account_id() 
            verified = request.verify_account()
            if not verified: 
                raise NotAuthorizedError()
            return func(request, *args, **kwargs)
        return wraps(func)(_decorator)
 
class APIV2Handler(tornado.web.RequestHandler, APIV2Sender):
    def initialize(self):
        self.set_header("Content-Type", "application/json")
        self.header_api_key = self.request.headers.get('X-Neon-API-Key')
        self.uri = self.request.uri 

    def set_account_id(request): 
        request.account_id = request.uri.split('/')[3]

    def verify_account(request):
        account = neondata.NeonUserAccount.get(request.account_id)
        if not account: 
            return False
        account_api_key = account.api_v2_key
        if request.header_api_key is None:
            return False
        if account_api_key not in request.header_api_key:
            return False
        return True
        
    def parse_args(self):
        args = {} 
        # if we have query_arguments only use them 
        if len(self.request.query_arguments) > 0: 
            for key, value in self.request.query_arguments.iteritems():
                args[key] = value[0]
        # otherwise let's use what we find in the body, json only
        elif len(self.request.body) > 0: 
            bjson = json.loads(self.request.body) 
            for key, value in bjson.items():
                args[key] = value

        return args

    def write_error(self, status_code, **kwargs):
        def get_exc_message(exception):
            return exception.log_message if \
                hasattr(exception, "log_message") else str(exception)

        self.clear()
        self.set_status(status_code)
        exception = kwargs["exc_info"][1]
        if any(isinstance(exception, c) for c in [Invalid, 
                                                  NotAuthorizedError,
                                                  NotFoundError,  
                                                  NotImplementedError]):
            if isinstance(exception, Invalid):
                statemon.state.increment(ref=_invalid_input_errors_ref,
                                         safe=False)
                self.set_status(ResponseCode.HTTP_BAD_REQUEST)
            if isinstance(exception, NotFoundError):
                statemon.state.increment(ref=_not_found_errors_ref,
                                         safe=False)
                self.set_status(ResponseCode.HTTP_NOT_FOUND)
            if isinstance(exception, NotAuthorizedError):
                statemon.state.increment(ref=_unauthorized_errors_ref,
                                         safe=False)
                self.set_status(ResponseCode.HTTP_UNAUTHORIZED)
            if isinstance(exception, NotImplementedError):
                statemon.state.increment(ref=_not_implemented_errors_ref,
                                         safe=False)
                self.set_status(ResponseCode.HTTP_NOT_IMPLEMENTED)

            self.error(get_exc_message(exception), code=self.get_status())

        elif isinstance(exception, AlreadyExists):
            self.set_status(ResponseCode.HTTP_CONFLICT)
            statemon.state.increment(ref=_already_exists_errors_ref,
                                     safe=False)
            self.error('this item already exists', extra_data=get_exc_message(exception))
 
        elif isinstance(exception, neondata.ThumbDownloadError): 
            self.set_status(ResponseCode.HTTP_BAD_REQUEST)
            self.error('failed to download thumbnail', extra_data=get_exc_message(exception)) 

        else:
            _log.exception(''.join(traceback.format_tb(kwargs['exc_info'][2])))
            statemon.state.increment(ref=_internal_server_errors_ref,
                                     safe=False)
            self.error(message=self._reason,
                extra_data=get_exc_message(exception),
                code=status_code)

    @tornado.gen.coroutine 
    def get(self, *args):
        raise NotImplementedError('get not implemented')  

    __get = get
 
    @tornado.gen.coroutine 
    def post(self, *args):
        raise NotImplementedError('post not implemented')  

    __post = post

    @tornado.gen.coroutine 
    def put(self, *args):
        raise NotImplementedError('put not implemented')  

    __put = put

    @tornado.gen.coroutine 
    def delete(self, *args):
        raise NotImplementedError('delete not implemented')  

    __delete = delete

'''****************************************************************
AccountHelper
****************************************************************'''
class AccountHelper():
    """Class responsible for helping the account handlers."""
    @staticmethod 
    @tornado.gen.coroutine
    def formatUserReturn(user_account):
        # we don't want to send back everything, build up object of what we want to send back 
        rv_account = {} 
        rv_account['tracker_account_id'] = user_account.tracker_account_id
        # this is weird, but neon_api_key is actually the "id" on this table, it's what we 
        # use to get information about the account, so send back api_key (as account_id) 
        rv_account['account_id'] = user_account.neon_api_key 
        rv_account['staging_tracker_account_id'] = user_account.staging_tracker_account_id 
        rv_account['default_thumbnail_id'] = user_account.default_thumbnail_id 
        rv_account['integrations'] = user_account.integrations
        rv_account['default_size'] = user_account.default_size
        rv_account['created'] = user_account.created 
        rv_account['updated'] = user_account.updated
        rv_account['api_key'] = user_account.api_v2_key
        rv_account['customer_name'] = user_account.customer_name
        raise tornado.gen.Return(rv_account)
    
'''****************************************************************
NewAccountHandler
****************************************************************'''
class NewAccountHandler(APIV2Handler):
    """Handles post requests to the account endpoint."""
    @tornado.gen.coroutine 
    def post(self):
        """handles account endpoint post request""" 
        schema = Schema({ 
          Required('customer_name') : Any(str, unicode, Length(min=1, max=1024)),
          'default_width': All(Coerce(int), Range(min=1, max=8192)), 
          'default_height': All(Coerce(int), Range(min=1, max=8192)),
          'default_thumbnail_id': Any(str, unicode, Length(min=1, max=2048)) 
        })
        args = self.parse_args()
        schema(args) 
        user = neondata.NeonUserAccount(uuid.uuid1().hex, customer_name=args['customer_name'])
        user.default_size = list(user.default_size) 
        user.default_size[0] = args.get('default_width', neondata.DefaultSizes.WIDTH)
        user.default_size[1] = args.get('default_height', neondata.DefaultSizes.HEIGHT)
        user.default_size = tuple(user.default_size)
        user.default_thumbnail_id = args.get('default_thumbnail_id', None)

        output = yield tornado.gen.Task(neondata.NeonUserAccount.save, user)
        user = yield tornado.gen.Task(neondata.NeonUserAccount.get, user.neon_api_key)
        user = yield AccountHelper.formatUserReturn(user)
            
        _log.debug(('New Account has been added : name = %s id = %s') 
                   % (user['customer_name'], user['account_id']))
        statemon.state.increment('post_account_oks')
 
        self.success(json.dumps(user))

'''*****************************************************************
AccountHandler 
*****************************************************************'''
class AccountHandler(APIV2Handler):
    """Handles get,put requests to the account endpoint. 
       Gets and updates existing accounts.
    """
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def get(self, account_id):
        """handles account endpoint get request""" 
        schema = Schema({ 
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
        })
 
        args = {} 
        args['account_id'] = account_id = str(account_id)  
        schema(args)

        user_account = yield tornado.gen.Task(neondata.NeonUserAccount.get, account_id)

        if not user_account: 
            raise NotFoundError()
         
        user_account = yield AccountHelper.formatUserReturn(user_account)
        statemon.state.increment('get_account_oks')
        self.success(json.dumps(user_account))
 
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def put(self, account_id):
        """handles account endpoint put request""" 
        schema = Schema({ 
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          'default_width': All(Coerce(int), Range(min=1, max=8192)), 
          'default_height': All(Coerce(int), Range(min=1, max=8192)),
          'default_thumbnail_id': Any(str, unicode, Length(min=1, max=2048)) 
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct_internal = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])

        if not acct_internal: 
            raise NotFoundError() 

        acct_for_return = yield AccountHelper.formatUserReturn(acct_internal)
        def _update_account(a):
            a.default_size = list(a.default_size) 
            a.default_size[0] = int(args.get('default_width', acct_internal.default_size[0]))
            a.default_size[1] = int(args.get('default_height', acct_internal.default_size[1]))
            a.default_size = tuple(a.default_size)
            a.default_thumbnail_id = args.get('default_thumbnail_id', acct_internal.default_thumbnail_id)
 
        result = yield tornado.gen.Task(neondata.NeonUserAccount.modify, acct_internal.key, _update_account)
        statemon.state.increment('put_account_oks')
        self.success(json.dumps(acct_for_return))

'''*********************************************************************
IntegrationHelper 
*********************************************************************'''
class IntegrationHelper():
    """Class responsible for helping the integration handlers."""
    @staticmethod 
    @tornado.gen.coroutine
    def createIntegration(acct, args, integration_type):
        """Creates an integration for any integration type. 
        
        Keyword arguments: 
        acct - a NeonUserAccount object 
        args - the args sent in via the API request 
        integration_type - the type of integration to create 
        """ 
        def _createOoyalaIntegration(p):
            try:
                p.account_id = acct.neon_api_key
                p.partner_code = args['publisher_id'] 
                p.ooyala_api_key = args.get('ooyala_api_key', None)
                p.api_secret = args.get('ooyala_api_secret', None)
            except KeyError as e: 
                pass 
             
        def _createBrightcoveIntegration(p):
            try:
                current_time = time.time()
                p.account_id = acct.neon_api_key
                p.publisher_id = args['publisher_id'] 
                p.read_token = args.get('read_token', p.read_token)
                p.write_token = args.get('write_token', p.write_token)
                p.callback_url = args.get('callback_url', p.callback_url)
                playlist_feed_ids = args.get('playlist_feed_ids', None)
                if playlist_feed_ids: 
                    p.playlist_feed_ids = playlist_feed_ids.split(',')
                p.id_field = args.get('id_field', p.id_field) 
                p.uses_batch_provisioning = bool(int(args.get('uses_batch_provisioning', 
                                                              p.uses_batch_provisioning)))
            except KeyError as e: 
                pass
 
        integration_id = uuid.uuid1().hex
        if integration_type == neondata.IntegrationType.OOYALA: 
            integration = yield tornado.gen.Task(neondata.OoyalaIntegration.modify, 
                                              integration_id, 
                                              _createOoyalaIntegration, 
                                              create_missing=True) 
        elif integration_type == neondata.IntegrationType.BRIGHTCOVE:
            integration = yield tornado.gen.Task(neondata.BrightcoveIntegration.modify, 
                                                 integration_id, 
                                                 _createBrightcoveIntegration, 
                                                 create_missing=True) 
        
        result = yield tornado.gen.Task(acct.modify, 
                                        acct.neon_api_key, 
                                        lambda p: p.add_platform(integration))
        
        # ensure the integration made it to the database by executing a get
        if integration_type == neondata.IntegrationType.OOYALA: 
            integration = yield tornado.gen.Task(neondata.OoyalaIntegration.get,
                                              integration_id)
        elif integration_type == neondata.IntegrationType.BRIGHTCOVE:
            integration = yield tornado.gen.Task(neondata.BrightcoveIntegration.get,
                                              integration_id)


        if integration: 
            raise tornado.gen.Return(integration)
        else: 
            raise SaveError('unable to save the integration')

    @staticmethod 
    @tornado.gen.coroutine
    def getIntegration(integration_id, integration_type): 
        """Gets an integration based on integration_id, account_id, and type.  
        
        Keyword arguments: 
        account_id - the account_id that owns the integration 
        integration_id - the integration_id of the integration we want
        integration_type - the type of integration to create 
        """ 
        if integration_type == neondata.IntegrationType.OOYALA: 
            integration = yield tornado.gen.Task(neondata.OoyalaIntegration.get, 
                                              integration_id)
        elif integration_type == neondata.IntegrationType.BRIGHTCOVE: 
            integration = yield tornado.gen.Task(neondata.BrightcoveIntegration.get, 
                                              integration_id)
        if integration:
            raise tornado.gen.Return(integration) 
        else: 
            raise NotFoundError('%s %s' % ('unable to find the integration for id:',integration_id))

    @staticmethod 
    @tornado.gen.coroutine
    def addVideoResponses(account): 
        return ""

'''*********************************************************************
OoyalaIntegrationHandler
*********************************************************************'''
class OoyalaIntegrationHandler(APIV2Handler):
    """Handles get,put,post requests to the ooyala endpoint within the v2 api."""
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def post(self, account_id):
        """Handles an ooyala endpoint post request 
        
        Keyword arguments:
        """ 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('publisher_id') : All(Coerce(str), Length(min=1, max=256)),
          'ooyala_api_key': Any(str, unicode, Length(min=1, max=1024)), 
          'ooyala_api_secret': Any(str, unicode, Length(min=1, max=1024)), 
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
        integration = yield tornado.gen.Task(IntegrationHelper.createIntegration, acct, args, neondata.IntegrationType.OOYALA)
        statemon.state.increment('post_ooyala_oks')
        self.success(json.dumps(integration.__dict__))
 
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def get(self, account_id):
        """handles an ooyala endpoint get request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id'] 
        integration = yield IntegrationHelper.getIntegration(integration_id, 
                                                    neondata.IntegrationType.OOYALA)

        statemon.state.increment('get_ooyala_oks')
        self.success(json.dumps(integration.__dict__))

    @apiv2.api_key_required
    @tornado.gen.coroutine
    def put(self, account_id):
        """handles an ooyala endpoint put request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
          'ooyala_api_key': Any(str, unicode, Length(min=1, max=1024)), 
          'ooyala_api_secret': Any(str, unicode, Length(min=1, max=1024)), 
          'publisher_id': Any(str, unicode, Length(min=1, max=1024))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id'] 
            
        integration = yield IntegrationHelper.getIntegration(integration_id, 
                                                     neondata.IntegrationType.OOYALA)

        def _update_integration(p):
            p.ooyala_api_key = args.get('ooyala_api_key', integration.ooyala_api_key)
            p.api_secret = args.get('ooyala_api_secret', integration.api_secret)
            p.partner_code = args.get('publisher_id', integration.partner_code)
 
        result = yield tornado.gen.Task(neondata.OoyalaIntegration.modify, 
                                        integration_id, 
                                        _update_integration)

        ooyala_integration = yield IntegrationHelper.getIntegration(integration_id, 
                                                            neondata.IntegrationType.OOYALA)
 
        statemon.state.increment('put_ooyala_oks')
        self.success(json.dumps(ooyala_integration.__dict__))

'''*********************************************************************
BrightcoveIntegrationHandler
*********************************************************************'''
class BrightcoveIntegrationHandler(APIV2Handler):
    """handles all requests to the brightcove endpoint within the v2 API"""  
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a brightcove endpoint post request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('publisher_id') : All(Coerce(str), Length(min=1, max=256)),
          'read_token': Any(str, unicode, Length(min=1, max=512)), 
          'write_token': Any(str, unicode, Length(min=1, max=512)),
          'id_field': Any(str, unicode, Length(min=1, max=32)),
          'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
          'uses_batch_provisioning': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
        integration = yield IntegrationHelper.createIntegration(acct, 
                                                             args, 
                                                             neondata.IntegrationType.BRIGHTCOVE)
        statemon.state.increment('post_brightcove_oks')
        self.success(json.dumps(integration.__dict__))

    @apiv2.api_key_required
    @tornado.gen.coroutine
    def get(self, account_id):  
        """handles a brightcove endpoint get request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id'] 
        integration = yield IntegrationHelper.getIntegration(integration_id,  
                                                       neondata.IntegrationType.BRIGHTCOVE) 
        statemon.state.increment('get_brightcove_oks')
        self.success(json.dumps(integration.__dict__))

    @apiv2.api_key_required
    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a brightcove endpoint put request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
          'read_token': Any(str, unicode, Length(min=1, max=1024)), 
          'write_token': Any(str, unicode, Length(min=1, max=1024)), 
          'publisher_id': Any(str, unicode, Length(min=1, max=512)),
          'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
          'uses_batch_provisioning': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        integration_id = args['integration_id'] 
        schema(args)

        integration = yield IntegrationHelper.getIntegration(integration_id,  
                                                  neondata.IntegrationType.BRIGHTCOVE) 

        def _update_integration(p):
            p.read_token = args.get('read_token', integration.read_token)
            p.write_token = args.get('write_token', integration.write_token)
            p.publisher_id = args.get('publisher_id', integration.publisher_id)
            playlist_feed_ids = args.get('playlist_feed_ids', None)
            if playlist_feed_ids: 
                p.playlist_feed_ids = playlist_feed_ids.split(',')
            p.uses_batch_provisioning = bool(int(args.get('uses_batch_provisioning', 
                                                          integration.uses_batch_provisioning)))
 
        result = yield tornado.gen.Task(neondata.BrightcoveIntegration.modify, 
                                     integration_id, 
                                     _update_integration)

        integration = yield IntegrationHelper.getIntegration(integration_id,  
                                                  neondata.IntegrationType.BRIGHTCOVE) 
 
        statemon.state.increment('put_brightcove_oks')
        self.success(json.dumps(integration.__dict__))

'''*********************************************************************
ThumbnailHandler
*********************************************************************'''
class ThumbnailHandler(APIV2Handler):
    """handles all requests to the thumbnails endpoint within the v2 API"""  
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a thumbnail endpoint post request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_location') : Any(str, unicode, Length(min=1, max=2048))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        video_id = args['video_id'] 
        internal_video_id = neondata.InternalVideoID.generate(account_id_api_key,video_id)

        video = yield tornado.gen.Task(neondata.VideoMetadata.get, internal_video_id)

        current_thumbnails = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many,
                                                    video.thumbnail_ids)
        cdn_key = neondata.CDNHostingMetadataList.create_key(account_id_api_key,
                                                             video.integration_id)
        cdn_metadata = yield tornado.gen.Task(neondata.CDNHostingMetadataList.get,
                                              cdn_key)
        # ranks can be negative 
        min_rank = 1
        for thumb in current_thumbnails:
            if (thumb.type == neondata.ThumbnailType.CUSTOMUPLOAD and
                thumb.rank < min_rank):
                min_rank = thumb.rank
        cur_rank = min_rank - 1
 
        new_thumbnail = neondata.ThumbnailMetadata(None,
                                                   internal_vid=internal_video_id, 
                                                   ttype=neondata.ThumbnailType.CUSTOMUPLOAD, 
                                                   rank=cur_rank)
        # upload image to cdn 
        yield video.download_and_add_thumbnail(new_thumbnail,
                                               args['thumbnail_location'],
                                               cdn_metadata,
                                               async=True)
        #save the thumbnail
        yield tornado.gen.Task(new_thumbnail.save)

        # save the video 
        new_video = yield tornado.gen.Task(neondata.VideoMetadata.modify, 
                                           internal_video_id, 
                                           lambda x: x.thumbnail_ids.append(new_thumbnail.key))

        if new_video: 
            statemon.state.increment('post_thumbnail_oks')
            self.success('{ "message": "thumbnail accepted for processing" }', code=ResponseCode.HTTP_ACCEPTED)  
        else:
            raise SaveError('unable to save thumbnail to video') 

    @apiv2.api_key_required
    @tornado.gen.coroutine
    def put(self, account_id): 
        """handles a thumbnail endpoint put request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_id') : Any(str, unicode, Length(min=1, max=512)),
          'enabled': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        thumbnail_id = args['thumbnail_id'] 
            
        thumbnail = yield tornado.gen.Task(neondata.ThumbnailMetadata.get, 
                                           thumbnail_id)
        def _update_thumbnail(t):
            t.enabled = bool(int(args.get('enabled', thumbnail.enabled)))

        yield tornado.gen.Task(neondata.ThumbnailMetadata.modify, 
                               thumbnail_id, 
                               _update_thumbnail)

        thumbnail = yield tornado.gen.Task(neondata.ThumbnailMetadata.get, 
                                           thumbnail_id)
 
        statemon.state.increment('put_thumbnail_oks')
        self.success(json.dumps(thumbnail.__dict__))

    @apiv2.api_key_required
    @tornado.gen.coroutine
    def get(self, account_id): 
        """handles a thumbnail endpoint get request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_id') : Any(str, unicode, Length(min=1, max=512))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        thumbnail_id = args['thumbnail_id'] 
        thumbnail = yield tornado.gen.Task(neondata.ThumbnailMetadata.get, 
                                           thumbnail_id)
        if not thumbnail: 
            raise NotFoundError('thumbnail does not exist with id = %s' % (thumbnail_id)) 
        statemon.state.increment('get_thumbnail_oks')
        self.success(json.dumps(thumbnail.__dict__))

'''*********************************************************************
VideoHelper  
*********************************************************************'''
class VideoHelper():
    """helper class designed to help the video endpoint handle requests"""  
    @staticmethod 
    @tornado.gen.coroutine 
    def createApiRequest(args, account_id_api_key):
        """creates an API Request object 
        
        Keyword arguments: 
        args -- the args sent to the api endpoint 
        account_id_api_key -- the account_id/api_key
        """  
        user_account = yield tornado.gen.Task(neondata.NeonUserAccount.get, account_id_api_key)
        job_id = uuid.uuid1().hex
        integration_id = args.get('integration_id', None) 

        request = neondata.NeonApiRequest(job_id, api_key=account_id_api_key)
        request.video_id = args['external_video_ref'] 
        if integration_id: 
            request.integration_id = integration_id
        request.video_url = args.get('video_url', None) 
        request.callback_url = args.get('callback_url', None)
        request.video_title = args.get('video_title', None) 
        request.default_thumbnail = args.get('default_thumbnail_url', None) 
        request.external_thumbnail_ref = args.get('thumbnail_ref', None) 
        request.publish_date = args.get('publish_date', None) 
        yield tornado.gen.Task(request.save)

        if request: 
            raise tornado.gen.Return(request) 

    @staticmethod 
    @tornado.gen.coroutine 
    def createVideoAndRequest(args, account_id_api_key):
        """creates Video object and ApiRequest object and 
           sends them back to the caller as a tuple
         
        Keyword arguments: 
        args -- the args sent to the api endpoint 
        account_id_api_key -- the account_id/api_key
        """  
        video_id = args['external_video_ref'] 
        video = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                       neondata.InternalVideoID.generate(account_id_api_key, video_id))
        if video is None:
            # make sure we can download the image before creating requests 
            # create the api_request
            api_request = yield tornado.gen.Task(VideoHelper.createApiRequest, 
                                                 args, 
                                                 account_id_api_key)

            video = neondata.VideoMetadata(neondata.InternalVideoID.generate(account_id_api_key, video_id),
                          video_url=args.get('video_url', None),
                          publish_date=args.get('publish_date', None),
                          duration=float(args.get('duration', 0.0)) or None, 
                          custom_data=args.get('custom_data', None), 
                          i_id=api_request.integration_id,
                          serving_enabled=False)
            
            default_thumbnail_url = args.get('default_thumbnail_url', None)
            if default_thumbnail_url: 
                # save the default thumbnail
                image = yield video.download_image_from_url(default_thumbnail_url)
                thumb = yield video.save_default_thumbnail_with_image(image, default_thumbnail_url,  
                                                      args.get('thumbnail_ref', None), async=True)

            # create the api_request
            api_request = yield tornado.gen.Task(VideoHelper.createApiRequest, 
                                                 args, 
                                                 account_id_api_key)
            # add the job id save the video
            video.job_id = api_request.job_id 
            yield tornado.gen.Task(video.save)
            raise tornado.gen.Return((video,api_request))
        else:
            raise AlreadyExists('job_id=%s' % (video.job_id))
         # TODO (elif) add a reprocess flag to this thing

    @staticmethod 
    @tornado.gen.coroutine
    def getThumbnailsFromIds(tids):
        """gets thumbnailmetadata objects 
         
        Keyword arguments: 
        tids -- a list of tids that needs to be retrieved
        """  
        thumbnails = []
        if tids: 
            thumbnails = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many, 
                                                tids)
            thumbnails = [obj.__dict__ for obj in thumbnails] 

        raise tornado.gen.Return(thumbnails)
     
'''*********************************************************************
VideoHandler 
*********************************************************************'''
class VideoHandler(APIV2Handler):
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a Video endpoint post request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('external_video_ref') : Any(str, unicode, Length(min=1, max=512)),
          'integration_id' : Any(str, unicode, Length(min=1, max=256)),
          'video_url': Any(str, unicode, Length(min=1, max=512)), 
          'callback_url': Any(str, unicode, Length(min=1, max=512)), 
          'video_title': Any(str, unicode, Length(min=1, max=256)),
          'duration': All(Coerce(float), Range(min=0.0, max=86400.0)), 
          'publish_date': All(CustomVoluptuousTypes.Date()), 
          'custom_data': All(CustomVoluptuousTypes.Dictionary()), 
          'default_thumbnail_url': Any(str, unicode, Length(min=1, max=128)),
          'thumbnail_ref': Any(str, unicode, Length(min=1, max=512))
        })

        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
          
        # add the video / request
        video_and_request = yield tornado.gen.Task(VideoHelper.createVideoAndRequest, 
                                                   args, 
                                                   account_id_api_key)
        new_video = video_and_request[0] 
        api_request = video_and_request[1]  
        # modify the video if there is a thumbnail set serving_enabled 
        def _set_serving_enabled(v):
            v.serving_enabled = len(v.thumbnail_ids) > 0
        yield tornado.gen.Task(neondata.VideoMetadata.modify,
                               new_video.key,
                               _set_serving_enabled)
            
        # add the job
        vs_job_url = 'http://%s:%s/job' % (options.video_server, 
                                           options.video_server_port)
        request = tornado.httpclient.HTTPRequest(url=vs_job_url,
                                                 method="POST",
                                                 body=api_request.to_json(),
                                                 request_timeout=30.0,
                                                 connect_timeout=10.0)

        response = utils.http.send_request(request)

        if response: 
            job_info = {} 
            job_info['job_id'] = api_request.job_id
            job_info['video'] = new_video.__dict__
            statemon.state.increment('post_video_oks')
            self.success(json.dumps(job_info), code=ResponseCode.HTTP_ACCEPTED) 
        else:
            raise Exception('unable to communicate with video server', HTTP_INTERNAL_SERVER_ERRROR)
        
    @apiv2.api_key_required
    @tornado.gen.coroutine
    def get(self, account_id):  
        """handles a Video endpoint get request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(CustomVoluptuousTypes.CommaSeparatedList()),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        fields = args.get('fields', None) 
            
        vid_dict = {} 
        output_list = []
        internal_video_ids = [] 
        video_ids = args['video_id'].split(',')
        for v_id in video_ids: 
            internal_video_id = neondata.InternalVideoID.generate(account_id_api_key,v_id)
            internal_video_ids.append(internal_video_id)
 
        videos = yield tornado.gen.Task(neondata.VideoMetadata.get_many, 
                                        internal_video_ids) 
        if videos:  
           new_videos = [] 
           if fields:
               field_set = set(fields.split(','))
               for obj in videos:
                   obj = obj.__dict__
                   new_video = {} 
                   for field in field_set: 
                       if field == 'thumbnails':
                           new_video['thumbnails'] = yield VideoHelper.getThumbnailsFromIds(obj['thumbnail_ids'])
                       elif field in obj: 
                           new_video[field] = obj[field] 
                   if new_video: 
                       new_videos.append(new_video)
           else: 
               new_videos = [obj.__dict__ for obj in videos] 

           vid_dict['videos'] = new_videos
           vid_dict['video_count'] = len(new_videos)

        statemon.state.increment('get_video_oks')
        self.success(json.dumps(vid_dict))

    @apiv2.api_key_required
    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a Video endpoint put request""" 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(str, unicode, Length(min=1, max=256)),
          'testing_enabled': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        abtest = bool(int(args['testing_enabled']))
        internal_video_id = neondata.InternalVideoID.generate(account_id_api_key,args['video_id']) 
        def _update_video(v): 
            v.testing_enabled = abtest
        result = yield tornado.gen.Task(neondata.VideoMetadata.modify, 
                                        internal_video_id, 
                                        _update_video)
        video = yield tornado.gen.Task(neondata.VideoMetadata.get, 
                                       internal_video_id)
        if not video: 
            raise NotFoundError('video does not exist with id: %s' % (args['video_id']))
        
        statemon.state.increment('put_video_oks')
        self.success(json.dumps(video.__dict__))

'''*********************************************************************
HealthCheckHandler 
*********************************************************************'''
class HealthCheckHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self):
        apiv1_url = 'http://localhost:%s/healthcheck' % (options.cmsapiv1_port)
        request = tornado.httpclient.HTTPRequest(url=apiv1_url,
                                                 method="GET",
                                                 request_timeout=4.0)
        response = yield utils.http.send_request(request)
        if response.code is 200: 
            self.success('<html>Server OK</html>') 
        else: 
            raise Exception('unable to get to the v1 api', 
                            ResponseCode.HTTP_INTERNAL_SERVER_ERROR) 

'''*********************************************************************
OptimizelyIntegrationHandler : class responsible for creating/updating/
                               getting an optimizely integration 
HTTP Verbs                   : get, post, put
Notes                        : not yet implemented, likely phase 2
*********************************************************************'''
class OptimizelyIntegrationHandler(tornado.web.RequestHandler):
    def __init__(self): 
        super(OptimizelyIntegrationHandler, self).__init__() 

'''*********************************************************************
LiveStreamHandler : class responsible for creating a new live stream job 
   HTTP Verbs     : post
        Notes     : outside of scope of phase 1, future implementation
*********************************************************************'''
class LiveStreamHandler(tornado.web.RequestHandler): 
    def __init__(self): 
        super(LiveStreamHandler, self).__init__() 

'''*********************************************************************
Controller Defined Exceptions 
*********************************************************************'''
class Error(Exception): 
    pass 

class SaveError(Error): 
    def __init__(self, msg, code=ResponseCode.HTTP_INTERNAL_SERVER_ERROR): 
        self.msg = msg
        self.code = code
 
class NotFoundError(tornado.web.HTTPError): 
    def __init__(self, msg='resource was not found', code=ResponseCode.HTTP_NOT_FOUND): 
        self.msg = self.reason = self.log_message = msg
        self.code = self.status_code = code
 
class NotAuthorizedError(tornado.web.HTTPError): 
    def __init__(self, msg='not authorized', code=ResponseCode.HTTP_UNAUTHORIZED): 
        self.msg = self.reason = self.log_message = msg
        self.code = self.status_code = code

class AlreadyExists(tornado.web.HTTPError): 
    def __init__(self, msg, code=ResponseCode.HTTP_BAD_REQUEST):
        self.msg = self.reason = self.log_message = msg
        self.code = self.status_code = code

'''*********************************************************************
Custom Voluptuous Types
*********************************************************************'''
class CustomVoluptuousTypes(): 
    @staticmethod
    def Date():
        return lambda v: dateutil.parser.parse(v)

    @staticmethod
    def CommaSeparatedList():
        return lambda v: v.split(',')
       
    @staticmethod
    def Dictionary():
        def f(v):
            if isinstance(ast.literal_eval(v), dict): 
                return ast.literal_eval(v)
            else:
                raise Invalid("not a dictionary") 
        return f 

'''*********************************************************************
Endpoints 
*********************************************************************'''
application = tornado.web.Application([
    (r'/healthcheck/?$', HealthCheckHandler),
    (r'/api/v2/accounts/?$', NewAccountHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/ooyala/?$', OoyalaIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove/?$', BrightcoveIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/optimizely/?$', OptimizelyIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/thumbnails/?$', ThumbnailHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/?$', VideoHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/?$', AccountHandler),
    (r'/api/v2/(\d+)/live_stream', LiveStreamHandler)
], gzip=True)

def main():
    global server
    print "API V2 is running" 
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
