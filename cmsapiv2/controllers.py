#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import datetime
import json
import hashlib
import PIL.Image as Image
import logging
import os
import random
import signal
import time

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient

import traceback

import utils.neon
import utils.logs
import utils.http

from StringIO import StringIO
from cmsdb import neondata
from utils.inputsanitizer import InputSanitizer
from utils import statemon
import utils.sync
from utils.options import define, options
from voluptuous import Schema, Required, All, Length, Range, MultipleInvalid, Invalid, Coerce

import logging
_log = logging.getLogger(__name__)
import uuid

define("port", default=8084, help="run on the given port", type=int)

HTTP_OK = 200 
HTTP_BAD_REQUEST = 400 
HTTP_NOT_IMPLEMENTED = 501
INTEGRATION_TYPE_BRIGHTCOVE = 'brightcove' 
INTEGRATION_TYPE_OOYALA = 'ooyala' 
INTEGRATION_TYPE_OPTIMIZELY = 'optimizely' 

def parse_args(request):
    args = {} 
    # if we have query_arguments only use them 
    if request.query_arguments is not None: 
        for key, value in request.query_arguments.iteritems():
            #TODO hack(and it's gonna break on a double value passed in)
            # let's use Coerce inside voluptuous instead of using isdigit
            args[key] = int(value[0]) if value[0].isdigit() else value[0]
    # otherwise let's use what we find in the body
    elif request.body_arguments is not None: 
        for key, value in request.body_arguments.iteritems():
            args[key] = int(value[0]) if value[0].isdigit() else value[0]

    return args

def api_key(request): 
    return self.request.headers.get('X-Neon-API-Key') 

def send_not_implemented_msg(self, verb): 
    send_json_response(self, 
                       generate_standard_error('%s %s' % (verb, 'is not implemented for this endpoint')), 
                       HTTP_NOT_IMPLEMENTED)

def generate_standard_error(error_msg, details=None):
    error_json = {} 
    error_json['error'] = error_msg
    if details is not None: 
        error_json['details'] = details 
    return json.dumps(error_json) 

def send_json_response(request, data, status=200):
    request.set_header("Content-Type", "application/json")
    request.set_status(status)
    request.write(data)
    request.finish()

'''****************************************************************
NewAccountHandler : class responsible for creating a new account
   HTTP Verbs     : post 
****************************************************************'''
class NewAccountHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine 
    def get(self, *args):
        send_not_implemented_msg(self, 'get') 

    @tornado.gen.coroutine 
    def post(self):
        schema = Schema({ 
          Required('customer_name') : All(str, Length(min=1, max=1024)),
          'default_width': All(int, Range(min=1, max=8192)), 
          'default_height': All(int, Range(min=1, max=8192)),
          'default_thumbnail_id': All(str, Length(min=1, max=2048)) 
        })
        try:
            args = parse_args(self.request)
            schema(args) 
            user = neondata.NeonUserAccount(customer_name=args['customer_name'])
            try:
                user.default_size[0] = args['default_width'] 
                user.default_size[1] = args['default_height']
                user.default_thumbnail_id = args['default_thumbnail_id']
            except KeyError as e: 
                pass 
            output = yield tornado.gen.Task(neondata.NeonUserAccount.save, user)
            user = yield tornado.gen.Task(neondata.NeonUserAccount.get, user.neon_api_key)
            output = user.to_json()
            code = HTTP_OK
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST
        
        send_json_response(self, output, code) 

    @tornado.gen.coroutine 
    def put(self, *args): 
        send_not_implemented_msg(self, 'put') 
 
    @tornado.gen.coroutine 
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete') 

'''*****************************************************************
AccountHandler : class responsible for updating and getting accounts 
   HTTP Verbs  : get, put 
*****************************************************************'''

class AccountHandler(tornado.web.RequestHandler):

    '''**********************
    AccountHandler.get
    **********************'''    
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({ 
          Required('account_id') : All(str, Length(min=1, max=256)),
        }) 
        try:
            args = {} 
            args['account_id'] = str(account_id)  
            schema(args) 
            user_account = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
            # we don't want to send back everything, build up object of what we want to send back 
            rv_account = {} 
            rv_account['tracker_account_id'] = user_account.tracker_account_id
            rv_account['account_id'] = user_account.account_id 
            rv_account['staging_tracker_account_id'] = user_account.staging_tracker_account_id 
            rv_account['default_thumbnail_id'] = user_account.default_thumbnail_id 
            rv_account['integrations'] = user_account.integrations
            rv_account['default_size'] = user_account.default_size
            rv_account['created'] = user_account.created 
            rv_account['updated'] = user_account.updated
            output = json.dumps(rv_account)
            code = HTTP_OK  
        except AttributeError as e:  
            output = generate_standard_error('%s %s' % 
                        ('Could not retrieve the account with id:',
                         account_id))
            code = HTTP_BAD_REQUEST 
        except MultipleInvalid as e:  
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST 
 
        send_json_response(self, output, code) 

    '''**********************
    AccountHandler.put
    **********************'''    
    @tornado.gen.coroutine
    def put(self, account_id):
        schema = Schema({ 
          Required('account_id') : All(str, Length(min=1, max=256)),
          'default_width': All(int, Range(min=1, max=8192)), 
          'default_height': All(int, Range(min=1, max=8192)),
          'default_thumbnail_id': All(str, Length(min=1, max=2048)) 
        })
        try:
            args = parse_args(self.request)
            args['account_id'] = str(account_id)
            schema(args)
            acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
            def _update_account(a):
                try: 
                    a.default_size[0] = args['default_width'] 
                    a.default_size[1] = args['default_height']
                    a.default_thumbnail_id = args['default_thumbnail_id']
                except KeyError as e: 
                    pass 
            result = yield tornado.gen.Task(neondata.NeonUserAccount.modify, acct.key, _update_account)
            output = result.to_json()
            code = HTTP_OK 
        except AttributeError as e:  
            output = generate_standard_error('%s %s %s' % 
                        ('Unable to fetch the account with id:',
                         account_id,
                         'can not perform an update.'))
            code = HTTP_BAD_REQUEST
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)

    '''**********************
    AccountHandler.post
    **********************'''    
    @tornado.gen.coroutine 
    def post(self, *args):
        send_not_implemented_msg(self, 'post') 
 
    '''**********************
    AccountHandler.delete
    **********************'''    
    @tornado.gen.coroutine 
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
IntegrationHelper : class responsible for helping the integration 
                    handlers, create the integration, update the 
                    integration, and add strategies to accounts based on 
                    the integration type

Method list: createIntegration, updateIntegration, addExperimentStrategy  
*********************************************************************'''
class IntegrationHelper():
    @staticmethod 
    @tornado.gen.coroutine
    def createIntegration(acct, args, integration_type):
        def _createOoyalaPlatform(p):
            try:
                p.account_id = acct.neon_api_key
                p.partner_code = args['publisher_id'] 
                p.ooyala_api_key = args.get('ooyala_api_key', None)
                p.api_secret = args.get('ooyala_api_secret', None)
                p.auto_update = args.get('autosync', False)
            except KeyError as e: 
                pass 
             
        def _createBrightcovePlatform(p):
            try:
                current_time = time.time()
                p.account_id = acct.neon_api_key
                p.publisher_id = args['publisher_id'] 
                p.read_token = args.get('read_token', None)
                p.write_token = args.get('write_token', None)
                p.callback_url = args.get('callback_url', None)
            except KeyError as e: 
                pass
 
        integration_id = uuid.uuid1().hex
        if integration_type == INTEGRATION_TYPE_OOYALA: 
            platform = yield tornado.gen.Task(neondata.OoyalaPlatform.modify, 
                                              acct.neon_api_key, integration_id, 
                                              _createOoyalaPlatform, 
                                              create_missing=True) 
        elif integration_type == INTEGRATION_TYPE_BRIGHTCOVE:
            platform = yield tornado.gen.Task(neondata.BrightcovePlatform.modify, 
                                              acct.neon_api_key, integration_id, 
                                              _createBrightcovePlatform, 
                                              create_missing=True) 
        
        result = yield tornado.gen.Task(acct.modify, 
                                        acct.neon_api_key, 
                                        lambda p: p.add_platform(platform))
        
        # ensure the platform made it to the database by executing a get
        if integration_type == INTEGRATION_TYPE_OOYALA: 
            platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get,
                                              acct.neon_api_key,
                                              integration_id)
        elif integration_type == INTEGRATION_TYPE_BRIGHTCOVE:
            platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get,
                                              acct.neon_api_key,
                                              integration_id)


        if platform: 
            raise tornado.gen.Return(platform)
        else: 
            raise SaveError('unable to save the integration')

    @staticmethod 
    @tornado.gen.coroutine
    def addStrategy(acct, integration_type): 
         if integration_type == INTEGRATION_TYPE_OOYALA: 
             strategy = neondata.ExperimentStrategy(acct.neon_api_key, only_exp_if_chosen=True) 
         elif integration_type == INTEGRATION_TYPE_BRIGHTCOVE: 
             strategy = neondata.ExperimentStrategy(acct.neon_api_key, only_exp_if_chosen=True)
         result = yield tornado.gen.Task(strategy.save)  
         if result: 
             raise tornado.gen.Return(result)
         else: 
             raise SaveError('unable to save strategy to account')
    
    @staticmethod 
    @tornado.gen.coroutine
    def getIntegration(request, account_id, integration_type): 
        schema = Schema({
          Required('account_id') : All(str, Length(min=1, max=256)),
          Required('integration_id') : All(str, Length(min=1, max=256))
        })
        args = parse_args(request)
        args['account_id'] = str(account_id)
        schema(args)
        integration_id = args['integration_id'] 
        if integration_type == INTEGRATION_TYPE_OOYALA: 
            platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                              args['account_id'], 
                                              integration_id)
        elif integration_type == INTEGRATION_TYPE_BRIGHTCOVE: 
            platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                              args['account_id'], 
                                              args['integration_id'])
        if platform: 
            raise tornado.gen.Return(platform) 
        else: 
            raise GetError('%s %s' % ('unable to find the integration for id:',integration_id))

    @staticmethod 
    @tornado.gen.coroutine
    def addVideoResponses(account): 
        return ""

'''*********************************************************************
OoyalaIntegrationHandler : class responsible for creating/updating/
                           getting an ooyala integration
   HTTP Verbs            : get, post, put
*********************************************************************'''
class OoyalaIntegrationHandler(tornado.web.RequestHandler):
 
    '''**********************
    OoyalaPlatform.post
    **********************'''    
    @tornado.gen.coroutine
    def post(self, account_id): 
        schema = Schema({
          Required('account_id') : All(str, Length(min=1, max=256)),
          Required('publisher_id') : All(Coerce(str), Length(min=1, max=256)),
          'ooyala_api_key': All(str, Length(min=1, max=1024)), 
          'ooyala_api_secret': All(str, Length(min=1, max=1024)), 
          'autosync': All(int, Range(min=0, max=1))
        })
        try: 
            args = parse_args(self.request)
            args['account_id'] = str(account_id)
            schema(args)
            acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
            platform = yield tornado.gen.Task(IntegrationHelper.createIntegration, acct, args, INTEGRATION_TYPE_OOYALA)
            yield tornado.gen.Task(IntegrationHelper.addStrategy, acct, INTEGRATION_TYPE_OOYALA)
            output = platform.to_json()
            code = HTTP_OK
        except SaveError as e: 
            output = generate_standard_error('%s %s %s %s' % 
                        ('Unable to create an integration for account_id :',
                         account_id,
                         'with publisher_id :', 
                         args['publisher_id']), e.msg)
            code = HTTP_BAD_REQUEST
        except AttributeError as e:  
            output = generate_standard_error('%s %s %s %s' % 
                        ('Unable to create an integration for account_id :',
                         account_id,
                         'with publisher_id :', 
                         args['publisher_id']))
            code = HTTP_BAD_REQUEST
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)
           
    '''**********************
    OoyalaPlatform.get
    **********************'''    
    @tornado.gen.coroutine
    def get(self, account_id):
        try: 
            platform = yield tornado.gen.Task(IntegrationHelper.getIntegration, 
                                              self.request,
                                              account_id,  
                                              INTEGRATION_TYPE_OOYALA) 
            output = platform.to_json() 
            code = HTTP_OK
        except GetError as e: 
            output = generate_standard_error('error getting the integration',
                                             e.msg)
            code = HTTP_BAD_REQUEST
        except AttributeError as e:  
            output = generate_standard_error('%s %s' % 
                        ('Unable to find integration for account id :',
                         account_id), 'unknown error')
            code = HTTP_BAD_REQUEST
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)
 
    '''**********************
    OoyalaPlatform.put
    **********************'''    
    @tornado.gen.coroutine
    def put(self, account_id):
        try: 
            schema = Schema({
              Required('account_id') : All(str, Length(min=1, max=256)),
              Required('integration_id') : All(str, Length(min=1, max=256)),
              'ooyala_api_key': All(str, Length(min=1, max=1024)), 
              'ooyala_api_secret': All(str, Length(min=1, max=1024)), 
              'publisher_id': All(str, Length(min=1, max=1024))
            })
            args = parse_args(self.request)
            args['account_id'] = str(account_id)
            schema(args)

            def _update_platform(p):
                try:
                    p.ooyala_api_key = args['ooyala_api_key'] 
                    p.api_secret = args['ooyala_api_secret'] 
                    p.partner_code = args['publisher_id'] 
                except KeyError as e: 
                    pass
 
            result = yield tornado.gen.Task(neondata.OoyalaPlatform.modify, 
                                         args['account_id'], 
                                         args['integration_id'], 
                                         _update_platform)

            ooyala_platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                                     args['account_id'], 
                                                     args['integration_id']) 
            output = ooyala_platform.to_json()
            code = HTTP_OK
             
        except AttributeError as e:  
            output = generate_standard_error('%s %s' % 
                        ('Unable to update integration with id :',
                         integration_id))
            code = HTTP_BAD_REQUEST

        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)
        
    '''**********************
    OoyalaPlatform.delete
    **********************'''    
    @tornado.gen.coroutine
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
BrightcoveIntegrationHandler : class responsible for creating/updating/
                               getting a brightcove integration
   HTTP Verbs                : get, post, put
*********************************************************************'''
class BrightcoveIntegrationHandler(tornado.web.RequestHandler):
 
    '''*********************
    BrightcovePlatform.post 
    *********************'''   
    @tornado.gen.coroutine
    def post(self, account_id):
        schema = Schema({
          Required('account_id') : All(str, Length(min=1, max=256)),
          Required('publisher_id') : All(Coerce(str), Length(min=1, max=256)),
          'read_token': All(str, Length(min=1, max=1024)), 
          'write_token': All(str, Length(min=1, max=1024))
        })
        try: 
            args = parse_args(self.request)
            args['account_id'] = str(account_id)
            schema(args)
            acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
            platform = yield tornado.gen.Task(IntegrationHelper.createIntegration, acct, args, INTEGRATION_TYPE_BRIGHTCOVE)
            yield tornado.gen.Task(IntegrationHelper.addStrategy, acct, INTEGRATION_TYPE_BRIGHTCOVE)
            output = platform.to_json()
            code = HTTP_OK
        except SaveError as e: 
            output = generate_standard_error('%s %s %s %s' % 
                        ('Unable to create an integration for account_id :',
                         account_id,
                         'with publisher_id :', 
                         args['publisher_id']), e.msg)
            code = HTTP_BAD_REQUEST
        except AttributeError as e:  
            output = generate_standard_error('%s %s %s %s' % 
                        ('Unable to create an integration for account_id :',
                         account_id,
                         'with publisher_id :', 
                         args['publisher_id']))
            code = HTTP_BAD_REQUEST
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)

    '''*********************
    BrightcovePlatform.get 
    *********************'''    
    @tornado.gen.coroutine
    def get(self, account_id):  
        try: 
            platform = yield tornado.gen.Task(IntegrationHelper.getIntegration, 
                                              self.request,
                                              account_id,  
                                              INTEGRATION_TYPE_BRIGHTCOVE) 
            output = platform.to_json() 
            code = HTTP_OK
        except GetError as e: 
            output = generate_standard_error('error getting the integration',
                                             e.msg)
            code = HTTP_BAD_REQUEST
        except AttributeError as e:  
            output = generate_standard_error('%s %s' % 
                        ('Unable to find integration for account id :',
                         account_id), 'unknown error')
            code = HTTP_BAD_REQUEST
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)
 
    '''*********************
    BrightcovePlatform.put 
    *********************'''    
    @tornado.gen.coroutine
    def put(self, account_id):
        try:   
            schema = Schema({
              Required('account_id') : All(str, Length(min=1, max=256)),
              Required('integration_id') : All(str, Length(min=1, max=256)),
              'read_token': All(str, Length(min=1, max=1024)), 
              'write_token': All(str, Length(min=1, max=1024)), 
              'publisher_id': All(str, Length(min=1, max=1024))
            })
            args = parse_args(self.request)
            args['account_id'] = account_id = str(account_id)
            integration_id = args['integration_id'] 
            schema(args)

            def _update_platform(p):
                try:
                    p.read_token = args['read_token'] 
                    p.write_token = args['write_token'] 
                    p.publisher_id = args['publisher_id'] 
                except KeyError as e: 
                    pass
 
            result = yield tornado.gen.Task(neondata.BrightcovePlatform.modify, 
                                         account_id, 
                                         integration_id, 
                                         _update_platform)

            platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                              account_id, 
                                              integration_id) 
            output = platform.to_json()
            code = HTTP_OK
        except AttributeError as e:  
            output = generate_standard_error('%s %s' % 
                        ('Unable to update integration with id :',
                         integration_id))
            code = HTTP_BAD_REQUEST

        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)
    
    '''**********************
    BrightcovePlatform.delete 
    **********************'''    
    @tornado.gen.coroutine
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
ThumbnailHandler : class responsible for creating/updating/getting a
                   thumbnail 
HTTP Verbs       : get, post, put
*********************************************************************'''
class ThumbnailHandler(tornado.web.RequestHandler):
 
    '''**********************
    Thumbnail.post 
    **********************'''    
    @tornado.gen.coroutine
    def post(self, *args):
        send_not_implemented_msg(self, 'post')
    
    '''**********************
    Thumbnail.get 
    **********************'''    
    @tornado.gen.coroutine
    def get(self, *args):  
        send_not_implemented_msg(self, 'get')
 
    '''**********************
    Thumbnail.put 
    **********************'''    
    @tornado.gen.coroutine
    def put(self, *args):
        send_not_implemented_msg(self, 'put')

    '''**********************
    Thumbnail.delete 
    **********************'''    
    @tornado.gen.coroutine
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
VideoHelper      : helper class responsible for creating new video jobs 
                   for any and all integration types
*********************************************************************'''
class VideoHelper():
    @staticmethod 
    @tornado.gen.coroutine
    def addVideo(request, account_id):
        schema = Schema({
          Required('account_id') : All(str, Length(min=1, max=256)),
          Required('integration_id') : All(str, Length(min=1, max=256)),
          # TODO we should probably just generate this for them, unless there is 
          # some use case where them sending in the id makes more sense
          Required('external_video_ref') : All(str, Length(min=1, max=512)),
          'video_url': All(str, Length(min=1, max=512)), 
          'callback_url': All(str, Length(min=1, max=512)), 
          'video_title': All(str, Length(min=1, max=256)),
          'default_thumbnail_id': All(str, Length(min=1, max=128)),
          'external_thumbnail_ref': All(str, Length(min=1, max=512))
        })
        args = parse_args(request)
        args['account_id'] = account_id = str(account_id)
        schema(args)
        account_id_api_key = args['account_id'] 
        integration_id = args['integration_id']
 
        user_account = yield tornado.gen.Task(neondata.NeonUserAccount.get, account_id_api_key)
        integration_type = user_account.integrations[integration_id] 
        # TODO investigate the data layer to see if we can get a 
        # top-level platform object that gets rid of having these ifs 
        if integration_type == INTEGRATION_TYPE_BRIGHTCOVE:
            platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          account_id_api_key, 
                                          integration_id)  
        elif integration_type == INTEGRATION_TYPE_OOYALA:
            platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                          account_id_api_key, 
                                          integration_id)  

        result = yield tornado.gen.Task(platform.create_job)
 
        if result: 
            raise tornado.gen.Return(result) 
        else: 
            raise SaveError('unable to add to video, unable to process')

'''*********************************************************************
VideoHandler     : class responsible for creating/updating/getting a
                   video
HTTP Verbs       : get, post, put
*********************************************************************'''
class VideoHandler(tornado.web.RequestHandler):
 
    '''**********************
    Video.post 
    **********************'''    
    @tornado.gen.coroutine
    def post(self, account_id):
        try:  
            video = yield tornado.gen.Task(VideoHelper.addVideo(self.request, account_id)) 
            output = video.to_json() 
            code = HTTP_OK
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST
        except SaveError as e: 
            output = generate_standard_error(e.msg) 
            code = HTTP_BAD_REQUEST
        except AttributeError as e:  
            output = generate_standard_error('%s %s' % 
                        ('Unable to create video request for account id: ',
                         account_id))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)
    
    '''**********************
    Video.get 
    **********************'''    
    @tornado.gen.coroutine
    def get(self, *args):  
        try: 
            schema = Schema({
              Required('account_id') : All(str, Length(min=1, max=256)),
              Required('video_id') : All(str, Length(min=1, max=4096)),
              'fields': All(str, Length(min=1, max=4096))
            })
            args = parse_args(request)
            args['account_id'] = account_id_api_key = str(account_id)
            schema(args)
            
            output_list = []
            video_ids = video_id.split(',')
            for vid in video_ids: 
                internal_video_id = neondata.InternalVideoID.generate(self.api_key,args['video_id'])
                result = yield tornado.gen.Task(neondata.VideoMetadata.get, 
                                                internal_video_id)
                #TODO explode fields and only return what was asked for 
                output_list.append(result)
             
            vid_array['videos'] = output_list
            output = json.dumps(vid_array) 
            code = HTTP_OK

        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)
 
    '''**********************
    Video.put 
    **********************'''    
    @tornado.gen.coroutine
    def put(self, account_id):
        try: 
            schema = Schema({
              Required('account_id') : All(str, Length(min=1, max=256)),
              Required('video_id') : All(str, Length(min=1, max=256)),
              'testing_enabled': All(int, Length(min=0, max=1))
            })
            args = parse_args(request)
            args['account_id'] = account_id_api_key = str(account_id)
            schema(args)

            abtest = bool(args['testing_enabled'])
            internal_video_id = neondata.InternalVideoID.generate(self.api_key,args['video_id']) 
            def _update_video(v): 
                v.testing_enabled = abtest

            result = yield tornado.gen.Task(neondata.VideoMetadata.modify, 
                                            internal_video_id, 
                                            _update_video)
            output = result.to_json() 
            code = HTTP_OK  

        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
            code = HTTP_BAD_REQUEST

        send_json_response(self, output, code)

    '''**********************
    Video.delete 
    **********************'''    
    @tornado.gen.coroutine
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
OptimizelyIntegrationHandler : class responsible for creating/updating/
                               getting an optimizely integration 
HTTP Verbs                   : get, post, put
Notes                        : not yet implemented, likely phase 2
*********************************************************************'''
class OptimizelyIntegrationHandler(tornado.web.RequestHandler): 
    @tornado.gen.coroutine
    def post(self, *args):
        send_not_implemented_msg(self, 'post')
    
    @tornado.gen.coroutine
    def get(self, *args):  
        send_not_implemented_msg(self, 'delete')
 
    @tornado.gen.coroutine
    def put(self, *args):
        send_not_implemented_msg(self, 'put')
    
    @tornado.gen.coroutine
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
LiveStreamHandler : class responsible for creating a new live stream job 
   HTTP Verbs     : post
        Notes     : outside of scope of phase 1, future implementation
*********************************************************************'''
class LiveStreamHandler(tornado.web.RequestHandler): 
    @tornado.gen.coroutine 
    def post(self, *args, **kwargs):
        print 'posting a live stream job' 

'''*********************************************************************
Controller Defined Exceptions 
*********************************************************************'''
class Error(Exception): 
    pass 

class SaveError(Error): 
    def __init__(self, msg): 
        self.msg = msg

class GetError(Error): 
    def __init__(self, msg): 
        self.msg = msg

'''*********************************************************************
Endpoints 
*********************************************************************'''
application = tornado.web.Application([
    (r'/api/v2/accounts/$', NewAccountHandler),
    (r'/api/v2/accounts$', NewAccountHandler),
    (r'/api/v2/accounts/([a-zA-Z0-9]+)$', AccountHandler), 
    (r'/api/v2/accounts/([a-zA-Z0-9]+)/$', AccountHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/ooyala$', OoyalaIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/ooyala/$', OoyalaIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove$', BrightcoveIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove/$', BrightcoveIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/optimizely$', OptimizelyIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/optimizely/$', OptimizelyIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/thumbnails$', ThumbnailHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/thumbnails/$', ThumbnailHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos$', VideoHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/$', VideoHandler),
    (r'/api/v2/([a-zA-Z0-9]+)$', AccountHandler), 
    (r'/api/v2/([a-zA-Z0-9]+)/$', AccountHandler),
    (r'/api/v2/(\d+)/jobs/live_stream', LiveStreamHandler)
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
