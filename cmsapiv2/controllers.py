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
from voluptuous import Schema, Required, All, Length, Range, MultipleInvalid, Invalid

import logging
_log = logging.getLogger(__name__)

define("port", default=8084, help="run on the given port", type=int)

HTTP_OK = 200 
HTTP_BAD_REQUEST = 400 
HTTP_NOT_IMPLEMENTED = 501

def parse_args(request):
    args = {} 
    # if we have query_arguments only use them 
    if request.query_arguments is not None: 
        for key, value in request.query_arguments.iteritems():
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

def generate_standard_error(error_msg):
    error_json = {} 
    error_json['error'] = error_msg
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

    def put(self, *args): 
        send_not_implemented_msg(self, 'put') 
 
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete') 

'''*****************************************************************
AccountHandler : class responsible for updating and getting accounts 
   HTTP Verbs  : get, put 
*****************************************************************'''

class AccountHandler(tornado.web.RequestHandler):
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
 
        send_json_response(self, output, code) 

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

    def post(self, *args):
        send_not_implemented_msg(self, 'post') 
 
    def delete(self, *args): 
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
OoyalaIntegrationHandler : class responsible for creating/updating/
                           getting an ooyala integration
   HTTP Verbs            : get, post, update
*********************************************************************'''
class OoyalaIntegrationHandler(tornado.web.RequestHandler): 
    @tornado.gen.coroutine
    def post(self, *args)  
        print 'posting' 
    
    @tornado.gen.coroutine
    def get(self, *args)  
        print 'getting'
 
    @tornado.gen.coroutine
    def update(self, *args)  
        print 'updating' 
    
    @tornado.gen.coroutine
    def delete(self, *args)  
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
BrightcoveIntegrationHandler : class responsible for creating/updating/
                               getting a brightcove integration
   HTTP Verbs                : get, post, update
*********************************************************************'''
class BrightcoveIntegrationHandler(tornado.web.RequestHandler): 
    @tornado.gen.coroutine
    def post(self, *args)  
        print 'posting' 
    
    @tornado.gen.coroutine
    def get(self, *args)  
        print 'getting'
 
    @tornado.gen.coroutine
    def update(self, *args)  
        print 'updating' 
    
    @tornado.gen.coroutine
    def delete(self, *args)  
        send_not_implemented_msg(self, 'delete')

'''*********************************************************************
OptimizelyIntegrationHandler : class responsible for creating/updating/
                               getting an optimizely integration 
   HTTP Verbs                : get, post, update
*********************************************************************'''
class OptimizelyIntegrationHandler(tornado.web.RequestHandler): 
    @tornado.gen.coroutine
    def post(self, *args)  
        print 'posting' 
    
    @tornado.gen.coroutine
    def get(self, *args)  
        print 'getting'
 
    @tornado.gen.coroutine
    def update(self, *args)  
        print 'updating' 
    
    @tornado.gen.coroutine
    def delete(self, *args)  
        send_not_implemented_msg(self, 'delete')

 
'''*********************************************************************
LiveStreamHandler : class responsible for creating a new video job 
   HTTP Verbs     : post
        Notes     : outside of scope of phase 1, future implementation
*********************************************************************'''

class LiveStreamHandler(tornado.web.RequestHandler): 
    @tornado.gen.coroutine 
    def post(self, *args, **kwargs):
        print 'posting a video job' 

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
