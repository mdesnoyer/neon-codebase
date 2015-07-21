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
                       501)

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
    @tornado.gen.coroutine 
    def get(self):
        send_not_implemented_msg(self, 'get') 

    @tornado.gen.coroutine 
    def post(self):
        print 'posting a new account' 

    @tornado.gen.coroutine 
    def put(self): 
        send_not_implemented_msg(self, 'put') 
 
    @tornado.gen.coroutine 
    def delete(self): 
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
            #TODO we need created/updated on neonuseraccount
            rv_account['created'] = str(datetime.datetime.utcnow()) 
            rv_account['updated'] = str(datetime.datetime.utcnow()) 
            output = json.dumps(rv_account) 
        except AttributeError as e:  
            output = generate_standard_error('%s %s' % 
                        ('Could not retrieve the account with id:',
                         account_id))
        except MultipleInvalid as e:  
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
 
        send_json_response(self, output, 200) 

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
        except AttributeError as e:  
            output = generate_standard_error('%s %s %s' % 
                        ('Unable to fetch the account with id:',
                         account_id,
                         'can not perform an update.'))
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))

        send_json_response(self, output, 200)

    @tornado.gen.coroutine 
    def post(self):
        send_not_implemented_msg(self, 'post') 
 
    @tornado.gen.coroutine 
    def delete(self): 
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
    (r'/accounts/$', NewAccountHandler),
    (r'/accounts$', NewAccountHandler),
    (r'/accounts/([a-zA-Z0-9]+)$', AccountHandler), 
    (r'/accounts/([a-zA-Z0-9]+)/$', AccountHandler),
    (r'/([a-zA-Z0-9]+)$', AccountHandler), 
    (r'/([a-zA-Z0-9]+)/$', AccountHandler),
    (r'/(\d+)/jobs/live_stream', LiveStreamHandler)
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
