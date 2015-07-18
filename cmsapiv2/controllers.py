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
        return args
    # otherwise let's use what we find in the body

def api_key(request): 
    return self.request.headers.get('X-Neon-API-Key') 

def generate_standard_error(error_msg):
    error_json = {} 
    error_json['error'] = error_msg
    return json.dumps(error_json) 

def send_json_response(request, data, status=200):
    request.set_header("Content-Type", "application/json")
    request.set_status(status)
    request.write(data)
    request.finish()

'''*************************************************************
AccountHandler : class responsible for handling put and get 
                 requests for Accounts 
*************************************************************'''

class AccountHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({ 
          Required('account_id') : All(int, Range(min=0)) 
        }) 
        try:
            args = {} 
            args['account_id'] = int(account_id)  
            schema(args) 
            output = json.dumps(args)
        except MultipleInvalid as e:  
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))
 
        send_json_response(self, output, 200) 

    @tornado.gen.coroutine
    def put(self, account_id):
        # validate me 
        schema = Schema({ 
          Required('account_id') : All(int, Range(min=0)),
          'default_width': All(int, Range(min=1, max=8192)), 
          'default_height': All(int, Range(min=1, max=8192)),
          'default_thumbnail': All(str, Length(min=1, max=2048)) 
        })
        try: 
            args = parse_args(self.request)
            args['account_id'] = int(account_id)  
            schema(args) 
            output = json.dumps(args)
        except MultipleInvalid as e: 
            output = generate_standard_error('%s %s' % (e.path[0], e.msg))

        send_json_response(self, output, 200) 

class LiveStreamHandler(tornado.web.RequestHandler): 
    @tornado.gen.coroutine 
    def post(self, *args, **kwargs):
        print 'posting a video job' 

application = tornado.web.Application([
    (r'/(\d+)$', AccountHandler), 
    (r'/(\d+)/$', AccountHandler),
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
