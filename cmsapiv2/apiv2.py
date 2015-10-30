#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import ast
from cmsdb import neondata
from datetime import datetime, timedelta
import dateutil.parser
from functools import wraps
import json 
import jwt 
import logging
import signal
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
import traceback
from urlparse import urlparse

from utils import statemon
import utils.neon
import utils.logs
import utils.http
import utils.sync
from utils.options import define, options
import uuid
from voluptuous import Schema, Required, All, Length, Range, MultipleInvalid, Coerce, Invalid, Any, Optional

_log = logging.getLogger(__name__)

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

define("token_secret", default="9gRvLemgdfHUlzpv", help="the secret for tokens", type=str)
define("access_token_exp", default=720, help="user access token expiration in seconds", type=int)
define("refresh_token_exp", default=1209600, help="user refresh token expiration in seconds", type=int)

class ResponseCode(object): 
    HTTP_OK = 200
    HTTP_ACCEPTED = 202 
    HTTP_BAD_REQUEST = 400
    HTTP_UNAUTHORIZED = 401 
    HTTP_NOT_FOUND = 404 
    HTTP_CONFLICT = 409 
    HTTP_INTERNAL_SERVER_ERROR = 500
    HTTP_NOT_IMPLEMENTED = 501

class TokenTypes(object): 
    ACCESS_TOKEN = 0 
    REFRESH_TOKEN = 1

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
    @tornado.gen.coroutine 
    def is_authorized(request, access_level_required, account_required=True):
        """checks to see if a user is authorized to call a function 
           
           in order to gain access a user can be in one of two camps 
           1) A GLOBAL_ADMIN user, meaning they have access to everything 
           2) A user, who has been granted account access - this user can 
              access every function based on an access level

           Return Values: 
             True if use is allowed 
           Raises: 
             NotAuthorizedErrors if not allowed 
        """ 
        request.set_account_id() 
        account = yield tornado.gen.Task(neondata.NeonUserAccount.get, request.account_id)
        access_token = request.access_token

        if account_required and not account:
            raise NotAuthorizedError('account does not exist')
        if not access_token:  
            raise NotAuthorizedError('this endpoint requires an access token')

        try:
            payload = JWTHelper.decode_token(access_token)  
            username = payload['username']

            user = yield tornado.gen.Task(neondata.User.get, username)
            if user:
                if user.access_level & neondata.AccessLevels.GLOBAL_ADMIN:
                    raise tornado.gen.Return(True)
                elif account and username in account.users:
                    if user.access_level & access_level_required:  
                        raise tornado.gen.Return(True)

                raise NotAuthorizedError('you can not access this resource')
 
            raise NotAuthorizedError('user does not exist') 

        except jwt.ExpiredSignatureError:
            raise NotAuthorizedError('access token is expired, please refresh the token')
        except (jwt.DecodeError, jwt.InvalidTokenError, KeyError): 
            raise NotAuthorizedError('invalid token') 
 
        raise tornado.gen.Return(True)

class APIV2Handler(tornado.web.RequestHandler, APIV2Sender):
    def initialize(self):
        self.set_header('Content-Type', 'application/json')
        self.set_access_token_information()
        self.header_api_key = self.request.headers.get('X-Neon-API-Key')
        self.uri = self.request.uri 
    
    def set_access_token_information(self): 
        """Helper function to get the access token 

           the key can be in one of three places 
              1) the Authorization header : Authorization: Bearer <token> 
              2) the query string params : &token=meisatoken 
              3) the post body params : as token 
        """
        self.access_token = None 
        auth_header = self.request.headers.get('Authorization') 
        if auth_header and auth_header.startswith('Bearer'):
            self.access_token = auth_header[7:]
        elif len(self.request.query_arguments) > 0: 
            query_args = self.request.query_arguments
            try: 
                self.access_token = str(query_args['token'][0])
            except KeyError: 
                pass
        elif len(self.request.body) > 0: 
            content_type = self.request.headers.get('Content-Type')
            if content_type and 'application/json' in content_type: 
                bjson = json.loads(self.request.body) 
                try:
                    self.access_token = str(bjson['token'])
                except KeyError: 
                    pass
        
    def set_account_id(request):
        parsed_url = urlparse(request.uri) 
        request.account_id = parsed_url.path.split('/')[3]
 
    def parse_args(self, keep_token=False):
        args = {} 
        # if we have query_arguments only use them 
        if len(self.request.query_arguments) > 0: 
            for key, value in self.request.query_arguments.iteritems():
                if key != 'token' or keep_token: 
                    args[key] = value[0]
        # otherwise let's use what we find in the body, json only
        elif len(self.request.body) > 0: 
            bjson = json.loads(self.request.body) 
            for key, value in bjson.items():
                if key != 'token' or keep_token: 
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

class JWTHelper(object):
    """This class is here to keep the token_secret in one place 
       Use this class to generate good tokens with the correct secret 
       And also to decode any tokens coming in 
    """ 
    @staticmethod
    def generate_token(payload={}, token_type=TokenTypes.ACCESS_TOKEN):
        if token_type is TokenTypes.ACCESS_TOKEN: 
            exp_time_add = options.access_token_exp
        elif token_type is TokenTypes.REFRESH_TOKEN:
            exp_time_add = options.refresh_token_exp
        else:
            _log.exception('requested a token_type that does not exist') 
            raise Exception('token type not recognized')  

        if 'exp' not in payload.keys(): 
            payload['exp'] = datetime.utcnow() + timedelta(seconds=exp_time_add) 
        token = jwt.encode(payload,  
                           options.token_secret, 
                           algorithm='HS256')
        return token 
    @staticmethod 
    def decode_token(access_token):
        return jwt.decode(access_token, options.token_secret, algorithms=['HS256'])

'''*********************************************************************
APIV2 Defined Exceptions 
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
APIV2 Custom Voluptuous Types
*********************************************************************'''
class CustomVoluptuousTypes(): 
    @staticmethod
    def Date():
        return lambda v: dateutil.parser.parse(v)

    @staticmethod
    def CommaSeparatedList(limit=100):
        def f(v): 
            csl_list = v.split(',') 
            if len(csl_list) > limit: 
                raise Invalid("list exceeds limit (%d)" % limit) 
            else: 
                return True 
        return f
 
    @staticmethod
    def Dictionary():
        def f(v):
            if isinstance(ast.literal_eval(v), dict): 
                return ast.literal_eval(v)
            else:
                raise Invalid("not a dictionary") 
        return f 
