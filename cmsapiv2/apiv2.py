#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import ast
import boto
from cmsdb import neondata
import concurrent.futures
from datetime import datetime, timedelta
import dateutil.parser
from functools import wraps
import json
import jwt
import logging
import re
import signal
import stripe 
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
from voluptuous import Schema, Required, All, Length, Range, MultipleInvalid, Coerce, Invalid, Any, Optional, Boolean

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
define("verify_token_exp", default=86400, help="account verify token expiration in seconds", type=int)
define("frontend_base_url",
    default='https://app.neon-lab.com',
    help="will default to this if the origin is null",
    type=str)
define("check_subscription_interval", 
    default=3600, 
    help="how many seconds in between checking the billing integration", 
    type=int)

# stripe stuff 
stripe.api_key = 'sk_test_mOzHk0K8yKfe57T63jLhfCa8'

class ResponseCode(object):
    HTTP_OK = 200
    HTTP_ACCEPTED = 202
    HTTP_BAD_REQUEST = 400
    HTTP_UNAUTHORIZED = 401
    # should be a 429, but tornado does not like that
    HTTP_TOO_MANY_REQUESTS = 402
    HTTP_NOT_FOUND = 404
    HTTP_CONFLICT = 409
    HTTP_INTERNAL_SERVER_ERROR = 500
    HTTP_NOT_IMPLEMENTED = 501

class HTTPVerbs(object):
    POST = 'POST'
    PUT = 'PUT'
    GET = 'GET'
    DELETE = 'DELETE'

class TokenTypes(object):
    ACCESS_TOKEN = 0
    REFRESH_TOKEN = 1
    VERIFY_TOKEN = 2

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
        self.write({'error': error_json})
        self.finish()

class APIV2Handler(tornado.web.RequestHandler, APIV2Sender):
    def initialize(self):
        self.set_header('Content-Type', 'application/json')
        self.uri = self.request.uri
        self.account = None
        self.account_limits = None
        self.origin = self.request.headers.get("Origin") or\
            options.frontend_base_url
        self.executor = concurrent.futures.ThreadPoolExecutor(5)

    
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
            content_type = self.request.headers.get('Content-Type', None)
            if content_type is None or 'application/json' not in content_type:
                raise BadRequestError('Content-Type must be JSON')
            else:
                bjson = json.loads(self.request.body)
                try:
                    self.access_token = str(bjson['token'])
                except KeyError:
                    pass

    def parse_args(self, keep_token=False):
        args = {}
        # if we have query_arguments only use them
        if len(self.request.query_arguments) > 0:
            for key, value in self.request.query_arguments.iteritems():
                if key != 'token' or keep_token:
                    args[key] = value[0]
        # otherwise let's use what we find in the body, json only
        elif len(self.request.body) > 0:
            content_type = self.request.headers.get('Content-Type', None)
            if content_type is None or 'application/json' not in content_type:
                raise BadRequestError('Content-Type must be JSON')
            else:
                bjson = json.loads(self.request.body)
                for key, value in bjson.items():
                    if key != 'token' or keep_token:
                        args[key] = value

        return args

    def set_account_id(request):
        parsed_url = urlparse(request.uri)
        try:
            request.account_id = parsed_url.path.split('/')[3]
        except IndexError:
            request.account_id = None

    @tornado.gen.coroutine
    def set_account(request):
        request.set_account_id()
        if request.account_id:
            account = yield neondata.NeonUserAccount.get(
                          request.account_id,
                          async=True)
            request.account = account

    @tornado.gen.coroutine
    def is_authorized(request,
                      access_level_required,
                      account_required=True,
                      internal_only=False):
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
        request.set_access_token_information()
        if access_level_required is neondata.AccessLevels.NONE:
            raise tornado.gen.Return(True)

        account = request.account
        access_token = request.access_token
        if account_required and not account:
            raise NotAuthorizedError('account does not exist')
        if not access_token:
            raise NotAuthorizedError('this endpoint requires an access token')

        try:
            payload = JWTHelper.decode_token(access_token)
            username = payload['username']

            user = yield neondata.User.get(username, async=True)
            if user:
                request.user = user

                def _check_internal_only():
                    al_internal_only = neondata.AccessLevels.INTERNAL_ONLY_USER
                    if internal_only:
                        if user.access_level & al_internal_only is \
                                neondata.AccessLevels.INTERNAL_ONLY_USER:
                            return True
                        return False
                    return True

                if user.access_level & neondata.AccessLevels.GLOBAL_ADMIN is \
                        neondata.AccessLevels.GLOBAL_ADMIN:
                    raise tornado.gen.Return(True)

                elif account_required and account and username in account.users:
                    if not _check_internal_only():
                        raise NotAuthorizedError('internal only resource')
                    if user.access_level & access_level_required is \
                            access_level_required:
                        raise tornado.gen.Return(True)
                else:
                    if internal_only:
                        if not _check_internal_only():
                            raise NotAuthorizedError('internal only resource')
                    if not account_required:
                        if user.access_level & access_level_required is \
                               access_level_required:
                            raise tornado.gen.Return(True)

                raise NotAuthorizedError('you can not access this resource')

            raise NotAuthorizedError('user does not exist')

        except jwt.ExpiredSignatureError:
            raise NotAuthorizedError('access token is expired, please refresh the token')
        except (jwt.DecodeError, jwt.InvalidTokenError, KeyError):
            raise NotAuthorizedError('invalid token')

        raise tornado.gen.Return(True)

    @tornado.gen.coroutine
    def check_valid_subscription(request):
        '''verifies we have a valid subscription and can make this call 

           called in prepare, and will raise an exception if the subscription
           is not valid for this account  
        '''
        if request.account is None:  
            raise tornado.gen.Return(True)

        # this account isn't billed through this integration (older account) 
        # just return true 
        if request.account.billed_elsewhere: 
            raise tornado.gen.Return(True)

        current_subscription = None 

        acct = request.account
        subscription_info = acct.subscription_information
        current_plan_type = subscription_info['plan']['id'] 
        acct_subscription_status = subscription_info['status']

        # should we check stripe for updated subscription state?
        if datetime.utcnow() > dateutil.parser.parse(
             acct.verify_subscription_expiry):
            try:  
                stripe_customer = yield request.executor.submit(
                    stripe.Customer.retrieve, 
                    acct.billing_provider_ref)
    
                # returns the most active subscriptions up to 10 
                cust_sub_obj = yield request.executor.submit(
                    stripe_customer.subscriptions.all)
                cust_subs = cust_sub_obj['data']

            except Exception as e: 
                _log.error('Unknown error occurred talking to Stripe %s' % e)
                raise 
 
            for cs in cust_subs:
                acct_subscription_status = cs.status  
                if acct_subscription_status in [ 
                    neondata.SubscriptionState.ACTIVE,
                    neondata.SubscriptionState.IN_TRIAL ] and\
                    current_plan_type == cs.plan.id:
                    # if we find a subscription in active/trial we 
                    # are good break out of the for loop, and 
                    # on the current plan type
                    current_subscription = cs 
                    break
                   
            new_date = (datetime.utcnow() + timedelta(
                seconds=options.check_subscription_interval)).strftime(
                    "%Y-%m-%d %H:%M:%S.%f")

            def _modify_account(a):
                a.verify_subscription_expiry = new_date
                if current_subscription is None: 
                    a.subscription_info = cust_subs[0]
                else: 
                    a.subscription_info = current_subscription

            yield neondata.NeonUserAccount.modify(
                acct.neon_api_key,
                _modify_account, 
                async=True)  

        if acct_subscription_status in [ neondata.SubscriptionState.ACTIVE, 
               neondata.SubscriptionState.IN_TRIAL ]:
            raise tornado.gen.Return(True)

        raise TooManyRequestsError('Your subscription is not valid') 

    @tornado.gen.coroutine
    def check_account_limits(request, limit_list):
        ''' responsible for checking account limits

            this is called in prepare, and that pulls this info
             from the get_limits functions in the children

            it checks the defined limits to see if any of them
               are exceeded. it will also reset the timer if
               that is necessary.
        '''

        if request.account is None:
            raise tornado.gen.Return(True)

        # grab the account_limit object for the requests
        acct_limits = yield neondata.AccountLimits.get(
                          request.account_id,
                          async=True)

        # limits are not set up for this account, let it
        # slide for now
        if acct_limits is None:
            raise tornado.gen.Return(True)

        request.account_limits = acct_limits
        al_data_dict = acct_limits.to_dict()['_data']
        for limit in limit_list:
            try:
                left_arg = al_data_dict[limit['left_arg']]
                right_arg = al_data_dict[limit['right_arg']]
                operator = limit['operator']

                eval_string = '%s %s %s' % (left_arg, operator, right_arg)
                if eval(eval_string):
                    return
                else:
                    # lets check the timer if there is one
                    timer_dict = request._get_timer_dict(limit, al_data_dict)
                    if timer_dict:
                        refresh_time = timer_dict['refresh_time']
                        # check to see if we should refresh
                        if dateutil.parser.parse(refresh_time) <= \
                           datetime.utcnow():
                            request.account_limits = yield \
                                request._reset_rate_limit(
                                      request.account_id,
                                      timer_dict['timer_resets'],
                                      limit['timer_info']['refresh_time'],
                                      timer_dict['add_to_refresh_time'])
                            return

                    msg = 'The max amount of requests have been reached for \
                           this endpoint. For more rate limit information \
                           please see the account/limits endpoint.'

                    raise TooManyRequestsError(msg)
            except KeyError as e:
                _log.warning('Limit issue %s was encountered\
                              when checking limits - passing' % (e))
                pass
            raise tornado.gen.Return(True)

    @staticmethod
    @tornado.gen.coroutine
    def _reset_rate_limit(account_id,
                          timer_resets,
                          key_to_add_time_to=None,
                          amount_of_time_to_add=0.0):
        ''' reset everything in the timer_resets for this
            rate limit '''
        def _modify_me(x):
            for tr in timer_resets:
                x.__dict__[tr[0]] = tr[1]
            if key_to_add_time_to:
                new_date = (datetime.utcnow() +
                            timedelta(seconds=amount_of_time_to_add)).strftime(
                                "%Y-%m-%d %H:%M:%S.%f")

                x.__dict__[key_to_add_time_to] = new_date

        limit = yield neondata.AccountLimits.modify(
            account_id,
            _modify_me,
            async=True)

        raise tornado.gen.Return(limit)

    @staticmethod
    def _get_timer_dict(limit_info, acct_limit):
        ''' helper to get values from acct_limit based on the
            key values in limit_info '''

        rv = {}
        try:
            timer_info = limit_info['timer_info']
            refresh_time_key = timer_info['refresh_time']
            add_to_refresh_time_key = timer_info['add_to_refresh_time']
            timer_resets = timer_info['timer_resets']

            rv['refresh_time'] = acct_limit[refresh_time_key]
            rv['add_to_refresh_time'] = acct_limit[add_to_refresh_time_key]
            rv['timer_resets'] = timer_resets

        except KeyError:
            pass

        return rv

    def get_access_levels(self):
        '''
            to be specified in each of the handlers
            this is a dictionary that maps http_verb to access_level

            eg
            {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET, HTTPVerbs.PUT],
                 'internal_only' : False
            }

            this means that GET requires READ, PUT requires UPDATE
             and an account is required on both endpoints, it also
             is not an internal_only function
        '''
        raise NotImplementedError('access levels are not defined')

    def get_limits(self):
        '''if your function needs to be rate limited, define
           this class to return a dictionary that will define
           what limits need to be checked

           the first two itemss are fields from the Limits table

           the third item is an operator that will be executed on
               the first two args
           supported operators :
           <, >, <=, >=, =

           the fourth item, is a dict of timer info , if sent in
           this will be checked as well, and reset if necessary

           the fourth and fifth items values_to_increase and decrease,
             tell the limit checker what value to increase/decrease
             after a successful call
           eg
           {
                HTTPVerbs.POST : [
                    {
                        'left_arg' : 'video_posts',
                        'right_arg' : 'max_video_posts',
                        'operator' : '<',
                        'timer_info : {
                            'refresh_time' : 'refresh_time_video_posts',
                            'add_to_refresh_time' : 'seconds_to_refresh_video_posts',
                            'timer_resets' : [ ('video_posts', 0) ]
                        },
                        'values_to_increase' : [ ('video_posts', 1) ],
                        'values_to_decrease' : None
                    },
                    ...
                    {
                        you can specify any number of
                        limits you need checked for each http verb
                    }
                ]
           }
           this would then do videos_posted < videos_posted_max in prepare
              check the timer (refresh if necessary, reset if necessary)
              on_finish will increase the values, if we successfully served 
                 the request 
        ''' 
        return None

    def get_special_functions(self): 
        return []   
 
    @tornado.gen.coroutine 
    def prepare(self):
        access_level_dict = self.get_access_levels()
        yield self.set_account()

        try:
            account_required_list = access_level_dict['account_required']
        except KeyError:
            account_required_list = []

        try:
            internal_only = access_level_dict['internal_only']
        except KeyError:
            internal_only = False

        try:
            yield self.is_authorized(access_level_dict[self.request.method],
                                     self.request.method in account_required_list,
                                     internal_only)
        except KeyError:
            raise NotImplementedError('access levels are not defined')

        limits_dict = self.get_limits()
        if limits_dict is not None:
            try:
                yield self.check_account_limits(
                    limits_dict[self.request.method])
            except KeyError: 
                pass

        try: 
            sub_required = access_level_dict['subscription_required']
            if self.request.method in sub_required: 
                yield self.check_valid_subscription() 
        except KeyError:
            pass  
 
    @tornado.gen.coroutine
    def on_finish(self):
        yield self._handle_limit_inc_dec()

    @tornado.gen.coroutine
    def _handle_limit_inc_dec(self):

        if self.account_limits is None:
            return
        if self.get_status() not in [ResponseCode.HTTP_OK,
                                     ResponseCode.HTTP_ACCEPTED]:
            return

        defined_limits_dict = self.get_limits()
        if defined_limits_dict is None:
            return

        try:
            defined_limit_list = defined_limits_dict[self.request.method]

            for dl in defined_limit_list:
                values_to_increase = dl['values_to_increase']
                values_to_decrease = dl['values_to_decrease']

                for v in values_to_increase:
                    self.account_limits.__dict__[v[0]] += v[1]
                for v in values_to_decrease:
                    self.account_limits.__dict__[v[0]] -= v[1]

            yield self.account_limits.save(async=True)
        except KeyError:
            pass

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
                                                  BadRequestError,
                                                  NotImplementedError,
                                                  TooManyRequestsError]):
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
            if isinstance(exception, BadRequestError):
                statemon.state.increment(ref=_invalid_input_errors_ref,
                                         safe=False)
                self.set_status(ResponseCode.HTTP_BAD_REQUEST)

            if isinstance(exception, TooManyRequestsError):
                statemon.state.increment(ref=_invalid_input_errors_ref,
                                         safe=False)
                self.set_status(ResponseCode.HTTP_TOO_MANY_REQUESTS)

            self.error(get_exc_message(exception), code=self.get_status())

        elif isinstance(exception, AlreadyExists):
            self.set_status(ResponseCode.HTTP_CONFLICT)
            statemon.state.increment(ref=_already_exists_errors_ref,
                                     safe=False)
            self.error('this item already exists', extra_data=get_exc_message(exception))

        elif isinstance(exception, neondata.ThumbDownloadError):
            self.set_status(ResponseCode.HTTP_BAD_REQUEST)
            self.error('failed to download thumbnail',
                       extra_data=get_exc_message(exception))

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

    @classmethod
    @tornado.gen.coroutine
    def db2api(cls, obj, fields=None):
        """Converts a database object to a response dictionary

        Keyword arguments:
        obj - The database object to convert
        fields - List of fields to return
        """
        if fields is None:
            fields = cls._get_default_returned_fields()

        retval = {}
        passthrough_fields = set(cls._get_passthrough_fields())

        for field in fields:
            try: 
                if field in passthrough_fields:
                    retval[field] = getattr(obj, field)
                else:
                    retval[field] = yield cls._convert_special_field(obj, field)
            except AttributeError: 
                pass 
        raise tornado.gen.Return(retval)

    @classmethod
    def _get_default_returned_fields(cls):
        '''Return a list of fields that should be returned in this API call.'''
        raise NotImplementedError(
            'List of default fields must be specified for this object. %s'
            % cls.__name__)

    @classmethod
    def _get_passthrough_fields(cls):
        '''Return a list of fields in a database object that should be
           returned by the api without change.
        '''
        raise NotImplementedError(
            'List of passthrough fields must be specified for this object. %s'
            % cls.__name__)

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        '''Converts a field on a database object that requires special
        processing.

        Inputs:
        obj - The database object
        field - The name of the field to process

        Returns:
        The value to place in a dictionay to represent this field.

        Raises:
        BadRequestError if the field not handled
        '''
        raise NotImplementedError(
            'Must specify how to convert %s for object %s' %
            (field, cls.__name__))

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
        elif token_type is TokenTypes.VERIFY_TOKEN:
            exp_time_add = options.verify_token_exp
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
    def __init__(self,
                 msg,
                 code=ResponseCode.HTTP_INTERNAL_SERVER_ERROR):
        self.msg = msg
        self.code = code

class NotFoundError(tornado.web.HTTPError):
    def __init__(self,
                 msg='resource was not found',
                 code=ResponseCode.HTTP_NOT_FOUND):
        self.msg = self.reason = self.log_message = msg
        self.code = self.status_code = code

class NotAuthorizedError(tornado.web.HTTPError):
    def __init__(self,
                 msg='not authorized',
                 code=ResponseCode.HTTP_UNAUTHORIZED):
        self.msg = self.reason = self.log_message = msg
        self.code = self.status_code = code

class TooManyRequestsError(tornado.web.HTTPError):
    def __init__(self,
                 msg='not authorized',
                 code=ResponseCode.HTTP_TOO_MANY_REQUESTS):
        self.msg = self.reason = self.log_message = msg
        self.code = self.status_code = code

class AlreadyExists(tornado.web.HTTPError):
    def __init__(self,
                 msg,
                 code=ResponseCode.HTTP_BAD_REQUEST):
        self.msg = self.reason = self.log_message = msg
        self.code = self.status_code = code

class BadRequestError(tornado.web.HTTPError):
    def __init__(self,
                 msg,
                 code=ResponseCode.HTTP_BAD_REQUEST):
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

    @staticmethod
    def Email():
        def f(v):
            if re.match("[a-zA-Z0-9\.\+_-]*@[a-zA-Z0-9\.\+_-]*\.\w+", str(v)):
                return str(v)
            else:
                raise Invalid("not a valid email address")
        return f
