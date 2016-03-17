#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from apiv2 import *
from datetime import datetime, timedelta
from passlib.hash import sha256_crypt

_log = logging.getLogger(__name__)

define("port", default=8084, help="run on the given port", type=int)

statemon.define('successful_authenticates', int)
_successful_authenticates_ref = statemon.state.get_ref('successful_authenticates')

statemon.define('failed_authenticates', int)
_failed_authenticates_ref = statemon.state.get_ref('failed_authenticates')

statemon.define('token_expiration_logout', int)
_token_expiration_logouts_ref = statemon.state.get_ref('token_expiration_logout') 

statemon.define('successful_logouts', int)
_successful_logouts_ref = statemon.state.get_ref('successful_logouts')

statemon.define('post_account_oks', int) 

'''*****************************************************************
AuthenticateHandler 
*****************************************************************'''
class AuthenticateHandler(APIV2Handler):
    """ Class responsible for returning a token to an authorized user or 
            application 
         
        Simply send in a username/password of a valid user, or an app_id
            app_secret of an NeonUserAccount(application) and get a 
            JWT in return
 
        Everytime Authenticate is called it refreshes the tokens - both 
           access_token and refresh_token

        This would be the endpoint you call when your refresh token has 
           expired.

        More obviously this is also the endpoint you would call with a 
           brand new user.  
    """  
    @tornado.gen.coroutine
    def post(self):
        schema = Schema({
          Required('username') : Any(str, unicode, Length(min=3, max=128)),
          Required('password') : Any(str, unicode, Length(min=8, max=64)),
        })
        args = self.parse_args()
        schema(args)
        username = args.get('username') 
        password = args.get('password')
 
        api_accessor = yield tornado.gen.Task(neondata.User.get, username)
        result = None
        access_token = JWTHelper.generate_token({ 'username' : username }, 
                                                token_type=TokenTypes.ACCESS_TOKEN) 
 
        refresh_token = JWTHelper.generate_token({ 'username' : username }, 
                                                 token_type=TokenTypes.REFRESH_TOKEN)  
        def _update_tokens(x): 
            x.access_token = access_token
            x.refresh_token = refresh_token 
 
        if api_accessor: 
            if sha256_crypt.verify(password, api_accessor.password_hash):
                yield tornado.gen.Task(neondata.User.modify, username, _update_tokens)
                result = {
                           'access_token' : access_token, 
                           'refresh_token' : refresh_token 
                         } 
        if result: 
            statemon.state.increment('successful_authenticates')
            self.success(json.dumps(result)) 
        else: 
            statemon.state.increment('failed_authenticates')
            raise NotAuthorizedError('User is Not Authorized')

    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.NONE
               }  
           
'''*****************************************************************
LogoutHandler 
*****************************************************************'''
class LogoutHandler(APIV2Handler): 
    """ Class responsible for logging out/invalidating a token for a user  
    
        Send in the access_token and it will clear the access_token and 
           refresh_token for the user that is in the payload 

        Authenticate must be called again to start using the api again.  
    """  
    @tornado.gen.coroutine
    def post(self):
        schema = Schema({
          Required('token') : Any(str, unicode, Length(min=1, max=512))
        })
        args = self.parse_args(keep_token=True)
        schema(args)

        try: 
            access_token = args.get('token') 
            payload = JWTHelper.decode_token(access_token) 
            username = payload['username'] 
            
            def _update_user(u): 
                u.access_token = None
                u.refresh_token = None 
            
            yield tornado.gen.Task(neondata.NeonUserAccount.modify, username, _update_user)
            statemon.state.increment('successful_logouts')
            self.success(json.dumps({'message' : 'successfully logged out user'})) 
        except jwt.ExpiredSignatureError:
            # TODO debating if this is the right course of action... 
            # should we always allow logout regardless of the access token state? 
            statemon.state.increment('token_expiration_logout')
            raise NotAuthorizedError('access token has expired, please refresh the access token')
 
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.NONE 
               }  

'''*********************************************************************
HealthCheckHandler 
*********************************************************************'''
class HealthCheckHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self):
        self.success('<html>Server OK</html>')
 
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.GET : neondata.AccessLevels.NONE 
               }  

'''*****************************************************************
RefreshTokenHandler 
*****************************************************************'''
class RefreshTokenHandler(APIV2Handler):
    """ Class responsible for refreshing a token for an authorized user 
        
        Just send in the authenticate issued refresh_token and receive 
           a new access_token in return 

        Will return a XXX error code if the refresh_token has expired 
           if this is the case, then the user must call authenticate 
           again, to get a new set of tokens.

        Should only be called using HTTPs since refresh_tokens have a 
           longer lifespan, losing one could result in an attacker 
           having access to a token for a long period of time  
    """  
    @tornado.gen.coroutine
    def post(self):
        schema = Schema({
          Required('token') : Any(str, unicode, Length(min=1, max=512))
        })

        args = self.parse_args(keep_token=True)
        schema(args)
        refresh_token = args.get('token') 
        try: 
            payload = JWTHelper.decode_token(refresh_token) 
    
            username = payload['username']
            user = yield tornado.gen.Task(neondata.User.get, username)

            access_token = JWTHelper.generate_token({ 'username' : username }, 
                                                    token_type=TokenTypes.ACCESS_TOKEN) 
    
            def _update_user(u): 
                u.access_token = access_token

            yield tornado.gen.Task(neondata.User.modify, username, _update_user)
            result = { 
                       'access_token' : access_token, 
                       'refresh_token' : refresh_token 
                     }

            self.success(json.dumps(result)) 
            
        except jwt.ExpiredSignatureError:
            raise NotAuthorizedError('refresh token has expired, please authenticate again')
 
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.NONE 
               }  

'''****************************************************************
NewAccountHandler
****************************************************************'''
class NewAccountHandler(APIV2Handler):
    """Handles post requests to the account endpoint."""
    @tornado.gen.coroutine 
    def post(self):
        """handles account endpoint post request""" 

        schema = Schema({ 
          Required('customer_name') : Any(str, unicode,
                                          Length(min=1, max=1024)),
          'default_width': All(Coerce(int), Range(min=1, max=8192)), 
          'default_height': All(Coerce(int), Range(min=1, max=8192)),
          'default_thumbnail_id': Any(str, unicode, Length(min=1, max=2048)),
          'admin_user_username' : All(str, Length(min=8, max=64)), 
          'admin_user_password' : All(str, Length(min=8, max=64))
        })
        args = self.parse_args()
        schema(args) 
        account = neondata.NeonUserAccount(uuid.uuid1().hex, 
                      name=args['customer_name'])
        account.default_size = list(account.default_size) 
        account.default_size[0] = args.get('default_width', 
                                      neondata.DefaultSizes.WIDTH)
        account.default_size[1] = args.get('default_height', 
                                      neondata.DefaultSizes.HEIGHT)
        account.default_size = tuple(account.default_size)
        account.default_thumbnail_id = args.get('default_thumbnail_id', None)
        
        username = args.get('admin_username', None) 
        password = args.get('admin_user_password', None) 
        if username is not None and password is not None:
            new_user = neondata.User(username=username, password=password)
            yield new_user.save(async=True) 
            account.users.append(username) 
        
        yield account.save(async=True)
        account = yield neondata.NeonUserAccount.get(
                      account.neon_api_key, 
                      async=True)

        tracker_p_aid_mapper = neondata.TrackerAccountIDMapper(
                                 account.tracker_account_id, 
                                 account.neon_api_key, 
                                 neondata.TrackerAccountIDMapper.PRODUCTION)

        tracker_s_aid_mapper = neondata.TrackerAccountIDMapper(
                                 account.staging_tracker_account_id, 
                                 account.neon_api_key, 
                                 neondata.TrackerAccountIDMapper.STAGING)

        yield tracker_p_aid_mapper.save(async=True)
        yield tracker_s_aid_mapper.save(async=True) 

        account = yield self.db2api(account)
        
        _log.debug(('New Account has been added : name = %s id = %s') 
                   % (account['customer_name'], account['account_id']))
        statemon.state.increment('post_account_oks')
 
        self.success(account)

    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.GLOBAL_ADMIN 
               } 
 
    @classmethod
    def _get_default_returned_fields(cls):
        return ['account_id', 'default_size', 'customer_name',
                'default_thumbnail_id', 'tracker_account_id',
                'staging_tracker_account_id',
                'integration_ids', 'created', 'updated', 'users']
    
    @classmethod
    def _get_passthrough_fields(cls):
        return ['default_size',
                'default_thumbnail_id', 'tracker_account_id',
                'staging_tracker_account_id',
                 'created', 'updated', 'users']

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'account_id':
            # this is weird, but neon_api_key is actually the
            # "id" on this table, it's what we use to get information
            # about the account, so send back api_key (as account_id)
            retval = obj.neon_api_key
        elif field == 'customer_name':
            retval = obj.name
        elif field == 'integration_ids':
            retval = obj.integrations.keys()
        else:
            raise BadRequestError('invalid field %s' % field)

        raise tornado.gen.Return(retval)

'''****************************************************************
NewUserHandler
****************************************************************'''
class NewUserHandler(APIV2Handler):
    """Handles post requests to the user endpoint."""
    @tornado.gen.coroutine 
    def post(self):
        """handles user endpoint post request""" 

        schema = Schema({ 
          Required('username') : All(Coerce(str), Length(min=8, max=64)),
          Required('password') : All(Coerce(str), Length(min=8, max=64)),
          Required('access_level') : All(Coerce(int), Range(min=1, max=63))
        })

        args = self.parse_args()
        schema(args)

        new_user = neondata.User(username=args.get('username'), 
                       password=args.get('password'),
                       access_level=args.get('access_level'))

        yield new_user.save(async=True)
 
        user = yield self.db2api(new_user)

        self.success(user)
 
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.GLOBAL_ADMIN 
               } 
 
    @classmethod
    def _get_default_returned_fields(cls):
        return ['username', 'created', 'updated' ]
    
    @classmethod
    def _get_passthrough_fields(cls):
        return ['username', 'created', 'updated' ]
         
'''*********************************************************************
Endpoints 
*********************************************************************'''
application = tornado.web.Application([
    (r'/healthcheck/?$', HealthCheckHandler),
    (r'/api/v2/authenticate/?$', AuthenticateHandler),
    (r'/api/v2/refresh_token/?$', RefreshTokenHandler),
    (r'/api/v2/accounts/?$', NewAccountHandler),
    (r'/api/v2/users/?$', NewUserHandler),
    (r'/api/v2/logout/?$', LogoutHandler)
], gzip=True)

def main():
    global server
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
