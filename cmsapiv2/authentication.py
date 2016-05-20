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
statemon.define('bad_password_reset_attempts', int) 

class CommunicationTypes(object): 
    EMAIL = 'email' 
    SECONDARY_EMAIL = 'secondary_email' 
    CELL_PHONE = 'cell_phone' 

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
          Required('username') : All(Coerce(str), Length(min=3, max=128)),
          Required('password') : All(Coerce(str), Length(min=8, max=64)),
        })
        args = self.parse_args()
        schema(args)
        username = args.get('username').lower() 
        password = args.get('password')
 
        api_accessor = yield neondata.User.get(username, async=True) 
        result = None

        access_token = JWTHelper.generate_token(
            { 'username' : username }, 
            token_type=TokenTypes.ACCESS_TOKEN) 
 
        refresh_token = JWTHelper.generate_token(
            { 'username' : username }, 
            token_type=TokenTypes.REFRESH_TOKEN)

        def _update_tokens(x): 
            x.access_token = access_token
            x.refresh_token = refresh_token 
 
        if api_accessor: 
            if sha256_crypt.verify(password, api_accessor.password_hash):
                user = yield neondata.User.modify(username, 
                    _update_tokens, 
                    async=True)
                user_rv = yield self.db2api(user) 
                account_ids = yield api_accessor.get_associated_account_ids(
                    async=True) 
                result = {
                           'access_token' : access_token, 
                           'refresh_token' : refresh_token, 
                           'account_ids' : account_ids, 
                           'user_info' : user_rv  
                         } 
        if result: 
            statemon.state.increment('successful_authenticates')
            self.success(json.dumps(result)) 
        else: 
            statemon.state.increment('failed_authenticates')
            raise NotAuthorizedError('User is Not Authorized')

    @classmethod
    def _get_default_returned_fields(cls):
        return ['username', 'created', 'updated', 
                'first_name', 'last_name', 'title' ]
    
    @classmethod
    def _get_passthrough_fields(cls):
        return ['username', 'created', 'updated',
                'first_name', 'last_name', 'title' ]

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
          Required('token') : All(Coerce(str), Length(min=1, max=512))
        })
        args = self.parse_args(keep_token=True)
        schema(args)

        try: 
            access_token = args.get('token') 
            payload = JWTHelper.decode_token(access_token) 
            username = payload['username'].lower() 
            
            def _update_user(u): 
                u.access_token = None
                u.refresh_token = None 
            
            yield tornado.gen.Task(neondata.NeonUserAccount.modify, username, _update_user)
            statemon.state.increment('successful_logouts')
            self.success(json.dumps({'message' : 'successfully logged out user'})) 
        except jwt.ExpiredSignatureError:
            statemon.state.increment('token_expiration_logout')
            self.success(json.dumps({'message' : 'logged out expired user'})) 

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
          Required('token') : All(Coerce(str), Length(min=1, max=512))
        })

        args = self.parse_args(keep_token=True)
        schema(args)
        refresh_token = args.get('token') 
        try: 
            payload = JWTHelper.decode_token(refresh_token) 
    
            username = payload['username'].lower()
            user = yield neondata.User.get(username, async=True)
            account_ids = yield user.get_associated_account_ids(async=True)

            access_token = JWTHelper.generate_token(
                { 'username' : username }, 
                token_type=TokenTypes.ACCESS_TOKEN) 
    
            def _update_user(u): 
                u.access_token = access_token

            yield neondata.User.modify(username, 
                _update_user, 
                async=True)

            result = { 
                       'access_token' : access_token, 
                       'refresh_token' : refresh_token, 
                       'account_ids' : account_ids 
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
    """Handles post requests to the account endpoint.

       This will create a row in verification table 
         and will not create any other objects until 
         the enduser verifies that the email address is 
         indeed legit. 
    """
    @tornado.gen.coroutine 
    def post(self):
        """handles account endpoint post request""" 
        schema = Schema({ 
          Required('email') : All(CustomVoluptuousTypes.Email(),
              Length(min=6, max=1024)),
          Required('admin_user_username') : All(CustomVoluptuousTypes.Email(), 
              Length(min=6, max=512)), 
          Required('admin_user_password') : All(Coerce(str), 
              Length(min=8, max=64)),
          'customer_name' : All(Coerce(str), Length(min=1, max=1024)),
          'default_width': All(Coerce(int), Range(min=1, max=8192)), 
          'default_height': All(Coerce(int), Range(min=1, max=8192)),
          'default_thumbnail_id': All(Coerce(str), Length(min=1, max=2048)),
          'admin_user_first_name': All(Coerce(str), Length(min=1, max=256)),
          'admin_user_last_name': All(Coerce(str), Length(min=1, max=256)),
          'admin_user_title': All(Coerce(str), Length(min=1, max=32))
        })
        args = self.parse_args()
        schema(args) 
        account = neondata.NeonUserAccount(uuid.uuid1().hex, 
                      name=args.get('customer_name', None))
        account.default_size = list(account.default_size) 
        account.default_size[0] = args.get('default_width', 
                                      neondata.DefaultSizes.WIDTH)
        account.default_size[1] = args.get('default_height', 
                                      neondata.DefaultSizes.HEIGHT)
        account.default_size = tuple(account.default_size)
        account.default_thumbnail_id = args.get('default_thumbnail_id', None)
        account.email = args.get('email') 
        
        username = args.get('admin_user_username').lower() 
        password = args.get('admin_user_password')

        # verify there isn't already an user with this email
        current_user = yield neondata.User.get(username, async=True) 
        if current_user: 
            raise AlreadyExists('user with that email already exists')
 
        new_user = neondata.User(username=username, 
                       password=password,
                       access_level = neondata.AccessLevels.ADMIN,
                       first_name = args.get('admin_user_first_name', None),
                       last_name = args.get('admin_user_last_name', None), 
                       title = args.get('admin_user_title', None))
        account.users.append(username) 

        extra_info = {} 
        extra_info['account'] = account.to_json() 
        extra_info['user'] = new_user.to_json()

        verify_token = JWTHelper.generate_token(
            { 'email' : account.email }, 
            token_type=TokenTypes.VERIFY_TOKEN)
 
        verifier = neondata.Verification(account.email,
            token=verify_token,  
            extra_info=extra_info)
 
        yield verifier.save(async=True)
        
        self.send_email(account, new_user, verify_token)  
        msg = 'account verification email sent to %s' % account.email
        self.success({'message' : msg})  
            
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.NONE 
               } 
    
    def send_email(self, 
                   account,
                   user,  
                   token): 
        """ Helper to send emails via ses 

            if the email is sent successfully, it returns True 
            if something goes wrong it logs, and raises an exception
        """ 
        kwargs = {}
        click_me_url = '%s/account/confirm?token=%s' % (self.origin, token) 
        kwargs['to_addresses'] = account.email 
        kwargs['subject'] = 'Welcome to Neon for Videos' 
             
        kwargs['body'] = """<html>
                              <body>
                                <p><a href="https://www.neon-lab.com">
                                <img class="logo"
                                 style = "height: 43.25px; width: 104px;"  
                                 src="https://s3.amazonaws.com/neon_website_assets/logo_777.png" 
                                 alt="Neon">
                                </a></p>
                                <br> 
                                <p>Hi {first_name},</p>
                                <p>Thank you for signing up for Neon for Video. 
                                   You're just a step away from getting more 
                                   value from your videos. First, please verify 
                                   your account by clicking here: 
                                   <a style="color: #f16122" href="{url}">
                                   verify your account
                                   </a>.
                                </p>
                                <p>For your reference, your username is: 
                                   <a href="" style="text-decoration:none !important; color:#000000 !important;">{username}</a>
                                </p>
                                <p>
                                  <a style="color: #f16122" href="https://app.neon-lab.com/signin">
                                  Login </a> to your account.
                                </p>
                                <p>Thanks,<br>
                                  The Neon Team
                                </p>
                                <p style="font-size:smaller">
                                   If the verification link does not work please copy 
                                   and paste the following address into your 
                                   browser : {url} 
                                </p> 
                                <p>------------</p>
                                <p class="footer" style="font-size: smaller">Neon Labs Inc.<br>
                                70 South Park St. | San Francisco, CA 94107<br>
                                (415) 355-4249<br>
                                <a style="color: #f16122" href="https://www.neon-lab.com">neon-lab.com</a>
                                </p>
                                <p class="footer" style="font-size: smaller">This is an automated email. 
                                Please get in touch with us at 
                                <a href="mailto:ask@neon-lab.com">
                                  ask@neon-lab.com</a></p>
                              </body>
                            </html>""".format(
                                first_name=user.first_name, 
                                url=click_me_url, 
                                username=user.username) 

                                 
        kwargs['source'] = 'Neon Account Creation <noreply@neon-lab.com>' 
        kwargs['reply_addresses'] = 'noreply@neon-lab.com' 
        kwargs['format'] = 'html' 
        ses = boto.connect_ses()
        try: 
            ses.send_email(**kwargs)
        except Exception as e: 
            _log.error('Failed to Verification Send email to %s exc_info %s' % 
                (user.username, e))
            raise Exception('unable to send verification email')
 
        return True 
 
'''****************************************************************
UserHandler
****************************************************************'''
class UserHandler(APIV2Handler):
    """Handles post requests to the user endpoint."""
    @tornado.gen.coroutine 
    def post(self):
        """handles user endpoint post request""" 

        schema = Schema({ 
          Required('username') : All(Coerce(str), Length(min=8, max=256)),
          Required('password') : All(Coerce(str), Length(min=8, max=64)),
          Required('access_level') : All(Coerce(int), Range(min=1, max=31)),
          'first_name': All(Coerce(str), Length(min=1, max=256)),
          'last_name': All(Coerce(str), Length(min=1, max=256)),
          'secondary_email': All(Coerce(str), Length(min=1, max=256)),
          'cell_phone_number': All(Coerce(str), Length(min=1, max=32)),
          'title': All(Coerce(str), Length(min=1, max=32))
        })

        args = self.parse_args()
        schema(args)

        new_user = neondata.User(username=args.get('username'), 
                       password=args.get('password'),
                       access_level=args.get('access_level'),
                       first_name=args.get('first_name', None),
                       last_name=args.get('last_name', None), 
                       secondary_email=args.get('secondary_email', None), 
                       cell_phone_number=args.get('cell_phone_number', None), 
                       title=args.get('title',None))

        yield new_user.save(async=True)
        new_user = yield neondata.User.get(args.get('username'), async=True)  
        user = yield self.db2api(new_user)

        self.success(user)

    @tornado.gen.coroutine 
    def put(self):
        """handles user endpoint put request, currently only 
           used for password resets"""
 
        schema = Schema({ 
          Required('username') : All(Coerce(str), Length(min=8, max=64)),
          Required('new_password') : All(Coerce(str), Length(min=8, max=64)),
          Required('reset_password_token') : All(
              Coerce(str), 
              Length(min=16, max=512))
        })

        args = self.parse_args()
        schema(args)

        username = args.get('username') 
        new_password = args.get('new_password') 
        reset_password_token = args.get('reset_password_token') 

        current_user = yield neondata.User.get(
            username, 
            async=True)
 
        if not current_user: 
            raise NotFoundError('User was not found')
 
        if reset_password_token != current_user.reset_password_token:
            _log.info('Invalid attempt(token mismatch)'\
                      ' to reset %s password' % username)  
            statemon.state.increment('bad_password_reset_attempts')
            raise NotAuthorizedError('Token mismatch') 

        try: 
            payload = JWTHelper.decode_token(reset_password_token) 
            pl_username = payload['username']
            if pl_username != username: 
                _log.info('Invalid attempt(mismatched usernames)'\
                          ' to reset %s password' % username)  
                statemon.state.increment('bad_password_reset_attempts')
                raise NotAuthorizedError('User mismatch please try again') 

        except jwt.ExpiredSignatureError:
            raise NotAuthorizedError(
                'reset password token has expired, please request another')
       
        def _modify_user(u):  
            u.password_hash = sha256_crypt.encrypt(new_password) 
            u.reset_password_token = None
 
        user = yield neondata.User.modify(
            username, 
            _modify_user, 
            async=True)   

        rv = yield self.db2api(user)

        self.success(rv)
 
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE, 
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE
               } 
 
    @classmethod
    def _get_default_returned_fields(cls):
        return ['username', 'created', 'updated', 
                'first_name', 'last_name', 'title', 
                'secondary_email', 'cell_phone_number' ]
    
    @classmethod
    def _get_passthrough_fields(cls):
        return ['username', 'created', 'updated',
                'first_name', 'last_name', 'title', 
                'secondary_email', 'cell_phone_number' ]

'''****************************************************************
VerifyAccountHandler
****************************************************************'''
class VerifyAccountHandler(APIV2Handler):
    """Handles post requests to the account endpoint.

       If we find a valid email that matches the token  
         in our database, we will create User/NeonUserAccount/
         TrackerAccountIDMappers
           
         Otherwise we will return the appropriate error code 
    """
    @tornado.gen.coroutine 
    def post(self):
        """handles account endpoint post request""" 
        schema = Schema({ 
          Required('token') : All(Coerce(str), Length(min=1, max=512))
        })

        args = self.parse_args(keep_token=True)
        schema(args)
        verify_token = args.get('token') 
        try: 
            payload = JWTHelper.decode_token(verify_token) 
    
            email = payload['email']
            verifier = yield neondata.Verification.get(email, async=True)
    
            if not verifier: 
                raise NotFoundError('no information for email %s' % (email))
            
            new_user_json = json.loads(verifier.extra_info['user'])
            new_user = neondata.User._create(new_user_json['_data']['key'], 
                           new_user_json)
            account = neondata.NeonUserAccount.create(
                verifier.extra_info['account'])
 
            try: 
                yield new_user.save(overwrite_existing_object=False, async=True)
            except neondata.psycopg2.IntegrityError: 
                raise AlreadyExists('user with that email already exists')
    
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

            # create every new account with the demo billing plan
            billing_plan = yield neondata.BillingPlans.get(
                'demo', 
                async=True) 
           
            account_limits = neondata.AccountLimits(account.neon_api_key)
            account_limits.populate_with_billing_plan(billing_plan)
            yield account_limits.save(async=True) 

            # add a default experimentstrategy as well 
            experiment_strategy = neondata.ExperimentStrategy( 
                account.neon_api_key) 
            yield experiment_strategy.save(async=True) 
    
            account = yield self.db2api(account)
            
            _log.debug(('New Account has been added : name = %s id = %s') 
                       % (account['customer_name'], account['account_id']))
            statemon.state.increment('post_account_oks')
     
            self.success(account)
    
        except jwt.ExpiredSignatureError:
            raise NotAuthorizedError('verify token has expired')
 
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.NONE
               }
 
    @classmethod
    def _get_default_returned_fields(cls):
        return ['account_id', 'default_size', 'customer_name',
                'default_thumbnail_id', 'tracker_account_id',
                'staging_tracker_account_id',
                'integration_ids', 'created', 'updated', 'users', 
                'serving_enabled', 'email']
    
    @classmethod
    def _get_passthrough_fields(cls):
        return ['default_size',
                'default_thumbnail_id', 'tracker_account_id',
                'staging_tracker_account_id',
                'created', 'updated', 'users', 
                'serving_enabled', 'email']

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
ForgotPasswordHandler
****************************************************************'''
class ForgotPasswordHandler(APIV2Handler):
    """Handles post requests to the forgot_password endpoint.

       This will send out an email to the username(email) that 
          requests it. 

       It will update the user object with a reset_password_token, 
         which will be valid for a short period of time. 

       If this is called and the reset_password_token is set, and 
         not expired, it will not send out another email. This will
         prevent spamming emails, based on just a username. 
    """
    @tornado.gen.coroutine 
    def post(self):
        """handles account endpoint post request""" 
        schema = Schema({ 
          Required('username') : All(CustomVoluptuousTypes.Email(),
              Length(min=6, max=1024)),
          'communication_type' : All(Coerce(str), Length(1,16)) 
        })
        alt_schema = Schema({
            Required('username') : All(Coerce(str), 
                Length(min=6, max=1024)),
            'communication_type' : All(Coerce(str), Length(1,16)) 
        })

        args = self.parse_args()
        email_address = None 
        phone_number = None 
 
        communication_type = args.get('communication_type', None)
        if communication_type is None: 
            communication_type = CommunicationTypes.EMAIL
 
        communication_type = communication_type.lower()

        try: 
            schema(args)
            username = args.get('username')  
        except Invalid as e:
            # let's see if it's a string username, they 
            # could possibly have a secondary_email on 
            # the account
            if 'not a valid email address' in e.error_message:
                alt_schema(args)
                username = args.get('username')
                if communication_type == CommunicationTypes.EMAIL: 
                    communication_type = CommunicationTypes.SECONDARY_EMAIL
            else: 
                raise 

        user = yield neondata.User.get(username, async=True) 

        if not user: 
            raise NotFoundError('User was not found.')
 
        if communication_type == CommunicationTypes.EMAIL:
            email_address = username
        elif communication_type == CommunicationTypes.SECONDARY_EMAIL:
            if not user.secondary_email: 
                raise BadRequestError('No recovery email is available'\
                   ' please contact us, or try a different form'\
                   ' of recovery.')  
            else: 
                email_address = user.secondary_email 
        elif communication_type == CommunicationTypes.CELL_PHONE:
            if not user.cell_phone_number: 
                raise BadRequestError('No cell phone number is available'\
                    ' please contact us, or try a different form'\
                    ' of recovery.')
            else: 
                phone_number = user.cell_phone_number
        else: 
            raise BadRequestError('Communication Type not recognized') 

        # TODO implement through SNS
        if phone_number: 
            raise NotImplementedError('recovery by phone is not ready') 
 
        if user.reset_password_token:
            # make sure we can't just spam communication 
            try: 
                payload = JWTHelper.decode_token(user.reset_password_token) 
                raise BadRequestError('There is a password reset comm'\
                    ' already sent to this user. Check your spam or junk'\
                    ' folders to ensure you have not missed it.') 
            except jwt.ExpiredSignatureError:
                pass 
            
        rp_token = JWTHelper.generate_token(
            { 'username' : username }, 
            token_type=TokenTypes.RESET_PASSWORD_TOKEN)

        def _modify_user(u): 
            u.reset_password_token = rp_token 

        user = yield neondata.User.modify(
            username,
            _modify_user, 
            async=True) 

        if email_address:
            self._send_email(email_address, user)  
            msg = 'Reset Password email sent to %s' % email_address
            self.success({'message' : msg})
 
    def _send_email(self, 
                    email_address,
                    user): 
        """ Helper to send emails via ses 

            if the email is sent successfully, it returns True 
            if something goes wrong it logs, and raises an exception
        """ 
        kwargs = {}
        click_me_url = '%s/user/reset/token/%s/username/%s/' % (
            self.origin, 
            user.reset_password_token,
            user.username) 
        kwargs['to_addresses'] = email_address
        kwargs['subject'] = 'Password Reset Request' 
             
        kwargs['body'] = """<html>
                              <body>
                                <p><a href="https://www.neon-lab.com">
                                <img class="logo"
                                 style = "height: 43.25px; width: 104px;"  
                                 src="https://s3.amazonaws.com/neon_website_assets/logo_777.png" 
                                 alt="Neon">
                                </a></p>
                                <br> 
                                <p>Hi {first_name},</p>
                                <p>
                                  Sorry to hear, you've forgotten your 
                                  password. But, have no fear the link to 
                                  reset it is right below. This link will 
                                  be good for the next hour. 
                                  <p>
                                    <a style="color: #f16122" href="{url}">
                                      Reset Your Password
                                    </a>
                                  </p> 
                                </p>
                                <p>
                                  You have received this message because 
                                  someone requested to have the password 
                                  reset for this user. If this was not you,
                                  don't worry. Someone may have typed in 
                                  your username by mistake. 
                                </p>
                                <p> 
                                  Nobody will be able to change your password
                                  without the token contained in the link above. 
                                  When you have completed the password change, 
                                  you can delete this email. 
                                </p>
                                <p>Thanks,<br>
                                  The Neon Team
                                </p>
                                <p style="font-size:smaller">
                                   If the reset password link does not work 
                                   please copy and paste the following address 
                                   into your browser : {url} 
                                </p> 
                                <p>------------</p>
                                <p class="footer" style="font-size: smaller">Neon Labs Inc.<br>
                                70 South Park St. | San Francisco, CA 94107<br>
                                (415) 355-4249<br>
                                <a style="color: #f16122" href="https://www.neon-lab.com">neon-lab.com</a>
                                </p>
                                <p class="footer" style="font-size: smaller">This is an automated email. 
                                Please get in touch with us at 
                                <a href="mailto:ask@neon-lab.com">
                                  ask@neon-lab.com</a></p>
                              </body>
                            </html>""".format(
                                first_name=user.first_name, 
                                url=click_me_url, 
                                username=user.username) 

        kwargs['source'] = 'Neon Forgot Your Password <noreply@neon-lab.com>' 
        kwargs['reply_addresses'] = 'noreply@neon-lab.com' 
        kwargs['format'] = 'html' 
        ses = boto.connect_ses()
        try: 
            ses.send_email(**kwargs)
        except Exception as e: 
            _log.error('Failed to Send Reset Password email to %s exc_info %s' % 
                (email_address, e))
            raise Exception('unable to send reset password email')
 
        return True
 
    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.NONE
               }
         
'''*********************************************************************
Endpoints 
*********************************************************************'''
application = tornado.web.Application([
    (r'/healthcheck/?$', HealthCheckHandler),
    (r'/api/v2/authenticate/?$', AuthenticateHandler),
    (r'/api/v2/refresh_token/?$', RefreshTokenHandler),
    (r'/api/v2/accounts/?$', NewAccountHandler),
    (r'/api/v2/accounts/verify?$', VerifyAccountHandler),
    (r'/api/v2/users/?$', UserHandler),
    (r'/api/v2/users/forgot_password?$', ForgotPasswordHandler),
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
