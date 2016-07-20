#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from apiv2 import *
from datetime import datetime, timedelta
from passlib.hash import sha256_crypt
from voluptuous import RequiredFieldInvalid

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

statemon.define('user_signup_warning', int)

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
           Required('username'): All(Coerce(str), Length(min=3, max=128)),
           Required('password'): All(Coerce(str), Length(min=8, max=64)),
        })
        args = self.parse_args()
        schema(args)
        username = args.get('username').lower()
        password = args.get('password')

        api_accessor = yield neondata.User.get(username, async=True)
        access_token, refresh_token = AccountHelper.get_auth_tokens(
            {'username': username})

        def _update_tokens(x):
            x.access_token = access_token
            x.refresh_token = refresh_token

        result = None
        if api_accessor:
            if not api_accessor.is_email_verified():
                raise NotAuthorizedError('Email needs verification')

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

            if username:
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
            if not user:
                raise NotFoundError('No user found for this username')

            account_ids = yield user.get_associated_account_ids(async=True)
            if not account_ids:
                raise HTTPError('User has no associated account')

            access_token = JWTHelper.generate_token(
                {'username': username,
                 'account_id': account_ids[0]},
                token_type=TokenTypes.ACCESS_TOKEN)

            def _update_user(u):
                u.access_token = access_token

            yield neondata.User.modify(username,
                _update_user,
                async=True)

            result = {
                'access_token': access_token,
                'refresh_token': refresh_token,
                'account_ids': account_ids}

            self.success(json.dumps(result))

        except jwt.ExpiredSignatureError:
            raise NotAuthorizedError('refresh token has expired, please authenticate again')
        except (jwt.InvalidTokenError, KeyError):
            raise NotAuthorizedError('refresh token invalid, please authenticate again')

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

    The behavior here depends on if user provides email address.
    """
    @tornado.gen.coroutine
    def post(self):
        """Create a new account

        If parameter "email" is set, then a verification object is saved and
        the verification email is sent to the user.

        If not, the loginless, session-based account flow is started.
        Verification is deferred until the user claims an email address.
        """
        schema = Schema({
            Optional('email'): CustomVoluptuousTypes.Email(),
            Optional('admin_user_username'): All(CustomVoluptuousTypes.Email(), Length(min=6, max=512)),
            Optional('admin_user_password'): All(Coerce(str), Length(min=8, max=64)),
            'customer_name': All(Coerce(str), Length(min=1, max=1024)),
            'default_width': All(Coerce(int), Range(min=1, max=8192)),
            'default_height': All(Coerce(int), Range(min=1, max=8192)),
            'default_thumbnail_id': All(Coerce(str), Length(min=1, max=2048)),
            'admin_user_first_name': All(Coerce(str), Length(min=1, max=256)),
            'admin_user_last_name': All(Coerce(str), Length(min=1, max=256)),
            'admin_user_title': All(Coerce(str), Length(min=1, max=32))
        })
        args = self.parse_args()
        schema(args)

        # If email is valued, then admin fields must be.
        if args.get('email'):
            requires_schema = Schema({
                Required('email'): CustomVoluptuousTypes.Email(),
                Required('admin_user_username'): All(CustomVoluptuousTypes.Email(), Length(min=6, max=512)),
                Required('admin_user_password'): All(Coerce(str), Length(min=8, max=64)),
            }, extra=ALLOW_EXTRA)
            schema(args)
        # If not email, then provide a loginless account.
        else:
            account, user = yield AccountHelper.save_loginless_account()

            # Generate and return tokens.
            access_token, refresh_token = AccountHelper.get_auth_tokens({
                'username': account.get_id(),
                'account_id': account.get_id()})
            self.success({
                'account_ids': [account.get_id()],
                'access_token': access_token,
                'refresh_token': refresh_token})
            return

        account = AccountHelper.create_account_with_args(args)

        # Check if email is claimed.
        username = args['admin_user_username'].lower()
        password = args['admin_user_password']
        is_address_claimed = yield AccountHelper.is_address_claimed(username)
        if is_address_claimed:
            raise AlreadyExists('User with that email already exists.')

        # Send verification email.
        user = neondata.User(
            username=username,
            password=password,
            access_level = neondata.AccessLevels.ADMIN,
            first_name = args.get('admin_user_first_name', None),
            last_name = args.get('admin_user_last_name', None),
            title = args.get('admin_user_title', None))
        account.users.append(username)
        yield AccountHelper.user_wants_verification(
            account=account,
            user=user,
            origin=self.origin,
            executor=self.executor)
        msg = 'Account verification email sent to %s' % account.email
        self.success({'message': msg})

    @classmethod
    def get_access_levels(self):
        return {HTTPVerbs.POST: neondata.AccessLevels.NONE}

class AccountHelper(object):
    '''Provide stateless account lifecycle functions.'''

    @staticmethod
    def create_empty_account(name=None):
        """Call to instantiate an empty account"""
        return neondata.NeonUserAccount(uuid.uuid1().hex, name=name)

    @staticmethod
    def create_account_with_args(args):
        """Instantiate (don't save) a account object given API arguments

        Input dict args:
            default_width
            default_height
            default_thumbnail_id
            email
        Returns a NeonUserAccount
        """
        account = AccountHelper.create_empty_account(name=args.get('customer_name'))
        account.default_size = list(account.default_size)
        account.default_size[0] = args.get('default_width', neondata.DefaultSizes.WIDTH)
        account.default_size[1] = args.get('default_height', neondata.DefaultSizes.HEIGHT)
        account.default_size = tuple(account.default_size)
        account.default_thumbnail_id = args.get('default_thumbnail_id', None)
        account.email = args.get('email')
        return account

    @staticmethod
    @tornado.gen.coroutine
    def save_loginless_account():
        """Add an account that is kept via the session cookie."""

        # Save anonymous Neon user account.
        account = neondata.NeonUserAccount(
            uuid.uuid1().hex,
            serving_enabled=False)
        account.users = [account.get_id()]
        yield account.save(async=True)
        # Save the initial admin user.
        user = neondata.User(
            account.get_id(),
            access_level=neondata.AccessLevels.ADMIN,
            email_verified=False)
        yield user.save(async=True)

        # Save other account objects that are based on the Neon api key.
        yield AccountHelper.save_default_objects(account)

        raise tornado.gen.Return((account, user))

    @staticmethod
    @tornado.gen.coroutine
    def is_address_claimed(email):
        """Ask if the email address has been claimed

        Input string email address
        Yields boolean True if claimed
        """
        user = yield neondata.User.get(email, async=True)
        raise tornado.gen.Return(user is not None)

    @staticmethod
    @tornado.gen.coroutine
    def user_verifies_address(verifier):
        """Handle user's verification of an email address

        Input verifier, a neondata.Verification
        Yields the NeonUserAccount, after save to db
        """

        # Instantiate account and user from payload in verifier.
        account = neondata.NeonUserAccount.create(
            verifier.extra_info['account'])
        user_json = json.loads(verifier.extra_info['user'])
        user = neondata.User._create(
            user_json['_data']['key'],
            user_json)
        user.email_verified = True

        # Enable this for Mastermind serving.
        account.serving_enabled = True
        # Bump up the processing priority
        account.processing_priority = 1

        # Let database confirm email's uniqueness.
        try:
            yield user.save(overwrite_existing_object=False, async=True)

            # Clean up any placeholder.
            if(user.get_id() in account.users):
                account.users.remove(user.get_id())
                yield user.delete(account.get_id(), async=True)
        except neondata.psycopg2.IntegrityError:
            raise AlreadyExists('User with that email already exists.')

        # Save other user objects (ExperimentStrategy, AccountLimits, etc.).
        account_limits, _, _, _ = yield AccountHelper.save_default_objects(account)

        # Increase a user account video limit if it's below the demo amount.
        limit_minimum = neondata.AccountLimits.MAX_VIDEOS_ON_DEMO_SIGNUP
        if account_limits.max_video_posts < limit_minimum:
            def _modify(limits):
                limits.max_video_posts = neondata.AccountLimits.MAX_VIDEOS_ON_DEMO_SIGNUP
            limit_success = yield neondata.AccountLimits.modify(
                account.get_id(),
                _modify,
                async=True)
            if not limit_success:
                _log.warn('Did not modify demo account limits for %s' %
                    account.get_id())
                statemon.state.increment('user_signup_warning')

        # Save account and return it.
        account.users.append(user.get_id())
        account.email = account.email if account.email else user.username
        yield account.save(async=True)
        raise tornado.gen.Return(account)

    @staticmethod
    @tornado.gen.coroutine
    def save_default_objects(account):
        """Save objects that back an empty, new account

        This can be called safely if the objects are already created.

        Args account, a NeonUserAccount
        Yields None
        """

        # Create or update trackers.
        tracker_p_aid_mapper = neondata.TrackerAccountIDMapper(
            account.tracker_account_id,
            account.neon_api_key,
            neondata.TrackerAccountIDMapper.PRODUCTION)
        yield tracker_p_aid_mapper.save(async=True)
        tracker_s_aid_mapper = neondata.TrackerAccountIDMapper(
            account.staging_tracker_account_id,
            account.neon_api_key,
            neondata.TrackerAccountIDMapper.STAGING)
        yield tracker_s_aid_mapper.save(async=True)

        # Set account to have the demo billing plan and limits.
        billing_plan = yield neondata.BillingPlans.get(
            neondata.BillingPlans.PLAN_DEMO,
            async=True)
        account_limits = neondata.AccountLimits(account.neon_api_key)
        account_limits.populate_with_billing_plan(billing_plan)
        try:
            yield account_limits.save(async=True)
        except neondata.psycopg2.IntegrityError:
            pass

        # Add the default experiment strategy.
        experiment_strategy = neondata.ExperimentStrategy(
            account.neon_api_key)
        try:
            yield experiment_strategy.save(
                overwrite_existing_object=False,
                async=True)
        except neondata.psycopg2.IntegrityError:
            pass

        raise tornado.gen.Return((
            account_limits,
            experiment_strategy,
            tracker_p_aid_mapper,
            tracker_s_aid_mapper
        ))

    @staticmethod
    @tornado.gen.coroutine
    def user_wants_verification(account, user, origin, executor):
        """Handle when user wants to verify an email address.

        This saves a verification token.
        """

        # Gather values for Verification object.
        info = {}
        info['account'] = account.to_json()
        info['user'] = user.to_json()
        token = JWTHelper.generate_token(
            {'email' : account.email},
            token_type=TokenTypes.VERIFY_TOKEN)

        # Keep track of token.
        verifier = neondata.Verification(
            account.email,
            token=token,
            extra_info=info)
        yield verifier.save(async=True)

        # Send email.
        rv = yield AccountHelper.send_verification_email(
            account=account,
            user=user,
            token=token,
            origin=origin,
            executor=executor)
        raise tornado.gen.Return(rv)

    @staticmethod
    @tornado.gen.coroutine
    def send_verification_email(account, user, token, origin, executor):
        """Send verification email.

        If the email is sent successfully, it returns True.
        If something goes wrong, it logs and raises an exception.
        """
        temp_args = {}
        url = '%s/account/confirm?token=%s' % (origin, token)
        temp_args['url'] = url 
        temp_args['first_name'] = user.first_name
        temp_args['username'] = user.username      
   
        yield MandrillEmailSender.send_mandrill_email(
            account.email, 
            'verify-account', 
            template_args=temp_args)

        raise tornado.gen.Return(True)

    @staticmethod
    def get_auth_tokens(payload):
        """Generate token pair (access, refresh) encoding dict payload"""
        return (
            JWTHelper.generate_token(payload.copy(), token_type=TokenTypes.ACCESS_TOKEN),
            JWTHelper.generate_token(payload.copy(), token_type=TokenTypes.REFRESH_TOKEN))


'''****************************************************************
VerifyAccountHandler
****************************************************************'''
class VerifyAccountHandler(APIV2Handler):
    """Handle a user's verification of an email address

       If we find a valid email that matches the token in our database, we will
       mark the user as verified. If the NeonUserAccount or either
       TrackerAccountIDMapper is not in the database, then save it.

       On failure, raise error code.
    """
    @tornado.gen.coroutine
    def post(self):
        """Validate token then create user objects."""
        schema = Schema({
            Required('token') : All(Coerce(str), Length(min=1, max=512))
        })

        args = self.parse_args(keep_token=True)
        schema(args)
        verify_token = args.get('token')
        try:
            # Check if we recognize the encoded email address.
            payload = JWTHelper.decode_token(verify_token)
            email = payload['email']
            verifier = yield neondata.Verification.get(email, async=True)
            if not verifier:
                raise NotFoundError('Unrecognized email {}.'.format(email))
        except jwt.ExpiredSignatureError:
            raise NotAuthorizedError('Verify token has expired.')

        # Associate account with address and save all user objects.
        account = yield AccountHelper.user_verifies_address(verifier)

        # Success.
        statemon.state.increment('post_account_oks')
        dict_account = yield self.db2api(account)
        _log.debug(('New Account has been added : name = %s id = %s')
                   % (dict_account['customer_name'],
                      dict_account['account_id']))
        self.success(dict_account)

    @classmethod
    def get_access_levels(self):
        return {HTTPVerbs.POST: neondata.AccessLevels.NONE}

    @classmethod
    def _get_default_returned_fields(cls):
        return ['account_id', 'default_size', 'customer_name',
                'default_thumbnail_id', 'tracker_account_id',
                'staging_tracker_account_id', 'integration_ids', 'created',
                'updated', 'users', 'serving_enabled', 'email']

    @classmethod
    def _get_passthrough_fields(cls):
        return ['default_size', 'default_thumbnail_id', 'tracker_account_id',
                'staging_tracker_account_id', 'created', 'updated', 'users',
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
UserHandler
****************************************************************'''
class UserHandler(APIV2Handler):
    """Handles post requests to the user endpoint."""
    @tornado.gen.coroutine
    def post(self):
        """handles user endpoint post request"""

        schema = Schema({
            Required('username'): CustomVoluptuousTypes.Email(),
            Required('password'): All(Coerce(str), Length(min=8, max=64)),
            'access_level' : All(Coerce(int), Range(min=1, max=31)),
            'first_name': All(Coerce(unicode), Length(min=1, max=256)),
            'last_name': All(Coerce(unicode), Length(min=1, max=256)),
            'secondary_email': CustomVoluptuousTypes.Email(),
            'cell_phone_number': All(Coerce(str), Length(min=1, max=32)),
            'title': All(Coerce(unicode), Length(min=1, max=32))
        })

        args = self.parse_args()
        schema(args)

        # Validate the email address is not claimed.
        username = args['username'].lower()
        is_address_claimed = yield AccountHelper.is_address_claimed(username)
        if is_address_claimed:
            raise AlreadyExists('User with that email already exists.')

        # Get the account in the authorization token payload.
        payload = JWTHelper.decode_token(self.access_token)
        account_id = payload['account_id']
        account = yield neondata.NeonUserAccount.get(account_id, async=True)
        if not account:
            raise NotAuthorizedError('This requires an account.')

        # Instantiate a user to store in the verification payload.
        user = neondata.User(
            username=username,
            password=args.get('password'),
            access_level=neondata.AccessLevels.ADMIN,
            first_name=args.get('first_name'),
            last_name=args.get('last_name'),
            secondary_email=args.get('secondary_email'),
            cell_phone_number=args.get('cell_phone_number'),
            title=args.get('title'))

        # Create and send verification.
        account.users = [username]
        account.email = username
        yield AccountHelper.user_wants_verification(
            account=account,
            user=user,
            origin=self.origin,
            executor=self.executor)

        # Respond with a sent email message.
        msg = 'Account verification email sent to %s' % account.email
        self.success({'message': msg})

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
            HTTPVerbs.POST: neondata.AccessLevels.CREATE,
            HTTPVerbs.PUT: neondata.AccessLevels.NONE}

    @classmethod
    def _get_default_returned_fields(cls):
        return ['username', 'created', 'updated',
                'first_name', 'last_name', 'title',
                'secondary_email', 'cell_phone_number']

    @classmethod
    def _get_passthrough_fields(cls):
        return cls._get_default_returned_fields()


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
            yield self._send_email(email_address, user)
            msg = 'Reset Password email sent to %s' % email_address
            self.success({'message' : msg})
    
    @tornado.gen.coroutine
    def _send_email(self,
                    email_address,
                    user):
        """ Helper to send emails via ses

            if the email is sent successfully, it returns True
            if something goes wrong it logs, and raises an exception
        """
        click_me_url = '%s/user/reset/token/%s/username/%s/' % (
            self.origin,
            user.reset_password_token,
            user.username)

        temp_args = {}
        temp_args['url'] = click_me_url 
        temp_args['first_name'] = user.first_name
        temp_args['username'] = user.username      
   
        yield MandrillEmailSender.send_mandrill_email(
            email_address, 
            'reset-password', 
            template_args=temp_args)

        raise tornado.gen.Return(True) 

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
    (r'/api/v2/users/forgot_password/?$', ForgotPasswordHandler),
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
