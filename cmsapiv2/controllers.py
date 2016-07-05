#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from apiv2 import *
import api.brightcove_api
import numpy as np
import PIL.Image
import io
import StringIO

import cmsapiv2.client
import fractions
import logging
import model
import utils.autoscale
import video_processor.video_processing_queue
_log = logging.getLogger(__name__)

define("port", default=8084, help="run on the given port", type=int)
define("cmsapiv1_port", default=8083, help="what port apiv1 is running on", type=int)
define("send_mandrill_emails", 
    default=1, 
    help="should we actually send the email", 
    type=str) 
define("mandrill_api_key", 
    default='Y7N4ELi5hMDp_RbTQH9OqQ', 
    help="key from mandrillapp.com used to make api calls", 
    type=str)
define("mandrill_base_url", 
    default='https://mandrillapp.com/api/1.0', 
    help="mandrill base api url", 
    type=str)

# For scoring non-video thumbnails.
define('model_server_port', default=9000, type=int,
       help='the port currently being used by model servers')
define('model_autoscale_groups', default='AquilaOnDemand', type=str,
       help='Comma separated list of autoscaling group names')
define('request_concurrency', default=22, type=int,
       help=('the maximum number of concurrent scoring requests to'
             ' make at a time. Should be less than or equal to the'
             ' server batch size.'))

statemon.define('put_account_oks', int)
statemon.define('get_account_oks', int)

statemon.define('post_ooyala_oks', int)
statemon.define('put_ooyala_oks', int)
statemon.define('get_ooyala_oks', int)

statemon.define('post_brightcove_oks', int)
statemon.define('put_brightcove_oks', int)
statemon.define('get_brightcove_oks', int)

statemon.define('put_brightcove_player_oks', int)
statemon.define('get_brightcove_player_oks', int)
statemon.define('brightcove_publish_plugin_error', int)

statemon.define('post_thumbnail_oks', int)
statemon.define('put_thumbnail_oks', int)
statemon.define('get_thumbnail_oks', int)

statemon.define('post_video_oks', int)
statemon.define('put_video_oks', int)
statemon.define('get_video_oks', int)
_get_video_oks_ref = statemon.state.get_ref('get_video_oks')

statemon.define('mandrill_template_not_found', int)
statemon.define('mandrill_email_not_sent', int)

'''*****************************************************************
AccountHandler
*****************************************************************'''
class AccountHandler(APIV2Handler):
    """Handles get,put requests to the account endpoint.
       Gets and updates existing accounts.
    """
    @tornado.gen.coroutine
    def get(self, account_id):
        """handles account endpoint get request"""

        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })

        args = {}
        args['account_id'] = account_id = str(account_id)
        schema(args)

        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        user_account = yield tornado.gen.Task(neondata.NeonUserAccount.get, account_id)

        if not user_account:
            raise NotFoundError()

        user_account = yield self.db2api(user_account, fields=fields)
        statemon.state.increment('get_account_oks')
        self.success(user_account)

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles account endpoint put request"""

        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
          'default_width': All(Coerce(int), Range(min=1, max=8192)),
          'default_height': All(Coerce(int), Range(min=1, max=8192)),
          'default_thumbnail_id': All(Coerce(str), Length(min=1, max=2048))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct_internal = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                                               args['account_id'])
        if not acct_internal:
            raise NotFoundError()

        acct_for_return = yield self.db2api(acct_internal)
        def _update_account(a):
            a.default_size = list(a.default_size)
            a.default_size[0] = int(args.get('default_width',
                                             acct_internal.default_size[0]))
            a.default_size[1] = int(args.get('default_height',
                                             acct_internal.default_size[1]))
            a.default_size = tuple(a.default_size)
            a.default_thumbnail_id = args.get(
                'default_thumbnail_id',
                acct_internal.default_thumbnail_id)

        yield tornado.gen.Task(neondata.NeonUserAccount.modify,
                                        acct_internal.key, _update_account)
        statemon.state.increment('put_account_oks')
        self.success(acct_for_return)

    @classmethod
    def get_access_levels(cls):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 HTTPVerbs.PUT: neondata.AccessLevels.UPDATE,
                 'account_required': [HTTPVerbs.GET, HTTPVerbs.PUT]
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


'''*********************************************************************
IntegrationHelper
*********************************************************************'''
class IntegrationHelper():
    """Class responsible for helping the integration handlers."""

    @staticmethod
    @tornado.gen.coroutine
    def create_integration(acct, args, integration_type, cdn=None):
        """Creates an integration for any integration type.

        Keyword arguments:
        acct - a NeonUserAccount object
        args - the args sent in via the API request
        integration_type - the type of integration to create
        schema - validate args with this Voluptuous schema
        cdn - an optional CDNHostingMetadata object to intialize the
              CDNHosting with
        """

        integration = None
        if integration_type == neondata.IntegrationType.OOYALA:
            integration = neondata.OoyalaIntegration()
            integration.account_id = acct.neon_api_key
            integration.partner_code = args['publisher_id']
            integration.api_key = args.get('api_key', integration.api_key)
            integration.api_secret = args.get('api_secret', integration.api_secret)

        elif integration_type == neondata.IntegrationType.BRIGHTCOVE:
            integration = neondata.BrightcoveIntegration()
            integration.account_id = acct.neon_api_key
            integration.publisher_id = args['publisher_id']

            integration.read_token = args.get(
                'read_token',
                integration.read_token)
            integration.write_token = args.get(
                'write_token',
                integration.write_token)
            integration.application_client_id = args.get(
                'application_client_id',
                integration.application_client_id)
            integration.application_client_secret = args.get(
                'application_client_secret',
                integration.application_client_secret)
            integration.callback_url = args.get(
                'callback_url',
                integration.callback_url)
            playlist_feed_ids = args.get('playlist_feed_ids', None)

            if playlist_feed_ids:
                integration.playlist_feed_ids = playlist_feed_ids.split(',')

            integration.id_field = args.get(
                'id_field',
                integration.id_field)
            integration.uses_batch_provisioning = Boolean()(args.get(
                'uses_batch_provisioning',
                integration.uses_batch_provisioning))
            integration.uses_bc_gallery = Boolean()(args.get(
                'uses_bc_gallery',
                integration.uses_bc_gallery))
            integration.uses_bc_thumbnail_api = Boolean()(args.get(
                'uses_bc_thumbnail_api',
                integration.uses_bc_thumbnail_api))
            integration.uses_bc_videojs_player = Boolean()(args.get(
                'uses_bc_videojs_player',
                integration.uses_bc_videojs_player))
            integration.uses_bc_smart_player = Boolean()(args.get(
                'uses_bc_smart_player',
                integration.uses_bc_smart_player))
            integration.last_process_date = args.get(
                'last_process_date',
                integration.last_process_date)
        else:
            raise ValueError('Unknown integration type')

        if cdn:
            cdn_list = neondata.CDNHostingMetadataList(
                neondata.CDNHostingMetadataList.create_key(
                    acct.neon_api_key,
                    integration.get_id()),
                [cdn])
            success = yield cdn_list.save(async=True)
            if not success:
                raise SaveError('unable to save CDN hosting')

        success = yield integration.save(async=True)
        if not success:
            raise SaveError('unable to save Integration')

        raise tornado.gen.Return(integration)

    @staticmethod
    @tornado.gen.coroutine
    def get_integration(integration_id, integration_type):
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
    def get_integrations(account_id):
        """ gets all integrations for an account.

        Keyword arguments
        account_id - the account_id that is associated with the integrations
        """
        user_account = yield neondata.NeonUserAccount.get(
            account_id,
            async=True)

        if not user_account:
            raise NotFoundError()

        integrations = yield user_account.get_integrations(async=True)
        rv = {}
        rv['integrations'] = []
        for i in integrations:
           new_obj = None
           if type(i).__name__.lower() == neondata.IntegrationType.BRIGHTCOVE:
               new_obj = yield BrightcoveIntegrationHandler.db2api(i)
               new_obj['type'] = 'brightcove'
           elif type(i).__name__.lower() == neondata.IntegrationType.OOYALA:
               new_obj = yield OoyalaIntegrationHandler.db2api(i)
               new_obj['type'] = 'ooyala'
           else:
               continue

           if new_obj:
               rv['integrations'].append(new_obj)

        raise tornado.gen.Return(rv)

    @staticmethod
    @tornado.gen.coroutine
    def validate_oauth_credentials(client_id, client_secret, integration_type):
        if integration_type is neondata.IntegrationType.BRIGHTCOVE:
            if client_id and not client_secret:
                raise BadRequestError(
                    'App id cannot be valued if secret is not also valued')
            if client_secret and not client_id:
                raise BadRequestError(
                    'App secret cannot be valued if id is not also valued')
            # TODO validate with BC that keys are valid and the granted
            # permissions are as expected. (This is implemented in the
            # Oauth feature branch. Need to invoke it here after merge)
        elif integration_type is neondata.IntegrationType.OOYALA:
            # Implement for Ooyala
            pass

'''*********************************************************************
OoyalaIntegrationHandler
*********************************************************************'''
class OoyalaIntegrationHandler(APIV2Handler):
    """Handles get,put,post requests to the ooyala endpoint within the v2 api."""
    @tornado.gen.coroutine
    def post(self, account_id):
        """Handles an ooyala endpoint post request

        Keyword arguments:
        """
        schema = Schema({
            Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
            Required('publisher_id') : All(Coerce(str), Length(min=1, max=256)),
            'api_key': All(Coerce(str), Length(min=1, max=1024)),
            'api_secret': All(Coerce(str), Length(min=1, max=1024))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)

        acct = yield neondata.NeonUserAccount.get(
            args['account_id'],
            async=True)
        integration = yield tornado.gen.Task(
            IntegrationHelper.create_integration, acct, args,
            neondata.IntegrationType.OOYALA)
        statemon.state.increment('post_ooyala_oks')
        rv = yield self.db2api(integration)
        self.success(rv)

    @tornado.gen.coroutine
    def get(self, account_id):
        """handles an ooyala endpoint get request"""

        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
          Required('integration_id'): All(Coerce(str), Length(min=1, max=256)),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)

        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        integration_id = args['integration_id']
        integration = yield IntegrationHelper.get_integration(
            integration_id,
            neondata.IntegrationType.OOYALA)

        statemon.state.increment('get_ooyala_oks')
        rv = yield self.db2api(integration, fields=fields)
        self.success(rv)

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles an ooyala endpoint put request"""

        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
          Required('integration_id'): All(Coerce(str), Length(min=1, max=256)),
          'api_key': All(Coerce(str), Length(min=1, max=1024)),
          'api_secret': All(Coerce(str), Length(min=1, max=1024)),
          'publisher_id': All(Coerce(str), Length(min=1, max=1024))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id']

        integration = yield IntegrationHelper.get_integration(
            integration_id, neondata.IntegrationType.OOYALA)

        def _update_integration(p):
            p.api_key = args.get('api_key', integration.api_key)
            p.api_secret = args.get('api_secret', integration.api_secret)
            p.partner_code = args.get('publisher_id', integration.partner_code)

        yield neondata.OoyalaIntegration.modify(
            integration_id, _update_integration, async=True)

        yield IntegrationHelper.get_integration(
            integration_id, neondata.IntegrationType.OOYALA)

        statemon.state.increment('put_ooyala_oks')
        rv = yield self.db2api(integration)
        self.success(rv)

    @classmethod
    def _get_default_returned_fields(cls):
        return [ 'integration_id', 'account_id', 'partner_code',
                 'api_key', 'api_secret' ]

    @classmethod
    def _get_passthrough_fields(cls):
        return [ 'integration_id', 'account_id', 'partner_code',
                 'api_key', 'api_secret' ]

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 HTTPVerbs.POST: neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT: neondata.AccessLevels.UPDATE,
                 'account_required': [HTTPVerbs.GET,
                                        HTTPVerbs.PUT,
                                        HTTPVerbs.POST]
               }

'''*********************************************************************
BrightcovePlayerHandler
*********************************************************************'''
class BrightcovePlayerHandler(APIV2Handler):
    """Handle requests to Brightcove player endpoint"""

    @tornado.gen.coroutine
    def get(self, account_id):
        """Get the list of BrightcovePlayers for the given integration"""

        # Validate request and data
        schema = Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('integration_id'): All(Coerce(str), Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id']
        integration = yield neondata.BrightcoveIntegration.get(
            integration_id,
            async=True)
        if not integration:
            raise NotFoundError(
                'BrighcoveIntegration does not exist for player reference:%s',
                args['player_ref'])

        # Retrieve the list of players from Brightcove api
        bc = api.brightcove_api.PlayerAPI(integration)
        r = yield bc.get_players()
        players = [p for p in r.get('items', []) if p['id'] != 'default']

        # @TODO batch transform dict-players to object-players
        objects  = yield map(self._bc_to_obj, players)
        ret_list = yield map(self.db2api, objects)


        # Envelope with players:, player_count:
        response = {
            'players': ret_list,
            'player_count': len(ret_list)
        }
        statemon.state.increment('get_brightcove_player_oks')
        self.success(response)

    @staticmethod
    @tornado.gen.coroutine
    def _bc_to_obj(bc_player):
        '''Retrieve or create a BrightcovePlayer from db given BC data

        If creating object, the object is not saved to the database.
        '''
        # Get the database record. Expect many to be missing, so don't log
        neon_player = yield neondata.BrightcovePlayer.get(
            bc_player['id'],
            async=True,
            log_missing=False)
        if neon_player:
            # Prefer Brightcove's data since it is potentially newer
            neon_player.name = bc_player['name']
        else:
            neon_player = neondata.BrightcovePlayer(
                player_ref=bc_player['id'],
                name=bc_player['name'])
        raise tornado.gen.Return(neon_player)

    @tornado.gen.coroutine
    def put(self, account_id):
        """Update a BrightcovePlayer tracking status and return the player

        Setting the is_tracked flag to True, will also publish the player
        via Brightcove's player management api.
        """

        # The only field that is set via public api is is_tracked.
        schema = Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('integration_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('player_ref'): All(Coerce(str), Length(min=1, max=256)),
            Required('is_tracked'): Boolean()
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        ref = args['player_ref']

        integration = yield neondata.BrightcoveIntegration.get(
            args['integration_id'],
            async=True)
        if not integration:
            raise NotFoundError(
                'BrighcoveIntegration does not exist for integration_id:%s',
                args['integration_id'])
        if integration.account_id != account_id:
            raise NotAuthorizedError('Player is not owned by this account')

        # Verify player_ref is at Brightcove
        bc = api.brightcove_api.PlayerAPI(integration)
        # This will error (expect 404) if player not found
        try:
            bc_player = yield bc.get_player(ref)
        except Exception as e:
            statemon.state.increment('brightcove_publish_plugin_error')
            raise e

        # Get or create db record
        def _modify(p):
            p.is_tracked = Boolean()(args['is_tracked'])
            p.name = bc_player['name'] # BC's name is newer
            p.integration_id = integration.integration_id
        player = yield neondata.BrightcovePlayer.modify(
            ref,
            _modify,
            create_missing=True,
            async=True)
        bc_player_config = bc_player['branches']['master']['configuration']

        # If the player is tracked, then send a request to Brightcove's
        # player managament API to put the plugin in the player
        # and publish the player.  We do this any time the user calls
        # this API with is_tracked=True because they are likely to be
        # troubleshooting their setup and publishing several times.

        # Alternatively, if player is not tracked, then send a request
        # to remove the player from the config and publish the player.
        if player.is_tracked:
            patch = BrightcovePlayerHelper._install_plugin_patch(
                bc_player_config,
                self.account.tracker_account_id)
            try:
                yield BrightcovePlayerHelper.publish_player(ref, patch, bc)
            except Exception as e:
                statemon.state.increment('brightcove_publish_plugin_error')
                raise e
            # Published. Update the player with the date and version
            def _modify(p):
                p.publish_date = datetime.now().isoformat()
                p.published_plugin_version = \
                    BrightcovePlayerHelper._get_current_tracking_version()
                p.last_attempt_result = None
            yield neondata.BrightcovePlayer.modify(ref, _modify, async=True)

        elif player.is_tracked is False:
            patch = BrightcovePlayerHelper._uninstall_plugin_patch(
                bc_player_config)
            if patch:
                try:
                    yield BrightcovePlayerHelper.publish_player(ref, patch, bc)
                except Exception as e:
                    statemon.state.increment('brightcove_publish_plugin_error')
                    raise e

        # Finally, respond with the current version of the player
        player = yield neondata.BrightcovePlayer.get(
            player.get_id(),
            async=True)
        response = yield self.db2api(player)
        statemon.state.increment('put_brightcove_player_oks')
        self.success(response)

    @classmethod
    def get_access_levels(self):
        return {
            HTTPVerbs.GET: neondata.AccessLevels.READ,
            HTTPVerbs.POST: neondata.AccessLevels.CREATE,
            HTTPVerbs.PUT: neondata.AccessLevels.UPDATE,
            'account_required': [HTTPVerbs.GET, HTTPVerbs.PUT, HTTPVerbs.POST]}

    @classmethod
    def _get_default_returned_fields(cls):
        return ['player_ref', 'name', 'is_tracked',
                'created', 'updated', 'publish_date',
                'published_plugin_version', 'last_attempt_result']

    @classmethod
    def _get_passthrough_fields(cls):
        # Player ref is transformed with get_id
        return ['name', 'is_tracked',
                'created', 'updated',
                'publish_date', 'published_plugin_version',
                'last_attempt_result']

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'player_ref':
            # Translate key to player_ref
            raise tornado.gen.Return(obj.get_id())
        raise BadRequestError('invalid field %s' % field)


'''*********************************************************************
BrightcovePlayerHelper
*********************************************************************'''

class BrightcovePlayerHelper():
    '''Contain functions that work on Players that are called internally.'''
    @staticmethod
    @tornado.gen.coroutine
    def publish_player(player_ref, patch, bc_api):
        """Update Brightcove player with patch and publishes it

        Assumes that the BC player referenced by player_ref is valid.

        Input-
        player_ref - Brightcove player reference
        patch - Dictionary of player configuration defined by Brightcove
        bc_api - Instance of Brightcove API with appropriate integration
        """
        yield bc_api.patch_player(player_ref, patch)
        yield bc_api.publish_player(player_ref)

    @staticmethod
    def _install_plugin_patch(player_config, tracker_account_id):
        """Make a patch that replaces our js and json with the current version

        Brightcove player's configuration api allows PUT to replace the entire
        configuration branch (master or preview). It allows and recommends PATCH
        to set any subset of fields. For our goal, the "plugins" field is a list
        that will be changed to a json payload that includes the Neon account id
        for tracking. The "scripts" field is a list of urls that includes our
        our minified javascript plugin url.

        Grabs the current values of the lists to change, removes any Neon info,
        then addends the Neon js url and json values with current ones.

        Inputs-
        player_config dict containing a configuration branch from Brightcove
        tracker_account_id neon tracking id for the publisher
        """

        # Remove Neon plugins from the config
        patch = BrightcovePlayerHelper._uninstall_plugin_patch(player_config)
        patch = patch if patch else {'scripts': [], 'plugins': []}

        # Append the current plugin
        patch['plugins'].append(BrightcovePlayerHelper._get_current_tracking_json(
            tracker_account_id))
        patch['scripts'].append(BrightcovePlayerHelper._get_current_tracking_url())

        return patch

    @staticmethod
    def _uninstall_plugin_patch(player_config):
        """Make a patch that removes any Neon plugin js or json"""
        plugins = [plugin for plugin in player_config.get('plugins')
            if plugin['name'] != 'neon']
        scripts = [script for script in player_config.get('scripts')
            if script.find('videojs-neon-') == -1]

        # If nothing changed, signal to caller no need to patch.
        if(len(plugins) == len(player_config['plugins']) and
                len(scripts) == len(player_config['scripts'])):
            return None

        return {
            'plugins': plugins,
            'scripts': scripts
        }

    @staticmethod
    def _get_current_tracking_version():
        """Get the version of the current tracking plugin"""
        return '0.0.1'

    @staticmethod
    def _get_current_tracking_url():
        """Get the url of the current tracking plugin"""
        return 'https://s3.amazonaws.com/neon-cdn-assets/videojs-neon-plugin.min.js'

    @staticmethod
    def _get_current_tracking_json(tracker_account_id):
        """Get JSON string that configures the plugin given the account_id

        These are options that injected into the plugin environment and override
        its defaults. { name, options { publisher { id }}} are required. Other
        flags can be found in the neon-videojs-plugin js."""

        return {
            'name': 'neon',
            'options': {
                'publisher': {
                    'id': tracker_account_id
                }
            }
        }

'''*********************************************************************
BrightcoveIntegrationHandler
*********************************************************************'''
class BrightcoveIntegrationHandler(APIV2Handler):
    """handles all requests to the brightcove endpoint within the v2 API"""
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a brightcove endpoint post request"""

        schema = Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('publisher_id'): All(Coerce(str), Length(min=1, max=256)),
            'read_token': All(Coerce(str), Length(min=1, max=512)),
            'write_token': All(Coerce(str), Length(min=1, max=512)),
            'application_client_id': All(Coerce(str), Length(min=1, max=1024)),
            'application_client_secret': All(Coerce(str), Length(min=1, max=1024)),
            'callback_url': All(Coerce(str), Length(min=1, max=1024)),
            'id_field': All(Coerce(str), Length(min=1, max=32)),
            'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
            'uses_batch_provisioning': Boolean(),
            'uses_bc_thumbnail_api': Boolean(),
            'uses_bc_videojs_player': Boolean(),
            'uses_bc_smart_player': Boolean(),
            Required('uses_bc_gallery'): Boolean()
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        publisher_id = args.get('publisher_id')

        # Check credentials with Brightcove's CMS API.
        client_id = args.get('application_client_id')
        client_secret = args.get('application_client_secret')
        IntegrationHelper.validate_oauth_credentials(
            client_id=client_id,
            client_secret=client_secret,
            integration_type=neondata.IntegrationType.BRIGHTCOVE)

        acct = yield neondata.NeonUserAccount.get(
            args['account_id'],
            async=True)

        if not acct:
            raise NotFoundError('Neon Account required.')

        app_id = args.get('application_client_id', None)
        app_secret = args.get('application_client_secret', None)

        if app_id or app_secret:
            # Check credentials with Brightcove's CMS API.
            IntegrationHelper.validate_oauth_credentials(
                client_id=app_id,
                client_secret=app_secret,
                integration_type=neondata.IntegrationType.BRIGHTCOVE)
            # Excecute a search and get last_processed_date

            lpd = yield self._get_last_processed_date(
                publisher_id,
                app_id,
                app_secret)

            if lpd:
                args['last_process_date'] = lpd
            else:
                raise BadRequestError('Brightcove credentials are bad, ' \
                    'application_id or application_secret are wrong.')

        cdn = None
        if Boolean()(args['uses_bc_gallery']):
            # We have a different set of image sizes to generate for
            # Gallery, so setup the CDN
            cdn = neondata.NeonCDNHostingMetadata(
                rendition_sizes = [
                    [120, 67],
                    [120, 90],
                    [160, 90],
                    [160, 120],
                    [210, 118],
                    [320, 180],
                    [374, 210],
                    [320, 240],
                    [460, 260],
                    [480, 270],
                    [622, 350],
                    [480, 360],
                    [640, 360],
                    [640, 480],
                    [960, 540],
                    [1280, 720]])
            args['uses_bc_thumbnail_api'] = True

        integration = yield IntegrationHelper.create_integration(
            acct,
            args,
            neondata.IntegrationType.BRIGHTCOVE,
            cdn=cdn)

        statemon.state.increment('post_brightcove_oks')
        rv = yield self.db2api(integration)
        self.success(rv)

    @tornado.gen.coroutine
    def get(self, account_id):
        """handles a brightcove endpoint get request"""

        schema = Schema({
            Required('account_id') : All(Coerce(str),
                Length(min=1, max=256)),
            Required('integration_id') : All(Coerce(str),
                Length(min=1, max=256)),
            'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)

        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        integration_id = args['integration_id']
        integration = yield IntegrationHelper.get_integration(
            integration_id,
            neondata.IntegrationType.BRIGHTCOVE)
        statemon.state.increment('get_brightcove_oks')
        rv = yield self.db2api(integration, fields=fields)
        self.success(rv)

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a brightcove endpoint put request"""

        schema = Schema({
            Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
            Required('integration_id') : All(Coerce(str), Length(min=1, max=256)),
            'read_token': All(Coerce(str), Length(min=1, max=1024)),
            'write_token': All(Coerce(str), Length(min=1, max=1024)),
            'application_client_id': All(Coerce(str), Length(min=1, max=1024)),
            'application_client_secret': All(Coerce(str), Length(min=1, max=1024)),
            'callback_url': All(Coerce(str), Length(min=1, max=1024)),
            'publisher_id': All(Coerce(str), Length(min=1, max=512)),
            'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
            'uses_batch_provisioning': Boolean(),
            'uses_bc_thumbnail_api': Boolean(),
            'uses_bc_videojs_player': Boolean(),
            'uses_bc_smart_player': Boolean()
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        integration_id = args['integration_id']
        schema(args)

        integration = yield IntegrationHelper.get_integration(
            integration_id,
            neondata.IntegrationType.BRIGHTCOVE)

        # Check credentials with Brightcove's CMS API.
        app_id = args.get('application_client_id', None)
        app_secret = args.get('application_client_secret', None)
        if app_id and app_secret:
            IntegrationHelper.validate_oauth_credentials(
                app_id,
                app_secret,
                neondata.IntegrationType.BRIGHTCOVE)

            # just run a basic search to see that the creds are ok
            lpd = yield self._get_last_processed_date(
                integration.publisher_id,
                app_id,
                app_secret)

            if not lpd:
                raise BadRequestError('Brightcove credentials are bad, ' \
                    'application_id or application_secret are wrong.')

        def _update_integration(p):
            p.read_token = args.get('read_token', integration.read_token)
            p.write_token = args.get('write_token', integration.write_token)
            p.application_client_id = app_id or \
                integration.application_client_id
            p.application_client_secret= app_secret or \
                integration.application_client_secret
            p.publisher_id = args.get('publisher_id', integration.publisher_id)
            playlist_feed_ids = args.get('playlist_feed_ids', None)
            if playlist_feed_ids:
                p.playlist_feed_ids = playlist_feed_ids.split(',')
            p.uses_batch_provisioning = Boolean()(
                args.get('uses_batch_provisioning',
                integration.uses_batch_provisioning))
            p.uses_bc_thumbnail_api = Boolean()(
                args.get('uses_bc_thumbnail_api',
                integration.uses_bc_thumbnail_api))
            p.uses_bc_videojs_player = Boolean()(
                args.get('uses_bc_videojs_player',
                integration.uses_bc_videojs_player))
            p.uses_bc_smart_player = Boolean()(
                args.get('uses_bc_smart_player',
                integration.uses_bc_smart_player))

        yield neondata.BrightcoveIntegration.modify(
            integration_id, _update_integration, async=True)

        integration = yield IntegrationHelper.get_integration(
            integration_id,
            neondata.IntegrationType.BRIGHTCOVE)

        statemon.state.increment('put_brightcove_oks')
        rv = yield self.db2api(integration)
        self.success(rv)

    @tornado.gen.coroutine
    def _get_last_processed_date(self, publisher_id, app_id, app_secret):
        """calls out to brightcove with the sent in app_id
             and app_secret to get the 4th most recent video
             so that we can set a reasonable last_process_date
             on this video

           raises on unknown exceptions
           returns none if a video search could not be completed
        """
        rv = None

        bc_cms_api = api.brightcove_api.CMSAPI(
            publisher_id,
            app_id,
            app_secret)
        try:
            # return the fourth oldest video
            videos = yield bc_cms_api.get_videos(
                limit=1,
                offset=3,
                sort='-updated_at')

            if videos and len(videos) is not 0:
                video = videos[0]
                rv = video['updated_at']
            else:
                rv = datetime.utcnow().strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
        except (api.brightcove_api.BrightcoveApiServerError,
                api.brightcove_api.BrightcoveApiClientError,
                api.brightcove_api.BrightcoveApiNotAuthorizedError,
                api.brightcove_api.BrightcoveApiError) as e:
            _log.error('Brightcove Error occurred trying to get \
                        last_processed_date : %s' % e)
            pass
        except Exception as e:
            _log.error('Unknown Error occurred trying to get \
                        last_processed_date: %s' % e)
            raise

        raise tornado.gen.Return(rv)

    @classmethod
    def _get_default_returned_fields(cls):
        return [ 'integration_id', 'account_id', 'read_token',
                 'write_token', 'last_process_date', 'application_client_id',
                 'application_client_secret', 'publisher_id', 'callback_url',
                 'enabled', 'playlist_feed_ids', 'uses_batch_provisioning',
                 'uses_bc_thumbnail_api', 'uses_bc_videojs_player',
                 'uses_bc_smart_player', 'uses_bc_gallery', 'id_field',
                 'created', 'updated' ]

    @classmethod
    def _get_passthrough_fields(cls):
        return [ 'integration_id', 'account_id', 'read_token',
                 'write_token', 'last_process_date', 'application_client_id',
                 'application_client_secret', 'publisher_id', 'callback_url',
                 'enabled', 'playlist_feed_ids', 'uses_batch_provisioning',
                 'uses_bc_thumbnail_api', 'uses_bc_videojs_player',
                 'uses_bc_smart_player', 'uses_bc_gallery', 'id_field',
                 'created', 'updated' ]

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 HTTPVerbs.POST: neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT: neondata.AccessLevels.UPDATE,
                 'account_required': [HTTPVerbs.GET,
                                        HTTPVerbs.PUT,
                                        HTTPVerbs.POST]
               }


'''*********************************************************************
TagHandler
*********************************************************************'''
class TagHandler(APIV2Handler):

    @tornado.gen.coroutine
    def get(self, account_id):
        Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('tag_id'): CustomVoluptuousTypes.CommaSeparatedList
        })(self.args)
        tag_ids = self.args['tag_id'].split(',')
        # Filter on account.
        account_id = self.args['account_id']
        tags = yield neondata.Tag.get_many(tag_ids, async=True)
        tags = [tag for tag in tags if tag and tag.account_id == account_id]
        thumbs = yield neondata.TagThumbnail.get_many(tag_id=tag_ids, async=True)
        result = yield {tag.get_id(): self.db2api(tag, thumbs[tag.get_id()])
                for tag in tags if tag}
        self.success(result)

    @tornado.gen.coroutine
    def post(self, account_id):
        Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('name'): All(Coerce(unicode), Length(min=1, max=256)),
            'thumbnail_ids': CustomVoluptuousTypes.CommaSeparatedList,
            'type': CustomVoluptuousTypes.TagType
        })(self.args)
        tag_type = self.args['type'] if self.args.get('type') \
            else neondata.TagType.GALLERY
        tag = neondata.Tag(
            None,
            account_id=self.args['account_id'],
            name=self.args['name'],
            tag_type=tag_type)
        yield tag.save(async=True)

        thumb_ids = yield self._set_thumb_ids(
            tag,
            self.args.get('thumbnail_ids', '').split(','))
        result = yield self.db2api(tag, thumb_ids)
        self.success(result)

    @tornado.gen.coroutine
    def put(self, account_id):
        Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('tag_id'): CustomVoluptuousTypes.CommaSeparatedList,
            'thumbnail_ids': CustomVoluptuousTypes.CommaSeparatedList,
            'name': All(Coerce(unicode), Length(min=1, max=256)),
            'type': CustomVoluptuousTypes.TagType
        })(self.args)

        # Update the tag itself.
        def _update(tag):
            if self.args['account_id'] != tag.account_id:
                raise NotAuthorizedError('Account does not own tag')
            if self.args.get('name'):
                tag.name = self.args['name']
            if self.args.get('type'):
                tag.type = self.args['type']
        tag = yield neondata.Tag.modify(
            self.args['tag_id'],
            _update,
            async=True)

        thumb_ids = yield self._set_thumb_ids(
            tag,
            self.args.get('thumbnail_ids', '').split(','))
        result = yield self.db2api(tag, thumb_ids)
        self.success(result)

    @tornado.gen.coroutine
    def delete(self, account_id):
        Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('tag_id'): CustomVoluptuousTypes.CommaSeparatedList,
        })(self.args)
        tag = yield neondata.Tag.get(self.args['tag_id'], async=True)
        if not tag:
            raise NotFoundError('That tag is not found')
        if not tag.account_id == self.args['account_id']:
            raise NotAuthorizedError('Tag not owned by this account')
        thumbnail_ids = yield neondata.TagThumbnail.get(
            tag_id=self.args['tag_id'],
            async=True)
        # Delete associations.
        if thumbnail_ids:
            yield neondata.TagThumbnail.delete_many(
                tag_id=self.args['tag_id'],
                thumbnail_id=thumbnail_ids,
                async=True)
        # Delete the tag.
        yield neondata.Tag.delete(tag.get_id(), async=True)
        self.success({'tag_id': tag.get_id()})

    @tornado.gen.coroutine
    def db2api(self, tag, thumb_ids):
        return {
            'tag_id': tag.get_id(),
            'account_id':  tag.account_id,
            'name': tag.name,
            'thumbnails': thumb_ids,
            'tag_type': tag.tag_type}

    @tornado.gen.coroutine
    def _set_thumb_ids(self, tag, thumb_ids):
        thumbs = yield neondata.ThumbnailMetadata.get_many(thumb_ids, async=True)
        if thumbs:
            valid_thumb_ids = [
                thumb.get_id() for thumb in thumbs
                if thumb and thumb.account_id == tag.account_id]
            if valid_thumb_ids:
                result = yield neondata.TagThumbnail.save_many(
                    tag_id=tag.get_id(),
                    thumbnail_id=valid_thumb_ids,
                    async=True)
        # Get the current list of thumbnails.
        thumb_ids = yield neondata.TagThumbnail.get(tag_id=tag.get_id(), async=True)
        raise tornado.gen.Return(thumb_ids)

    @classmethod
    def _get_default_returned_fields(cls):
        return {'tag_id', 'name', 'tag_type'}

    @classmethod
    def _get_passthrough_fields(cls):
        return cls._get_default_returned_fields()

    @classmethod
    def get_access_levels(self):
        return {
            HTTPVerbs.GET: neondata.AccessLevels.READ,
            HTTPVerbs.POST: neondata.AccessLevels.CREATE,
            HTTPVerbs.PUT: neondata.AccessLevels.UPDATE,
            HTTPVerbs.DELETE: neondata.AccessLevels.DELETE,
            'account_required': [HTTPVerbs.GET,
                                 HTTPVerbs.PUT,
                                 HTTPVerbs.POST]}

class ThumbnailResponse(object):
    @classmethod
    def _get_default_returned_fields(cls):
        return ['video_id', 'thumbnail_id', 'rank', 'frameno',
                'neon_score', 'enabled', 'url', 'height', 'width',
                'type', 'external_ref', 'created', 'updated', 'renditions',
                'tag_ids']
    @classmethod
    def _get_passthrough_fields(cls):
        return ['rank', 'frameno', 'enabled', 'type', 'width', 'height',
                'created', 'updated']

    @tornado.gen.coroutine
    def _items(self, tags, fields):
        '''Build a result from tag keys.

        Input- list tag objects
        Returns - list of tag with nested thumbnails:
            [tag dict{
                id: tag id,
                name: tag name,
                thumbnails: <list of thumbnail dicts>}]'''

        # Get all the Thumbnails mapped by a tag.
        tag_ids = [tag.get_id() for tag in tags]
        mapping = yield neondata.TagThumbnail.get_many(tag_id=tag_ids, async=True)
        thumb_ids = list({tid for tids in mapping.values() for tid in tids})
        thumbs = yield neondata.ThumbnailMetadata.get_many(thumb_ids, async=True)
        # Make a map from key to object.
        tag_map = {tag.get_id(): tag for tag in tags}
        thumb_map = yield {th.get_id(): self.db2api(th, fields) for th in thumbs}
        # Replace each tid in mapping with a ThumbnailMetadata.
        result = [{
            'id': tag_id,
            'name': tag_map[tag_id].name,
            'thumbnails': [thumb_map[tid] for tid in tids if thumb_map.get(tid)]}
                for tag_id, tids in mapping.items()]
        raise tornado.gen.Return(result)


    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'video_id':
            retval = neondata.InternalVideoID.to_external(
                neondata.InternalVideoID.from_thumbnail_id(obj.key))
        elif field == 'thumbnail_id':
            retval = obj.key
        elif field == 'tag_ids':
            tag_ids = yield neondata.TagThumbnail.get(thumbnail_id=obj.key, async=True)
            retval = list(tag_ids)
        elif field == 'neon_score':
            retval = obj.get_neon_score()
        elif field == 'url':
            retval = obj.urls[0] if obj.urls else []
        elif field == 'external_ref':
            retval = obj.external_id
        elif field == 'renditions':
            urls = yield neondata.ThumbnailServingURLs.get(obj.key, async=True)
            retval = ThumbnailHelper.renditions_of(urls)
        else:
            raise BadRequestError('invalid field %s' % field)

        raise tornado.gen.Return(retval)


'''*********************************************************************
TagSearchExternalHandler : class responsible for searching tags
                           from an external source
   HTTP Verbs     : get
*********************************************************************'''
class TagSearchExternalHandler(ThumbnailResponse, APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            'limit': All(Coerce(int), Range(min=1, max=100)),
            'name': str,
            'since': Coerce(float),
            'until': Coerce(float),
            'show_hidden': Coerce(bool),
            'fields': CustomVoluptuousTypes.CommaSeparatedList(),
            'tag_type': CustomVoluptuousTypes.TagType
        })(self.args)
        self.args['base_url'] = '/api/v2/%s/tags/search/' % self.account_id
        searcher = ContentSearcher(**self.args)
        tags, count, prev_page, next_page = yield searcher.get()

        _fields = self.args.get('fields')
        fields = _fields.split(',') if _fields else None

        items = yield self._items(tags, fields)

        self.success({
            'items': items,
            'count': len(items),
            'next_page': next_page,
            'prev_page': prev_page})

    @classmethod
    def get_access_levels(self):
        return {
            HTTPVerbs.GET: neondata.AccessLevels.READ,
            'account_required': [HTTPVerbs.GET]}

class ContentSearcher(object):
    '''A searcher to run search requests and make results.'''

    def __init__(self, account_id=None, since=None, until=None, query=None,
                 fields=None, limit=None, show_hidden=False, base_url=None,
                 tag_type=None):
        self.account_id = account_id
        self.since = since or 0.0
        self.until = until or 0.0
        self.query = query
        self.fields = fields
        self.limit = limit
        self.show_hidden = show_hidden
        self.base_url = base_url or '/api/v2/tags/search/'
        self.tag_type = tag_type

    @tornado.gen.coroutine
    def get(self):
        '''Gets a search result tuple.

        Returns tuple of
            list of content items,
            int count of items in this response,
            str prev page url,
            str next page url.'''
        args = {k:v for k,v in self.__dict__.items() if k not in ['base_url', 'fields']}
        args['async'] = True
        tags, min_time, max_time = yield neondata.Tag.objects_and_times(**args)
        raise tornado.gen.Return((
            tags,
            len(tags),
            self._prev_page_url(min_time),
            self._next_page_url(max_time)))

    def _prev_page_url(self, timestamp):
        '''Build the previous page url.'''
        return self._page_url('since', timestamp)

    def _next_page_url(self, timestamp):
        '''Build the previous page url.'''
        return self._page_url('until', timestamp)

    def _page_url(self, time_type, timestamp):
        return '{base}?{time_type}={ts}&limit={limit}{query}{fields}{acct}'.format(
            base=self.base_url,
            time_type=time_type,
            ts=timestamp,
            limit=self.limit,
            query='&query=%s' % self.query if self.query else '',
            fields='&fields=%s' % ','.join(self.fields) if self.fields else '',
            acct='&account_id=%s' % self.account_id if self.account_id else '')


'''*********************************************************************
TagSearchInternalHandler : class responsible for searching tags
                           from an internal source
   HTTP Verbs     : get
*********************************************************************'''
class TagSearchInternalHandler(ThumbnailResponse, APIV2Handler):
    @tornado.gen.coroutine
    def get(self):
        Schema({
            'account_id': All(Coerce(str), Length(min=1, max=256)),
            'limit': All(Coerce(int), Range(min=1, max=100)),
            'name': str,
            'since': All(Coerce(float)),
            'until': All(Coerce(float)),
            'show_hidden': Coerce(bool),
            'fields': CustomVoluptuousTypes.CommaSeparatedList(),
            'tag_type': CustomVoluptuousTypes.TagType
        })(self.args)

        self.args['base_url'] = '/api/v2/tags/search/'
        searcher = ContentSearcher(**self.args)
        tags, count, prev_page, next_page = yield searcher.get()

        _fields = self.args.get('fields')
        fields = _fields.split(',') if _fields else None

        items = yield self._items(tags, fields)

        self.success({
            'items': items,
            'count': len(items),
            'next_page': next_page,
            'prev_page': prev_page})

    @classmethod
    def get_access_levels(self):
        return {
            HTTPVerbs.GET: neondata.AccessLevels.READ,
            'internal_only': True,
            'account_required': []}


'''*********************************************************************
ThumbnailHandler
*********************************************************************'''
class ThumbnailHandler(ThumbnailResponse, APIV2Handler):

    def _initialize_predictor(self):
        '''Instantiate and connect an Aquila predictor.'''
        aquila_conn = utils.autoscale.MultipleAutoScaleGroups(
            options.model_autoscale_groups.split(','))
        self.predictor = model.predictor.DeepnetPredictor(
            port=options.model_server_port,
            concurrency=options.request_concurrency,
            aquila_connection=aquila_conn)
        self.predictor.connect()
        self.model_version = 'aqv1.1.250'

    @tornado.gen.coroutine
    def post(self, account_id):
        """Create a new thumbnail"""

        # The client can submit either a url argument or file in the body
        # with a Content-Type: multipart/form-data header.
        schema = Schema({
            Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
            # Video id associates this image as thumbnail of a video.
            'video_id' : All(Coerce(str), Length(min=1, max=256)),
            'url': Url(),
            # Tag id associates the image with a collection.
            'tag_id': All(Coerce(str), Length(min=1, max=256)),
            # This is a partner's id for the image.
            'thumbnail_ref' : All(Coerce(str), Length(min=1, max=1024))
        })
        self.args = self.parse_args()
        self.args['account_id'] = account_id
        schema(self.args)

        self.thumb = self.image = self.video = None

        # Switch on whether a video is tied to this submission.
        if self.args.get('video_id'):
            yield self._post_with_video()
            return
        yield self._post_without_video()

    @tornado.gen.coroutine
    def _post_with_video(self):
        """Set image and thumbnail data object with video association.

        Confirm video exists, then add the thumbnail to the video's
        list of thumbnails. Calculate the new thumbnail's rank from the old
        thumbnails."""
        _video_id = neondata.InternalVideoID.generate(
            self.account_id, self.args['video_id'])
        self.video = video = yield neondata.VideoMetadata.get(
            _video_id,
            async=True)
        if not video:
            raise NotFoundError('No video for {}'.format(_video_id))
        thumbs = yield neondata.ThumbnailMetadata.get_many(
            video.thumbnail_ids,
            async=True)

        # Calculate new thumbnail's rank: one less than everything else
        # or default value 1 if no other thumbnail.
        rank = min([t.rank for t in thumbs
            if t.type == neondata.ThumbnailType.CUSTOMUPLOAD]) - 1 if thumbs else 1

        # Save the image file and thumbnail data object.
        yield self._set_thumb(rank)

        # Update video's thumbnail list.
        video = yield neondata.VideoMetadata.modify(
            _video_id,
            lambda x: x.thumbnail_ids.append(self.thumb.key),
            async=True)
        if not video:
            raise SaveError("Can't save thumbnail to video {}".format(_video_id))

        statemon.state.increment('post_thumbnail_oks')
        yield self._respond_with_thumb()

    @tornado.gen.coroutine
    def _post_without_video(self):
        """Set the image to CDN. Set the thumb data to database.

        Returns- the new thumbnail."""
        yield self._set_thumb()
        statemon.state.increment('post_thumbnail_oks')
        yield self._respond_with_thumb()

    @tornado.gen.coroutine
    def _set_thumb(self, rank=None):
        """Set self.thumb to a new thumbnail from submitted image."""

        # Instantiate the thumbnail data object.
        if self.video:
            video_id = self.video.get_id()
            integration_id = self.video.integration_id
        else:
            video_id = neondata.InternalVideoID.generate(self.account_id)
            integration_id = None
        self.thumb = neondata.ThumbnailMetadata(
            None,
            internal_vid=video_id,
            external_id=self.args.get('thumbnail_ref'),
            ttype=neondata.ThumbnailType.CUSTOMUPLOAD,
            rank=rank)

        # Set the image from url or body form data.
        yield self._set_image()

        # Get CDN store.
        cdn = yield neondata.CDNHostingMetadataList.get(
            neondata.CDNHostingMetadataList.create_key(
                self.account_id,
                integration_id),
            async=True)

        # If the thumbnail is tied to a video, set that association.
        if self.video:
            self.thumb = yield self.video.download_and_add_thumbnail(
                self.thumb,
                image=self.image,
                image_url=self.args.get('url'),
                cdn_metadata=cdn,
                async=True)
        else:
            yield self.thumb.add_image_data(self.image, cdn_metadata=cdn, async=True)
            # Score non-video image here.
            yield self._score_image()

        yield self.thumb.save(async=True)

        # Set tags if requested.
        if self.args.get('tag_id'):
            request_tag_ids = self.args.get('tag_id').split(',')
            tags = yield neondata.Tag.get_many(request_tag_ids, async=True)
            valid_tag_ids = [t.get_id() for t in tags
                if t and t.account_id == self.account_id]
            yield neondata.TagThumbnail.save_many(
                tag_id=valid_tag_ids,
                thumbnail_id=self.thumb.get_id(),
                async=True)

    @tornado.gen.coroutine
    def _set_image(self):
        """Set self.image to a PIL image or raise HTTP_BAD_REQUEST."""

        # Get from url.
        url = self.args.get('url')
        if url:
            self.image = yield neondata.ThumbnailMetadata.download_image_from_url(url, async=True)
            if self.image:
                return

        # Get image from body.
        try:
            self.image = ThumbnailHandler._get_image_from_httpfile(
                self.request.files['upload'][0])
            if self.image:
                return
        except IOError as e:
            # If an Image() can't be made, the client sent the wrong thing.
            e.errno = 400
            raise e
        except KeyError:
            pass

        if not self.image:
            raise BadRequestError('Image not available', ResponseCode.HTTP_BAD_REQUEST)

    @staticmethod
    def _get_image_from_httpfile(httpfile):
        """Get the image from the http post request.
           Inputs- a HTTPFile, or any dict with body string
           Returns- instance of PIL.Image
        """
        return PIL.Image.open(io.BytesIO(httpfile.body))

    @tornado.gen.coroutine
    def _score_image(self):
        if not hasattr(self, 'predictor'):
            self._initialize_predictor()
        yield self.thumb.score_image(
            self.predictor,
            self.model_version,
            self.image,
            True)

    @tornado.gen.coroutine
    def _respond_with_thumb(self):
        """Success. Reload the thumbnail and return it."""
        thumb = yield neondata.ThumbnailMetadata.get(
            self.thumb.key,
            async=True)
        rv = yield self.db2api(thumb)
        self.success(rv, code=ResponseCode.HTTP_ACCEPTED)

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a thumbnail endpoint put request"""

        schema = Schema({
          Required('account_id'): Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_id'): Any(str, unicode, Length(min=1, max=512)),
          'enabled': Boolean()
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        thumbnail_id = args['thumbnail_id']

        def _update_thumbnail(t):
            t.enabled = Boolean()(args.get('enabled', t.enabled))

        thumbnail = yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumbnail_id,
                                           _update_thumbnail)

        statemon.state.increment('put_thumbnail_oks')
        thumbnail = yield self.db2api(thumbnail)
        self.success(thumbnail)

    @tornado.gen.coroutine
    def get(self, account_id):
        """handles a thumbnail endpoint get request"""

        schema = Schema({
          Required('account_id'): Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_id'): Any(str, unicode, Length(min=1, max=512)),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        thumbnail_id = args['thumbnail_id']
        thumbnail = yield neondata.ThumbnailMetadata.get(
            thumbnail_id,
            async=True)
        if not thumbnail:
            raise NotFoundError('thumbnail does not exist with id = %s' %
                                (thumbnail_id))
        statemon.state.increment('get_thumbnail_oks')
        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))
        thumbnail = yield self.db2api(thumbnail, fields)
        self.success(thumbnail)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 HTTPVerbs.POST: neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT: neondata.AccessLevels.UPDATE,
                 'account_required': [HTTPVerbs.GET,
                                        HTTPVerbs.PUT,
                                        HTTPVerbs.POST]
               }


'''*********************************************************************
ThumbnailHelper
*********************************************************************'''
class ThumbnailHelper(object):
    """A collection of stateless functions for working on Thumbnails"""

    @staticmethod
    @tornado.gen.coroutine
    def get_renditions_from_tids(tids):
        """Given list of thumbnails ids, get all renditions as map of tid.

        Input- list of thumbnail ids
        Yields- [ tid0: [rendition1, .. renditionN], tid1: [...], ...}
            where rendition has format {
                'url': string
                'width': int,
                'height': int,
                'aspect_ratio': string in format "WxH"
        """
        urls = yield neondata.ThumbnailServingURLs.get_many(tids, async=True)
        # Build a map of {tid: [renditions]}.
        rv = {}
        for chunk in urls:
            if chunk:
                renditions = [ThumbnailHelper._to_dict(pair) for pair in chunk]
                try:
                    rv[chunk.get_id()].extend(renditions)
                except KeyError:
                    rv[chunk.get_id()] = renditions
        # Ensure that every tid in request has a list mapped.
        for tid in tids:
            if not rv.get(tid):
                rv[tid] = []
        raise tornado.gen.Return(rv)

    @staticmethod
    def renditions_of(urls_obj):
        """Given a ThumbnailServingURLs, get a list of rendition dicts.

        Input- urls_obj a ThumbnailServingURLs
        Returns- list of rendition dictionaries
            i.e., [rendition1, rendition2, ... , renditionN]
            where rendition has format {
                'url': string
                'width': int,
                'height': int,
                'aspect_ratio': string in format "WxH"
        """
        return [ThumbnailHelper._to_dict(item) for item
                in urls_obj.size_map.items()]

    @staticmethod
    def _to_dict(pair):
        """Given a size map (sizes, url) tuple return a rendition dictionary."""
        dimensions, url = pair 

        return {
            'url': url,
            'width': dimensions[0],
            'height': dimensions[1],
            'aspect_ratio': '%sx%s' % ThumbnailHelper._get_ar(*dimensions)}

    @staticmethod
    def _get_ar(width, height):
        """Calculate aspect ratio from width, height."""
        f = fractions.Fraction(width, height)
        if f.numerator == 120 and f.denominator == 67:
            return 16, 9
        return f.numerator, f.denominator


'''*********************************************************************
VideoHelper
*********************************************************************'''
class VideoHelper(object):
    """helper class designed to help the video endpoint handle requests"""
    @staticmethod
    @tornado.gen.coroutine
    def create_api_request(args, account_id_api_key):
        """creates an API Request object

        Keyword arguments:
        args -- the args sent to the api endpoint
        account_id_api_key -- the account_id/api_key
        """
        job_id = uuid.uuid1().hex
        integration_id = args.get('integration_id', None)

        request = neondata.NeonApiRequest(job_id, api_key=account_id_api_key)
        request.video_id = args['external_video_ref']
        if integration_id:
            request.integration_id = integration_id
        request.video_url = args.get('url', None)
        request.callback_url = args.get('callback_url', None)
        request.video_title = args.get('title', None)
        request.default_thumbnail = args.get('default_thumbnail_url', None)
        request.external_thumbnail_ref = args.get('thumbnail_ref', None)
        request.publish_date = args.get('publish_date', None)
        request.api_param = int(args.get('n_thumbs', 5))
        request.callback_email = args.get('callback_email', None) 
        yield request.save(async=True)

        if request:
            raise tornado.gen.Return(request)

    @staticmethod
    @tornado.gen.coroutine
    def create_video_and_request(args, account_id_api_key):
        """creates Video object and ApiRequest object and
           sends them back to the caller as a tuple

        Keyword arguments:
        args -- the args sent to the api endpoint
        account_id_api_key -- the account_id/api_key
        """

        video_id = args['external_video_ref']
        internal_video_id = neondata.InternalVideoID.generate(
            account_id_api_key,
            video_id)
        video = yield neondata.VideoMetadata.get(
            internal_video_id,
            async=True)
        if video is None:
            # Generate share token.
            share_payload = {
                'content_type': 'VideoMetadata',
                'content_id': internal_video_id
            }
            share_token = ShareJWTHelper.encode(share_payload)

            duration = args.get('duration', None)
            if duration:
                duration=float(duration)

            video = neondata.VideoMetadata(
                internal_video_id,
                video_url=args.get('url', None),
                publish_date=args.get('publish_date', None),
                duration=duration,
                custom_data=args.get('custom_data', None),
                i_id=args.get('integration_id', '0'),
                serving_enabled=False,
                share_token=share_token)

            default_thumbnail_url = args.get('default_thumbnail_url', None)
            if default_thumbnail_url:
                # save the default thumbnail
                image = yield neondata.ThumbnailMetadata.download_image_from_url(
                    default_thumbnail_url, async=True)
                thumb = yield video.download_and_add_thumbnail(
                    image=image,
                    image_url=default_thumbnail_url,
                    external_thumbnail_id=args.get('thumbnail_ref', None),
                    async=True)
                # bypassing save_objects to avoid the extra video save
                # that comes later
                yield tornado.gen.Task(thumb.save)

            # create the api_request
            api_request = yield VideoHelper.create_api_request(
                args,
                account_id_api_key)

            # Create a Tag for this video.
            tag = neondata.Tag(
                account_id=account_id_api_key,
                tag_type='video',
                name=api_request.video_title)
            tag.save()
            video.tag_id = tag.get_id()

            # add the job id save the video
            video.job_id = api_request.job_id
            yield video.save(async=True)
            raise tornado.gen.Return((video, api_request))
        else:
            reprocess = Boolean()(args.get('reprocess', False))
            if reprocess:
                # Flag the request to be reprocessed
                def _flag_reprocess(x):
                    x.state = neondata.RequestState.REPROCESS
                    x.fail_count = 0
                    x.try_count = 0
                    x.response = {}
                api_request = yield neondata.NeonApiRequest.modify(
                    video.job_id,
                    account_id_api_key,
                    _flag_reprocess,
                    async=True)

                raise tornado.gen.Return((video, api_request))
            else:
                raise AlreadyExists('job_id=%s' % (video.job_id))

    @staticmethod
    @tornado.gen.coroutine
    def get_thumbnails_from_ids(tids):
        """gets thumbnailmetadata objects

        Keyword arguments:
        tids -- a list of tids that needs to be retrieved
        """
        thumbnails = []
        if tids:
            thumbnails = yield tornado.gen.Task(
                neondata.ThumbnailMetadata.get_many,
                tids)
            thumbnails = yield [ThumbnailHandler.db2api(x) for
                                x in thumbnails]
            renditions = yield ThumbnailHelper.get_renditions_from_tids(tids)
            for thumbnail in thumbnails:
                thumbnail['renditions'] = renditions[thumbnail['thumbnail_id']]

        raise tornado.gen.Return(thumbnails)

    @staticmethod
    @tornado.gen.coroutine
    def get_search_results(account_id=None, since=None, until=None, query=None,
                           limit=None, fields=None,
                           base_url='/api/v2/videos/search',
                           show_hidden=False):

        videos, until_time, since_time = yield neondata.VideoMetadata.objects_and_times(
            account_id=account_id,
            since=since,
            until=until,
            limit=limit,
            query=query,
            show_hidden=show_hidden,
            async=True)

        vid_dict = yield VideoHelper.build_video_dict(videos, fields)

        vid_dict['next_page'] = VideoHelper.build_page_url(
            base_url,
            until_time if until_time else 0.0,
            limit=limit,
            page_type='until',
            query=query,
            fields=fields,
            account_id=account_id)
        vid_dict['prev_page'] = VideoHelper.build_page_url(
            base_url,
            since_time if since_time else 0.0,
            limit=limit,
            page_type='since',
            query=query,
            fields=fields,
            account_id=account_id)
        raise tornado.gen.Return(vid_dict)

    @staticmethod
    @tornado.gen.coroutine
    def build_video_dict(videos,
                         fields,
                         video_ids=None):
        vid_dict = {}
        vid_dict['videos'] = None
        vid_dict['video_count'] = 0
        new_videos = []
        vid_counter = 0
        index = 0
        videos = [x for x in videos if x and x.job_id]
        job_ids = [(v.job_id, v.get_account_id())
                      for v in videos]

        requests = yield neondata.NeonApiRequest.get_many(
                       job_ids,
                       async=True)
        for video, request in zip(videos, requests):
            if video is None or request is None and video_ids:
                new_videos.append({'error': 'video does not exist',
                                   'video_id': video_ids[index] })
                index += 1
                continue

            new_video = yield VideoHelper.db2api(video,
                                                 request,
                                                 fields)
            new_videos.append(new_video)
            vid_counter += 1

        vid_dict['videos'] = new_videos
        vid_dict['video_count'] = vid_counter

        raise tornado.gen.Return(vid_dict)


    @staticmethod
    def build_page_url(base_url,
                       time_stamp,
                       limit,
                       page_type=None,
                       query=None,
                       fields=None,
                       account_id=None):
        next_page_url = '%s?%s=%f&limit=%d' % (
            base_url,
            page_type,
            time_stamp,
            limit)
        if query:
            next_page_url += '&query=%s' % query
        if fields:
            next_page_url += '&fields=%s' % \
                ",".join("{0}".format(f) for f in fields)
        if account_id:
            next_page_url += '&account_id=%s' % account_id

        return next_page_url

    @staticmethod
    @tornado.gen.coroutine
    def db2api(video, request, fields=None, tag=None):
        """Converts a database video metadata object to a video
        response dictionary

        Overwrite the base function because we have to do a join on the request

        Keyword arguments:
        video - The VideoMetadata object
        request - The NeonApiRequest object
        fields - List of fields to return
        """
        if fields is None:
            fields = ['state', 'video_id', 'publish_date', 'title', 'url',
                      'testing_enabled', 'job_id', 'tag_id']

        new_video = {}
        for field in fields:
            if field == 'thumbnails':
                new_video['thumbnails'] = yield \
                  VideoHelper.get_thumbnails_from_ids(video.thumbnail_ids)
            elif field == 'state':
                new_video[field] = neondata.ExternalRequestState.from_internal_state(request.state)
            elif field == 'integration_id':
                new_video[field] = video.integration_id
            elif field == 'testing_enabled':
                # TODO: maybe look at the account level abtest?
                new_video[field] = video.testing_enabled
            elif field == 'job_id':
                new_video[field] = video.job_id
            elif field == 'title':
                if request:
                    new_video[field] = request.video_title
                elif tag:
                    new_video[field] = tag.video_title
            elif field == 'video_id':
                new_video[field] = \
                  neondata.InternalVideoID.to_external(video.key)
            elif field == 'serving_url':
                new_video[field] = video.serving_url
            elif field == 'publish_date':
                new_video[field] = request.publish_date
            elif field == 'duration':
                new_video[field] = video.duration
            elif field == 'custom_data':
                new_video[field] = video.custom_data
            elif field == 'created':
                new_video[field] = video.created
            elif field == 'updated':
                new_video[field] = video.updated
            elif field == 'url':
                new_video[field] = video.url
            elif field == 'tag_id':
                new_video[field] = video.tag_id
            else:
                raise BadRequestError('invalid field %s' % field)

            if request:
                err = request.response.get('error', None)
                if err:
                    new_video['error'] = err

        raise tornado.gen.Return(new_video)


'''*********************************************************************
VideoHandler
*********************************************************************'''
class VideoHandler(ShareableContentHandler):
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a Video endpoint post request"""
        schema = Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('external_video_ref'): All(Any(Coerce(str), unicode),
                Length(min=1, max=512)),
            'url': All(Any(Coerce(str), unicode), Length(min=1, max=2048)),
            'reprocess': Boolean(),
            'integration_id': All(Coerce(str), Length(min=1, max=256)),
            'callback_url': All(Any(Coerce(str), unicode),
                Length(min=1, max=2048)),
            'title': All(Any(Coerce(str), unicode),
                Length(min=1, max=2048)),
            'duration': Any(All(Coerce(float), Range(min=0.0, max=86400.0)),
                None),
            'publish_date': All(CustomVoluptuousTypes.Date()),
            'custom_data': All(CustomVoluptuousTypes.Dictionary()),
            'default_thumbnail_url': All(Any(Coerce(str), unicode),
                Length(min=1, max=2048)),
            'thumbnail_ref': All(Coerce(str), Length(min=1, max=512)),
            'callback_email': All(Coerce(str), Length(min=1, max=2048)),
            'n_thumbs': All(Coerce(int), Range(min=1, max=32))
        })

        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        reprocess = args.get('reprocess', None)
        url = args.get('url', None)
        if (reprocess is None) == (url is None):
            raise Invalid('Exactly one of reprocess or url is required')

        # add the video / request
        video_and_request = yield tornado.gen.Task(
            VideoHelper.create_video_and_request,
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
        sqs_queue = video_processor.video_processing_queue.VideoProcessingQueue()

        account = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                                         account_id)
        duration = new_video.duration

        message = yield sqs_queue.write_message(
                    account.get_processing_priority(),
                    json.dumps(api_request.__dict__),
                    duration)

        if message:
            job_info = {}
            job_info['job_id'] = api_request.job_id
            job_info['video'] = yield self.db2api(new_video,
                                                  api_request)
            statemon.state.increment('post_video_oks')
            self.success(job_info,
                         code=ResponseCode.HTTP_ACCEPTED)
        else:
            raise SubmissionError('Unable to submit job to queue')

    @tornado.gen.coroutine
    def get(self, account_id):
        """handles a Video endpoint get request"""

        schema = Schema({
            Required('account_id'): Any(str, unicode, Length(min=1, max=256)),
            Required('video_id'): Any(
                CustomVoluptuousTypes.CommaSeparatedList()),
            'fields': Any(CustomVoluptuousTypes.CommaSeparatedList()),
            'share_token': Any(str)
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        vid_dict = {}
        internal_video_ids = []
        video_ids = args['video_id'].split(',')
        for v_id in video_ids:
            internal_video_id = neondata.InternalVideoID.generate(
                account_id_api_key,v_id)
            internal_video_ids.append(internal_video_id)

        videos = yield tornado.gen.Task(neondata.VideoMetadata.get_many,
                                        internal_video_ids)

        vid_dict = yield VideoHelper.build_video_dict(
                       videos,
                       fields,
                       video_ids)

        if vid_dict['video_count'] is 0:
            raise NotFoundError('video(s) do not exist with id(s): %s' %
                                (args['video_id']))

        statemon.state.increment('get_video_oks')
        self.success(vid_dict)

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a Video endpoint put request"""

        schema = Schema({
            Required('account_id'): Any(str, unicode, Length(min=1, max=256)),
            Required('video_id'): Any(str, unicode, Length(min=1, max=256)),
            'testing_enabled': Coerce(Boolean()),
            'title': Any(str, unicode, Length(min=1, max=1024)),
            'hidden': Boolean(),
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        title = args.get('title', None)

        internal_video_id = neondata.InternalVideoID.generate(
            account_id_api_key,
            args['video_id'])

        if 'testing_enabled' in args or 'hidden' in args:
            def _update_video(v):
                v.testing_enabled =  Boolean()(args.get('testing_enabled', v.testing_enabled))
                v.hidden =  Boolean()(args.get('hidden', v.hidden))

            video = yield neondata.VideoMetadata.modify(
                internal_video_id,
                _update_video,
                async=True)
        else:
            video = yield neondata.VideoMetadata.get(internal_video_id, async=True)

        if not video:
            raise NotFoundError('video does not exist with id: %s' %
                (args['video_id']))

        # we may need to update the request and/or tag object as well
        db2api_fields = {'testing_enabled', 'video_id', 'tag_id'}
        api_request, tag = None, None
        if title is not None:
            if video.job_id is not None:
                def _update_request(r):
                    r.video_title = title

                api_request = yield neondata.NeonApiRequest.modify(
                    video.job_id,
                    account_id,
                    _update_request,
                    async=True)
                db2api_fields.add('title')
            if video.tag_id is not None:
                def _update_tag(t):
                    t.name = title
                tag = yield neondata.Tag.modify(
                    video.tag_id,
                    _update_tag,
                    async=True)
                db2api_fields.add('title')

        statemon.state.increment('put_video_oks')
        output = yield self.db2api(video, api_request,
                                   fields=list(db2api_fields), tag=tag)
        self.success(output)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET,
                                        HTTPVerbs.PUT,
                                        HTTPVerbs.POST],
                 'subscription_required' : [HTTPVerbs.POST]
               }

    @classmethod
    def get_limits(self):
        post_list = [{ 'left_arg': 'video_posts',
                       'right_arg': 'max_video_posts',
                       'operator': '<',
                       'timer_info': {
                           'refresh_time': 'refresh_time_video_posts',
                           'add_to_refresh_time': 'seconds_to_refresh_video_posts',
                           'timer_resets': [ ('video_posts', 0) ]
                       },
                       'values_to_increase': [ ('video_posts', 1) ],
                       'values_to_decrease': []
        }]
        return {
                   HTTPVerbs.POST: post_list
               }

    @staticmethod
    @tornado.gen.coroutine
    def db2api(video, request, fields=None, tag=None):
        video_obj = yield VideoHelper.db2api(video, request, fields)
        raise tornado.gen.Return(video_obj)

'''*********************************************************************
VideoStatsHandler
*********************************************************************'''
class VideoStatsHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        """gets the video statuses of 1 -> n videos"""

        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
          Required('video_id'): Any(CustomVoluptuousTypes.CommaSeparatedList()),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        internal_video_ids = []
        stats_dict = {}
        video_ids = args['video_id'].split(',')

        for v_id in video_ids:
            internal_video_id = neondata.InternalVideoID.generate(account_id_api_key,v_id)
            internal_video_ids.append(internal_video_id)

        # even if the video_id does not exist an object is returned
        video_statuses = yield tornado.gen.Task(neondata.VideoStatus.get_many,
                                                internal_video_ids)
        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))
        video_statuses = yield [self.db2api(x, fields) for x in video_statuses]
        stats_dict['statistics'] = video_statuses
        stats_dict['count'] = len(video_statuses)

        self.success(stats_dict)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 'account_required': [HTTPVerbs.GET]
               }

    @classmethod
    def _get_default_returned_fields(cls):
        return ['video_id', 'experiment_state', 'winner_thumbnail']

    @classmethod
    def _get_passthrough_fields(cls):
        return ['experiment_state', 'created', 'updated']

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'video_id':
            retval = neondata.InternalVideoID.to_external(
                obj.get_id())
        elif field == 'winner_thumbnail':
            retval = obj.winner_tid
        else:
            raise BadRequestError('invalid field %s' % field)

        raise tornado.gen.Return(retval)

'''*********************************************************************
ThumbnailStatsHandler
*********************************************************************'''
class ThumbnailStatsHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        """handles a thumbnail stats request
           account_id/thumbnail_ids - returns stats information about thumbnails
           account_id/video_id - returns stats information about all thumbnails
                                 for that video
        """

        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
          Optional('thumbnail_id'): Any(CustomVoluptuousTypes.CommaSeparatedList()),
          Optional('video_id'): Any(CustomVoluptuousTypes.CommaSeparatedList(20)),
          Optional('fields'): Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        thumbnail_ids = args.get('thumbnail_id', None)
        video_ids = args.get('video_id', None)
        if not video_ids and not thumbnail_ids:
            raise Invalid('thumbnail_id or video_id is required')
        if video_ids and thumbnail_ids:
            raise Invalid('you can only have one of thumbnail_id or video_id')

        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        if thumbnail_ids:
            thumbnail_ids = thumbnail_ids.split(',')
            objects = yield tornado.gen.Task(neondata.ThumbnailStatus.get_many,
                                             thumbnail_ids)
        elif video_ids:
            video_ids = video_ids.split(',')
            internal_video_ids = []
            # first get all the internal_video_ids
            internal_video_ids = [neondata.InternalVideoID.generate(
                account_id_api_key, x) for x in video_ids]

            # now get all the videos
            videos = yield tornado.gen.Task(neondata.VideoMetadata.get_many,
                                            internal_video_ids)
            # get the list of thumbnail_ids
            thumbnail_ids = []
            for video in videos:
                if video:
                    thumbnail_ids = thumbnail_ids + video.thumbnail_ids
            # finally get the thumbnail_statuses for these things
            objects = yield tornado.gen.Task(neondata.ThumbnailStatus.get_many,
                                             thumbnail_ids)

        # build up the stats_dict and send it back
        stats_dict = {}
        objects = yield [self.db2api(obj, fields)
                         for obj in objects]
        stats_dict['statistics'] = objects
        stats_dict['count'] = len(objects)

        self.success(stats_dict)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 'account_required': [HTTPVerbs.GET]
               }

    @classmethod
    def _get_default_returned_fields(cls):
        return ['thumbnail_id', 'video_id', 'ctr']

    @classmethod
    def _get_passthrough_fields(cls):
        return ['serving_frac', 'ctr',
                'created', 'updated']

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'video_id':
            retval = neondata.InternalVideoID.from_thumbnail_id(
                obj.get_id())
        elif field == 'thumbnail_id':
            retval = obj.get_id()
        elif field == 'serving_frac':
            retval = obj.serving_frac
        elif field == 'ctr':
            retval = obj.ctr
        elif field == 'impressions':
            retval = obj.imp
        elif field == 'conversions':
            retval = obj.conv
        else:
            raise BadRequestError('invalid field %s' % field)

        raise tornado.gen.Return(retval)


'''*********************************************************************
LiftStatsHandler
*********************************************************************'''
class LiftStatsHandler(APIV2Handler):

    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('base_id'): All(Coerce(str), Length(min=1, max=2048)),
            Required('thumbnail_ids'): Any(CustomVoluptuousTypes.CommaSeparatedList())})
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        base_thumb = yield neondata.ThumbnailMetadata.get(
            args['base_id'],
            async=True)
        if not base_thumb:
            raise NotFoundError('Base thumbnail does not exist')

        query_tids = args['thumbnail_ids'].split(',')
        thumbs = yield neondata.ThumbnailMetadata.get_many(
            query_tids,
            async=True,
            as_dict=True)

        default_neon_score = base_thumb.get_neon_score()
        def _get_estimated_lift(thumb):
            # The ratio of a thumbnail's Neon score to the default's.
            score = thumb.get_neon_score()
            if default_neon_score is None or score is None:
                return None
            return round(score / float(default_neon_score) - 1, 3)

        lift = [{'thumbnail_id': k, 'lift': _get_estimated_lift(t) if t else None}
                for k, t in thumbs.items()]

        # Check thumbnail exists.
        rv = {
            'baseline_thumbnail_id': args['base_id'],
            'lift': lift}
        self.success(rv)

    def get_access_levels(self):
        return {HTTPVerbs.GET: neondata.AccessLevels.READ}


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
        response = yield tornado.gen.Task(utils.http.send_request, request)
        if response.code is 200:
            self.success('<html>Server OK</html>')
        else:
            raise Exception('unable to get to the v1 api',
                            ResponseCode.HTTP_INTERNAL_SERVER_ERROR)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.NONE
               }

'''*********************************************************************
AccountLimitsHandler : class responsible for returning limit information
                          about an account
   HTTP Verbs     : get
*********************************************************************'''
class AccountLimitsHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        acct_limits = yield neondata.AccountLimits.get(
                          account_id_api_key,
                          async=True)

        if not acct_limits:
            raise NotFoundError()

        result = yield self.db2api(acct_limits)

        self.success(result)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 'account_required': [HTTPVerbs.GET]
               }

    @classmethod
    def _get_default_returned_fields(cls):
        return ['video_posts', 'max_video_posts', 'refresh_time_video_posts',
                'max_video_size' ]

    @classmethod
    def _get_passthrough_fields(cls):
        return ['video_posts', 'max_video_posts', 'refresh_time_video_posts',
                'max_video_size' ]

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
VideoSearchInternalHandler : class responsible for searching videos
                             from an internal source
   HTTP Verbs     : get
*********************************************************************'''
class VideoSearchInternalHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self):
        schema = Schema({
            'limit': All(Coerce(int), Range(min=1, max=100)),
            'account_id': All(Coerce(str), Length(min=1, max=256)),
            Optional('query'): str,
            'fields': Any(CustomVoluptuousTypes.CommaSeparatedList()),
            'since': All(Coerce(float)),
            'until': All(Coerce(float))
        })
        args = self.parse_args()
        schema(args)
        since = args.get('since', None)
        until = args.get('until', None)
        query = args.get('query', None)

        account_id = args.get('account_id', None)
        limit = int(args.get('limit', 25))
        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        vid_dict = yield VideoHelper.get_search_results(
                       account_id,
                       since,
                       until,
                       query,
                       limit,
                       fields)

        self.success(vid_dict)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 'internal_only': True,
                 'account_required': []
               }

'''*********************************************************************
VideoShareHandler : class responsible for generating video share tokens
   HTTP Verbs     : get
*********************************************************************'''
class VideoShareHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            Required('video_id'): All(Coerce(str), Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        # Validate video exists.
        internal_video_id = neondata.InternalVideoID.generate(
            account_id_api_key,
            args['video_id'])
        video = yield neondata.VideoMetadata.get(internal_video_id, async=True)
        if not video:
            raise NotFoundError('video does not exist with id: %s' %
                (args['video_id']))
        if not video.share_token:
            payload = {
                'content_type': 'VideoMetadata',
                'content_id': video.get_id()}
            video.share_token = ShareJWTHelper.encode(payload)
            yield video.save(async=True)
        self.success({'share_token':video.share_token})

    @classmethod
    def get_access_levels(self):
        return {
            HTTPVerbs.GET: neondata.AccessLevels.READ,
            'account_required': [HTTPVerbs.GET]}


'''*********************************************************************
VideoSearchExternalHandler : class responsible for searching videos from
                             an external source
   HTTP Verbs     : get
*********************************************************************'''
class VideoSearchExternalHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
            Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
            'limit': All(Coerce(int), Range(min=1, max=100)),
            'query': Any(CustomVoluptuousTypes.Regex(), str),
            'fields': Any(CustomVoluptuousTypes.CommaSeparatedList()),
            'since': All(Coerce(float)),
            'until': All(Coerce(float)),
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        since = args.get('since', None)
        until = args.get('until', None)
        query = args.get('query', None)

        limit = int(args.get('limit', 25))
        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        base_url = '/api/v2/%s/videos/search' % account_id
        vid_dict = yield VideoHelper.get_search_results(
            account_id,
            since,
            until,
            query,
            limit,
            fields,
            base_url=base_url,
            show_hidden=False)

        self.success(vid_dict)

    @classmethod
    def get_access_levels(self):
        return {
            HTTPVerbs.GET: neondata.AccessLevels.READ,
            'account_required': [HTTPVerbs.GET]}


'''*****************************************************************
AccountIntegrationHandler : class responsible for getting all
                            integrations, on a specific account
  HTTP Verbs      : get
*****************************************************************'''
class AccountIntegrationHandler(APIV2Handler):
    """This is a bit of a one-off API, it will return
          all integrations (regardless of type) for an
          individual account.
    """
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)

        user_account = yield neondata.NeonUserAccount.get(
            account_id,
            async=True)

        if not user_account:
            raise NotFoundError()

        rv = yield IntegrationHelper.get_integrations(account_id)
        rv['integration_count'] = len(rv['integrations'])
        self.success(rv)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 'account_required': [HTTPVerbs.GET]
               }


'''*****************************************************************
UserHandler
*****************************************************************'''
class UserHandler(APIV2Handler):
    """Handles get,put requests to the user endpoint.
       Gets and updates existing users
    """
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
          Required('account_id'): All(Coerce(str), Length(min=1, max=256)),
          Required('username'): All(Coerce(str), Length(min=8, max=64)),
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)

        username = args.get('username')

        user = yield neondata.User.get(
                   username,
                   async=True)

        if not user:
            raise NotFoundError()

        if self.user.username != username:
            raise NotAuthorizedError('Cannot view another users account')

        result = yield self.db2api(user)

        self.success(result)

    @tornado.gen.coroutine
    def put(self, account_id):
        # TODO give ability to modify access_level
        schema = Schema({
          Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
          Required('username') : All(Coerce(str), Length(min=8, max=64)),
          'first_name': All(Coerce(str), Length(min=1, max=256)),
          'last_name': All(Coerce(str), Length(min=1, max=256)),
          'secondary_email': All(Coerce(str), Length(min=1, max=256)),
          'cell_phone_number': All(Coerce(str), Length(min=1, max=32)),
          'title': All(Coerce(str), Length(min=1, max=32)),
          'send_emails': Boolean()
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        username = args.get('username')

        if self.user.access_level is not neondata.AccessLevels.GLOBAL_ADMIN:
            if self.user.username != username:
                raise NotAuthorizedError('Cannot update another\
                               users account')

        def _update_user(u):
            u.first_name = args.get('first_name', u.first_name)
            u.last_name = args.get('last_name', u.last_name)
            u.title = args.get('title', u.title)
            u.cell_phone_number = args.get(
                'cell_phone_number',
                u.cell_phone_number)
            u.secondary_email = args.get(
                'secondary_email',
                u.secondary_email)
            u.send_emails = Boolean()(args.get(
                'send_emails', 
                u.send_emails))

        user_internal = yield neondata.User.modify(
            username,
            _update_user,
            async=True)

        if not user_internal:
            raise NotFoundError()

        result = yield self.db2api(user_internal)

        self.success(result)

    @classmethod
    def get_access_levels(cls):
        return {
                 HTTPVerbs.GET: neondata.AccessLevels.READ,
                 HTTPVerbs.PUT: neondata.AccessLevels.UPDATE,
                 'account_required' : [HTTPVerbs.GET, HTTPVerbs.PUT]
               }

    @classmethod
    def _get_default_returned_fields(cls):
        return ['username', 'created', 'updated',
                'first_name', 'last_name', 'title',
                'secondary_email', 'cell_phone_number',
                'access_level' ]

    @classmethod
    def _get_passthrough_fields(cls):
        return ['username', 'created', 'updated',
                'first_name', 'last_name', 'title',
                'secondary_email', 'cell_phone_number',
                'access_level' ]

'''*****************************************************************
BillingAccountHandler
*****************************************************************'''
class BillingAccountHandler(APIV2Handler):
    """This talks to a sevice and creates a billing account with our
          external billing integration (currently stripe).

       This acts as an upreate function, essentially always call
        post, to save account information on the recurly side of
        things.
    """
    @tornado.gen.coroutine
    def post(self, account_id):
        schema = Schema({
          Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
          Required('billing_token_ref') : All(
              Coerce(str),
              Length(min=1, max=512))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        billing_token_ref = args.get('billing_token_ref')
        account = yield neondata.NeonUserAccount.get(
            account_id,
            async=True)

        if not account:
            raise NotFoundError('Neon Account required.')

        customer_id = None

        @tornado.gen.coroutine
        def _create_account():
            customer = yield self.executor.submit(
                stripe.Customer.create,
                email=account.email,
                source=billing_token_ref)
            cid = customer.id
            _log.info('New Stripe customer %s created with id %s' % (
                account.email, cid))
            raise tornado.gen.Return(customer)

        try:
            if account.billing_provider_ref:
                customer = yield self.executor.submit(
                    stripe.Customer.retrieve,
                    account.billing_provider_ref)

                customer.email = account.email or customer.email
                customer.source = billing_token_ref
                customer_id = customer.id
                yield self.executor.submit(customer.save)
            else:
                customer = yield _create_account()
        except stripe.error.InvalidRequestError as e:
            if 'No such customer' in str(e):
                # this is here just in case the ref got
                # screwed up, it should rarely if ever happen
                customer = yield _create_account()
            else:
                _log.error('Invalid request error we do not handle %s' % e)
                raise
        except Exception as e:
            _log.error('Unknown error occurred talking to Stripe %s' % e)
            raise

        def _modify_account(a):
            a.billed_elsewhere = False
            a.billing_provider_ref = customer.id

        yield neondata.NeonUserAccount.modify(
            account.neon_api_key,
            _modify_account,
            async=True)

        result = yield self.db2api(customer)

        self.success(result)

    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)

        account = yield neondata.NeonUserAccount.get(
            account_id,
            async=True)

        if not account:
            raise NotFoundError('Neon Account required.')

        if not account.billing_provider_ref:
            raise NotFoundError('No billing account found - no ref.')

        try:
            customer = yield self.executor.submit(
                stripe.Customer.retrieve,
                account.billing_provider_ref)

        except stripe.error.InvalidRequestError as e:
            if 'No such customer' in str(e):
                raise NotFoundError('No billing account found - not in stripe')
            else:
                _log.error('Unknown invalid error occurred talking'\
                           ' to Stripe %s' % e)
                raise Exception('Unknown Stripe Error')
        except Exception as e:
            _log.error('Unknown error occurred talking to Stripe %s' % e)
            raise

        result = yield self.db2api(customer)
        self.success(result)

    @classmethod
    def _get_default_returned_fields(cls):
        return ['id', 'account_balance', 'created', 'currency',
                'default_source', 'delinquent', 'description',
                'discount', 'email', 'livemode', 'subscriptions',
                'metadata', 'sources']

    @classmethod
    def _get_passthrough_fields(cls):
        return ['id', 'account_balance', 'created', 'currency',
                'default_source', 'delinquent', 'description',
                'discount', 'email', 'livemode']

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'subscriptions':
            retval = obj.subscriptions.to_dict()
        elif field == 'sources':
            retval = obj.sources.to_dict()
        elif field == 'metadata':
            retval = obj.metadata.to_dict()
        else:
            raise BadRequestError('invalid field %s' % field)

        raise tornado.gen.Return(retval)


    @classmethod
    def get_access_levels(cls):
        return {
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE,
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required'  : [HTTPVerbs.POST, HTTPVerbs.GET]
               }

'''*****************************************************************
BillingSubscriptionHandler
*****************************************************************'''
class BillingSubscriptionHandler(APIV2Handler):
    """This talks to recurly and creates a billing subscription with our
          recurly integration.
    """
    @tornado.gen.coroutine
    def post(self, account_id):
        schema = Schema({
          Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
          Required('plan_type'): All(Coerce(str), Length(min=1, max=32))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        plan_type = args.get('plan_type')

        account = yield neondata.NeonUserAccount.get(
            account_id,
            async=True)

        billing_plan = yield neondata.BillingPlans.get(
            plan_type,
            async=True)

        if not billing_plan:
            raise NotFoundError('No billing plan for that plan_type')

        if not account:
            raise NotFoundError('Neon Account was not found')

        if not account.billing_provider_ref:
            raise NotFoundError(
                'There is not a billing account set up for this account')
        try:
            original_plan_type = account.subscription_information['plan']['id']
        except TypeError:
            original_plan_type = None

        try:
            customer = yield self.executor.submit(
                stripe.Customer.retrieve,
                account.billing_provider_ref)

            # get all subscriptions, they are sorted
            # by most recent, if there are not any, just
            # submit the new one, otherwise cancel the most
            # recent and submit the new one
            cust_subs = yield self.executor.submit(
                customer.subscriptions.all)

            if len(cust_subs['data']) > 0:
                cancel_me = cust_subs['data'][0]
                yield self.executor.submit(cancel_me.delete)

            if plan_type == 'demo':
                subscription = stripe.Subscription(id='canceled')
                def _modify_account(a):
                    a.subscription_information = None
                    a.billed_elsewhere = True
                    a.billing_provider_ref = None
                    a.verify_subscription_expiry = None
                # cancel all the things!
                cards = yield self.executor.submit(
                    customer.sources.all,
                    object='card')
                for card in cards:
                    yield self.executor.submit(card.delete)
                _log.info('Subscription downgraded for account %s' %
                     account.neon_api_key)
            else:
                subscription = yield self.executor.submit(
                    customer.subscriptions.create,
                    plan=plan_type)
                def _modify_account(a):
                    a.serving_enabled = True
                    a.subscription_information = subscription
                    a.verify_subscription_expiry = \
                        (datetime.utcnow() + timedelta(
                        seconds=options.get(
                        'cmsapiv2.apiv2.check_subscription_interval'))
                        ).strftime(
                            "%Y-%m-%d %H:%M:%S.%f")

                _log.info('New subscription created for account %s' %
                    account.neon_api_key)

            yield neondata.NeonUserAccount.modify(
                account.neon_api_key,
                _modify_account,
                async=True)

        except stripe.error.InvalidRequestError as e:
            if 'No such customer' in str(e):
                _log.error('Billing mismatch for account %s' % account.email)
                raise NotFoundError('No billing account found in Stripe')

            _log.error('Unhandled InvalidRequestError\
                 occurred talking to Stripe %s' % e)
            raise
        except stripe.error.CardError as e:
            raise
        except Exception as e:
            _log.error('Unknown error occurred talking to Stripe %s' % e)
            raise

        billing_plan = yield neondata.BillingPlans.get(
            plan_type.lower(),
            async=True)

        # only update limits if we have actually changed the plan type
        if original_plan_type != plan_type.lower():
            def _modify_limits(a):
                a.populate_with_billing_plan(billing_plan)

            yield neondata.AccountLimits.modify(
                account.neon_api_key,
                _modify_limits,
                create_missing=True,
                async=True)

        result = yield self.db2api(subscription)

        self.success(result)

    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)

        account = yield neondata.NeonUserAccount.get(
            account_id,
            async=True)

        if not account:
            raise NotFoundError('Neon Account required.')

        if not account.billing_provider_ref:
            raise NotFoundError('No billing account found - no ref.')

        try:
            customer = yield self.executor.submit(
                stripe.Customer.retrieve,
                account.billing_provider_ref)

            cust_subs = yield self.executor.submit(
                customer.subscriptions.all)

            most_recent_sub = cust_subs['data'][0]
        except stripe.error.InvalidRequestError as e:
            if 'No such customer' in str(e):
                raise NotFoundError('No billing account found - not in stripe')
            else:
                _log.error('Unknown invalid error occurred talking'\
                           ' to Stripe %s' % e)
                raise Exception('Unknown Stripe Error')
        except IndexError:
            raise NotFoundError('A subscription was not found.')
        except Exception as e:
            _log.error('Unknown error occurred talking to Stripe %s' % e)
            raise

        result = yield self.db2api(most_recent_sub)

        self.success(result)

    @classmethod
    def _get_default_returned_fields(cls):
        return ['id', 'application_fee_percent', 'cancel_at_period_end',
                'canceled_at', 'current_period_end', 'current_period_start',
                'customer', 'discount', 'ended_at', 'plan',
                'quantity', 'start', 'tax_percent', 'trial_end',
                'trial_start']

    @classmethod
    def _get_passthrough_fields(cls):
        return ['id', 'application_fee_percent', 'cancel_at_period_end',
                'canceled_at', 'current_period_end', 'current_period_start',
                'customer', 'discount', 'ended_at', 'metadata', 'plan',
                'quantity', 'start', 'tax_percent', 'trial_end',
                'trial_start']

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'metadata':
            retval = obj.metadata.to_dict()
        else:
            raise BadRequestError('invalid field %s' % field)

        raise tornado.gen.Return(retval)

    @classmethod
    def get_access_levels(cls):
        return {
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE,
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required'  : [HTTPVerbs.POST]
               }

'''*********************************************************************
TelemetrySnippetHandler : class responsible for creating the telemetry snippet
   HTTP Verbs     : get
*********************************************************************'''
class TelemetrySnippetHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        '''Generates a telemetry snippet for a given account'''

        schema = Schema({
            Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
            })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        data = schema(args)

        # Find out if there is a Gallery integration
        integrations = yield self.account.get_integrations(async=True)

        using_gallery = any([x.uses_bc_gallery for x in integrations if
                             isinstance(x, neondata.BrightcoveIntegration)])

        # Build the snippet
        if using_gallery:
            template = (
                '<!-- Neon -->',
                '<script id="neon">',
                "  var neonPublisherId = '{tai}';",
                "  var neonBrightcoveGallery = true;",
                '</script>',
                "<script src='//cdn.neon-lab.com/neonoptimizer_dixon.js'></script>',",
                '<!-- Neon -->'
                )
        else:
            template = (
                '<!-- Neon -->',
                '<script id="neon">',
                "  var neonPublisherId = '{tai}';",
                '</script>',
                "<script src='//cdn.neon-lab.com/neonoptimizer_dixon.js'></script>',",
                '<!-- Neon -->'
                )

        self.set_header('Content-Type', 'text/plain')
        self.success('\n'.join(template).format(
            tai=self.account.tracker_account_id))

    @classmethod
    def get_access_levels(cls):
        return {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required'  : [HTTPVerbs.GET]
               }

'''*****************************************************************
BatchHandler 
*****************************************************************'''
class BatchHandler(APIV2Handler):
    @tornado.gen.coroutine
    def post(self):
        schema = Schema({
          Required('call_info') : All(CustomVoluptuousTypes.Dictionary())
        })
      
        args = self.parse_args()
        schema(args)
         
        call_info = args['call_info']
        access_token = call_info.get('access_token', None) 
        refresh_token = call_info.get('refresh_token', None)
        
        client = cmsapiv2.client.Client(
            access_token=access_token,  
            refresh_token=refresh_token)

        requests = call_info.get('requests', None)
        output = { 'results' : [] } 
        for req in requests: 
            # request will be information about 
            # the call we want to make 
            result = {} 
            try:
                result['relative_url'] = req['relative_url'] 
                result['method'] = req['method']
 
                method = req['method'] 
                http_req = tornado.httpclient.HTTPRequest(
                    req['relative_url'], 
                    method=method) 

                if method == 'POST' or method == 'PUT': 
                    http_req.headers = {"Content-Type" : "application/json"}
                    http_req.body = json.dumps(req.get('body', None))
                
                response = yield client.send_request(http_req)
                if response.error:
                    error = { 'error' : 
                        { 
                            'message' : response.reason, 
                            'code' : response.code 
                        } 
                    }
                    result['response'] = error 
                else:  
                    result['relative_url'] = req['relative_url'] 
                    result['method'] = req['method'] 
                    result['response'] = json.loads(response.body)
                    result['response_code'] = response.code
            except AttributeError:
                result['response'] = 'Malformed Request'
            except Exception as e: 
                result['response'] = 'Unknown Error Occurred' 
            finally: 
                output['results'].append(result)
                 
        self.success(output) 
 
    @classmethod
    def get_access_levels(cls):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.NONE 
               }

class EmailHandler(APIV2Handler): 
    @tornado.gen.coroutine
    def post(self, account_id):
        schema = Schema({
            Required('account_id') : All(Coerce(str), Length(min=1, max=256)), 
            Required('template_slug') : All(Coerce(str), Length(min=1, max=512)), 
            'template_args' : All(Coerce(str), Length(min=1, max=2048)),
            'to_email_address' : All(Coerce(str), Length(min=1, max=1024)),
            'from_email_address' : All(Coerce(str), Length(min=1, max=1024)),
            'from_name' : All(Coerce(str), Length(min=1, max=1024)),
            'subject' : All(Coerce(str), Length(min=1, max=1024)),
            'reply_to' : All(Coerce(str), Length(min=1, max=1024))
        })

        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        data = schema(args)
        
        args_email = args.get('to_email_address', None)
        template_args = args.get('template_args', None)  
        template_slug = args.get('template_slug') 
          
        cur_user = self.user 
        if cur_user:
            cur_user_email = cur_user.username

        if args_email: 
            send_to_email = args_email
        elif cur_user_email: 
            send_to_email = cur_user_email
            if not cur_user.send_emails: 
                self.success({'message' : 'user does not want emails'})
        else:  
            raise NotFoundError('Email address is required.')

        url = '{base_url}/templates/info.json?key={api_key}&name={slug}'.format(
            base_url=options.mandrill_base_url, 
            api_key=options.mandrill_api_key, 
            slug=template_slug) 
            
        request = tornado.httpclient.HTTPRequest(
            url=url,
            method="GET",
            request_timeout=8.0)

        response = yield tornado.gen.Task(utils.http.send_request, request)
        if response.code != ResponseCode.HTTP_OK:
            statemon.state.increment('mandrill_template_not_found')
            raise BadRequestError('Mandrill template unable to be loaded.')
        
        template_obj = json.loads(response.body) 
        template_string = template_obj['code'] 

        if template_args: 
            email_html = template_string.format(**template_args)
        else: 
            email_html = template_string
 
        # send email via mandrill
        headers_dict = { 
            'Reply-To' : args.get('reply_to', 'no-reply@neonlabs.com') 
        }
        to_list = [{ 
            'email' : send_to_email, 
            'type' : 'to'     
        }]   
        message_dict = { 
            'html' : email_html, 
            'subject' : args.get('subject', 'Neon Labs'),
            'from_email' : args.get('from_email_address', 
                'admin@neon-lab.com'),  
            'from_name' : args.get('from_name', 'Neon Labs'),
            'headers' : headers_dict, 
            'to' : to_list  
        }
        json_body = { 
            'key' : options.mandrill_api_key, 
            'message' : message_dict  
        }
 
        url = '{base_url}/messages/send.json'.format(
            base_url=options.mandrill_base_url) 
        
        request = tornado.httpclient.HTTPRequest( 
            url=url, 
            body=json.dumps(json_body),
            method='POST', 
            headers = {"Content-Type" : "application/json"},
            request_timeout=20.0)
 
        response = yield tornado.gen.Task(utils.http.send_request, request)

        if response.code != ResponseCode.HTTP_OK:
            statemon.state.increment('mandrill_email_not_sent')
            raise BadRequestError('Unable to send email')  
        
        self.success({'message' : 'Email sent to %s' % send_to_email }) 
            
    @classmethod
    def get_access_levels(cls):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE, 
                 'account_required'  : [HTTPVerbs.POST] 
               }

'''*********************************************************************
Endpoints
*********************************************************************'''
application = tornado.web.Application([
    (r'/api/v2/batch/?$',                                          BatchHandler),
    (r'/api/v2/tags/search/?$',                                    TagSearchInternalHandler),
    (r'/api/v2/videos/search/?$',                                  VideoSearchInternalHandler),
    (r'/api/v2/(\d+)/live_stream',                                 LiveStreamHandler),

    (r'/api/v2/([a-zA-Z0-9]+)/?$',                                 AccountHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/billing/account/?$',                 BillingAccountHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/billing/subscription/?$',            BillingSubscriptionHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/email/?$',                           EmailHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/?$',                    AccountIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove/?$',         BrightcoveIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove/players/?$', BrightcovePlayerHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/ooyala/?$',             OoyalaIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/optimizely/?$',         OptimizelyIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/limits/?$',                          AccountLimitsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/statistics/estimated_lift/?$',       LiftStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/statistics/thumbnails/?$',           ThumbnailStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/statistics/videos/?$',               VideoStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/stats/estimated_lift/?$',            LiftStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/stats/thumbnails/?$',                ThumbnailStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/stats/videos/?$',                    VideoStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/tags/?$',                            TagHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/tags/search/?$',                     TagSearchExternalHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/telemetry/snippet/?$',               TelemetrySnippetHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/thumbnails/?$',                      ThumbnailHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/users/?$',                           UserHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/?$',                          VideoHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/search/?$',                   VideoSearchExternalHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/share/?$',                    VideoShareHandler),

    (r'/healthcheck/?$',                                           HealthCheckHandler)
], gzip=True)

def main():
    global server
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    server = tornado.httpserver.HTTPServer(application)
    #utils.ps.register_tornado_shutdown(server)
    server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
