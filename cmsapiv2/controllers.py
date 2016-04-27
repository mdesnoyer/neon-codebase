#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from apiv2 import *

_log = logging.getLogger(__name__)

define("port", default=8084, help="run on the given port", type=int)
define("video_server", default="50.19.216.114", help="thumbnails.neon api", type=str)
define("video_server_port", default=8081, help="what port the video server is running on", type=int)
define("cmsapiv1_port", default=8083, help="what port apiv1 is running on", type=int)

statemon.define('put_account_oks', int)
statemon.define('get_account_oks', int)

statemon.define('post_ooyala_oks', int)
statemon.define('put_ooyala_oks', int)
statemon.define('get_ooyala_oks', int)

statemon.define('post_brightcove_oks', int)
statemon.define('put_brightcove_oks', int)
statemon.define('get_brightcove_oks', int)

statemon.define('post_thumbnail_oks', int)
statemon.define('put_thumbnail_oks', int)
statemon.define('get_thumbnail_oks', int)

statemon.define('post_video_oks', int)
statemon.define('put_video_oks', int)
statemon.define('get_video_oks', int)
_get_video_oks_ref = statemon.state.get_ref('get_video_oks')

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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          'default_width': All(Coerce(int), Range(min=1, max=8192)),
          'default_height': All(Coerce(int), Range(min=1, max=8192)),
          'default_thumbnail_id': Any(str, unicode, Length(min=1, max=2048))
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

        result = yield tornado.gen.Task(neondata.NeonUserAccount.modify,
                                        acct_internal.key, _update_account)
        statemon.state.increment('put_account_oks')
        self.success(acct_for_return)

    @classmethod
    def get_access_levels(cls):
        return {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET, HTTPVerbs.PUT]
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
    def create_integration(acct, args, integration_type):
        """Creates an integration for any integration type.

        Keyword arguments:
        acct - a NeonUserAccount object
        args - the args sent in via the API request
        integration_type - the type of integration to create
        """

        if integration_type == neondata.IntegrationType.OOYALA:
            integration = neondata.OoyalaIntegration()
            integration.account_id = acct.neon_api_key
            integration.partner_code = args['publisher_id']
            integration.api_key = args.get('api_key', integration.api_key)
            integration.api_secret = args.get('api_secret', integration.api_secret)
            integration.save()

        elif integration_type == neondata.IntegrationType.BRIGHTCOVE:
            integration = neondata.BrightcoveIntegration()
            integration.account_id = acct.neon_api_key
            integration.publisher_id = args['publisher_id']
            integration.read_token = args.get('read_token', integration.read_token)
            integration.write_token = args.get('write_token', integration.write_token)
            integration.callback_url = args.get('callback_url', integration.callback_url)
            playlist_feed_ids = args.get('playlist_feed_ids', None)
            if playlist_feed_ids:
                integration.playlist_feed_ids = playlist_feed_ids.split(',')
            integration.id_field = args.get('id_field', integration.id_field)
            integration.uses_batch_provisioning = bool(int(args.get('uses_batch_provisioning',
                                                          integration.uses_batch_provisioning)))
            integration.save()

        result = yield tornado.gen.Task(acct.modify,
                                        acct.neon_api_key,
                                        lambda p: p.add_platform(integration))

        # ensure the integration made it to the database by executing a get
        if integration_type == neondata.IntegrationType.OOYALA:
            integration = yield tornado.gen.Task(neondata.OoyalaIntegration.get,
                                              integration.integration_id)
        elif integration_type == neondata.IntegrationType.BRIGHTCOVE:
            integration = yield tornado.gen.Task(neondata.BrightcoveIntegration.get,
                                              integration.integration_id)
        if integration:
            raise tornado.gen.Return(integration)
        else:
            raise SaveError('unable to save the integration')

    @staticmethod
    @tornado.gen.coroutine
    def get_integration(integration_id, integration_type):
        """Gets an integration based on integration_id and type.

        Keyword arguments:
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('publisher_id') : All(Coerce(str), Length(min=1, max=256)),
          'api_key': Any(str, unicode, Length(min=1, max=1024)),
          'api_secret': Any(str, unicode, Length(min=1, max=1024)),
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
        integration = yield tornado.gen.Task(IntegrationHelper.create_integration, acct, args, neondata.IntegrationType.OOYALA)
        statemon.state.increment('post_ooyala_oks')
        rv = yield self.db2api(integration)
        self.success(rv)

    @tornado.gen.coroutine
    def get(self, account_id):
        """handles an ooyala endpoint get request"""

        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
          'api_key': Any(str, unicode, Length(min=1, max=1024)),
          'api_secret': Any(str, unicode, Length(min=1, max=1024)),
          'publisher_id': Any(str, unicode, Length(min=1, max=1024))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id']

        integration = yield IntegrationHelper.get_integration(integration_id,
                                                     neondata.IntegrationType.OOYALA)

        def _update_integration(p):
            p.api_key = args.get('api_key', integration.api_key)
            p.api_secret = args.get('api_secret', integration.api_secret)
            p.partner_code = args.get('publisher_id', integration.partner_code)

        result = yield tornado.gen.Task(neondata.OoyalaIntegration.modify,
                                        integration_id,
                                        _update_integration)

        ooyala_integration = yield IntegrationHelper.get_integration(integration_id,
                                                            neondata.IntegrationType.OOYALA)

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
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET,
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
        """Get the list of BrightcovePlayer for the given integration"""

        schema = Schema({
            Required('integration_id'): Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        schema(args)

        players = yield neondata.BrightcovePlayers.get_players(integration_id, async=True)
        rv = map(self.db2api, players)
        self.success(rv)

    @tornado.gen.coroutine
    def put(self, account_id):
        """Update a BrightcovePlayer and return it"""

        # The only field that is set via public api is is_tracked.
        schema = Schema({
            Required('account_id'): Any(str, unicode, Length(min=1, max=256)),
            Required('player_id'): Any(str, unicode, Length(min=1, max=256)),
            Required('is_tracked'): Boolean()
        })
        args = self.parse_args()
        scheme(args)

        player = yield neondata.BrightcovePlayer.get(args['player_id', async=True)
        if not player:
            raise NotFoundError('Player does not exist for id:%s', args['player_id'])
        if player.account_id != account_id:
            raise NotAuthorizedError('Player is not owned by this account')

        def _update(p):
            p.is_tracked = Boolean()(args['is_tracked'])

        yield neondata.BrightcovePlayer.modify(args['player_id'], _update, async=True)

        # If the player is tracked, then send a request to Brightcove's
        # CMS Api to put the plugin in the player and publish the player.
        if player.is_tracked:
            publish_result, error = _publish_plugin_to_player(player)

        player = yield neondata.BrightcovePlayer.get(args['player_id'])
        rv = yield self.db2api(player)
        self.success(rv)

    @tornado.gen.corotune
    def _publish_plugin_to_player
        integration = yield neondata.BrightcoveIntegration.get(player.integration_id, async=True)
        bc_api = BrightcovePlayerManagementApi(integration=integration)
        return True, None

'''*********************************************************************
BrightcoveIntegrationHandler
*********************************************************************'''
class BrightcoveIntegrationHandler(APIV2Handler):
    """handles all requests to the brightcove endpoint within the v2 API"""
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a brightcove endpoint post request"""

        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('publisher_id') : All(Coerce(str), Length(min=1, max=256)),
          'read_token': Any(str, unicode, Length(min=1, max=512)),
          'write_token': Any(str, unicode, Length(min=1, max=512)),
          'callback_url': Any(str, unicode, Length(min=1, max=1024)),
          'id_field': Any(str, unicode, Length(min=1, max=32)),
          'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
          'uses_batch_provisioning': Boolean()
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
        integration = yield IntegrationHelper.create_integration(acct,
                                                             args,
                                                             neondata.IntegrationType.BRIGHTCOVE)
        statemon.state.increment('post_brightcove_oks')
        rv = yield self.db2api(integration)
        self.success(rv)

    @tornado.gen.coroutine
    def get(self, account_id):
        """handles a brightcove endpoint get request"""

        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
          'read_token': Any(str, unicode, Length(min=1, max=1024)),
          'write_token': Any(str, unicode, Length(min=1, max=1024)),
          'application_client_id': Any(str, unicode, Length(min=1, max=1024)),
          'application_client_secret': Any(str, unicode, Length(min=1, max=1024)),
          'callback_url': Any(str, unicode, Length(min=1, max=1024)),
          'publisher_id': Any(str, unicode, Length(min=1, max=512)),
          'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
          'uses_batch_provisioning': Boolean(),
          'uses_bc_thumbnail_api': Boolean(),
          'uses_bc_videojs_player': Boolean(),
          'uses_bc_smart_player': Boolean(),
          'uses_bc_gallery': Boolean()
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        integration_id = args['integration_id']
        schema(args)

        integration = yield IntegrationHelper.get_integration(
            integration_id,
            neondata.IntegrationType.BRIGHTCOVE)

        def _update_integration(p):
            p.read_token = args.get('read_token', integration.read_token)
            p.write_token = args.get('write_token', integration.write_token)
            p.publisher_id = args.get('publisher_id', integration.publisher_id)
            playlist_feed_ids = args.get('playlist_feed_ids', None)
            if playlist_feed_ids:
                p.playlist_feed_ids = playlist_feed_ids.split(',')
            p.uses_batch_provisioning = Boolean()(
               args.get('uses_batch_provisioning',
               integration.uses_batch_provisioning))
            p.uses_bc_thumbnail_api = Boolean()(
               args.get('uses_bc_thumbnail_api',
               integration.uses_batch_provisioning))
            p.uses_bc_videojs_player = Boolean()(
               args.get('uses_bc_videojs_player',
               integration.uses_batch_provisioning))
            p.uses_bc_smart_player = Boolean()(
               args.get('uses_bc_smart_player',
               integration.uses_batch_provisioning))
            p.uses_bc_gallery = Boolean()(
               args.get('uses_bc_gallery',
               integration.uses_batch_provisioning))

        result = yield neondata.BrightcoveIntegration.modify(
            integration_id,
            _update_integration,
            async=True)

        integration = yield IntegrationHelper.get_integration(
            integration_id,
            neondata.IntegrationType.BRIGHTCOVE)

        statemon.state.increment('put_brightcove_oks')
        rv = yield self.db2api(integration)
        self.success(rv)

    @classmethod
    def _get_default_returned_fields(cls):
        return [ 'integration_id', 'account_id', 'read_token',
                 'write_token', 'last_process_date', 'publisher_id',
                 'callback_url', 'enabled', 'playlist_feed_ids',
                 'uses_batch_provisioning', 'id_field',
                 'created', 'updated' ]

    @classmethod
    def _get_passthrough_fields(cls):
        return [ 'integration_id', 'read_token', 'account_id',
                 'write_token', 'last_process_date', 'publisher_id',
                 'callback_url', 'enabled', 'playlist_feed_ids',
                 'uses_batch_provisioning', 'id_field',
                 'created', 'updated' ]

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET,
                                        HTTPVerbs.PUT,
                                        HTTPVerbs.POST]
               }

'''*********************************************************************
ThumbnailHandler
*********************************************************************'''
class ThumbnailHandler(APIV2Handler):
    """handles all requests to the thumbnails endpoint within the v2 API"""
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a thumbnail endpoint post request"""

        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('url') : Any(str, unicode, Length(min=1, max=2048)),
          'thumbnail_ref' : Any(str, unicode, Length(min=1, max=1024)) 
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        video_id = args['video_id']
        internal_video_id = neondata.InternalVideoID.generate(
            account_id_api_key, video_id)
        external_thumbnail_id = args.get('thumbnail_ref', None) 

        video = yield neondata.VideoMetadata.get(
            internal_video_id, 
            async=True)

        current_thumbnails = yield neondata.ThumbnailMetadata.get_many( 
            video.thumbnail_ids, 
            async=True)

        cdn_key = neondata.CDNHostingMetadataList.create_key(
            account_id_api_key, 
            video.integration_id)

        cdn_metadata = yield neondata.CDNHostingMetadataList.get(
            cdn_key, 
            async=True)

        # ranks can be negative 
        min_rank = 1
        for thumb in current_thumbnails:
            if (thumb.type == neondata.ThumbnailType.CUSTOMUPLOAD and
                thumb.rank < min_rank):
                min_rank = thumb.rank
        cur_rank = min_rank - 1

        new_thumbnail = neondata.ThumbnailMetadata(
            None,
            internal_vid=internal_video_id, 
            external_id=external_thumbnail_id,
            ttype=neondata.ThumbnailType.CUSTOMUPLOAD, 
            rank=cur_rank)

        # upload image to cdn 
        yield video.download_and_add_thumbnail(
            new_thumbnail,
            image_url=args['url'],
            cdn_metadata=cdn_metadata,
            async=True)

        #save the thumbnail
        yield new_thumbnail.save(async=True)

        # save the video 
        new_video = yield neondata.VideoMetadata.modify(
            internal_video_id, 
            lambda x: x.thumbnail_ids.append(new_thumbnail.key),
            async=True) 

        if new_video:
            statemon.state.increment('post_thumbnail_oks')
            new_thumbnail = yield neondata.ThumbnailMetadata.get(
                new_thumbnail.key,
                async=True)
            retobj = yield self.db2api(new_thumbnail)
            self.success(retobj, code=ResponseCode.HTTP_ACCEPTED)
        else:
            raise SaveError('unable to save thumbnail to video')

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a thumbnail endpoint put request"""

        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_id') : Any(str, unicode, Length(min=1, max=512)),
          'enabled': Boolean()
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_id') : Any(str, unicode, Length(min=1, max=512)),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        thumbnail_id = args['thumbnail_id']
        thumbnail = yield tornado.gen.Task(neondata.ThumbnailMetadata.get,
                                           thumbnail_id)
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
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET,
                                        HTTPVerbs.PUT,
                                        HTTPVerbs.POST]
               }

    @classmethod
    def _get_default_returned_fields(cls):
        return ['video_id', 'thumbnail_id', 'rank', 'frameno',
                'neon_score', 'enabled', 'url', 'height', 'width',
                'type', 'external_ref', 'created', 'updated']

    @classmethod
    def _get_passthrough_fields(cls):
        return ['rank', 'frameno', 'enabled', 'type', 'width', 'height',
                'created', 'updated']

    @classmethod
    @tornado.gen.coroutine
    def _convert_special_field(cls, obj, field):
        if field == 'video_id':
            retval = neondata.InternalVideoID.to_external(
                neondata.InternalVideoID.from_thumbnail_id(obj.key))
        elif field == 'thumbnail_id':
            retval = obj.key
        elif field == 'neon_score':
            # TODO(mdesnoyer): convert the model score into the neon score
            retval = obj.model_score
        elif field == 'url':
            retval = obj.urls[0] or []
        elif field == 'external_ref':
            retval = obj.external_id
        else:
            raise BadRequestError('invalid field %s' % field)

        raise tornado.gen.Return(retval)

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
        user_account = yield tornado.gen.Task(neondata.NeonUserAccount.get, account_id_api_key)
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
        yield tornado.gen.Task(request.save)

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
        video = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                       neondata.InternalVideoID.generate(account_id_api_key, video_id))
        if video is None:
            # make sure we can download the image before creating requests
            # create the api_request
            api_request = yield tornado.gen.Task(
                VideoHelper.create_api_request,
                args,
                account_id_api_key)

            video = neondata.VideoMetadata(
                neondata.InternalVideoID.generate(account_id_api_key, video_id),
                video_url=args.get('url', None),
                publish_date=args.get('publish_date', None),
                duration=float(args.get('duration', 0.0)) or None,
                custom_data=args.get('custom_data', None),
                i_id=api_request.integration_id,
                serving_enabled=False)

            default_thumbnail_url = args.get('default_thumbnail_url', None)
            if default_thumbnail_url:
                # save the default thumbnail
                image = yield video.download_image_from_url(
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
            api_request = yield tornado.gen.Task(
                VideoHelper.create_api_request,
                args,
                account_id_api_key)
            # add the job id save the video
            video.job_id = api_request.job_id
            yield tornado.gen.Task(video.save)
            raise tornado.gen.Return((video,api_request))
        else:
            reprocess = args.get('reprocess', False)
            if reprocess:

                reprocess_url = 'http://%s:%s/reprocess' % (
                    options.video_server,
                    options.video_server_port)
                # get the neonapirequest
                api_request = neondata.NeonApiRequest.get(video.job_id,
                                                          account_id_api_key)

                # send the request to the video server
                request = tornado.httpclient.HTTPRequest(url=reprocess_url,
                                                         method="POST",
                                                         body=api_request.to_json(),
                                                         request_timeout=30.0,
                                                         connect_timeout=15.0)
                response = yield tornado.gen.Task(utils.http.send_request, request)
                if response and response.code is ResponseCode.HTTP_OK:
                    raise tornado.gen.Return((video,api_request))
                else:
                    raise Exception('unable to communicate with video server',
                                    ResponseCode.HTTP_INTERNAL_SERVER_ERROR)
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

        raise tornado.gen.Return(thumbnails)

    @staticmethod
    @tornado.gen.coroutine
    def get_search_results(account_id=None,
                           since=None,
                           until=None,
                           query=None,
                           limit=None,
                           fields=None,
                           base_url='/api/v2/videos/search'):

        search_res = yield neondata.VideoMetadata.search_videos(
                         account_id,
                         since=since,
                         until=until,
                         limit=limit)

        videos = search_res['videos']
        since_time = search_res['since_time']
        until_time = search_res['until_time']
        vid_dict = yield VideoHelper.build_video_dict(
                       videos,
                       fields)

        next_page_url = VideoHelper.build_page_url(
            base_url,
            until_time if until_time else 0.0,
            limit=limit,
            page_type='until',
            query=query,
            fields=fields,
            account_id=account_id)

        prev_page_url = VideoHelper.build_page_url(
            base_url,
            since_time if since_time else 0.0,
            limit=limit,
            page_type='since',
            query=query,
            fields=fields,
            account_id=account_id)

        vid_dict['next_page'] = next_page_url
        vid_dict['prev_page'] = prev_page_url
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
        if videos:
            videos = [x for x in videos if x and x.job_id]
            job_ids = [(v.job_id, v.get_account_id())
                          for v in videos]

            requests = yield neondata.NeonApiRequest.get_many(
                           job_ids,
                           async=True)
            for video, request in zip(videos, requests):
                if video is None or request is None and video_ids:
                    new_videos.append({'error' : 'video does not exist',
                                       'video_id' : video_ids[index] })
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

        next_page_url = '%s?%s=%f&limit=%d' % (base_url,
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
    def db2api(video, request, fields=None):
        """Converts a database video metadata object to a video
        response dictionary

        Overrite the base function because we have to do a join on the request

        Keyword arguments:
        video - The VideoMetadata object
        request - The NeonApiRequest object
        fields - List of fields to return
        """
        if fields is None:
            fields = ['state', 'video_id', 'publish_date', 'title', 'url',
                      'testing_enabled', 'job_id']

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
                new_video[field] = request.video_title
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
class VideoHandler(APIV2Handler):
    @tornado.gen.coroutine
    def post(self, account_id):
        """handles a Video endpoint post request"""
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('external_video_ref') : Any(str, unicode, Length(min=1, max=512)),
          Optional('url'): Any(str, unicode, Length(min=1, max=512)),
          Optional('reprocess'): Boolean(),
          'integration_id' : Any(str, unicode, Length(min=1, max=256)),
          'callback_url': Any(str, unicode, Length(min=1, max=512)),
          'title': Any(str, unicode, Length(min=1, max=1024)),
          'duration': All(Coerce(float), Range(min=0.0, max=86400.0)),
          'publish_date': All(CustomVoluptuousTypes.Date()),
          'custom_data': All(CustomVoluptuousTypes.Dictionary()),
          'default_thumbnail_url': Any(str, unicode, Length(min=1, max=128)),
          'thumbnail_ref': Any(str, unicode, Length(min=1, max=512))
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
        vs_job_url = 'http://%s:%s/job' % (options.video_server,
                                           options.video_server_port)
        request = tornado.httpclient.HTTPRequest(url=vs_job_url,
                                                 method="POST",
                                                 body=api_request.to_json(),
                                                 request_timeout=30.0,
                                                 connect_timeout=10.0)

        response = yield tornado.gen.Task(utils.http.send_request, request)

        if response and response.code is ResponseCode.HTTP_OK:
            job_info = {}
            job_info['job_id'] = api_request.job_id
            job_info['video'] = yield self.db2api(new_video,
                                                  api_request)
            statemon.state.increment('post_video_oks')
            self.success(job_info,
                         code=ResponseCode.HTTP_ACCEPTED)
        else:
            raise Exception('unable to communicate with video server',
                            ResponseCode.HTTP_INTERNAL_SERVER_ERROR)

    @tornado.gen.coroutine
    def get(self, account_id):
        """handles a Video endpoint get request"""

        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(
              CustomVoluptuousTypes.CommaSeparatedList()),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        fields = args.get('fields', None)
        if fields:
            fields = set(fields.split(','))

        vid_dict = {}
        output_list = []
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(str, unicode, Length(min=1, max=256)),
          'testing_enabled': Boolean(),
          'title': Any(str, unicode, Length(min=1, max=1024))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        title = args.get('title', None)

        internal_video_id = neondata.InternalVideoID.generate(
            account_id_api_key,
            args['video_id'])

        def _update_video(v):
            v.testing_enabled = Boolean()(
                args.get('testing_enabled', v.testing_enabled))

        video = yield neondata.VideoMetadata.modify(
            internal_video_id,
            _update_video,
            async=True)

        if not video:
            raise NotFoundError('video does not exist with id: %s' %
                (args['video_id']))

        # we may need to update the request object as well
        db2api_fields = ['testing_enabled', 'video_id']
        api_request = None
        if title is not None and video.job_id is not None:
            def _update_request(r):
                r.video_title = title

            api_request = yield neondata.NeonApiRequest.modify(
                video.job_id,
                account_id,
                _update_request,
                async=True)

            db2api_fields.append('title')

        statemon.state.increment('put_video_oks')
        output = yield self.db2api(video, api_request,
                                   fields=db2api_fields)
        self.success(output)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET,
                                        HTTPVerbs.PUT,
                                        HTTPVerbs.POST]
               }

    @classmethod
    def get_limits(self):
        post_list = [{ 'left_arg' : 'video_posts',
                       'right_arg' : 'max_video_posts',
                       'operator' : '<',
                       'timer_info' : {
                           'refresh_time' : 'refresh_time_video_posts',
                           'add_to_refresh_time' : 'seconds_to_refresh_video_posts',
                           'timer_resets' : [ ('video_posts', 0) ]
                       },
                       'values_to_increase': [ ('video_posts', 1) ],
                       'values_to_decrease': []
        }]
        return {
                   HTTPVerbs.POST : post_list
               }

    @staticmethod
    @tornado.gen.coroutine
    def db2api(video, request, fields=None):
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(CustomVoluptuousTypes.CommaSeparatedList()),
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
        video_statuses = yield [self.db2api(x) for x in video_statuses]
        stats_dict['statistics'] = video_statuses
        stats_dict['count'] = len(video_statuses)

        self.success(stats_dict)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required'  : [HTTPVerbs.GET]
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Optional('thumbnail_id') : Any(CustomVoluptuousTypes.CommaSeparatedList()),
          Optional('video_id') : Any(CustomVoluptuousTypes.CommaSeparatedList(20)),
          Optional('fields'): Any(CustomVoluptuousTypes.CommaSeparatedList())
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        data = schema(args)
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
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required'  : [HTTPVerbs.GET]
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
                 HTTPVerbs.GET : neondata.AccessLevels.NONE
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)

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
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required' : [HTTPVerbs.GET]
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
          'limit' : All(Coerce(int), Range(min=1, max=100)),
          'account_id' : All(Coerce(str), Length(min=1, max=256)),
          'query' : All(Coerce(str), Length(min=1, max=256)),
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
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'internal_only' : True,
                 'account_required' : []
               }

'''*********************************************************************
VideoSearchExternalHandler : class responsible for searching videos from
                             an external source
   HTTP Verbs     : get
*********************************************************************'''
class VideoSearchExternalHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        schema = Schema({
          Required('account_id') : All(Coerce(str), Length(min=1, max=256)),
          'limit' : All(Coerce(int), Range(min=1, max=100)),
          'query' : All(Coerce(str), Length(min=1, max=256)),
          'fields': Any(CustomVoluptuousTypes.CommaSeparatedList()),
          'since': All(Coerce(float)),
          'until': All(Coerce(float))
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
                       base_url=base_url)

        self.success(vid_dict)

    @classmethod
    def get_access_levels(self):
        return {
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required' : [HTTPVerbs.GET]
               }

'''*********************************************************************
ThumbnailSearchInternalHandler : class responsible for searching thumbs
                                 from an internal source
   HTTP Verbs     : get
*********************************************************************'''
class ThumbnailSearchInternalHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self):
        self.success({})

'''*********************************************************************
ThumbnailSearchExternalHandler : class responsible for searching thumbs
                                 from an external source
   HTTP Verbs     : get
*********************************************************************'''
class ThumbnailSearchExternalHandler(APIV2Handler):
    @tornado.gen.coroutine
    def get(self, account_id):
        self.success({})

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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
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
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 'account_required' : [HTTPVerbs.GET]
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
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('username') : All(Coerce(str), Length(min=8, max=64)),
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
            raise NotAuthorizedError('Can not view another users account')

        result = yield self.db2api(user)

        self.success(result)

    @tornado.gen.coroutine
    def put(self, account_id):
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('username') : All(Coerce(str), Length(min=8, max=64)),
          Optional('access_level') : All(Coerce(int), Range(min=1, max=63)),
          'first_name': Any(str, unicode, Length(min=1, max=256)),
          'last_name': Any(str, unicode, Length(min=1, max=256)),
          'title': Any(str, unicode, Length(min=1, max=32))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        username = args.get('username')
        new_access_level = args.get('access_level')

        if self.user.access_level is not neondata.AccessLevels.GLOBAL_ADMIN:
            if self.user.username != username:
                raise NotAuthorizedError('Can not update another\
                               users account')

            if new_access_level > self.user.access_level:
                raise NotAuthorizedError('Can not set access_level above\
                               requesting users access level')

        def _update_user(u):
            u.access_level = new_access_level
            u.first_name = args.get('first_name', u.first_name)
            u.last_name = args.get('last_name', u.last_name)
            u.title = args.get('title', u.title)

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
                 HTTPVerbs.GET : neondata.AccessLevels.READ,
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET, HTTPVerbs.PUT]
               }

    @classmethod
    def _get_default_returned_fields(cls):
        return ['username', 'access_level', 'created', 'updated',
                'first_name', 'last_name', 'title' ]

    @classmethod
    def _get_passthrough_fields(cls):
        return ['username', 'access_level', 'created', 'updated',
                'first_name', 'last_name', 'title' ]

'''*********************************************************************
Endpoints
*********************************************************************'''
application = tornado.web.Application([
    (r'/healthcheck/?$', HealthCheckHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/ooyala/?$',
        OoyalaIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove/?$',
        BrightcoveIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove/players/?$',
        BrightcovePlayerHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/optimizely/?$',
        OptimizelyIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/?$',
        AccountIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/thumbnails/?$', ThumbnailHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/?$', VideoHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/search?$', VideoSearchExternalHandler),
    (r'/api/v2/videos/search?$', VideoSearchInternalHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/thumbnails/search?$',
        ThumbnailSearchExternalHandler),
    (r'/api/v2/thumbnails/search?$', ThumbnailSearchInternalHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/?$', AccountHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/limits/?$', AccountLimitsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/stats/videos?$', VideoStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/stats/thumbnails?$', ThumbnailStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/statistics/videos?$', VideoStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/statistics/thumbnails?$', ThumbnailStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/users?$', UserHandler),
    (r'/api/v2/(\d+)/live_stream', LiveStreamHandler)
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
