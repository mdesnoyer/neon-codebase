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

statemon.define('post_account_oks', int) 
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
          'default_thumbnail_id': Any(str, unicode, Length(min=1, max=2048)) 
        })
        args = self.parse_args()
        schema(args) 
        user = neondata.NeonUserAccount(uuid.uuid1().hex, name=args['customer_name'])
        user.default_size = list(user.default_size) 
        user.default_size[0] = args.get('default_width', neondata.DefaultSizes.WIDTH)
        user.default_size[1] = args.get('default_height', neondata.DefaultSizes.HEIGHT)
        user.default_size = tuple(user.default_size)
        user.default_thumbnail_id = args.get('default_thumbnail_id', None)

        output = yield tornado.gen.Task(neondata.NeonUserAccount.save, user)
        user = yield tornado.gen.Task(neondata.NeonUserAccount.get, user.neon_api_key)

        tracker_p_aid_mapper = neondata.TrackerAccountIDMapper(
                                 user.tracker_account_id, 
                                 user.neon_api_key, 
                                 neondata.TrackerAccountIDMapper.PRODUCTION)

        tracker_s_aid_mapper = neondata.TrackerAccountIDMapper(
                                 user.staging_tracker_account_id, 
                                 user.neon_api_key, 
                                 neondata.TrackerAccountIDMapper.STAGING)

        yield tornado.gen.Task(tracker_p_aid_mapper.save) 
        yield tornado.gen.Task(tracker_s_aid_mapper.save) 

        user = AccountHelper.db2api(user)
        
        _log.debug(('New Account has been added : name = %s id = %s') 
                   % (user['customer_name'], user['account_id']))
        statemon.state.increment('post_account_oks')
 
        self.success(json.dumps(user))

    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.POST : neondata.AccessLevels.CREATE 
               }  

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
 
        user_account = AccountHelper.db2api(user_account, fields=fields)
        statemon.state.increment('get_account_oks')
        self.success(json.dumps(user_account))
 
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
        acct_internal = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])

        if not acct_internal: 
            raise NotFoundError()
 
        acct_for_return = AccountHelper.db2api(acct_internal)
        def _update_account(a):
            a.default_size = list(a.default_size) 
            a.default_size[0] = int(args.get('default_width', acct_internal.default_size[0]))
            a.default_size[1] = int(args.get('default_height', acct_internal.default_size[1]))
            a.default_size = tuple(a.default_size)
            a.default_thumbnail_id = args.get('default_thumbnail_id', acct_internal.default_thumbnail_id)
 
        result = yield tornado.gen.Task(neondata.NeonUserAccount.modify, acct_internal.key, _update_account)
        statemon.state.increment('put_account_oks')
        self.success(json.dumps(acct_for_return))

    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.GET : neondata.AccessLevels.READ, 
                 HTTPVerbs.PUT : neondata.AccessLevels.UPDATE,
                 'account_required'  : [HTTPVerbs.GET, HTTPVerbs.PUT] 
               }

class AccountHelper(object):
    @staticmethod
    def db2api(account, fields=None):
        """Converts a database account object to an account
        response dictionary
         
        Keyword arguments: 
        account - The NeonUserAccount object
        fields - List of fields to return
        """
        if fields is None:
            fields = ['account_id', 'default_size', 'customer_name',
                      'default_thumbnail_id', 'tracker_account_id',
                      'staging_tracker_account_id',
                      'integration_ids', 'created', 'updated']
        
        obj = {}
        for field in fields:
            if field == 'account_id':
                 # this is weird, but neon_api_key is actually the
                 # "id" on this table, it's what we use to get information
                 # about the account, so send back api_key (as account_id)
                 obj[field] = account.neon_api_key
            elif field == 'default_size':
                obj[field] = account.default_size
            elif field == 'customer_name':
                obj[field] = account.name
            elif field == 'default_thumbnail_id':
                obj[field] = account.default_thumbnail_id
            elif field == 'tracker_account_id':
                obj[field] = account.tracker_account_id
            elif field == 'staging_tracker_account_id':
                obj[field] = account.staging_tracker_account_id
            elif field == 'integration_ids':
                obj[field] = account.integrations.keys()
            elif field == 'created':
                obj[field] = account.created
            elif field == 'updated':
                obj[field] = account.updated
            else:
                raise BadRequestError('invalid field %s' % field)
                
        return obj
         

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
            integration.api_key = args.get('ooyala_api_key', integration.api_key)
            integration.api_secret = args.get('ooyala_api_secret', integration.api_secret)
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
          'ooyala_api_key': Any(str, unicode, Length(min=1, max=1024)), 
          'ooyala_api_secret': Any(str, unicode, Length(min=1, max=1024)), 
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
        integration = yield tornado.gen.Task(IntegrationHelper.create_integration, acct, args, neondata.IntegrationType.OOYALA)
        statemon.state.increment('post_ooyala_oks')
        self.success(json.dumps(integration.__dict__))
 
    @tornado.gen.coroutine
    def get(self, account_id):
        """handles an ooyala endpoint get request"""
 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id'] 
        integration = yield IntegrationHelper.get_integration(integration_id, 
                                                    neondata.IntegrationType.OOYALA)

        statemon.state.increment('get_ooyala_oks')
        self.success(json.dumps(integration.__dict__))

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles an ooyala endpoint put request"""
 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
          'ooyala_api_key': Any(str, unicode, Length(min=1, max=1024)), 
          'ooyala_api_secret': Any(str, unicode, Length(min=1, max=1024)), 
          'publisher_id': Any(str, unicode, Length(min=1, max=1024))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id'] 
            
        integration = yield IntegrationHelper.get_integration(integration_id, 
                                                     neondata.IntegrationType.OOYALA)

        def _update_integration(p):
            p.api_key = args.get('ooyala_api_key', integration.api_key)
            p.api_secret = args.get('ooyala_api_secret', integration.api_secret)
            p.partner_code = args.get('publisher_id', integration.partner_code)
 
        result = yield tornado.gen.Task(neondata.OoyalaIntegration.modify, 
                                        integration_id, 
                                        _update_integration)

        ooyala_integration = yield IntegrationHelper.get_integration(integration_id, 
                                                            neondata.IntegrationType.OOYALA)
 
        statemon.state.increment('put_ooyala_oks')
        self.success(json.dumps(ooyala_integration.__dict__))

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
          'id_field': Any(str, unicode, Length(min=1, max=32)),
          'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
          'uses_batch_provisioning': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = str(account_id)
        schema(args)
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, args['account_id'])
        integration = yield IntegrationHelper.create_integration(acct, 
                                                             args, 
                                                             neondata.IntegrationType.BRIGHTCOVE)
        statemon.state.increment('post_brightcove_oks')
        self.success(json.dumps(integration.__dict__))

    @tornado.gen.coroutine
    def get(self, account_id):  
        """handles a brightcove endpoint get request"""
 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        schema(args)
        integration_id = args['integration_id'] 
        integration = yield IntegrationHelper.get_integration(integration_id,  
                                                       neondata.IntegrationType.BRIGHTCOVE) 
        statemon.state.increment('get_brightcove_oks')
        self.success(json.dumps(integration.__dict__))

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a brightcove endpoint put request"""

        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('integration_id') : Any(str, unicode, Length(min=1, max=256)),
          'read_token': Any(str, unicode, Length(min=1, max=1024)), 
          'write_token': Any(str, unicode, Length(min=1, max=1024)), 
          'publisher_id': Any(str, unicode, Length(min=1, max=512)),
          'playlist_feed_ids': All(CustomVoluptuousTypes.CommaSeparatedList()),
          'uses_batch_provisioning': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = account_id = str(account_id)
        integration_id = args['integration_id'] 
        schema(args)

        integration = yield IntegrationHelper.get_integration(integration_id,  
                                                  neondata.IntegrationType.BRIGHTCOVE) 

        def _update_integration(p):
            p.read_token = args.get('read_token', integration.read_token)
            p.write_token = args.get('write_token', integration.write_token)
            p.publisher_id = args.get('publisher_id', integration.publisher_id)
            playlist_feed_ids = args.get('playlist_feed_ids', None)
            if playlist_feed_ids: 
                p.playlist_feed_ids = playlist_feed_ids.split(',')
            p.uses_batch_provisioning = bool(int(args.get('uses_batch_provisioning', 
                                                          integration.uses_batch_provisioning)))
 
        result = yield tornado.gen.Task(neondata.BrightcoveIntegration.modify, 
                                     integration_id, 
                                     _update_integration)

        integration = yield IntegrationHelper.get_integration(integration_id,  
                                                  neondata.IntegrationType.BRIGHTCOVE) 
 
        statemon.state.increment('put_brightcove_oks')
        self.success(json.dumps(integration.__dict__))

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
          Required('thumbnail_location') : Any(str, unicode, Length(min=1, 
                                                                    max=2048))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        video_id = args['video_id'] 
        internal_video_id = neondata.InternalVideoID.generate(
            account_id_api_key,video_id)

        video = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                       internal_video_id)

        current_thumbnails = yield tornado.gen.Task(
            neondata.ThumbnailMetadata.get_many, video.thumbnail_ids)
        cdn_key = neondata.CDNHostingMetadataList.create_key(
            account_id_api_key, video.integration_id)
        cdn_metadata = yield tornado.gen.Task(
            neondata.CDNHostingMetadataList.get, cdn_key)
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
            ttype=neondata.ThumbnailType.CUSTOMUPLOAD, 
            rank=cur_rank)
        # upload image to cdn 
        yield video.download_and_add_thumbnail(new_thumbnail,
                                               external_thumbnail_id=args['thumbnail_location'],
                                               cdn_metadata=cdn_metadata,
                                               async=True)
        #save the thumbnail
        yield tornado.gen.Task(new_thumbnail.save)

        # save the video 
        new_video = yield tornado.gen.Task(neondata.VideoMetadata.modify, 
                                           internal_video_id, 
                                           lambda x: x.thumbnail_ids.append(
                                               new_thumbnail.key))

        if new_video: 
            statemon.state.increment('post_thumbnail_oks')
            self.success('{ "message": "thumbnail accepted for processing" }',
                         code=ResponseCode.HTTP_ACCEPTED)  
        else:
            raise SaveError('unable to save thumbnail to video') 

    @tornado.gen.coroutine
    def put(self, account_id): 
        """handles a thumbnail endpoint put request"""
 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('thumbnail_id') : Any(str, unicode, Length(min=1, max=512)),
          'enabled': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
        thumbnail_id = args['thumbnail_id'] 
            
        thumbnail = yield tornado.gen.Task(neondata.ThumbnailMetadata.get, 
                                           thumbnail_id)
        def _update_thumbnail(t):
            t.enabled = bool(int(args.get('enabled', thumbnail.enabled)))

        thumbnail = yield tornado.gen.Task(neondata.ThumbnailMetadata.modify, 
                                           thumbnail_id, 
                                           _update_thumbnail)
 
        statemon.state.increment('put_thumbnail_oks')
        self.success(json.dumps(ThumbnailHelper.db2api(thumbnail)))

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
        self.success(json.dumps(
            ThumbnailHelper.db2api(thumbnail, fields)))

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
class ThumbnailHelper(object):
    """Helper class for dealing with Thumbnail objects."""
    
    @staticmethod
    def db2api(tmeta, fields=None):
        """Converts a database thumbnail metadata object to a thumbnail
        response dictionary
         
        Keyword arguments: 
        tmeta - The ThumbnailMetadata object
        fields - List of fields to return
        """
        if fields is None:
            fields = ['video_id', 'thumbnail_id', 'rank', 'frameno',
                      'neon_score', 'enabled', 'url', 'height', 'width',
                      'type', 'external_ref', 'created', 'updated']
        
        obj = {}
        for field in fields:
            if field == 'video_id':
                obj[field] = neondata.InternalVideoID.to_external(
                    neondata.InternalVideoID.from_thumbnail_id(tmeta.key))
            elif field == 'thumbnail_id':
                obj[field] = tmeta.key
            elif field == 'rank':
                obj[field] = tmeta.rank
            elif field == 'frameno':
                obj[field] = tmeta.frameno
            elif field == 'neon_score':
                obj[field] = tmeta.model_score
            elif field == 'enabled':
                obj[field] = tmeta.enabled
            elif field == 'url':
                obj[field] = tmeta.urls[0] or []
            elif field == 'type':
                obj[field] = tmeta.type
            elif field == 'width':
                obj[field] = tmeta.width
            elif field == 'height':
                obj[field] = tmeta.height
            elif field == 'external_ref':
                obj[field] = tmeta.external_id
            elif field == 'created':
                obj[field] = tmeta.created
            elif field == 'updated':
                obj[field] = tmeta.updated
            else:
                raise BadRequestError('invalid field %s' % field)
                
        return obj

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
        request.video_url = args.get('video_url', None) 
        request.callback_url = args.get('callback_url', None)
        request.video_title = args.get('video_title', None) 
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
                video_url=args.get('video_url', None),
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
            reprocess = bool(int(args.get('reprocess', 0))) 
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
    def db2api(video, request, fields=None):
        """Converts a database video metadata object to a video
        response dictionary
         
        Keyword arguments: 
        video - The VideoMetadata object
        request - The NeonApiRequest object
        fields - List of fields to return
        """
        if fields is None:
            fields = ['state', 'video_id', 'publish_date', 'title', 'url',
                      'testing_enabled']

        new_video = {}
        for field in fields:
            if field == 'thumbnails':
                new_video['thumbnails'] = yield VideoHelper.get_thumbnails_from_ids(video.thumbnail_ids)
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
                new_video['error'] = request.response.get('error', None)
            else:
                new_video['error'] = None

        raise tornado.gen.Return(new_video)

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
            thumbnails = [ThumbnailHelper.db2api(x) for x in thumbnails] 

        raise tornado.gen.Return(thumbnails)
     
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
          'integration_id' : Any(str, unicode, Length(min=1, max=256)),
          'video_url': Any(str, unicode, Length(min=1, max=512)), 
          'callback_url': Any(str, unicode, Length(min=1, max=512)), 
          'video_title': Any(str, unicode, Length(min=1, max=256)),
          'duration': All(Coerce(float), Range(min=0.0, max=86400.0)), 
          'publish_date': All(CustomVoluptuousTypes.Date()), 
          'custom_data': All(CustomVoluptuousTypes.Dictionary()), 
          'default_thumbnail_url': Any(str, unicode, Length(min=1, max=128)),
          'reprocess': All(Coerce(int), Range(min=0, max=1)),
          'thumbnail_ref': Any(str, unicode, Length(min=1, max=512))
        })

        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)
          
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
            job_info['video'] = new_video.__dict__
            statemon.state.increment('post_video_oks')
            self.success(json.dumps(job_info),
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
        new_videos = []
        empty = True 
        index = 0 
        if videos:  
            requests = yield tornado.gen.Task(
                neondata.NeonApiRequest.get_many,
                [(x.job_id if x else '', account_id_api_key) for x in videos])
            for video, request in zip(videos, requests):
                if video is None or request is None:
                    new_videos.append({'error' : 'video does not exist', 
                                       'video_id' : video_ids[index] }) 
                    index += 1
                    continue

                new_video = yield VideoHelper.db2api(video,
                                                     request,
                                                     fields)
                new_videos.append(new_video)
                empty = False 
 
                index += 1

            vid_dict['videos'] = new_videos
            vid_dict['video_count'] = len(new_videos)
            
        if vid_dict['video_count'] is 0 or empty: 
            raise NotFoundError('video(s) do not exist with id(s): %s' % 
                                (args['video_id']))

        statemon.state.increment('get_video_oks')
        self.success(json.dumps(vid_dict))

    @tornado.gen.coroutine
    def put(self, account_id):
        """handles a Video endpoint put request"""
 
        schema = Schema({
          Required('account_id') : Any(str, unicode, Length(min=1, max=256)),
          Required('video_id') : Any(str, unicode, Length(min=1, max=256)),
          'testing_enabled': All(Coerce(int), Range(min=0, max=1))
        })
        args = self.parse_args()
        args['account_id'] = account_id_api_key = str(account_id)
        schema(args)

        abtest = bool(int(args['testing_enabled']))
        internal_video_id = neondata.InternalVideoID.generate(account_id_api_key,args['video_id']) 
        def _update_video(v): 
            v.testing_enabled = abtest
        result = yield tornado.gen.Task(neondata.VideoMetadata.modify, 
                                        internal_video_id, 
                                        _update_video)
        video = yield tornado.gen.Task(neondata.VideoMetadata.get, 
                                       internal_video_id)
        if not video: 
            raise NotFoundError('video does not exist with id: %s' % (args['video_id']))
        
        statemon.state.increment('put_video_oks')
        output = yield VideoHelper.db2api(video, None,
                                          fields=['testing_enabled',
                                                  'video_id'])
        self.success(json.dumps(output))

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
        video_statuses = [VideoStatsHelper.db2api(x) for x in video_statuses]
        stats_dict['statistics'] = video_statuses
        stats_dict['count'] = len(video_statuses)

        self.success(json.dumps(stats_dict))

    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.GET : neondata.AccessLevels.READ, 
                 'account_required'  : [HTTPVerbs.GET] 
               }  

class VideoStatsHelper(object):
    @staticmethod
    def db2api(vstatus, fields=None):
        """Converts a database video status object to a video status
        response dictionary
         
        Keyword arguments: 
        vstatus - The VideoStatus object
        fields - List of fields to return
        """
        if fields is None:
            fields = ['video_id', 'experiment_state', 'winner_thumbnail',
                      'created', 'updated']
        
        obj = {}
        for field in fields:
            if field == 'video_id':
                obj[field] = neondata.InternalVideoID.to_external(
                    vstatus.get_id())
            elif field == 'experiment_state':
                obj[field] = vstatus.experiment_state
            elif field == 'winner_thumbnail':
                obj[field] = vstatus.winner_tid
            elif field == 'created':
                obj[field] = vstatus.created
            elif field == 'updated':
                obj[field] = vstatus.updated
            else:
                raise BadRequestError('invalid field %s' % field)
                
        return obj

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
            raise MultipleInvalid('thumbnail_id or video_id is required') 
        if video_ids and thumbnail_ids: 
            raise MultipleInvalid('you can only have one of thumbnail_id or video_id') 
        
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
        objects = [ThumbnailStatsHelper.db2api(obj, fields)
                   for obj in objects] 
        stats_dict['statistics'] = objects
        stats_dict['count'] = len(objects)

        self.success(json.dumps(stats_dict))

    @classmethod
    def get_access_levels(self):
        return { 
                 HTTPVerbs.GET : neondata.AccessLevels.READ, 
                 'account_required'  : [HTTPVerbs.GET] 
               }  

class ThumbnailStatsHelper(object):
    @staticmethod
    def db2api(tstatus, fields=None):
        """Converts a database thumbnail status object to a thumbnail status
        response dictionary
         
        Keyword arguments: 
        tstatus - The ThubmnailStatus object
        fields - List of fields to return
        """
        if fields is None:
            fields = ['thumbnail_id', 'video_id', 'ctr']
        
        obj = {}
        for field in fields:
            if field == 'video_id':
                obj[field] = neondata.InternalVideoID.from_thumbnail_id(
                    tstatus.get_id())
            elif field == 'thumbnail_id':
                obj[field] = tstatus.get_id()
            elif field == 'serving_frac':
                obj[field] = tstatus.serving_frac
            elif field == 'ctr':
                obj[field] = tstatus.ctr
            elif field == 'impressions':
                obj[field] = tstatus.imp
            elif field == 'conversions':
                obj[field] = tstatus.conv
            elif field == 'created':
                obj[field] = vstatus.created
            elif field == 'updated':
                obj[field] = vstatus.updated
            else:
                raise BadRequestError('invalid field %s' % field)
                
        return obj

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
VideoSearchHandler : class responsible for searching videos
   HTTP Verbs     : get
        Notes     : outside of scope of phase 1, future implementation
*********************************************************************'''
class VideoSearchHandler(tornado.web.RequestHandler): 
    def __init__(self): 
        super(VideoSearchHandler, self).__init__() 

'''*********************************************************************
Endpoints 
*********************************************************************'''
application = tornado.web.Application([
    (r'/healthcheck/?$', HealthCheckHandler),
    (r'/api/v2/accounts/?$', NewAccountHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/ooyala/?$', OoyalaIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/brightcove/?$', BrightcoveIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/integrations/optimizely/?$', OptimizelyIntegrationHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/thumbnails/?$', ThumbnailHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/?$', VideoHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/videos/search?$', VideoSearchHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/?$', AccountHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/stats/videos?$', VideoStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/stats/thumbnails?$', ThumbnailStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/statistics/videos?$', VideoStatsHandler),
    (r'/api/v2/([a-zA-Z0-9]+)/statistics/thumbnails?$', ThumbnailStatsHandler),
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
