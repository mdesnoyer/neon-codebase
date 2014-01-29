#/usr/bin/env python
'''
Data Model classes 

Defines interfaces for Neon User Account, Platform accounts
Account Types
- NeonUser
- BrightcovePlatform
- YoutubePlatform

Api Request Types
- Neon, Brightcove, youtube

'''
import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import binascii
import hashlib
import json
from multiprocessing.pool import ThreadPool
import random
import redis as blockingRedis
import string
from StringIO import StringIO
import tornado.ioloop
import tornado.gen
import tornado.httpclient
import threading
import time
from api import brightcove_api #coz of cyclic import 
import api.youtube_api

from utils.options import define, options
import utils.sync

import logging
_log = logging.getLogger(__name__)

define("accountDB", default="127.0.0.1", type=str, help="")
define("videoDB", default="127.0.0.1", type=str, help="")
define("thumbnailDB", default="127.0.0.1", type=str ,help="")
define("dbPort", default=6379, type=int, help="redis port")
define("watchdogInterval", default=3, type=int, 
        help="interval for watchdog thread")


class DBConnection(object):
    '''Connection to the database.'''

    #Note: Lock for each instance, currently locks for any instance creation
    __singleton_lock = threading.Lock() 
    _singleton_instance = {} 

    def __init__(self, *args, **kwargs):
        otype = args[0]
        cname = None
        if otype:
            if isinstance(otype, basestring):
                cname = otype
            else:
                cname = otype.__class__.__name__ \
                        if otype.__class__.__name__ != "type" else otype.__name__
        
        host = options.accountDB 
        port = options.dbPort 
        
        if cname:
            if cname in ["AbstractPlatform", "BrightcovePlatform", "NeonApiKey"
                    "YoutubePlatform", "NeonUserAccount", "NeonApiRequest"]:
                host = options.accountDB 
                port = options.dbPort 
            elif cname == "VideoMetadata":
                host = options.videoDB
                port = options.dbPort 
            elif cname in ["ThumbnailIDMapper", "ThumbnailURLMapper"]:
                host = options.thumbnailDB 
                port = options.dbPort 
        
        self.conn, self.blocking_conn = RedisClient.get_client(host, port)

    def fetch_keys_from_db(self, key_prefix, callback=None):
        ''' fetch keys that match a prefix '''

        if callback:
            self.conn.keys(key_prefix,callback)
        else:
            keys = self.blocking_conn.keys(key_prefix)
            return keys

    def clear_db(self):
        '''Erases all the keys in the database.

        This should really only be used in test scenarios.
        '''
        self.blocking_conn.flushdb()

    @classmethod
    def update_instance(cls,cname):
        ''' Method to update the connection object in case of 
        db config update '''
        if cls._singleton_instance.has_key(cname):
            with cls.__singleton_lock:
                if cls._singleton_instance.has_key(cname):
                    cls._singleton_instance[cname] = cls(cname)

    def __new__(cls, *args, **kwargs):
        ''' override new '''
        otype = args[0] #Arg pass can either be class name or class instance
        cname = None
        if otype:
            if isinstance(otype,basestring):
                cname = otype
            else:
                #handle the case for classmethod
                cname = otype.__class__.__name__ \
                      if otype.__class__.__name__ != "type" else otype.__name__
        
        if not cls._singleton_instance.has_key(cname):
            with cls.__singleton_lock:
                if not cls._singleton_instance.has_key(cname):
                    cls._singleton_instance[cname] = \
                            object.__new__(cls, *args, **kwargs)
        return cls._singleton_instance[cname]

class RedisAsyncWrapper(object):
    '''
    Replacement class for tornado-redis 
    
    This is a wrapper class which does redis operation
    in a background thread and on completion transfers control
    back to the tornado ioloop. If you wrap this around gen/Task,
    you can write db operations as if they were synchronous.
    
    usage: 
    value = yield tornado.gen.Task(RedisAsyncWrapper().get,key)


    #TODO: see if we can completely wrap redis-py calls, helpful if
    you can get the callback attribuet as well when call is made
    '''

    _thread_pool = ThreadPool(10)
    
    def __init__(self, host='127.0.0.1', port=6379):
        self.client = blockingRedis.StrictRedis(host, port, socket_timeout=10)

    def get(self, key, callback):
        ''' get key '''
        def _callback(result):
            ''' result callback'''
            tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(result))
        RedisAsyncWrapper._thread_pool.apply_async(
                self.client.get, args=(key,), callback=_callback)
   
    def set(self,key,value,callback):
        ''' set key '''
        def _callback(result):
            ''' result callback'''
            tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(result))
        RedisAsyncWrapper._thread_pool.apply_async(
            self.client.set, args=(key, value,), callback=_callback)
    
    def pipeline(self):
        ''' pipeline '''
        return self.client.pipeline()

    def mget(self, keys, callback):
        ''' multi get '''
        def _callback(result):
            ''' result callback'''
            tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(result))
        RedisAsyncWrapper._thread_pool.apply_async(
            self.client.mget, args=(keys,), callback=_callback)
    
    def mset(self, keys, callback):
        ''' multi set '''
        def _callback(result):
            ''' result callback'''
            tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(result))
        RedisAsyncWrapper._thread_pool.apply_async(
            self.client.mset, args=(keys,), callback=_callback)
    
    def keys(self, prefix, callback):
        ''' key regex match'''
        def _callback(result):
            ''' result callback'''
            tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(result))
        RedisAsyncWrapper._thread_pool.apply_async(
            self.client.keys, args=(prefix,), callback=_callback)
    
class DBConnectionCheck(threading.Thread):

    ''' Watchdog thread class to check the DB connection objects '''
    def __init__(self):
        super(DBConnectionCheck, self).__init__()
        self.interval = options.watchdogInterval
        self.daemon = True

    def run(self):
        ''' run loop ''' 
        while True:
            try:
                for key, value in DBConnection._singleton_instance.iteritems():
                    DBConnection.update_instance(key)
                    value.blocking_conn.get("dummy")
            except Exception, e:
                _log.exception("key=DBConnection check msg=%s"%e)
            
            time.sleep(self.interval)

#start watchdog thread for the DB connection
DBCHECK_THREAD = DBConnectionCheck()
DBCHECK_THREAD.start()

def _erase_all_data():
    '''Erases all the data from the redis databases.

    This should only be used for testing purposes.
    '''
    _log.warn('Erasing all the data. I hope this is a test.')
    AbstractPlatform._erase_all_data()
    ThumbnailMetaData._erase_all_data()
    ThumbnailURLMapper._erase_all_data()
    ThumbnailIDMapper._erase_all_data()
    VideoMetadata._erase_all_data()

class RedisClient(object):
    '''
    Static class for REDIS configuration
    '''
    #static variables
    host = '127.0.0.1'
    port = 6379
    client = None
    blocking_client = None

    def __init__(self, host='127.0.0.1', port=6379):
        self.client = RedisAsyncWrapper(host, port)
        self.blocking_client = blockingRedis.StrictRedis(host, port)
    
    @staticmethod
    def get_client(host=None, port=None):
        '''
        return connection objects (blocking and non blocking)
        '''
        if host is None:
            host = RedisClient.host 
        if port is None:
            port = RedisClient.port
        
        RedisClient.c = RedisAsyncWrapper(host, port)
        RedisClient.bc = blockingRedis.StrictRedis(
                            host, port, socket_timeout=10)
        return RedisClient.c, RedisClient.bc 

##############################################################################

def generate_request_key(api_key, job_id):
    ''' Format request key (with job_id) to find NeonApiRequest Object'''
    key = "request_" + api_key + "_" + job_id
    return key
##############################################################################


class AbstractHashGenerator(object):
    ' Abstract Hash Generator '

    @staticmethod
    def _api_hash_function(_input):
        ''' Abstract hash generator '''
        return hashlib.md5(_input).hexdigest()

class NeonApiKey(object):
    ''' Static class to generate Neon API Key'''
    @classmethod
    def id_generator(cls, size=24, 
            chars=string.ascii_lowercase + string.digits):
        random.seed(time.time())
        return ''.join(random.choice(chars) for x in range(size))

    @classmethod
    def format_key(cls, a_id):
        ''' format db key '''
        return cls.__name__.lower() + '_%s' %a_id
        
    @classmethod
    def generate(cls, a_id):
        ''' generate api key hash'''
        api_key = NeonApiKey.id_generator()
        
        #save api key mapping
        db_connection = DBConnection(cls)
        key = NeonApiKey.format_key(a_id)
        if db_connection.blocking_conn.set(key, api_key):
            return api_key

    @classmethod
    def get_api_key(cls, a_id, callback=None):
        ''' get api key from db '''
        db_connection = DBConnection(cls)
        key = NeonApiKey.format_key(a_id)
        if callback:
            db_connection.conn.get(key, callback) 
        else:
            return db_connection.blocking_conn.get(key) 

class InternalVideoID(object):
    ''' Internal Video ID Generator '''
    @staticmethod
    def generate(api_key, vid):
        ''' external platform vid --> internal vid '''
        key = api_key + "_" + vid
        return key

    @staticmethod
    def to_external(internal_vid):
        ''' internal vid -> external platform vid'''
        vid = internal_vid.split('_')[-1]
        return vid

class TrackerAccountID(object):
    ''' Tracker Account ID generation '''
    @staticmethod
    def generate(_input):
        ''' Generate a CRC 32 for Tracker Account ID'''
        return abs(binascii.crc32(_input))

class TrackerAccountIDMapper(object):
    '''
    Maps a given Tracker Account ID to API Key 

    This is needed to keep the tracker id => api_key
    '''
    STAGING = "staging"
    PRODUCTION = "production"

    def __init__(self, tai, account_id, itype):
        self.key = self.__class__.format_key(tai)
        self.value = account_id 
        self.itype = itype

    @classmethod
    def format_key(cls, tai):
        ''' format db key '''
        return cls.__name__.lower() + '_%s'%tai
    
    def to_json(self):
        ''' to json '''
        return json.dumps(self, default=lambda o: o.__dict__)
    
    def save(self, callback=None):
        ''' save trackerIDMapper instance '''
        db_connection = DBConnection(self)
        value = self.to_json()     
        if callback:
            db_connection.conn.set(self.key, value, callback)
        else:
            return db_connection.blocking_conn.set(self.key, value)
    
    @classmethod
    def get_neon_account_id(cls, tai, callback=None):
        '''
        returns tuple of account_id, type(staging/production)
        '''
        def format_tuple(result):
            ''' format result tuple '''
            if result:
                data = json.loads(result)
                return data['value'], data['itype']

        key = cls.format_key(tai)
        db_connection = DBConnection(cls)
       
        if callback:
            db_connection.conn.get(key, lambda x: callback(format_tuple(x)))
        else:
            data = db_connection.blocking_conn.get(key)
            return format_tuple(data)

class NeonUserAccount(object):
    ''' NeonUserAccount

    Every user in the system has a neon account and all other integrations are 
    associated with this account. 

    @videos: video id / jobid map of requests made directly through neon api
    @integrations: all the integrations associated with this acccount

    '''
    def __init__(self, a_id, api_key=None):
        self.account_id = a_id
        self.neon_api_key = NeonApiKey.generate(a_id) if api_key is None \
                            else api_key
        self.key = self.__class__.__name__.lower()  + '_' + self.neon_api_key
        self.tracker_account_id = TrackerAccountID.generate(self.neon_api_key)
        self.staging_tracker_account_id = \
                TrackerAccountID.generate(self.neon_api_key + "staging") 
        self.videos = {} #phase out,should be stored in neon integration
        # a mapping from integration id -> get_ovp() string
        self.integrations = {}

    def add_platform(self, platform):
        '''Adds a platform object to the account.'''
        if len(self.integrations) == 0:
            self.integrations = {}

        self.integrations[platform.integration_id] = platform.get_ovp()

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_platforms(self):
        ''' get all platform accounts for the user '''

        ovp_map = {}
        for plat in [NeonPlatform, BrightcovePlatform, YoutubePlatform]:
            ovp_map[plat.get_ovp()] = plat

        calls = []
        for integration_id, ovp_string in self.integrations.iteritems():
            try:
                plat_type = ovp_map[ovp_string]
                if plat_type == NeonPlatform:
                    calls.append(tornado.gen.Task(plat_type.get_account,
                                                  self.neon_api_key))
                else:
                    calls.append(tornado.gen.Task(plat_type.get_account,
                                                  self.neon_api_key,
                                                  integration_id))
                    
            except KeyError:
                _log.error('key=get_platforms msg=Invalid ovp string: %s' % 
                           ovp_string)

            except Exception as e:
                _log.exception('key=get_platforms msg=Error getting platform '
                               '%s' % e)

        retval = yield calls
        raise tornado.gen.Return(retval)

    @classmethod
    def get_ovp(cls):
        ''' ovp string '''
        return "neon"
    
    def add_video(self, vid, job_id):
        ''' vid,job_id in to vidoes'''

        self.videos[str(vid)] = job_id
    
    def to_json(self):
        ''' to json '''
        return json.dumps(self, default=lambda o: o.__dict__)
    
    def save(self, callback=None):
        ''' save instance'''
        db_connection = DBConnection(self)
        if callback:
            db_connection.conn.set(self.key, self.to_json(), callback)
        else:
            return db_connection.blocking_conn.set(self.key, self.to_json())
    
    def save_platform(self, new_integration, callback=None):
        '''
        Save Neon User account and corresponding platform object
        '''
        
        #temp: changing this to a blocking pipeline call   
        db_connection = DBConnection(self)
        pipe = db_connection.blocking_conn.pipeline()
        pipe.set(self.key, self.to_json())
        pipe.set(new_integration.key, new_integration.to_json()) 
        callback(pipe.execute())

    @classmethod
    def get_account(cls, api_key, callback=None):
        ''' return neon useraccount instance'''
        db_connection = DBConnection(cls)
        key = "neonuseraccount_%s" %api_key
        if callback:
            db_connection.conn.get(key, lambda x: callback(cls.create(x))) 
        else:
            return cls.create(db_connection.blocking_conn.get(key))
    
    @classmethod
    def create(cls, json_data):
        ''' create obj from json data'''
        if not json_data:
            return None
        params = json.loads(json_data)
        a_id = params['account_id']
        api_key = params['neon_api_key']
        na = cls(a_id, api_key)
       
        for key in params:
            na.__dict__[key] = params[key]
        
        return na
   
    @classmethod 
    def get_all_accounts(cls):
        ''' Get all NeonUserAccount instances '''
        nuser_accounts = []
        db_connection = DBConnection(cls)
        accounts = db_connection.blocking_conn.keys(cls.__name__.lower() + "*")
        for accnt in accounts:
            api_key = accnt.split('_')[-1]
            nu = NeonUserAccount.get_account(api_key)
            nuser_accounts.append(nu)
        return nuser_accounts

class AbstractPlatform(object):
    ''' Abstract Platform/ Integration class '''

    def __init__(self, abtest=False):
        self.key = None 
        self.neon_api_key = ''
        self.videos = {} # External video id (Original Platform VID) => Job ID
        self.abtest = abtest # Boolean on wether AB tests can run
        self.integration_id = None # Unique platform ID to 
    
    def generate_key(self,i_id):
        ''' generate db key '''
        return '_'.join([self.__class__.__name__.lower(),
                         self.neon_api_key, i_id])
    
    def to_json(self):
        ''' to json '''
        return json.dumps(self, default=lambda o: o.__dict__) 

    def save(self, callback=None):
        ''' save instance '''
        db_connection = DBConnection(self)
        value = self.to_json()
        if not self.key:
            raise Exception("Key is empty")

        if callback:
            db_connection.conn.set(self.key, value, callback)
        else:
            return db_connection.blocking_conn.set(self.key, value)

    @classmethod
    def get_ovp(cls):
        ''' ovp string '''
        raise NotImplementedError

    @classmethod
    def get_account(cls, api_key, i_id, callback=None):
        '''Returns the platform object for the key.

        Inputs:
          api_key - The api key for the Neon account
          i_id - The integration id for the platform
          callback - If None, done asynchronously
        '''
        key = cls.__name__.lower()  + '_%s_%s' %(api_key, i_id) 
        db_connection = DBConnection(cls)
        if callback:
            db_connection.conn.get(key, lambda x: callback(cls.create(x))) 
        else:
            return cls.create(db_connection.blocking_conn.get(key))

    @classmethod
    def get_all_instances(cls, callback=None):
        '''Returns a list of all the platform instances from the db.'''
        instances = []
        instances.extend(NeonPlatform.get_all_instances())
        instances.extend(BrightcovePlatform.get_all_instances())
        return instances

    @classmethod
    def get_all_platform_data(cls):
        ''' get all platform data '''
        db_connection = DBConnection(cls)
        accounts = db_connection.blocking_conn.keys(cls.__name__.lower() + "*")
        platform_data = []
        for accnt in accounts:
            api_key = accnt.split('_')[-2]
            i_id = accnt.split('_')[-1]
            jdata = db_connection.blocking_conn.get(accnt) 
            if jdata:
                platform_data.append(jdata)
            else:
                _log.debug("key=get_all_platform data"
                            " msg=no data for acc %s i_id %s" %(api_key, i_id))
        
        return platform_data

    @classmethod
    def _erase_all_data(cls):
        ''' erase all data ''' 
        db_connection = DBConnection(cls)
        db_connection.clear_db()

class NeonPlatform(AbstractPlatform):
    '''
    Neon Integration ; stores all info about calls via Neon API
    '''
    def __init__(self, a_id, api_key, abtest=False):
        AbstractPlatform.__init__(self, abtest=abtest)
        self.neon_api_key = api_key 
        self.integration_id = '0'
        self.key = self.__class__.__name__.lower()  + '_%s_%s' \
                %(self.neon_api_key, self.integration_id)
        self.account_id = a_id
        
        #By default integration ID 0 represents 
        #Neon Platform Integration (access via neon api)
   
    def add_video(self, vid, job_id):
        ''' video => job_id '''
        self.videos[str(vid)] = job_id

    def get_videos(self):
        ''' list of video ids '''
        if len(self.videos) > 0:
            return self.videos.keys()

    @classmethod
    def get_ovp(cls):
        ''' ovp string '''
        return "neon"

    @classmethod
    def get_account(cls, api_key, callback=None):
        ''' return NeonPlatform account object '''
        return super(NeonPlatform, cls).get_account(api_key, 0, callback)

    @classmethod
    def create(cls, json_data): 
        ''' create obj'''
        if not json_data:
            return None

        data_dict = json.loads(json_data)
        obj = NeonPlatform("dummy", "dummy")

        #populate the object dictionary
        for key in data_dict.keys():
            obj.__dict__[key] = data_dict[key]
        
        return obj

    @classmethod
    def get_all_instances(cls, callback=None):
        ''' get all instances [NeonPlatform..]''' 
        platforms = NeonPlatform.get_all_platform_data()
        instances = [] 
        for pdata in platforms:
            platform = NeonPlatform.create(pdata)
            instances.append(platform)

        return instances

class BrightcovePlatform(AbstractPlatform):
    ''' Brightcove Platform/ Integration class '''
    
    def __init__(self, a_id, i_id, api_key, p_id=None, rtoken=None, wtoken=None,
                auto_update=False, last_process_date=None, abtest=False):

        ''' On every request, the job id is saved '''
        AbstractPlatform.__init__(self, abtest)
        self.neon_api_key = api_key
        self.key = self.generate_key(i_id)
        self.account_id = a_id
        self.integration_id = i_id
        self.publisher_id = p_id
        self.read_token = rtoken
        self.write_token = wtoken
        self.auto_update = auto_update 
        #The publish date of the last processed video - UTC timestamp 
        self.last_process_date = last_process_date 
        self.linked_youtube_account = False
        self.account_created = time.time() #UTC timestamp of account creation

    @classmethod
    def get_ovp(cls):
        ''' return ovp name'''
        return "brightcove"

    def add_video(self, vid, job_id):
        ''' add video,job_id in to videos '''
        self.videos[str(vid)] = job_id
    
    def get_videos(self):
        ''' return list of video ids'''
        if len(self.videos) > 0:
            return self.videos.keys()
    
    def get(self, callback=None):
        ''' get instance'''
        db_connection = DBConnection(self)
        if callback:
            db_connection.conn.get(self.key, callback)
        else:
            return db_connection.blocking_conn.get(self.key)

    @tornado.gen.engine
    def update_thumbnail(self, i_vid, new_tid, nosave=False, callback=None):
        ''' method to keep video metadata and thumbnail data consistent '''
        bc = api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id,
            self.read_token, self.write_token, self.auto_update)
       
        #Get video metadata
        platform_vid = InternalVideoID.to_external(i_vid)
        vmdata = yield tornado.gen.Task(VideoMetadata.get, i_vid)
        if not vmdata:
            _log.error("key=update_thumbnail msg=vid %s not found" %i_vid)
            callback(None)
            return
        
        #Thumbnail ids for the video
        tids = vmdata.thumbnail_ids
        
        #Aspect ratio of the video 
        fsize = vmdata.get_frame_size()

        #Get all thumbnails
        thumb_mappings = yield tornado.gen.Task(
                ThumbnailIDMapper.get_thumb_mappings, tids)
        t_url = None
       
        #Check if the new tid exists
        for thumb_mapping in thumb_mappings:
            if thumb_mapping.thumbnail_metadata["thumbnail_id"] == new_tid:
                t_url = thumb_mapping.thumbnail_metadata["urls"][0]
        
        if not t_url:
            _log.error("key=update_thumbnail msg=tid %s not found" %new_tid)
            callback(None)
            return
        
        #Update the database with video first
        #Get previous thumbnail and new thumb
        modified_thumbs = [] 
        new_thumb, old_thumb = ThumbnailIDMapper.enable_thumbnail(
                                    thumb_mappings, new_tid)
        modified_thumbs.append(new_thumb)
        if old_thumb is None:
            #old_thumb can be None if there was no neon thumb before
            _log.debug("key=update_thumbnail" 
                    " msg=set thumbnail in DB %s tid %s"%(i_vid, new_tid))
        else:
            modified_thumbs.append(old_thumb)
        
        if new_thumb is not None:
            res = yield tornado.gen.Task(ThumbnailIDMapper.save_all,
                                        modified_thumbs)  
            if not res:
                _log.error("key=update_thumbnail msg=[pre-update]" 
                        " ThumbnailIDMapper save_all failed for %s" %new_tid)
                callback(False)
                return
        else:
            callback(False)
            return

        # Update the new_tid as the thumbnail for the video
        thumb_res = yield tornado.gen.Task(bc.async_enable_thumbnail_from_url,
                                           platform_vid,
                                           t_url,
                                           new_tid,
                                           fsize)
        if thumb_res is None:
            callback(None)
            return

        tref, sref = thumb_res[0], thumb_res[1]
        if not sref:
            _log.error("key=update_thumbnail msg=brightcove error" 
                    " update video still for video %s %s" %(i_vid, new_tid))

        #NOTE: When the call is made from brightcove controller, do not 
        #save the changes in the db, this is just a temp change for A/B testing
        if nosave:
            callback(tref)
            return

        if not tref:
            _log.error("key=update_thumbnail msg=failed to" 
                    " enable thumb %s for %s" %(new_tid, i_vid))
            
            # Thumbnail was not update via the brightcove api, revert the DB changes
            modified_thumbs = []
            
            #get old thumbnail tid to revert to, this was the tid 
            #that was previously live before this request
            old_tid = "no_thumb" if old_thumb is None \
                    else old_thumb.thumbnail_metadata["thumbnail_id"] 
            new_thumb, old_thumb = ThumbnailIDMapper.enable_thumbnail(
                                    thumb_mappings, old_tid)
            modified_thumbs.append(new_thumb)
            if old_thumb: 
                modified_thumbs.append(old_thumb)
            
            if new_thumb is not None:
                res = yield tornado.gen.Task(ThumbnailIDMapper.save_all,
                                             modified_thumbs)  
                if res:
                    callback(False) #return False coz bcove thumb not updated
                    return
                else:
                    _log.error("key=update_thumbnail msg=ThumbnailIDMapper save_all" 
                            "failed for video=%s cur_db_tid=%s cur_bcove_tid=%s," 
                            "DB not reverted" %(i_vid, new_tid, old_tid))
                    
                    #The tid that was passed to the method is reflected in the DB,
                    #but not on Brightcove. the old_tid is the current bcove thumbnail
                    callback(False)
            else:
                #Why was new_thumb None?
                _log.error("key=update_thumbnail msg=enable_thumbnail"
                        "new_thumb data missing") 
                callback(False)
        else:
            #Success      
            #Updaate the request state to Active to facilitate faster filtering
            req_data = NeonApiRequest.get_request(self.neon_api_key, vmdata.job_id) 
            vid_request = NeonApiRequest.create(req_data)
            vid_request.state = RequestState.ACTIVE
            ret = vid_request.save()
            if not ret:
                _log.error("key=update_thumbnail msg=%s state not updated to active"
                        %vid_request.key)
            callback(True)

    def create_job(self, vid, callback):
        ''' Create neon job for particular video '''
        def created_job(result):
            if not result.error:
                try:
                    job_id = tornado.escape.json_decode(result.body)["job_id"]
                    self.add_video(vid, job_id)
                    self.save(callback)
                except Exception,e:
                    #_log.exception("key=create_job msg=" + e.message) 
                    callback(False)
            else:
                callback(False)

        bc = api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id, self.read_token,
            self.write_token, self.auto_update)
        bc.create_video_request(vid, self.integration_id, created_job)

    def check_feed_and_create_api_requests(self):
        ''' Use this only after you retreive the object from DB '''

        bc = api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id,
            self.read_token, self.write_token, self.auto_update,
            self.last_process_date,account_created=self.account_created)
        bc.create_neon_api_requests(self.integration_id)    
        bc.create_requests_unscheduled_videos(self.integration_id)

    def check_feed_and_create_request_by_tag(self):
        ''' Temp method to support backward compatibility '''

        bc = api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id, self.read_token,
            self.write_token, self.auto_update, self.last_process_date)
        bc.create_brightcove_request_by_tag(self.integration_id)

    def check_current_thumbnail_in_db(self, i_vid, callback=None):
        '''
        Check if the current thumbnail for the given video on brightcove
        has been recorded in Neon DB. Returns True if it has
        '''
        p_vid = InternalVideoID.to_external(i_vid)
        bc = api.brightcove_api.BrightcoveApi(self.neon_api_key,
                                              self.publisher_id,
                                              self.read_token,
                                              self.write_token,
                                              self.auto_update,
                                              self.last_process_date)
        if callback:
            bc.async_check_thumbnail(p_vid, callback)
        else:
            return bc.check_thumbnail(p_vid) #TODO: Impl

    def verify_token_and_create_requests_for_video(self, n, callback=None):
        ''' Method to verify brightcove token on account creation 
            And create requests for processing
            @return: Callback returns job id, along with brightcove vid metadata
        '''

        bc = api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id, self.read_token,
            self.write_token, False, self.last_process_date)
        if callback:
            bc.async_verify_token_and_create_requests(self.integration_id,
                                                      n,
                                                      callback)
        else:
            return bc.verify_token_and_create_requests(self.integration_id,n)

    def sync_individual_video_metadata(self):
        ''' sync video metadata from bcove individually using 
        find_video_id api '''
        bcove_api = api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id, self.read_token,
            self.write_token, self.auto_update, self.last_process_date)
        bcove_api.sync_individual_video_metadata(self.integration_id)

    @classmethod
    def create(cls, json_data):
        ''' create object from json data '''

        if not json_data:
            return None

        params = json.loads(json_data)
        a_id = params['account_id']
        i_id = params['integration_id'] 
        p_id = params['publisher_id']
        rtoken = params['read_token']
        wtoken = params['write_token']
        auto_update = params['auto_update']
        api_key = params['neon_api_key']
         
        ba = BrightcovePlatform(a_id, i_id, api_key, p_id, rtoken, 
                wtoken, auto_update)
        ba.videos = params['videos']
        ba.last_process_date = params['last_process_date'] 
        ba.linked_youtube_account = params['linked_youtube_account']
        
        #backward compatibility
        if params.has_key('abtest'):
            ba.abtest = params['abtest'] 
      
        if not params.has_key('account_created'):
            ba.account_created = None
        
        #populate rest of keys
        for key in params:
            ba.__dict__[key] = params[key]
        return ba

    @staticmethod
    def find_all_videos(token, limit, callback=None):
        ''' find all brightcove videos '''

        # Get the names and IDs of recently published videos:
        url = 'http://api.brightcove.com/services/library?\
                command=find_all_videos&sort_by=publish_date&token=' + token
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url=url, method="GET", 
                request_timeout=60.0, connect_timeout=10.0)
        http_client.fetch(req, callback)

    @classmethod
    def get_all_instances(cls, callback=None):
        ''' get all brightcove instances'''

        platforms = BrightcovePlatform.get_all_platform_data()
        instances = [] 
        for pdata in platforms:
            platform = BrightcovePlatform.create(pdata)
            if platform:
                instances.append(platform)
        return instances

class YoutubePlatform(AbstractPlatform):
    ''' Youtube platform integration '''

    def __init__(self, a_id, i_id, api_key, access_token=None, refresh_token=None,
                expires=None, auto_update=False, abtest=False):
        AbstractPlatform.__init__(self)
        
        self.key = self.__class__.__name__.lower()  + '_%s_%s' \
                %(api_key, i_id) #TODO: fix
        self.account_id = a_id
        self.integration_id = i_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires = expires
        self.generation_time = None
        self.valid_until = 0  

        #if blob is being created save the time when access token was generated
        if access_token:
            self.valid_until = time.time() + float(expires) - 50
        self.auto_update = auto_update
    
        self.channels = None

    @classmethod
    def get_ovp(cls):
        ''' ovp '''
        return "youtube"
    
    def add_video(self, vid, job_id):
        ''' add video, job_id '''
        self.videos[str(vid)] = job_id
    
    def get_access_token(self, callback):
        ''' Get a valid access token, if not valid -- get new one and set expiry'''
        def access_callback(result):
            if result:
                self.access_token = result
                self.valid_until = time.time() + 3550
                callback(self.access_token)
            else:
                callback(False)

        #If access token has expired
        if time.time() > self.valid_until:
            yt = api.youtube_api.YoutubeApi(self.refresh_token)
            yt.get_access_token(access_callback)
        else:
            #return current token
            callback(self.access_token)
   
    def add_channels(self, callback):
        '''
        Add a list of channels that the user has
        Get a valid access token first
        '''
        def save_channel(result):
            if result:
                self.channels = result
                callback(True)
            else:
                callback(False)

        def atoken_exec(atoken):
            if atoken:
                yt = api.youtube_api.YoutubeApi(self.refresh_token)
                yt.get_channels(atoken, save_channel)
            else:
                callback(False)

        self.get_access_token(atoken_exec)


    def get_videos(self, callback, channel_id=None):
        '''
        get list of videos from youtube
        '''

        def atoken_exec(atoken):
            if atoken:
                yt = api.youtube_api.YoutubeApi(self.refresh_token)
                yt.get_videos(atoken, playlist_id, callback)
            else:
                callback(False)

        if channel_id is None:
            playlist_id = self.channels[0]["contentDetails"]["relatedPlaylists"]["uploads"] 
            self.get_access_token(atoken_exec)
        else:
            # Not yet supported
            callback(None)


    def update_thumbnail(self, vid, thumb_url, callback):
        '''
        Update thumbnail for the given video
        '''

        def atoken_exec(atoken):
            if atoken:
                yt = api.youtube_api.YoutubeApi(self.refresh_token)
                yt.async_set_youtube_thumbnail(vid, thumb_url, atoken, callback)
            else:
                callback(False)
        self.get_access_token(atoken_exec)


    def create_job(self):
        '''
        Create youtube api request
        '''
        pass
    
    @classmethod
    def create(cls, json_data):
        if json_data is None:
            return None
        
        params = json.loads(json_data)
        a_id = params['account_id']
        i_id = params['integration_id'] 
        api_key = params['neon_api_key'] 
        yt = YoutubePlatform(a_id, i_id, api_key=api_key)
       
        for key in params:
            yt.__dict__[key] = params[key]

        return yt
    
    @classmethod
    def get_all_instances(cls, callback=None):
        platforms = YoutubePlatform.get_all_platform_data()
        instances = [] 
        for pdata in platforms:
            platform = YoutubePlatform.create(pdata)
            instances.append(platform)

        return instances

#######################
# Request Blobs 
######################

class RequestState(object):
    'Request state enumeration'

    SUBMIT     = "submit"
    PROCESSING = "processing"
    REQUEUED   = "requeued"
    FAILED     = "failed"
    FINISHED   = "finished"
    INT_ERROR  = "internal_error"
    ACTIVE     = "active" #thumbnail live 

class NeonApiRequest(object):
    '''
    Instance of this gets created during request creation
    (Neon web account, RSS Cron)
    Json representation of the class is saved in the server queue and redis  
    
    Saving request blobs : 
    create instance of the request object and call save()

    Getting request blobs :
    use static get method to get a json based response NeonApiRequest.get_request()
    '''

    def __init__(self, job_id, api_key, vid, title, url, 
            request_type, http_callback):
        self.key = generate_request_key(api_key, job_id) 
        self.job_id = job_id
        self.api_key = api_key 
        self.video_id = vid
        self.video_title = title
        self.video_url = url
        self.request_type = request_type
        self.callback_url = http_callback
        self.state = "submit" # submit / processing / success / fail 
        self.integration_type = "neon"

        #Save the request response
        self.response = {}  

        #API Method
        self.api_method = None
        self.api_param  = None
        self.publish_date = None

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__) 

    def add_response(self, frames, timecodes=None, urls=None, error=None):
        ''' add response to the api request '''

        self.response['frames'] = frames
        self.response['timecodes'] = timecodes 
        self.response['urls'] = urls 
        self.response['error'] = error
  
    def set_api_method(self, method, param):
        ''' 'set api method and params ''' 
        
        self.api_method = method
        self.api_param  = param

        #TODO:validate supported methods

    def save(self, callback=None):
        ''' save instance '''
        db_connection = DBConnection(self)
        value = self.to_json()
        if self.key is None:
            raise Exception("key not set")
        if callback:
            db_connection.conn.set(self.key, value, callback)
        else:
            return db_connection.blocking_conn.set(self.key, value)

    @classmethod
    def get(cls, api_key, job_id, callback=None):
        ''' get instance '''
        db_connection = DBConnection(cls)
        def package(result):
            if result:
                nar = NeonApiRequest.create(result)
                callback(nar)
            else:
                callback(None)

        key = generate_request_key(api_key,job_id)
        if callback:
            db_connection.conn.get(key,callback)
        else:
            result = db_connection.blocking_conn.get(key)
            if result:
                return NeonApiRequest.create(result)

    @classmethod
    def get_request(cls, api_key, job_id, callback=None):
        ''' get request data '''
        db_connection=DBConnection(cls)
        key = generate_request_key(api_key, job_id)
        if callback:
            db_connection.conn.get(key,callback)
        else:
            return db_connection.blocking_conn.get(key)

    @classmethod
    def get_requests(cls, keys, callback=None):
        ''' mget results '''
        db_connection = DBConnection(cls)
        def create(jdata):
            if not jdata:
                return 
            data_dict = json.loads(jdata)
            #create basic object
            obj = NeonApiRequest("dummy", "dummy", None, None, None, None, None) 
            for key in data_dict.keys():
                obj.__dict__[key] = data_dict[key]
            return obj
       
        def get_results(results):
            response = [create(result) for result in results]
            callback(response)

        if callback:
            db_connection.conn.mget(keys, get_results)
        else:
            results = db_connection.blocking_conn.mget(keys)
            response = [create(result) for result in results]
            return response 

    @staticmethod
    def create(json_data):
        ''' create object '''
        data_dict = json.loads(json_data)

        #create basic object
        obj = NeonApiRequest("dummy", "dummy", None, None, None, None, None) 

        #populate the object dictionary
        for key in data_dict.keys():
            obj.__dict__[key] = data_dict[key]

        return obj

class BrightcoveApiRequest(NeonApiRequest):
    '''
    Brightcove API Request class
    '''
    def __init__(self, job_id, api_key, vid, title, url, rtoken, wtoken, pid,
                callback=None, i_id=None):
        self.read_token = rtoken
        self.write_token = wtoken
        self.publisher_id = pid
        self.integration_id = i_id 
        self.previous_thumbnail = None
        self.autosync = False
        request_type = "brightcove"
        super(BrightcoveApiRequest,self).__init__(job_id, api_key, vid, title, url,
                request_type, callback)

class YoutubeApiRequest(NeonApiRequest):
    '''
    Youtube API Request class
    '''
    def __init__(self, job_id, api_key, vid, title, url, access_token, refresh_token,
            expiry, callback=None):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.integration_type = "youtube"
        self.previous_thumbnail = None
        self.expiry = expiry
        request_type = "youtube"
        super(YoutubeApiRequest,self).__init__(job_id, api_key, vid, title, url,
                request_type, callback)

###############################################################################
## Thumbnail store T_URL => TID => Metadata
###############################################################################

class ThumbnailType(object):
    ''' Thumbnail type enumeration '''
    NEON        = "neon"
    CENTERFRAME = "centerframe"
    BRIGHTCOVE  = "brightcove"
    RANDOM      = "random"
    FILTERED    = "filtered"

class ThumbnailMetaData(object):

    '''
    Schema for storing thumbnail metadata
    A single thumbnail id maps to all its urls 
    [Neon, OVP name space ones, other associated ones] 
    '''
    def __init__(self, tid, urls, created, width, height, ttype, model_score,
                 model_version, enabled=True, chosen=False, rank=None, 
                 refid=None):
        self.thumbnail_id = tid
        self.urls = urls  # All urls associated with single image
        self.created_time = created #Timestamp when thumbnail was created 
        self.enabled = enabled #boolen, indicates if this thumbnail can be displayed/ tested with 
        self.chosen  = chosen #boolean, indicates this thumbnail is live
        self.width = width
        self.height = height
        self.type = ttype #neon1../ brightcove / youtube
        self.rank = 0 if not rank else rank  #int 
        self.model_score = model_score #float
        self.model_version = model_version #float
        self.refid = refid #If referenceID exists *in case of a brightcove thumbnail

    def to_dict(self):
        ''' to dict '''
        return self.__dict__

    @staticmethod
    def create(params_dict):
        ''' create object '''
        obj = ThumbnailMetaData(0 ,0 ,0 ,0 ,0 ,0 ,0 ,0)
        for key in params_dict:
            obj.__dict__[key] = params_dict[key]
        return obj 
    
    @classmethod
    def _erase_all_data(cls):
        db_connection = DBConnection(cls)
        db_connection.clear_db()

class ThumbnailID(AbstractHashGenerator):
    '''
    Static class to generate thumbnail id

    _input: String or Image stream. 

    Thumbnail ID is: <internal_video_id>_<md5 MD5 hash of image data>
    '''

    @staticmethod
    def generate(_input, internal_video_id):
        return '%s_%s' % (internal_video_id, ThumbnailMD5.generate(_input))

class ThumbnailMD5(AbstractHashGenerator):
    '''Static class to generate the thumbnail md5.

    _input: String or Image stream.
    '''
    salt = 'Thumbn@il'
    
    @staticmethod
    def generate_from_string(_input):
        ''' generate hash from string '''
        _input = ThumbnailMD5.salt + str(_input)
        return AbstractHashGenerator._api_hash_function(_input)

    @staticmethod
    def generate_from_image(imstream):
        ''' generate hash from image '''

        filestream = StringIO()
        imstream.save(filestream,'jpeg')
        filestream.seek(0)
        return ThumbnailMD5.generate_from_string(filestream.buf)

    @staticmethod
    def generate(_input):
        ''' generate hash method ''' 
        if isinstance(_input, basestring):
            return ThumbnailMD5.generate_from_string(_input)
        else:
            return ThumbnailMD5.generate_from_image(_input)


class ThumbnailURLMapper(object):
    '''
    Schema to map thumbnail url to thumbnail ID. 

    _input - thumbnail url ( key ) , tid - string/image, converted to thumbnail ID
            if imdata given, then generate tid 
    
    THUMBNAIL_URL => (tid)
    '''
    
    def __init__(self, thumbnail_url, tid, imdata=None):
        self.key = thumbnail_url
        if not imdata:
            self.value = tid
        else:
            #TODO: Is this imdata really needed ? 
            raise #self.value = ThumbnailID.generate(imdata) 

    def save(self, callback=None):
        ''' 
        save url mapping 
        ''' 
        db_connection = DBConnection(self)
        if self.key is None:
            raise Exception("key not set")
        if callback:
            db_connection.conn.set(self.key, self.value, callback)
        else:
            return db_connection.blocking_conn.set(self.key, value)

    @classmethod
    def save_all(cls, thumbnailMapperList, callback=None):
        ''' multi save '''

        db_connection = DBConnection(cls)
        data = {}
        for t in thumbnailMapperList:
            data[t.key] = t.value 

        if callback:
            db_connection.conn.mset(data, callback)
        else:
            return db_connection.blocking_conn.mset(data)

    @classmethod
    def get_id(cls, key, callback=None):
        ''' get thumbnail id '''
        db_connection = DBConnection(cls)
        if callback:
            db_connection.conn.get(key, callback)
        else:
            return db_connection.blocking_conn.get(key)

    @classmethod
    def _erase_all_data(cls):
        ''' del all data'''
        db_connection = DBConnection(cls)
        db_connection.clear_db()

class ImageMD5Mapper(object):
    '''
    Maps a given Image MD5 to Thumbnail ID

    This is needed to keep the mapping of individual image md5's to tid
    A single image can exist is different sizes, for example brightcove has 
    videostills and thumbnails for any given video

    '''
    def __init__(self, ext_video_id, imgdata,tid):
        self.key = self.format_key(ext_video_id, imgdata)
        self.value = tid

    def get_md5(self):
        ''' return md5 '''
        return self.key.split('_')[-1]

    def format_key(self,video_id,imdata):
        ''' format key for ImageMD5Mapper '''
        if imdata:
            md5 = ThumbnailID.generate(imdata, video_id)
            return self.__class__.__name__.lower() + '_' + md5
        else:
            raise

    def save(self,callback=None):
        ''' save ''' 
        db_connection = DBConnection(self)
        
        if callback:
            db_connection.conn.set(self.key, self.value, callback)
        else:
            db_connection.blocking_conn.set(self.key, self.value)

    @classmethod   
    def get_tid(cls, ext_video_id, image_md5, callback=None):
        ''' get tid for the image md5 '''
        db_connection = DBConnection(cls)
        
        key = "ImageMD5Mapper".lower() + '_%s_%s' %(ext_video_id, image_md5)
        if callback:
            db_connection.conn.get(key, callback)
        else:
            return db_connection.blocking_conn.get(key)
    
    @classmethod
    def save_all(cls,objs,callback=None):
        ''' multi save ''' 
        db_connection = DBConnection(cls)
        data = {}
        for obj in objs:
            data[obj.key] = obj.value

        if callback:
            db_connection.conn.mset(data,callback)
        else:
            return db_connection.blocking_conn.mset(data)


class ThumbnailIDMapper(object):
    '''
    Class schema for Thumbnail URL to thumbnail metadata map
    Thumbnail ID  => (Internal Video ID, ThumbnailMetadata) 
    
    Primary source for all the data associated with the thumbnail
    contains the dictionary of thumbnail_metadata
    '''
    def __init__(self, tid, internal_vid, thumbnail_metadata):
        super(ThumbnailIDMapper,self).__init__()
        self.key = tid
        self.video_id = internal_vid #api_key + platform video id
        self.thumbnail_metadata = thumbnail_metadata #dict of ThumbnailMetadata obj

    @classmethod
    def generate_key(cls, video_id, tid):
        ''' generate IDMapper key '''
        return video_id + '_' + tid 

    def get_account_id(self):
        ''' get account id '''
        return self.video_id.split('_')[0]

    def _hash(self,_input):
        return hashlib.md5(_input).hexdigest()
    
    def get_metadata(self):
        ''' get thumbnail metadata '''
        return self.thumbnail_metadata
        #return only specific fields

    def to_dict(self):
        ''' to dict '''
        return self.__dict__
    
    def to_json(self):
        ''' to json '''
        return json.dumps(self, default=lambda o: o.__dict__) 

    @staticmethod
    def create(json_data):
        ''' create object '''

        data_dict = json.loads(json_data)
        #create basic object
        obj = ThumbnailIDMapper(None, None, None)

        #populate the object dictionary
        for key in data_dict.keys():
            obj.__dict__[key] = data_dict[key]
        
        return obj

    @classmethod
    def get_video_id(cls, tid, callback=None):
        '''Given a thumbnail id, retrieves the video id 
            asscociated with tid'''

        def get_metadata(result):
            ''' extract thumbnail metadata from obj '''
            vid = None
            if result:
                obj = ThumbnailIDMapper.create(result)
                callback(obj.video_id)
            callback(vid)

        db_connection = DBConnection(cls)
        if callback:
            db_connection.conn.get(tid, get_metadata)
        else:
            result = db_connection.blocking_conn.get(tid)
            if result:
                obj = ThumbnailIDMapper.create(result)
                return obj.video_id

    @classmethod
    def get_thumb_metadata(cls, id, callback=None):
        '''Given a thumbnail id, retrieves the thumbnail metadata.

        Inputs:
        id - The thumbnail id 

        Returns:
        ThumbnailMetadata object.
        '''
        def get_metadata(result):
            ''' extract thumbnail metadata from obj '''
            tmdata = None
            if result:
                obj = ThumbnailIDMapper.create(result)
                tmdata = ThumbnailMetaData.create(obj.thumbnail_metadata)
            callback(tmdata)

        db_connection = DBConnection(cls)
        if callback:
            db_connection.conn.get(id, get_metadata)
        else:
            result = db_connection.blocking_conn.get(id)
            if result:
                obj = ThumbnailIDMapper.create(result)
                return ThumbnailMetaData.create(obj.thumbnail_metadata)


    @classmethod
    def get_thumb_mappings(cls,keys,callback=None):
        ''' Returns list of thumbnail mappings for give thumb ids(keys)
        '''
        db_connection = DBConnection(cls)

        def process(results):
            mappings = [] 
            for item in results:
                obj = ThumbnailIDMapper.create(item)
                mappings.append(obj)
            callback(mappings)

        if callback:
            db_connection.conn.mget(keys,process)
        else:
            mappings = [] 
            items = db_connection.blocking_conn.mget(keys)
            for item in items:
                obj = ThumbnailIDMapper.create(item)
                mappings.append(obj)
            return mappings

    @classmethod
    def save_all(cls, thumbnailMapperList, callback=None):
        ''' multi save '''
        db_connection = DBConnection(cls)
        data = {}
        for t in thumbnailMapperList:
            data[t.key] = t.to_json()

        if callback:
            db_connection.conn.mset(data, callback)
        else:
            return db_connection.blocking_conn.mset(data)

    @staticmethod
    def enable_thumbnail(mapper_objs, new_tid):
        ''' enable thumb in a list of mapper obj given a new thumb id '''
        new_thumb_obj = None; old_thumb_obj = None
        for mapper_obj in mapper_objs:
            #set new tid as chosen
            if mapper_obj.thumbnail_metadata["thumbnail_id"] == new_tid: 
                mapper_obj.thumbnail_metadata["chosen"] = True
                new_thumb_obj = mapper_obj 
            else:
                #set chosen=False for old tid
                if mapper_obj.thumbnail_metadata["chosen"] == True:
                    mapper_obj.thumbnail_metadata["chosen"] = False 
                    old_thumb_obj = mapper_obj 

        #return only the modified thumbnail objs
        return new_thumb_obj,old_thumb_obj 

    @classmethod
    def save_integration(cls, mapper_objs, callback=None):
        ''' save integration '''
        db_connection = DBConnection(cls)
        if callback:
            pipe = db_connection.conn.pipeline()
        else:
            pipe = db_connection.blocking_conn.pipeline() 

        for mapper_obj in mapper_objs:
            pipe.set(mapper_obj.key, mapper_obj.to_json())
        
        if callback:
            pipe.execute(callback)
        else:
            return pipe.execute()

    @classmethod
    def _erase_all_data(cls):
        ''' clear db '''
        db_connection = DBConnection(cls)
        db_connection.clear_db()

class VideoMetadata(object):
    '''
    Schema for metadata associated with video which gets stored
    when the video is processed

    Contains list of Thumbnail IDs associated with the video
    '''

    '''  Keyed by API_KEY + VID '''
    
    def __init__(self, video_id, tids, request_id, video_url, duration,
                 vid_valence, model_version, i_id, frame_size=None):

        self.key = video_id #internal video id 
        self.thumbnail_ids = tids 
        self.url = video_url 
        self.duration = duration
        self.video_valence = vid_valence 
        self.model_version = model_version
        self.job_id = request_id
        self.integration_id = i_id
        self.frame_size = frame_size #(w,h)

    def get_id(self):
        ''' get video id '''
        return self.key

    def get_frame_size(self):
        ''' framesize of the video '''
        #if self.frame_size:
        #    return float(self.frame_size[0])/self.frame_size[1]
        if self.__dict__.has_key('frame_size'):
            return self.frame_size

    def to_json(self):
        ''' to json'''
        return json.dumps(self, default=lambda o: o.__dict__) 

    def save(self, callback=None):
        ''' save  '''
        db_connection = DBConnection(self)
        value = self.to_json()
        if callback:
            db_connection.conn.set(self.key, value, callback)
        else:
            return db_connection.blocking_conn.set(self.key, value)

    @classmethod
    def get(cls, internal_video_id, callback=None):
        ''' get video metadata '''
        db_connection = DBConnection(cls)

        def create(jdata):
            ''' create obj'''
            data_dict = json.loads(jdata) 
            obj = VideoMetadata(None, None, None, None, None, None, None, None)
            for key in data_dict.keys():
                obj.__dict__[key] = data_dict[key]
            return obj
        
        def cb(result):
            if result:
                obj = create(result)
                callback(obj)
            else:
                callback(None)

        if callback:
            db_connection.conn.get(internal_video_id, cb)
        else:
            jdata = db_connection.blocking_conn.get(internal_video_id)
            if jdata is None:
                return None
            return create(jdata)

    @classmethod
    def multi_get(cls, internal_video_ids, callback=None):
        ''' multi get '''
        db_connection = DBConnection(cls) 
        def create(jdata):
            data_dict = json.loads(jdata)
            obj = VideoMetadata(None, None, None, None, None, None, None, None)
            for key in data_dict.keys():
                obj.__dict__[key] = data_dict[key]
            return obj

        def cb(results):
            ''' result callback '''
            if len(results) > 0:
                vmdata = []
                for result in results:
                    if result:
                        vm = create(result)
                    else:
                        vm = None
                    vmdata.append(vm)
                callback(vmdata)
            else:
                callback(None)

        if callback:
            db_connection.conn.mget(internal_video_ids, cb) 
        else:
            results = db_connection.blocking_conn.mget(internal_video_ids) 
            vmdata  = []
            for result in results:
                vm = None
                if result:
                    vm = create(result)
                vmdata.append(vm)
            return vmdata

    @staticmethod
    def get_video_metadata(internal_accnt_id, internal_video_id):
        ''' get video metadata '''
        jdata = NeonApiRequest.get_request(internal_accnt_id, internal_video_id)
        nreq = NeonApiRequest.create(jdata)
        return nreq

    @classmethod
    def _erase_all_data(cls):
        ''' clear db '''
        db_connection = DBConnection(cls)
        db_connection.clear_db()

