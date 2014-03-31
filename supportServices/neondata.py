#!/usr/bin/env python
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

import base64
import binascii
import contextlib
import copy
import hashlib
import json
from multiprocessing.pool import ThreadPool
import random
import redis as blockingRedis
import string
from StringIO import StringIO
from supportServices.url2thumbnail import URL2ThumbnailIndex
import tornado.ioloop
import tornado.gen
import tornado.httpclient
import threading
import time
import api.brightcove_api #coz of cyclic import 
import api.youtube_api
from api import ooyala_api

from utils.options import define, options
import utils.sync
import urllib

import logging
_log = logging.getLogger(__name__)

define("accountDB", default="127.0.0.1", type=str, help="")
define("videoDB", default="127.0.0.1", type=str, help="")
define("thumbnailDB", default="127.0.0.1", type=str ,help="")
define("dbPort", default=6379, type=int, help="redis port")
define("watchdogInterval", default=3, type=int, 
        help="interval for watchdog thread")

#constants 
BCOVE_STILL_WIDTH = 480

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
                    "YoutubePlatform", "NeonUserAccount", "OoyalaPlatform", "NeonApiRequest"]:
                host = options.accountDB 
                port = options.dbPort 
            elif cname == "VideoMetadata":
                host = options.videoDB
                port = options.dbPort 
            elif cname in ["ThumbnailMetadata", "ThumbnailURLMapper"]:
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
            if isinstance(otype, basestring):
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

    @staticmethod
    def _get_wrapped_async_func(func):
        '''Returns an asynchronous function wrapped around the given func.

        The asynchronous call has a callback keyword added to it
        '''
        def AsyncWrapper(*args, **kwargs):
            # Find the callback argument
            try:
                callback = kwargs['callback']
                del kwargs['callback']
            except KeyError:
                if len(args) > 0 and hasattr(args[-1], '__call__'):
                    callback = args[-1]
                    args = args[:-1]
                else:
                    raise AttributeError('A callback is necessary')
                    
            io_loop = tornado.ioloop.IOLoop.current()
            def _cb(result):
                io_loop.add_callback(lambda: callback(result))

            RedisAsyncWrapper._thread_pool.apply_async(
                func, args=args,
                kwds=kwargs, callback=_cb)
        return AsyncWrapper
        

    def __getattr__(self, attr):
        '''Allows us to wrap all of the redis-py functions.'''
        if hasattr(self.client, attr):
            if hasattr(getattr(self.client, attr), '__call__'):
                return RedisAsyncWrapper._get_wrapped_async_func(
                    getattr(self.client, attr))
                
        raise AttributeError(attr)
    
    def pipeline(self):
        ''' pipeline '''
        #TODO(Sunil) make this asynchronous
        return self.client.pipeline()
    
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
            except RuntimeError, e:
                #ignore if dict size changes while iterating
                #a new class just created its own dbconn object
                pass
            except Exception, e:
                _log.exception("key=DBConnection check msg=%s"%e)
            
            time.sleep(self.interval)

#start watchdog thread for the DB connection
#Disable for now, some issue with connection pool, throws reconnection
#error, I think its due to each object having too many stored connections
#DBCHECK_THREAD = DBConnectionCheck()
#DBCHECK_THREAD.start()

def _erase_all_data():
    '''Erases all the data from the redis databases.

    This should only be used for testing purposes.
    '''
    _log.warn('Erasing all the data. I hope this is a test.')
    AbstractPlatform._erase_all_data()
    ThumbnailMetadata._erase_all_data()
    ThumbnailURLMapper._erase_all_data()
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

def id_generator(size=32, 
            chars=string.ascii_lowercase + string.digits):
    ''' Generate a random alpha numeric string to be used as 
        unique ids
    '''

    random.seed(time.time())
    return ''.join(random.choice(chars) for x in range(size))

##############################################################################

class StoredObject(object):
    '''Abstract class to represent an object that is stored in the database.

    This contains common routines for interacting with the data.
    TODO: Convert all the objects to use this consistent interface.
    '''
    def __init__(self, key):
        self.key = key

    def to_json(self):
        '''Returns a json version of the object'''
        return json.dumps(self, default=lambda o: o.__dict__)

    def save(self, callback=None):
        '''Save the object to the database.'''
        db_connection = DBConnection(self)
        value = self.to_json()
        if self.key is None:
            raise Exception("key not set")
        if callback:
            db_connection.conn.set(self.key, value, callback)
        else:
            return db_connection.blocking_conn.set(self.key, value)

    @classmethod
    def get(cls, key, callback=None):
        '''Retrieve this object from the database.

        Returns the object or None if it couldn't be found
        '''
        db_connection = DBConnection(cls)

        def cb(result):
            if result:
                obj = cls._create(key, result)
                callback(obj)
            else:
                callback(None)

        if callback:
            db_connection.conn.get(key, cb)
        else:
            jdata = db_connection.blocking_conn.get(key)
            if jdata is None:
                return None
            return cls._create(key, jdata)

    @classmethod
    def get_many(cls, keys, callback=None):
        ''' Get many objects of the same type simultaneously

        This is more efficient than one at a time.

        Inputs:
        keys - List of keys to get
        callback - Optional callback function to call

        Returns:
        A list of cls objects or None if it wasn't there, one for each key
        '''
        db_connection = DBConnection(cls)

        def process(results):
            mappings = [] 
            for key, item in zip(keys, results):
                obj = cls._create(key, item)
                mappings.append(obj)
            callback(mappings)

        if callback:
            db_connection.conn.mget(keys, process)
        else:
            mappings = [] 
            items = db_connection.blocking_conn.mget(keys)
            for key, item in zip(keys, items):
                obj = cls._create(key, item)
                mappings.append(obj)
            return mappings

    
    @classmethod
    def modify(cls, key, func, callback=None):
        '''Allows you to modify the object in the database atomically.

        While in func, you have a lock on the object so you are
        guaranteed for it not to change. It is automatically saved at
        the end of func.
        
        Inputs:
        func - Function that takes a single parameter (the object being edited)
        key - The key of the object to modify

        Returns: A copy of the updated object or None, if the object wasn't
                 in the database and thus couldn't be updated.

        Example usage:
        SoredObject.modify('thumb_a', lambda thumb: thumb.update_phash())
        '''
        def _getandset(pipe):
            json_data = pipe.get(key)
            pipe.multi()

            if json_data is None:
                _log.error('Could not get redis object: %s' % key)
                return None
            
            obj = cls._create(key, json_data)
            try:
                func(obj)
            finally:
                pipe.set(key, obj.to_json())
            return obj

        db_connection = DBConnection(cls)
        if callback:
            return db_connection.conn.transaction(_getandset, key,
                                                  callback=callback,
                                                  value_from_callable=True)
        else:
            return db_connection.blocking_conn.transaction(
                _getandset, key, value_from_callable=True)
            
    @classmethod
    def save_all(cls, objects, callback=None):
        '''Save many objects simultaneously'''
        db_connection = DBConnection(cls)
        data = {}
        for obj in objects:
            data[obj.key] = obj.to_json()

        if callback:
            db_connection.conn.mset(data, callback)
        else:
            return db_connection.blocking_conn.mset(data)

    @classmethod
    def _create(cls, key, json_data):
        '''Create an object from the json_data.

        Returns None if the object could not be created.
        '''
        if json_data:
            data_dict = json.loads(json_data)
            #create basic object
            obj = cls(key)

            #populate the object dictionary
            for key, value in data_dict.iteritems():
                obj.__dict__[key] = value
        
            return obj

    @classmethod
    def _erase_all_data(cls):
        '''Clear the database that contains objects of this type '''
        db_connection = DBConnection(cls)
        db_connection.clear_db()

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
        key = '%s_%s' % (api_key, vid)
        return key

    @staticmethod
    def to_external(internal_vid):
        ''' internal vid -> external platform vid'''
        
        #first part of the key doesn't have _, hence use this below to 
        #generate the internal vid. 
        #note: found later that Ooyala can have _ in their video ids

        vid = "_".join(internal_vid.split('_')[1:])

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
        ''' vid,job_id in to videos'''
        
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
    
    def generate_key(self, i_id):
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

    def add_video(self, vid, job_id):
        ''' external video id => job_id '''
        self.videos[str(vid)] = job_id

    def get_videos(self):
        ''' list of external video ids '''
        if len(self.videos) > 0:
            return self.videos.keys()
    
    def get_internal_video_ids(self):
        ''' return list of internal video ids for the account ''' 
        i_vids = [] 
        for vid in self.videos.keys(): 
            i_vids.append(InternalVideoID.generate(self.neon_api_key, vid))
        return i_vids

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
        instances.extend(OoyalaPlatform.get_all_instances())
        return instances

    @classmethod
    def _get_all_instances_impl(cls, callback=None):
        '''Implements get_all_instances for a single platform type.'''
        platforms = cls.get_all_platform_data()
        instances = [] 
        for pdata in platforms:
            platform = cls.create(pdata)
            if platform:
                instances.append(platform)

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
        ''' get all brightcove instances'''
        return cls._get_all_instances_impl()

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
        self.rendition_frame_width = None #Resolution of video to process
        self.video_still_width = 480 #default brightcove still width

    @classmethod
    def get_ovp(cls):
        ''' return ovp name'''
        return "brightcove"

    def get(self, callback=None):
        ''' get json'''
        db_connection = DBConnection(self)
        if callback:
            db_connection.conn.get(self.key, callback)
        else:
            return db_connection.blocking_conn.get(self.key)

    def get_api(self):
        '''Return the Brightcove API object for this platform integration.'''
        return api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id,
            self.read_token, self.write_token, self.auto_update,
            self.last_process_date, account_created=self.account_created)

    @tornado.gen.engine
    def update_thumbnail(self, i_vid, new_tid, nosave=False, callback=None):
        ''' method to keep video metadata and thumbnail data consistent 
        callback(None): bad request
        callback(False): internal error
        callback(True): success
        '''
        bc = self.get_api()
      
        #update the default still size, if set
        if self.video_still_width != BCOVE_STILL_WIDTH:
            bc.update_still_width(self.video_still_width) 

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
        thumbnails = yield tornado.gen.Task(
                ThumbnailMetadata.get_many, tids)
        t_url = None
        
        # Get the type of thumbnail (Neon/ Brighcove)
        thumb_type = "" #type_rank

        #Check if the new tid exists
        for thumbnail in thumbnails:
            if thumbnail.key == new_tid:
                t_url = thumbnail.urls[0]
                thumb_type = "bc" if thumbnail.type == "brightcove" else ""
        
        if not t_url:
            _log.error("key=update_thumbnail msg=tid %s not found" %new_tid)
            callback(None)
            return
        
        #Update the database with video first
        #Get previous thumbnail and new thumb
        modified_thumbs = [] 
        new_thumb, old_thumb = ThumbnailMetadata.enable_thumbnail(
            thumbnails, new_tid)
        modified_thumbs.append(new_thumb)
        if old_thumb is None:
            #old_thumb can be None if there was no neon thumb before
            _log.debug("key=update_thumbnail" 
                    " msg=set thumbnail in DB %s tid %s"%(i_vid, new_tid))
        else:
            modified_thumbs.append(old_thumb)
      
        #Don't reflect change in the DB, used by AB Controller methods
        if nosave == False:
            if new_thumb is not None:
                res = yield tornado.gen.Task(ThumbnailMetadata.save_all,
                                             modified_thumbs)  
                if not res:
                    _log.error("key=update_thumbnail msg=[pre-update]" 
                            " ThumbnailMetadata save_all failed for %s" %new_tid)
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
                                           fsize,
                                           image_suffix=thumb_type)
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
                    else old_thumb.key
            new_thumb, old_thumb = ThumbnailMetadata.enable_thumbnail(
                                    thumbnails, old_tid)
            modified_thumbs.append(new_thumb)
            if old_thumb: 
                modified_thumbs.append(old_thumb)
            
            if new_thumb is not None:
                res = yield tornado.gen.Task(ThumbnailMetadata.save_all,
                                             modified_thumbs)  
                if res:
                    callback(False) #return False coz bcove thumb not updated
                    return
                else:
                    _log.error("key=update_thumbnail msg=ThumbnailMetadata save_all" 
                            "failed for video=%s cur_db_tid=%s cur_bcove_tid=%s," 
                            "DB not reverted" %(i_vid, new_tid, old_tid))
                    
                    #The tid that was passed to the method is reflected in the DB,
                    #but not on Brightcove.the old_tid is the current bcove thumbnail
                    callback(False)
            else:
                #Why was new_thumb None?
                _log.error("key=update_thumbnail msg=enable_thumbnail"
                        "new_thumb data missing") 
                callback(False)
        else:
            #Success      
            #Update the request state to Active to facilitate faster filtering
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
                
        self.get_api().create_video_request(vid, self.integration_id,
                                            created_job)

    def check_feed_and_create_api_requests(self):
        ''' Use this only after you retreive the object from DB '''

        bc = self.get_api()
        bc.create_neon_api_requests(self.integration_id)    
        bc.create_requests_unscheduled_videos(self.integration_id)

    def check_feed_and_create_request_by_tag(self):
        ''' Temp method to support backward compatibility '''
        self.get_api().create_brightcove_request_by_tag(self.integration_id)


    def verify_token_and_create_requests_for_video(self, n, callback=None):
        ''' Method to verify brightcove token on account creation 
            And create requests for processing
            @return: Callback returns job id, along with brightcove vid metadata
        '''

        bc = self.get_api()
        if callback:
            bc.async_verify_token_and_create_requests(self.integration_id,
                                                      n,
                                                      callback)
        else:
            return bc.verify_token_and_create_requests(self.integration_id,n)

    def sync_individual_video_metadata(self):
        ''' sync video metadata from bcove individually using 
        find_video_id api '''
        self.get_api().bcove_api.sync_individual_video_metadata(
            self.integration_id)

    def set_rendition_frame_width(self, f_width):
        ''' Set framewidth of the video resolution to process '''
        self.rendition_frame_width = f_width

    def set_video_still_width(self, width):
        ''' Set framewidth of the video still to be used 
            when the still is updated in the brightcove account '''
        self.video_still_width = width

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
        return cls._get_all_instances_impl()

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
        ''' get all brightcove instances'''
        return cls._get_all_instances_impl()

class OoyalaPlatform(AbstractPlatform):
    '''
    OOYALA Platform
    '''
    def __init__(self, a_id, i_id, api_key, p_code, 
                 o_api_key, api_secret, auto_update=False): 
        '''
        Init ooyala platform 
        
        Partner code, o_api_key & api_secret are essential 
        for api calls to ooyala 

        '''
        AbstractPlatform.__init__(self)
        self.neon_api_key = api_key
        self.key = self.generate_key(i_id)
        self.account_id = a_id
        self.integration_id = i_id
        self.partner_code = p_code
        self.ooyala_api_key = o_api_key
        self.api_secret = api_secret 
        self.auto_update = auto_update 
    
    @classmethod
    def get_ovp(cls):
        ''' return ovp name'''
        return "ooyala"
    
    @classmethod
    def generate_signature(cls, secret_key, http_method, 
                    request_path, query_params, request_body=''):
        ''' Generate signature for ooyala requests'''
        signature = secret_key + http_method.upper() + request_path
        for key, value in query_params.iteritems():
            signature += key + '=' + value
            signature = base64.b64encode(hashlib.sha256(signature).digest())[0:43]
            signature = urllib.quote_plus(signature)
            return signature

    def check_feed_and_create_requests(self):
        '''
        #check feed and create requests
        '''
        oo = ooyala_api.OoyalaAPI(self.ooyala_api_key, self.api_secret)
        oo.process_publisher_feed(copy.deepcopy(self)) 

    #verify token and create requests on signup
    def create_video_requests_on_signup(self, n, callback=None):
        ''' Method to verify ooyala token on account creation 
            And create requests for processing
            @return: Callback returns job id, along with ooyala vid metadata
        '''
        oo = ooyala_api.OoyalaAPI(self.ooyala_api_key, self.api_secret)
        oo._create_video_requests_on_signup(copy.deepcopy(self), n, callback) 

    @tornado.gen.engine
    def update_thumbnail(self, i_vid, new_tid, callback=None):
        '''
        Update the Preview image on Ooyala video 
        
        callback(None): bad request/ Gateway error
        callback(False): internal error
        callback(True): success

        '''
        #Get video metadata
        platform_vid = InternalVideoID.to_external(i_vid)
        
        vmdata = yield tornado.gen.Task(VideoMetadata.get, i_vid)
        if not vmdata:
            _log.error("key=ooyala update_thumbnail msg=vid %s not found" %i_vid)
            callback(None)
            return
        
        #Thumbnail ids for the video
        tids = vmdata.thumbnail_ids
        
        #Aspect ratio of the video 
        fsize = vmdata.get_frame_size()

        #Get all thumbnails
        thumbnails = yield tornado.gen.Task(
                ThumbnailMetadata.get_many, tids)
        t_url = None
        
        #Check if the new tid exists
        for thumb in thumbnails:
            if thumb.key == new_tid:
                t_url = thumb.urls[0]
        
        if not t_url:
            _log.error("key=update_thumbnail msg=tid %s not found" %new_tid)
            callback(None)
            return
        
        # Update the new_tid as the thumbnail for the video
        oo = ooyala_api.OoyalaAPI(self.ooyala_api_key, self.api_secret)
        update_result = yield tornado.gen.Task(oo.update_thumbnail_from_url,
                                           platform_vid,
                                           t_url,
                                           new_tid,
                                           fsize)
        #check if thumbnail was updated 
        if not update_result:
            callback(None)
            return
      
        #Update the database with video
        #Get previous thumbnail and new thumb
        modified_thumbs = [] 
        new_thumb, old_thumb = ThumbnailMetadata.enable_thumbnail(
                                    thumbnails, new_tid)
        modified_thumbs.append(new_thumb)
        if old_thumb is None:
            #old_thumb can be None if there was no neon thumb before
            _log.debug("key=update_thumbnail" 
                    " msg=set thumbnail in DB %s tid %s"%(i_vid, new_tid))
        else:
            modified_thumbs.append(old_thumb)
       
        #Verify that new_thumb data is not empty 
        if new_thumb is not None:
            res = yield tornado.gen.Task(ThumbnailMetadata.save_all,
                                            modified_thumbs)  
            if not res:
                _log.error("key=update_thumbnail msg=ThumbnailMetadata save_all"
                                " failed for %s" %new_tid)
                callback(False)
                return
        else:
            _log.error("key=oo_update_thumbnail msg=new_thumb is None %s"%new_tid)
            callback(False)
            return

        req_data = NeonApiRequest.get_request(self.neon_api_key, vmdata.job_id) 
        vid_request = NeonApiRequest.create(req_data)
        vid_request.state = RequestState.ACTIVE
        ret = vid_request.save()
        if not ret:
            _log.error("key=update_thumbnail msg=%s state not updated to active"
                        %vid_request.key)
        callback(True)
    
    @classmethod
    def create(cls, json_data):
        if json_data is None:
            return None
        
        params = json.loads(json_data)
        a_id = params['account_id']
        i_id = params['integration_id'] 
        api_key = params['neon_api_key'] 
        oo= OoyalaPlatform(a_id, i_id, api_key, None, None, None)
        for key in params:
            oo.__dict__[key] = params[key]
        return oo
    
    @classmethod
    def get_all_instances(cls, callback=None):
        ''' get all ooyala instances'''

        platforms = OoyalaPlatform.get_all_platform_data()
        instances = [] 
        for pdata in platforms:
            platform = OoyalaPlatform.create(pdata)
            if platform:
                instances.append(platform)
        return instances

    @classmethod
    def get_all_instances(cls, callback=None):
        ''' get all brightcove instances'''
        return cls._get_all_instances_impl()

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

class OoyalaApiRequest(NeonApiRequest):
    '''
    Ooyala API Request class
    '''
    def __init__(self, job_id, api_key, i_id, vid, title, url, 
                        oo_api_key, oo_secret_key,
                        p_thumb, http_callback):
        self.oo_api_key = oo_api_key
        self.oo_secret_key = oo_secret_key
        self.integration_id = i_id 
        self.previous_thumbnail = p_thumb 
        self.autosync = False
        request_type = "ooyala"
        super(OoyalaApiRequest, self).__init__(job_id, api_key, vid, title, url,
                request_type, http_callback)

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
    OOYALA      = "ooyala"
    RANDOM      = "random"
    FILTERED    = "filtered"

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
            return db_connection.blocking_conn.set(self.key, self.value)

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


class ThumbnailMetadata(StoredObject):
    '''
    Class schema for Thumbnail information.

    Keyed by thumbnail id
    '''
    def __init__(self, tid, internal_vid, urls=None, created=None,
                 width=None, height=None, ttype=None,
                 model_score=None, model_version=None, enabled=True,
                 chosen=False, rank=None, refid=None, phash=None):
        super(ThumbnailMetadata,self).__init__(tid)
        self.video_id = internal_vid #api_key + platform video id
        self.urls = urls  # List of all urls associated with single image
        self.created_time = created # Timestamp when thumbnail was created 
        self.enabled = enabled #boolen, indicates if this thumbnail can be displayed/ tested with 
        self.chosen = chosen #boolean, indicates this thumbnail is chosen by the user as the primary one
        self.width = width
        self.height = height
        self.type = ttype #neon1../ brightcove / youtube
        self.rank = 0 if not rank else rank  #int 
        self.model_score = model_score #float
        self.model_version = model_version #string
        #TODO: remove refid. It's not necessary
        self.refid = refid #If referenceID exists *in case of a brightcove thumbnail
        self.phash = phash # Perceptual hash of the image. None if unknown

    def update_phash(self, image):
        '''Update the phash from a PIL image.'''
        self.phash = URL2ThumbnailIndex().hash_index.hash_pil_image(image)

    def get_account_id(self):
        ''' get the internal account id. aka api key '''
        return self.video_id.split('_')[0]
    
    def get_metadata(self):
        ''' get a dictionary of the thumbnail metadata

        This function is deprecated and is kept only for backwards compatibility
        '''
        return self.to_dict()

    def to_dict(self):
        ''' to dict '''
        return self.__dict__
    
    def to_dict_for_video_response(self):
        ''' to dict for video response object
            replace key to thumbnail_id 
        '''
        new_dict = self.__dict__
        new_dict["thumbnail_id"] = new_dict.pop("key")
        return new_dict  

    @classmethod
    def _create(cls, key, json_data):
        ''' create object '''

        if json_data:
            data_dict = json.loads(json_data)
            #create basic object
            obj = ThumbnailMetadata(key, None, None, None, None, None, None,
                                    None, None)

            # For backwards compatibility, check to see if there is a
            # json entry for thumbnail_metadata. If so, grab all
            # entries from there.
            if 'thumbnail_metadata' in data_dict:
                for key, value in data_dict['thumbnail_metadata'].items():
                    if key != 'thumbnail_id':
                        obj.__dict__[key] = value
                del data_dict['thumbnail_metadata']

            #populate the object dictionary
            for key, value in data_dict.iteritems():
                obj.__dict__[key] = value
        
            return obj

    @classmethod
    def get_video_id(cls, tid, callback=None):
        '''Given a thumbnail id, retrieves the internal video id 
            asscociated with thumbnail
        '''

        if callback:
            def handle_obj(obj):
                if obj:
                    callback(obj.video_id)
                else:
                    callback(None)
            cls.get(tid, callback=handle_obj)
        else:
            obj = cls.get(tid)
            if obj:
                return obj.video_id
            else:
                return None

    @staticmethod
    def enable_thumbnail(thumbnails, new_tid):
        ''' enable thumb in a list of thumbnails given a new thumb id '''
        new_thumb_obj = None; old_thumb_obj = None
        for thumb in thumbnails:
            #set new tid as chosen
            if thumb.key == new_tid: 
                thumb.chosen = True
                new_thumb_obj = thumb 
            else:
                #set chosen=False for old tid
                if thumb.chosen == True:
                    thumb.chosen = False 
                    old_thumb_obj = thumb 

        #return only the modified thumbnail objs
        return new_thumb_obj, old_thumb_obj 

    @classmethod
    def save_integration(cls, thumbnails, callback=None):
        ''' save integration 

        TODO(sunil): Explain what this is for
        '''
        db_connection = DBConnection(cls)
        if callback:
            pipe = db_connection.conn.pipeline()
        else:
            pipe = db_connection.blocking_conn.pipeline() 

        for thumbnail in thumbnails:
            pipe.set(thumbnail.key, thumbnail.to_json())
        
        if callback:
            pipe.execute(callback)
        else:
            return pipe.execute()

    @classmethod
    def iterate_all_thumbnails(cls):
        '''Iterates through all of the thumbnails in the system.

        ***WARNING*** This function is a best effort iteration. There
           is a good chance that the database changes while the
           iteration occurs. Given that we only ever add thumbnails to
           the system, this means that it is likely that some
           thumbnails will be missing.

        Returns - A generator that does the iteration and produces 
                  ThumbnailMetadata objects.
        '''

        for platform in AbstractPlatform.get_all_instances():
            for video_id in platform.get_internal_video_ids():
                video_metadata = VideoMetadata.get(video_id)
                if video_metadata is None:
                    _log.error('Could not find information about video %s' %
                               video_id)
                    continue

                for thumb in ThumbnailMetadata.get_many(
                        video_metadata.thumbnail_ids):
                    yield thumb

class VideoMetadata(StoredObject):
    '''
    Schema for metadata associated with video which gets stored
    when the video is processed

    Contains list of Thumbnail IDs associated with the video
    '''

    '''  Keyed by API_KEY + VID (internal video id) '''
    
    def __init__(self, video_id, tids=None, request_id=None, video_url=None,
                 duration=None, vid_valence=None, model_version=None,
                 i_id=None, frame_size=None):
        super(VideoMetadata, self).__init__(video_id) 
        self.thumbnail_ids = tids 
        self.url = video_url 
        self.duration = duration
        self.video_valence = vid_valence 
        self.model_version = model_version
        self.job_id = request_id
        self.integration_id = i_id
        self.frame_size = frame_size #(w,h)

    def get_id(self):
        ''' get internal video id '''
        return self.key

    def get_account_id(self):
        ''' get the internal account id. aka api key '''
        return self.key.split('_')[0]

    def get_frame_size(self):
        ''' framesize of the video '''
        if self.__dict__.has_key('frame_size'):
            return self.frame_size

    @classmethod
    def get_video_request(cls, internal_video_id, callback=None):
        ''' get video request data '''
        if not callback:
            vm = cls.get(internal_video_id)
            api_key = vm.key.split('_')[0]
            jdata = NeonApiRequest.get_request(api_key, vm.job_id)
            nreq = NeonApiRequest.create(jdata)
            return nreq
        else:
            raise AttributeError("Callbacks not allowed")

class InMemoryCache(object):

    '''
    Class to keep data in memory cache to avoid
    fetching the key from redis db every time

    Every timeout period the cache data is refetched
    from the DB

    NOTE: Use this only for read only data
    Currently no timeout for each key

    '''
    def __init__(self, classname, timeout=3):
        self.classname = classname
        self.timeout = timeout
        self.data = {} # key => object of classname
        self._thread_pool = ThreadPool(1)
        self._thread_pool.apply_async(
            self.update_thread, callback=self._callback)
        self.rlock = threading.RLock()

    def add_key(self, key):
        '''
        Add a key to the cache
        '''
        with self.rlock:
            db_connection = DBConnection(self.classname)
            value = db_connection.blocking_conn.get(key)
            cls = eval(self.classname)
            if cls:
                try:
                    f_create = getattr(cls, "create")
                    self.data[key] = f_create(value)
                    return True
                except AttributeError, e:
                    return 

    def get_key(self, key):
        '''
        Retrieve key from the cache
        '''
        if self.data.has_key(key):
            return self.data[key] 

    def update_thread(self):
        '''
        Update the value of each key
        '''
        while True:
            for key in self.data.keys():
                self.add_key(key)
            time.sleep(self.timeout)

    def _callback(self):
        '''
        Dummy callback
        '''
        print "callback done"
