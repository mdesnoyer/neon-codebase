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

This module can also be called as a script, in which case you get an
interactive console to talk to the database with.

'''
import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import base64
import binascii
import code
import concurrent.futures
import contextlib
import copy
import datetime
import hashlib
import json
import random
import redis as blockingRedis
import string
import supportServices.url2thumbnail
import tornado.ioloop
import tornado.gen
import tornado.httpclient
import threading
import time
import api.brightcove_api #coz of cyclic import 
import api.youtube_api
import utils.logs
import utils.neon
import utils.sync
import utils.s3
import utils.http 
import urllib

from api.cdnhosting import CDNHosting
from api.cdnhosting import upload_to_s3_host_thumbnails 
from api import ooyala_api
from PIL import Image
from StringIO import StringIO
from utils.options import define, options

import logging
_log = logging.getLogger(__name__)

define("thumbnailBucket", default="host-thumbnails", type=str,
        help="S3 bucket to Host thumbnails ")

define("accountDB", default="127.0.0.1", type=str, help="")
define("videoDB", default="127.0.0.1", type=str, help="")
define("thumbnailDB", default="127.0.0.1", type=str ,help="")
define("dbPort", default=6379, type=int, help="redis port")
define("watchdogInterval", default=3, type=int, 
        help="interval for watchdog thread")
define("maxRedisRetries", default=5, type=int,
       help="Maximum number of retries when sending a request to redis")
define("baseRedisRetryWait", default=0.1, type=float,
       help="On the first retry of a redis command, how long to wait in seconds")
define("video_server", default="127.0.0.1", type=str, help="Neon video server")
define('async_pool_size', type=int, default=10,
       help='Number of processes that can talk simultaneously to the db')

#constants 
BCOVE_STILL_WIDTH = 480

class DBConnection(object):
    '''Connection to the database.

    There is one connection for each object type, so to get the
    connection, please use the get() function and don't create it
    directly.
    '''

    #Note: Lock for each instance, currently locks for any instance creation
    __singleton_lock = threading.Lock() 
    _singleton_instance = {} 

    def __init__(self, class_name):
        '''Init function.

        DO NOT CALL THIS DIRECTLY. Use the get() function instead
        '''
        host = options.accountDB 
        port = options.dbPort 

        if class_name:
            if class_name in ["AbstractPlatform", "BrightcovePlatform", "NeonApiKey"
                    "YoutubePlatform", "NeonUserAccount", "OoyalaPlatform", "NeonApiRequest"]:
                host = options.accountDB 
                port = options.dbPort 
            elif class_name == "VideoMetadata":
                host = options.videoDB
                port = options.dbPort 
            elif class_name in ["ThumbnailMetadata", "ThumbnailURLMapper"]:
                host = options.thumbnailDB 
                port = options.dbPort 

        self.conn, self.blocking_conn = RedisClient.get_client(host, port)

    def fetch_keys_from_db(self, key_prefix, callback=None):
        ''' fetch keys that match a prefix '''

        if callback:
            self.conn.keys(key_prefix, callback)
        else:
            keys = self.blocking_conn.keys(key_prefix)
            return keys

    def clear_db(self):
        '''Erases all the keys in the database.

        This should really only be used in test scenarios.
        '''
        self.blocking_conn.flushdb()

    @classmethod
    def update_instance(cls, cname):
        ''' Method to update the connection object in case of 
        db config update '''
        if cls._singleton_instance.has_key(cname):
            with cls.__singleton_lock:
                if cls._singleton_instance.has_key(cname):
                    cls._singleton_instance[cname] = cls(cname)

    @classmethod
    def get(cls, otype=None):
        '''Gets a DB connection for a given object type.

        otype - The object type to get the connection for.
                Can be a class object, an instance object or the class name 
                as a string.
        '''
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
                      DBConnection(cname)
        return cls._singleton_instance[cname]

    @classmethod
    def clear_singleton_instance(cls):
        '''
        Clear the singleton instance for each of the classes

        NOTE: To be only used by the test code
        '''
        cls._singleton_instance = {}

class RedisRetryWrapper(object):
    '''Wraps a redis client so that it retries with exponential backoff.

    You use this class exactly the same way that you would use the
    StrctRedis class. 

    Calls on this object are blocking.

    '''

    def __init__(self, *args, **kwargs):
        self.client = blockingRedis.StrictRedis(*args, **kwargs)
        self.max_tries = options.maxRedisRetries
        self.base_wait = options.baseRedisRetryWait

    def _get_wrapped_retry_func(self, func):
        '''Returns an blocking retry function wrapped around the given func.
        '''
        def RetryWrapper(*args, **kwargs):
            cur_try = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    _log.error('Error talking to redis on attempt %i: %s' % 
                               (cur_try, e))
                    cur_try += 1
                    if cur_try == self.max_tries:
                        raise

                    # Do an exponential backoff
                    delay = (1 << cur_try) * self.base_wait # in seconds
                    time.sleep(delay)
        return RetryWrapper

    def __getattr__(self, attr):
        '''Allows us to wrap all of the redis-py functions.'''
        
        if hasattr(self.client, attr):
            if hasattr(getattr(self.client, attr), '__call__'):
                return self._get_wrapped_retry_func(
                    getattr(self.client, attr))
                
        raise AttributeError(attr)

class RedisAsyncWrapper(object):
    '''
    Replacement class for tornado-redis 
    
    This is a wrapper class which does redis operation
    in a background thread and on completion transfers control
    back to the tornado ioloop. If you wrap this around gen/Task,
    you can write db operations as if they were synchronous.
    
    usage: 
    value = yield tornado.gen.Task(RedisAsyncWrapper().get, key)


    #TODO: see if we can completely wrap redis-py calls, helpful if
    you can get the callback attribue as well when call is made
    '''

    _thread_pool = concurrent.futures.ThreadPoolExecutor(
        options.async_pool_size)
    
    def __init__(self, host='127.0.0.1', port=6379):
        self.client = blockingRedis.StrictRedis(host, port, socket_timeout=10)
        self.max_tries = options.maxRedisRetries
        self.base_wait = options.baseRedisRetryWait

    def _get_wrapped_async_func(self, func):
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
            
            def _cb(future, cur_try=0):
                if future.exception() is None:
                    callback(future.result())
                else:
                    _log.error('Error talking to redis on attempt %i: %s' % 
                               (cur_try, future.exception()))
                    cur_try += 1
                    if cur_try == self.max_tries:
                        raise future.exception()

                    delay = (1 << cur_try) * self.base_wait # in seconds
                    io_loop.add_timeout(
                        time.time() + delay,
                        lambda: io_loop.add_future(
                            RedisAsyncWrapper._thread_pool.submit(
                                func, *args, **kwargs),
                            lambda x: _cb(x, cur_try)))

            future = RedisAsyncWrapper._thread_pool.submit(
                func, *args, **kwargs)
            io_loop.add_future(future, _cb)
        return AsyncWrapper
        

    def __getattr__(self, attr):
        '''Allows us to wrap all of the redis-py functions.'''
        if hasattr(self.client, attr):
            if hasattr(getattr(self.client, attr), '__call__'):
                return self._get_wrapped_async_func(
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
        self.blocking_client = RedisRetryWrapper(host, port)
    
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
        RedisClient.bc = RedisRetryWrapper(
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
## Enum types
############################################################################## 

class ThumbnailType(object):
    ''' Thumbnail type enumeration '''
    NEON        = "neon"
    CENTERFRAME = "centerframe"
    BRIGHTCOVE  = "brightcove"
    OOYALA      = "ooyala"
    RANDOM      = "random"
    FILTERED    = "filtered"
    DEFAULT     = "default" #sent via api request
    CUSTOMUPLOAD = "customupload" #uploaded by the customer/editor

class ExperimentState:
    '''A class that acts like an enum for the state of the experiment.'''
    UNKNOWN = 'unknown'
    RUNNING = 'running'
    COMPLETE = 'complete'
    DISABLED = 'disabled'
    OVERRIDE = 'override' # Experiment has be manually overridden

class MetricType:
    '''The different kinds of metrics that we care about.'''
    LOADS = 'loads'
    VIEWS = 'views'
    CLICKS = 'clicks'
    PLAYS = 'plays'

##############################################################################
class StoredObject(object):
    '''Abstract class to represent an object that is stored in the database.

    This contains common routines for interacting with the data.
    TODO: Convert all the objects to use this consistent interface.
    '''
    def __init__(self, key):
        self.key = str(key)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)

    def __cmp__(self, other):
        return cmp(self.__dict__, other.__dict__)

    def to_json(self):
        '''Returns a json version of the object'''
        return json.dumps(self, default=lambda o: o.__dict__)

    def save(self, callback=None):
        '''Save the object to the database.'''
        db_connection = DBConnection.get(self)
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
        db_connection = DBConnection.get(cls)

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
        #MGET raises an exception for wrong number of args if keys = []
        if len(keys) == 0:
            if callback:
                callback([])
            else:
                return []

        db_connection = DBConnection.get(cls)

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
        def _process_one(d):
            val = d[key]
            if val is not None:
                func(val)

        if callback:
            return StoredObject.modify_many(
                [key], _process_one,
                callback=lambda d: callback(d[key]))
        else:
            updated_d = StoredObject.modify_many([key], _process_one)
            return updated_d[key]

    @classmethod
    def modify_many(cls, keys, func, callback=None):
        '''Allows you to modify objects in the database atomically.

        While in func, you have a lock on the objects so you are
        guaranteed for them not to change. The objects are
        automatically saved at the end of func.
        
        Inputs:
        func - Function that takes a single parameter (dictionary of key -> object being edited)
        keys - List of keys of the objects to modify

        Returns: A dictionary of {key -> updated object}. The updated
        object could be None if it wasn't in the database and thus
        couldn't be modified

        Example usage:
        SoredObject.modify_many(['thumb_a'], 
          lambda d: thumb.update_phash() for thumb in d.iteritems())
        '''
        def _getandset(pipe):
            # mget can't handle an empty list 
            if len(keys) == 0:
                return {}

            items = pipe.mget(keys)
            pipe.multi()

            mappings = {}
            for key, item in zip(keys, items):
                if item is None:
                    _log.error('Could not get redis object: %s' % key)
                    mappings[key] = None
                else:
                    mappings[key] = cls._create(key, item)
            try:
                func(mappings)
            finally:
                to_set = {}
                for key, obj in mappings.iteritems():
                    if obj is not None:
                        to_set[key] = obj.to_json()

                if len(to_set) > 0:
                    pipe.mset(to_set)
            return mappings

        db_connection = DBConnection.get(cls)
        if callback:
            return db_connection.conn.transaction(_getandset, *keys,
                                                  callback=callback,
                                                  value_from_callable=True)
        else:
            return db_connection.blocking_conn.transaction(
                _getandset, *keys, value_from_callable=True)
            
    @classmethod
    def save_all(cls, objects, callback=None):
        '''Save many objects simultaneously'''
        db_connection = DBConnection.get(cls)
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
                obj.__dict__[str(key)] = value
        
            return obj

    @classmethod
    def _erase_all_data(cls):
        '''Clear the database that contains objects of this type '''
        db_connection = DBConnection.get(cls)
        db_connection.clear_db()

class NamespacedStoredObject(StoredObject):
    '''An abstract StoredObject that is namespaced by its classname.'''
    def __init__(self, key):
        super(NamespacedStoredObject, self).__init__(
            self.__class__.format_key(key))

    @classmethod
    def format_key(cls, key):
        ''' Format the database key with a class specific prefix '''
        return cls.__name__.lower() + '_%s' % key

    @classmethod
    def get(cls, key, callback=None):
        '''Return the object for a given key.'''
        return super(NamespacedStoredObject, cls).get(
            cls.format_key(key),
            callback=callback)

    @classmethod
    def get_many(cls, keys, callback=None):
        '''Returns the list of objects from a list of keys.'''
        return super(NamespacedStoredObject, cls).get_many(
            [cls.format_key(x) for x in keys],
            callback=callback)

    @classmethod
    def get_all(cls, callback=None):
        ''' Get all the objects in the database of this type

        Inputs:
        callback - Optional callback function to call

        Returns:
        A list of cls objects.
        '''
        retval = []
        db_connection = DBConnection.get(cls)

        def filtered_callback(data_list):
            callback([x for x in data_list if x is not None])

        def process_keylist(keys):
            super(NamespacedStoredObject, cls).get_many(
                keys, callback=filtered_callback)
            
        if callback:
            db_connection.conn.keys(cls.__name__.lower() + "_*",
                                    callback=process_keylist)
        else:
            keys = db_connection.blocking_conn.keys(cls.__name__.lower()+"_*")
            return  [x for x in 
                     super(NamespacedStoredObject, cls).get_many(keys)
                     if x is not None]

    @classmethod
    def modify(cls, key, func, callback=None):
        super(NamespacedStoredObject, cls).modify(
            cls.format_key(key),
            func,
            callback=callback)

    @classmethod
    def modify_many(cls, keys, func, callback=None):
        super(NamespacedStoredObject, cls).modify_many(
            [cls.format_key(x) for x in keys],
            func,
            callback=callback)

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
        db_connection = DBConnection.get(cls)
        key = NeonApiKey.format_key(a_id)
        if db_connection.blocking_conn.set(key, api_key):
            return api_key

    @classmethod
    def get_api_key(cls, a_id, callback=None):
        ''' get api key from db '''
        db_connection = DBConnection.get(cls)
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
                
        if "_" not in internal_vid:
            _log.error('key=InternalVideoID msg=Invalid internal id %s' %internal_vid)
            return internal_vid

        vid = "_".join(internal_vid.split('_')[1:])
        return vid

class TrackerAccountID(object):
    ''' Tracker Account ID generation '''
    @staticmethod
    def generate(_input):
        ''' Generate a CRC 32 for Tracker Account ID'''
        return abs(binascii.crc32(_input))

class TrackerAccountIDMapper(NamespacedStoredObject):
    '''
    Maps a given Tracker Account ID to API Key 

    This is needed to keep the tracker id => api_key
    '''
    STAGING = "staging"
    PRODUCTION = "production"

    def __init__(self, tai, api_key=None, itype=None):
        super(TrackerAccountIDMapper, self).__init__(tai)
        self.value = api_key 
        self.itype = itype

    def get_tai(self):
        '''Retrieves the TrackerAccountId of the object.'''
        return self.key.partition('_')[2]
    
    @classmethod
    def get_neon_account_id(cls, tai, callback=None):
        '''
        returns tuple of api_key, type(staging/production)
        '''
        def format_tuple(result):
            ''' format result tuple '''
            if result:
                data = json.loads(result)
                return data['value'], data['itype']

        key = cls.format_key(tai)
        db_connection = DBConnection.get(cls)
       
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
    def __init__(self, a_id, api_key=None, default_size=(160,90)):
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

        # The default thumbnail (w, h) to serve for this account
        self.default_size = default_size

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
        db_connection = DBConnection.get(self)
        if callback:
            db_connection.conn.set(self.key, self.to_json(), callback)
        else:
            return db_connection.blocking_conn.set(self.key, self.to_json())
    
    def save_platform(self, new_integration, callback=None):
        '''
        Save Neon User account and corresponding platform object
        '''
        
        #temp: changing this to a blocking pipeline call   
        db_connection = DBConnection.get(self)
        pipe = db_connection.blocking_conn.pipeline()
        pipe.set(self.key, self.to_json())
        pipe.set(new_integration.key, new_integration.to_json()) 
        callback(pipe.execute())

    @classmethod
    def get_account(cls, api_key, callback=None):
        ''' return neon useraccount instance'''
        db_connection = DBConnection.get(cls)
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
        db_connection = DBConnection.get(cls)
        accounts = db_connection.blocking_conn.keys(cls.__name__.lower() + "*")
        for accnt in accounts:
            api_key = accnt.split('_')[-1]
            nu = NeonUserAccount.get_account(api_key)
            nuser_accounts.append(nu)
        return nuser_accounts
    
    @classmethod
    def get_neon_publisher_id(cls, api_key):
        '''
        Get Neon publisher ID; This is also the Tracker Account ID
        '''
        na = cls.get_account(api_key)
        if nc:
            return na.tracker_account_id  


class ExperimentStrategy(NamespacedStoredObject):
    '''Stores information about the experimental strategy to use.

    Keyed by account_id (aka api_key)
    '''
    SEQUENTIAL='sequential'
    MULTIARMED_BANDIT='multi_armed_bandit'
    
    def __init__(self, account_id, exp_frac=0.01,
                 holdback_frac=0.01,
                 only_exp_if_chosen=False,
                 always_show_baseline=True,
                 baseline_type=ThumbnailType.CENTERFRAME,
                 chosen_thumb_overrides=False,
                 override_when_done=True,
                 experiment_type=MULTIARMED_BANDIT,
                 impression_type=MetricType.VIEWS,
                 conversion_type=MetricType.CLICKS,
                 max_neon_thumbs=None):
        super(ExperimentStrategy, self).__init__(account_id)
        # Fraction of traffic to experiment on.
        self.exp_frac = exp_frac
        
        # Fraction of traffic in the holdback experiment once
        # convergence is complete
        self.holdback_frac = holdback_frac

        # If true, an experiment will only be run if a thumb is
        # explicitly chosen. This and chosen_thumb_overrides had
        # better not both be true.
        self.only_exp_if_chosen = only_exp_if_chosen

        # If True, a baseline of baseline_type will always be used in the
        # experiment. The other baseline could be an editor generated
        # one, which is always shown if it's there.
        self.always_show_baseline = always_show_baseline

        # The type of thumbnail to consider the baseline
        self.baseline_type = baseline_type

        # If true, if there is a chosen thumbnail, it automatically
        # takes 100% of the traffic and the experiment is shutdown.
        self.chosen_thumb_overrides =  chosen_thumb_overrides

        # If true, then when the experiment has converged on a best
        # thumbnail, it overrides the majority one and leaves a
        # holdback. If this is false, when the experiment is done, we
        # will only run the best thumbnail in the experiment
        # percentage. This is useful for pilots that are hidden from
        # the editors.
        self.override_when_done = override_when_done

        # The strategy used to run the experiment phase
        self.experiment_type = experiment_type

        # The types of measurements that mean an impression or a
        # conversion for this account
        self.impression_type = impression_type
        self.conversion_type = conversion_type

        # The maximum number of Neon thumbs to run in the
        # experiment. If None, all of them are used.
        self.max_neon_thumbs = max_neon_thumbs


class CDNHostingMetadata(object):
    '''
    Class to format the information to be stored in the platform.cdn_metadata
    member.

    Currently on S3 hosting to customer bucket is well defined
    '''
    def __init__(self, host_type, cdn_prefixes):
        self.host_type = host_type
        self.cdn_prefixes = cdn_prefixes
    
    def to_json(self):
        ''' to json '''
        return json.dumps(self, default=lambda o: o.__dict__) 
    
    def to_dict(self):
        return self.__dict__

    @classmethod
    def save_metadata(cls, api_key, i_id, hosting_metadata):
        '''
        Save the hosting directive in the platform account

        @hosting_directive - dict of the hosting metadata object
        '''
        if i_id == "0":
            accnt = NeonPlatform.get_account(api_key)
        else:
            raise Exception("To be implemented")
        # NOTE: Its safer to use a dict than double encoded json   
        if isinstance(hosting_metadata, dict):
            accnt.cdn_metadata = hosting_metadata
            return accnt.save()
        else:
            raise Exception("hosting_directive should be a dictionary")

class S3CDNHostingMetadata(CDNHostingMetadata):
    '''
    If the images are to be uploaded to S3 bucket use this formatter  

    '''
    def __init__(self, access_key, secret_key, bucket_name, cdn_prefixes,
                    folder_prefix):
        '''
        @cdn_prefixes : list : list of cdn prefixes that the customer uses
        '''
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.folder_prefix = folder_prefix
        CDNHostingMetadata.__init__(self, "s3", cdn_prefixes)

class CloudinaryCDNHostingMetadata(CDNHostingMetadata):
    '''
    Cloudinary images
    '''

    def __init__(self):
        CDNHostingMetadata.__init__(self, "cloudinary", [])

class AbstractPlatform(object):
    ''' Abstract Platform/ Integration class '''

    def __init__(self, abtest=False, enabled=True, 
                serving_enabled=True, serving_controller="imageplatform"):
        self.key = None 
        self.neon_api_key = ''
        self.videos = {} # External video id (Original Platform VID) => Job ID
        self.abtest = abtest # Boolean on wether AB tests can run
        self.integration_id = None # Unique platform ID to 
        self.enabled = enabled # Account enabled for auto processing of videos 

        # Indicates whose CDN the images are hosted on
        # None = Neon hosts the images on its CDN
        # else contains a json of CDNHostingDirective
        self.cdn_metadata = None   

        # Will thumbnails be served by our system?
        self.serving_enabled = serving_enabled

        # How is the A/B testing done, default to imageplatform
        self.serving_controller = serving_controller 

    def generate_key(self, i_id):
        ''' generate db key '''
        return '_'.join([self.__class__.__name__.lower(),
                         self.neon_api_key, i_id])
    
    def to_json(self):
        ''' to json '''
        return json.dumps(self, default=lambda o: o.__dict__) 

    def save(self, callback=None):
        ''' save instance '''
        db_connection = DBConnection.get(self)
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
    
    def get_processed_internal_video_ids(self):
        ''' return list of i_vids for an account which have been processed '''

        i_vids = []
        processed_state = [RequestState.FINISHED, 
                            RequestState.ACTIVE,
                            RequestState.REPROCESS, RequestState.SERVING, 
                            RequestState.SERVING_AND_ACTIVE]
        request_keys = [generate_request_key(self.neon_api_key, v) for v in
                        self.videos.values()]
        api_requests = NeonApiRequest.get_requests(request_keys)
        for api_request in api_requests:
            if api_request and api_request.state in processed_state:
                i_vids.append(InternalVideoID.generate(self.neon_api_key, 
                                                        api_request.video_id)) 
                
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
        db_connection = DBConnection.get(cls)
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
        db_connection = DBConnection.get(cls)
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
        db_connection = DBConnection.get(cls)
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

        # populate the object dictionary
        for key in data_dict.keys():
            obj.__dict__[key] = data_dict[key]
        
        try:
            # create the cdnhostingmetadata object
            if obj.cdn_metadata is not None:
                if isinstance(obj.cdn_metadata, dict):
                    cdn_dict = obj.cdn_metadata
                else:
                    _log.error("The object changed externally for %s" %
                            obj.neon_api_key)
                    return None

                cdn_obj = CDNHostingMetadata("", "")
                for key in cdn_dict.keys():
                    cdn_obj.__dict__[key] = cdn_dict[key]
                obj.cdn_metadata = cdn_obj
        except Exception, e:
            #Catch any exception
            _log.error("Error creating CDN Metadata Object %s" % e)
            return
        return obj
    
    @classmethod
    def get_all_instances(cls, callback=None):
        ''' get all neonplatform instances'''
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
        db_connection = DBConnection.get(self)
        if callback:
            db_connection.conn.get(self.key, callback)
        else:
            return db_connection.blocking_conn.get(self.key)

    def get_api(self, video_server_uri=None):
        '''Return the Brightcove API object for this platform integration.'''
        return api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id,
            self.read_token, self.write_token, self.auto_update,
            self.last_process_date, neon_video_server=video_server_uri,
            account_created=self.account_created)

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
                    callback(False)
            else:
                callback(False)
        
        vserver = options.video_server
        self.get_api(vserver).create_video_request(vid, self.integration_id,
                                            created_job)

    def check_feed_and_create_api_requests(self):
        ''' Use this only after you retreive the object from DB '''

        vserver = options.video_server
        bc = self.get_api(vserver)
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

        vserver = options.video_server
        bc = self.get_api(vserver)
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
        oo = ooyala_api.OoyalaAPI(self.ooyala_api_key, self.api_secret,
                neon_video_server=options.video_server)
        oo.process_publisher_feed(copy.deepcopy(self)) 

    #verify token and create requests on signup
    def create_video_requests_on_signup(self, n, callback=None):
        ''' Method to verify ooyala token on account creation 
            And create requests for processing
            @return: Callback returns job id, along with ooyala vid metadata
        '''
        oo = ooyala_api.OoyalaAPI(self.ooyala_api_key, self.api_secret,
                neon_video_server=options.video_server)
        oo._create_video_requests_on_signup(copy.deepcopy(self), n, callback) 

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def update_thumbnail(self, i_vid, new_tid):
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
            raise tornado.gen.Return(None)
        
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
            raise tornado.gen.Return(None)
            
        
        # Update the new_tid as the thumbnail for the video
        oo = ooyala_api.OoyalaAPI(self.ooyala_api_key, self.api_secret)
        update_result = yield tornado.gen.Task(oo.update_thumbnail,
                                               platform_vid,
                                               t_url,
                                               new_tid,
                                               fsize)
        #check if thumbnail was updated 
        if not update_result:
            raise tornado.gen.Return(None)
            
      
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
                raise tornado.gen.Return(False)
                
        else:
            _log.error("key=oo_update_thumbnail msg=new_thumb is None %s"%new_tid)
            raise tornado.gen.Return(False)
            

        req_data = NeonApiRequest.get_request(self.neon_api_key, vmdata.job_id) 
        vid_request = NeonApiRequest.create(req_data)
        vid_request.state = RequestState.ACTIVE
        ret = vid_request.save()
        if not ret:
            _log.error("key=update_thumbnail msg=%s state not updated to active"
                        %vid_request.key)
        raise tornado.gen.Return(True)
    
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
    FAILED     = "failed" # Failed due to video url issue/ network issue
    FINISHED   = "finished"
    SERVING    = "serving" # Thumbnails are ready to be served 
    INT_ERROR  = "internal_error" # Neon had some code error
    ACTIVE     = "active" # Thumbnail selected by editor; Only releavant to BC
    REPROCESS  = "reprocess" #new state added to support clean reprocessing

    # NOTE: This state is being added to save DB lookup calls to determine the active state
    # This is required for the UI. Re-evaluate this state for new UI
    # For CMS API response if SERVING_AND_ACTIVE return active state
    SERVING_AND_ACTIVE = "serving_active" # indicates there is a chosen thumb & is serving ready 

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
        self.video_id = vid #external video_id
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
        db_connection = DBConnection.get(self)
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
        db_connection = DBConnection.get(cls)
        def package(result):
            if result:
                nar = NeonApiRequest.create(result)
                callback(nar)
            else:
                callback(None)

        key = generate_request_key(api_key, job_id)
        if callback:
            db_connection.conn.get(key, lambda x: callback(package(x)))
        else:
            result = db_connection.blocking_conn.get(key)
            if result:
                return NeonApiRequest.create(result)

    @classmethod
    def get_request(cls, api_key, job_id, callback=None):
        ''' get request data '''
        db_connection=DBConnection.get(cls)
        key = generate_request_key(api_key, job_id)
        if callback:
            db_connection.conn.get(key,callback)
        else:
            return db_connection.blocking_conn.get(key)

    @classmethod
    def get_requests(cls, keys, callback=None):
        ''' mget results '''
        #MGET raises an exception for wrong number of args if keys = []
        if len(keys) == 0:
            if callback:
                callback([])
            else:
                return []

        db_connection = DBConnection.get(cls)
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
        if json_data:
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


class ThumbnailServingURLs(NamespacedStoredObject):
    '''
    Keeps track of the URLs to serve for each thumbnail id.

    Specifically, maps:

    thumbnail_id -> { (width, height) -> url }
    '''

    def __init__(self, thumbnail_id, size_map=None):
        super(ThumbnailServingURLs, self).__init__(thumbnail_id)
        self.size_map = size_map or {}

    def get_thumbnail_id(self):
        '''Return the thumbnail id for this mapping.'''
        return str(self.key.partition('_')[2])

    def add_serving_url(self, url, width, height):
        '''Adds a url to serve for a given width and height.

        If there was a previous entry, it is overwritten.
        '''
        self.size_map[(width, height)] = str(url)

    def get_serving_url(self, width, height):
        '''Get the serving url for a given width and height.

        Raises a KeyError if there isn't one.
        '''
        return self.size_map[(width, height)]

    def to_json(self):
        new_dict = copy.copy(self.__dict__)
        new_dict['size_map'] = self.size_map.items()
        return json.dumps(new_dict)

    @classmethod
    def _create(cls, key, json_data):
        if json_data:
            data_dict = json.loads(json_data)
            #create basic object
            obj = cls(key)

            #populate the object dictionary
            for key, value in data_dict.iteritems():
                obj.__dict__[str(key)] = value

            # Load in the size map as a dictionary
            obj.size_map = dict([[tuple(x[0]), str(x[1])] for 
                                 x in obj.size_map])
            return obj
        
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
        db_connection = DBConnection.get(self)
        if self.key is None:
            raise Exception("key not set")
        if callback:
            db_connection.conn.set(self.key, self.value, callback)
        else:
            return db_connection.blocking_conn.set(self.key, self.value)

    @classmethod
    def save_all(cls, thumbnailMapperList, callback=None):
        ''' multi save '''

        db_connection = DBConnection.get(cls)
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
        db_connection = DBConnection.get(cls)
        if callback:
            db_connection.conn.get(key, callback)
        else:
            return db_connection.blocking_conn.get(key)

    @classmethod
    def _erase_all_data(cls):
        ''' del all data'''
        db_connection = DBConnection.get(cls)
        db_connection.clear_db()

class ThumbnailMetadata(StoredObject):
    '''
    Class schema for Thumbnail information.

    Keyed by thumbnail id
    '''
    def __init__(self, tid, internal_vid=None, urls=None, created=None,
                 width=None, height=None, ttype=None,
                 model_score=None, model_version=None, enabled=True,
                 chosen=False, rank=None, refid=None, phash=None,
                 serving_frac=None):
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
        self.model_score = model_score #string
        self.model_version = model_version #string
        #TODO: remove refid. It's not necessary
        self.refid = refid #If referenceID exists *in case of a brightcove thumbnail
        self.phash = phash # Perceptual hash of the image. None if unknown
        
        # Fraction of traffic currently being served by this thumbnail.
        # =None indicates that Mastermind doesn't know of the fraction yet
        self.serving_frac = serving_frac 
        
        # NOTE: If you add more fields here, modify the merge code in
        # api/client, Add unit test to check this

    def update_phash(self, image):
        '''Update the phash from a PIL image.'''
        self.phash = supportServices.url2thumbnail.hash_pil_image(image)

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
                        obj.__dict__[str(key)] = value
                del data_dict['thumbnail_metadata']

            #populate the object dictionary
            for key, value in data_dict.iteritems():
                obj.__dict__[str(key)] = value
        
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
        db_connection = DBConnection.get(cls)
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
                 i_id=None, frame_size=None, testing_enabled=True,
                 experiment_state=ExperimentState.UNKNOWN,
                 experiment_value_remaining=None,
                 serving_enabled=True):
        super(VideoMetadata, self).__init__(video_id) 
        self.thumbnail_ids = tids 
        self.url = video_url 
        self.duration = duration
        self.video_valence = vid_valence 
        self.model_version = model_version
        self.job_id = request_id
        self.integration_id = i_id
        self.frame_size = frame_size #(w,h)
        # Is A/B testing enabled for this video?
        self.testing_enabled = testing_enabled
        self.experiment_state = \
          experiment_state if testing_enabled else ExperimentState.DISABLED

        # For the multi-armed bandit strategy, the value remaining
        # from the monte carlo analysis.
        self.experiment_value_remaining = experiment_value_remaining

        # Will thumbnails for this video be served by our system?
        self.serving_enabled = serving_enabled 

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

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_winner_tid(self):
        '''
        Get the TID that won the A/B test
        '''
        def almost_equal(a, b, threshold=0.001):
            return abs(a -b) <= threshold

        tmds = yield tornado.gen.Task(ThumbnailMetadata.get_many, self.thumbnail_ids)
        tid = None

        if self.experiment_state == ExperimentState.COMPLETE:
            #1. If serving fraction = 1.0 its the winner
            for tmd in tmds:
                if tmd.serving_frac == 1.0:
                    tid = tmd.key
                    raise tornado.gen.Return(tid)

            #2. Check the experiment strategy 
            es = ExperimentStrategy.get(self.get_account_id())
            if es.override_when_done == False:

                #Check if the experiment is in holdback state or exp state
                if almost_equal(es.exp_frac, es.holdback_frac): 
                    # we are in experimental state now, find the thumb with the
                    # experimental fraction
                    winner_tmd = filter(lambda t: almost_equal(t.serving_frac,
                                         es.exp_frac), tmds)
                    if len(winner_tmd) != 1:
                        _log.error("Error in the logic to determine winner tid")
                    else:
                        tid = winner_tmd[0].key

                    raise tornado.gen.Return(tid)

                else:
                    # Holdback state, return majoriy fraction
                    pass

            #Pick the max serving fraction
            max_tmd = max(tmds, key=lambda t: t.serving_frac)
            tid = max_tmd.key

        raise tornado.gen.Return(tid)

    def save_thumbnail_to_s3_and_store_metadata(self, image, score, keyname,
                                        s3fname, ttype, rank=0, model_version=0,
                                        cdn_metadata=None, callback=None):

        '''
        Save a thumbnail to s3 and its metadata in the DB
        @image: Image object
        @score: model score of the image
        @keyname: the basename or filename of the image

        @s3fname: s3 URI
        @ttype: Thumbnail type
        @rank: rank (int) of the thumbnail

        @return thumbnail metadata object
        '''
        i_vid = self.key #internal video id

        fmt = 'jpeg'
        filestream = StringIO()
        image.save(filestream, fmt, quality=90) 
        filestream.seek(0)
        imgdata = filestream.read()
        
        #upload to the host-thumbnail s3 bucket
        upload_to_s3_host_thumbnails(keyname, imgdata) 

        urls = []
        tid = ThumbnailID.generate(imgdata, i_vid)

        #If tid already exists, then skip saving metadata
        #if ThumbnailMetadata.get(tid) is not None:
        #    _log.warn('Already have thumbnail id: %s' % tid)
        #    return

        urls.append(s3fname)
        created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        width   = image.size[0]
        height  = image.size[1] 

        #populate thumbnails
        tdata = ThumbnailMetadata(tid, i_vid, urls, created, width, height,
                                  ttype, score, model_version, rank=rank)
        tdata.update_phash(image)

        #Add tid to video thumbnail list
        self.thumbnail_ids.append(tid)

        #Host this image on the Neon Image CDN in diff sizes
        hoster = CDNHosting.create(cdn_metadata)
        hoster.upload(image, tid)
        
        #Host this image on Cloudinary for dynamic resizing
        cloudinary_hoster = CDNHosting.create(CloudinaryCDNHostingMetadata())
        cloudinary_hoster.upload(s3fname, tid)

        return tdata

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def download_and_add_thumbnail(self, image_url, keyname,
                                    s3url, ttype, rank=1, cdn_metadata=None):
        '''
        Download the image and save its metadata. Used to save thumbnail
        of custom type or previous thumbnail given in the Neon API

        Note: s3bucket to upload to and the filename(keyname) of the thumbnail
        to be speicified to this function.

        '''
        score = None # no score assigned currently to uploaded thumbnails
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url=image_url,
                                             method="GET",
                                             request_timeout=60.0,
                                             connect_timeout=10.0)

        response = yield tornado.gen.Task(utils.http.send_request, req)
        if not response.error:
            imgdata = response.body
            image = Image.open(StringIO(imgdata))
            tdata = self.save_thumbnail_to_s3_and_store_metadata(image, score,
                                                keyname, s3url, ttype, rank,
                                                cdn_metadata=cdn_metadata)
            tid_save = yield tornado.gen.Task(tdata.save)
            if tid_save:
                raise tornado.gen.Return(tdata)
            else:
                _log.error("failed to save thumbnail %s" % tdata.key)
                raise tornado.gen.Return(False)

        else:
            _log.error("failed to download the image")
        raise tornado.gen.Return(None)

    def add_custom_thumbnail(self, image_url, callback=None):
        '''
        Add a custom thumbnail for the video
        '''

        fname = "custom%s.jpeg" % int(time.time())
        keyname = "%s/%s/%s" % (self.get_account_id(), self.job_id, fname)  
        s3url = "https://%s.s3.amazonaws.com/%s" % (options.thumbnailBucket, keyname) 
        np = NeonPlatform.get_account(self.get_account_id())
        cdn_metadata = np.cdn_metadata
        ttype = ThumbnailType.CUSTOMUPLOAD
        return self.download_and_add_thumbnail(image_url, keyname, s3url, ttype,
                cdn_metadata=cdn_metadata)

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

    @classmethod
    def get_video_requests(cls, i_vids):
        '''
        Get video request objs given video_ids
        '''
        vms = VideoMetadata.get_many(i_vids)
        request_keys = []
        for vm in vms:
            rkey = "request_nonexistent_key"
            if vm:
                api_key = vm.key.split('_')[0]
                rkey = generate_request_key(api_key, vm.job_id)
            request_keys.append(rkey)
        return NeonApiRequest.get_requests(request_keys)


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
            db_connection = DBConnection.get(self.classname)
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

class VideoResponse(object):
    ''' VideoResponse object that contains list of thumbs for a video '''
    def __init__(self, vid, job_id, status, i_type, i_id, title, duration,
            pub_date, cur_tid, thumbs, abtest=True, winner_thumbnail=None,
            serving_url=None):
        self.video_id = vid
        self.job_id = job_id 
        self.status = status
        self.integration_type = i_type
        self.integration_id = i_id
        self.title = title
        self.duration = duration
        self.publish_date = pub_date
        self.current_thumbnail = cur_tid
        #list of ThumbnailMetdata dicts 
        self.thumbnails = thumbs if thumbs else [] 
        self.abtest = abtest
        self.winner_thumbnail = winner_thumbnail
        self.serving_url = serving_url

    def to_dict(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

if __name__ == '__main__':
    # If you call this module you will get a command line that talks
    # to the server. nifty eh?
    utils.neon.InitNeon()
    code.interact(local=locals())
