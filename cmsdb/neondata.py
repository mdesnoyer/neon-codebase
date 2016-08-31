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
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import base64
import binascii
import cmsdb.cdnhosting
import code
from collections import OrderedDict, defaultdict
import concurrent.futures
import copy
import cv.imhash_index
import datetime
import dateutil.parser
import hashlib
import itertools
import simplejson as json
import logging
import model.scores
import momoko
import numpy 
import psycopg2
from passlib.hash import sha256_crypt
import random
import re
import sre_constants
import string
from StringIO import StringIO
import tornado.ioloop
import tornado.gen
import tornado.web
import tornado.httpclient
import time
import api.brightcove_api #coz of cyclic import
import api.youtube_api
import utils.botoutils
import utils.logs
import cvutils.imageutils
from cvutils.imageutils import PILImageUtils
import utils.neon
from utils.options import define, options
from utils import statemon
import utils.sync
import utils.s3
import utils.http
import urllib
import urlparse
import uuid


_log = logging.getLogger(__name__)

define("thumbnailBucket", default="host-thumbnails", type=str,
        help="S3 bucket to Host thumbnails ")

define("video_server", default="127.0.0.1", type=str, help="Neon video server")
define('async_pool_size', type=int, default=10,
       help='Number of processes that can talk simultaneously to the db')

define("db_address", default="localhost", type=str, 
       help="postgresql database address")
define("db_user", default="postgres", type=str, 
       help="postgresql database user")
define("db_password", default="", type=str, help="postgresql database user")
define("db_port", default=5432, type=int, help="postgresql port")
define("db_name", default="cmsdb", type=str, help="postgresql database name")
define("wants_postgres", default=0, type=int, help="should we use postgres")
define("max_connection_retries", default=5, type=int, 
       help="maximum times we should try to connect to db")
# this basically means how many open pubsubs we can have at once, most other pools will be relatively 
# small, and not get to this size. see momoko pool for more info. 
define("max_pool_size", default=250, type=int, 
       help="maximum size the connection pools can be")
define("max_io_loop_dict_size", default=500, type=int, 
       help="how many io_loop ids we want to store before cleaning")
define("connection_wait_time", default=2.5, type=float, 
       help="how long in seconds to wait for a connection from momoko")

## Parameters for thumbnail perceptual hashing
define("hash_type", default="dhash", type=str,
       help="Type of perceptual hash function to use. ahash, phash or dhash")
define("hash_size", default=64, type=int,
       help="Size of the perceptual hash in bits")

# Other parameters
define('send_callbacks', default=1, help='If 1, callbacks are sent')

define('isp_host', default='isp-usw-388475351.us-west-2.elb.amazonaws.com',
       help=('Host address to get to the ISP that is checked for if images '
             'are there'))

statemon.define('subscription_errors', int)
statemon.define('pubsub_errors', int)
statemon.define('sucessful_callbacks', int)
statemon.define('callback_error', int)
statemon.define('invalid_callback_url', int)

statemon.define('postgres_pubsub_connections', int) 
statemon.define('postgres_unknown_errors', int) 
statemon.define('postgres_connection_failed', int) 
statemon.define('postgres_listeners', int) 
statemon.define('postgres_successful_pubsub_callbacks', int) 
statemon.define('postgres_pools', int) 
statemon.define('postgres_pool_full', int)

class ThumbDownloadError(IOError):pass
class DBStateError(ValueError):pass
class DBConnectionError(IOError):pass

def _get_db_information(): 
    '''Function that returns the address to the database for an object.

    Inputs:
    Returns: { host:, port:, dbname:, user:, password:, }  
    '''
    # This function can get called a lot, so all the options lookups
    # are done without introspection.
    db_info = {} 
    db_info['host'] = options.get('cmsdb.neondata.db_address') 
    db_info['port'] = options.get('cmsdb.neondata.db_port') 
    db_info['dbname'] = options.get('cmsdb.neondata.db_name') 
    db_info['user'] = options.get('cmsdb.neondata.db_user') 
    db_info['password'] = options.get('cmsdb.neondata.db_password')
    return db_info  

def _object_to_classname(otype=None):
    '''Returns the class name of an object.

    otype can be a class object, an instance object or the class name
    as a string.
    '''
    cname = None
    if otype is not None:
        if isinstance(otype, basestring):
            cname = otype
        else:
            #handle the case for classmethod
            cname = otype.__class__.__name__ \
              if otype.__class__.__name__ != "type" else otype.__name__
    return cname

class PostgresDB(tornado.web.RequestHandler): 
    '''A DB singleton class for postgres. Manages 
       connections and pools that are currently 
       connected to the postgres db. 
    ''' 
    class _PostgresDB: 
        def __init__(self):
            # where the db is located 
            self.db_info = None 
            # keeps track of the io_loops we have seen, mapped from 
            # io_loop_obj -> pool
            self.io_loop_dict = {}
            # amount of time to wait until we will reconnect a dead conn
            # this comes from momoko reconnect_interval 
            self.reconnect_dead = 250.0  
            # the starting size of the pool 
            self.pool_start_size = 3
            
            # support numpy array types 
            psycopg2.extensions.register_adapter(
                numpy.ndarray, 
                psycopg2._psycopg.Binary)
            def typecast_numpy_array(data, cur): 
                if data is None: 
                    return None 
                buf = psycopg2.BINARY(data,cur)
                return numpy.frombuffer(buf) 
            np_array_type = psycopg2.extensions.new_type(
                psycopg2.BINARY.values, 
                'np_array_type', 
                typecast_numpy_array)
            psycopg2.extensions.register_type(np_array_type) 
        
        def _set_current_host(self): 
            self.old_host = self.host 
            self.host = options.db_address
 
        def _build_dsn(self): 
            return 'dbname=%s user=%s host=%s port=%s password=%s' % (self.db_info['dbname'], 
                        self.db_info['user'], 
                        self.db_info['host'], 
                        self.db_info['port'], 
                        self.db_info['password'])

        def _clean_up_io_dict(self):
            '''since we allow optional_sync on many of these calls 
                  and it creates a ton of io_loops, walk through this 
                  dict and cleanup any io_loops that have been killed 
                  recently 
            '''  
            if len(self.io_loop_dict) > options.max_io_loop_dict_size:
                for key in self.io_loop_dict.keys():
                    if key._running is False:
                        try: 
                            pool = self.io_loop_dict[key]['pool']
                            if not pool.closed: 
                                pool.close() 
                        except (KeyError, TypeError, AttributeError): 
                            pass
                        try: 
                            del self.io_loop_dict[key]
                        except Exception as e: 
                            pass 
            statemon.state.postgres_pools = len(self.io_loop_dict)

        def _get_momoko_db(self): 
            current_io_loop = tornado.ioloop.IOLoop.current()
            conn = momoko.Connection(dsn=self._build_dsn(),
                                     ioloop=current_io_loop,
                                     cursor_factory=psycopg2.extras.RealDictCursor)
            return conn
                        
        @tornado.gen.coroutine
        def get_connection(self): 
            '''gets a connection to postgres, this is ioloop based 
                 we want to be able to share pools across ioloops 
                 however, since we have optional_sync (which creates 
                 a new ioloop) we have to manage how the pools/connections 
                 are created.  
            '''
            self._clean_up_io_dict() 
            conn = None 
            current_io_loop = tornado.ioloop.IOLoop.current()
            io_loop_id = current_io_loop
            dict_item = self.io_loop_dict.get(io_loop_id)

            def _get_momoko_db(): 
                current_io_loop = tornado.ioloop.IOLoop.current()
                conn = momoko.Connection(
                           dsn=self._build_dsn(),
                           ioloop=current_io_loop,
                           cursor_factory=psycopg2.extras.RealDictCursor)
                return conn

            if self.db_info is None: 
                self.db_info = current_db_info = _get_db_information()
            else:
                current_db_info = _get_db_information()
  
            if dict_item is None: 
                # this is the first time we've seen this io_loop just 
                # get them set up for a pool, and return the connection 
                item = {}
                item['pool'] = None 
                self.io_loop_dict[io_loop_id] = item 
                db = _get_momoko_db()
                conn = yield self._get_momoko_connection(db) 
            else:
                # we have seen this ioloop before, it has a pool use it
                if dict_item['pool'] is None:
                    dict_item['pool'] = yield self._connect_a_pool()
                pool = dict_item['pool']
                try: 
                    conn = yield self._get_momoko_connection(
                               pool, 
                               True, 
                               dict_item)
                except Exception as e: 
                    pool.close() 
                    dict_item['pool'] = None 
                    raise
 
            raise tornado.gen.Return(conn)

        def return_connection(self, conn): 
            '''
            call this to return connections you are done with 

            this should always be called to ensure the 
            connections are properly returned to the pool, or 
            closed entirely assuming the connection was made 
            without a pool 
            '''  
            current_io_loop = tornado.ioloop.IOLoop.current()
            io_loop_id = current_io_loop
            dict_item = self.io_loop_dict.get(io_loop_id)
            pool = dict_item['pool']
            try: 
                if pool is None:
                    conn.close()
                else:
                    try: 
                        pool.putconn(conn) 
                    except AssertionError: 
                        # probably a release of an already released conn
                        pass 
            except Exception as e: 
                _log.exception('Unknown Error : on close connection %s' % e) 
      
        @tornado.gen.coroutine 
        def _connect_a_pool(self): 
            def _get_momoko_pool():
                current_io_loop = tornado.ioloop.IOLoop.current()
                pool = momoko.Pool(
                           dsn=self._build_dsn(),  
                           ioloop=current_io_loop, 
                           size=self.pool_start_size, 
                           max_size=options.max_pool_size,
                           auto_shrink=True, 
                           reconnect_interval=self.reconnect_dead,  
                           cursor_factory=psycopg2.extras.RealDictCursor)
                return pool

            pool = _get_momoko_pool() 
            num_of_tries = options.get('cmsdb.neondata.max_connection_retries')
            pool_connection = None
            for i in range(int(num_of_tries)):
                try: 
                    pool_connection = yield pool.connect()
                    break 
                except Exception as e:
                    current_db_info = _get_db_information() 
                    if current_db_info != self.db_info: 
                        self.db_info = current_db_info 
                        pool = _get_momoko_pool() 
                    _log.error('Retrying PG Pool connection : attempt=%d : %s' % 
                               (int(i+1), e))
                    sleepy_time = (1 << (i+1)) * 0.2 * random.random()
                    yield tornado.gen.sleep(sleepy_time)

            if pool_connection: 
                raise tornado.gen.Return(pool_connection)
            else: 
                _log.error('Unable to create a pool for Postgres Database')
                statemon.state.increment('postgres_connection_failed')
                raise Exception('Unable to get a pool of connections')
 
        @tornado.gen.coroutine
        def _get_momoko_connection(self, db, is_pool=False, dict_item=None):
            conn = None 
            num_of_tries = options.max_connection_retries

            for i in range(int(num_of_tries)):
                try:
                    if is_pool:
                        # momoko has a reconnect_interval on dead 
                        # connections, it will hang and not reconnect 
                        # if we don't wait long enough 
                        if len(db.conns.dead) > 0:
                            yield tornado.gen.sleep(
                                self.reconnect_dead / 1000.0)

                        if db.size >= options.max_pool_size:
                            statemon.state.increment('postgres_pool_full')
 
                        conn = yield tornado.gen.with_timeout(
                            datetime.timedelta(
                                seconds=options.connection_wait_time), 
                            db.getconn()) 
                    else: 
                        conn = yield tornado.gen.with_timeout(
                            datetime.timedelta( 
                                seconds=options.connection_wait_time), 
                            db.connect()) 
                    break
                except Exception as e: 
                    current_db_info = _get_db_information()  
                    if current_db_info != self.db_info: 
                        self.db_info = current_db_info
                        if is_pool:
                            db = yield self._connect_a_pool()
                            dict_item['pool'] = db  
                        else: 
                            db = self._get_momoko_db()
 
                    _log.error('Retrying PG connection : attempt=%d : %s' % 
                               (int(i+1), e))
                    sleepy_time = (1 << (i+1)) * 0.1 
                    yield tornado.gen.sleep(sleepy_time)
 
            if conn: 
                raise tornado.gen.Return(conn)
            else: 
                _log.error('Unable to get a connection to Postgres Database')
                statemon.state.increment('postgres_connection_failed')
                raise Exception('Unable to get a connection')

        def get_insert_json_query_tuple(self, obj):
            now_str = datetime.datetime.utcnow().strftime(
                "%Y-%m-%d %H:%M:%S.%f")
            obj.__dict__['created'] = obj.__dict__['updated'] = now_str
            fv = obj._get_iq_fields_and_values()
            fields = fv[0]
            values = fv[1] 

            extra_params = obj._get_query_extra_params()
            params = (
                obj.get_json_data(), 
                obj.__class__.__name__, 
                now_str, 
                now_str) + extra_params

            query = "INSERT INTO {tn} {fields} {values}".format(
                tn=obj._baseclass_name().lower(),
                fields=fields, 
                values=values)
        
            return (query, params)

        def get_update_json_query_tuple(self, obj):
            query = "UPDATE {tn} {sets} WHERE _data->>'key' = %s".format(
                tn=obj._baseclass_name().lower(),
                sets=obj._get_uq_set_string())
            params = (obj.get_json_data(),)
            extra_params = obj._get_query_extra_params()
            params += extra_params + (obj.key,) 
            return (query, params)
 
        def get_update_many_query_tuple(self, objects): 
            ''' helper function to build up an update multiple 
                   query  
                builds queries of the form 
                UPDATE table AS t SET t._data = changes.data
                FROM (values (obj.key, obj.get_json_data())) 
                  AS changes(key, data)
                WHERE changes.key = t._data->>'key' 
            '''
            try:
                strs = objects[0]._get_umq_sstr_vals_changes(len(objects)) 
                param_list = []
                table = objects[0]._baseclass_name().lower()
                query = "UPDATE {tn} AS t {setstr} FROM {valstr} AS {changestr} \
                    WHERE changes.key = t._data->>'key'".format(
                        tn=table,
                        setstr=strs[0], 
                        valstr=strs[1],
                        changestr=strs[2]) 
                for obj in objects: 
                    param_list.append(obj.key)
                    param_list.append(obj.get_json_data())
                    param_list += obj._get_query_extra_params() 
                                    
                return (query, tuple(param_list))  
            except KeyError: 
                return
 
    instance = None 
    
    def __new__(cls): 
        if not PostgresDB.instance: 
            PostgresDB.instance = PostgresDB._PostgresDB() 
        return PostgresDB.instance 

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name):
        return setattr(self.instance, name)
 
class PostgresPubSub(object):
    class _PostgresPubSub: 
        def __init__(self): 
            self.channels = {} 
        
        @tornado.gen.coroutine    
        def _connect(self):
            '''connect function for pubsub
               
               just use the PostgresDB class to get a connection
               to the database 
            '''
            self.db = PostgresDB() 
            conn = yield self.db.get_connection()
            raise tornado.gen.Return(conn)
        
        @tornado.gen.coroutine 
        def _receive_notification(self, 
                                  fd, 
                                  events, 
                                  channel_name):
 
            '''_receive_notification, callback for add_handler that monitors an open 
               pg file handler 

               will use the current io_loop to add a future to the expecting callbacks
 
               sends a future with a list of json strings  
            '''
            try:  
                channel = self.channels[channel_name]
                notifications = [] 
                connection = channel['connection'].connection 
                connection.poll()
                _log.info_n('Notifying listeners of db changes - %s' % 
                    (connection.notifies),25)
                while connection.notifies:
                    notification = connection.notifies.pop() 
                    payload = notification.payload
                    notifications.append(payload) 

                future = concurrent.futures.Future()
                future.set_result(notifications)
                channel = self.channels[channel_name]
                callback_functions = channel['callback_functions']
                for func in callback_functions:
                    tornado.ioloop.IOLoop.current().add_future(future, func) 
            except Exception as e: 
                statemon.state.increment('postgres_unknown_errors')
                _log.exception('Error in pubsub trying to get notifications %s. ' % e) 
                self._reconnect(channel_name) 
        
        def _reconnect(self, channel_name):
            '''reconnects to postgres in the case of a database mishap, or 
               a possible io_loop mishap 

               grabs the current channel that is listening 
               closes the connection (in case it's still open) 
               deletes it from the singleton list
               readds it and relistens on the callback functions that
                   currently exist

               TODO - make it so previous notifications that may have 
                      been lost get retried
            '''
            channel = self.channels[channel_name] 
            callback_functions = channel['callback_functions']
            self.unlisten(channel_name)  
 
            for cb in callback_functions: 
                self.listen(channel_name, cb)  

        @tornado.gen.coroutine
        def listen(self, channel_name, func):
            '''publicly accessible function that starts listening on a channel 
               
               handles the postgres connection as well, no need to connect before 
               calling me 
  
               channel - baseclassname of the object, will call PG LISTEN on this 
               func - the function to callback to if we receive notifications 
 
               simply is added to the io_loop via add_handler 
            ''' 
            if channel_name in self.channels:
                self.channels[channel_name]['callback_functions'].append(func) 
            else: 
                try: 
                    momoko_conn = yield self._connect()
                    yield momoko_conn.execute('LISTEN %s' % channel_name)
                    io_loop = tornado.ioloop.IOLoop.current()
                    self.channels[channel_name] = { 
                                                    'connection' : momoko_conn, 
                                                    'callback_functions' : [func] 
                                                  }
                    io_loop.add_handler(momoko_conn.connection.fileno(), 
                                        lambda fd, events: self._receive_notification(fd, events, channel_name), 
                                        io_loop.READ) 
                    _log.info(
                        'Opening a new listener on postgres at %s for channel %s' %
                        (self.db.db_info['host'], channel_name))
                    statemon.state.increment('postgres_listeners')
                except psycopg2.Error as e: 
                    _log.exception('a psycopg error occurred when listening to %s on postgres %s' \
                                   % (channel_name, e)) 
                except Exception as e: 
                    _log.exception('an unknown error occurred when listening to %s on postgres %s' \
                                   % (channel_name, e)) 
                    statemon.state.increment('postgres_unknown_errors')
                    
        @tornado.gen.coroutine
        def unlisten(self, channel_name):
            '''unlisten from a subscribed channel 

               WARN : this is a clear all command, any callback functions 
                      that have open listeners to this will be cleared out

               TODO : have function_id passed in as well, to only clear 
                      the callback we care about  
            '''
            _log.info(
                'Unlistening on postgres at %s for channel %s' %
                (self.db.db_info['host'], channel_name))
            try: 
                channel = self.channels[channel_name] 
                connection = channel['connection']
                try: 
                    yield connection.execute('UNLISTEN %s' % channel_name)
                    connection.close()
                except psycopg2.InterfaceError as e:
                    # this means we already lost connection, and can not close a 
                    # closed connection  
                    _log.exception('psycopg error when unlistening to %s on postgres %s' \
                                   % (channel, e))
                    pass  
                self.channels.pop(channel_name, None)
                statemon.state.decrement('postgres_listeners')
            except psycopg2.Error as e: 
                _log.exception('a psycopg error occurred when UNlistening to %s on postgres %s' \
                                % (channel_name, e)) 
            except Exception as e: 
                _log.exception('an unknown error occurred when UNlistening to %s on postgres %s' \
                               % (channel_name, e)) 
                statemon.state.increment('postgres_unknown_errors')

    instance = None 
    
    def __new__(cls): 
        if not PostgresPubSub.instance: 
            PostgresPubSub.instance = PostgresPubSub._PostgresPubSub() 
        return PostgresPubSub.instance 

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name):
        return setattr(self.instance, name) 
        
##############################################################################

def id_generator(size=32, 
            chars=string.ascii_lowercase + string.digits):
    ''' Generate a random alpha numeric string to be used as 
    '''
    retval = ''.join(random.choice(chars) for x in range(size))

    return retval

##############################################################################
## Enum types
############################################################################## 

class ThumbnailType(object):
    ''' Thumbnail type enumeration '''
    NEON        = "neon"
    BAD_NEON    = "bad_neon" # Low scoring, bad thumbnails for contrast
    CENTERFRAME = "centerframe"
    BRIGHTCOVE  = "brightcove" # DEPRECATED. Will be DEFAULT instead
    OOYALA      = "ooyala" # DEPRECATED. Will be DEFAULT instead
    RANDOM      = "random"
    FILTERED    = "filtered"
    DEFAULT     = "default" #sent via api request
    CUSTOMUPLOAD = "customupload" #uploaded by the customer/editor

class TagType(object):
    '''All valid Tag types'''
    VIDEO = 'video'
    COLLECTION = 'col'

class VideoRenditionContainerType(object):
    '''Valid video rendition types'''
    MP4 = 'mp4'
    GIF = 'gif'  # ffmpeg may only be able to take gif input for gif output

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

class IntegrationType(object): 
    BRIGHTCOVE = 'brightcoveintegration'
    OOYALA = 'ooyalaintegration'
    OPTIMIZELY = 'optimizelyintegration'

class DefaultSizes(object): 
    WIDTH = 160 
    HEIGHT = 90 

class ServingControllerType(object): 
    IMAGEPLATFORM = 'imageplatform'

class SubscriptionState(object): 
    ACTIVE = 'active' 
    CANCELED = 'canceled'
    UNPAID = 'unpaid' 
    PAST_DUE = 'past_due' 
    IN_TRIAL = 'trialing'

class AccessLevels(object):
    NONE = 0
    READ = 1
    UPDATE = 2
    CREATE = 4
    DELETE = 8
    ACCOUNT_EDITOR = 16
    INTERNAL_ONLY_USER = 32
    GLOBAL_ADMIN = 64
    SHARE = 128             # Resource permits share token authorization

    # Helpers  
    ALL_NORMAL_RIGHTS = READ | UPDATE | CREATE | DELETE
    ADMIN = ALL_NORMAL_RIGHTS | ACCOUNT_EDITOR
    EVERYTHING = ALL_NORMAL_RIGHTS |\
                 ACCOUNT_EDITOR | INTERNAL_ONLY_USER |\
                 GLOBAL_ADMIN

class ResultType(object):
    THUMBNAILS = 'thumbnails'
    CLIPS = 'clips'

    # Helpers 
    ARRAY_OF_TYPES = [ THUMBNAILS, CLIPS ]

class PythonNaNStrings(object): 
    INF = 'Infinite' 
    NEGINF = '-Infinite' 
    NAN = 'NaN' 

class PostgresColumn(object):
    '''Gives information on additional columns, and information stored outside 
         of _data, _type, created, updated on Postgres tables. 

       This is a class instead of a dictionary, just to enforce/explain what 
         the various things that are needed  
    ''' 
    def __init__(self, 
                 column_name, 
                 format_string, 
                 data_stored=None):
        # name of the additional column
        self.column_name = column_name
        # a format string for insert/update operations 
        # eg %s::jsonb
        #    %s::bytea
        #    %s 
        #    %d 
        self.format_string = format_string 
        # where on the object the data is stored 
        # a string, this will access the __dict__ directly
        self.data_stored = data_stored 

##############################################################################
class StoredObject(object):
    '''Abstract class to represent an object that is stored in the database.

    Fields can be either native types or other StoreObjects

    This contains common routines for interacting with the data.
    TODO: Convert all the objects to use this consistent interface.
    ''' 
    def __init__(self, key):
        self.key = str(key)

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        return str(self)

    def __cmp__(self, other):
        classcmp = cmp(self.__class__, other.__class__) 
        if classcmp:
            return classcmp

        obj_one = set(self.__dict__).difference(('created', 'updated'))
        obj_two = set(other.__dict__).difference(('created', 'updated')) 
        classcmp = obj_one == obj_two and all(self.__dict__[k] == other.__dict__[k] for k in obj_one)

        if classcmp: 
            return 0
 
        return cmp(self.__dict__, other.__dict__)

    @classmethod
    def key2id(cls, key):
        '''Converts a key to an id'''
        return key

    def _set_keyname(self):
        '''Returns the key in the database for the set that holds this object.

        The result of the key lookup will be a set of objects for this class
        '''
        raise NotImplementedError()

    @classmethod
    def format_key(cls, key):
        return key

    @classmethod
    def is_valid_key(cls, key):
        return True

    def to_dict(self):
        return {
            '_type': self.__class__.__name__,
            '_data': self.__dict__
            }

    def to_json(self):
        '''Returns a json version of the object'''
        def json_dumper(obj): 
            if isinstance(obj, numpy.ndarray): 
                return obj.tolist() 
            return obj.to_dict()
        return json.dumps(self, default=json_dumper)
    
    def get_json_data(self):
        '''
            for postgres we only want the _data field, since we 
            have a column that is named _data we do not want _data->_data

            override this if you need something custom to get _data
        '''
        def _json_fixer(obj):
            cur_data = obj
            for key, value in cur_data.items():
                try:  
                    if value == float('-inf'):
                        cur_data[key] = PythonNaNStrings.NEGINF
                    if value == float('inf'):
                        cur_data[key] = PythonNaNStrings.INF
                    if value == float('nan'):
                        cur_data[key] = PythonNaNStrings.NAN
                except ValueError: 
                    pass 
            # we want to remove these extras from the _data object 
            # to prevent the duplication of data
            addcs = self._additional_columns() 
            for c in addcs:
                try:  
                    del cur_data[c.column_name]
                except KeyError: 
                    pass 
            return obj
        obj = _json_fixer(copy.deepcopy(self.to_dict()['_data'])) 
        def json_serial(obj):
            if isinstance(obj, datetime.datetime):
                serial = obj.isoformat()
                return serial
            elif isinstance(obj, StoredObject):
                return obj.to_dict()
            return obj.__dict__
        return json.dumps(obj, default=json_serial)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def save(self, overwrite_existing_object=True):
        '''Save the object to the database.'''
        value = self.to_json()
        if self.key is None:
            raise ValueError("key not set")

        rv = True  
        db = PostgresDB()
        conn = yield db.get_connection()
        query_tuple = db.get_insert_json_query_tuple(self)
        try:  
            result = yield conn.execute(query_tuple[0], query_tuple[1])
        except psycopg2.IntegrityError as e:  
            # since upsert is not available until postgres 9.5 
            # we need to do an update here
            if overwrite_existing_object: 
                query_tuple = db.get_update_json_query_tuple(self)
                result = yield conn.execute(query_tuple[0], query_tuple[1])
            else: 
                raise  
        except Exception as e: 
            rv = False
            _log.exception('an unknown error occurred when saving an object %s' % e) 
            statemon.state.increment('postgres_unknown_errors')

        db.return_connection(conn)
        raise tornado.gen.Return(rv)

    @classmethod
    def _create(cls, key, obj_dict):
        '''Create an object from a dictionary that was created by save().

        Returns None if the object could not be created.
        '''
        if obj_dict:
            # Get the class type to create
            try:
                data_dict = obj_dict['_data']
                classname = obj_dict['_type']
                try:
                    classtype = globals()[classname]
                except KeyError:
                    _log.error('Unknown class of type %s in database key %s'
                               % (classname, key))
                    return None
            except KeyError:
                # For backwards compatibility, we didn't store the
                # type in the databse, so assume that the class is cls
                classtype = cls
                data_dict = obj_dict

            # throw created updated on the object if its there 
            try:
                for k in ['created_time_pg', 'updated_time_pg']:
                    if isinstance(obj_dict[k], datetime.datetime): 
                        data_dict[k.split('_')[0]] = obj_dict[k].strftime(
                            "%Y-%m-%d %H:%M:%S.%f")
                    elif isinstance(obj_dict[k], str):
                        data_dict[k.split('_')[0]] = dateutil.parser.parse(
                            obj_dict[k]).strftime("%Y-%m-%d %H:%M:%S.%f")
            except KeyError: 
                pass

            addcs = cls._additional_columns()
            if addcs: 
                for c in addcs: 
                    try:
                        data_dict[c.column_name] = obj_dict[c.column_name]
                    except KeyError: 
                        pass 
 
            # create basic object using the "default" constructor
            obj = classtype(key)

            #populate the object dictionary
            try:
                for k, value in data_dict.iteritems():
                    if value == PythonNaNStrings.NEGINF:
                        value = float('-inf') 
                    if value == PythonNaNStrings.INF:
                        value = float('inf')
                    if value == PythonNaNStrings.NAN:
                        value = float('nan') 
                    obj.__dict__[str(k)] = cls._deserialize_field(k, value)
            except ValueError:
                return None
            return obj

    @classmethod
    def _deserialize_field(cls, key, value):
        '''Deserializes a field by creating a StoredObject as necessary.'''
        if isinstance(value, dict):
            if '_type' in value and '_data' in value:
                # It is a stored object, so unpack it
                try:
                    classtype = globals()[value['_type']]
                    return classtype._create(key, value)
                except KeyError:
                    _log.error('Unknown class of type %s' % value['_type'])
                    raise ValueError('Bad class type %s' % value['_type'])
            else:
                # It is a dictionary do deserialize each of the fields
                for k, v in value.iteritems():
                    value[str(k)] = cls._deserialize_field(k, v)
        elif hasattr(value, '__iter__'):
            # It is iterable to treat it like a list
            value = [cls._deserialize_field(None, x) for x in value]
        return value

    def get_id(self):
        '''Return the non-namespaced id for the object.'''
        return self.key

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get(cls, key, create_default=False, log_missing=True):
        '''Retrieve this object from the database.

        Inputs:
        key - Key for the object to retrieve
        create_default - If true, then if the object is not in the database, 
                         return a default version. Otherwise return None.
        log_missing - Log if the object is missing in the database.

        Returns the object
        '''
        db = PostgresDB()
        conn = yield db.get_connection()

        obj = None
        query = ( "SELECT " + cls._get_gq_column_string() +
                  " FROM " + cls._baseclass_name().lower() +
                  " WHERE _data->>'key' = %s")
        cursor = yield conn.execute(query, [key])
        result = cursor.fetchone()
        if result:
            obj = cls._create(key, result)
        else:
            if log_missing:
                _log.warn('No %s for id %s in db' % (cls.__name__, key))
            if create_default:
                obj = cls(key)
        db.return_connection(conn)
        raise tornado.gen.Return(obj)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_many(cls, keys, create_default=False, log_missing=True,
                 func_level_wpg=True, as_dict=False):
        ''' Get many objects of the same type simultaneously

        This is more efficient than one at a time.

        Inputs:
        keys - List of keys to get
        create_default - If true, then if the object is not in the database, 
                         return a default version. Otherwise return None.
        log_missing - Log if the object is missing in the database.
        callback - Optional callback function to call

        Returns:
        A list of cls objects or None depending on create_default settings
        '''
        return cls._get_many_with_raw_keys(
            keys,
            create_default,
            log_missing,
            func_level_wpg,
            as_dict)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_many_with_pattern(cls, pattern):
        '''Returns many objects that match a pattern.

        Note, this can be a slow call because getting the keys is slow

        Inputs:
        pattern - A pattern, usually with a * to match keys

        Outputs:
        A list of cls objects
        '''
        results = []
        db = PostgresDB()
        conn = yield db.get_connection()
        query = ( "SELECT " + cls._get_gq_column_string() +
                  " FROM " + cls._baseclass_name().lower() +
                  " WHERE _data->>'key' ~ %s" )

        cursor = yield conn.execute(query, [pattern])
        for result in cursor:
            obj = cls._create(result['_data']['key'], result)
            results.append(obj)
        db.return_connection(conn)
        raise tornado.gen.Return(results)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_many_with_key_like(cls, key_portion):
        ''' Returns many rows that have a key that matches 
              in a query LIKE 'key_portion%' 

            In Postgres this is much faster(4x) than the pattern 
            matching ~ and makes this the much better choice if 
            you are trying to match on accountid_blah 
        ''' 
        results = [] 
        db = PostgresDB()
        conn = yield db.get_connection()
        baseclass_name = cls._baseclass_name().lower()
        column_string = cls._get_gq_column_string() 
        query = "SELECT " + column_string + " FROM " + baseclass_name + \
                " WHERE _data->>'key' LIKE %s"

        params = ['%'+key_portion+'%']
        cursor = yield conn.execute(query, params)
        for result in cursor:
            obj = cls._create(result['_data']['key'], result)
            results.append(obj) 
        db.return_connection(conn)
        raise tornado.gen.Return(results)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all(cls):
        ''' Get all the objects in the database of this type

        Inputs:
        callback - Optional callback function to call

        Returns:
        A list of cls objects.
        '''
        retval = []
        i = cls.iterate_all()
        while True:
            item = yield i.next(async=True)
            if isinstance(item, StopIteration):
                break
            retval.append(item)

        raise tornado.gen.Return(retval)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def iterate_all(cls, max_request_size=100, max_results=None):
        '''Return an iterator for all the ojects of this type.

        The set of keys to grab happens once so if the db changes while
        the iteration is going, so neither new or deleted objects will

        You can use it asynchronously like:
        iter = cls.get_iterator()
        while True:
          item = yield iter.next(async=True)
          if isinstance(item, StopIteration):
            break

        or just use it synchronously like a normal iterator.
        '''
        keys = yield cls.get_all_keys(async=True)
        raise tornado.gen.Return(
            StoredObjectIterator(cls, keys, page_size=max_request_size,
                                 max_results=max_results, skip_missing=True))

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_keys(cls):
        '''Return all the keys in the database for this object type.'''
        rv = True  
        db = PostgresDB()
        conn = yield db.get_connection()

        query = "SELECT _data->>'key' FROM %s" % cls._baseclass_name().lower()
        cursor = yield conn.execute(
            query, 
            cursor_factory=psycopg2.extensions.cursor)
        keys_list = [i[0] for i in cursor.fetchall()]
        db.return_connection(conn)
        rv = [x.partition('_')[2] for x in keys_list if
                                  x is not None]
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _get_many_with_raw_keys(cls, keys, create_default=False,
                                log_missing=True, func_level_wpg=True,
                                as_dict=False):
        '''Gets many objects with raw keys instead of namespaced ones.
        '''
        #MGET raises an exception for wrong number of args if keys = []
        if len(keys) == 0:
            raise tornado.gen.Return([])

        chunk_size = 1000
        rv = []
        obj_map = OrderedDict() 
        db = PostgresDB()
        conn = yield db.get_connection()
        # let's use a server-side cursor here 
        # since momoko won't let me declare a cursor by name, I need to 
        # do this manually 
        yield conn.execute("BEGIN")
 
        query_in_part = ','.join(['%s' for _ in range(len(keys))])
        query = ( "DECLARE get_many CURSOR FOR SELECT " +
                  cls._get_gq_column_string() +
                  " FROM " + cls._baseclass_name().lower() +
                  " WHERE _data->>'key' IN (%s)" % query_in_part)

        yield conn.execute(query, list(keys))
        for key in keys: 
            obj_map[key] = None 

        def _map_new_results(results):
            for result in results:
                obj_key = result['_data']['key'] 
                obj_map[obj_key] = result
 
        def _build_return_items(): 
            rv = {} if as_dict else []
            for key, item in obj_map.iteritems():
                if item: 
                    obj = cls._create(key, item) 
                else:
                    if log_missing:
                        _log.warn('No %s for %s' % (cls.__name__, key))
                    if create_default:
                        obj = cls(key)
                    else:
                        obj = None
                if as_dict:
                    rv[key] = obj
                else:
                    rv.append(obj)
            return rv
 
        rows = True
        while rows:
            cursor = yield conn.execute("FETCH %s FROM get_many", (chunk_size,))  
            rows = cursor.fetchmany(chunk_size) 
            _map_new_results(rows)

        yield conn.execute("CLOSE get_many")  
        yield conn.execute("COMMIT")
 
        db.return_connection(conn)
        items = _build_return_items()
        raise tornado.gen.Return(items)
    
    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify(cls, key, func, create_missing=False):
        '''Allows you to modify the object in the database atomically.

        While in func, you have a lock on the object so you are
        guaranteed for it not to change. It is automatically saved at
        the end of func.
        
        Inputs:
        func - Function that takes a single parameter (the object being edited)
        key - The key of the object to modify
        create_missing - If True, create the default object if it doesn't exist

        Returns: A copy of the updated object or None, if the object wasn't
                 in the database and thus couldn't be updated.

        Example usage:
        StoredObject.modify('thumb_a', lambda thumb: thumb.update_phash())
        '''
        def _process_one(d):
            val = d[key]
            if val is not None:
                func(val)

        updated_d = yield StoredObject.modify_many(
                 [key], _process_one,
                 create_missing=create_missing,
                 create_class=cls, 
                 async=True)
        raise tornado.gen.Return(updated_d[key])

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify_many(cls, keys, func, create_missing=False, create_class=None):
        '''Allows you to modify objects in the database atomically.

        While in func, you have a lock on the objects so you are
        guaranteed for them not to change. The objects are
        automatically saved at the end of func.
        
        Inputs:
        func - Function that takes a single parameter (dictionary of key -> object being edited)
        keys - List of keys of the objects to modify
        create_missing - If True, create the default object if it doesn't exist
        create_class - The class of the object to create. If None, 
                       cls is used (which is the most common case)

        Returns: A dictionary of {key -> updated object}. The updated
        object could be None if it wasn't in the database and thus
        couldn't be modified

        Example usage:
        SoredObject.modify_many(['thumb_a'], 
          lambda d: thumb.update_phash() for thumb in d.itervalues())
        '''
        if create_class is None:
            create_class = cls

        db = PostgresDB()
        conn = yield db.get_connection()
        if len(keys) == 0:
            raise tornado.gen.Return({})
            
        mappings = {}
        key_to_object = {}  
        for key in keys: 
            key_to_object[key] = None
 
        # Build a %s,%s,...,%s expression equal in length to keys.
        query_in_part = ','.join(['%s' for _ in range(len(keys))])
        query = ( "SELECT " + create_class._get_gq_column_string() +
                  " FROM " + create_class._baseclass_name().lower() +
                  " WHERE _data->>'key' IN (%s)" ) % query_in_part

        cursor = yield conn.execute(query, keys)
        items = cursor.fetchall()
        for item in items:
            current_key = item['_data']['key']
            key_to_object[current_key] = item
        
        for key, item in key_to_object.iteritems(): 
            if item is None:  
                if create_missing:
                    cur_obj = create_class(key)
                else:
                    _log.warn_n('Could not find postgres object: %s' % key)
                    cur_obj = None
            else:
                def json_serial(obj):
                    if isinstance(obj, datetime.datetime):
                        serial = obj.isoformat()
                        return serial
                # hack we need two copies of the object, copy won't work here
                item_one = json.loads(json.dumps(item, default=json_serial))
                cur_obj = create_class._create(key, item_one)

            mappings[key] = cur_obj 
        try:
            vals = func(mappings)
            if isinstance(vals, concurrent.futures.Future):
                yield vals
        finally:
            insert_statements = []
            update_objs = [] 
            for key, obj in mappings.iteritems():
               original_object = key_to_object.get(key, None)
               if obj is not None and original_object is None: 
                   query_tuple = db.get_insert_json_query_tuple(obj)
                   insert_statements.append(query_tuple) 
               elif obj is not None and obj != original_object:
                   update_objs.append(obj)

        if update_objs:  
            try:
                update_query = db.get_update_many_query_tuple(
                    update_objs)
                yield conn.execute(update_query[0], 
                                   update_query[1]) 
            except Exception as e: 
                _log.error('unknown error when running \
                            update_query %s : %s' % 
                            (update_query, e))

        if insert_statements: 
            try: 
                for it in insert_statements:  
                    yield conn.execute(it[0], it[1])
            except psycopg2.IntegrityError:
                pass  
            except Exception as e: 
                _log.error('unknown error when running \
                            inserts %s : %s' % 
                            (insert_statements, e))
            
        db.return_connection(conn)
        raise tornado.gen.Return(mappings)
            
    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def save_all(cls, objects):
        '''Save many objects simultaneously'''
        data = {}
        rv = True
 
        db = PostgresDB()
        conn = yield db.get_connection()
        sql_statements = [] 
        for obj in objects:
            query_tuple = db.get_insert_json_query_tuple(obj)
            sql_statements.append(query_tuple)
        try:  
            cursor = yield conn.transaction(sql_statements)
        except psycopg2.IntegrityError as e: 
            '''we rollback the transaction, but we still need to 
               save all the objects that were in the transaction
               we also do not know what object caused the integrityerror, 
               so just save on all of them'''
            for obj in objects: 
                obj.save() 
        except Exception as e: 
            rv = False
            _log.exception('an unknown error occurred when saving an object %s' % e) 
            statemon.state.increment('postgres_unknown_errors')

        db.return_connection(conn)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(cls, key):
        '''Delete an object from the database.

        Returns True if the object was successfully deleted
        '''
        rv = yield cls._delete_many_raw_keys([key], async=True)
        raise tornado.gen.Return(rv)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_many(cls, keys):
        '''Deletes many objects simultaneously

        Inputs:
        keys - List of keys to delete

        Returns:
        True if it was delete sucessfully
        '''
        rv = yield cls._delete_many_raw_keys(keys, async=True)
        raise tornado.gen.Return(rv)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _delete_many_raw_keys(cls, keys):
        '''Deletes many objects by their raw keys'''
        db = PostgresDB()
        conn = yield db.get_connection()
        sql_statements = []
        for key in keys:
            query = "DELETE FROM %s \
                     WHERE _data->>'key' = '%s'" % (cls._baseclass_name().lower(), 
                                          key)
            sql_statements.append(query)
 
        cursor = yield conn.transaction(sql_statements)
        db.return_connection(conn) 
        raise tornado.gen.Return(True)  
    
    @classmethod
    @tornado.gen.coroutine 
    def _handle_all_changes_pg(cls, future, func): 
        '''Callback to handle all changes occurring on postgres objects 
           
           the singleton connection that stores a map from classname -> func 

           it will take this list and call all the expecting cbs with a format 
           of : 
               func(key, object, operation) 

           these come in off a postgres trigger, see migrations/cmsdb.sql for 
             the definition of the this trigger. 
        '''
        results = future.result()
        for r in results: 
            r = json.loads(r)
            key = r['_key'] 
            op = r['tg_op']
            obj = yield cls.get(key, async=True)
            try:
                statemon.state.increment('postgres_successful_pubsub_callbacks')
                yield tornado.gen.maybe_future(func(obj.get_id() if obj else key, obj, op))
            except Exception as e:
                _log.error('Unexpected exception on PG db change when calling'
                           ' %s with arguments %s: %s' % 
                           (func, (key, obj, op), e))

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_related_data(cls, key):
        '''Deletes all data associated with a given object.

        For example, on the video object, this will delete the
        VideoMetadata object, the VideoStatus object, the
        NeonApiRequest object and all data related to each
        thumbnail. This function is not defined for every object type.

        Inputs:
        key - The internal key to delete
        '''
        raise NotImplementedError()
        
    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def subscribe_to_changes(cls, func, pattern='*', get_object=True):
        '''Subscribes to changes in the database.

        When a change occurs, func is called with the key, the updated
        object and the operation. The function must be thread safe as
        it will be called in a thread in a different context.

        Inputs:
        func - The function to call with signature func(key, obj, op)
        pattern - Pattern of keys to subscribe to
        get_object - If True, the object will be grabbed from the db.
                     Otherwise, it will be passed into the function as None
        '''
        pubsub = PostgresPubSub();
        pattern = cls._baseclass_name().lower()
        yield pubsub.listen(pattern, 
                            lambda x: cls._handle_all_changes_pg(x, func))

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def unsubscribe_from_changes(cls, channel):
        pubsub = PostgresPubSub();
        yield pubsub.unlisten(cls._baseclass_name().lower()) 

    @classmethod
    def format_subscribe_pattern(cls, pattern):
        return cls.format_key(pattern)

    @classmethod
    def get_select_query(cls, fields, where_clause=None, table_name=None,
                         join_clause=None, wc_params=[], limit_clause=None,
                         order_clause=None, group_clause=None):
        ''' helper function to build up a select query

               fields : an array of the fields you want
               where_clause : the portion of the query following WHERE
               table_name : defaults to _baseclass_name, but this populates
                            the from portion of the query

               eg fields = ["_data->>'neon_api_key'",
                            "_data->>'key'"]
                  object_type = neonuseraccount
                  where_clause = "_data->'users' ? %s"
                  params = [user1]
               would build
                  SELECT _data->>'neon_api_key', _data->>'key'
                   FROM neonuseraccount
                  WHERE _data->'users' ? user1
        '''
        if table_name is None:
            table_name = cls._baseclass_name().lower()

        csl_fields = ",".join("{0}".format(f) for f in fields)
        query = "SELECT " + csl_fields + " FROM " + table_name
        if join_clause:
            query += " JOIN " + join_clause
        if where_clause:
            query += " WHERE " + where_clause
        if order_clause:
            query += " " + order_clause
        if group_clause:
            query += " " + group_clause
        if limit_clause:
            query += " " + limit_clause

        return query

    @classmethod
    @tornado.gen.coroutine
    def explain_query(cls, query, wc_params, cursor_factory):
        ''' Get database query plan of query.'''
        db = PostgresDB()
        conn = yield db.get_connection()
        cursor = yield conn.execute(
            'EXPLAIN {}'.format(query), 
            wc_params, 
            cursor_factory=cursor_factory)
        rv = cursor.fetchall()
        db.return_connection(conn)
        raise tornado.gen.Return(rv)

    @classmethod
    @tornado.gen.coroutine
    def execute_select_query(cls, query, wc_params,
                             cursor_factory=psycopg2.extensions.cursor):
        ''' helper function to execute a select query

            returns the result array from the query

            be nice, this will do a fetchall, which can be
            memory intensive -- TODO make an option that
            operates like get_many does currently

            wc_params : any params you need in the where clause
        '''
        db = PostgresDB()
        conn = yield db.get_connection()

        cursor = yield conn.execute(
            query, 
            wc_params, 
            cursor_factory=cursor_factory)
        rv = cursor.fetchall()
        db.return_connection(conn)
        raise tornado.gen.Return(rv)

    @classmethod
    def _additional_columns(cls):
        '''Returns columns not named _data/_type/created/updated 
 
           Return Value is a list 

           For example, if a StoredObject needs something stored 
             outside of _data, eg if table t has a widget and a fidget column
             this should return [PostgresColumn('widget',...), 
                 PostgresColumn('fidget',...)]

           NOTE these must follow the order of the _get_query_extra_params 
            tuple from the object itself. 
        '''
        return []

    @classmethod
    def _get_gq_column_string(cls):
        '''Returns a string of columns that we need to retrieve 
            on this call 

           only override this method if you need different default
              columns.  
        '''
        dcs = ['_data', 
               '_type', 
               'created_time AS created_time_pg',
               'updated_time AS updated_time_pg']

        acs = cls._additional_columns()
        ac_col_names = [] 
        for a in acs: 
            ac_col_names.append(a.column_name) 
 
        return ','.join(dcs + ac_col_names)

    @classmethod 
    def _get_iq_fields_and_values(cls): 
        '''Returns a the fields we need on an insert query 
              eg (_data, _type)
 
           only override this method if you need different default
              columns.

           if you want additional fields(columns) added override 
           the _additional_columns function in this class 
        '''
        dcs = [PostgresColumn('_data', '%s::jsonb'), 
               PostgresColumn('_type', '%s'), 
               PostgresColumn('created_time', '%s'),
               PostgresColumn('updated_time', '%s')]
        acs = cls._additional_columns() 
        alls = dcs + acs 
        fields = '(%s)' % ','.join(['%s' % x.column_name for x in alls]) 
        values = 'VALUES(%s)' % ','.join(['%s' % x.format_string for x in alls])
        return (fields, values)

    @classmethod 
    def _get_uq_set_string(cls): 
        '''Returns the proper set string based on default columns 
            and the classes extra columns. 

           only override if you need different defaults 

           if you want additional fields override the _additional_columns
           function in this class 
 
           currently only supports string types TODO add other types 
        ''' 

        dcs = [PostgresColumn('_data', '%s::jsonb')]
        acs = cls._additional_columns() 
        alls = dcs + acs
        ss = 'SET %s' % ','.join(
            ['%s = %s' % (x.column_name, x.format_string) for x in alls])
        return ss
 
    @classmethod 
    def _get_umq_sstr_vals_changes(cls, object_length): 
        '''Returns the proper update_many string based on default columns 
            and the classes extra columns. 

           only override if you need different defaults 

           if you want additional fields override the _additional_columns
           function in this class 
 
           currently only supports string types TODO add other types

           returns a triple where 
           0 -> set str SET t._data = changes._data, t._addc1 = changes._addc1
           1 -> values str VALUES(%s, %s::jsonb, %s, ... , n) 
           2 -> changes str  changes(key, _data, _addc1, _addc2, _addcn)
        ''' 

        dcs = [PostgresColumn('key', '%s'), 
               PostgresColumn('_data', '%s::jsonb')]
        acs = cls._additional_columns() 
        alls = dcs + acs
        ss = 'SET %s' % ','.join(
            ['{cn} = changes.{cn}'.format(cn=x.column_name) for x in alls[1:]])
        # this is a somewhat horrible one-liner, 
        # but it builds up the necessary VALUES string, 
        # based on the length of the acs, as well as 
        # how many objects we are updating 
        vs = '(VALUES %s)' % ','.join(
            ['(%s)' % (','.join(
                ['%s' % x.format_string for x in alls])) for x in range(
                    object_length)])
        cs = 'changes(%s)' % ','.join(
            ['%s' % x.column_name for x in alls])
        return (ss,vs,cs)    

    def _get_query_extra_params(self):
        '''Returns a tuple of the data meant to be inserted/updated 
             into the database. Works off of the additional_columns 
             which should be defined in the baseclass 
        '''
        acs = self._additional_columns()  
        return tuple([self.__dict__[x.data_stored] for x in acs])


class Searchable(object):
    '''A search interface used with classes dervied from StoredObject

    Searchable's search_keys method returns a list of keys of matching
    StoredObjects. Its search_objects method return a list of instances. The
    arguments of each method are the natural map from attributes of the class,
    e.g., to find videos owned by an account "a1b1", use
    VideoMetadata.objects(account_id="a1b1").

    There are also methods that give the min and max updated times since
    searching often is paginated by time called search_keys_and_times and
    search_objects_and_times.

    Subclasses must implement:

        _get_search_arguments: Returns list of string, where each is a valid
            search key. Typically a subset of the storedobject's attributes.
            Attempting a search with a key not in the list raises KeyError.
            Example:
                return ['account_id', 'limit', 'name', 'offset', 'query']

        _get_where_part: If searching on a "query" argument, this function shall
            return the "this LIKE %that%"-like wildcard expression for the where
            clause, e.g., if query searches within the object's name:

            Example:
                if key == 'query':
                    return "_data->>'name' ~* %s"

    Subclasses can implement:

        _get_join_part: Handle objects backed by two tables. Shall return a
            string in the form 'JOIN table AS alias ON alias.key = t.key' (note,
            the primary table is generally aliased to 't').

        Example:
            "JOIN request AS r ON t._data->>'job_id' = r._data->>'job_id'"'''

    @staticmethod
    def _get_search_arguments():
        raise NotImplementedError('Add list of valid args in subclass')

    @staticmethod
    def _get_where_part(key, args):
        '''Support custom "query" searches

        This funcion should return a string that implements a custom query in the where
        for whichever data fields make sense for your subclass.

        For example, Tag having _name uses:

            return "_data->>'name' ~* %s"
        '''
        raise NotImplementedError('If using query search, define a query handler')

    @staticmethod
    def _get_join_part():
        '''Add join clause if needed

        Example: "JOIN request AS r ON t._data->>'job_id' = r._data->>'job_id'"'''
        return ''

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def search_for_keys(cls, **kwargs):
        '''Get list of keys that match search parameters in the database.

        Examples:
            given search parameters {'account_id': 'a'}
            and a database state [
              {'_data': {"key": 1, "account_id": 'a', ...}
              {'_data': {"key": 2, "account_id": 'b', ...}
              {'_data': {"key": 3, "account_id": 'a', ...}]
            yields [1, 3]'''
        kwargs['async'] = True
        keys_and_times = yield cls.search_for_keys_and_times(**kwargs)
        raise tornado.gen.Return(keys_and_times[0])

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def search_for_keys_and_times(cls, **kwargs):
        '''Like keys but returns min and max time of set, in 3-tuple.'''
        rows = yield cls._search(**kwargs)
        keys = [row['_data']['key'] for row in rows]
        min_time = cls._get_min_time(rows)
        max_time = cls._get_max_time(rows)
        raise tornado.gen.Return((
            keys,
            min_time,
            max_time))

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def search_for_objects(cls, **kwargs):
        '''Get list of objects that match search parameters in the database.

        Examples:
            given search parameters {'account_id': 'a'}
            and a database state [
              {'_data': {"key": 1, "account_id": 'a', ...}
              {'_data': {"key": 2, "account_id": 'b', ...}
              {'_data': {"key": 3, "account_id": 'a', ...}]
            yields [
                <Searchable instance with key 1>,
                <Searchable instance with key 3>]'''
        kwargs['async'] = True
        objects_and_times = yield cls.search_for_objects_and_times(**kwargs)
        raise tornado.gen.Return(objects_and_times[0])

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def search_for_objects_and_times(cls, **kwargs):
        '''Like objects but returns the min / max times of the set, as 3-tuple.'''

        rows = yield cls._search(**kwargs)
        objects = [cls._create(row['_data']['key'], row) for row in rows]
        min_time = cls._get_min_time(rows)
        max_time = cls._get_max_time(rows)
        raise tornado.gen.Return((
            objects,
            min_time,
            max_time))

    @classmethod
    def _get_min_time(cls, results):
        if not results:
            return None
        return min([cls._get_time(r) for r in results])

    @classmethod
    def _get_max_time(cls, results):
        if not results:
            return None
        return max([cls._get_time(r) for r in results])

    @staticmethod
    def _get_time(result):
        # need micros here
        created_time = result['created_time_pg']
        cc_tt = time.mktime(created_time.timetuple())
        _time = (cc_tt + created_time.microsecond / 1000000.0)
        return _time

    @classmethod
    @tornado.gen.coroutine
    def _search(cls, **kwargs):
        '''Builds and executes query to search on arugments.'''

        args = OrderedDict(sorted(kwargs.items()))
        reverse = args.get('since') == True

        def _validate():
            args.pop('async', None)
            allowed = cls._get_search_arguments()
            if filter(lambda k: k in allowed, args) != args.keys():
                raise KeyError('Bad argument to search %s' % args)
        def _query():
            return 'SELECT {c} FROM {table} AS t {join}{where}{order}{limit}{offset}'.format(
                c=_columns(),
                table=cls._baseclass_name().lower(),
                join=cls._get_join_part(),
                where=_where(),
                limit=_limit(),
                offset=_offset(),
                order=_order())
        def _columns():
            return 't._data, t._type, t.created_time as created_time_pg, t.updated_time as updated_time_pg'
        def _where():
            parts = []
            for key, value in args.items():
                if key in ['limit', 'offset']:
                    pass
                elif key == 'show_hidden':
                    if value:
                        pass
                    else:
                        parts.append("(t._data->>'hidden')::BOOLEAN IS NOT TRUE")
                elif key == 'query' and value:
                    # Check if we need to escape regex specials.
                    try:
                        re.compile(args[key])
                    except sre_constants.error:
                        args[key] = re.escape(args[key])
                    parts.append(cls._get_where_part(key, args))
                elif cls._get_where_part(key) and value:
                    parts.append(cls._get_where_part(key, args))
                elif key == 'since' and value:
                    parts.append('t.created_time > TO_TIMESTAMP(%s)::TIMESTAMP')
                elif key == 'until' and value:
                    parts.append('t.created_time < TO_TIMESTAMP(%s)::TIMESTAMP')
                # Generally just match whole value.
                elif value:
                    parts.append("t._data->>'{}' = %s".format(key))
            return ' WHERE %s' % ' AND '.join(parts) if parts else ''
        def _limit():
            return ' LIMIT %s' % kwargs.get('limit') \
                    if kwargs.get('limit') else ''
        def _offset():
            return ' OFFSET %s' % kwargs.get('offset') \
                    if kwargs.get('offset') else ''
        def _order():
            if reverse:
                return ' ORDER BY t.created_time ASC'
            else:
                return ' ORDER BY t.created_time DESC'
        def _bind():
            return [v for k, v in args.items() \
                    if k not in ['limit', 'offset', 'show_hidden'] and v]

        _validate()
        result = yield cls.execute_select_query(
            _query(),
            _bind(),
            cursor_factory=psycopg2.extras.RealDictCursor)
        if reverse:
            result.reverse()
        raise tornado.gen.Return(result)


class StoredObjectIterator():
    '''An iterator that generates objects of a specific type.

    It needs a list of keys to iterate through. Also, can be used
    synchronously in the normal way, or asynchronously by:

    iter = StoredObjectIterator(cls, keys)
    while True:
        item = yield iter.next(async=True)
        if isinstance(item, StopIteration):
          break
    '''
    def __init__(self, obj_class, keys, page_size=100, max_results=None,
                 skip_missing=False):
        '''Create the iterator

        Inputs:
        cls - Type of object to return
        keys - List of keys to iterate through
        page_size - Number of entries to grab from the db at once
        max_results - Maximum number of entries to return
        skip_missing - Should missing entries be skipped on the iteration
        '''
        self.obj_class = obj_class
        self.keys = keys
        self.page_size = page_size
        self.max_results = max_results
        self.curidx = 0
        self.items_returned = 0
        self.cur_objs = []
        self.skip_missing = skip_missing

    def __iter__(self):
        self.curidx = 0
        self.items_returned = 0
        self.cur_objs = []
        return self

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def next(self):
        if (self.max_results is not None and
            self.items_returned >= self.max_results):
            e = StopIteration()
            e.value = StopIteration()
            raise e

        while len(self.cur_objs) == 0:
            if self.curidx >= len(self.keys):
                # Got all the entries
                e = StopIteration()
                e.value = StopIteration()
                raise e

            # Get more objects
            self.cur_objs = yield tornado.gen.Task(
                self.obj_class.get_many,
                self.keys[self.curidx:(self.curidx+self.page_size)])
            if self.skip_missing:
                self.cur_objs = [x for x in self.cur_objs if x is not None]
            self.curidx += self.page_size

        self.items_returned += 1
        raise tornado.gen.Return(self.cur_objs.pop())


class NamespacedStoredObject(StoredObject):
    '''An abstract StoredObject that is namespaced by the baseclass classname.

    Subclasses of this must define _baseclass_name in the base class
    of the hierarchy. 
    '''
    
    def __init__(self, key):
        super(NamespacedStoredObject, self).__init__(
            self.__class__.format_key(key))

    def get_id(self):
        '''Return the non-namespaced id for the object.'''
        return self.key2id(self.key)

    @classmethod
    def key2id(cls, key):
        '''Converts a key to an id'''
        return re.sub(cls._baseclass_name().lower() + '_', '', key)

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.

        This should be implemented in the base class as:
        return <Class>.__name__
        '''
        raise NotImplementedError()

    @classmethod
    def _set_keyname(cls):
        return 'objset:%s' % cls._baseclass_name()

    @classmethod
    def format_key(cls, key):
        ''' Format the database key with a class specific prefix '''
        if key and key.startswith(cls._baseclass_name().lower()):
            return key
        else:
            return '%s_%s' % (cls._baseclass_name().lower(), key)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get(cls, key, create_default=False, log_missing=True):
        '''Return the object for a given key.'''
        rv = yield super(NamespacedStoredObject, cls).get(
                         cls.format_key(key),
                         create_default=create_default,
                         log_missing=log_missing, 
                         async=True)
        raise tornado.gen.Return(rv)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_many(cls, keys, create_default=False, log_missing=True, func_level_wpg=True):
        '''Returns the list of objects from a list of keys.

        Each key must be a tuple
        '''
        rv = yield super(NamespacedStoredObject, cls).get_many(
                         [cls.format_key(x) for x in keys],
                         create_default=create_default,
                         log_missing=log_missing, 
                         func_level_wpg=func_level_wpg, 
                         async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify(cls, key, func, create_missing=False):
        rv = yield super(NamespacedStoredObject, cls).modify(
                                 cls.format_key(key),
                                 func,
                                 create_missing=create_missing, async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify_many(cls, keys, func, create_missing=False):
        def _do_modify(raw_mappings):
            # Need to convert the keys in the mapping to the ids of the objects
            mod_mappings = dict(((v.get_id(), v) for v in 
                                 raw_mappings.itervalues()))
            return func(mod_mappings)

        rv = yield super(NamespacedStoredObject, cls).modify_many(
                                 [cls.format_key(x) for x in keys],
                                 _do_modify,
                                 create_missing=create_missing, async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(cls, key, callback=None):
        rv = yield super(NamespacedStoredObject, cls).delete(
                                 cls.format_key(key), async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_many(cls, keys, callback=None):
        rv = yield super(NamespacedStoredObject, cls).delete_many(
                               [cls.format_key(k) for k in keys], async=True)
        raise tornado.gen.Return(rv) 


class UnsaveableStoredObject(NamespacedStoredObject):
    '''A Stored object that cannot be saved directly to the DB.'''
    def __init__(self):
        self.key = ''

    def save(self):
        raise NotImplementedError()

    @classmethod
    def save_all(cls, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def modify(cls, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def modify_many(cls, *args, **kwargs):
        raise NotImplementedError()


class DefaultedStoredObject(NamespacedStoredObject):
    '''Namespaced object where a get-like operation will never returns None.

    Instead of None, a default object is returned, so a subclass should
    specify a reasonable default constructor
    '''
    def __init__(self, key):
        super(DefaultedStoredObject, self).__init__(key)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get(cls, key, log_missing=True, callback=None):
        rv = yield super(DefaultedStoredObject, cls).get(
                    key,
                    create_default=True,
                    log_missing=log_missing,
                    callback=callback, 
                    async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_many(cls, keys, log_missing=True, callback=None, func_level_wpg=True):
        rv = yield super(DefaultedStoredObject, cls).get_many(
                    keys,
                    create_default=True,
                    log_missing=log_missing,
                    callback=callback, 
                    func_level_wpg=func_level_wpg,
                    async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify_many(cls, keys, func, create_missing=None, callback=None):
        rv = yield super(DefaultedStoredObject, cls).modify_many(
                    keys, 
                    func, 
                    create_missing=True, 
                    callback=callback, 
                    async=True)
        raise tornado.gen.Return(rv) 


class MappingObject(object):
    '''Abstract class that backs an abstract 2-way, many-to-many relation.

    Arguments should match the implementing class's table column names.
    Ids are enforced by foreign key index to be valid references.
    Uniqueness is likewise enforced by a unique index on the id pair.

    Assumes the database contains a table with name in cls._table,
    and that table columns are the values in _keys:
        e.g., _table = tag_thumbnail and _keys = ['tag_id', 'thumbnail_id']'''

    @staticmethod
    def _get_table():
        '''Name of table that sits behind this mappingobject.'''
        raise NotImplementedError('Override in subclass')

    @staticmethod
    def _get_keys():
        '''Names of keys, or columns, that back this mappingobject.'''
        #  Must match name and order of schema
        raise NotImplementedError('Override in subclass')

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get(cls, **kwargs):
        '''Get list of matching values for one key.

        Given exactly one key and value, get list of the keys
        that exist in the database that are associated to it.

        Yields the empty list if no match found.

        Example: for TagThumbnail, kwargs={'tag_id': 'a'}, yields
        list of thumbnail ids that are associated with 'a' in the db.'''

        _, value = cls._get_key_and_value(**kwargs)
        if not value:
            raise tornado.gen.Return([])
        kwargs['async'] = True
        fetched = yield cls.get_many(**kwargs)
        raise tornado.gen.Return(fetched[value])

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_many(cls, **kwargs):
        '''Get map of keys to lists of associated keys.

        Given exactly one of the column names and a list of ids
        return the map of ids to lists of their associated values.

        Example: for TagThumbnail, kwargs={'thumbnail_id': ['a', 'b', 'c'],
        yields {
            'a': [thumbnail_id_a1, thumbnail_id_a2, ...]
            'b': [thumbnail_id_b1, thumbnail_id_b2, ...]
            'c': [thumbnail_id_c1, thumbnail_id_c2, ...]
        } provided the thumbnails match on id.

        The map includes the empty list if no match found for a
        particular key. '''
        kwargs['async'] = True
        key, values = cls._get_key_and_values(**kwargs)
        if not values:
            raise tornado.gen.Return({})
        keys = cls._get_keys()
        other_key = keys[0] if keys[1] == key else keys[1]
        fetch = []
        sql, bind = cls._get_select_single_col_tuple(key, values)
        yield cls._execute(sql, bind, fetch)
        result = {str(v): [item[other_key] for item in fetch
                  if item[key] == str(v)] for v in values}
        raise tornado.gen.Return(result)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def has(cls, **kwargs):
        '''Given a key-pair, returns True if the pair is associated in the db.

        Example: kwargs={'tag_id': 'tag0', 'thumbnail_id': 'thmb0'} => True
        if (tag0, thumb0 is in the database.'''
        cls._validate_scalar_values(**kwargs)
        kwargs['async'] = True
        fetched = yield cls.has_many(**kwargs)
        raise tornado.gen.Return(len(fetched) == 1)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def has_many(cls, **kwargs):
        '''Given lists of keys, return dictionary of every pair to boolean.

        Takes the cartesian product of the input lists.

        Example: kwargs={
            'tag_id': ['tag0', 'tag1', ...],
            'thumbnail_id': ['thumb0', 'thumb1', ...]
        Yields the map {
            (tag0, thumb0): bool,
            (tag0, thumb1): bool,
            (tag1, thumb0): bool,
            (tag1, thumb1): bool}'''
        cls._validate_keys(kwargs.keys())
        fetch = []
        sql, bind = cls._get_select_tuple(**kwargs)
        yield cls._execute(sql, bind, fetch)
        result = defaultdict(lambda: False)
        keys = cls._get_keys()
        for row in fetch:
            result[(row[keys[0]], row[keys[1]])] = True
        raise tornado.gen.Return(result)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def save(cls, **kwargs):
        '''Given a pair of keys, return True if save creates new row.

        Yields False if row already exists. Raise exception on other results.
        Example: kwargs={'tag_id': 'tag0', 'thumbnail_id': 'thumb0'}'''
        cls._validate_scalar_values(**kwargs)
        kwargs['async'] = True
        rows_changed = yield cls.save_many(**kwargs)
        raise tornado.gen.Return(rows_changed == 1)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def save_many(cls, **kwargs):
        '''Allow saving of many mapping relations at once.

        Returns the number of rows created.

        Example: kwargs={
            'tag_id': ['tag0', 'tag1', ...],
            'thumbnail_id': ['thumb0', 'thumb1', ...]}
        Saves all the pairs of items in given lists, i.e., saves
        [(tag0, thumb0), (tag0, thumb1), (tag1, thumb0), (tag1, thumb1)]'''

        # Validate input
        cls._validate_keys(kwargs.keys())

        try:
            pairs = cls._get_unique_pairs(**kwargs)
        except ValueError:
            raise tornado.gen.Return(0)

        # Build and execute insert.
        sql, bind = cls._get_insert_tuple(pairs)
        cursor = yield cls._execute(sql, bind)
        raise tornado.gen.Return(cursor.rowcount)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(cls, **kwargs):
        '''Delete a single mapping object row.

        Example kwargs={
            'tag_id': 'tag0', 'thumbnail_id': 'thumb0'}
        Yields True if the row exists and is deleted else False.'''
        cls._validate_scalar_values(**kwargs)
        kwargs['async'] = True
        rows_changed = yield cls.delete_many(**kwargs)
        raise tornado.gen.Return(rows_changed == 1)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_many(cls, **kwargs):
        '''Delete many mapping object rows.

        Deletes all pairs made from the product of input lists.
        Yields the number of rows deleted.'''
        cls._validate_keys(kwargs.keys())
        sql, bind = cls._get_delete_tuple(**kwargs)
        cursor = yield cls._execute(sql, bind)
        raise tornado.gen.Return(cursor.rowcount)

    @staticmethod
    @tornado.gen.coroutine
    def _execute(sql, bind, fetch=None):
        db = PostgresDB()
        conn = yield db.get_connection()
        try:
            cursor = yield conn.execute(sql, bind)
            if type(fetch) is list:
                fetch.extend(cursor.fetchall())

        except Exception as e:
            statemon.state.increment('postgres_unknown_errors')
            raise e
        finally:
            db.return_connection(conn)
        raise tornado.gen.Return(cursor)

    @classmethod
    def _get_key_and_value(cls, **kwargs):
        args = kwargs.copy()
        args.pop('async', None)
        key, values = cls._get_key_and_values(**args)
        if len(values) is not 1:
            raise TypeError('Expect exactly one value in get')
        return key, values[0]

    @classmethod
    def _get_key_and_values(cls, **kwargs):
        args = kwargs.copy()
        args.pop('async', None)
        if len(args) is not 1:
            raise TypeError('Expect exactly one key in get kwargs')
        keys = cls._get_keys()
        if keys[0] in args.keys():
            values = args[keys[0]]
            key = keys[0]
        elif keys[1] in args.keys():
            values = args[keys[1]]
            key = keys[1]
        else:
            raise KeyError('Unrecognized key in %s' % args.keys())
        # Ensure strings are not ever treated as indexable list.
        if isinstance(values, str):
            values = [values]
        if type(values) is not list:
            values = [values]
        if any([v for v in values if type(v) not in [str, unicode, None]]):
            raise ValueError('Unhandled value type %s', values)
        return key, values

    @classmethod
    def _validate_scalar_values(cls, **kwargs):
        '''Return true if both values inputs are int/str or lists
        of one element.'''
        keys = cls._get_keys()
        values0 = kwargs[keys[0]]
        values1 = kwargs[keys[1]]
        if (
            (type(values0) in [str, unicode]) or
            (type(values0) is list and len(values0) is 1)
        ) and (
            (type(values1) in [str, unicode]) or
            (type(values1) is list and len(values1) is 1)
        ):
            return
        raise ValueError(
            'Called with long list, int or object left:%s right:%s',
            values0,
            values1)

    @classmethod
    def _validate_keys(cls, keys):
        if not len(keys) == 2:
            raise KeyError('Wrong number of arguments')
        if not set(keys) == set(cls._get_keys()):
            raise KeyError('Bad key in save %s' % keys)

    @classmethod
    def _get_unique_pairs(cls, **kwargs):
        '''Gather unique pairs of input keys.'''
        keys = cls._get_keys()
        left_values = kwargs[keys[0]]
        right_values = kwargs[keys[1]]
        if not left_values or not right_values:
            raise ValueError('Input must be valued')
        if type(left_values) in [str, unicode, int]:
            left_values = [left_values]
        else:
            left_values = list(set(left_values))
        if type(right_values) in [str, unicode, int]:
            right_values = [right_values]
        else:
            right_values = list(set(right_values))
        return set(itertools.product(left_values, right_values))

    @classmethod
    def _get_select_single_col_tuple(cls, key, values):
        return (
            'SELECT * FROM {table} WHERE {key} IN ({values})'.format(
                table=cls._get_table(),
                key=key,
                values=cls._format_values_bind(values)),
            [str(v) for v in values])

    @classmethod
    def _get_select_tuple(cls, **kwargs):
        '''Get select sql string with %s placeholders, and a list of binds.'''
        keys = cls._get_keys()
        left_values = kwargs[keys[0]]
        right_values = kwargs[keys[1]]
        if type(left_values) is not list:
            left_values = [left_values]
        if type(right_values) is not list:
            right_values = [right_values]
        return ('''
            SELECT *
            FROM {table}
            WHERE {table}.{key1} IN ({left_values}) AND
                  {table}.{key2} IN ({right_values});'''.format(
                table=cls._get_table(),
                key1=keys[0],
                key2=keys[1],
                left_values=cls._format_values_bind(left_values),
                right_values=cls._format_values_bind(right_values)),
            [str(v) for v in left_values + right_values])

    @classmethod
    def _get_insert_tuple(cls, values):
        '''Get a merge insert sql string with %s placeholders, and a list of binds.'''
        keys = cls._get_keys()
        return ('''
            WITH new_values AS (
                SELECT *
                FROM (
                    VALUES {values}
                ) AS input_values
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table}
                    WHERE input_values.column1 = {table}.{key1} AND
                          input_values.column2 = {table}.{key2}
                )
            )
            INSERT INTO {table}
            SELECT * FROM new_values;'''.format(
                table=cls._get_table(),
                key1=keys[0],
                key2=keys[1],
                values=cls._format_insert_values_bind(values)),
            [str(v) for v in itertools.chain.from_iterable(values)])

    @classmethod
    def _get_delete_tuple(cls, **kwargs):
        keys = cls._get_keys()
        left_values = kwargs[keys[0]]
        right_values = kwargs[keys[1]]
        if type(left_values) is not list:
            left_values = [left_values]
        if type(right_values) is not list:
            right_values = [right_values]
        return ('''
            DELETE FROM {table}
            WHERE {table}.{key1} IN ({left_values}) AND
                  {table}.{key2} IN ({right_values})'''.format(
                table=cls._get_table(),
                key1=keys[0],
                key2=keys[1],
                left_values=cls._format_values_bind(left_values),
                right_values=cls._format_values_bind(right_values)),
            [str(v) for v in (left_values + right_values)])

    @staticmethod
    def _format_values_bind(values):
        '''Given list of values, get valid IN clause expression.
        E.g., [0, 1, 2, 3] => '%s,%s,%s,%s' '''
        return ','.join(['%s' for _ in values])

    @staticmethod
    def _format_insert_values_bind(values):
        '''Given list of 2-ples, get valid VALUES clause expression.
        E.g., [(0,0), (0,1), (0,1), (0,2)] =>
              '(%s,%s),(%s,%s),(%s,%s),(%s,%s)' '''
        return ','.join(['(%s, %s)' for _ in values])


class TagThumbnail(MappingObject):
    '''TagThumbnail represents an association of a tag to a thumbnail.'''

    @staticmethod
    def _get_table():
        return 'tag_thumbnail'

    @staticmethod
    def _get_keys():
        return ['tag_id', 'thumbnail_id']


class Tag(Searchable, StoredObject):
    '''Tag is a generic relation associating a set of user objects.'''

    def __init__(self, tag_id=None, account_id=None, name=None, tag_type=None,
                 video_id=None, hidden=False, share_token=None):
        tag_id = tag_id or uuid.uuid4().hex

        # Owner
        self.account_id = account_id
        # User's descriptive name
        self.name = name
        # System's definition of how this tag is used
        self.tag_type = tag_type if tag_type in [
            TagType.VIDEO, TagType.COLLECTION] else None
        self.video_id = video_id
        self.share_token = share_token
        self.hidden = hidden

        super(Tag, self).__init__(tag_id)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(cls, key):
        '''Overrides parent to also delete associations of tag.'''

        # Delete associations.
        thumbnail_ids = yield TagThumbnail.get(
            tag_id=key,
            async=True)
        if thumbnail_ids:
            yield TagThumbnail.delete_many(
                tag_id=key,
                thumbnail_id=thumbnail_ids,
                async=True)
        # Delete tag itself.
        yield super(Tag, cls).delete(key, async=True)

    @staticmethod
    def _get_search_arguments():
        return [
            'account_id',
            'limit',
            'name',
            'offset',
            'query',
            'since',
            'show_hidden',
            'until',
            'tag_type']

    @staticmethod
    def _get_where_part(key, args={}):
        if key == 'query':
            return "_data->>'name' ~* %s"

    @staticmethod
    def _baseclass_name():
        return Tag.__name__

    def get_account_id(self):
        return self.account_id


class Clip(StoredObject):
    '''Stub for gif clips'''

    def __init__(self, clip_id, account_id=None, share_token=None):
        super(Clip, self).__init__(clip_id)
        self.account_id = account_id
        self.share_token = share_token

    @staticmethod
    def _baseclass_name():
        return Tag.__name__

    def get_account_id(self):
        return self.account_id


class AbstractHashGenerator(object):
    '''Abstract Hash Generator'''

    @staticmethod
    def _api_hash_function(_input):
        ''' Abstract hash generator '''
        return hashlib.md5(_input).hexdigest()

class NeonApiKey(NamespacedStoredObject):
    ''' Static class to generate Neon API Key'''

    def __init__(self, a_id, api_key=None):
        super(NeonApiKey, self).__init__(a_id)
        self.api_key = api_key

    @classmethod
    def _baseclass_name(cls):
        '''
        Returns the class name of the base class of the hierarchy.
        '''
        return NeonApiKey.__name__
    
    @classmethod
    def id_generator(cls, size=24, 
            chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

        
    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def generate(cls, a_id):
        ''' generate api key hash
            if present in DB, then return it
        
        #NOTE: Generate method directly saves the key
        '''
        api_key = NeonApiKey.id_generator()
        obj = NeonApiKey(a_id, api_key)
        
        # Check if the api_key for the account id exists in the DB
        _api_key = cls.get_api_key(a_id)
        if _api_key is not None:
            raise tornado.gen.Return(_api_key)
        else:
            result = yield obj.save(async=True) 
            if result:
                raise tornado.gen.Return(api_key)

    def to_json(self):
        #NOTE: This is a misnomer. It is being overriden here since the save()
        # function uses to_json() and the NeonApiKey is saved as a plain string
        # in the database
        return json.dumps(self.__dict__) 
    
    @classmethod
    def _create(cls, key, obj_dict):
        obj = NeonApiKey(key)
        obj.value = obj_dict
        return obj_dict
    
    @classmethod
    def get_api_key(cls, a_id, callback=None):
        ''' get api key from db '''

        # Use get
        api_key = cls.get(a_id, callback)
        return api_key

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get(cls, a_id, callback=None):
        #NOTE: parent get() method uses json.loads() hence overriden here
        key = cls.format_key(a_id)
        db = PostgresDB()
        conn = yield db.get_connection()

        query = ( "SELECT _data  FROM " + cls.__name__.lower() +
                  " WHERE _data->>'key' = %s" )

        cursor = yield conn.execute(query, [key])
        result = cursor.fetchone()
        db.return_connection(conn)
        if result:  
            raise tornado.gen.Return(result['_data']['api_key']) 
        else: 
            raise tornado.gen.Return(None) 
   
    @classmethod
    def get_many(cls, keys, callback=None):
        raise NotImplementedError()
    
    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all(cls, keys, callback=None):
        raise NotImplementedError()

    @classmethod
    def modify(cls, key, func, create_missing=False, callback=None):
        raise NotImplementedError()
    
    @classmethod
    def modify_many(cls, keys, func, create_missing=False, callback=None):
        raise NotImplementedError()

class InternalVideoID(object):
    ''' Internal Video ID Generator '''
    NOVIDEO = 'nvd' # External video id to specify that there is no video

    VALID_EXTERNAL_REGEX = '[0-9a-zA-Z\-\.~]+'
    VALID_INTERNAL_REGEX = ('[0-9a-zA-Z]+_%s' % VALID_EXTERNAL_REGEX)
    
    @staticmethod
    def generate(api_key, vid=None):
        ''' external platform vid --> internal vid '''
        if vid is None:
            vid = InternalVideoID.NOVIDEO
        key = '%s_%s' % (api_key, vid)
        return key

    @staticmethod
    def is_no_video(internal_vid):
        '''Returns true if this video id refers to there not being a video'''
        return internal_vid.partition('_')[2] == InternalVideoID.NOVIDEO

    @staticmethod
    def from_thumbnail_id(thumbnail_id):
        '''Extracts the video id from a thumbnail id'''
        return thumbnail_id.rpartition('_')[0]

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
        return str(abs(binascii.crc32(_input)))

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
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return TrackerAccountIDMapper.__name__
    
    @classmethod
    def get_neon_account_id(cls, tai, callback=None):
        '''
        returns tuple of api_key, type(staging/production)
        '''
        def format_tuple(result):
            ''' format result tuple '''
            if result:
                return result.value, result.itype

        if callback:
            cls.get(tai, lambda x: callback(format_tuple(x)))
        else:
            return format_tuple(cls.get(tai))

class User(NamespacedStoredObject): 
    ''' User 
    
    These are users that can used across multiple systems most notably 
    the API and the current UI. 

    Each of these can be attached to a NeonUserAccount (misnamed, but this 
    is our Application/Customer layer). This will grant the User access to 
    anything the NeonUserAccount can access.  
        
    Users can be associated to many NeonUserAccounts     
    ''' 
    def __init__(self, 
                 username, 
                 password='password', 
                 access_level=AccessLevels.ALL_NORMAL_RIGHTS, 
                 first_name=None,
                 last_name=None,
                 title=None,
                 reset_password_token=None, 
                 secondary_email=None, 
                 cell_phone_number=None, 
                 send_emails=True,
                 email_verified=None):
 
        super(User, self).__init__(username)

        # here for the conversion to postgres, not used yet  
        self.user_id = uuid.uuid1().hex

        # the users username, chosen by them, email is required 
        # on the frontend 
        self.username = username.lower()

        # the users password_hash, we don't store plain text passwords 
        # This is slow to compute, so if it's the default, then speed it up
        # TODO: If a password isn't supplied, do not store a hash. 
        # It's a security risk
        rounds = 50000 if password == 'password' else None
        self.password_hash = sha256_crypt.encrypt(password, rounds=rounds)

        # short-lived JWtoken that will give user access to API calls 
        self.access_token = None

        # longer-lived JWtoken that will allow a user to refresh a token
        # this token should only be sent over HTTPS to the auth endpoints
        # for now this is not encrypted 
        self.refresh_token = None

        # access level granted to this user, uses class AccessLevels 
        self.access_level = access_level

        # the first name of the user 
        self.first_name = first_name 
 
        # the last name of the user 
        self.last_name = last_name 
 
        # the title of the user 
        self.title = title 

        # short lived JWT that is utilized in resetting passwords
        self.reset_password_token = reset_password_token

        # optional email, for users with non-email based usernames 
        # also for users that may want a secondary form of being reached
        self.secondary_email = secondary_email
 
        # optional cell phone number, can be used for recovery purposes 
        # eventually 
        self.cell_phone_number = cell_phone_number

        # whether or not we should send this user emails 
        self.send_emails = send_emails 

        # If the user has verified their email address.
        # If set to None, assume the address is verified to support legacy.
        self.email_verified = email_verified

    def is_email_verified(self):
        return self.email_verified is not False

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_associated_account_ids(self):
        results = yield self.execute_select_query(self.get_select_query(
                        [ "_data->>'neon_api_key'" ],
                        "_data->'users' ? %s",
                        table_name='neonuseraccount'),
                    wc_params=[self.username])

        rv = [i[0] for i in results]
        raise tornado.gen.Return(rv)

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return 'users' 
        
class NeonUserAccount(NamespacedStoredObject):
    ''' NeonUserAccount

    Every user in the system has a neon account and all other integrations are 
    associated with this account. 

    @videos: video id / jobid map of requests made directly through neon api
    @integrations: all the integrations associated with this acccount

    '''
    def __init__(self, 
                 a_id, 
                 api_key=None, 
                 default_size=(DefaultSizes.WIDTH,DefaultSizes.HEIGHT), 
                 name=None, 
                 abtest=True, 
                 serving_enabled=True, 
                 serving_controller=ServingControllerType.IMAGEPLATFORM, 
                 users=None, 
                 email=None, 
                 subscription_information=None, 
                 verify_subscription_expiry=datetime.datetime(1970,1,1), 
                 billed_elsewhere=True, 
                 billing_provider_ref=None,
                 processing_priority=2,
                 callback_states_ignored=None):

        # Account id chosen/or generated by the api when account is created 
        self.account_id = a_id 
        splits = '_'.split(a_id)
        if api_key is None and len(splits) == 2:
            api_key = splits[1]
        self.neon_api_key = self.get_api_key() if api_key is None else api_key
        super(NeonUserAccount, self).__init__(self.neon_api_key)
        self.tracker_account_id = TrackerAccountID.generate(self.neon_api_key)
        self.staging_tracker_account_id = \
                TrackerAccountID.generate(self.neon_api_key + "staging") 
        self.videos = {} #phase out,should be stored in neon integration
        # a mapping from integration id -> get_ovp() string
        self.integrations = {}
        # name of the individual who owns the account, mainly for internal use 
        # so we know who it is 
        self.name = name

        # The default thumbnail (w, h) to serve for this account
        self.default_size = default_size
        
        # Priority Q number for processing, currently supports {0,1}
        self.processing_priority = processing_priority

        # Default thumbnail to show if we don't have one for a video
        # under this account.
        self.default_thumbnail_id = None
         
        # create on account creation this gives access to the API, 
        # passed via header
        self.api_v2_key = NeonApiKey.id_generator()
        
        # Boolean on wether AB tests can run
        self.abtest = abtest

        # Will thumbnails be served by our system?
        self.serving_enabled = serving_enabled

        # What controller is used to serve the image? 
        # Default to imageplatform
        self.serving_controller = serving_controller

        # What users are privy to the information assoicated to this 
        # NeonUserAccount simply a list of usernames 
        self.users = users or [] 

        # email address associated with this account 
        self.email = email

        # most recent subscription from stripe
        self.subscription_information = subscription_information

        # we want to cache some information on subscription info, 
        # this is when we should next check the service for updates 
        # to the subscription
        self.verify_subscription_expiry = verify_subscription_expiry.strftime(
            "%Y-%m-%d %H:%M:%S.%f")

        # this relates to our billing provider, we default this to True, 
        # but all new accounts that need to be billed through our provider 
        # should set this to False 
        self.billed_elsewhere = billed_elsewhere

        # the key on the billing site that we need to get information 
        # about this customer 
        self.billing_provider_ref = billing_provider_ref

        # List of CallbackStates where we will not send a callback
        # Needed for backwards compatibility
        self.callback_states_ignored = callback_states_ignored or []
    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return NeonUserAccount.__name__

    def get_api_key(self):
        '''
        Get the API key for the account, If already in the DB the generate method
        returns it
        '''
        # Note: On DB retrieval the object gets created again, this may lead to
        # creation of an addional api key mapping ; hence prevent it
        # Figure out a cleaner implementation
        try:
            return self.neon_api_key
        except AttributeError:
            if NeonUserAccount.__name__.lower() not in self.account_id:
                return NeonApiKey.generate(self.account_id) 
            return 'None'

    def get_processing_priority(self):
        return self.processing_priority

    def set_processing_priority(self, p):
        self.processing_priority = p

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
        #TODO: Add Ooyala when necessary
         
        for plat in [NeonPlatform, BrightcovePlatform, 
                     YoutubePlatform, BrightcoveIntegration, OoyalaIntegration]:
            ovp_map[plat.get_ovp()] = plat

        calls = []
        for integration_id, ovp_string in self.integrations.iteritems():
            try:
                plat_type = ovp_map[ovp_string]
                if plat_type == NeonPlatform:
                    calls.append(tornado.gen.Task(plat_type.get,
                                                  self.neon_api_key, '0'))
                else:
                    calls.append(tornado.gen.Task(plat_type.get,
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

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_integrations(self):
        rv = []

        # due to old data, these could either have account_id or api_key
        # as account_id
        results = yield self.execute_select_query(self.get_select_query(
                        [ "_data",
                          "_type",
                          "created_time AS created_time_pg",
                          "updated_time AS updated_time_pg"],
                        "_data->>'account_id' IN(%s, %s)",
                        table_name='abstractintegration',
                        group_clause = "ORDER BY _type"),
                    wc_params=[self.neon_api_key, self.account_id],
                    cursor_factory=psycopg2.extras.RealDictCursor)

        for result in results:
            obj = self._create(result['_data']['key'], result)
            rv.append(obj)

        raise tornado.gen.Return(rv)

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

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def add_default_thumbnail(self, image, integration_id='0', replace=False):
        '''Adds a default thumbnail to the account.

        Note that the NeonUserAccount object is saved after this change.

        Inputs:
        image - A PIL image that will be added as the default thumb
        integration_id - Used to specify the CDN hosting parameters.
                         Defaults to the one associated with the NeonPlatform
        replace - If true, then will replace the existing default thumb
        '''
        if self.default_thumbnail_id is not None and not replace:
            raise ValueError('The account %s already has a default thumbnail'
                             % self.neon_api_key)

        cur_rank = 0
        if self.default_thumbnail_id is not None:
            old_default = yield tornado.gen.Task(ThumbnailMetadata.get,
                                                 self.default_thumbnail_id)
            if old_default is None:
                raise ValueError('The old thumbnail is not in the database. '
                                 'This should never happen')
            cur_rank = old_default.rank - 1

        cdn_key = CDNHostingMetadataList.create_key(self.neon_api_key,
                                                    integration_id)
        cdn_metadata = yield tornado.gen.Task(
            CDNHostingMetadataList.get,
            cdn_key)

        tmeta = ThumbnailMetadata(
            None,
            InternalVideoID.generate(self.neon_api_key, None),
            ttype=ThumbnailType.DEFAULT,
            rank=cur_rank)
        yield tmeta.add_image_data(image, cdn_metadata=cdn_metadata, async=True)
        self.default_thumbnail_id = tmeta.key
        
        success = yield tornado.gen.Task(tmeta.save)
        if not success:
            raise IOError("Could not save thumbnail")

        success = yield tornado.gen.Task(self.save)
        if not success:
            raise IOError("Could not save account data with new default thumb")

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
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_accounts(cls):
        ''' Get all NeonUserAccount instances '''
        retval = yield tornado.gen.Task(cls.get_all)
        raise tornado.gen.Return(retval)
    
    @classmethod
    def get_neon_publisher_id(cls, api_key):
        '''
        Get Neon publisher ID; This is also the Tracker Account ID
        '''
        na = cls.get(api_key)
        if nc:
            return na.tracker_account_id

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_internal_video_ids(self, since=None):
        '''Return the list of internal videos ids for this account.

           Orders by updated_time ASC 
        '''
        if since is None: 
            since = '1969-01-01'
 
        rv = True  
        db = PostgresDB()
        conn = yield db.get_connection()
        # right now, we are gonna do this with a LIKE query on the 
        # indexed key field, however, as data grows it may become 
        # necessary to store account_id/api_key on the object or table : 
        # index that, and query based on that
        query = "SELECT _data->>'key' FROM " + \
                VideoMetadata._baseclass_name().lower() + \
                " WHERE _data->>'key' LIKE %s AND updated_time > %s"\
                " ORDER BY updated_time ASC"
        # what a mess...escaping 'hack' 
        params = [self.neon_api_key+'%', since]
        cursor = yield conn.execute(
            query, 
            params, 
            cursor_factory=psycopg2.extensions.cursor)

        rv = [i[0] for i in cursor.fetchall()]

        db.return_connection(conn)
        raise tornado.gen.Return(rv) 

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def iterate_all_videos(self, max_request_size=100):
        '''Returns an iterator across all the videos for this account in the
        database.

        The iterator can be used asynchronously. See StoredObjectIterator

        The set of keys to grab happens once so if the db changes while
        the iteration is going, so neither new or deleted objects will
        be returned.

        Inputs:
        max_request_size - Maximum number of objects to request from
        the database at a time.
        '''
        vids = yield self.get_internal_video_ids(async=True)
        raise tornado.gen.Return(
            StoredObjectIterator(VideoMetadata, vids,
                                 page_size=max_request_size,
                                 skip_missing=True))

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_job_keys(self):
        '''Return a list of (job_id, api_key) of all the jobs for this account.
        '''
        db = PostgresDB()
        conn = yield db.get_connection()
        base_class_name = NeonApiRequest._baseclass_name().lower()

        query = "SELECT _data->>'key' FROM " + base_class_name + \
                " WHERE _data->>'key' LIKE %s"
 
        params = [base_class_name+'_'+self.neon_api_key+'_%']
        cursor = yield conn.execute(query, params, cursor_factory=psycopg2.extensions.cursor)
        tuple_to_list = [i[0] for i in cursor.fetchall()]
        db.return_connection(conn)

        raise tornado.gen.Return([x.split('_')[:0:-1] for x in tuple_to_list])

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def iterate_all_jobs(self, max_request_size=100):
        '''Returns an iterator across all the jobs for this account in the
        database.

        The iterator can be used asynchronously. See StoredObjectIterator

        The set of keys to grab happens once so if the db changes while
        the iteration is going, so neither new or deleted objects will
        be returned.

        Inputs:
        max_request_size - Maximum number of objects to request from
        the database at a time.
        '''
        keys = yield self.get_all_job_keys(async=True)
        raise tornado.gen.Return(
            StoredObjectIterator(NeonApiRequest, keys,
                                 page_size=max_request_size,
                                 skip_missing=True))

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_videos_and_statuses(self):
        ''' 
            the join on this is slow in the database 
              perform via two separate queries 
        '''  
        db = PostgresDB()
        conn = yield db.get_connection()
        video_to_status_dict = {}
        query = "SELECT video_id, serving_enabled, video_status_data, video_status_type FROM (" \
                "  SELECT v._data->>'key' AS video_id, v._data->>'serving_enabled' AS serving_enabled" \
                "   FROM videometadata v" \
                "   WHERE v._data->>'key'  LIKE %s" \
                "  ) q1 LEFT JOIN LATERAL ( " \
                "    SELECT vs._data AS video_status_data, vs._type AS video_status_type " \
                "      FROM videostatus vs " \
                "      WHERE replace(vs._data->>'key', 'videostatus_', '') = q1.video_id " \
                "  ) q2 ON TRUE"
        params = [self.neon_api_key+'_%']
        cursor = yield conn.execute(
                    query, 
                    params, 
                    cursor_factory=psycopg2.extensions.cursor)
        for res in cursor.fetchall():
            try: 
                obj_dict = {} 
                obj_dict['_data'] = res[2]
                key = obj_dict['_data']['key']  
                obj_dict['_type'] = res[3] 
                video_to_status_dict[res[0]] = {
                    'serving_enabled' : res[1], 
                    'video_status_obj' : self._create(key, obj_dict), 
                    'thumbnail_status_list' : [] 
                }
            except (TypeError, KeyError): 
                pass  
        
        query = "SELECT _data, _type FROM thumbnailstatus " \
                " WHERE replace(_data->>'key', 'thumbnailstatus_', '') IN( "\
                "  SELECT jsonb_array_elements_text(v._data->'thumbnail_ids') " \
                "   FROM videometadata v " \
                "   WHERE v._data->>'key' LIKE %s)"
 
        params = [self.neon_api_key+'_%']
        cursor = yield conn.execute(
                    query, 
                    params) 

        for res in cursor.fetchall():
            ts = self._create(res['_data']['key'], res) 
            video_id = ts.get_video_id()
            try:  
                video_to_status_dict[video_id]['thumbnail_status_list'].append(ts) 
            except KeyError as e: 
                _log.error('video_id %s was not in the dictionary : %s' % (video_id, e))
                continue 
            
        db.return_connection(conn)
        raise tornado.gen.Return(video_to_status_dict)
        
# define a ProcessingStrategy, that will dictate the behavior of the model.
class ProcessingStrategy(DefaultedStoredObject):
    '''
    Defines the model parameters with which a client wishes their data to be
    analyzed.

    NOTE: The majority of these parameters share their names with the
    parameters that are used to initialize local_video_searcher. For any
    parameter for which this is the case, see local_video_searcher.py for
    more elaborate documentation.
    '''
    def __init__(self, account_id, processing_time_ratio=2.0,
                 clip_processing_time_ratio=1.2,
                 local_search_width=32, local_search_step=4, n_thumbs=5,
                 feat_score_weight=2.0, mixing_samples=40, max_variety=True,
                 startend_clip=0.1, adapt_improve=True, analysis_crop=None,
                 filter_text=True, text_filter_params=None, 
                 filter_text_thresh=0.04, m_thumbs=6,
                 clip_cross_scene_boundary=True,
                 min_scene_piece=15, scene_threshold=30.0,
                 custom_predictor_weight=0.5,
                 custom_predictor=None):
        super(ProcessingStrategy, self).__init__(account_id)

        # The processing time ratio dictates the maximum amount of time the
        # video can spend in processing, which is given by:
        #
        # max_processing_time = (length of video in seconds * 
        #                        processing_time_ratio)
        self.processing_time_ratio = processing_time_ratio
        self.clip_processing_time_ratio = clip_processing_time_ratio

        # (this should rarely need to be changed)
        # Local search width is the size of the local search regions. If the
        # local_search_step is x, then for any frame which starts a local
        # search region i, the frames searched are given by
        # 
        # i : i + local_search_width in steps of x.
        self.local_search_width = local_search_width

        # (this should rarely need to be changed)
        # Local search step gives the step size between frames that undergo
        # analysis in a local search region. See the documentation for
        # local search width for the documentation.
        self.local_search_step = local_search_step

        # The number of top thumbs that are desired as output from the video
        # searching process.
        self.n_thumbs = n_thumbs

        # Likewise, the number of bottom thumbs
        self.m_thumbs = m_thumbs

        # (this should rarely need to be changed)
        # feat_score_weight is a multiplier that allows the feature score to
        # be combined with the valence score. This is given by:
        # 
        # combined score = (valence score) + 
        #                  (feat_score_weight * feature score)
        self.feat_score_weight = feat_score_weight

        # (this should rarely need to be changed)
        # Mixing samples is the number of initial samples to take to get
        # estimates for the running statistics.
        self.mixing_samples = mixing_samples

        # (this should rarely need to be changed)
        # max variety determines whether or not the model should pay attention
        # to the content of the images with respect to the variety of the top
        # thumbnails.
        self.max_variety = max_variety

        # startend clip determines how much of the video should be 'clipped'
        # prior to the analysis, to exclude things like titleframes and
        # credit rolls.
        self.startend_clip = startend_clip

        # adapt improve is a boolean that determines whether or not we should
        # be using CLAHE (contrast-limited adaptive histogram equalization) to
        # improve frames. 
        self.adapt_improve = adapt_improve

        # analysis crop dictates the region of the image that should be
        # excluded prior to the analysis. It can be expressed in three ways:
        #
        # All methods are performed by specifying floats x.
        #
        # Method one: A single float x, 0 < x <= 1.0
        #       - Takes the center (x*100)% of the image. For instance, if x 
        #         were 0.4, then 60% of the image's horizontal and vertical 
        #         would be removed (i.e., 30% off the left, 30% off the right, 
        #         30% off the top, 30% off the bottom). 
        # 
        # Method two: Two floats x y, both between 0 and 1.0 excluding 0.
        #       - Takes (1.0 - x)/2 off the top and (1.0 - x)/2 off the bottom
        #         and (1.0 -y)/2 off the left and (1.0 - y)/2 off the right.
        #
        # Method three: All sides are specified with four floats, clockwise 
        #         order from the top (top, right, bottom, left). Four floats, 
        #         as a list.
        #           NOTE:
        #         In contrast to the other methods, the floats specify how
        #         much to remove from each side (rather than how much to leave
        #         in). So they are all between 0 and 0.5 (although higher
        #         values are possible, they will no longer be with respect to
        #         the center of the image and the behavior can get wonkey). 
        #         Given x1, y1, x2, y2, crops (x1 * 100)% off the top, 
        #         (y1 * 100)% off the right, etc. 
        #         For example, to remove the bottom 1/3rd of an image, you
        #         would specify [0., 0., .3333, 0.]
        self.analysis_crop = analysis_crop

        # filter_text is a boolean indicating whether or not frames should
        # filtered on the basis of detected text.
        self.filter_text = filter_text

        # text_filter_params defines the 9 parameters required to
        # instantiate the text detector (in order):
        # classifier xml 1 
        #     - (str) The first level classifier filename. Must be
        #             located in options.text_model_path (see local search)
        # classifier xml 2 
        #     - (str) The second level classifier filename. Must be
        #             located in options.text_model_path (see local search)
        # threshold delta [def: 16]
        #     - (int) the number of steps for MSER 
        # min area [def: 0.00015]
        #     - (float) minimum ratio of the detection area to the
        #     total area of the image for acceptance as a text region.
        # max area [def: 0.003]
        #     - (float) maximum ratio of the detection area to the
        #     total area of the image for acceptance as a text region.
        # min probability, step 1 [def: 0.8]
        #     - (float) minimum probability for step 1 to proceed.
        # non max suppression [def: True]
        #     - (bool) whether or not to use non max suppression.
        # min probability difference [def: 0.5]
        #     - (float) minimum probability difference for 
        #     classification to proceed.
        # min probability, step 2 [def: 0.9]
        #     - (float) minimum probability for step 2 to proceed.
        if text_filter_params is None:
            tcnm1 = 'trained_classifierNM1.xml'
            tcnm2 = 'trained_classifierNM2.xml'
            text_filter_params = [tcnm1, tcnm2, 16, 0.00015, 0.003, 0.8, 
                                  True, 0.5, 0.9]
        self.text_filter_params = text_filter_params

        # filter_text_thresh is the maximum allowable ratio of the area 
        # occupied by the bounding boxes of detected text to the area of
        # the entire image. If the ratio is greater than this, and
        # filter_text is true, the frame will be filtered.
        self.filter_text_thresh = filter_text_thresh

        # Can a clip cross a scene boundary?
        self.clip_cross_scene_boundary = clip_cross_scene_boundary

        # Minimum number of frames from a scene to grab when making clips
        self.min_scene_piece = min_scene_piece

        # Threshold for scene detection
        self.scene_threshold = scene_threshold

        # Name of the custom predictor. This must be a file in the
        # model_data directory. It must be an object that has a
        # predict() function, which, if given a valence feature
        # vector, predicts some kind of score. In other words, like a
        # scikit-learn object.
        self.custom_predictor = custom_predictor

        # The weight to assign to a custom predictor relative to the
        # other features
        self.custom_predictor_weight = custom_predictor_weight

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return ProcessingStrategy.__name__

class ExperimentStrategy(DefaultedStoredObject):
    '''Stores information about the experimental strategy to use.

    Keyed by account_id (aka api_key)
    '''
    SEQUENTIAL='sequential'
    MULTIARMED_BANDIT='multi_armed_bandit'
    
    def __init__(self, account_id, exp_frac=1.0,
                 holdback_frac=0.05,
                 min_conversion = 50,
                 min_impressions = 500,
                 frac_adjust_rate = 0.0,
                 only_exp_if_chosen=False,
                 always_show_baseline=True,
                 baseline_type=ThumbnailType.RANDOM,
                 chosen_thumb_overrides=False,
                 override_when_done=True,
                 experiment_type=SEQUENTIAL,
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

        # minimum combined conversion numbers before calling an experiment
        # complete
        self.min_conversion = min_conversion

        # minimum number of impressions on a single thumb to declare a winner
        self.min_impressions = min_impressions

        # Fraction adjusting power rate. When this number is 0, it is
        # equivalent to all the serving fractions being the same,
        # while if it is 1.0, the serving fraction will be controlled
        # by the strategy.
        self.frac_adjust_rate = frac_adjust_rate

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

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return ExperimentStrategy.__name__

class Feature(DefaultedStoredObject):
    def __init__(self, key, name=None, variance_explained=0.0):
        super(Feature, self).__init__(key)
        splits = self.get_id().split('_') 
        if self.get_id() and len(splits) != 2:
            raise ValueError('Invalid key %s. Must be generated using '
                             'create_key()' % self.get_id())

        # the 'real' name of the model this references 
        self.model_name = splits[0] 

        # where in the feature vector this is at 
        self.index = int(splits[1])

        # the 'human' name of the model 
        self.name = name 

        # a float explaining the variance of this feature 
        self.variance_explained = variance_explained 
        
    @classmethod
    def create_key(cls, model_name, index):
        '''Create a key for using in this table'''
        return '%s_%s' % (model_name, index)

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return Feature.__name__

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_by_model_name(cls, model_name): 
        rv = []

        results = yield cls.execute_select_query(cls.get_select_query(
                        [ "_data",
                          "_type",
                          "created_time AS created_time_pg",
                          "updated_time AS updated_time_pg"],
                        "_data->>'model_name' = %s",
                        table_name='feature'),
                    wc_params=[model_name],
                    cursor_factory=psycopg2.extras.RealDictCursor)

        rv = [ cls._create(r['_data']['key'], r) for r in results ]

        raise tornado.gen.Return(rv)

class CDNHostingMetadataList(DefaultedStoredObject):
    '''A list of CDNHostingMetadata objects.

    Keyed by (api_key, integration_id). Use the create_key method to
    generate it before calling a normal function like get().
    
    '''
    def __init__(self, key, cdns=None):
        super(CDNHostingMetadataList, self).__init__(key)
        if self.get_id() and len(self.get_id().split('_')) != 2:
            raise ValueError('Invalid key %s. Must be generated using '
                             'create_key()' % self.get_id())
        if cdns is None:
            self.cdns = [NeonCDNHostingMetadata()]
        else:
            self.cdns = cdns

    def __iter__(self):
        '''Iterate through the cdns.'''
        return [x for x in self.cdns if x is not None].__iter__()

    @classmethod
    def create_key(cls, api_key, integration_id):
        '''Create a key for using in this table'''
        return '%s_%s' % (api_key, integration_id)

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return CDNHostingMetadataList.__name__


class CDNHostingMetadata(UnsaveableStoredObject):
    '''
    Specify how to host the the images with one CDN platform.

    Currently on S3 hosting to customer bucket is well defined

    These objects are not stored directly in the database.  They are
    actually stored in CDNHostingMetadataLists. If you try to save
    them directly, you will get a NotImplementedError.
    ''' 
    
    def __init__(self, key=None, cdn_prefixes=None, resize=False, 
                 update_serving_urls=False,
                 rendition_sizes=None,
                 source_crop=None,
                 crop_with_saliency=True,
                 crop_with_face_detection=True,
                 crop_with_text_detection=True,
                 video_rendition_sizes=None):

        self.key = key

        # List of url prefixes to put in front of the path. If there
        # is no transport scheme, http:// will be added. Also, there
        # is no trailing slash
        cdn_prefixes = cdn_prefixes or []
        self.cdn_prefixes = map(CDNHostingMetadata._normalize_cdn_prefix,
                                cdn_prefixes)
        
        # If true, the images should be resized into all the desired
        # renditions.
        self.resize = resize

        # Should the images be added to ThumbnailServingURL object?
        self.update_serving_urls = update_serving_urls

        # source crop specifies the region of the image from which
        # the result will originate. It can be expressed in three ways:
        #
        # All methods are performed by specifying floats x.
        #
        # Method one: A single float x, 0 < x <= 1.0
        #       - Takes the center (x*100)% of the image. For instance, if x 
        #         were 0.4, then 60% of the image's horizontal and vertical 
        #         would be removed (i.e., 30% off the left, 30% off the right, 
        #         30% off the top, 30% off the bottom). 
        # 
        # Method two: Two floats x y, both between 0 and 1.0 excluding 0.
        #       - Takes (1.0 - x)/2 off the top and (1.0 - x)/2 off the bottom
        #         and (1.0 -y)/2 off the left and (1.0 - y)/2 off the right.
        #
        # Method three: All sides are specified with four floats, clockwise 
        #         order from the top (top, right, bottom, left). Four floats, 
        #         as a list.
        #           NOTE:
        #         In contrast to the other methods, the floats specify how
        #         much to remove from each side (rather than how much to leave
        #         in). So they are all between 0 and 0.5 (although higher
        #         values are possible, they will no longer be with respect to
        #         the center of the image and the behavior can get wonkey). 
        #         Given x1, y1, x2, y2, crops (x1 * 100)% off the top, 
        #         (y1 * 100)% off the right, etc. 
        #         For example, to remove the bottom 1/3rd of an image, you
        #         would specify [0., 0., .3333, 0.]
        self.source_crop = source_crop
        self.crop_with_saliency = crop_with_saliency
        self.crop_with_face_detection = crop_with_face_detection
        self.crop_with_text_detection = crop_with_text_detection

        # A list of image rendition sizes to generate if resize is
        # True. The list is of (w, h) tuples.
        self.rendition_sizes = rendition_sizes or [
            [100, 100],
            [120, 67],
            [120, 90],
            [160, 90],
            [160, 120],
            [210, 118],
            [320, 180],
            [320, 240],
            [350, 350],
            [480, 270],
            [480, 360],
            [640, 360],
            [640, 480],
            [800, 800],
            [875, 500],
            [1280, 720]]

        # @TODO configure clip render.
        # List of 4-element list [width, height, container_type, codec].
        self.video_rendition_sizes = video_rendition_sizes or [
            [160, 90, 'mp4', 'h264'],
            [320, 180, 'mp4', 'h264'],]

    @classmethod
    def _create(cls, key, obj_dict):
        obj = super(CDNHostingMetadata, cls)._create(key, obj_dict)

        # Normalize the CDN prefixes
        obj.cdn_prefixes = map(CDNHostingMetadata._normalize_cdn_prefix,
                               obj.cdn_prefixes)

        return obj

    @staticmethod
    def _normalize_cdn_prefix(prefix):
      '''Normalizes a cdn prefix so that it starts with a scheme and does
      not end with a slash.

      e.g. http://neon.com
           https://neon.com
      '''
      prefix_split = urlparse.urlparse(prefix, 'http')
      if prefix_split.netloc == '':
        path_split = prefix_split.path.partition('/')
        prefix_split = [x for x in prefix_split]
        prefix_split[1] = path_split[0]
        prefix_split[2] = path_split[1]
      scheme_added = urlparse.urlunparse(prefix_split)
      return scheme_added.strip('/')

    @staticmethod
    @tornado.gen.coroutine
    def get_by_video(video):
        cdn_key = CDNHostingMetadataList.create_key(
            video.get_account_id(),
            video.integration_id)
        cdn_metadata = yield CDNHostingMetadataList.get(cdn_key, async=True)
        # Default to hosting on the Neon CDN if we don't know about it
        raise tornado.gen.Return(cdn_metadata or [NeonCDNHostingMetadata()])


class S3CDNHostingMetadata(CDNHostingMetadata):
    '''
    If the images are to be uploaded to S3 bucket use this formatter

    '''
    def __init__(self, key=None, access_key=None, secret_key=None, 
                 bucket_name=None, cdn_prefixes=None, folder_prefix=None,
                 resize=False, update_serving_urls=False, do_salt=True,
                 make_tid_folders=False, rendition_sizes=None, policy=None,
                 source_crop=None, crop_with_saliency=True,
                 crop_with_face_detection=True,
                 crop_with_text_detection=True, video_rendition_sizes=None):
        '''
        Create the object
        '''
        super(S3CDNHostingMetadata, self).__init__(
            key, cdn_prefixes, resize, update_serving_urls, rendition_sizes, 
            source_crop, crop_with_saliency, crop_with_face_detection,
            crop_with_text_detection)
        self.access_key = access_key # S3 access key
        self.secret_key = secret_key # S3 secret access key
        self.bucket_name = bucket_name # S3 bucket to host in
        self.folder_prefix = folder_prefix # Folder prefix to host in

        # Add a random named directory between folder prefix and the 
        # image name? Useful for performance when serving.
        self.do_salt = do_salt

        # make folders for easy navigation. This puts the image in the
        # form <api_key>/<video_id>/<thumb_id>.jpg
        self.make_tid_folders = make_tid_folders

        # What aws policy should the images be uploaded with
        self.policy = policy

class NeonCDNHostingMetadata(S3CDNHostingMetadata):
    '''
    Hosting on S3 using the Neon keys.
    
    This default hosting just uses pure S3, no cloudfront.
    '''
    def __init__(self, key=None,
                 bucket_name='n3.neon-images.com',
                 cdn_prefixes=None,
                 folder_prefix=None,
                 resize=True,
                 update_serving_urls=True,
                 do_salt=True,
                 make_tid_folders=False,
                 rendition_sizes=None,
                 source_crop=None, crop_with_saliency=True,
                 crop_with_face_detection=True,
                 crop_with_text_detection=True,
                 video_rendition_sizes=None):
        super(NeonCDNHostingMetadata, self).__init__(
            key,
            bucket_name=bucket_name,
            cdn_prefixes=(cdn_prefixes or ['n3.neon-images.com']),
            folder_prefix=folder_prefix,
            resize=resize,
            update_serving_urls=update_serving_urls,
            do_salt=do_salt,
            make_tid_folders=make_tid_folders,
            rendition_sizes=rendition_sizes,
            video_rendition_sizes=video_rendition_sizes,
            policy='public-read',
            source_crop=source_crop,
            crop_with_saliency=crop_with_saliency,
            crop_with_face_detection=crop_with_face_detection,
            crop_with_text_detection=crop_with_text_detection)

class PrimaryNeonHostingMetadata(S3CDNHostingMetadata):
    '''
    Primary Neon S3 Hosting
    This is where the primary copy of the thumbnails are stored
    
    @make_tid_folders: If true, _ is replaced by '/' to create folder
    '''
    def __init__(self, key=None,
                 bucket_name='host-thumbnails',
                 folder_prefix=None,
                 source_crop=None, crop_with_saliency=True,
                 crop_with_face_detection=True,
                 crop_with_text_detection=True):
        super(PrimaryNeonHostingMetadata, self).__init__(
            key,
            bucket_name=bucket_name,
            folder_prefix=folder_prefix,
            resize=False,
            update_serving_urls=False,
            do_salt=False,
            make_tid_folders=True,
            policy='public-read',
            source_crop=source_crop,
            crop_with_saliency=crop_with_saliency,
            crop_with_face_detection=crop_with_face_detection,
            crop_with_text_detection=crop_with_text_detection)

class CloudinaryCDNHostingMetadata(CDNHostingMetadata):
    '''
    Cloudinary images
    '''

    def __init__(self, key=None,source_crop=None, crop_with_saliency=True,
            crop_with_face_detection=True,
            crop_with_text_detection=True):
        super(CloudinaryCDNHostingMetadata, self).__init__(
            key,
            resize=False,
            update_serving_urls=False,
            source_crop=source_crop,
            crop_with_saliency=crop_with_saliency,
            crop_with_face_detection=crop_with_face_detection,
            crop_with_text_detection=crop_with_text_detection)

class AkamaiCDNHostingMetadata(CDNHostingMetadata):
    '''
    Akamai Netstorage CDN Metadata
    '''

    def __init__(self, key=None, host=None, akamai_key=None, akamai_name=None,
                 folder_prefix=None, cdn_prefixes=None, rendition_sizes=None,
                 cpcode=None,source_crop=None, crop_with_saliency=True,
                 crop_with_face_detection=True,
                 crop_with_text_detection=True):
        super(AkamaiCDNHostingMetadata, self).__init__(
            key,
            cdn_prefixes=cdn_prefixes,
            resize=True,
            update_serving_urls=True,
            rendition_sizes=rendition_sizes,
            source_crop=source_crop,
            crop_with_saliency=crop_with_saliency,
            crop_with_face_detection=crop_with_face_detection,
            crop_with_text_detection=crop_with_text_detection)

        # Host for uploading to akamai. Can have http:// or not
        self.host = host

        # Parameters to talk to akamai
        self.akamai_key = akamai_key
        self.akamai_name = akamai_name

        # The folder prefix to prepend to where the file will be
        # stored and served from. Slashes at the beginning and end are
        # optional
        self.folder_prefix=folder_prefix

        # CPCode string for uploading to Akamai. Should be something
        # like 17645
        self.cpcode = cpcode

    @classmethod
    def _create(cls, key, obj_dict):
        obj = super(AkamaiCDNHostingMetadata, cls)._create(key, obj_dict)

        # An old object could have had a baseurl, which was smashed
        # together the folder prefix and cpcode. That was confusing,
        # but in case there's an old object around, fix it. Also, in
        # that case, the cdn_prefixes could have had the folder prefix
        # in them, so remove them.
        if hasattr(obj, 'baseurl'):
            split = obj.baseurl.strip('/').partition('/')
            obj.cpcode = split[0].strip('/')
            obj.folder_prefix = split[2].strip('/')
            obj.cdn_prefixes = [re.sub(obj.folder_prefix, '', x).strip('/')
                                for x in obj.cdn_prefixes]
            del obj.baseurl
        
        return obj

class AbstractIntegration(NamespacedStoredObject):
    ''' Abstract Integration class '''

    def __init__(self, integration_id=None, enabled=True, 
                       video_submit_retries=0):
        
        integration_id = integration_id or uuid.uuid4().hex
        super(AbstractIntegration, self).__init__(integration_id)
        self.integration_id = integration_id
        
        # should this integration be used 
        self.enabled = enabled
        
        # how many times have we tried to submit the current video
        self.video_submit_retries = video_submit_retries

    @classmethod
    def _baseclass_name(cls):
        return AbstractIntegration.__name__


# DEPRECATED use AbstractIntegration instead
class AbstractPlatform(NamespacedStoredObject):
    ''' Abstract Platform/ Integration class

    The ids for these objects are tuples of (type, api_key, i_id)
    type can be None, in which case it becomes cls._baseclass_name()
    '''

    def __init__(self, api_key, i_id=None, abtest=False, enabled=True, 
                serving_enabled=True,
                serving_controller=ServingControllerType.IMAGEPLATFORM):
        super(AbstractPlatform, self).__init__((None, api_key, i_id))
        self.neon_api_key = api_key 
        self.integration_id = i_id 
        self.videos = {} # External video id (Original Platform VID) => Job ID
        self.abtest = abtest # Boolean on wether AB tests can run
        self.enabled = enabled # Account enabled for auto processing of videos 

        # Will thumbnails be served by our system?
        self.serving_enabled = serving_enabled

        # What controller is used to serve the image? Default to imageplatform
        self.serving_controller = serving_controller 

    @classmethod
    def format_key(cls, key):
        if isinstance(key, basestring):
            # It's already the proper key
            return key

        if len(key) == 2:
            typ = None
            api_key, i_id = key
        else:
            typ, api_key, i_id = key
        if typ is None:
            typ = cls._baseclass_name().lower()
        api_splits = api_key.split('_')
        if len(api_splits) > 1:
            api_key, i_id = api_splits[1:]
        return '_'.join([typ, api_key, i_id])

    @classmethod
    def key2id(cls, key):
        '''Converts a key to an id'''
        return key.split('_')

    @classmethod
    def _baseclass_name(cls):
        return cls.__name__

    @classmethod
    def _set_keyname(cls):
        return 'objset:%s' % cls._baseclass_name()
   
    @classmethod
    def _create(cls, key, obj_dict):
        def __get_type(key):
            '''
            Get the platform type
            '''
            platform_type = key.split('_')[0]
            typemap = {
                'neonplatform' : NeonPlatform,
                'brightcoveplatform' : BrightcovePlatform,
                'ooyalaplatform' : OoyalaPlatform,
                'youtubeplatform' : YoutubePlatform
                }
            try:
                platform = typemap[platform_type]
                return platform.__name__
            except KeyError, e:
                _log.exception("Invalid Platform Object")
                raise ValueError() # is this the right exception to throw?

        if obj_dict:
            if not '_type' in obj_dict or not '_data' in obj_dict:
                obj_dict = {
                    '_type': __get_type(obj_dict['key']),
                    '_data': copy.deepcopy(obj_dict)
                }
            
            return super(AbstractPlatform, cls)._create(cls.format_key(key),
                                                        obj_dict)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def save(self, callback=None):
        raise NotImplementedError("To save this object use modify()")
        # since we need a default constructor with empty strings for the 
        # eval magic to work, check here to ensure apikey and i_id aren't empty
        # since the key is generated based on them
        if self.neon_api_key == '' or self.integration_id == '':
            raise Exception('Invalid initialization of AbstractPlatform or its\
                subclass object. api_key and i_id should not be empty')

        super(AbstractPlatform, self).save(callback)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get(cls, api_key, i_id):
        ''' get instance '''
        rv = yield super(AbstractPlatform, cls).get((None, api_key, i_id), async=True)
        raise tornado.gen.Return(rv) 
    
    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify(cls, api_key, i_id, func, create_missing=False):
        def _set_parameters(x):
            typ, api_key, i_id = x.get_id()
            x.neon_api_key = api_key
            x.integration_id = i_id
            func(x)
        rv = yield super(AbstractPlatform, cls).modify(
                (None, api_key, i_id),
                _set_parameters,
                create_missing=create_missing, 
                async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify_many(cls, keys, func, create_missing=False):
        def _set_parameters(objs):
            for x in objs.itervalues():
                typ, api_key, i_id = x.get_id()
                x.neon_api_key = api_key
                x.integration_id = i_id
            func(objs)

        rv = yield super(AbstractPlatform, cls).modify_many(
              keys, 
              _set_parameters,
              create_missing=create_missing, 
              async=True)
        raise tornado.gen.Return(rv)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(cls, api_key, i_id):
        rv = yield super(AbstractPlatform, cls).delete(
                         (None, api_key, i_id), async=True)
        raise tornado.gen.Return(rv)
        
    #@classmethod
    #@utils.sync.optional_sync
    #@tornado.gen.coroutine
    #def delete_many(cls, keys):
    #    rv = yield super(AbstractPlatform, cls).delete_many(
    #                     [cls._generate_subkey(api_key, i_id) for 
    #                     api_key, i_id in keys], async=True)
    #    raise tornado.gen.Return(rv)

    def to_json(self):
        ''' to json '''
        return json.dumps(self, default=lambda o: o.__dict__) 

    def add_video(self, vid, job_id):
        ''' external video id => job_id '''
        self.videos[str(vid)] = job_id

    def get_videos(self):
        ''' list of external video ids '''
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
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_keys(cls):
        '''The keys will be of the form (type, api_key, integration_id).'''
        
        neon_keys = yield NeonPlatform._get_all_keys_impl(async=True)
        bc_keys = yield BrightcovePlatform._get_all_keys_impl(async=True)
        oo_keys = yield OoyalaPlatform._get_all_keys_impl(async=True)
        yt_keys = yield YoutubePlatform._get_all_keys_impl(async=True)

        keys = neon_keys + bc_keys + oo_keys + yt_keys

        raise tornado.gen.Return(keys)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _get_all_keys_impl(cls):
        keys = yield super(AbstractPlatform, cls).get_all_keys(async=True)
        raise tornado.gen.Return([[cls._baseclass_name().lower()] + x.split('_') 
                                  for x in keys])

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def subscribe_to_changes(cls, func, pattern='*', get_object=True):
        yield [
            NeonPlatform.subscribe_to_changes(func, pattern, get_object,
                                              async=True),
            BrightcovePlatform.subscribe_to_changes(
                func, pattern, get_object, async=True),
            YoutubePlatform.subscribe_to_changes(
                func, pattern, get_object, async=True),
            OoyalaPlatform.subscribe_to_changes(
                func, pattern, get_object, async=True)]

    @classmethod
    @tornado.gen.coroutine
    def _subscribe_to_changes_impl(cls, func, pattern, get_object):
        yield super(AbstractPlatform, cls).subscribe_to_changes(
            func, pattern, get_object, async=True)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def unsubscribe_from_changes(cls, channel):
        yield [
            NeonPlatform.unsubscribe_from_changes(channel, async=True),
            BrightcovePlatform.unsubscribe_from_changes(channel, async=True),
            YoutubePlatform.unsubscribe_from_changes(channel, async=True),
            OoyalaPlatform.unsubscribe_from_changes(channel, async=True)]

    @classmethod
    @tornado.gen.coroutine
    def _unsubscribe_from_changes_impl(cls, channel):
        yield super(AbstractPlatform, cls).unsubscribe_from_changes(
            channel, async=True)

    @classmethod
    def format_subscribe_pattern(cls, pattern):
        return '%s_%s' % (cls._baseclass_name().lower(), pattern)
    

    @classmethod
    def _erase_all_data(cls):
        ''' erase all data ''' 
        db_connection = DBConnection.get(cls)
        db_connection.clear_db()
 

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_all_video_related_data(self, platform_vid,
            *args, **kwargs):
        '''
        Delete all data related to a given video

        request, vmdata, thumbs, thumb serving urls
        
        #NOTE: Don't you dare call this method unless you really want to 
        delete 
        '''
        
        do_you_want_to_delete = kwargs.get('really_delete_keys', False)
        if do_you_want_to_delete == False:
            return

        def _del_video(p_inst):
            try:
                p_inst.videos.pop(platform_vid)
            except KeyError, e:
                _log.error('no such video to delete')
                return
        
        i_vid = InternalVideoID.generate(self.neon_api_key, 
                                         platform_vid)
        vm = yield tornado.gen.Task(VideoMetadata.get, i_vid)
        # update platform instance
        yield self.modify(self.neon_api_key, '0', _del_video, async=True)

        yield VideoMetadata.delete_related_data(i_vid, async=True)
        
class NeonPlatform(AbstractPlatform):
    '''
    Neon Integration ; stores all info about calls via Neon API
    '''
    def __init__(self, api_key, a_id=None, abtest=False):
        # By default integration ID 0 represents 
        # Neon Platform Integration (access via neon api)
        
        super(NeonPlatform, self).__init__(api_key, '0', abtest)
        self.account_id = a_id
        self.neon_api_key = api_key 
   
    @classmethod
    def get_ovp(cls):
        ''' ovp string '''
        return "neon"

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_keys(cls):
        keys = yield cls._get_all_keys_impl(async=True)
        raise tornado.gen.Return(keys)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def subscribe_to_changes(cls, func, pattern='*', get_object=True):
        yield cls._subscribe_to_changes_impl(func, pattern, get_object)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def unsubscribe_from_changes(cls, channel):
        yield cls._unsubscribe_from_changes_impl(channel)

    @classmethod
    def _baseclass_name(cls):
        return AbstractPlatform.__name__ 

class BrightcoveIntegration(AbstractIntegration):
    ''' Brightcove Integration class '''

    REFERENCE_ID = '_reference_id'
    BRIGHTCOVE_ID = '_bc_id'

    def __init__(self, a_id='', p_id=None,
                 rtoken=None, wtoken=None,
                 last_process_date=None, abtest=False, callback_url=None,
                 uses_batch_provisioning=False,
                 id_field=BRIGHTCOVE_ID,
                 enabled=True,
                 serving_enabled=True,
                 oldest_video_allowed=None,
                 video_submit_retries=0,
                 application_client_id=None,
                 application_client_secret=None,
                 uses_bc_thumbnail_api=False,
                 uses_bc_videojs_player=False,
                 uses_bc_smart_player=False,
                 uses_bc_gallery=False):

        ''' On every request, the job id is saved '''

        super(BrightcoveIntegration, self).__init__(None, enabled)
        self.account_id = a_id
        self.publisher_id = p_id
        self.read_token = rtoken
        self.write_token = wtoken

        # Configure Brightcove OAuth2, if publisher uses this feature
        # In the Brightcove Cloud 
        self.application_client_id = application_client_id
        self.application_client_secret = application_client_secret

        #The publish date of the last processed video - UTC timestamp seconds
        self.last_process_date = last_process_date
        self.application_client_id = application_client_id
        self.application_client_secret = application_client_secret

        #The publish date of the last processed video - UTC timestamp seconds
        self.last_process_date = last_process_date
        self.linked_youtube_account = False
        self.account_created = time.time() #UTC timestamp of account creation
        self.rendition_frame_width = None #Resolution of video to process
        self.video_still_width = 480 #default brightcove still width
        # the ids of playlist to create video requests from
        self.playlist_feed_ids = []
        # the url that will be called when a video is finished processing
        self.callback_url = callback_url

        # Does the customer use batch provisioning (i.e. FTP
        # uploads). If so, we cannot rely on the last modified date of
        # videos. http://support.brightcove.com/en/video-cloud/docs/finding-videos-have-changed-media-api
        self.uses_batch_provisioning = uses_batch_provisioning

        # The more Neon knows about how the publisher's images are placed
        # on the page, the more accurately we can capture tracking info.
        # Does publisher use BC's CMS to manage their video thumbnails
        self.uses_bc_thumbnail_api = uses_bc_thumbnail_api
        # Does publisher use BC's player based on html5 library named video.js
        self.uses_bc_videojs_player = uses_bc_videojs_player
        # Does publisher use the older Flash-based player
        self.uses_bc_smart_player = uses_bc_smart_player
        # Does publisher use BC's gallery product to display many
        # videos on a page
        self.uses_bc_gallery = uses_bc_gallery

        # Which custom field to use for the video id. If it is
        # BrightcovePlatform.REFERENCE_ID, then the reference_id field
        # is used. If it is BRIGHTCOVE_ID, the 'id' field is used.
        self.id_field = id_field

        # A ISO date string of the oldest video publication date to
        # ingest even if is updated in Brightcove.
        self.oldest_video_allowed = oldest_video_allowed

        # Amount of times we have retried a video submit
        self.video_submit_retries = video_submit_retries


    @classmethod
    def get_ovp(cls):
        ''' return ovp name'''
        return "brightcove_integration"

    def get_api(self, video_server_uri=None):
        '''Return the Brightcove API object for this platform integration.'''
        return api.brightcove_api.BrightcoveApi(
            self.account_id, self.publisher_id, 
            self.read_token, self.write_token) 

    def set_rendition_frame_width(self, f_width):
        ''' Set framewidth of the video resolution to process '''
        self.rendition_frame_width = f_width

    def set_video_still_width(self, width):
        ''' Set framewidth of the video still to be used 
            when the still is updated in the brightcove account '''
        self.video_still_width = width

class CNNIntegration(AbstractIntegration):
    ''' CNN Integration class '''

    def __init__(self, 
                 account_id='',
                 api_key_ref='', 
                 enabled=True, 
                 last_process_date=None):  

        ''' On every successful processing, the last video processed date is saved '''

        super(CNNIntegration, self).__init__(None, enabled=enabled)
        # The publish date of the last video we looked at - ISO 8601
        self.last_process_date = last_process_date 
        # user.neon_api_key this integration belongs to 
        self.account_id = account_id
        # the api_key required to make requests to cnn api - external
        self.api_key_ref = api_key_ref

class FoxIntegration(AbstractIntegration):
    ''' Fox Integration class '''

    def __init__(self, 
                 account_id='',
                 feed_pid_ref='', 
                 enabled=True, 
                 last_process_date=None):  

        ''' On every successful processing, the last video processed date is saved '''

        super(FoxIntegration, self).__init__(None, enabled=enabled)
        # The publish date of the last video we looked at - ISO 8601
        self.last_process_date = last_process_date 
        # user.account_id this integration belongs to 
        self.account_id = account_id
        # the feed_pid_ref required by the fox api - external
        self.feed_pid_ref = feed_pid_ref

# DEPRECATED use BrightcoveIntegration instead 
class BrightcovePlatform(AbstractPlatform):
    ''' Brightcove Platform/ Integration class '''
    REFERENCE_ID = '_reference_id'
    BRIGHTCOVE_ID = '_bc_id'
    
    def __init__(self, api_key, i_id=None, a_id='', p_id=None, 
                rtoken=None, wtoken=None, auto_update=False,
                last_process_date=None, abtest=False, callback_url=None,
                uses_batch_provisioning=False,
                id_field=BRIGHTCOVE_ID,
                enabled=True,
                serving_enabled=True,
                oldest_video_allowed=None, 
                video_submit_retries=0):

        ''' On every request, the job id is saved '''

        super(BrightcovePlatform, self).__init__(api_key, i_id, abtest,
                                                 enabled, serving_enabled)
        self.account_id = a_id
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
        # the ids of playlist to create video requests from
        self.playlist_feed_ids = []
        # the url that will be called when a video is finished processing 
        self.callback_url = callback_url

        # Does the customer use batch provisioning (i.e. FTP
        # uploads). If so, we cannot rely on the last modified date of
        # videos. http://support.brightcove.com/en/video-cloud/docs/finding-videos-have-changed-media-api
        self.uses_batch_provisioning = uses_batch_provisioning

        # Which custom field to use for the video id. If it is
        # BrightcovePlatform.REFERENCE_ID, then the reference_id field
        # is used. If it is BRIGHTCOVE_ID, the 'id' field is used.
        self.id_field = id_field

        # A ISO date string of the oldest video publication date to
        # ingest even if is updated in Brightcove.
        self.oldest_video_allowed = oldest_video_allowed

        # Amount of times we have retried a video submit 
        self.video_submit_retries = video_submit_retries 

    @classmethod
    def get_ovp(cls):
        ''' return ovp name'''
        return "brightcove"

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def subscribe_to_changes(cls, func, pattern='*', get_object=True):
        yield cls._subscribe_to_changes_impl(func, pattern, get_object)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def unsubscribe_from_changes(cls, channel):
        yield cls._unsubscribe_from_changes_impl(channel)

    def get_api(self, video_server_uri=None):
        '''Return the Brightcove API object for this platform integration.'''
        return api.brightcove_api.BrightcoveApi(
            self.neon_api_key, self.publisher_id,
            self.read_token, self.write_token)

    def set_rendition_frame_width(self, f_width):
        ''' Set framewidth of the video resolution to process '''
        self.rendition_frame_width = f_width

    def set_video_still_width(self, width):
        ''' Set framewidth of the video still to be used 
            when the still is updated in the brightcove account '''
        self.video_still_width = width

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_keys(cls):
        keys = yield cls._get_all_keys_impl(async=True)
        raise tornado.gen.Return(keys)

    @classmethod
    def _baseclass_name(cls):
        return AbstractPlatform.__name__ 

class YoutubePlatform(AbstractPlatform):
    ''' Youtube platform integration '''

    # TODO(Sunil): Fix this class when Youtube is implemented 

    def __init__(self, api_key, i_id=None, a_id='', access_token=None,
                 refresh_token=None,
                expires=None, auto_update=False, abtest=False):
        super(YoutubePlatform, self).__init__(api_key, i_id, abtest)
        self.account_id = a_id
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

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def subscribe_to_changes(cls, func, pattern='*', get_object=True):
        yield cls._subscribe_to_changes_impl(func, pattern, get_object)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def unsubscribe_from_changes(cls, channel):
        yield cls._unsubscribe_from_changes_impl(channel)
    
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

    def create_job(self):
        '''
        Create youtube api request
        '''
        pass

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_keys(cls):
        keys = yield cls._get_all_keys_impl(async=True)
        raise tornado.gen.Return(keys)

    @classmethod
    def _baseclass_name(cls):
        return AbstractPlatform.__name__ 

class OoyalaIntegration(AbstractIntegration):
    '''
    OOYALA Integration
    '''
    def __init__(self, 
                 a_id='',
                 p_code=None, 
                 api_key=None, 
                 api_secret=None): 
        '''
        Init ooyala platform 
        
        Partner code, o_api_key & api_secret are essential 
        for api calls to ooyala 

        '''
        super(OoyalaIntegration, self).__init__(None, True)
        self.account_id = a_id
        self.partner_code = p_code
        self.api_key = api_key
        self.api_secret = api_secret 
 
    @classmethod
    def get_ovp(cls):
        ''' return ovp name'''
        return "ooyala_integration"

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
    
# DEPRECATED use OoyalaIntegration instead 
class OoyalaPlatform(AbstractPlatform):
    '''
    OOYALA Platform
    '''
    def __init__(self, api_key, i_id=None, a_id='', p_code=None, 
                 o_api_key=None, api_secret=None, auto_update=False): 
        '''
        Init ooyala platform 
        
        Partner code, o_api_key & api_secret are essential 
        for api calls to ooyala 

        '''

        super(OoyalaPlatform, self).__init__(api_key, i_id)
 
        self.account_id = a_id
        self.partner_code = p_code
        self.ooyala_api_key = o_api_key
        self.api_secret = api_secret 
        self.auto_update = auto_update 
    
    @classmethod
    def get_ovp(cls):
        ''' return ovp name'''
        return "ooyala"

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def subscribe_to_changes(cls, func, pattern='*', get_object=True):
        yield cls._subscribe_to_changes_impl(func, pattern, get_object)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def unsubscribe_from_changes(cls, channel):
        yield cls._unsubscribe_from_changes_impl(channel)
    
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
    
    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_all_keys(cls):
        keys = yield cls._get_all_keys_impl(async=True)
        raise tornado.gen.Return(keys)

    @classmethod
    def _baseclass_name(cls):
        return AbstractPlatform.__name__ 

#######################
# Request Blobs 
######################

class RequestState(object):
    'Request state enumeration'

    UNKNOWN    = "unknown"
    SUBMIT     = "submit"
    PROCESSING = "processing"
    FINALIZING = "finalizing" # In the process of finalizing the request
    REQUEUED   = "requeued"
    FINISHED   = "finished"
    SERVING    = "serving" # Thumbnails are ready to be served 
    INT_ERROR  = "internal_error" # Neon had some code error
    CUSTOMER_ERROR = "customer_error" # customer request had a partial error 
    REPROCESS  = "reprocess" #new state added to support clean reprocessing

    # The following states are all DEPRECATED
    SERVING_AND_ACTIVE = "serving_active" # DEPRECATED    
    FAILED     = "failed" # DEPRECATED in favor of INT_ERROR, CUSTOMER_ERROR
    ACTIVE     = "active" # DEPRECATED. Thumbnail selected by editor; Only releavant to BC

class ExternalRequestState(object):
    '''State enums for the request state that will be sent to the user.'''
    UNKNOWN = 'unknown' # We don't know the state
    PROCESSING = 'processing' # The object is being analyzed
    PROCESSED = 'processed' # The object has been analyzed 
    SERVING = 'serving' # The object is available for serving at scale
    FAILED = 'failed' # There was an error processing the object

    @staticmethod
    def from_internal_state(state):
        '''Converts the internal state to an external one.'''
        state_map = {
            RequestState.SUBMIT : ExternalRequestState.PROCESSING,
            RequestState.PROCESSING : ExternalRequestState.PROCESSING,
            RequestState.FINALIZING : ExternalRequestState.PROCESSING,
            RequestState.REQUEUED : ExternalRequestState.PROCESSING,
            RequestState.REPROCESS : ExternalRequestState.PROCESSING,
            RequestState.FINISHED : ExternalRequestState.PROCESSED,
            RequestState.ACTIVE : ExternalRequestState.PROCESSED,
            RequestState.SERVING : ExternalRequestState.SERVING,
            RequestState.SERVING_AND_ACTIVE : ExternalRequestState.SERVING,
            RequestState.FAILED : ExternalRequestState.FAILED,
            RequestState.INT_ERROR : ExternalRequestState.FAILED,
            RequestState.CUSTOMER_ERROR : ExternalRequestState.FAILED}
        return state_map.get(state, ExternalRequestState.UNKNOWN)
            

class CallbackState(object):
    '''State enums for callbacks being sent.'''
    NOT_SENT = 'not_sent' # Callback has not been sent
    PROCESSED_SENT = 'processed_sent' # The processed callback has been sent
    SERVING_SENT = 'serving_sent' # The serving callback has been sent
    FAILED_SENT = 'failed_sent' # The failed callback has been sent
    WINNER_SENT = 'winner_sent' # The winner thumbnail callback has been sent
    UNKNOWN_SENT = 'unknown' # A callback with an unknown state was sent
    ERROR = 'error' # Error sending the callback

    # Deprecated
    SUCESS = 'sucess' # Callback was sent sucessfully

    @staticmethod
    def from_external_processing_state(state):
        state_map = {
            ExternalRequestState.PROCESSED : CallbackState.PROCESSED_SENT,
            ExternalRequestState.SERVING : CallbackState.SERVING_SENT,
            ExternalRequestState.FAILED : CallbackState.FAILED_SENT
            }
        try:
            return state_map[state]
        except KeyError:
            _log.warn('Unknown external processing state: %s' % state)
            return CallbackState.UNKNOWN_SENT
    

class NeonApiRequest(NamespacedStoredObject):
    '''
    Instance of this gets created during request creation
    (Neon web account, RSS Cron)
    Json representation of the class is saved in the server queue and redis  
    '''

    def __init__(self, job_id, api_key=None, vid=None, title=None, url=None, 
            request_type=None, http_callback=None, default_thumbnail=None,
            integration_type='neon', integration_id='0',
            external_thumbnail_id=None, publish_date=None,
            callback_state=CallbackState.NOT_SENT, 
            callback_email=None,
            age=None,
            gender=None, 
            result_type=None, 
            n_clips=None,
            clip_length=None):
        splits = job_id.split('_')
        if len(splits) == 3:
            # job id was given as the raw key
            job_id = splits[2]
            api_key = splits[1]
        super(NeonApiRequest, self).__init__(
            self._generate_subkey(job_id, api_key))
        self.job_id = job_id
        self.api_key = api_key 
        self.video_id = vid #external video_id
        self.video_title = title
        self.video_url = url
        self.request_type = request_type
        # The url to send the callback response
        self.callback_url = http_callback
        self.callback_state = callback_state
        self.state = RequestState.SUBMIT
        self.fail_count = 0 # Number of failed processing tries
        self.try_count = 0 # Number of attempted processing tries
        
        self.integration_type = integration_type
        self.integration_id = integration_id
        self.default_thumbnail = default_thumbnail # URL of a default thumb
        self.external_thumbnail_id = external_thumbnail_id

        # The job response. Should be a dictionary defined by 
        # VideoCallbackResponse
        self.response = {}

        # API Method
        self.api_method = None
        self.api_param  = None
        self.publish_date = publish_date # ISO date format of when video is published
       
        # field used to store error message on partial error, explict error or 
        # additional information about the request
        self.msg = None

        # what email address should we send this to, when done processing
        # this could be associated to an existing user(username), 
        # but that is not required 
        self.callback_email = callback_email 

        # Demographic parameters for the video processing
        self.age = age
        self.gender= gender

        # what result type (currently tnails or clips) we would like 
        self.result_type = result_type 

        # number of clips that would be desired if clips are chosen 
        self.n_clips = n_clips

        # desired length of clip in seconds
        self.clip_length = clip_length

    @classmethod
    def key2id(cls, key):
        '''Converts a key to an id'''
        splits = key.split('_')
        return (splits[2], splits[1])

    @classmethod
    def _generate_subkey(cls, job_id, api_key=None):
        if job_id.startswith('request'):
            # Is is really the full key, so just return the subportion
            return job_id.partition('_')[2]
        if job_id is None or api_key is None:
            return None
        return '_'.join([api_key, job_id])

    def _set_keyname(self):
        return '%s:%s' % (super(NeonApiRequest, self)._set_keyname(),
                          self.api_key)

    @classmethod
    def _baseclass_name(cls):
        # For backwards compatibility, we don't use the classname
        return 'request'

    @classmethod
    def _create(cls, key, obj_dict):
        '''Create the object.

        Needed for backwards compatibility for old style data that
        doesn't include the classname. Instead, request_type holds
        which class to create.
        '''
        if obj_dict:
            if not '_type' in obj_dict or not '_data' in obj_dict:
                # Old style object, so adjust the object dictionary
                typemap = {
                    'brightcove' : BrightcoveApiRequest,
                    'ooyala' : OoyalaApiRequest,
                    'youtube' : YoutubeApiRequest,
                    'neon' : NeonApiRequest,
                    None : NeonApiRequest
                    }
                obj_dict = {
                    '_type': typemap[obj_dict['request_type']].__name__,
                    '_data': copy.deepcopy(obj_dict)
                    }
            obj = super(NeonApiRequest, cls)._create(key, obj_dict)

            try:
                obj.publish_date = datetime.datetime.utcfromtimestamp(
                    obj.publish_date / 1000.)
                obj.publish_date = obj.publish_date.isoformat()
            except ValueError:
                pass
            except TypeError:
                pass
            return obj

    def get_default_thumbnail_type(self):
        '''Return the thumbnail type that should be used for a default 
        thumbnail in the request.
        '''
        return ThumbnailType.DEFAULT
  
    def set_api_method(self, method, param):
        ''' 'set api method and params ''' 
        
        self.api_method = method
        self.api_param  = param

        #TODO:validate supported methods

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get(cls, job_id, api_key, log_missing=True):
        ''' get instance '''
        rv = yield super(NeonApiRequest, cls).get(cls._generate_subkey(job_id, api_key),
                                                  log_missing=log_missing,
                                                  async=True) 
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_many(cls, keys, log_missing=True, func_level_wpg=True):
        '''Returns the list of objects from a list of keys.

        Each key must be a tuple of (job_id, api_key)
        '''
        rv = yield super(NeonApiRequest, cls).get_many(
                          [cls._generate_subkey(*k) for k in keys],
                          log_missing=log_missing,
                          func_level_wpg=func_level_wpg, 
                          async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    def get_all(cls):
        raise NotImplementedError()

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify(cls, job_id, api_key, func, create_missing=False, 
               callback=None):
        rv = yield super(NeonApiRequest, cls).modify(
                cls._generate_subkey(job_id, api_key),
                func,
                create_missing=create_missing,
                callback=callback, 
                async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def modify_many(cls, keys, func, create_missing=False, callback=None):
        '''Modify many keys.

        Each key must be a tuple of (job_id, api_key)
        '''
        rv = yield super(NeonApiRequest, cls).modify_many(
                    [cls._generate_subkey(*k) for k in keys],
                    func,
                    create_missing=create_missing,
                    callback=callback, 
                    async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(cls, job_id, api_key, callback=None):
        rv = yield super(NeonApiRequest, cls).delete(
                cls._generate_subkey(job_id, api_key),
                callback=callback, 
                async=True)
        raise tornado.gen.Return(rv) 

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_many(cls, keys, callback=None):
        rv = yield super(NeonApiRequest, cls).delete_many(
                [cls._generate_subkey(job_id, api_key) for 
                job_id, api_key in keys],
                callback=callback, 
                async=True)
        raise tornado.gen.Return(rv) 
    
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def save_default_thumbnail(self, cdn_metadata=None):
        '''Save the default thumbnail by attaching it to a video. The video
        metadata for this request must be in the database already.

        Inputs:
        cdn_metadata - If known, the metadata to save to the cdn.
                       Otherwise it will be looked up.
        '''
        try:
            thumb_url = self.default_thumbnail
        except AttributeError:
            thumb_url = None

        if not thumb_url:
            # No default thumb to upload
            return

        thumb_type = self.get_default_thumbnail_type()

        # Check to see if there is already a thumbnail that the system
        # knows about (and thus was already uploaded)
        
        video = yield tornado.gen.Task(
            VideoMetadata.get,
            InternalVideoID.generate(self.api_key,
                                     self.video_id))
        if video is None:
            msg = ('VideoMetadata for job %s is missing. '
                   'Cannot add thumbnail' % self.job_id)
            _log.error(msg)
            raise DBStateError(msg)

        known_thumbs = yield tornado.gen.Task(
            ThumbnailMetadata.get_many,
            video.thumbnail_ids)
        min_rank = 1
        for thumb in known_thumbs:
            if thumb.type == thumb_type:
                if thumb_url in thumb.urls:
                    # The exact thumbnail is already there
                    raise tornado.gen.Return(thumb)
            
                if thumb.rank < min_rank:
                    min_rank = thumb.rank
        cur_rank = min_rank - 1

        # Upload the new thumbnail
        meta = ThumbnailMetadata(
            None,
            ttype=thumb_type,
            rank=cur_rank,
            external_id=self.external_thumbnail_id)
        thumb = yield video.download_and_add_thumbnail(meta,
                                               thumb_url,
                                               cdn_metadata,
                                               save_objects=True,
                                               async=True)
        raise tornado.gen.Return(thumb) 
        # Push a thumbnail serving directive to Kinesis so that it can
        # be served quickly.

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def send_callback(self, send_kwargs=None):
        '''Sends the callback to the customer if necessary.

        Inputs:
        send_kwargs - Keyword arguments to utils.http.send_request for when
                      sending the callback
        '''
        if not options.send_callbacks:
            return
        new_callback_state = CallbackState.NOT_SENT
        response = None

        def _get_successful_state(response):
            if response.experiment_state in [ExperimentState.COMPLETE,
                                             ExperimentState.OVERRIDE]:
                return CallbackState.WINNER_SENT
            else:
                return CallbackState.from_external_processing_state(
                    response.processing_state)
                
        if self.callback_url:
            # Check the callback url format
            parsed = urlparse.urlsplit(self.callback_url)
            if parsed.scheme not in ('http', 'https'):
                _log.error_n('Invalid callback url job %s acct %s: %s'
                             % (self.job_id, self.api_key, self.callback_url))
                statemon.state.increment('invalid_callback_url')
                new_callback_state = CallbackState.ERROR
            else:

                # Build the response
                response = VideoCallbackResponse.create_from_dict(
                    self.response)

                internal_vid = InternalVideoID.generate(self.api_key,
                                                        self.video_id)
                vstatus = yield VideoStatus.get(internal_vid, async=True)
                response.experiment_state = vstatus.experiment_state
                response.winner_thumbnail = vstatus.winner_tid
                response.set_processing_state(self.state)
                if response.processing_state in [
                        ExternalRequestState.PROCESSED,
                        ExternalRequestState.SERVING]:
                    response.error = None
                    vidmeta = yield VideoMetadata.get(internal_vid,
                                                      async=True)
                    thumbs = yield ThumbnailMetadata.get_many(
                        vidmeta.thumbnail_ids, async=True)
                    thumbs = [x for x in thumbs 
                              if x and x.type == ThumbnailType.NEON]
                    response.framenos = [x.frameno for x in thumbs]
                    response.thumbnails = [x.key for x in thumbs]
                response.job_id = self.job_id
                response.video_id = self.video_id

                # If we have sucessfully sent this callback already, we're done
                next_callback_state = _get_successful_state(response)
                if (self.callback_state == next_callback_state or
                    self.callback_state in [CallbackState.SUCESS,
                                            CallbackState.ERROR]):
                    return

                # Check to see if this account wants this callback
                acct = yield NeonUserAccount.get(self.api_key,
                                                 async=True)
                if acct is None:
                    raise DBStateError('Could not find account %s' % 
                                       self.api_key)
                
                if next_callback_state not in acct.callback_states_ignored:
            
                    # Send the callback
                    self.response = response.to_dict()
                    send_kwargs = send_kwargs or {}
                    cb_request = tornado.httpclient.HTTPRequest(
                        url=self.callback_url,
                        method='PUT',
                        headers={'content-type' : 'application/json'},
                        body=response.to_json(),
                        request_timeout=20.0,
                        connect_timeout=10.0)
                    cb_response = yield utils.http.send_request(
                        cb_request,
                        no_retry_codes=[405],
                        async=True,
                        **send_kwargs)
                    if cb_response.error:
                        # Now try a POST for backwards compatibility
                        cb_request.method='POST'
                        cb_response = yield utils.http.send_request(
                            cb_request,
                            async=True,
                            **send_kwargs)
                        if cb_response.error:
                            statemon.state.define_and_increment(
                                'callback_error.%s' % self.api_key)

                            statemon.state.increment('callback_error')
                            _log.warn('Error when sending callback to %s for '
                                      'video %s: %s' %
                                      (self.callback_url, self.video_id,
                                       cb_response.error))
                            new_callback_state = CallbackState.ERROR
                        else:
                           statemon.state.increment('sucessful_callbacks')
                           new_callback_state = _get_successful_state(response)
                    else:
                        statemon.state.increment('sucessful_callbacks')
                        new_callback_state = _get_successful_state(response)

                else:
                    new_callback_state = next_callback_state

            # Modify the database state
            def _mod_obj(x):
                x.callback_state = new_callback_state
                if response:
                    x.response = response.to_dict()
            yield tornado.gen.Task(self.modify, self.job_id, self.api_key,
                                   _mod_obj)

class BrightcovePlayer(NamespacedStoredObject):
    '''
    Brightcove Player model
    '''
    def __init__(self, player_ref, integration_id=None,
                 name=None, is_tracked=None, publish_date=None,
                 published_plugin_version=None, last_attempt_result=None):

        super(BrightcovePlayer, self).__init__(player_ref)

        # The Neon integration that has this player
        self.integration_id = integration_id
        # Set if publisher needs the Neon event tracking plugin published to this
        self.is_tracked = is_tracked
        # Descriptive name of the player
        self.name = name

        # Properties to track publishing:
        self.publish_date = publish_date
        # Version is an increasing integer
        self.published_plugin_version = published_plugin_version
        # Descriptive string of last failed attempt to publish.
        # Set to None when last attempt was successful
        self.last_attempt_result = last_attempt_result

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_players(cls, integration_id):
        '''Get all players associated to the integration'''

        rv = []
        results = yield self.execute_select_query(self.get_select_query(
                        [ "_data",
                          "_type",
                          "created_time AS created_time_pg",
                          "updated_time AS updated_time_pg"],
                        "_data->>'integration_id' = '%s' ",
                        table_name='brightcoveplayer'),
                    wc_params=[integration_id])
        for result in results:
            player = self._create(result['_data']['key'], result)
            rv.append(player)
        raise tornado.gen.Return(rv)

    @classmethod
    def _baseclass_name(cls):
        return BrightcovePlayer.__name__


class BrightcoveApiRequest(NeonApiRequest):
    '''
    Brightcove API Request class
    '''
    def __init__(self, job_id, api_key=None, vid=None, title=None, url=None,
                 rtoken=None, wtoken=None, pid=None, http_callback=None,
                 i_id=None, default_thumbnail=None):
        super(BrightcoveApiRequest,self).__init__(
            job_id, api_key, vid, title, url,
            request_type='brightcove',
            http_callback=http_callback,
            default_thumbnail=default_thumbnail)
        self.read_token = rtoken
        self.write_token = wtoken
        self.publisher_id = pid
        self.integration_id = i_id 
        self.autosync = False
     
    def get_default_thumbnail_type(self):
        '''Return the thumbnail type that should be used for a default 
        thumbnail in the request.
        '''
        return ThumbnailType.BRIGHTCOVE

class OoyalaApiRequest(NeonApiRequest):
    '''
    Ooyala API Request class
    '''
    def __init__(self, job_id, api_key=None, i_id=None, vid=None, title=None,
                 url=None, oo_api_key=None, oo_secret_key=None,
                 http_callback=None, default_thumbnail=None):
        super(OoyalaApiRequest, self).__init__(
            job_id, api_key, vid, title, url,
            request_type='ooyala',
            http_callback=http_callback,
            default_thumbnail=default_thumbnail)
        self.oo_api_key = oo_api_key
        self.oo_secret_key = oo_secret_key
        self.integration_id = i_id 
        self.autosync = False

    def get_default_thumbnail_type(self):
        '''Return the thumbnail type that should be used for a default 
        thumbnail in the request.
        '''
        return ThumbnailType.OOYALA

class YoutubeApiRequest(NeonApiRequest):
    '''
    Youtube API Request class
    '''
    def __init__(self, job_id, api_key=None, vid=None, title=None, url=None,
                 access_token=None, refresh_token=None, expiry=None,
                 http_callback=None, default_thumbnail=None):
        super(YoutubeApiRequest,self).__init__(
            job_id, api_key, vid, title, url,
            request_type='youtube',
            http_callback=http_callback,
            default_thumbnail=default_thumbnail)
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.integration_type = "youtube"
        self.previous_thumbnail = None # TODO(Sunil): Remove this
        self.expiry = expiry

    def get_default_thumbnail_type(self):
        '''Return the thumbnail type that should be used for a default 
        thumbnail in the request.
        '''
        return ThumbnailType.YOUTUBE

###############################################################################
## Thumbnail store T_URL => TID => Metadata
############################################################################### 

class ThumbnailID(AbstractHashGenerator):
    '''
    Static class to generate thumbnail id

    _input: String or Image stream. 

    Thumbnail ID is: <internal_video_id>_<md5 MD5 hash of image data>
    '''
    VALID_REGEX = '%s_[0-9A-Za-z]+' % InternalVideoID.VALID_INTERNAL_REGEX

    @staticmethod
    def generate(_input, internal_video_id):
        return '%s_%s' % (internal_video_id, ThumbnailMD5.generate(_input))

    @classmethod
    def is_valid_key(cls, key):
        return len(key.split('_')) == 3
        

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

    or, instead of a full url map, there can be a base_url and a list of sizes.
    In that case, the full url would be generated by 
    <base_url>/FNAME_FORMAT % (thumbnail_id, width, height)
    '''    
    FNAME_FORMAT = "neontn%s_w%s_h%s.jpg"
    FNAME_REGEX = ('neontn(%s)_w([0-9]+)_h([0-9]+)\.jpg' % 
                   ThumbnailID.VALID_REGEX)

    def __init__(self, thumbnail_id, size_map=None, base_url=None, sizes=None):
        super(ThumbnailServingURLs, self).__init__(thumbnail_id)
        self.size_map = size_map or {}
        
        self.base_url = base_url
        self.sizes = sizes or set([]) # List of (width, height)

    def __eq__(self, other):
        '''Sets can't do cmp, so we need to overright so that == and != works.
        '''
        if ((other is None) or 
            (type(other) != type(self)) or 
            (self.__dict__.keys() != other.__dict__.keys())):
            return False
        for k, v in self.__dict__.iteritems():
            if v != other.__dict__[k]:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.size_map) + len(self.sizes)
    
    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return ThumbnailServingURLs.__name__

    def get_thumbnail_id(self):
        '''Return the thumbnail id for this mapping.'''
        return self.get_id()

    def add_serving_url(self, url, width, height):
        '''Adds a url to serve for a given width and height.

        If there was a previous entry, it is overwritten.
        '''
        if self.base_url is not None:
            urlRe = re.compile(
                '%s/%s' % (re.escape(self.base_url),
                           ThumbnailServingURLs.FNAME_REGEX))
            if urlRe.match(url):
                self.sizes.add((width, height))
                return
            else:
                # TODO(mdesnoyer): once the db is cleaned, make this
                # raise a ValueError
                _log.warn_n('url %s does not conform to base %s' %
                            (url, self.base_url),
                    50)
        self.size_map[(width, height)] = str(url)

    def get_serving_url(self, width, height):
        '''Get the serving url for a given width and height.

        Raises a KeyError if there isn't one.
        '''
        if (width, height) in self.sizes:
            return (self.base_url + '/' + ThumbnailServingURLs.FNAME_FORMAT %
                    (self.get_thumbnail_id(), width, height))
        return self.size_map[(width, height)]

    def get_serving_url_count(self):
        '''Return the number of serving urls in this object.'''
        return len(self.size_map) + len(self.sizes)

    def is_valid_size(self, width, height):
        '''Returns true if there is a url for this size image.'''
        sz = (width, height)
        return sz in self.sizes or sz in self.size_map

    def __iter__(self):
        '''Iterator of size, url pairs.'''
        return itertools.chain(
            self.size_map.iteritems(),
            ((k, self.get_serving_url(*k)) for k in self.sizes))

    @staticmethod
    def create_filename(tid, width, height):
        '''Creates a filename for a given thumbnail id at a specific size.'''
        return ThumbnailServingURLs.FNAME_FORMAT % (tid, width, height)

    def to_dict(self):
        new_dict = {
            '_type': self.__class__.__name__,
            '_data': copy.copy(self.__dict__)
            }
        new_dict['_data']['size_map'] = self.size_map.items()
        new_dict['_data']['sizes'] = list(self.sizes)
        return new_dict

    @classmethod
    def _create(cls, key, obj_dict):
        obj = super(ThumbnailServingURLs, cls)._create(key, obj_dict)
        if obj:
            # Convert the sizes into tuples and a set
            obj.sizes = set((tuple(x) for x in obj.sizes))
            
            # Load in the url entries into the object
            size_map = obj.size_map
            obj.size_map = {}
            # Find the base url to save that way
            bases = set((os.path.dirname(x[1]) for x in size_map))
            if len(bases) == 1 and obj.base_url is None:
                obj.base_url = bases.pop()
            for k, v in size_map:
                width, height = k
                obj.add_serving_url(v, width, height)
            return obj

        
class ThumbnailURLMapper(NamespacedStoredObject):
    '''
    Schema to map thumbnail url to thumbnail ID. 

    _input - thumbnail url ( key ) , tid - string/image, converted to thumbnail ID
            if imdata given, then generate tid 
    
    THUMBNAIL_URL => (tid)
    
    # NOTE: This has been deprecated and hence not being updated to be a stored
    object
    TODO: Remove this object. It is no longer needed
    '''
    
    def __init__(self, thumbnail_url, tid, imdata=None):
        self.key = thumbnail_url
        if not imdata:
            self.value = tid
        else:
            #TODO: Is this imdata really needed ? 
            raise #self.value = ThumbnailID.generate(imdata) 

    def to_json(self):
        # Actually not json because we are only storing the value
        return str(self.value)

    @classmethod
    def _baseclass_name(cls):
        return ThumbnailURLMapper.__name__

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

class ClipMetadata(StoredObject): 
    '''
    Class schema for Clip information.

    Keyed by clip_id
    '''
    def __init__(self, clip_id, video_id=None, thumbnail_id=None, urls=None,
                 ttype=None, rank=0, model_version=None, enabled=True,
                 refid=None, score=None,
                 serving_frac=None, ctr=None,
                 start_frame=None, end_frame=None,
                 model_params=None, rendition_ids=None):
        super(ClipMetadata,self).__init__(clip_id)

        # video id this clip was generated from
        self.video_id = video_id
        # url for this clip
        self.urls = urls or []
        # The thumbnail that can be used to represent this clip
        self.thumbnail_id = thumbnail_id
        # the type of this thumbnail. Uses ThumbnailType
        self.type = ttype
        # where this clip ranks amongst the other clips of this type
        self.rank = rank or 0
        # is this clip enabled for mastermind A/B testing
        self.enabled = enabled
        # what version of the model generated this clip
        self.model_version = model_version
        # what frame this clip starts at
        self.start_frame = start_frame
        # what frame this clip ends at
        self.end_frame = end_frame

        # List of video rendition ids for versions of this clip that
        # are available
        self.rendition_ids = rendition_ids or []

        # The score of this clip. Higher is better. Note that this
        # will be a score combined of a raw valence score plus some
        # other stuff (like motion analysis)
        self.score = score

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return ClipMetadata.__name__

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def add_clip_data(self, clip, video_info=None, cdn_metadata=None):
        '''Put the clip to CDN. Write the database for this ClipMetadata.

        Inputs- clip a cv2 VideoCapture
            -video_info a VideoMetadata or None
            -cdn_metadata a CDNHostingMetadata or None'''

        primary_hoster = cmsdb.cdnhosting.CDNHosting.create(
            PrimaryNeonHostingMetadata())
        s3_url_list = yield primary_hoster.upload_video(
            video,
            self.key,
            self.start_frame,
            self.end_frame,
            async=True)
        s3_url = None
        if len(s3_url_list) == 1:
            s3_url = s3_url_list[0][0]
            self.urls.insert(0, s3_url)

        if video_info is None:
            video_info = yield VideoMetadata.get(self.video_id, async=True)
        if cdn_metadata is None:
            cdn_metadata = yield CDNHostingMetadata.get_by_video(video_info)

        hosters = [cmsdb.cdnhosting.CDNHosting.create(c) for c in cdn_metadata]
        yield [h.upload_video(clip, self.key, self.start_frame, self.end_frame,
                              s3_url, async=True) for h in hosters]


class VideoRendition(StoredObject):
    '''
    Class schema for a rendition of a video
    '''
    def __init__(self, rendition_id=None, url=None, width=None,
                 height=None, duration=None, codec=None, container=None,
                 encoding_rate=None):
        rendition_id = rendition_id or uuid.uuid4().hex
        super(VideoRendition, self).__init__(rendition_id)

        # Url where the video is available
        self.url = url

        # Size of the rendition
        self.width = width
        self.height = height

        # Duration of the video in seconds
        self.duration = duration

        # Codec used to encode the video
        self.codec = codec

        # Container type for the video (e.g. 'mp4', 'gif')
        self.container = container

        # Average encoding rate in kpbs
        self.encoding_rate = encoding_rate

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return VideoRendition.__name__

class ThumbnailMetadata(StoredObject):
    '''
    Class schema for Thumbnail information.

    Keyed by thumbnail id
    '''
    def __init__(self, tid, internal_vid=None, urls=None, created=None,
                 width=None, height=None, ttype=None,
                 model_score=None, model_version=None, enabled=True,
                 chosen=False, rank=None, refid=None, phash=None,
                 serving_frac=None, frameno=None, filtered=None, ctr=None,
                 external_id=None, features=None):
        super(ThumbnailMetadata,self).__init__(tid)
        self.video_id = internal_vid #api_key + platform video id
        self.external_id = external_id # External id if appropriate
        self.urls = urls or []  # List of all urls associated with single image
        self.created_time = created or datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")# Timestamp when thumbnail was created 
        self.enabled = enabled #boolen, indicates if this thumbnail can be displayed/ tested with 
        self.chosen = chosen #boolean, indicates this thumbnail is chosen by the user as the primary one
        self.width = width
        self.height = height
        self.type = ttype #neon1../ brightcove / youtube
        self.rank = 0 if not rank else rank  #int 
        self.model_score = model_score # DEPRECATED use features instead
        self.model_version = model_version #string
        self.frameno = frameno #int Frame Number
        self.filtered = filtered # String describing how it was filtered
        #TODO: remove refid. It's not necessary
        self.refid = refid #If referenceID exists *in case of a brightcove thumbnail
        self.phash = phash # Perceptual hash of the image. None if unknown
        self.do_source_crop = False # see cdnhosting.CDNHosting.upload
        self.do_smart_crop = False # see cdnhosting.CDNHosting.upload
        if self.type is ThumbnailType.NEON:
            self.do_source_crop = True
            self.do_smart_crop = True

        # DEPRECATED: Use the ThumbnailStatus table instead
        self.serving_frac = serving_frac 

        # DEPRECATED: Use the ThumbnailStatus table instead
        self.ctr = ctr
       
        # This is a full feature vector. It stores a numpy array of
        # floats.  Each index is dependent on the model used. Human
        # readable versions of this exist in the Features table.
        self.features = features 
         
        # NOTE: If you add more fields here, modify the merge code in
        # video_processor/client, Add unit test to check this

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return ThumbnailMetadata.__name__

    @classmethod
    def _additional_columns(cls):
        return [PostgresColumn('features', '%s::bytea', 'features')]

    def _set_keyname(self):
        '''Key the set by the video id'''
        return 'objset:%s' % self.key.rpartition('_')[0]

    @classmethod
    def is_valid_key(cls, key):
        return ThumbnailID.is_valid_key(key)

    def update_phash(self, image):
        '''Update the phash from a PIL image.'''
        self.phash = cv.imhash_index.hash_pil_image(
            image,
            hash_type=options.hash_type,
            hash_size=options.hash_size)

    def get_account_id(self):
        ''' get the internal account id. aka api key '''
        return self.get_account_id_from_tid(self.key)

    @staticmethod
    def get_account_id_from_tid(tid):
        return tid.split('_')[0]
    
    def get_metadata(self):
        ''' get a dictionary of the thumbnail metadata

        This function is deprecated and is kept only for backwards compatibility
        '''
        return self.to_dict()
    
    def to_dict_for_video_response(self):
        ''' to dict for video response object
            replace key to thumbnail_id 
        '''
        new_dict = copy.copy(self.__dict__)
        new_dict["thumbnail_id"] = new_dict.pop("key")
        return new_dict

    @staticmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def download_image_from_url(image_url):
        '''Downloads an image from a given url.

        returns: The PIL image
        '''
        try:
            image = yield cvutils.imageutils.PILImageUtils.download_image(image_url,
                    async=True)
        except IOError, e:
            msg = "IOError while downloading image %s: %s" % (
                image_url, e)
            _log.warn(msg)
            raise ThumbDownloadError(msg)
        except tornado.httpclient.HTTPError as e:
            msg = "HTTP Error while dowloading image %s: %s" % (
                image_url, e)
            _log.warn(msg)
            raise ThumbDownloadError(msg)

        raise tornado.gen.Return(image)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def add_image_data(self, image, video_info=None, cdn_metadata=None):
        '''Incorporates image data to the ThumbnailMetadata object.

        Also uploads the image to the CDNs and S3.

        Inputs:
        image - A PIL image
        cdn_metadata - A list CDNHostingMetadata objects for how to upload the
                       images. If this is None, it is looked up, which is
                       slow. If a source_crop is requested, the image is also
                       cropped here.'''
        image = PILImageUtils.convert_to_rgb(image)
        # Update the image metadata
        self.width = image.size[0]
        self.height = image.size[1]
        self.update_phash(image)

        # Convert the image to JPG
        fmt = 'jpeg'
        filestream = StringIO()
        image.save(filestream, fmt, quality=90)
        filestream.seek(0)
        imgdata = filestream.read()

        self.key = ThumbnailID.generate(imgdata, self.video_id)

        # Host the primary copy of the image
        primary_hoster = cmsdb.cdnhosting.CDNHosting.create(
            PrimaryNeonHostingMetadata())
        s3_url_list = yield primary_hoster.upload(
            image,
            self.key,
            async=True,
            do_source_crop=self.do_source_crop,
            do_smart_crop=self.do_smart_crop)

        # TODO (Sunil):  Add redirect for the image

        # Add the primary image to Thumbmetadata
        s3_url = None
        if len(s3_url_list) == 1:
            s3_url = s3_url_list[0][0]
            self.urls.insert(0, s3_url)

        # Host the image on the CDN
        if video_info is None:
            video_info = yield VideoMetadata.get(self.video_id, async=True)
        if cdn_metadata is None:
            cdn_metadata = yield CDNHostingMetadata.get_by_video(video_info)

        hosters = [cmsdb.cdnhosting.CDNHosting.create(x) for x in cdn_metadata]
        yield [x.upload(image, self.key, s3_url, async=True,
                        do_source_crop=self.do_source_crop,
                        do_smart_crop=self.do_smart_crop) for x in hosters]

    @tornado.gen.coroutine
    def score_image(self, predictor, image=None, save_object=False):
        '''Adds the model score to the image.

        Inputs:
        predictor - a model.predictor.Predictor object used to get the score
        image - OpenCV image data. If not provided, image will be downloaded
        save_object - If true, the score is saved to the database
        '''
        if (self.model_score is not None or self.features is not None):
            # No need to compute the score, it's there
            return

        if image is None:
            pil_image = yield ThumbnailMetadata.download_image_from_url(
                self.urls[-1], async=True)
            image = cvutils.imageutils.PILImageUtils.to_cv(pil_image)

        (self.model_score, self.features, self.model_version) = \
          yield predictor.predict(image, async=True)

        if save_object:
            def _set_score(x):
                x.model_score = self.model_score
                x.model_version = self.model_version
                x.features = self.features
            new_thumb = yield ThumbnailMetadata.modify(
                self.key,
                _set_score,
                async=True)
            if new_thumb:
                self.__dict__ = new_thumb.__dict__

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
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_related_data(cls, key):
        yield ThumbnailStatus.delete(key, async=True) 
        yield ThumbnailServingURLs.delete(key, async=True)
        yield ThumbnailMetadata.delete(key, async=True) 

    def get_score(self, gender=None, age=None):
        """Computes the raw valence score for this image."""
        model_score = self.model_score
        if self.features is not None:
            # We can calculate the underlying score for a demographic
            try:
                sig = model.predictor.DemographicSignatures(
                        self.model_version)
                model_score = sig.compute_score_for_demo(
                    self.features, gender, age)
            except KeyError as e:
                # We don't know about this model, gender, age combo
                pass
        return model_score

    def get_neon_score(self, gender=None, age=None):
        """Get a value in [1..99] that the Neon score maps to.

        Uses a mapping dictionary according to the name of the
        scoring model.
        """ 
        model_score = self.get_score(gender=gender, age=age)
        if model_score is not None and float(model_score):
            return model.scores.lookup(self.model_version, model_score,
                                       gender, age)
        return None

    def get_estimated_lift(self, other_thumb, gender=None, age=None): 
        """ returns the estimated lift of this object vs another 
            thumbnail""" 
        ot_score = other_thumb.get_score(gender=gender, age=age) 
        score = self.get_score(gender=gender, age=age) 
        if ot_score is None or score is None:
            return None
        # determine the model
        if (self.model_version and 
            (re.match('20[0-9]{6}-[a-zA-Z0-9]+', 
                      self.model_version) or ('aqv1' in self.model_version))):
            # aquila v2
            return round(numpy.exp(score) / numpy.exp(ot_score) - 1, 3)
        elif ot_score > 0:
            # it's an older model
            return round(float(score) / float(ot_score) - 1, 3)
        else:
            return None


class ThumbnailStatus(DefaultedStoredObject):
    '''Holds the current status of the thumbnail in the wild.'''

    def __init__(self, thumbnail_id, serving_frac=None, ctr=None,
                 imp=None, conv=None, serving_history=None):
        super(ThumbnailStatus, self).__init__(thumbnail_id)

        # The fraction of traffic this thumbnail will get
        self.serving_frac = serving_frac

        # List of (time, serving_frac) tuples
        self.serving_history = serving_history or []

        # The current click through rate for this thumbnail
        self.ctr = ctr

        # The number of impressions this thumbnail received
        self.imp = imp

        # The number of conversions this thumbnail received
        self.conv = conv

    def set_serving_frac(self, serving_frac):
        '''Sets the serving fraction. Returns true if it is new.'''
        if (self.serving_frac is None or 
            abs(serving_frac - self.serving_frac) > 1e-3):
            self.serving_frac = serving_frac
            self.serving_history.append(
                (datetime.datetime.utcnow().isoformat(),
                 serving_frac))
            return True
        return False
            
    def get_video_id(self): 
        splits = self.key.split('_')
        return '_'.join([splits[1], splits[2]])

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return ThumbnailStatus.__name__

class Verification(StoredObject):
    '''
    Class schema for Verification

    Keyed by email
    '''
    def __init__(self, email, token=None, extra_info=None): 
        super(Verification, self).__init__(email)
        
        # the special token that is used to verify the account
        self.token = token or uuid.uuid1().hex  

        # extra_info is a json store, that could store any 
        # number of things, but is mostly used for objects 
        # that may need to be saved after verification is 
        # complete 
        self.extra_info = extra_info or {}
 
    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return Verification.__name__

class AccountLimits(StoredObject):

    # A limit of videos uploaded for a non-paid, signed up account.
    MAX_VIDEOS_ON_DEMO_SIGNUP = 100

    '''
    Class schema for AccountLimits

    Keyed by account_id(api_key)
    '''
    def __init__(self, 
                 account_id, 
                 video_posts=0, 
                 max_video_posts=10, 
                 refresh_time_video_posts=datetime.datetime(2050,1,1), 
                 seconds_to_refresh_video_posts=2592000.0,
                 max_video_size=900.0,
                 email_posts=0,
                 max_email_posts=60, 
                 refresh_time_email_posts=datetime.datetime(2000,1,1), 
                 seconds_to_refresh_email_posts=3600.0):
 
        super(AccountLimits, self).__init__(account_id)
        
        # the number of video posts this account has made in the time window 
        self.video_posts = video_posts 
         
        # the maximum amount of video posts the account is allowed in a time 
        # window 
        self.max_video_posts = max_video_posts 

        # when the video_posts counter will be reset 
        self.refresh_time_video_posts = refresh_time_video_posts.strftime(
            "%Y-%m-%d %H:%M:%S.%f") 

        # amount of seconds to add to now() when resetting the timer 
        self.seconds_to_refresh_video_posts = seconds_to_refresh_video_posts

        # maximum video length we will process in seconds 
        self.max_video_size = max_video_size

        # the number of email posts this account has made in the period 
        self.email_posts = email_posts

        # maximum amount of emails this account can send in a time period 
        self.max_email_posts = max_email_posts 

        # when the email posts counter will be reset 
        self.refresh_time_email_posts = refresh_time_email_posts.strftime(
            "%Y-%m-%d %H:%M:%S.%f") 

        # amount of seconds to add to now() when resetting refresh_time 
        self.seconds_to_refresh_email_posts = seconds_to_refresh_email_posts 

    def populate_with_billing_plan(self, bp): 
        '''helper that takes a billing plan and populates the object 
              with the plan information. 
         
        '''
        sref = bp.seconds_to_refresh_video_posts

        self.max_video_posts = bp.max_video_posts
        self.seconds_to_refresh_video_posts = sref
        self.max_video_size = bp.max_video_size 
        self.refresh_time_video_posts = \
            (datetime.datetime.utcnow() +\
             datetime.timedelta(seconds=sref)).strftime(
                 "%Y-%m-%d %H:%M:%S.%f")
 
    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return AccountLimits.__name__

class BillingPlans(StoredObject):

    # These match key and plan_type of a billing plan record.
    PLAN_DEMO = 'demo'
    PLAN_PRO_MONTHLY = 'pro_monthly'
    PLAN_PRO_YEARLY = 'pro_yearly'
    PLAN_PREMEIRE = 'premeire'

    '''
    Class schema for BillingPlans

    Keyed by plan_type, these correspond to the plan_types 
      we have defined in our external billing integration.
      This defines the limits that the billing plans will 
      have.  
    '''
    def __init__(self, 
                 plan_type, 
                 max_video_posts=None, 
                 seconds_to_refresh_video_posts=None,
                 max_video_size=None):
 
        super(BillingPlans, self).__init__(plan_type)
        
        # the max number of video posts that are allowed  
        self.max_video_posts = max_video_posts
         
        # this will take now() and add this to it, for when the next 
        # refresh will happen
        self.seconds_to_refresh_video_posts = seconds_to_refresh_video_posts

        # maximum video length we will process in seconds 
        self.max_video_size = max_video_size 
 
    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return BillingPlans.__name__

class VideoJobThumbnailList(UnsaveableStoredObject):
    '''Represents the list of thumbnails from a video processing job.'''
    def __init__(self, age=None, gender=None, thumbnail_ids=None,
                 bad_thumbnail_ids=None,
                 model_version=None,
                 clip_ids=None):
        self.model_version = model_version
        self.thumbnail_ids = thumbnail_ids or []
        self.bad_thumbnail_ids = bad_thumbnail_ids or []
        self.clip_ids = clip_ids or []

        # WARNING: If anything is added here, make sure to update
        # _merge_video_data in video_processor/client.py
        self.age = age
        self.gender = gender

class VideoMetadata(Searchable, StoredObject):
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
                 serving_enabled=True, custom_data=None,
                 publish_date=None, hidden=None, share_token=None,
                 job_results=None, non_job_thumb_ids=None,
                 bad_tids=None, tag_id=None):
        super(VideoMetadata, self).__init__(video_id)
        # DEPRECATED in favour of job_results and non_job_thumb_ids. Will
        # contain the thumbs from the most recent job only.
        self.thumbnail_ids = tids or []
        self.bad_thumbnail_ids = bad_tids or []

        # A list of VideoJobThumbnailList objects representing the
        # thumbnails extracted for each processing step.
        self.job_results = job_results or []

        # A list of thumbnail ids that are not associated with a
        # specific run of the job (e.g. default thumb)
        self.non_job_thumb_ids = non_job_thumb_ids or []

        self.url = video_url
        self.duration = duration # in seconds
        self.video_valence = vid_valence
        self.model_version = model_version
        self.job_id = request_id
        self.integration_id = i_id
        self.frame_size = frame_size #(w,h)
        # Is A/B testing enabled for this video?
        self.testing_enabled = testing_enabled
        self.share_token = share_token

        # DEPRECATED. Use VideoStatus table instead
        self.experiment_state = \
          experiment_state if testing_enabled else ExperimentState.DISABLED
        self.experiment_value_remaining = experiment_value_remaining

        # Will thumbnails for this video be served by our system?
        self.serving_enabled = serving_enabled

        # Serving URL (ISP redirect URL)
        # NOTE: This is set by mastermind by calling get_serving_url() method
        # after the request state has been changed to SERVING
        self.serving_url = None

        # A dictionary of extra metadata
        self.custom_data = custom_data or {}

        # The time the video was published in ISO 8601 format
        self.publish_date = publish_date

        # If user has deleted this video, flag it deleted.
        self.hidden = hidden

        # Tag ids associate lists of thumbnails to the video.
        self.tag_id = tag_id

    @property
    def clip_ids(self):
        '''Returns a unique list of clip ids associated with this video.'''
        return reduce(
            lambda x,y: x | y,
            [set(x.clip_ids) for x in self.job_results],
            set())

    @staticmethod
    def _get_search_arguments():
        return [
            'account_id',
            'limit',
            'title',
            'offset',
            'query',
            'since',
            'show_hidden',
            'until']

    @staticmethod
    def _get_join_part():
        return "JOIN request AS r ON t._data->>'job_id' = r._data->>'job_id'"

    @staticmethod
    def _get_where_part(key, args={}):
        if key == 'query':
            return "r._data->>'video_title' ~* %s"
        if key == 'account_id':
            if args.get(key):
                args[key] = '{}%'.format(args[key])
            return "t._data->>'key' LIKE %s"

    def _set_keyname(self):
        '''Key by the account id'''
        return 'objset:%s' % self.get_account_id()

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return VideoMetadata.__name__

    @classmethod
    def is_valid_key(cls, key):
        return len(key.split('_')) == 2

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
        video_status = yield tornado.gen.Task(VideoStatus.get, self.key)
        raise tornado.gen.Return(video_status.winner_tid)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def add_thumbnail(self, thumb, image, cdn_metadata=None,
                      save_objects=False, video=None):
        '''Add thumbnail to the video.

        Saves the thumbnail object, and the video object if
        save_object is true.

        Inputs:
        @thumb: ThumbnailMetadata object. Should be incomplete
                because image based data will be added along with
                information about the video. The object will be updated with
                the proper key and other information
        @image: PIL Image
        @cdn_metadata: A list of CDNHostingMetadata objects for how to upload
                       the images. If this is None, it is looked up, which is
                       slow.
        @save_objects: If true, the database is updated. Otherwise,
                       just this object is updated along with the thumbnail
                       object.
        '''
        thumb.video_id = self.key
        yield thumb.add_image_data(image, self, cdn_metadata, async=True)

        def _add_thumb_to_video_object(video_obj):
            video_obj.thumbnail_ids.append(thumb.key)
            video_obj.non_job_thumb_ids.append(thumb.key)

        # TODO(mdesnoyer): Use a transaction to make sure the changes
        # to the two objects are atomic. For now, put in the thumbnail
        # data and then update the video metadata.
        if save_objects:
            sucess = yield thumb.save(async=True)
            if not sucess:
                raise IOError("Could not save thumbnail")

            updated_video = yield self.modify(
                    self.key,
                    _add_thumb_to_video_object,
                    async=True)

            if updated_video is None:
                # It wasn't in the database, so save this object
                _add_thumb_to_video_object(self)
                success = yield self.save(async=True)
                if not success:
                    raise IOError("Could not save video data")
            else:
                self.__dict__ = updated_video.__dict__

            # Tag the thumbnail with the video's tag.
            if self.tag_id:
                yield TagThumbnail.save(
                    tag_id=self.tag_id,
                    thumbnail_id=thumb.get_id(),
                    async=True)
        else:
            _add_thumb_to_video_object(self)

        raise tornado.gen.Return(thumb)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def download_and_add_thumbnail(self,
                                   thumb=None,
                                   image_url=None,
                                   cdn_metadata=None,
                                   image=None,
                                   external_thumbnail_id=None,
                                   save_objects=False):
        '''
        Download the image and add it to this video metadata

        Inputs:
        @thumb: ThumbnailMetadata object. Should be incomplete
                because image based data will be added along with
                information about the video. The object will be updated with
                the proper key and other information
        @image_url: url of the image to download
        @cdn_metadata: A list CDNHostingMetadata objects for how to upload the
                       images. If this is None, it is looked up, which is slow.
        @save_objects: If true, the database is updated. Otherwise,
                       just this object is updated along with the thumbnail
                       object.
        '''
        if image is None:
            image = yield ThumbnailMetadata.download_image_from_url(
                image_url,
                async=True)
        if thumb is None:
            thumb = ThumbnailMetadata(None,
                                      ttype=ThumbnailType.DEFAULT,
                                      external_id=external_thumbnail_id)
        thumb.urls.append(image_url)
        thumb = yield self.add_thumbnail(thumb, image, cdn_metadata,
                                         save_objects, async=True)
        raise tornado.gen.Return(thumb)

    @classmethod
    def get_video_request(cls, internal_video_id, callback=None):
        ''' get video request data '''
        if not callback:
            vm = cls.get(internal_video_id)
            if vm:
                api_key = vm.key.split('_')[0]
                return NeonApiRequest.get(vm.job_id, api_key)
            else:
                return None
        else:
            raise AttributeError("Callbacks not allowed")

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_video_requests(cls, i_vids):
        '''
        Get video request objs given video_ids
        '''
        vms = yield tornado.gen.Task(VideoMetadata.get_many, i_vids)
        retval = [None for x in vms]
        request_keys = []
        request_idx = []
        cur_idx = 0
        for vm in vms:
            rkey = None
            if vm:
                api_key = vm.key.split('_')[0]
                rkey = (vm.job_id, api_key)
                request_keys.append(rkey)
                request_idx.append(cur_idx)
            cur_idx += 1

        requests = yield NeonApiRequest.get_many(request_keys, async=True)
        for api_request, idx in zip(requests, request_idx):
            retval[idx] = api_request
        raise tornado.gen.Return(retval)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_serving_url(self, staging=False, save=True):
        '''
        Get the serving URL of the video. If self.serving_url is not
        set, fetch the neon publisher id (TAI) and save the video object 
        with the serving_url set
        
        NOTE: any call to this function will return a valid serving url. 
        multiple calls to this function may or may not return the same URL 

        @save : If true, the url is saved to the database
        '''
        subdomain_index = random.randrange(1, 4)
        platform_vid = InternalVideoID.to_external(self.get_id())
        serving_format = "http://i%s.neon-images.com/v1/client/%s/neonvid_%s.jpg"

        if self.serving_url and not staging:
            # Return the saved serving_url
            raise tornado.gen.Return(self.serving_url)

        nu = yield tornado.gen.Task(
                NeonUserAccount.get, self.get_account_id())
        pub_id = nu.staging_tracker_account_id if staging else \
          nu.tracker_account_id
        serving_url = serving_format % (subdomain_index, pub_id,
                                                platform_vid)

        if not staging:

            def _update_serving_url(vobj):
                vobj.serving_url = self.serving_url
            if save:
                # Keep information about the serving url around
                self.serving_url = serving_url
                yield tornado.gen.Task(VideoMetadata.modify, self.key,
                                       _update_serving_url)
        raise tornado.gen.Return(serving_url)
        
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def image_available_in_isp(self):
        try:
            neon_user_account = yield NeonUserAccount.get(
                                          self.get_account_id(), 
                                          async=True)
            if neon_user_account is None:
                msg = ('Cannot find the neon user account %s for video %s. '
                       'This should never happen' % 
                       (self.get_account_id(), self.key))
                _log.error(msg)
                raise DBStateError(msg)
                
            request = tornado.httpclient.HTTPRequest(
                'http://%s/v1/video?%s' % (
                    options.isp_host,
                    urllib.urlencode({
                        'video_id' : InternalVideoID.to_external(self.key),
                        'publisher_id' : neon_user_account.tracker_account_id
                        })),
                follow_redirects=True)
            res = yield utils.http.send_request(request, async=True)

            if res.code != 200:
                if res.code != 204:
                    _log.error('Unexpected response looking up video %s on '
                               'isp: %s' % (self.key, res))
                else:
                    _log.debug('Image not available in ISP yet.')
                raise tornado.gen.Return(False)
                
            raise tornado.gen.Return(True)
        except tornado.httpclient.HTTPError as e: 
            _log.error('Unexpected response looking up video %s on '
                       'isp: %s' % (self.key, e))

        raise tornado.gen.Return(False)

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete_related_data(cls, key):
        vmeta = yield VideoMetadata.get(key, async=True) 
        if vmeta is None:
            # Nothing to delete
            return
        
        yield VideoStatus.delete(key, async=True)

        yield NeonApiRequest.delete(vmeta.job_id,
                                    vmeta.get_account_id(), 
                                    async=True)

        for tid in vmeta.thumbnail_ids:
            yield ThumbnailMetadata.delete_related_data(tid, async=True)

        yield VideoMetadata.delete(key, async=True)


    def get_all_thumbnail_ids(self):

        return list(set(self.thumbnail_ids + self.bad_thumbnail_ids +
            list(itertools.chain.from_iterable(
            [vj.thumbnail_ids + vj.bad_thumbnail_ids for vj in self.job_results]))))

class VideoStatus(DefaultedStoredObject):
    '''Stores the status of the video in the wild for often changing entries.

    '''
    def __init__(self, video_id, experiment_state=ExperimentState.UNKNOWN,
                 winner_tid=None,
                 experiment_value_remaining=None,
                 state_history=None):
        super(VideoStatus, self).__init__(video_id)

        # State of the experiment
        self.experiment_state = experiment_state

        # Thumbnail id of the winner thumbnail
        self.winner_tid = winner_tid

        # For the multi-armed bandit strategy, the value remaining
        # from the monte carlo analysis.
        self.experiment_value_remaining = experiment_value_remaining

        # [(time, new_state)]
        self.state_history = state_history or []

    def set_experiment_state(self, value):
        if value != self.experiment_state:
            self.experiment_state = value
            self.state_history.append(
                (datetime.datetime.utcnow().isoformat(),
                 value))

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.
        '''
        return VideoStatus.__name__ 

class AbstractJsonResponse(object):
    def to_dict(self):
        return self.__dict__

    def to_json(self):
        def json_dumper(obj):
            if isinstance(obj, numpy.ndarray):
                return obj.tolist()
            return obj.__dict__
        return json.dumps(self, default=json_dumper)

    @classmethod
    def create_from_dict(cls, d):
        '''Create the object from a dictionary.'''
        retval = cls()
        if d is not None:
            for k, v in d.iteritems():
                retval.__dict__[k] = v
        retval.timestamp = str(time.time())

        return retval

class VideoResponse(AbstractJsonResponse):
    ''' VideoResponse object that contains list of thumbs for a video 
        # NOTE: this obj is only used to format in to a json response 
    '''
    def __init__(self, vid, job_id, status, i_type, i_id, title, duration,
            pub_date, cur_tid, thumbs, abtest=True, winner_thumbnail=None,
            serving_url=None):
        self.video_id = vid # External video id
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

class VideoCallbackResponse(AbstractJsonResponse):
    def __init__(self, jid=None, vid=None, fnos=None, thumbs=None,
                 s_url=None, err=None,
                 processing_state=RequestState.UNKNOWN,
                 experiment_state=ExperimentState.UNKNOWN,
                 winner_thumbnail=None,  clip_ids=None):
        self.job_id = jid
        self.video_id = vid
        self.framenos = fnos if fnos is not None else []
        self.thumbnails = thumbs if thumbs is not None else []
        self.serving_url = s_url
        self.error = err
        self.timestamp = str(time.time())
        self.set_processing_state(processing_state)
        self.experiment_state = experiment_state
        self.winner_thumbnail = winner_thumbnail
        self.clip_ids = clip_ids or []

    def set_processing_state(self, internal_state):
        self.processing_state = ExternalRequestState.from_internal_state(
            internal_state)


if __name__ == '__main__':
    # If you call this module you will get a command line that talks
    # to the server. nifty eh?
    utils.neon.InitNeon()
    code.interact(local=locals())
