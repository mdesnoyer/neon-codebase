#/usr/bin/env python
'''
Data Model classes 

Blob Types available 

Account Types
- NeonUser
- BrightcoveAccount
- YoutubeAccount

Api Request Types
- Neon, Brightcove, youtube

#TODO Connection pooling of redis connection https://github.com/leporo/tornado-redis/blob/master/demos/connection_pool/app.py
'''

import redis as blockingRedis
import brukva as redis
import tornado.gen
import hashlib
import json
import shortuuid
import tornado.httpclient
import datetime
import time
import sys
import os
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../api')))
import brightcove_api
import youtube_api
from PIL import Image
from StringIO import StringIO
import dbsettings

'''
Static class for REDIS configuration
'''
class RedisClient(object):
    #static variables
    host = '127.0.0.1'
    port = 6379
    client = redis.Client(host,port)
    client.connect()

    #pool = blockingRedis.ConnectionPool(host, port, db=0)
    #blocking_client = blockingRedis.StrictRedis(connection_pool=pool)
    blocking_client = blockingRedis.StrictRedis(host,port)

    def __init__(self):
        pass
    
    @staticmethod
    def get_client(host=None,port=None):
        if host is None and port is None:
            return RedisClient.client, RedisClient.blocking_client
        else:
            RedisClient.c = redis.Client(host,port)
            RedisClient.bc = blockingRedis.StrictRedis(host,port)
            return RedisClient.c,RedisClient.bc 

''' Format request key'''
def generate_request_key(api_key,job_id):
    key = "request_" + api_key + "_" + job_id
    return key

'''
Abstract Hash Generator
'''

class AbstractHashGenerator(object):
    @staticmethod
    def _api_hash_function(input):
        return hashlib.md5(input).hexdigest()

''' Static class to generate Neon API Key'''
class NeonApiKey(AbstractHashGenerator):
    salt = 'SUNIL'
    
    @staticmethod
    def generate(input):
        input = NeonApiKey.salt + str(input)
        return NeonApiKey._api_hash_function(input)

class RedisMultiKeyHandler(object):
    ''' Handler to retrive multiple keys from redis storage'''
    
    conn,blocking_conn = RedisClient.get_client()
    def __init__(self,objects,callback):
        self.objects = objects
        self.external_callbaclk = callback

    def add_callack(self):
        #Parse objects in to their constituent classes (key,object) 
        #self.external_callback(self)
        return

    def get_all(self):
        keys = self.get_keynames()
        RedisMultiKeyHandler.conn.mget(keys,self.external_callback)
    
    # from objects, get their keynames
    def get_keynames():
        keys = []
        for o in objects:
            keys.append(o.key)
        return keys
    
    @staticmethod
    def multi_set(items,callback):
        pairs = {}
        for item in items:
            pairs[item.key] = item.to_json()
        
        RedisMultiKeyHandler.conn.mset(pairs,callback)


class AbstractRedisUserBlob(object):
    ''' 
        Abstract Redis interface and operations
        Use only one operation at a time, since only a
        single external callback can be registered
    '''
    
    conn,blocking_conn = RedisClient.get_client()

    def __init__(self,keyname=None):
        self.key = keyname
        self.external_callback = None
        self.lock_ttl = 3 #secs
        return

    def add_callback(self,result):
        try:
            items = json.loads(result)
            for key in items.keys():
                self.__dict__[key] = items[key]
        except:
            print "error decoding"

        if self.external_callback:
            self.external_callback(self)

    #Delayed callback function which performs async sleep
    #On wake up executes the callback which it was intended to perform
    #In this case calls the callback with the external_callback function as param
    @tornado.gen.engine
    def delayed_callback(self,secs,callback):
        yield tornado.gen.Task(tornado.ioloop.IOLoop.instance().add_timeout, time.time() + secs)
        self.callback(self.external_callback)

    def _get(self,callback):
        if self.key is None:
            raise Exception("key not set")
        self.external_callback = callback
        AbstractRedisUserBlob.conn.get(self.key,self.add_callback)

    def _save(self,value,callback=None):
        if self.key is None:
            raise Exception("key not set")
        self.external_callback = callback
        AbstractRedisUserBlob.conn.set(self.key,value,self.external_callback)

    def to_json(self):
        #TODO : don't save all the class specific params ( keyname,callback,ttl )
        return json.dumps(self, default=lambda o: o.__dict__) #don't save keyname

    def get(self,callback=None):
        if callback:
            return self._get(callback)
        else:
            return AbstractRedisUserBlob.blocking_conn.get(self.key)

    def save(self,callback=None):
        value = self.to_json()
        if callback:
            self._save(value,callback)
        else:
            return AbstractRedisUserBlob.blocking_conn.save(self.key,value)

    def lget_callback(self,result):
        #lock unsuccessful, lock exists: 
        print "lget", result
        if result == True:
            #return False to callback to retry
            self.external_callback(False)
            '''  delayed callback stub
            #delayed_callback to lget()
            #delay the call to lget() by the TTL time
            #self.delayed_callback(self.lock_ttl,self.lget)
            #return
            '''

        #If not locked, lock the key and return value (use transaction) 
        #save with TTL
        lkey = self.key + "_lock"
        value = shortuuid.uuid() 
        pipe = AbstractRedisUserBlob.conn.pipeline()
        pipe.setex(lkey,self.lock_ttl,value)
        pipe.get(self.key)
        pipe.get(lkey)
        pipe.execute(self.external_callback)

    #lock and get
    def lget(self,callback):
        ttl = self.lock_ttl
        self.external_callback = callback
        lkey = self.key + "_lock"
        AbstractRedisUserBlob.conn.exists(lkey,self.lget_callback)
        
    def _unlock_set(self,callback):
        self.external_callback = callback
        value = self.to_json()
        lkey = self.key + "_lock"
        
        #pipeline set, delete lock 
        pipe = AbstractRedisUserBlob.conn.pipeline()
        pipe.set(self.key,value)
        pipe.delete(lkey)
        pipe.execute(self.external_callback)

    #exists
    def exists(self,callback):
        self.external_callback = callback
        AbstractRedisUserBlob.conn.exists(self.key,callback)

''' NeonUserAccount

Every user in the system has a neon account and all other integrations are 
associated with this account. 

Account usage aggregation, Billing information is computed here

@videos: video id / jobid map of requests made directly through neon api
@integrations: all the integrations associated with this acccount (brightcove,youtube, ... ) 

'''

class NeonUserAccount(AbstractRedisUserBlob):
    def __init__(self,a_id,plan_start=None,processing_mins=None):
        super(NeonUserAccount,self).__init__()
        self.account_id = a_id
        self.neon_api_key = NeonApiKey.generate(a_id)
        self.key = self.__class__.__name__.lower()  + '_' + self.neon_api_key
        self.plan_start_date = plan_start
        self.processing_minutes = processing_mins
        self.videos = {} 
        self.integrations = {} 

    def add_integration(self,integration_id, accntkey):
        if len(self.integrations) ==0 :
            self.integrations = {}

        self.integrations[integration_id] = accntkey

    def add_video(self,vid,job_id):
        self.videos[str(vid)] = job_id
    
    def add_callback(self,result):
        try:
            items = json.loads(result)
            for key in items.keys():
                self.__dict__[key] = items[key]
        except:
            print "error decoding"

        if self.external_callback:
            self.external_callback(self)
   
    '''
    Save Neon User account and corresponding integration
    '''
    def save_integration(self,new_integration,callback):
        pipe = AbstractRedisUserBlob.conn.pipeline()
        pipe.set(self.key,self.to_json())
        pipe.set(new_integration.key,new_integration.to_json()) 
        pipe.execute(callback)

    @staticmethod
    def get_account(api_key,result_callback=None,lock=False):
        key = "NeonUserAccount".lower() + '_' + api_key
        if result_callback:
            NeonUserAccount.conn.get(key,result_callback) 
        else:
            return NeonUserAccount.blocking_conn.get(key)
    
    @staticmethod
    def create(json_data):
        params = json.loads(json_data)
        a_id = params['account_id']
        na = NeonUserAccount(a_id)
       
        for key in params:
            na.__dict__[key] = params[key]
        
        return na
    
    @staticmethod
    def delete(a_id):
        #check if test account
        if "test" in a_id:
            key = 'neonuseraccount' + NeonApiKey.generate(a_id)  
            NeonUserAccount.blocking_conn.delete(key)

''' Brightcove Account '''
class BrightcoveAccount(AbstractRedisUserBlob):
    def __init__(self,a_id,i_id,p_id=None,rtoken=None,wtoken=None,auto_update=False,last_process_date=None,abtest=False):
        super(BrightcoveAccount,self).__init__()
        self.neon_api_key = NeonApiKey.generate(a_id)
        self.key = self.__class__.__name__.lower()  + '_' + self.neon_api_key + '_' + i_id
        self.account_id = a_id
        self.integration_id = i_id
        self.publisher_id = p_id
        self.read_token = rtoken
        self.write_token = wtoken
        self.auto_update = auto_update 
        
        '''
        On every request, the job id is saved
        videos[video_id] = job_id 
        '''
        self.videos = {} 
        self.last_process_date = last_process_date #The publish date of the last processed video 
        self.linked_youtube_account = False
        self.abtest = abtest 

    def add_video(self,vid,job_id):
        self.videos[str(vid)] = job_id
    
    def get_videos(self):
        if len(self.videos) > 0:
            return self.videos.keys()

    def get(self,callback=None):
        if callback:
            self.lget(callback)
        else:
            return AbstractRedisUserBlob.blocking_conn.get(self.key)

    def save(self,callback=None,create=False):
        # if create, then set directly
        if callback:
            if create:
                self._save(callback)
            else:
                self._unlock_set(callback)
        else:
            value = self.to_json()
            return AbstractRedisUserBlob.blocking_conn.set(self.key,value)

    '''
    Called after getting the thumbnail url of the new thumbnail to be made 
    the default
    '''
    def update_thumbnail(self,vid,t_url,tid,update_callback=None):
        bc = brightcove_api.BrightcoveApi(self.neon_api_key,self.publisher_id,self.read_token,self.write_token,self.auto_update)
        ref_id = tid
        if update_callback:
            return bc.async_enable_thumbnail_from_url(vid,t_url,update_callback,reference_id=ref_id)
        else:
            return bc.enable_thumbnail_from_url(vid,t_url)

    ''' 
    Create neon job for particular video
    '''
    def create_job(self,vid,callback):
        def created_job(result):
            if not result.error:
                try:
                    job_id = tornado.escape.json_decode(result.body)["job_id"]
                    self.add_video(vid,job_id)
                    self.save(callback)
                except Exception,e:
                    #log.exception("key=create_job msg=" + e.message) 
                    callback(False)
            else:
                callback(False)

        bc = brightcove_api.BrightcoveApi(self.neon_api_key,self.publisher_id,self.read_token,self.write_token,self.auto_update)
        bc.create_video_request(vid,created_job)

    '''
    Use this only after you retreive the object from DB
    '''
    def check_feed_and_create_api_requests(self):
        bc = brightcove_api.BrightcoveApi(self.neon_api_key,self.publisher_id,self.read_token,self.write_token,self.auto_update,self.last_process_date)
        bc.create_neon_api_requests(self.integration_id)    

    '''
    Temp method to support backward compatibility
    '''
    def check_feed_and_create_request_by_tag(self):
        bc = brightcove_api.BrightcoveApi(self.neon_api_key,self.publisher_id,self.read_token,self.write_token,self.auto_update,self.last_process_date)
        bc.create_brightcove_request_by_tag(self.integration_id)

    @staticmethod
    def create(json_data):
        params = json.loads(json_data)
        a_id = params['account_id']
        i_id = params['integration_id'] 
        p_id = params['publisher_id']
        rtoken = params['read_token']
        wtoken = params['write_token']
        auto_update = params['auto_update']
         
        ba = BrightcoveAccount(a_id,i_id,p_id,rtoken,wtoken,auto_update)
        ba.videos = params['videos']
        ba.last_process_date = params['last_process_date'] 
        ba.linked_youtube_account = params['linked_youtube_account']
        if params.has_key('abtest'):
            ba.abtest = params['abtest'] 
        #for key in params:
        #    ba.__dict__[key] = params[key]
        return ba

    @staticmethod
    def get_account(api_key,i_id,result_callback=None,lock=False):
        key = "BrightcoveAccount".lower() + '_' + api_key + '_' + i_id
        if result_callback:
            BrightcoveAccount.conn.get(key,result_callback) 
        else:
            return BrightcoveAccount.blocking_conn.get(key)

    @staticmethod
    def find_all_videos(token,limit,result_callback):
        # Get the names and IDs of recently published videos:
        # http://api.brightcove.com/services/library?command=find_all_videos&sort_by=publish_date&video_fields=name,id&token=[token]
        url = 'http://api.brightcove.com/services/library?command=find_all_videos&sort_by=publish_date&token=' + token
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        http_client.fetch(req,result_callback)


class YoutubeAccount(AbstractRedisUserBlob):
    def __init__(self,a_id,i_id,access_token=None,refresh_token=None,expires=None,auto_update=False):
        self.key = self.__class__.__name__.lower()  + '_' + NeonApiKey.generate(a_id ) + '_' + i_id
        self.account_id = a_id
        self.integration_id = i_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires = expires
        self.generation_time = None
        self.videos = {} 
        self.valid_until = 0  

        #if blob is being created save the time when access token was generated
        if access_token:
            self.valid_until = time.time() + float(expires) - 50
        self.auto_update = auto_update
    
        self.channels = None

    def add_video(self,vid,job_id):
        self.videos[str(vid)] = job_id
    
    '''
    Get a valid access token, if not valid -- get new one and set expiry
    '''
    def get_access_token(self,callback):
        def access_callback(result):
            if result:
                self.access_token = result
                self.valid_until = time.time() + 3550
                callback(self.access_token)
            else:
                callback(False)

        #If access token has expired
        if time.time() > self.valid_until:
            yt = youtube_api.YoutubeApi(self.refresh_token)
            yt.get_access_token(access_callback)
        else:
            #return current token
            callback(self.access_token)
   
    '''
    Add a list of channels that the user has
    Get a valid access token first
    '''
    def add_channels(self,ch_callback):
        def save_channel(result):
            if result:
                self.channels = result
                ch_callback(True)
            else:
                ch_callback(False)

        def atoken_exec(atoken):
            if atoken:
                yt = youtube_api.YoutubeApi(self.refresh_token)
                yt.get_channels(atoken,save_channel)
            else:
                ch_callback(False)

        self.get_access_token(atoken_exec)


    '''
    get list of videos from youtube
    '''
    def get_videos(self,callback,channel_id=None):

        def atoken_exec(atoken):
            if atoken:
                yt = youtube_api.YoutubeApi(self.refresh_token)
                yt.get_videos(atoken,playlist_id,callback)
            else:
                callback(False)

        if channel_id is None:
            playlist_id = self.channels[0]["contentDetails"]["relatedPlaylists"]["uploads"] 
            self.get_access_token(atoken_exec)
        else:
            # Not yet supported
            callback(None)

    '''
    Update thumbnail for the given video
    '''

    def update_thumbnail(self,vid,thumb_url,callback):

        def atoken_exec(atoken):
            if atoken:
                yt = youtube_api.YoutubeApi(self.refresh_token)
                yt.async_set_youtube_thumbnail(vid,thumb_url,atoken,callback)
            else:
                callback(False)
        self.get_access_token(atoken_exec)

    '''
    Create youtube api request
    '''

    def create_job(self):
        pass

    @staticmethod
    def get_account(api_key,i_id,result_callback=None,lock=False):
        key = "YoutubeAccount".lower() + '_' + api_key + '_' + i_id
        if result_callback:
            YoutubeAccount.conn.get(key,result_callback) 
        else:
            return YoutubeAccount.blocking_conn.get(key)
    
    @staticmethod
    def create(json_data):
        params = json.loads(json_data)
        a_id = params['account_id']
        i_id = params['integration_id'] 
        yt = YoutubeAccount(a_id,i_id)
       
        for key in params:
            yt.__dict__[key] = params[key]

        return yt


#######################
# Request Blobs 
######################


class NeonApiRequest(object):
    '''
    Instance of this gets created during request creation (Neon web account, RSS Cron)
    Json representation of the class is saved in the server queue and redis  
    
    Saving request blobs : 
    create instance of the request object and call save()

    Getting request blobs :
    use static get method to get a json based response NeonApiRequest.get_request()
    '''
    host,port = dbsettings.DBConfig.videoDB 
    conn,blocking_conn = RedisClient.get_client(host,port)

    def __init__(self,job_id,api_key,vid,title,url,request_type,http_callback):
        self.key = generate_request_key(api_key,job_id) 
        self.job_id = job_id
        self.api_key = api_key 
        self.video_id = vid
        self.video_title = title
        self.video_url = url
        self.request_type = request_type
        self.callback_url = http_callback
        self.state = "submit" # submit / processing / success / fail 
        self.integration_type = "neon"

        #Meta data to be filled by the consumer client
        self.duration = None
        self.submit_time = None
        self.end_time = None
        self.frame_rate = None
        self.bitrate = None
        self.video_valence = None
        self.model_version = None

        #Save the request response
        self.response = {}  

        #Thumbnail Data
        self.thumbnails = []  

        #API Method
        self.api_method = None
        self.api_param  = None

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__) 

    def add_response(self,frames,timecodes=None,urls=None,error=None):
        self.response['frames'] = frames
        self.response['timecodes'] = timecodes 
        self.response['urls'] = urls 
        self.response['error'] = error
  
    '''
    Enable thumbnail given the id
    iterate and set the given thumbnail and disable the previous
    '''
    def enable_thumbnail(self,tid):
        t_url = None
        for t in self.thumbnails:
            if t['thumbnail_id'] == tid:
                t['enabled'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
                t_url = t['url'][0]
            else:
                t['enabled'] = None
        return t_url

    def add_thumbnail(self,tid,url,created,enabled,width,height,ttype,refid=None,rank=None):
        tdata = ThumbnailMetaData(tid,url,created,enabled,width,height,ttype,refid,rank)
        thumb = tdata.to_dict()
        self.thumbnails.append(thumb)
   
    def get_current_thumbnail(self):
        tid = None
        for t in self.thumbnails:
            if t['enabled'] is not None:
                tid = t['thumbnail_id']
                return tid
    
    def set_api_method(self,method,param):
        #TODO Verify
        self.api_method = method
        self.api_param  = param

    def save(self,callback=None):
        value = self.to_json()
        if self.key is None:
            raise Exception("key not set")
        if callback:
            NeonApiRequest.conn.set(self.key,value,callback)
        else:
            return NeonApiRequest.blocking_conn.set(self.key,value)

    @staticmethod
    def get_request(api_key,job_id,result_callback=None):
        key = generate_request_key(api_key,job_id)
        if result_callback:
            NeonApiRequest.conn.get(key,result_callback)
        else:
            return NeonApiRequest.blocking_conn.get(key)

    @staticmethod
    def multiget(keys,external_callback):
        if external_callback:
            NeonApiRequest.conn.mget(keys,external_callback)
        else:
            return NeonApiRequest.blocking_conn.mget(keys)

    @staticmethod
    def create(json_data):
        data_dict = json.loads(json_data)

        #create basic object
        obj = NeonApiRequest("dummy","dummy",None,None,None,None,None) 

        #populate the object dictionary
        for key in data_dict.keys():
            obj.__dict__[key] = data_dict[key]

        return obj

class BrightcoveApiRequest(NeonApiRequest):
    '''
    Brightcove API Request class
    '''
    def __init__(self,job_id,api_key,vid,title,url,rtoken,wtoken,pid,
                callback=None):
        self.read_token = rtoken
        self.write_token = wtoken
        self.publisher_id = pid
        self.integration_type = "brightcove"
        self.previous_thumbnail = None
        self.autosync = False
        request_type = "brightcove"
        super(BrightcoveApiRequest,self).__init__(job_id,api_key,vid,title,url,
                request_type,callback)

class YoutubeApiRequest(NeonApiRequest):
    '''
    Youtube API Request class
    '''
    def __init__(self,job_id,api_key,vid,title,url,access_token,refresh_token,
            expiry,callback=None):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.integration_type = "youtube"
        self.previous_thumbnail = None
        self.expiry = expiry
        request_type = "youtube"
        super(YoutubeApiRequest,self).__init__(job_id,api_key,vid,title,url,
                request_type,callback)


###############################################################################
## Thumbnail store T_URL => TID => Metadata
###############################################################################

class ThumbnailMetaData(object):

    '''
    Schema for storing thumbnail metadata

    A single thumbnail id maps to all its urls [ Neon, OVP name space ones, other associated ones] 
    '''
    def init(self,tid,urls,created,enabled,width,height,ttype,refid=None,rank=None):
        self.thumb = {}
        self.thumb['thumbnail_id'] = tid
        self.thumb['urls'] = urls  # All urls associated with single image
        self.thumb['created'] = created
        self.thumb['enabled'] = enabled
        self.thumb['width'] = width
        self.thumb['height'] = height
        self.thumb['type'] = ttype #neon1../ brightcove / youtube
        self.thumb['rank'] = 0 if not rank else rank
        self.thumb['refid'] = refid #If referenceID exists as in case of a brightcove thumbnail

    def to_dict(self):
        return self.thumb

class ThumbnailID(AbstractHashGenerator):
    '''
    Static class to generate thumbnail id

    input: String or Image stream. 

    Thumbnail ID is the MD5 hash of image data
    '''
    salt = 'Thumbn@il'
    
    @staticmethod
    def generate_from_string(input):
        input = ThumbnailID.salt + str(input)
        return AbstractHashGenerator._api_hash_function(input)

    @staticmethod
    def generate_from_image(imstream):   
        filestream = StringIO()
        imstream.save(filestream,'jpeg')
        filestream.seek(0)
        return ThumbnailID.generate_from_string(filestream.buf)

    @staticmethod
    def generate(input):
        if isinstance(input,basestring):
            return ThumbnailID.generate_from_string(input)
        else:
            return ThumbnailID.generate_from_image(input)


class ThumbnailURLMapper(AbstractRedisUserBlob):
    '''
    Schema to map thumbnail url to thumbnail ID. 

    input - thumbnail url ( key ) , tid - string/image, converted to thumbnail ID
            if imdata given, then generate tid 
    '''
    
    host,port = dbsettings.DBConfig.urlMapperDB
    conn,blocking_conn = RedisClient.get_client(host,port)
   
    def __init__(self,thumbnail_url,tid,imdata=None):
        self.key = thumbnail_url
        if not imdata:
            self.value = tid
        else:
            self.value = ThumbnailID.generate(imdata) 

    def save(self,external_callback=None):
        if self.key is None:
            raise Exception("key not set")
        if external_callback:
            ThumbnailURLMapper.conn.set(self.key,self.value,external_callback)
        else:
            return ThumbnailURLMapper.blocking_conn.set(self.key,value)

    @staticmethod
    def save_all(thumbnailMapperList,callback=None):
        data = {}
        for t in thumbnailMapperList:
            data[t.key] = t.value 

        if callback:
            ThumbnailURLMapper.conn.mset(data,callback)
        else:
            return ThumbnailURLMapper.blocking_conn.mset(data)

    @staticmethod
    def get_id(key,external_callback):
        if external_callback:
            ThumbnailURLMapper.conn.get(key,external_callback)
        else:
            return ThumbnailURLMapper.blocking_conn.get(key)

class ThumbnailIDMapper(AbstractRedisUserBlob):
    '''
    Class schema for Thumbnail URL to thumbnail metadata map
    Thumbnail url => ( id, platform, video id, account id, thumb type, rank) 

    Used as a cache like store for the map reduce jobs
    '''
    host,port = dbsettings.DBConfig.thumbnailDB
    conn,blocking_conn = RedisClient.get_client(host,port)

    #TODO: Configure the DB IP Address
    def __init__(self,tid,ovp,vid,aid,ttype,rank):
        super(ThumbnailIDMapper,self).__init__()
        self.key = tid 
        self.urls = urls
        self.platform = ovp
        self.video_id = vid
        self.account_id = a_id
        self.thumbnail_type = ttype
        self.rank = rank

    def _hash(self,input):
        return hashlib.md5(input).hexdigest()
    
    def get_metdata(self):
        pass
        #return only specific fields

    @staticmethod
    def save_all(thumbnailMapperList,callback=None):
        data = {}
        for t in thumbnailMapperList:
            data[t.key] = t.to_json()

        if callback:
            ThumbnailIDMapper.conn.mset(data,callback)
        else:
            ThumbnailIDMapper.blocking_conn.mset(data)

'''
MISC DB UTILS
'''

class DBUtils(object):

    def __init__(self):
        pass

    def get_all_brightcove_accounts(self,callback=None):
        bc_prefix = 'brightcoveaccount_*'
        host,port = dbsettings.DBConfig.accountDB
        conn,blocking_conn = RedisClient.get_client(host,port)
    
        if callback:
            conn.keys(bc_prefix,callback)
        else:
            accounts = blocking_conn.keys(bc_prefix)
            data = [] 
            for accnt in acccounts:
                jdata = blocking_conn.get(accnt)
                ba = BrightcoveAccount.create(jdata)
                data.append(ba)
            return data

    #ba.neon_api_key,job_id
    def get_video_data(self,api_key,job_id,callback=None):
        def wrap_callback(res):
            if res:
                callback(nar.thumbnails)
            else:
                callback(None)

        if callback:
            NeonApiRequest.get_request(api_key,job_id,wrap_callback)
        else:
            nar = NeonApiRequest.get_request(api_key,job_id)
            if nar:
                return nar.thumbnails
