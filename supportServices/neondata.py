#/usr/bin/env python
import redis as blockingRedis
import brukva as redis
import tornado.gen
import hashlib
import json
import shortuuid
import tornado.httpclient
import sys
import os
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../api')))
import brightcove_api

''' 
Neon Data Model Classes

Apikey Generator
Multikey Handler for Redis

Blob Types available 
- NeonUser
- BrightcoveAccount
- YoutubeAccount

#TODO 
#1. Connection pooling of redis connection https://github.com/leporo/tornado-redis/blob/master/demos/connection_pool/app.py
'''

class RedisClient(object):
    #static variables
    host = '127.0.0.1'
    port = 6379
    client = redis.Client(host,port)
    client.connect()

    blocking_client = blockingRedis.StrictRedis(host,port)
    
    def __init__(self):
        pass
    
    @staticmethod
    def get_client():
        return RedisClient.client, RedisClient.blocking_client

''' Format request key'''
def generate_request_key(api_key,job_id):
    key = "request_" + api_key + "_" + job_id
    return key

''' Static class to generate Neon API Key'''
class NeonApiKey(object):
    salt = 'SUNIL'

    @staticmethod
    def generate(input):
        input = NeonApiKey.salt + str(input)
        return NeonApiKey._api_hash_function(input)

    @staticmethod
    def _api_hash_function(input):
        return hashlib.md5(input).hexdigest()

''' Handler to retrive multiple keys from redis storage'''
class RedisMultiKeyHandler(object):
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


''' Abstract Redis interface and operations
    Use only one operation at a time, since only a
    single external callback can be registered
'''
class AbstractRedisBlob(object):
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
        AbstractRedisBlob.conn.get(self.key,self.add_callback)

    def _save(self,value,callback=None):
        if self.key is None:
            raise Exception("key not set")
        self.external_callback = callback
        AbstractRedisBlob.conn.set(self.key,value,self.external_callback)

    def to_json(self):
        #TODO : don't save all the class specific params ( keyname,callback,ttl )
        return json.dumps(self, default=lambda o: o.__dict__) #don't save keyname

    def get(self,callback=None):
        if callback:
            return self._get(callback)
        else:
            return AbstractRedisBlob.blocking_conn.get(self.key)

    def save(self,callback=None):
        value = self.to_json()
        if callback:
            self._save(value,callback)

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
        pipe = AbstractRedisBlob.conn.pipeline()
        pipe.setex(lkey,self.lock_ttl,value)
        pipe.get(self.key)
        pipe.get(lkey)
        pipe.execute(self.external_callback)

    #lock and get
    def lget(self,callback):
        ttl = self.lock_ttl
        self.external_callback = callback
        lkey = self.key + "_lock"
        AbstractRedisBlob.conn.exists(lkey,self.lget_callback)
        
    def _unlock_set(self,callback):
        self.external_callback = callback
        value = self.to_json()
        lkey = self.key + "_lock"
        
        #pipeline set, delete lock 
        pipe = AbstractRedisBlob.conn.pipeline()
        pipe.set(self.key,value)
        pipe.delete(lkey)
        pipe.execute(self.external_callback)

    #exists
    def exists(self,callback):
        self.external_callback = callback
        AbstractRedisBlob.conn.exists(self.key,callback)

''' Neon User Class '''
class NeonUserAccount(AbstractRedisBlob):
    def __init__(self,a_id,plan_start=None,processing_mins=None):
        super(NeonUserAccount,self).__init__()
        self.account_id = a_id
        self.neon_api_key = NeonApiKey.generate(a_id)
        self.key = self.__class__.__name__.lower()  + '_' + self.neon_api_key
        self.plan_start_date = plan_start
        self.processing_minutes = processing_mins

    def add_callback(self,result):
        try:
            items = json.loads(result)
            for key in items.keys():
                self.__dict__[key] = items[key]
        except:
            print "error decoding"

        if self.external_callback:
            self.external_callback(self)
       
''' Brightcove Account '''
class BrightcoveAccount(AbstractRedisBlob):
    def __init__(self,a_id,i_id,p_id=None,rtoken=None,wtoken=None,auto_update=False,last_process_date=None):
        super(BrightcoveAccount,self).__init__()
        self.neon_api_key = NeonApiKey.generate(a_id)
        self.key = self.__class__.__name__.lower()  + '_' + self.neon_api_key
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
        self.last_process_date = last_process_date 

    def add_video(self,vid,job_id):
        self.videos[str(vid)] = job_id
    
    def get_videos(self):
        if len(self.videos) > 0:
            return self.videos.keys()

    def get(self,callback=None):
        if callback:
            self.lget(callback)
        else:
            return AbstractRedisBlob.blocking_conn.get(self.key)

    def save(self,callback=None,create=False):
        # if create, then set directly
        if callback:
            if create:
                self._save(callback)
            else:
                self._unlock_set(callback)
        else:
            value = self.to_json()
            return AbstractRedisBlob.blocking_conn.set(self.key,value)

    def update_thumbnail(self,vid,tid,update_callback=None):
        bc = brightcove_api.BrightcoveApi(self.neon_api_key,self.publisher_id,self.read_token,self.write_token,True,self.auto_update)
        return bc.enable_thumbnail_from_url(vid,tid)

    '''
    Use this only after you retreive the object from DB
    '''
    def check_feed_and_create_api_requests(self):
        bc = brightcove_api.BrightcoveApi(self.neon_api_key,self.publisher_id,self.read_token,self.write_token,True,self.auto_update)
        bc.create_neon_api_requests()    

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
        return ba

    @staticmethod
    def get_account(api_key,result_callback=None,lock=False):
        #key = BrightcoveAccount.__class__.__name__.lower()  + '_' + api_key
        key = "BrightcoveAccount".lower() + '_' + api_key
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


class YoutubeAccount(AbstractRedisBlob):
    def __init__(self,a_id,i_id,access_token=None,refresh_token=None,expires=None,auto_update=False):
        self.key = self.__class__.__name__.lower()  + '_' + NeonApiKey.generate(a_id)
        self.account_id = a_id
        self.integration_id = i_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires = expires
        self.generation_time = None
        self.videos = {} 
        
        #if blob is being created save the time when access token was generated
        if access_token:
            self.generation_time = str(time.time())
        self.auto_update = auto_update


#######################
# Request Blobs 
######################
'''
Instance of this gets created during request creation (Neon web account, RSS Cron)
Json representation of the class is saved in the server queue and redis  

Saving request blobs : 
    create instance of the request object and call save()

Getting request blobs :
    use static get method to get a json based response NeonApiRequest.get_request()
'''

class NeonApiRequest(object):
    conn,blocking_conn = RedisClient.get_client()

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
   
    def add_thumbnail(self,tid,url,created,enabled,width,height,ttype):
        thumb = {}
        thumb['thumbnail_id'] = tid
        thumb['url'] = url
        thumb['created'] = created
        thumb['enabled'] = enabled
        thumb['width'] = width
        thumb['height'] = height
        thumb['type'] = ttype #neon1../ brightcove / youtube
        self.thumbnails.append(thumb)
   
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
        RedisMultiKeyHandler.conn.mget(keys,external_callback)

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
    def __init__(self,job_id,api_key,vid,title,url,rtoken,wtoken,pid,callback=None):
        self.read_token = rtoken
        self.write_token = wtoken
        self.publisher_id = pid
        self.integration_type = "brightcove"
        self.previous_thumbnail = None
        self.autosync = False
        request_type = "brightcove"
        super(BrightcoveApiRequest,self).__init__(job_id,api_key,vid,title,url,request_type,callback)

class YoutubeApiRequest(NeonApiRequest):
    def __init__(self,job_id,api_key,vid,title,url,access_token,refresh_token,pid,callback=None):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.integration_type = "youtube"
        self.previous_thumbnail = None
        request_type = "youtube"
        super(BrightcoveApiRequest,self).__init__(job_id,api_key,vid,title,url,request_type,callback)
