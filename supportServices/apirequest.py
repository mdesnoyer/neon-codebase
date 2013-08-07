#!/usr/bin/env python
import brukva as redis

'''
Instance of this gets created during request creation (Neon web account, RSS Cron)
Json representation of the class is saved in the server queue and redis  

Saving request blobs : 
    create instance of the request object and call save()

Getting request blobs :
    use static get method to get a json based response NeonApiRequest.get_request()
'''

class NeonApiRequest(object):
    
    #TODO Config file 
    conn = redis.Client()
    conn.connect()
    
    def __init__(self,job_id,api_key,vid,title,url,request_type,callback):
        self.job_id = job_id
        self.api_key = api_key 
        self.video_id = vid
        self.video_title = title
        self.video_url = url
        self.request_type = request_type
        self.callback_url = callback
        self.state = "submit" # submit / in_progress / success / fail 
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
        thumb['created'] = created
        thumb['enabled'] = enabled
        thumb['width'] = width
        thumb['height'] = height
        thumb['type'] = ttype #neon1../ brightcove / youtube
        self.thumbnails.append(thumb)

    @staticmethod
    def get_request(job_id,api_key,result_callback):
        key = "request_" + api_key + "_" + job_id 
        NeonApiRequest.conn.get(key,result_callback)
    
    def save_request_blob(self,save_callback):
        pass

class BrightcoveApiRequest(NeonApiRequest):
    def __init__(self,job_id,api_key,vid,title,url,rtoken,wtoken,pid,callback=None):
        self.read_token = rtoken
        self.write_token = wtoken
        self.publisher_id = pid
        self.integration_type = "brightcove"
        request_type = "topn"
        super(BrightcoveApiRequest,self).__init__(job_id,api_key,vid,title,url,request_type,callback)

class YoutubeApiRequest(NeonApiRequest):
    def __init__(self,job_id,api_key,vid,title,url,access_token,refresh_token,pid,callback=None):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.integration_type = "youtube"
        request_type = "topn"
        super(BrightcoveApiRequest,self).__init__(job_id,api_key,vid,title,url,request_type,callback)

