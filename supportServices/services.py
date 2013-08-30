import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
import time
import os
import sys
import signal
import hashlib
import json
import datetime
import traceback
from tornado.stack_context import ExceptionStackContext
import tornado.stack_context
import contextlib

#Neon classes
import errorlog
from neondata import *

from tornado.options import define, options
define("port", default=8083, help="run on the given port", type=int)

global log
log = errorlog.FileLogger("server")


def sig_handler(sig, frame):
    log.debug('Caught signal: ' + str(sig) )
    tornado.ioloop.IOLoop.instance().stop()

#TODO :  Implement
# On Bootstrap and periodic intervals, Load important blobs that don't change with TTL
# From storage in to cache
def CachePrimer():
    pass

class BaseHandler(tornado.web.RequestHandler):
    def save_key(self,key,value):
        return

    def get_key(self,value):
        return


class UserHandler(BaseHandler):

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        ''' 
        #supported methods
            getApiKey
            getAccountDetails(type,usageStats...) 
            getInitialVideos(userId,n)
            getNVideos(userId,n) #return metadata
        '''
        
        uri_parts = self.request.uri.split('/')
        if "getapikey" in uri_parts[-1].lower() :
            pass

        if "getaccountdetails" in uri_parts[-1].lower() :
            pass

        if "getinitialnvideos" in uri_parts[-1].lower() :
            pass
        
        if "getnvideos" in uri_parts[-1].lower() :
            pass
        
        self.finish()

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        '''
        #supported methods
            addUser(uuid,name,email,company); return neonApiKey 
            add/updateUserPreferences(autosync,thumbnailSize)
            addBrightcoveAccount(pubId,rtoken,wtoken)
            addYoutubeAccount(uuid,refreshToken)
        '''
        self.finish()

    #User Methods
    #All api return 2xx on success, 4xx on failure with json response
    #Error messages where ever applicable are listed
    #Currently assume no authentication for service calls 

    '''
    GET /api/v1/user/getNYoutubeVideos?userid=UID123
    
    { "neonapikey": "MYKEY", "account" : "youtube" , "videos": [
        { "videoid":"v123", "title": "sample" , "duration" : 10, "publishdate": "datestring", "thumbnails" : [ 
            { "url": "http://thumb1" , "current": false , "created": "date",
                        "enabled" : "date", "width" : 480 ,"height":360, "type":"neon1" }, 
            ...
            { "url": "http://yt_thumb" , "current": true , "created": "date",
                        "enabled" : "date", "width" : 480 ,"height":360, "type":"youtube" }] 
    ]}
   
    #description
    #account = youtube
    '''
    def get_n_youtube_videos(self,uuid,n=5):
        return


    '''
    POST /api/v1/user/createYoutubeApiRequest
    postdata : { "uuid":"XYZ", "videoid": "YT123", "fileurl":"http://fileloc", "autosync": true } 
   
    return { "jobid": "job1234" }
    HTTP 200
    '''
    def create_youtube_api_request(self,uuid,videoid,fileurl,autosync):
        return
    
    def update_youtube_thumbnail(self,uuid,videoid,thumbnail_id,width,height):
        return

####################################################################
# Account Handler
####################################################################

class AccountHandler(tornado.web.RequestHandler):
    
    def prepare(self):
        self.api_key = self.request.headers.get('X-Neon-API-Key') 
        if self.api_key == None:
            if self.request.uri.split('/')[-1] == "accounts" and self.request.method == 'POST':
                #only account creation call can lack this header
                return
            else:
                log.exception("key=initialize msg=api header missing")
                data = '{"error": "missing or invalid api key" }'
                self.send_json_response(data,400)

    @tornado.gen.engine
    def async_sleep(self,secs):
        yield tornado.gen.Task(tornado.ioloop.IOLoop.instance().add_timeout, time.time() + secs)


    @tornado.gen.engine
    def delayed_callback(self,secs,callback):
        yield tornado.gen.Task(tornado.ioloop.IOLoop.instance().add_timeout, time.time() + secs)
        callback(secs)

    @tornado.gen.engine
    def test(self):
        def on_result(result):
            print result

        def on_get(result):
            print "onget", result
            if result:
                bc2.save(on_result)

        bc = neondata.BrightcoveAccount(1,1)
        bc.videos['s'] = 1 
        bc.save(on_result,True)
        
        bc2 = neondata.BrightcoveAccount(1,1)
        bc2.videos['s'] = 2 
        bc.get(on_get)
        yield tornado.gen.Task(tornado.ioloop.IOLoop.instance().add_timeout, time.time() + 1)
        bc2.get(on_get)
        
        yield tornado.gen.Task(tornado.ioloop.IOLoop.instance().add_timeout, time.time() + 5)
        bc2.get(on_get)
   

    def test_bc_account(self):
        def cb(res):
            print res

        bc = BrightcoveAccount('test1','test1','2294876105001','cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..','vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..',last_process_date=1351792924)
        #harcode videos
        bc.videos['vid1'] =  'request_e85ebba0cd846e3ed620bc4792ed47d1_jid123'
        bc.videos['vid2'] =  'request_e85ebba0cd846e3ed620bc4792ed47d1_jid124' 
        bc.save(cb)
        bc.get(cb)

        #Create test neonapi request
        req = NeonApiRequest('jid123','e85ebba0cd846e3ed620bc4792ed47d1','2323153341001','test','test','api','test') 
        req.state = "finished" 
        req.add_thumbnail('1','http://brightcove.vo.llnwd.net/e1/pd/2294876105001/2294876105001_2520908716001_thumbnail-2296855886001.jpg',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),None,480,270,'neon1')
        req.add_thumbnail('2','http://brightcove.vo.llnwd.net/e1/pd/2294876105001/2294876105001_2520908716001_thumbnail-2296855886001.jpg',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),480,270,'neon2')
        req.save(cb)

        #Second request
        req = NeonApiRequest('jid124','e85ebba0cd846e3ed620bc4792ed47d1','2520415927001','test','test','api','test') 
        req.state = "in_progress"
        req.save(cb)

    #### Support Functions #####

    def verify_apikey(self):
        return True
    
    def verify_account(self,a_id):
        if NeonApiKey.generate(a_id) == self.api_key:
            return True
        else:
            data = '{"error":"invalid api key or account id doesnt match api key"}'
            self.send_json_response(data,400)
            return False

    ######## HTTP Methods #########

    '''
    Send response to service client
    '''
    @tornado.web.asynchronous
    def send_json_response(self,data,status=200):
        #self.set_header(application/json)
        self.set_status(status)
        self.write(data)
        self.finish()
       

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        #self.test_bc_account()
        #tapikey = 'e85ebba0cd846e3ed620bc4792ed47d1'

        uri_parts = self.request.uri.split('/')
        if "accounts" in self.request.uri:
            #Get account id
            try:
                a_id = uri_parts[4]
                method = uri_parts[5]
            except Exception,e:
                log.error("key=get request msg=" + e.message)
                self.set_status(400)
                self.finish()
                return
            
            #Verify Account
            if not self.verify_account(a_id):
                return

            #GET /accounts/:account_id/status
            if method == "status":
                self.get_account_status()
            
            #GET /accounts/:account_id/videos
            elif method == "videos":
                if uri_parts[-1] == "videos":
                    self.get_brightcove_videos(self.api_key,'test1',5)
                #videoid requested
                else:
                    vid = uri_parts[5]
                    self.get_video(a_id,vid)

            #GET /accounts/:account_id/youtube
            elif method == "youtube":
                self.get_youtube_videos()

        else:
            self.write("API not supported")
            self.set_status(400)
            self.finish()

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        
        #POST /accounts/:account_id/brightcove_integrations
        if "brightcove_integrations" in self.request.uri:
            self.create_brightcove_account()
        
        #POST /accounts/:account_id/youtube_integrations
        elif "youtube_integrations" in self.request.uri:
            self.create_youtube_account()
        
        #POST /accounts
        elif "accounts" in self.request.uri.split('/')[-1]:
            a_id = self.get_argument("account_id") 
            self.create_account(a_id)
        else:
            self.set_status(400)
            self.finish()
        return
    
    @tornado.web.asynchronous
    def put(self, *args, **kwargs):
        
        #1 PUT or PATCH /brightcove_integrations/:integration_id
        if "brightcove_integrations" in self.request.uri:
            #1a /brightcove_integrations/:integration_id/create_video_request
            if "create_video_request" in self.request.uri:
                self.create_brightcove_video_request()
            else:
                self.update_brightcove_account()
        
        #2 PUT or PATCH /youtube_integrations/:integration_id
        elif "youtube_integrations" in self.request.uri:
            #2a /youtube_integrations/:integration_id/create_video_request
            if "create_video_request" in self.request.uri:
                self.create_youtube_video_request()
            else:
                self.update_youtube_account()
        
        #3 PUT /accounts/:account_id
        elif "accounts" in self.request.uri:
            try:
                account_id = self.request.uri.split('/')[-1]
                pmins = self.get_argument("processing_minutes")
                pstart = self.get_argument("plan_start_date")
                self.update_account(account_id,pmins,pstart)
            except Exception,e:
                log.error('key=update account msg=' + e.message)
                self.set_status(400)
                self.finish()

        elif "videos" in self.request.uri:
            self.update_brightcove_video()
        else:
            self.set_status(400)
            self.finish()
    
    ############## User defined methods ###########

    '''
    Get account status for the neon account
    '''
    @tornado.web.asynchronous
    def get_account_status(self):
        
        client_response = {}
        client_response["queued"] = 0
        client_response["in_progress"] = 0
        client_response["finished"] = 0
        client_response["failed"] = 0
        client_response["minutes_used"] = 0

        def get_videos(result):

            #TODO Aggregate videos from the day of the billing date    
            
            if result and len(result) == 0:
                self.send_json_response('{"error":"could not retrieve videos"}',500)
                return

            total_duration = 0 
            for r,key in zip(result,self.keys):
                if r is None:
                    log.error("key=get_account_status subkey=get_videos request not found %s" %key )
                    continue

                req = NeonApiRequest.create(r)
                if req.duration:
                    client_response["minutes_used"] += req.duration
                else:
                    log.error("key=get_account_status subkey=get_videos request duration error %s" %key)

                if req.state == "submit" or req.state == "requeued" :
                    client_response["queued"] += 1 

                elif req.state == "processing":
                    client_response["in_progress"] += 1 

                elif req.state == "finished":
                    client_response["finished"] += 1 
                
                elif req.state == "failed":
                    client_response["failed"] += 1 
                
            data = tornado.escape.json_encode(client_response)
            self.send_json_response(data,200)

        def account_callback(account_data):
            if account_data:
                account = NeonUserAccount.create(account_data)
                self.keys = [ generate_request_key(self.api_key,j_id) for j_id in account.videos.values()] 
                NeonApiRequest.multiget(self.keys,get_videos)
            else:
                log.error("key=get_account_status msg=account not found for %s" %self.api_key)
                data = '{"error": "no account found"}'
                self.send_json_response(data,400)
                return

        #NeonUserAccount.get_account(self.api_key,account_callback)
        BrightcoveAccount.get_account(self.api_key,account_callback)


    ''' Get brightcove video to populate in the web account
     Get account details from db, including videos that have been
     processed so far.
     Multiget all video requests, using jobid 
     Check cached videos to reduce the multiget ( lazy load)
     Make a call to Brightcove for videos
     Aggregrate results and format for the client
    '''
    
    def get_brightcove_videos(self,api_key,account_id,limit):
        self.bc_aggr = 0 
        self.video_results = {}
        self.brightcove_results = {}
        
        self.client_response = {} 
        ''' Format
            { "videos": [
                {
            "video_id": "v123",
            "integration_type": "brightcove",
            "integration_id": "1234",
            "title": "sample",
            "duration": 10,
            "publish_date": "2013-01-01",
            "status": "finished",
            "current_thumbnail_id": "1234"
            "thumbnails": [
                {
                "url": "http://thumb1",
                "created": "2013-01-01",
                "enabled": "2013-01-01",
                "width": 480,
                "height": 360,
                "type": "neon1"
                },
                    ]
              } ...
            }
            status : not_processed, processing, finished, failed
        '''
        
        def process_video_results(res):
            for r in res:
                rd = tornado.escape.json_decode(r)
                self.video_results[rd['video_id']] = rd

        def process_brightcove_results(res):
            items = res['items']
            for i in items:
                key = str(i['id'])
                self.brightcove_results[key] = i   

        '''
        Format resposne to the client
        '''
        def format_response():
                def get_current_thumbnail(thumbnails):
                    tid = None
                    for t in thumbnails:
                        if t['enabled'] is not None:
                            tid = t['thumbnail_id']
                            return tid

                videos = []
                for vid in self.brightcove_results.keys():
                    video = {}
                    video['video_id'] = str(vid) 
                    video['integration_type'] = "brightcove"
                    video['integration_id'] = self.integration_id
                    video['title'] = self.brightcove_results[vid]['name']
                    video['duration'] =  self.brightcove_results[vid]['videoFullLength']['videoDuration']
                    pdate = int(self.brightcove_results[vid]['publishedDate'])
                    video['publish_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pdate/1000))
                    
                    #Fill from DB result
                    if vid in self.video_results.keys():
                        video['status'] = self.video_results[vid]['state'] 
                        thumbs = self.video_results[vid]['thumbnails']
                        video['current_thumbnail_id'] = get_current_thumbnail(thumbs) 
                        video['thumbnails'] = thumbs
                    else:
                        video['status'] = "unprocessed"
                        video['current_thumbnail_id'] = None 
                        video['thumbnails'] = None 

                    videos.append(video)
                
                data = tornado.escape.json_encode(videos)
                return data

        def result_aggregator(result):
            self.bc_aggr += 1
            #check each discrete call
            
            if isinstance(result,tornado.httpclient.HTTPResponse):
                bcove_result = tornado.escape.json_decode(result.body)
                process_brightcove_results(bcove_result)
            else:
                process_video_results(result)

            #both calls have finished
            if self.bc_aggr == 2:

                data = format_response()
                self.set_header("Content-Type", "application/json")
                self.write(data)
                self.finish()
                #send response and terminate
                #self.send_json_response()

        def account_callback(data):
            if data is None:
                print "No such account"
                return

            try:
                bc_account = BrightcoveAccount.create(data)
                token = bc_account.read_token
                self.integration_id = bc_account.integration_id
                if (bc_account.videos) > 0:
                    keys = [ generate_request_key(api_key,j_id) for j_id in bc_account.videos.values()] 
                    NeonApiRequest.multiget(keys,result_aggregator)
                    BrightcoveAccount.find_all_videos(token,limit,result_aggregator)
                else:
                    raise Exception("NOT YET IMPL")
            except Exception,e:
                print e

        #get brightcove tokens and video info from neondb 
        BrightcoveAccount.get_account(api_key,account_callback)

        #get video metadata from brightcove 
        return

    ''' GET Status for a particular video '''
    def get_video(self,a_id,vid):

        #Neon Account

        #Brightcove Account

        #Youtube Account
        pass

    ''' Create request for brightcove video 
        submit a job on neon server, update video in the brightcove account
    '''
    def create_brightcove_video_request(self):
        def job_created(result):
            if not result.error:
                data = result.body
                self.send_json_response(data,200)  
            else:
                data = '{"error": "failed to create job, bad request"}'
                self.send_json_response(data,400)  

        def get_account_callback(result):
            if result:
                bc = BrightcoveAccount.create(result)
                #submit job for processing
                bc.create_job(vid,job_created)
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        #check video id
        try:
            vid = self.get_argument('video_id')
        except:
            data = '{"error": "video_id not set"}'
            self.send_json_response(data,400)
            
        #get brightcove account
        BrightcoveAccount.get_account(self.api_key,get_account_callback)
        

    ''' Update the thumbnail for a particular video '''
    def update_brightcove_video(self):
        
        def update_thumbnail(t_result):
            if t_result:
                #self.vid_request.save()
                #self.bc.update_cache(self.vid_request)
                #If youtube enabled, upload to youtube too

                data = '{"error" :""}'
                self.send_json_response(data,200)
            else:
                data = '{"error": "thumbnail not updated"}'
                self.send_json_response(data,500)

        def get_request(r_result):
            if r_result:
                self.vid_request = BrightcoveApiRequest.create(r_result) 
                thumbnail_url = self.vid_request.enable_thumbnail(tid)
                self.bc.update_thumbnail(vid,thumbnail_url,update_thumbnail)
            else:
                data = '{"error": "thumbnail not updated"}'
                self.send_json_response(data,500)

        def get_account_callback(result):
            if result:
                self.bc = BrightcoveAccount.create(result)
                job_id = self.bc.videos[vid] 
                bc_request = BrightcoveApiRequest.get_request(self.api_key,job_id,get_request) 
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        try:
            uri_parts = self.request.uri.split('/')
            vid = uri_parts[6]
            thumbnail_id = self.get_argument('thumbnail_id')

        except Exception,e:
            log.exception('type=update brightcove thumbnail' + e.message)
            self.set_status(400)
            self.finish()
            return

        BrightcoveAccount.get_account(self.api_key,get_account_callback)

    ''' 
    Create a Neon Account
    '''

    def create_account(self,a_id):
        def saved_user(result):
            if result:
                data = '{ "neon_api_key": "' + api_key + '" }'
            else:
                data = '{"error": "account not created, DB error"}'
            self.send_json_response(data)
        
        def check_exists(result):
            if result:
                data = '{"error": "account already exists"}'
                log.error("key=create_account mag=account already exists %s" %api_key)
                self.send_json_response(data)
            else:
                user.save(saved_user)

        user = NeonUserAccount(a_id)
        api_key = user.neon_api_key
        NeonUserAccount.get_account(api_key,check_exists)
    
    '''
    Update a Neon account
    '''

    def update_account(self,account_id,pmins,pstart):
        def updated_account(result):
            if result:
                self.finish()
            else:
                self.set_status(500)
                self.finish()
        ua = NeonUserAccount(account_id,pstart,pmins)
        ua.save(updated_account)

    ''' Create Brightcove Account for the Neon user
    Add the integration in to the neon user account
    Extract params from post request --> create acccount in DB --> verify tokens in brightcove -->
    send top 5 videos or appropriate error to client
    '''
    
    def create_brightcove_account(self):

        def verify_brightcove_tokens(result):
            if "error" not in result.body:
                vitems = tornado.escape.json_decode(result.body)
                items = vitems['items']
                videos = [] 
                for item in items:
                    video = {}
                    video['video_id'] = str(item['id'])
                    video['title'] = item['name']
                    video['duration'] =  item['videoFullLength']['videoDuration']
                    pdate = int(item['publishedDate'])
                    video['publish_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pdate/1000))
                    video['thumbnail_url'] = item['videoStillURL']
                    videos.append(video)
                data = tornado.escape.json_encode(videos)
                self.send_json_response(data,201)
            else:
                data = result.body
                self.send_json_response(data,400)

        def saved_account(result):
            if result:
                #create bcove api request
                bapi = brightcove_api.BrightcoveApi(self.api_key,p_id,rtoken,wtoken)
                bapi.async_get_n_videos(5,verify_brightcove_tokens) 
            else:
                data = '{"error": "integration was not added, account creation issue"}'
                self.send_json_response(data,500)
            return

        def create_account(result):
            if result:
                na = NeonUserAccount.create(result)
                if na.integrations.has_key(i_id):
                    data = '{"error": "integration already exists" }'
                    self.send_json_response(data,409)
                else:
                    bc = BrightcoveAccount(a_id,i_id,p_id,rtoken,wtoken,autosync)
                    na.add_integration(bc.integration_id,bc.key)
                    na.save_integration(bc,saved_account)
            else:
                self.send_json_response('',400)

        try:
            a_id = self.request.uri.split('/')[-2]
            i_id = self.get_argument("integration_id")
            p_id = self.get_argument("publisher_id")
            rtoken = self.get_argument("read_token")
            wtoken = self.get_argument("write_token")
            autosync = self.get_argument("auto_update")
            NeonUserAccount.get_account(self.api_key,create_account)

        except Exception,e:
            log.error("key=create brightcove account msg=" + e.message)
            data = '{"error": "API Params missing" }'
            self.send_json_response(data,400)
        
    '''
    Update Brightcove account details
    '''
    def update_brightcove_account(self):
        
        def saved_account(result):
            if result:
                data = ''
                self.send_json_response(data,200)
            else:
                data = '{"error": "account not updated"}'
                self.send_json_response(data,500)
            return

        def update_account(result):
            if result:
                bc = BrightcoveAccount.create(result)
                bc.read_token = rtoken
                bc.write_token = wtoken
                bc.auto_update = autosync
                bc.save(saved_account)
            else:
                log.error("key=create brightcove account msg=" + e.message)
                data = '{"error": "Account doesnt exists" }'
                self.send_json_response(data,400)
       
        try:
            rtoken = self.get_argument("read_token")
            wtoken = self.get_argument("write_token")
            autosync = self.get_argument("auto_update")
        except Exception,e:
            log.error("key=create brightcove account msg=" + e.message)
            data = '{"error": "API Params missing" }'
            self.send_json_response(data,400)
            return

        BrightcoveAccount.get_account(self.api_key,update_account)


    '''
    Cretate a Youtube Account

    if brightcove account associated with the user, link them
    '''
    def create_youtube_account(self):

        def saved_account(result):
            if result:
                data = '{"error" : ""}'
                self.send_json_response(data,201)
            else:
                data = '{"error": "account creation issue"}'
                self.send_json_response(data,500)
            return
       
        def neon_account(result):
            if result:
                na = NeonUserAccount.create(result)
                na.add_integration(yt.integration_id,yt.key) 
                na.save_integration(yt,saved_account)
            else:
                log.error("key=create_youtube_account msg=neon account not found")
                data = '{"error": "account creation issue"}'
                self.send_json_response(data,500)

        def channel_callback(result):
            if result:
                NeonUserAccount.get_account(self.api_key,neon_account)
            else:
                data = '{"error": "account creation issue"}'
                self.send_json_response(data,500)

        i_id = self.get_argument("integration_id")
        a_token = self.get_argument("access_token")
        r_token = self.get_argument("refresh_token")
        expires = self.get_argument("expires")    
        autosync = self.get_argument("auto_update")
        yt = YoutubeAccount(self.api_key,i_id,a_token,r_token,expires,autosync)
        #Add channel
        yt.add_channels(channel_callback)

    def update_youtube_account(self):
        self.write("NOT YET IMPLEMENTED")
        self.finish()

    def create_youtube_video_request(self):
        def job_created(response):
            if not response.error:
                data = response.body 
                self.send_json_response(data,200)
            else:
                data = '{"error": "job not created"}'
                self.send_json_response(data,400)
        
        #Get params from request
        #Get account details   
       
        def get_account(result):
            if result:
                ya = YoutubeAccount.create(result)
                params = {}
                params["api_key"] = self.api_key
                params["video_id"] = self.get_argument("video_id")
                params["video_title"] = self.get_argument("video_id")
                params["video_url"] = self.get_argument("video_url") 
                params["topn"] = 5
                params["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"
                params["access_token"] = ya.access_token
                params["refresh_token"] = ya.refresh_token
                params["token_expiry"] = ya.expires
                params["autosync"] = ya.auto_update

                body = tornado.escape.json_encode(params)
                yt_request = "http://thumbnails.neon-lab.com/api/v1/submitvideo/youtube"
                yt_request = "http://localhost:8081/api/v1/submitvideo/youtube"
                http_client = tornado.httpclient.AsyncHTTPClient()
                req = tornado.httpclient.HTTPRequest(url = yt_request,
                                                    method = "POST",
                                                    body = body,
                                                    request_timeout = 60.0,
                                                    connect_timeout = 10.0)
                http_client.fetch(req,job_created)         
            else:
                data = '{"error" : "no such youtube account" }'
                self.send_json_response(data,400)

        YoutubeAccount.get_account(self.api_key,get_account)


    '''
    Populate youtube videos
    '''
    def get_youtube_videos(self):
        self.counter = 0
        self.yt_results = None
        self.video_results = None

        def format_result(response):
            if response and not response.error:
                vitems = tornado.escape.json_decode(response.body)
                items = vitems['items']
                videos = [] 
                for item in items:
                    video = {}
                    video['video_id'] = item['snippet']['resourceId']['videoId']
                    video['title'] = item['snippet']['title']
                    video['publish_date'] = item['snippet']['publishedAt']
                    video['thumbnail_url'] = item['snippet']['thumbnails']['default']['url'] 
                    videos.append(video)
                data = tornado.escape.json_encode(videos)
                self.send_json_response(data,200)
            else:
                data = '{"error": "youtube api issue"}'
                self.send_json_response(data,500)

       
        def process_youtube_results(yt_response):
            if not yt.response.error:
                self.yt_results = yt_response.body
            else:
                data = '{"error": "youtube api error"}'
                self.send_json_response(data,500)

        def process_video_results(vid_result):
            if vid_result:
                self.video_results = tornado.escape.json_decode(vid_result)
            else:
                data = '{"error": "database error"}'
                self.send_json_response(data,500)

        def format_response():
            def get_current_thumbnail(thumbnails):
                    tid = None
                    for t in thumbnails:
                        if t['enabled'] is not None:
                            tid = t['thumbnail_id']
                            return tid

            videos = []
            vitems = tornado.escape.json_decode(self.yt_results)
            items = vitems['items']
            for vid in items:
                    video = {}
                    video['video_id'] = item['snippet']['resourceId']['videoId']
                    video['title'] = item['snippet']['title']
                    video['publish_date'] = item['snippet']['publishedAt']
                    video['thumbnail_url'] = item['snippet']['thumbnails']['default']['url'] 
                    video['duration'] = None 
                    
                    #Fill from DB result
                    if vid in self.video_results.keys():
                        video['status'] = self.video_results[vid]['state'] 
                        thumbs = self.video_results[vid]['thumbnails']
                        video['current_thumbnail_id'] = get_current_thumbnail(thumbs) 
                        video['thumbnails'] = thumbs
                    else:
                        video['status'] = "unprocessed"
                        video['current_thumbnail_id'] = None 
                        video['thumbnails'] = None 
            videos.append(video)
              
            data = tornado.escape.json_encode(videos)
            return data

        def result_aggregator(result):
            self.bc_aggr += 1
            #check each discrete call
            
            if isinstance(result,tornado.httpclient.HTTPResponse):
                bcove_result = tornado.escape.json_decode(result.body)
                process_brightcove_results(bcove_result)
            else:
                process_video_results(result)

            #both calls have finished
            if self.bc_aggr == 2:

                data = format_response()
                self.set_header("Content-Type", "application/json")
                self.write(data)
                self.finish()
                #send response and terminate
                #self.send_json_response()

        def result_aggregator(result):
            self.counter += 1

            if isinstance(result,tornado.httpclient.HTTPResponse):
                yt_result = tornado.escape.json_decode(result.body)
                process_youtube_results(yt_result)
            else:
                process_video_results(result)
            
            if self.counter == 2:
                data = format_response()
                self.set_header("Content-Type", "application/json")
                self.write(data)
                self.finish()

        def account_callback(account_response):
            if account_response:
                yt_account = YoutubeAccount.create(account_response)
                
                
                if (yt_account.videos) > 0:
                    #1.Get videos from youtube api
                    yt_account.get_videos(format_result)
                    
                    #2.Get videos that have been already processed from Neon Youtube account
                    keys = [ generate_request_key(api_key,j_id) for j_id in yt_account.videos.values()] 
                    NeonApiRequest.multiget(keys,result_aggregator)
                else:
                    raise Exception("NOT YET IMPL")
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        YoutubeAccount.get_account(self.api_key,account_callback)

    ''' Update the thumbnail for a particular video '''
    def update_youtube_video(self):
        
        def update_thumbnail(t_result):
            if t_result:
                data = '{"error" :""}'
                self.send_json_response(data,200)
            else:
                data = '{"error": "thumbnail not updated"}'
                self.send_json_response(data,500)

        def get_request(r_result):
            if r_result:
                self.vid_request = YoutubeApiRequest.create(r_result) 
                thumbnail_url = self.vid_request.enable_thumbnail(tid)
                self.yt.update_thumbnail(vid,thumbnail_url,update_thumbnail)
            else:
                data = '{"error": "thumbnail not updated"}'
                self.send_json_response(data,500)

        def get_account_callback(result):
            if result:
                self.yt = YoutubeAccount.create(result)
                job_id = self.yt.videos[vid] 
                yt_request = YoutubeApiRequest.get_request(self.api_key,job_id,get_request) 
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        try:
            uri_parts = self.request.uri.split('/')
            vid = uri_parts[6]
            thumbnail_id = self.get_argument('thumbnail_id')

        except Exception,e:
            log.exception('type=update brightcove thumbnail' + e.message)
            self.set_status(400)
            self.finish()
            return

        YoutubeAccount.get_account(self.api_key,get_account_callback)

###########################################################
## Job Handler
###########################################################

class JobHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        
        #Get Job status
        self.get_job_status()

    def get_job_status(self):
        def status_callback(result):
            pass

        j_id = self.request.uri.split('/')[-1]
        #self.api_key
        NeonApiRequest.get_request(self.api_key,j_id,status_callback)


################################################################
### MAIN
################################################################

@contextlib.contextmanager
def exp_handler(self,*args,**kwargs):
        print args, kwargs

if __name__ == "__main__":
    application = tornado.web.Application([
        (r'/api/v1/user/(.*)', UserHandler),
        (r'/api/v1/accounts(.*)', AccountHandler),
        (r'/api/v1/jobs(.*)', JobHandler)])
    
    global server
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    tornado.options.parse_command_line()
    
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
