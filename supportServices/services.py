#!/usr/bin/env python
'''
This script launches the services server which hosts Services that neon web account use.
- Neon Account managment
- Submit video processing request via Neon API, Brightcove, Youtube
'''

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
define("local", default=0, help="call local service", type=int)

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


################################################################################
# Helper classes  
################################################################################

class VideoResponse(object):
    def __init__(self,vid,status,i_type,i_id,title,duration,pub_data,cur_tid,thumb):
        self.video_id = vid
        self.status = status
        self.integration_type = i_type
        self.integration_id = i_id
        self.title = title
        self.duration = duration
        self.publish_date = pub_date
        self.current_thumbnail = cur_tid
        self.thumbnails = thumbs #dict of ThumbnailMetdata class

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

################################################################################
# Account Handler
################################################################################

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
    def send_json_response(self,data,status=200):
        self.set_header("Content-Type", "application/json")
        self.set_status(status)
        self.write(data)
        self.finish()
       
    
    def method_not_supported(self):
        data = '{"error":"api method not supported or REST URI is incorrect"}'
        self.send_json_response(data,400)

    ''' 
    GET /accounts/:account_id/status
    GET /accounts/:account_id/[brightcove_integrations|youtube_integrations]/:integration_id/videos

    '''
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        
        log.info("Request %r" %self.request)

        uri_parts = self.request.uri.split('/')
        if "accounts" in self.request.uri:
            #Get account id
            try:
                a_id = uri_parts[4]
                itype = uri_parts[5]
                i_id = uri_parts[6]
                method = uri_parts[7]
            except Exception,e:
                log.error("key=get request msg=  %s" %e)
                self.set_status(400)
                self.finish()
                return
            
            #Verify Account
            if not self.verify_account(a_id):
                return

            if method == "status":
                self.get_account_status(itype,i_id)
            
            elif method == "videos":
                #videoid requested
                if len(uri_parts) == 9:
                    self.get_video(a_id,vid,i_id)
                    return

                if itype  == "neon_integrations":
                    self.get_neon_videos()
            
                elif itype  == "brightcove_integrations":
                    self.get_brightcove_videos(i_id)

                elif itype == "youtube_integrations":
                    self.get_youtube_videos(i_id)
            
        else:
            self.write("API not supported")
            self.set_status(400)
            self.finish()

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        
        #POST /accounts/:account_id/brightcove_integrations
        if "brightcove_integrations" in self.request.uri:
            self.create_brightcove_integration()
        
        #POST /accounts/:account_id/youtube_integrations
        elif "youtube_integrations" in self.request.uri:
            self.create_youtube_integration()
        
        #POST /accounts ##Crete neon user account
        elif "accounts" in self.request.uri.split('/')[-1]:
            try:
                a_id = self.get_argument("account_id") 
                self.create_account(a_id)
            except:
                data = '{"error":"account id not specified"}'
                self.send_json_response(data,400)                

        else:
            self.set_status(400)
            self.finish()
   
    '''
    /accounts/:account_id/[brightcove_integrations|youtube_integrations]/:integration_id/{method}
    '''
    @tornado.web.asynchronous
    def put(self, *args, **kwargs):
       
        uri_parts = self.request.uri.split('/')
        method = None
        itype  = None
        try:
            a_id = uri_parts[4]
            itype = uri_parts[5]
            i_id = uri_parts[6]
            method = uri_parts[7]
        except Exception,e:
            pass

        #Create a new API request 
        if method == 'create_video_request':
            if "brightcove_integrations" == itype:
                self.create_brightcove_video_request(i_id)
            elif "youtube_integrations" == itype:
                self.create_youtube_video_request(i_id)
            else:
                self.method_not_supported()

        #Update Accounts
        elif method is None or method == "update":
            if "brightcove_integrations" == itype:
                self.update_brightcove_integration(i_id)
            elif "youtube_integrations" == itype:
                self.update_youtube_account(i_id)
            elif itype is None:
                #Update basic neon account
                try:
                    pmins = self.get_argument("processing_minutes")
                    pstart = self.get_argument("plan_start_date")
                    self.update_account(a_id,pmins,pstart)
                except Exception,e:
                    log.error('key=update account msg=' + e.message)
                    self.set_status(400)
                    self.finish()
            else:
                self.method_not_supported()

        #Update the thumbnail
        elif method == "videos":
            if len(uri_parts) == 8:
                vid = uri_parts[8]
                if "brightcove_integrations" == itype:
                    self.update_brightcove_video(i_id,vid)
                    return
                elif "youtube_integrations" == itype:
                    self.update_youtube_video(i_id,vid)
                    return
            self.method_not_supported()
        else:
            log.error("Method not supported")
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
        
        if itype == "neon_integrations":
            NeonUserAccount.get_account(self.api_key,account_callback)
        elif itype =="brightcove_integrations":
            BrightcovePlatform.get_account(self.api_key,i_id,account_callback)
        elif itype == "youtube_integrations":
            YoutubePlatform.get_account(self.api_key,i_id,account_callback)
        else:
            pass


    ''' Get Videos which were called from the Neon API '''

    def get_neon_videos(self):
        self.send_json_response('',200)

    ''' Get brightcove video to populate in the web account
     Get account details from db, including videos that have been
     processed so far.
     Multiget all video requests, using jobid 
     Check cached videos to reduce the multiget ( lazy load)
     Make a call to Brightcove for videos
     Aggregrate results and format for the client
    '''
    
    def get_brightcove_videos(self,i_id,limit=1):
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
            if data: 
                try:
                    bc_account = BrightcovePlatform.create(data)
                    token = bc_account.read_token
                    self.integration_id = bc_account.integration_id
                    if (bc_account.videos) > 0:
                        keys = [ generate_request_key(api_key,j_id) for j_id in bc_account.videos.values()] 
                        NeonApiRequest.multiget(keys,result_aggregator)
                        BrightcovePlatform.find_all_videos(token,limit,result_aggregator)
                    else:
                        raise Exception("NOT YET IMPL")
                except Exception,e:
                    log.exception("key=account_callback %s" %e)
            else:
                data = '{"error":"no such account"}'
                self.send_json_response(data,400)

        limit = self.get_argument('limit')
        #get brightcove tokens and video info from neondb 
        BrightcovePlatform.get_account(self.api_key,i_id,account_callback)

    ''' GET Status for a particular video from any integration type
    '''
    def get_video(self,a_id,vid,i_id=0):

        #Brightcove Account
        #Youtube Account
        #Neon Account
     
        def get_request(result):
            if result:
                req = NeonApiRequest.create(result)
                response = {}
                response['video_id'] = vid
                response['duration'] = req.duration 
                response['title'] = req.video_title
                response['status'] = req.state
                response['current_thumbnail_id'] = self.req.get_current_thumbnail()
                response['thumbnails'] = req.thumbnails
            else:
                data = '{"error": "no such video request"}'
                self.send_json_response(data,400)

        def get_account(result):
            if result:
                account = NeonUserAccount.create(result)
                jobid = account.videos[vid]
                NeonApiRequest.get_request(self.api_key,job_id,get_request)
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,400)

        ## Its ok to use the neon account for all account types, since we are looking
        ## at the video field only, todo - refactor for every account type
        def get_neon_account(result):
            if result:
                account = NeonUserAccount.create(result)
                #video accessed via neon api
                if not i_id:
                    jobid = account.videos[vid]
                    NeonApiRequest.get_request(self.api_key,job_id,get_request)
                else:
                    key = account.integrations[i_id]
                    AbstractRedisUserBlob.get(key,get_account)
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,400)

        if i_id !=0:
            NeonUserAccount.get_account(self.api_key,get_neon_account)
        else:
            #assume brightcove account
            BrightcovePlatform.get_account(self.api_key,i_id,get_neon_account)

    
    ''' Create request for brightcove video 
        submit a job on neon server, update video in the brightcove account
    '''
    def create_brightcove_video_request(self,i_id):
        def job_created(result):
            if not result: 
                data = '{"error": ""}'
                self.send_json_response(data,200)  
            else:
                data = '{"error": "failed to create job, bad request"}'
                self.send_json_response(data,400)  

        def get_account_callback(result):
            if result:
                bc = BrightcovePlatform.create(result)
                #submit job for processing
                bc.create_job(vid,job_created)
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,400)

        #check video id
        try:
            vid = self.get_argument('video_id')
        except:
            data = '{"error": "video_id not set"}'
            self.send_json_response(data,400)
            
        BrightcovePlatform.get_account(self.api_key,i_id,get_account_callback)
        

    ''' Update the thumbnail for a particular video '''
    def update_brightcove_video(self,i_id,vid):
       
        #TODO : Check for the linked youtube account 
        def update_thumbnail(t_result):
            if t_result:
                #self.vid_request.save()
                #self.bc.update_cache(self.vid_request)
                #If youtube enabled, upload to youtube too

                #Update the new thumbnail with the refID
                
                data = '{"error" :""}'
                self.send_json_response(data,200)
            else:
                data = '{"error": "thumbnail not updated"}'
                self.send_json_response(data,500)

        def get_request(r_result):
            if r_result:
                self.vid_request = BrightcoveApiRequest.create(r_result) 
                thumbnail_url = self.vid_request.choose_thumbnail(tid)
                self.bc.update_thumbnail(vid,thumbnail_url,tid,update_thumbnail)
            else:
                data = '{"error": "thumbnail not updated"}'
                self.send_json_response(data,500)

        def get_account_callback(result):
            if result:
                self.bc = BrightcovePlatform.create(result)
                job_id = self.bc.videos[vid] 
                bc_request = BrightcoveApiRequest.get_request(self.api_key,job_id,get_request) 
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        try:
            thumbnail_id = self.get_argument('thumbnail_id')
        except Exception,e:
            log.error('type=update brightcove thumbnail' + e.message)
            self.set_status(400)
            self.finish()
            return
        
        BrightcovePlatform.get_account(self.api_key,i_id,get_account_callback)

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
    
    def create_brightcove_integration(self):

        rcount = 5
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
                   
                    if video['duration'] < 0: ## skip live stream video
                        continue

                    if len(videos) > rcount:
                        break

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
                bapi.async_get_n_videos(10,verify_brightcove_tokens) 
            else:
                data = '{"error": "integration was not added, account creation issue"}'
                self.send_json_response(data,500)
            return

        def create_account(result):
            if result:
                na = NeonUserAccount.create(result)
                if len(na.integrations) >0 and na.integrations.has_key(i_id):
                    data = '{"error": "integration already exists"}'
                    self.send_json_response(data,409)
                else:
                    curtime = time.time() #account creation time
                    bc = BrightcovePlatform(a_id,i_id,p_id,rtoken,wtoken,autosync,curtime)
                    na.add_integration(bc.integration_id,bc.key)
                    na.save_integration(bc,saved_account)
            else:
                self.send_json_response('{"error":"account id error or internal error"}',400)

        try:
            a_id = self.request.uri.split('/')[-2]
            i_id = self.get_argument("integration_id")
            p_id = self.get_argument("publisher_id")
            rtoken = self.get_argument("read_token")
            wtoken = self.get_argument("write_token")
            autosync = self.get_argument("auto_update")
            NeonUserAccount.get_account(self.api_key,create_account)

        except Exception,e:
            log.error("key=create brightcove account msg= %" %e)
            data = '{"error": "API Params missing" }'
            self.send_json_response(data,400)
       

    def new_bcove_account(self):
        

        try:
            a_id = self.request.uri.split('/')[-2]
            i_id = self.get_argument("integration_id")
            p_id = self.get_argument("publisher_id")
            rtoken = self.get_argument("read_token")
            wtoken = self.get_argument("write_token")
            autosync = self.get_argument("auto_update")

        except Exception,e:
            log.error("key=create brightcove account msg= %" %e)
            data = '{"error": "API Params missing" }'
            self.send_json_response(data,400)
            return 

        na_data = yield tornado.gen.Task(NeonUserAccount.get_account,self.api_key)
        #Create and Add Platform Integration
        if na:
            na = NeonUserAccount.create(na_data)
            
            #Check if integration exists
            if len(na.integrations) >0 and na.integrations.has_key(i_id):
                data = '{"error": "integration already exists"}'
                self.send_json_response(data,409)
            else:
                curtime = time.time() #account creation time
                bc = BrightcovePlatform(a_id,i_id,p_id,rtoken,wtoken,autosync,curtime)
                na.add_integration(bc.integration_id,"brightcove")
                res = yield tornado.gen.Task(na.save_integration) #save integration & update acnt
                
                #Saved Integration
                if res:
                    bc.verify_token_and_create_requests_for_video(5)
                else:
                    data = '{"error": "integration was not added, account creation issue"}'
                    self.send_json_response(data,500)
                    return

        else:
            log.error("key=create brightcove account msg= account not found %s" %self.api_key)

    '''
    Update Brightcove account details
    '''
    def update_brightcove_integration(self,i_id):
        
        def saved_account(result):
            if result:
                data = ''
                self.send_json_response(data,200)
            else:
                data = '{"error": "account not updated"}'
                self.send_json_response(data,500)

        def update_account(result):
            if result:
                bc = BrightcovePlatform.create(result)
                bc.read_token = rtoken
                bc.write_token = wtoken
                bc.auto_update = autosync
                bc.save(saved_account)
            else:
                log.error("key=update_brightcove_integration msg= no such account %s integration id %s" %(self.api_key,i_id))
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

        uri_parts = self.request.uri.split('/')
        BrightcovePlatform.get_account(self.api_key,i_id,update_account)


    '''
    Get brightcove videos of a given state
    '''

    def get_brightcove_videos_by_state(self,i_id):

        def get_account(result):
            if result:
                bc = BrightcovePlatform.create(result)
                #Get all videos for this account
                #Aggregate result based on state

                #Get unprocessed list from brightcove 
            else:
                log.error("key=update_brightcove_integration msg= no such account %s integration id %s" %(self.api_key,i_id))
                data = '{"error": "Account doesnt exists" }'
                self.send_json_response(data,400)

        BrightcovePlatform.get_account(self.api_key,i_id,get_account)

    #### YOUTUBE ####

    '''
    Cretate a Youtube Account

    if brightcove account associated with the user, link them

    validate the refresh token and retreive list of channels for the acccount    
    '''
    def create_youtube_integration(self):

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
                log.error("key=create_youtube_integration msg=neon account not found")
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
        yt = YoutubePlatform(self.api_key,i_id,a_token,r_token,expires,autosync)
        #Add channel
        yt.add_channels(channel_callback)

    '''
    Update Youtube account
    '''
    def update_youtube_account(self,i_id):
        def saved_account(result):
            if result:
                data = ''
                self.send_json_response(data,200)
            else:
                data = '{"error": "account not updated"}'
                self.send_json_response(data,500)

        def update_account(result):
            if result:
                ya = YoutubePlatform.create(result)
                ya.access_token = access_token
                ya.refresh_token = refresh_token
                ya.auto_update = auto_update
                ya.save(saved_account)
            else:
                log.error("key=update youtube account msg= no such account %s integration id %s" %(self.api_key,i_id))
                data = '{"error": "Account doesnt exists" }'
                self.send_json_response(data,400)
       
        try:
            access_token = self.request.get_argument('access_token')
            refresh_token = self.request.get_argument('refresh_token')
            auto_update = self.request.get_argument('auto_update')
            YoutubePlatform.get_account(self.api_key,i_id,update_account)
        except:
            data = '{"error": "missing arguments"}'
            self.send_json_response()

    '''
    Create a youtube video request 
    '''
    def create_youtube_video_request(self,i_id):
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
                ya = YoutubePlatform.create(result)
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

        YoutubePlatform.get_account(self.api_key,i_id,get_account)


    '''
    Populate youtube videos
    '''
    def get_youtube_videos(self,i_id):
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
                yt_account = YoutubePlatform.create(account_response)
                
                
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

        uri_parts = self.request.uri.split('/')
        YoutubePlatform.get_account(self.api_key,i_id,account_callback)

    ''' Update the thumbnail for a particular video '''
    def update_youtube_video(self,i_id,vid):
        
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
                self.yt = YoutubePlatform.create(result)
                job_id = self.yt.videos[vid] 
                yt_request = YoutubeApiRequest.get_request(self.api_key,job_id,get_request) 
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        try:
            thumbnail_id = self.get_argument('thumbnail_id')
        except Exception,e:
            log.exception('type=update brightcove thumbnail' + e.message)
            self.set_status(400)
            self.finish()
            return

        YoutubePlatform.get_account(self.api_key,i_id,get_account_callback)

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


##
## Delete handler only for test accounts, use cautiously

class DeleteHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        a_id = self.request.uri.split('/')[-1]
        
        #make sure you delete only a test account
        if "test" in a_id:
            NeonUserAccount.remove(a_id)

class BcoveHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self, *args, **kwargs):
        pass

    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self, *args, **kwargs):
        self.video_id = self.request.uri.split('/')[-1]
        method = self.request.uri.split('/')[-2]
        self.a_id = self.request.uri.split('/')[-3]
       
        if "update" in method:
            #update thumbnail  (vid, new tid)
            self.update_thumbnail()
        elif "check" in method:
            #Check thumbnail on bcove
            self.check_thumbnail()

    def update_thumbnail(self):
        new_tid = self.get_argument('tid')
        vmdata = yield tornado.gen.Task(VideoMetadata.get,self.video_id)
        ba = yield tornado.gen.Task(BrightcovePlatform.get_account,self.a_id,vmdata.integration_id)
        bcove_video_id = InternalVideoID.to_external(self.video_id) 
        res = yield tornado.gen.Task(ba.update_thumbnail,bcove_video_id,new_tid)

    def check_thumbnail(self):
        vmdata = yield tornado.gen.Task(VideoMetadata.get,self.video_id)
        ba = yield tornado.gen.Task(BrightcovePlatform.get_account,self.a_id,vmdata.integration_id)
        bcove_video_id = InternalVideoID.to_external(self.video_id) 
        res = yield tornado.gen.Task(ba.check_current_thumbnail_in_db,bcove_video_id)

################################################################
### MAIN
################################################################

if __name__ == "__main__":
    tornado.options.parse_command_line()
    
    application = tornado.web.Application([
        (r'/api/v1/removeaccount(.*)', DeleteHandler),
        (r'/api/v1/accounts(.*)', AccountHandler),
        (r'/api/v1/brightcovecontroller(.*)', BcoveHandler),
        (r'/api/v1/jobs(.*)', JobHandler)])
    
    global server
    global BASE_URL 
    BASE_URL = "http://thumbnails.neon-lab.com" if options.local else "http://localhost:8081" 
    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
