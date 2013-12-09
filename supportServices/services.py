#!/usr/bin/env python
'''
This script launches the services server which hosts Services that neon web account use.
- Neon Account managment
- Submit video processing request via Neon API, Brightcove, Youtube
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import datetime
import json
import hashlib
import PIL.Image as Image
import logging
import os
import random
import sys
import signal
import time
import traceback
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
import utils.neon

from StringIO import StringIO
from supportServices import neondata
from utils.inputsanitizer import InputSanitizer

from utils.options import define, options
define("port", default=8083, help="run on the given port", type=int)
define("local", default=0, help="call local service", type=int)

import logging
_log = logging.getLogger(__name__)

def sig_handler(sig, frame):
    _log.debug('Caught signal: ' + str(sig) )
    tornado.ioloop.IOLoop.instance().stop()

#TODO :  Implement
# On Bootstrap and periodic intervals, Load important blobs that don't change with TTL
# From storage in to cache
def CachePrimer():
    pass

################################################################################
# Helper classes  
################################################################################

class VideoResponse(object):
    def __init__(self,vid,status,i_type,i_id,title,duration,pub_date,cur_tid,thumbs):
        self.video_id = vid
        self.status = status
        self.integration_type = i_type
        self.integration_id = i_id
        self.title = title
        self.duration = duration
        self.publish_date = pub_date
        self.current_thumbnail = cur_tid
        self.thumbnails = thumbs if thumbs else []  #list of ThumbnailMetdata dicts 
    
    def to_dict(self):
        return self.__dict__

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
                _log.exception("key=initialize msg=api header missing")
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
        if neondata.NeonApiKey.generate(a_id) == self.api_key:
            return True
        else:
            data = '{"error":"invalid api key or account id doesnt match api key"}'
            _log.warning(("key=verify_account "
                          "msg=api key doesn't match for account %s") % a_id)
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
        
        _log.info("Request %r" %self.request)

        uri_parts = self.request.uri.split('/')
        if "accounts" in self.request.uri:
            #Get account id
            try:
                a_id = uri_parts[4]
                itype = uri_parts[5]
                i_id = uri_parts[6]
                method = uri_parts[7]
            except Exception,e:
                _log.error("key=get request msg=  %s" %e)
                self.set_status(400)
                self.finish()
                return
            
            #Verify Account
            if not self.verify_account(a_id):
                return

            if method == "status":
                self.get_account_status(itype,i_id)
            
            elif method == "videos" or "videos" in method:
                #videoid requested
                if len(uri_parts) == 9:
                    try:
                        ids = self.get_argument('video_ids')
                        vids = ids.split(',') 
                    except:
                        #Get all the videos from the account
                        self.get_video_status_brightcove(i_id,None)
                        return
                    self.get_video_status_brightcove(i_id,vids)
                    return

                if itype  == "neon_integrations":
                    self.get_neon_videos()
            
                elif itype  == "brightcove_integrations":
                    try:
                        ids = self.get_argument('video_ids')
                        vids = ids.split(',') 
                    except:
                        self.get_video_status_brightcove(i_id,None)
                        return

                    self.get_video_status_brightcove(i_id,vids)

                elif itype == "youtube_integrations":
                    self.get_youtube_videos(i_id)
            else:
                self.write("API not supported")
                _log.warning(('key=account_handler '
                              'msg=Invalid method in request %s method %s') 
                              % (self.request.uri,method))
                self.set_status(400)
                self.finish()

        else:
            self.write("API not supported")
            _log.warning(('key=account_handler '
                          'msg=Account missing in request %s')
                          % self.request.uri)
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
                #self.create_account(a_id)
                self.create_account_and_neon_integration(a_id)
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
                    _log.error('key=update account msg=' + e.message)
                    self.set_status(400)
                    self.finish()
            else:
                self.method_not_supported()

        #Update the thumbnail
        elif method == "videos":
            if len(uri_parts) == 9:
                vid = uri_parts[-1]
                if "brightcove_integrations" == itype:
                    try:
                        new_tid = self.get_argument('thumbnail_id')
                    except:
                        data = '{"error": "missing thumbnail_id argument"}'
                        self.send_json_response(data,400)
                        return
                        
                    self.update_video_brightcove(i_id,vid,new_tid)

                elif "youtube_integrations" == itype:
                    self.update_youtube_video(i_id,vid)
                    return
            else:
                self.method_not_supported()
        else:
            _log.error("Method not supported")
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
                    _log.error("key=get_account_status subkey=get_videos request not found %s" %key )
                    continue

                req = neondata.NeonApiRequest.create(r)
                if req.duration:
                    client_response["minutes_used"] += req.duration
                else:
                    _log.error("key=get_account_status subkey=get_videos request duration error %s" %key)

                if req.state == "submit" or req.state == "requeued" :
                    client_response["queued"] += 1 

                elif req.state == "processing":
                    client_response["in_progress"] += 1 

                elif (req.state == "finished" or 
                      req.state == neondata.RequestState.FINISHED):
                    client_response["finished"] += 1 
                
                elif req.state == "failed":
                    client_response["failed"] += 1 
                
            data = tornado.escape.json_encode(client_response)
            self.send_json_response(data,200)

        def account_callback(account_data):
            if account_data:
                account = neondata.NeonUserAccount.create(account_data)
                self.keys = [ neondata.generate_request_key(self.api_key,j_id) for j_id in account.videos.values()] 
                neondata.NeonApiRequest.multiget(self.keys,get_videos)
            else:
                _log.error("key=get_account_status msg=account not found for %s" %self.api_key)
                data = '{"error": "no account found"}'
                self.send_json_response(data,400)
                return
        
        if itype == "neon_integrations":
            neondata.NeonUserAccount.get_account(self.api_key,account_callback)
        elif itype =="brightcove_integrations":
            neondata.BrightcovePlatform.get_account(self.api_key,i_id,account_callback)
        elif itype == "youtube_integrations":
            neondata.YoutubePlatform.get_account(self.api_key,i_id,account_callback)
        else:
            pass


    ''' Get Videos which were called from the Neon API '''

    def get_neon_videos(self):
        self.send_json_response('{"msg":"not yet implemented"}',200)

    ''' Get brightcove video to populate in the web account
     Get account details from db, including videos that have been
     processed so far.
     Multiget all video requests, using jobid 
     Check cached videos to reduce the multiget ( lazy load)
     Make a call to Brightcove for videos
     Aggregrate results and format for the client
    '''
    
    ''' Get video status for multiple videos -- Brightcove Integration '''
    @tornado.gen.engine
    def get_video_status_brightcove(self,i_id,vids):
        result = {}
        incomplete_states = [
            neondata.RequestState.SUBMIT,neondata.RequestState.PROCESSING,
            neondata.RequestState.REQUEUED,neondata.RequestState.INT_ERROR]
        
        #1 Get job ids for the videos from account, get the request status
        jdata = yield tornado.gen.Task(neondata.BrightcovePlatform.get_account,
                                       self.api_key,i_id)
        ba = neondata.BrightcovePlatform.create(jdata)
        if not ba:
            _log.error("key=get_video_status_brightcove msg=account not found")
            self.send_json_response("brightcove account not found",400)
            return
       
        #return all videos in the account
        if vids is None:
            vids = ba.get_videos()
        
        # No videos in the account
        if not vids:
            data = '[]'
            self.send_json_response(data,200)
            return

        job_ids = [] 
        for vid in vids:
            try:
                jid = neondata.generate_request_key(self.api_key,
                                                    ba.videos[vid])
                job_ids.append(jid)
            except:
                pass #job id not found

        
        #2 Get Job status
        completed_videos = [] #jobs that have completed 

        #Hack for first time video requests in brightcove #TODO: cleanup
        requests = yield tornado.gen.Task(neondata.NeonApiRequest.get_requests,job_ids) 
        ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for request,vid in zip(requests,vids):
            if not request:
                result[vid] = None #indicate job not found
                continue

            status = neondata.RequestState.PROCESSING 
            if request.state in incomplete_states:
                t_urls = []; thumbs = []
                t_urls.append(request.previous_thumbnail)
                #Create TID 0 as a temp place holder for previous thumbnail during processing stage
                tm = neondata.ThumbnailMetaData(0,t_urls,ctime,0,0,"brightcove",0,0)
                thumbs.append(tm.to_dict())
            elif request.state is neondata.RequestState.FAILED:
                pass
            else:
                #Jobs have finished
                completed_videos.append(vid)
                status = "finished"
                thumbs = None

            pub_date = None if not request.__dict__.has_key('publish_date') else request.publish_date
            vr = VideoResponse(vid,
                              status,
                              request.request_type,
                              i_id,
                              request.video_title,
                              None, #duration
                              pub_date,
                              0, #current tid,add fake tid
                              thumbs)
            result[vid] = vr

        #3. Populate Completed videos
        keys = [neondata.InternalVideoID.generate(self.api_key,vid) for vid in completed_videos] #get internal vids
        if len(keys) > 0:
            video_results = yield tornado.gen.Task(neondata.VideoMetadata.multi_get,keys)
            tids = []
            for vresult in video_results:
                if vresult:
                    tids.extend(vresult.thumbnail_ids)
        
            #Get all the thumbnail data for videos that are done
            thumbnails = yield tornado.gen.Task(neondata.ThumbnailIDMapper.get_thumb_mappings,tids)
            for thumb in thumbnails:
                if thumb:
                    vid = neondata.InternalVideoID.to_external(thumb.video_id)
                    tdata = thumb.get_metadata() #to_dict()
                    if not result.has_key(vid):
                        _log.debug("key=get_video_status_brightcove msg=video deleted %s"%vid)
                    else:
                        result[vid].thumbnails.append(tdata) 
        
        #4. Set the default thumbnail for each of the video
        for res in result:
            vres = result[res]
            bcove_thumb_id = None
            for thumb in vres.thumbnails:
                if thumb["chosen"] == True:
                    vres.current_thumbnail = thumb["thumbnail_id"]
                    if "neon" in thumb["type"]:
                        vres.status = "active"

                if thumb["type"] == "brightcove":
                    bcove_thumb_id = thumb["thumbnail_id"]

            if vres.status == "finished" and vres.current_thumbnail == 0:
                vres.current_thumbnail = bcove_thumb_id

        #convert to dict
        vresult = []
        for res in result:
            vres = result[res]
            vresult.append(vres.to_dict())

        s_vresult = sorted(vresult, key=lambda k: k['publish_date'],reverse=True)
        data = tornado.escape.json_encode(s_vresult)
        self.send_json_response(data,200)


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
                bc = neondata.BrightcovePlatform.create(result)
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
            
        neondata.BrightcovePlatform.get_account(self.api_key,i_id,get_account_callback)
        

    ''' update thumbnail for a brightcove video '''
    @tornado.gen.engine
    def update_video_brightcove(self,i_id,p_vid,new_tid):
        #TODO : Check for the linked youtube account 
        
        #Get account/integration
        jdata = yield tornado.gen.Task(neondata.BrightcovePlatform.get_account,
                self.api_key,i_id)
        ba = neondata.BrightcovePlatform.create(jdata)
        if not ba:
            _log.error("key=update_video_brightcove" 
                    " msg=account doesnt exist api key=%s i_id=%s"%(self.api_key,i_id))
            data = '{"error": "no such account"}'
            self.send_json_response(data,400)
            return

        result = yield tornado.gen.Task(ba.update_thumbnail,p_vid,new_tid)
        
        if result:
            _log.debug("key=update_video_brightcove" 
                    " msg=thumbnail updated for video=%s tid=%s"%(p_vid,new_tid))
            data = ''
            self.send_json_response(data,200)
        else:
            if result is None:
                data = '{"error": "internal error"}'
                self.send_json_response(data,500)
            else:
                data = '{"error": "brightcove api failure"}'
                self.send_json_response(data,502)


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
        ua = neondata.NeonUserAccount(account_id,pstart,pmins)
        ua.save(updated_account)

    '''
    Create Neon user account and add neon integration
    '''
    @tornado.gen.engine
    def create_account_and_neon_integration(self,a_id):
        user = neondata.NeonUserAccount(a_id)
        api_key = user.neon_api_key
        nuser_data = yield tornado.gen.Task(neondata.NeonUserAccount.get_account,a_id)
        if not nuser_data:
            nplatform = neondata.NeonPlatform(a_id)
            user.add_integration(nplatform.integration_id,"neon")
            res = yield tornado.gen.Task(user.save_integration,nplatform)
            if res:
                data = '{ "neon_api_key": "' + user.neon_api_key + '" }'
                self.send_json_response(data,200)
            else:
                data = '{"error": "account not created"}'
                self.send_json_response(data,500)

        else:
            data = '{"error": "integration/ account already exists"}'
            self.send_json_response(data,409)



    ''' Create Brightcove Account for the Neon user
    Add the integration in to the neon user account
    Extract params from post request --> create acccount in DB --> verify tokens in brightcove -->
    send top 5 videos requests or appropriate error to client
    '''
       
    @tornado.gen.engine
    def create_brightcove_integration(self):

        try:
            a_id = self.request.uri.split('/')[-2]
            i_id = InputSanitizer.to_string(self.get_argument("integration_id"))
            p_id = InputSanitizer.to_string(self.get_argument("publisher_id"))
            rtoken = InputSanitizer.to_string(self.get_argument("read_token"))
            wtoken = InputSanitizer.to_string(self.get_argument("write_token"))
            autosync = InputSanitizer.to_bool(self.get_argument("auto_update"))

        except Exception,e:
            _log.error("key=create brightcove account msg= %s" %e)
            data = '{"error": "API Params missing"}'
            self.send_json_response(data,400)
            return 

        na_data = yield tornado.gen.Task(neondata.NeonUserAccount.get_account,
                                         self.api_key)
        #Create and Add Platform Integration
        if na_data:
            na = neondata.NeonUserAccount.create(na_data)
            
            #Check if integration exists
            if len(na.integrations) >0 and na.integrations.has_key(i_id):
                data = '{"error": "integration already exists"}'
                self.send_json_response(data,409)
            else:
                curtime = time.time() #account creation time
                bc = neondata.BrightcovePlatform(a_id, i_id, p_id, rtoken, 
                                                 wtoken, autosync, curtime)
                na.add_integration(bc.integration_id,"brightcove")
                res = yield tornado.gen.Task(na.save_integration,bc) #save integration & update acnt
                
                #Saved Integration
                if res:
                    response = bc.verify_token_and_create_requests_for_video(10)
                    
                    # TODO: investigate further, ReferenceError: weakly-referenced object no longer exists
                    # (self.subscribed and cmd == 'PUBLISH')):
                    #Not Async due to tornado redis bug in neon server
                    #yield tornado.gen.Task(bc.verify_token_and_create_requests_for_video,5)
                    
                    ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    #TODO : Add expected time of completion !
                    video_response = []
                    if not response:
                        #TODO : Distinguish between api call failure and bad tokens
                        _log.error("key=create brightcove account msg=brightcove api call failed or token error")
                        data = '{"error": "Read token given is incorrect or brightcove api failed"}'
                        self.send_json_response(data,502)
                        return

                    for item in response:
                        t_urls =[]; thumbs = []
                        t_urls.append(item['videoStillURL'])
                        tm = neondata.ThumbnailMetaData(0,t_urls,ctime,0,0,"brightcove",0,0)
                        thumbs.append(tm.to_dict())
                        vr = VideoResponse(item["id"],
                              "processing",
                              "brightcove",
                              i_id,
                              item['name'],
                              None,
                              None,
                              0, #current tid,add fake tid
                              thumbs)
                        video_response.append(vr.to_dict())
                        
                    data = tornado.escape.json_encode(video_response)
                    self.send_json_response(data,201)
                else:
                    data = '{"error": "integration was not added, account creation issue"}'
                    self.send_json_response(data,500)
                    return
        else:
            _log.error("key=create brightcove account msg= account not found %s" %self.api_key)

    '''
    Update Brightcove account details
    '''
    @tornado.gen.engine
    def update_brightcove_integration(self,i_id):
        
        try:
            rtoken = InputSanitizer.to_string(self.get_argument("read_token"))
            wtoken = InputSanitizer.to_string(self.get_argument("write_token"))
            autosync = InputSanitizer.to_bool(self.get_argument("auto_update"))
        except Exception,e:
            _log.error("key=create brightcove account msg= %s" %e)
            data = '{"error": "API Params missing"}'
            self.send_json_response(data,400)
            return

        uri_parts = self.request.uri.split('/')

        result = yield tornado.gen.Task(neondata.BrightcovePlatform.get_account,self.api_key,i_id)
        if result:
            bc = neondata.BrightcovePlatform.create(result)
            bc.read_token = rtoken
            bc.write_token = wtoken
                
            #Auto publish all the previous thumbnails in the account
            if bc.auto_update == False and autosync == True:
                self.autopublish_brightcove_videos(bc)

            bc.auto_update = autosync
            res = yield tornado.gen.Task(bc.save)
            if res:
                data = ''
                self.send_json_response(data,200)
            else:
                data = '{"error": "account not updated"}'
                self.send_json_response(data,500)
        else:
            _log.error("key=update_brightcove_integration msg= no such account %s integration id %s" %(self.api_key,i_id))
            data = '{"error": "Account doesnt exists"}'
            self.send_json_response(data,400)
   
    '''
    Auto publish the videos in the account which are not active
    '''

    @tornado.gen.engine
    def autopublish_brightcove_videos(self,bplatform_account):
        vids = bplatform_account.get_videos()
        
        # No videos in the account
        if not vids:
            return

        keys = [neondata.InternalVideoID.generate(self.api_key,vid) for vid in vids]
        video_results = neondata.VideoMetadata.multi_get(keys)
        video_results = yield tornado.gen.Task(neondata.VideoMetadata.multi_get,keys)
        tids = []
        video_thumb_mappings = {} #vid => [thumbnail metadata ...] 
        update_videos = {} #vid => neon_tid 
      
        #for all videos in account where status is not active 
        for vresult in video_results:
            if vresult:
                tids.extend(vresult.thumbnail_ids)
                video_thumb_mappings[vresult.get_id()] = [] 

            #Get all the thumbnail data for videos that are done
            thumbnails = yield tornado.gen.Task(neondata.ThumbnailIDMapper.get_thumb_mappings,tids)
            for thumb in thumbnails:
                if thumb:
                    vid = thumb.video_id
                    #neondata.InternalVideoID.to_external(thumb.video_id)
                    tdata = thumb.get_metadata()
                    video_thumb_mappings[vid].append(tdata) 
                
        # Check if Neon thumbnail is set as the top rank neon thumbnail
        for vid,thumbs in video_thumb_mappings.iteritems():
            update = True
            neon_tid = None
            for thumb in thumbs:
                if thumb["chosen"] == True and thumb["type"] == 'neon':
                    update = False
                if thumb["type"] == 'neon' and thumb["rank"] == 1:
                    neon_tid = thumb["thumbnail_id"]

            if update and neon_tid is not None:    
                update_videos[vid] = neon_tid

        #update thumbnail for videos without a current neon thumbnail 
        for vid,new_tid in update_videos.iteritems():
            p_vid = neondata.InternalVideoID.to_external(vid)
            result = yield tornado.gen.Task(bplatform_account.update_thumbnail,p_vid,new_tid)
            if not result:
                _log.error("key=autopublish msg=update thumbnail failed for api_key=%s vid=%s tid=%s" %(self.api_key,p_vid,new_tid)) 

    '''
    Get brightcove videos of a given state
    '''

    def get_brightcove_videos_by_state(self,i_id):

        def get_account(result):
            if result:
                bc = neondata.BrightcovePlatform.create(result)
                #Get all videos for this account
                #Aggregate result based on state

                #Get unprocessed list from brightcove 
            else:
                _log.error("key=update_brightcove_integration msg= no such account %s integration id %s" %(self.api_key,i_id))
                data = '{"error": "Account doesnt exists" }'
                self.send_json_response(data,400)

        neondata.BrightcovePlatform.get_account(self.api_key,i_id,get_account)


    ##################################################################
    # Youtube methods
    ##################################################################

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
                na = neondata.NeonUserAccount.create(result)
                na.add_integration(yt.integration_id,yt.key) 
                na.save_integration(yt,saved_account)
            else:
                _log.error("key=create_youtube_integration msg=neon account not found")
                data = '{"error": "account creation issue"}'
                self.send_json_response(data,500)

        def channel_callback(result):
            if result:
                neondata.NeonUserAccount.get_account(self.api_key,neon_account)
            else:
                data = '{"error": "account creation issue"}'
                self.send_json_response(data,500)

        i_id = self.get_argument("integration_id")
        a_token = self.get_argument("access_token")
        r_token = self.get_argument("refresh_token")
        expires = self.get_argument("expires")    
        autosync = self.get_argument("auto_update")
        yt = neondata.YoutubePlatform(self.api_key,i_id,a_token,r_token,expires,autosync)
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
                ya = neondata.YoutubePlatform.create(result)
                ya.access_token = access_token
                ya.refresh_token = refresh_token
                ya.auto_update = auto_update
                ya.save(saved_account)
            else:
                _log.error("key=update youtube account msg= no such account %s integration id %s" %(self.api_key,i_id))
                data = '{"error": "Account doesnt exists" }'
                self.send_json_response(data,400)
       
        try:
            access_token = self.request.get_argument('access_token')
            refresh_token = self.request.get_argument('refresh_token')
            auto_update = self.request.get_argument('auto_update')
            neondata.YoutubePlatform.get_account(self.api_key,i_id,update_account)
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
                ya = neondata.YoutubePlatform.create(result)
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

        neondata.YoutubePlatform.get_account(self.api_key,i_id,get_account)


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
                yt_account = neondata.YoutubePlatform.create(account_response)
                
                
                if (yt_account.videos) > 0:
                    #1.Get videos from youtube api
                    yt_account.get_videos(format_result)
                    
                    #2.Get videos that have been already processed from Neon Youtube account
                    keys = [ neondata.generate_request_key(api_key,j_id) for j_id in yt_account.videos.values()] 
                    neondata.NeonApiRequest.multiget(keys,result_aggregator)
                else:
                    raise Exception("NOT YET IMPL")
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        uri_parts = self.request.uri.split('/')
        neondata.YoutubePlatform.get_account(self.api_key,i_id,account_callback)

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
                self.yt = neondata.YoutubePlatform.create(result)
                job_id = self.yt.videos[vid] 
                yt_request = YoutubeApiRequest.get_request(self.api_key,job_id,get_request) 
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data,500)

        try:
            thumbnail_id = self.get_argument('thumbnail_id')
        except Exception,e:
            _log.exception('type=update brightcove thumbnail' + e.message)
            self.set_status(400)
            self.finish()
            return

        neondata.YoutubePlatform.get_account(self.api_key,i_id,get_account_callback)

###########################################################
## Util Handler 
###########################################################

class UtilHandler(tornado.web.RequestHandler):
    def prepare(self):
        random.seed(340)

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        
        width = 480
        height = 360
        try:
            width = int(self.get_argument("width"))
            height = (self.get_argument("height"))
        except:
            pass
        
        seed = int(hashlib.md5(self.request.uri).hexdigest(),16)
        random.seed(seed)
        im = self._create_random_image(height,width)
        imgstream = StringIO() 
        im.save(imgstream, "jpeg", quality=100)
        imgstream.seek(0)
        data = imgstream.read()
        self.finish(data)

    def _create_random_image(self,h,w):
        pixels = [(0,0,0) for _w in range(h*w)] 
        r = random.randrange(0,255)
        g = random.randrange(0,255)
        b = random.randrange(0,255)
        pixels[0] = (r,g,b)
        im = Image.new("RGB",(h,w))
        im.putdata(pixels)
        return im


## Delete handler only for test accounts, use cautiously

class DeleteHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        a_id = self.request.uri.split('/')[-1]
        
        #make sure you delete only a test account
        if "test" in a_id:
            neondata.NeonUserAccount.remove(a_id)
        
        self.finish()
        
######################################################################
## Brightcove support handler -- Mainly used by brigthcovecontroller 
######################################################################

class BcoveHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self, *args, **kwargs):
        self.finish()

    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self, *args, **kwargs):
        self.internal_video_id = self.request.uri.split('/')[-1]
        method = self.request.uri.split('/')[-2]
        self.a_id = self.request.uri.split('/')[-3] #internal a_id (api_key)
       
        if "update" in method:
            #update thumbnail  (vid, new tid)
            self.update_thumbnail()
        elif "check" in method:
            #Check thumbnail on bcove
            self.check_thumbnail()

    @tornado.gen.engine
    def update_thumbnail(self):
        try:
            new_tid = self.get_argument('thumbnail_id')
        except:
            self.set_status(400)
            self.finish()
            return

        vmdata = yield tornado.gen.Task(neondata.VideoMetadata.get,self.internal_video_id)
        if vmdata:
            i_id = vmdata.integration_id
            jdata = yield tornado.gen.Task(neondata.BrightcovePlatform.get_account,self.a_id,i_id)
            ba = neondata.BrightcovePlatform.create(jdata)
            if ba:
                bcove_vid = neondata.InternalVideoID.to_external(self.internal_video_id) 
                result = yield tornado.gen.Task(ba.update_thumbnail,bcove_vid,new_tid,True)
                if result:
                    self.set_status(200)
                else:
                    _log.error('key=bcove_handler msg=failed to update thumbnail for %s %s'%(self.internal_video_id,new_tid))
                    self.set_status(502)
            else:
                _log.error('key=bcove_handler msg=failed to fetch neondata.BrightcovePlatform %s i_id %s'%(self.a_id,i_id))
                self.set_status(502)
        else:
            _log.error('key=bcove_handler msg=failed to fetch video metadata for %s %s'%(self.internal_video_id,new_tid))
            self.set_status(502)
        self.finish()
    
    @tornado.gen.engine   
    def check_thumbnail(self):
        vmdata = yield tornado.gen.Task(neondata.VideoMetadata.get,self.internal_video_id)
        if vmdata:
            i_id = vmdata.integration_id
            jdata = yield tornado.gen.Task(neondata.BrightcovePlatform.get_account,self.a_id,i_id)
            ba = neondata.BrightcovePlatform.create(jdata)
            if ba:
                bcove_vid = neondata.InternalVideoID.to_external(self.internal_video_id) 
                result = yield tornado.gen.Task(ba.check_current_thumbnail_in_db,bcove_vid)
                if result:
                    self.set_status(200)
                else:
                    _log.error('key=bcove_handler msg=failed to check thumbnail for %s'%self.internal_video_id)
                    self.set_status(502)
            else:
                _log.error('key=bcove_handler msg=failed to fetch neondata.BrightcovePlatform %s i_id %s'%(self.a_id,i_id))
        else:
            _log.error('key=bcove_handler msg=failed to fetch video metadata for %s'%self.internal_video_id)
            self.set_status(502)

        self.finish()

################################################################
### MAIN
################################################################

application = tornado.web.Application([
        (r'/api/v1/removeaccount(.*)', DeleteHandler),
        (r'/api/v1/accounts(.*)', AccountHandler),
        (r'/api/v1/brightcovecontroller(.*)', BcoveHandler),
        (r'/api/v1/utils(.*)', UtilHandler)],debug=True)

def main():
    
    global server
    global BASE_URL 
    BASE_URL = "http://thumbnails.neon-lab.com" if options.local else "http://localhost:8081" 
    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
