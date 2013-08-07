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

import errorlog
import brukva as redis

#Neon classes
from neondata import *

from tornado.options import define, options
define("port", default=8083, help="run on the given port", type=int)

global log
log = errorlog.FileLogger("server")


def sig_handler(sig, frame):
    log.debug('Caught signal: ' + str(sig) )
    tornado.ioloop.IOLoop.instance().stop()


#TODO
''' 
Define API Classes
Request and Return types for every call
REDIS DB Configuration

'''

#TODO :  Implement
# On Bootstrap and periodic intervals, Load important blobs
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
    GET /api/v1/user/getApiKey?userid=UID123
    
    return { "neonapikey" : "MYKEY" }
    
    '''
    def get_api_key(self,userid):
        return

    '''
    #Account summary
    GET /api/v1/user/getAccountDetails?userid=UID123
    
    {"inprogress" : 2 , "finished" : 10 , "failed" : 0, "minssincelastbilling": 200, "status":"active" }  
    
    #status : active/ inactive/ exceeded ... to be decided
    '''
    def get_account_details(self,userid):
        return

    '''
    GET /api/v1/user/getInitialNVideos?userid=UID123&n=10
    #default n=5
    
    { "neonapikey": "MYKEY", "videos": [
        { "videoid":"v123", "title": "sample" , "thumbnail" : "http://thumburl1" ,
                "duration" : 10, "publishdate": "datestring" },
        { "videoid":"v789", "title": "sampl2" , "thumbnail" : "http://thumburl2" ,
                "duration" : 10, "publishdate": "datestring" }
    ]}

    #thumbnail url size = 480x360
    '''
    def get_initial_n_videos(self,userid,n=5):
        return

    '''
    GET /api/v1/user/getNBrightcoveVideos?userid=UID123&n=10
    
    { "neonapikey": "MYKEY", "account" : "brightcove" , "videos": [
        { "videoid":"v123", "title": "sample", "duration" : 10, "publishdate": "datestring" } , "thumbnails" : [ 
            { "url": "http://thumb1" , "current": true , "created": "date",
                        "enabled" : "date", "width" : 480 ,"height":360, "type":"neon1" }, 
            ... 
            { "url": "http://brightcove" , "current": false , "created": "date",
                        "enabled" : "date", "width" : 480 ,"height":360, "type":"brightcove" }]
            
    ]}
   
    #description
    #account = brightcove/brightcoveyt
    #Thumbnail resource - { url, current = true/false, size, created=date, enabled=date/null, type=neon1/neon2../brightcove/yt }  

    '''
    def get_n_brightcove_videos(self,uuid,n=5):
        return

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
    POST /api/v1/user/addUser
    postdata : { "uuid":"XYZ", "name":"sunil", "email":"sun@neon.com","company":"neon" ,
                        "billingplan" : "trial", "date": "datenow"} 

    return {"neonapikey": "NAPIKEY" } 
    HTTP 201 Status code
    
    on error HTTP 400
    { "error" : "message" }
    message = Account already exists
    '''
    def add_user(self,uuid,name,email,company,billing_plan):
        return

    '''
    POST /api/v1/user/addBrightcoveAccount
    postdata : {"uuid":"XYZ", "publisherid":123, "rtoken":"rtk..", "wtoken":"wtk.." } 
    
    return
    HTTP 201 Status code

    on error HTTP 400
    { "error" : "message" }
    #message = Account already exists / rtoken invalid 

    '''
    def addBrightcoveAccount(self,uuid,pubId,rtoken,wtoken):
        return

    '''
    POST /api/v1/user/addYoutubeAccount
    postdata : { "uuid":"XYZ","youtubetoken":"YTtoken" }
    
    return
    HTTP 201 Status code
    '''
    def add_youtube_account(self,uuid,refreshToken):
        return

    '''
    POST /api/v1/user/update_brightcove_account
    postdata : { "uuid":"XYZ", "fields": [ {"$fieldName":"$value"} ] }
   
    #can enter multiple fields in the array
    #fieldnames /value pairs
    
    return
    HTTP 201 Status code
    '''
    def update_brightcove_account(self,uuid,field_value_pairs):
        return

    '''
    POST /api/v1/user/updateUserPreferences
    postdata : { "uuid":"XYZ", "fields": [ {"$fieldName":"$value"} ] }
   
    #can enter multiple fields in the array
    #fieldnames /value pairs
    "autosync" : true /false
    "width" : any integer
    "height" : any integer

    return
    HTTP 200 Status code
    '''
    def update_user_preferences(self,uuid,field_value_pairs):
        return


    '''
    POST /api/v1/user/createBrightcoveApiRequest
    postdata : { "uuid":"XYZ", "videoid": "1234"  } 
   
    return { "jobid": "job1234" }
    HTTP 200
    '''
    def create_brightcove_api_request(self,uuid,videoid,autosync,width,height):
        return


    '''
    POST /api/v1/user/createYoutubeApiRequest
    postdata : { "uuid":"XYZ", "videoid": "YT123", "fileurl":"http://fileloc", "autosync": true } 
   
    return { "jobid": "job1234" }
    HTTP 200
    '''
    def create_youtube_api_request(self,uuid,videoid,fileurl,autosync):
        return

    '''
    POST /api/v1/user/updateBrightcoveThumbnail
    postdata : { "uuid":"XYZ", "platform":"brightcove", "videoid": "1234" ,
                            "thumbnailid" :"thum1234_1" , "width":480, "height": 270 } 

    #platform = brightcove/youtube/....more to come
    return
    HTTP 200
    '''
    def update_brightcove_thumbnail(self,uuid,platform,videoid,thumbnail_id,width,height):
        return
    
    '''
    POST /api/v1/user/updateYoutubeThumbnail
    postdata : { "uuid":"XYZ", "videoid": "1234" , "thumbnailid" :"thum1234_1" , "width":480, "height": 270 } 

    return
    HTTP 200
    '''
    def update_youtube_thumbnail(self,uuid,videoid,thumbnail_id,width,height):
        return
    
class StatusHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        def on_save(result):
            print result
            self.finish()

        def on_result(result):
            print result
            #self.finish()

        u = NeonUser("foo")
        u.save(on_result)
        u = NeonUser("foo")
        u.get(on_result)


####################################################################
# Account Handler

class AccountHandler(tornado.web.RequestHandler):
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

        bc = BrightcoveAccount('test1','test1','2294876105001','cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..','vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..')
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

    ######## HTTP Methods #########

    '''
    Send response to service client
    '''
    def send_json_response(self,data,status=200):
        #self.set_header(application/json)
        self.set_status(status)
        self.write(data)
        self.finish()

    #TODO : API Key to be part of a header X-Neon-API-Key
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):

        #GET /accounts/:account_id/status
        
        #GET /accounts/:account_id/videos
        #self.test_bc_account()
        tapikey = 'e85ebba0cd846e3ed620bc4792ed47d1'
       
        account_id = self.request.uri.split('/')[-2]
        if account_id == "test1":
            self.get_brightcove_videos(tapikey,'test1',5)
        else:
            self.send_json_response('')

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        #POST /accounts

        #POST /accounts/:account_id/brightcove_integrations

        #POST /accounts/:account_id/youtube_integrations
        return

    @tornado.web.asynchronous
    def put(self, *args, **kwargs):
        #PUT /accounts/:account_id
        #PUT or PATCH /brightcove_integrations/:integration_id
        #PUT or PATCH /youtube_integrations/:integration_id
        return

    #### User defined methods

    ''' 
    - Get account details from db, including videos that have been
      processed so far.
    - Multiget all video requests, using jobid 
    - Check cached videos to reduce the multiget ( lazy load)
    - Make a call to Brightcove for videos
    - Aggregrate results and format for the client
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
                    video['integration_id'] = 1234 
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
                if (bc_account.videos) > 0:
                    keys = [ generate_request_key(api_key,j_id) for j_id in bc_account.videos.values()] 
                    keys = bc_account.videos.values() #test
                    NeonApiRequest.multiget(keys,result_aggregator)
                    BrightcoveAccount.find_all_videos(token,limit,result_aggregator)

            except Exception,e:
                print e

        #get brightcove tokens and video info from neondb 
        BrightcoveAccount.get_account(api_key,account_callback)

        #get video metadata from brightcove 
        return

if __name__ == "__main__":
    application = tornado.web.Application([
        (r'/api/v1/user/(.*)', UserHandler),
        (r'/api/v1/accounts/(.*)', AccountHandler),
        #(r'/api/v1/jobs/(.*)', JobHandler),
        (r'/test', StatusHandler)])
    
    global server
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
