#!/usr/bin/python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.httpclient
import json
import multiprocessing
import Queue
import signal
import time
import urllib
import urlparse
import youtube
import hashlib
import re
import properties
import os
import utils.ps

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from StringIO import StringIO

from neon_apikey import APIKey
from brightcove_metadata import BrightcoveMetadata

from supportServices.neondata import *
import utils.neon

#Tornado options
from utils.options import define, options
define("port", default=8081, help="run on the given port", type=int)
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3
    
_log = logging.getLogger(__name__)

dir = os.path.dirname(__file__)

#=============== Global Handlers ======================================#

def format_status_json(state,timestamp,data=None):

    status = {}
    result = {}

    status['state'] = state
    status['timestamp'] = timestamp
    result['status'] = status
    result['result'] = ''

    if data is not None:
        result['result'] = data

    json = tornado.escape.json_encode(result)
    return json

def check_remote_ip(request):
    is_remote = False
    if request.headers.has_key('X-Real-Ip'):
        real_ip = request.headers['X-Real-Ip']
        if re.search('^10.*',real_ip) is None:
            if re.search('^127.*',real_ip) is None:
                is_remote = True
            else:
                pass
        else:
            pass

    return is_remote


## ===================== API ===========================================#
## Internal Handlers and not be exposed externally
## ===================== API ===========================================#

class RegisterBrightcove(tornado.web.RequestHandler):
    def post(self, *args, **kwargs):
        try:
            json_data = self.get_argument('JSONRPC')
            data = tornado.escape.json_decode(json_data)
            uri = self.request.uri
            publisher_name = data['publisherName'].strip()
            read_token   = data['rtoken'] 
            write_token  = data['wtoken']
            publisher_id = data['publisherID']
            ak = APIKey()
            neon_api_key = ak.add_key(publisher_name) 
            bm = BrightcoveMetadata(publisher_name,read_token,write_token,neon_api_key,publisher_id)
            bm.save()
        
            s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
            s3bucket_name = properties.S3_CUSTOMER_ACCOUNT_BUCKET_NAME
            s3bucket = Bucket(name = s3bucket_name, connection = s3conn)
            k = Key(s3bucket)
            k.key = 'brightcoveCustomerTokens.json'
            k.set_contents_from_filename('brightcoveCustomerTokens.json')

            #api key
            k.key = 'apikeys.json'
            k.set_contents_from_filename('apikeys.json')
        except Exception,e:
            _log.error("key=RegisterBrightcove msg=exception " + e.__str__())
            raise tornado.web.HTTPError(400)

        self.write("Success")
        self.finish()

class StatsHandler(tornado.web.RequestHandler):
    def get(self, *args, **kwargs):
        size = -1
        try:
            size = global_api_work_queue.qsize() #Doesn't work on mac osX
        except:
            pass

        if check_remote_ip(self.request) == False:
            self.write("Qsize = " + str(size) )
        self.finish()

class DequeueHandler(tornado.web.RequestHandler):
    """ DEQUEUE JOB Handler - The queue stores data in json format already """
    def get(self, *args, **kwargs):
        
        try:
            element = global_api_work_queue.get_nowait()
            #send http response
            h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
            self.write(str(element))
        
        except Queue.Empty:
            #Send Queue empty message as a string {}
            self.write("{}")

        except Exception,e:
            _log.error("key=dequeue_handler msg=error from work queue")
            raise tornado.web.HTTPError(500)

        self.finish()

class RequeueHandler(tornado.web.RequestHandler):
    """ REQUEUE JOB Handler"""
    def post(self, *args, **kwargs):
        
        try:
            _log.info("key=requeue_handler msg=requeing ")
            #data = tornado.escape.url_unescape(self.request.body, encoding='utf-8')
            data = self.request.body
            #TODO Verify data Format
            global_api_work_queue.put(data)
        except Exception,e:
            _log.error("key=requeue_handler msg=error " + e.__str__())
            raise tornado.web.HTTPError(500)

        self.finish()

## ===================== API ===========================================#
# External Handlers
## ===================== API ===========================================#

class GetResultsHandler(tornado.web.RequestHandler):
    """ Return results gzipped """
    def get(self, *args, **kwargs):
        try:
            query = self.request.query
            params = urlparse.parse_qs(query)
            uri = self.request.uri
            api_key = params[properties.API_KEY][0]
            request_id = params[properties.REQUEST_UUID_KEY][0]
            s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
            s3bucket_name = properties.S3_BUCKET_NAME
            s3bucket = Bucket(name = s3bucket_name, connection = s3conn)
            k = Key(s3bucket)
            try:
                k.key = str(api_key) + "/" + str(request_id) + "/"+ 'result.tar.gz'
                data = k.get_contents_as_string()
                self.write(data)
            except Exception,e:
                _log.exception("key=getresultshandler msg=traceback")
                raise tornado.web.HTTPError(400)
            self.finish()

        except:
            _log.exception("key=getresultshandler msg=general traceback")

class JobStatusHandler(tornado.web.RequestHandler):
    """ JOB Status Handler  """
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
       
        def db_callback(result):
            self.set_header("Content-Type", "application/json")
            if not result:
                self.set_status(400)
                resp = '{"status":"no such job"}'
                self.finish()
                return

            self.write(result)
            self.finish()

        try:
            api_key = self.get_argument(properties.API_KEY)
            job_id  = self.get_argument(properties.REQUEST_UUID_KEY)
            NeonApiRequest.get_request(api_key,job_id,db_callback)

        except Exception,e:
            _log.error("key=jobstatus_handler msg=exception " + e.__str__())
            raise tornado.web.HTTPError(400)


class GetThumbnailsHandler(tornado.web.RequestHandler):
    test_mode = False
    parsed_params = {}

    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self, *args, **kwargs):

        #insert job in to user account
        def update_account(result):
            if not result:
                _log.error("key=thumbnail_handler update account  msg=video not added to account")
                self.write(response_data)
                self.set_status(201)
                self.finish()
            else:
                self.set_status(201)
                self.write(response_data)
                self.finish()

        def get_yt_account(result):

            #For brightcove account, its saved
            if result:
                if "youtubeaccount" in result:
                    yt = Youtube.create(result)
                    yt.add_video(vid,job_id)
                    yt.save(update_account)
            else:
                _log.error("key=thumbnail_handler update account  msg=account not found or api key error")
                self.set_status(502)
                self.finish()
               
        def get_platform(nplatform):
            if nplatform:
                nplatform.add_video(vid,job_id)
                nplatform.save(update_account)
            else:
                _log.error("key=thumbnail_handler update account  msg=account not found or api key error")

        #DB Callback
        def saved_request(result):
            if not result:
                _log.error("key=thumbnail_handler  msg=request save failed: ")
                self.set_status(502)
                self.finish()
            else:
                if request_type == 'youtube':
                    YoutubePlatform.get_account(api_key,get_yt_account) #i_id ?  
                elif request_type == 'neon':
                    NeonPlatform.get_account(api_key,get_platform) 
                else:
                    self.set_status(201)
                    self.write(response_data)
                    self.finish()
        
        try:
            params = tornado.escape.json_decode(self.request.body)
            uri = self.request.uri
            self.parsed_params = {}
            api_request = None 

            #Verify essential parameters
            try:
                api_key = params[properties.API_KEY]
                vid = params[properties.VIDEO_ID]
                title = params[properties.VIDEO_TITLE]
                url = params[properties.VIDEO_DOWNLOAD_URL]
                http_callback = params[properties.CALLBACK_URL]
            except KeyError,e:
                raise Exception("params not set") #convert to custom exception

            #TODO Verify API Key
            #if not self.verify_api_key(api_key):
            #    raise Exception("API key invalid")
            
            #compare with supported api methods
            if params.has_key(properties.TOP_THUMBNAILS):
                api_method = "topn"
                api_param = min(int(params[properties.TOP_THUMBNAILS]),
                        properties.MAX_THUMBNAILS)
            elif params.has_key(properties.THUMBNAIL_RATE):
                api_method = "rate"
                api_param = params[properties.THUMBNAIL_RATE]
            else:
                #DEFAULT
                raise Exception("api method not supported")
           
            #Generate JOB ID  
            #Use Params that can change to generate UUID, support same video to be processed with diff params
            intermediate = api_key + str(vid) + api_method + str(api_param) 
            job_id = hashlib.md5(intermediate).hexdigest()
           
            #Identify Request Type
            if "brightcove" in self.request.uri:
                pub_id  = params[properties.PUBLISHER_ID] #publisher id
                p_thumb = params[properties.PREV_THUMBNAIL]
                rtoken = params[properties.BCOVE_READ_TOKEN]
                wtoken = params[properties.BCOVE_WRITE_TOKEN]
                autosync = params["autosync"]
                request_type = "brightcove"
                i_id = params[properties.INTEGRATION_ID]
                api_request = BrightcoveApiRequest(job_id,api_key,vid,title,url,
                                        rtoken,wtoken,pub_id,http_callback,i_id)
                api_request.previous_thumbnail = p_thumb 
                api_request.autosync = autosync

            elif "youtube" in self.request.uri:
                request_type = "youtube"
                access_token = params["access_token"]
                refresh_token = params["refresh_token"]
                expiry = params["token_expiry"]
                autosync = params["autosync"]
                api_request = YoutubeApiRequest(job_id,api_key,vid,title,url,
                                        access_token,refresh_token,expiry,http_callback)
                api_request.previous_thumbnail = "http://img.youtube.com/vi/" + vid + "maxresdefault.jpg"

            else:
                request_type = "neon"
                api_request = NeonApiRequest(job_id,api_key,vid,title,url,
                        request_type,http_callback)
            
            #API Method
            api_request.set_api_method(api_method,api_param)
            api_request.submit_time = str(time.time())
            api_request.state = RequestState.SUBMIT

            #Validate Request & Insert in to Queue (serialized/json)
            job_result = yield tornado.gen.Task(NeonApiRequest.conn.get, api_request.key)
            if False and job_result is not None:
                response_data = '{"error":"duplicate job"}' 
                self.write(response_data)
                self.finish()
                return
            
            #TODO: insert in to work queue after saving request in db
            #TODO (2): keep a video id queue in db for hot swapping the Q
            json_data = api_request.to_json()
            global_api_work_queue.put(json_data)
            
            #Response for the submission of request
            response_data = "{\"job_id\":\"" + job_id + "\"}"
            
            result = yield tornado.gen.Task(api_request.save)
            result = api_request.save()
            if not result:
                _log.error("key=thumbnail_handler  msg=request save failed: ")
                self.set_status(502)
            else:
                if request_type == 'youtube':
                    YoutubePlatform.get_account(api_key,get_yt_account) #i_id ?  
                elif request_type == 'neon':
                    NeonPlatform.get_account(api_key,get_platform) 
                else:
                    self.set_status(201)
                    self.write(response_data)
                    self.finish()

        except Exception,e:
            _log.error("key=thumbnail_handler msg=" + e.__str__());
            self.set_status(400)
            self.finish("<html><body>Bad Request " + e.__str__() + " </body></html>")
            return

    def verify_api_key(selfi,key):
        fname = os.path.join(dir,properties.API_KEY_FILE) 
        with open(fname, 'r') as f:
            json = f.readline()
        
        keys = tornado.escape.json_decode(json)
        if key not in keys.values():
            return False
        
        return True


###########################################
# TEST Handlers 
###########################################

class TestCallback(tornado.web.RequestHandler):
    """ Test callback Handler to print the api output """
    def post(self, *args, **kwargs):
        
        try:
            _log.info("key=testcallback msg=output: " + self.request.body)
        except Exception,e:
            raise tornado.web.HTTPError(500)  
            _log.error("key=testcallback msg=error recieving message")
        
        self.finish()

###########################################
# Create Tornado server application
###########################################
global_api_work_queue = multiprocessing.Queue()

application = tornado.web.Application([
    (r'/api/v1/submitvideo/(.*)', GetThumbnailsHandler),
    (r"/stats",StatsHandler),
    (r"/dequeue",DequeueHandler),
    (r"/requeue",RequeueHandler),
    (r"/testcallback",TestCallback),
    (r'/api/v1/jobstatus',JobStatusHandler),
    (r'/api/v1/getresults',GetResultsHandler),    
    (r'/registerbrightcove',RegisterBrightcove),    
])

def main():
    utils.neon.InitNeon()
    global server
    server = tornado.httpserver.HTTPServer(application)
    os.ps.utils.register_tornado_shutdown(server)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
	main()
