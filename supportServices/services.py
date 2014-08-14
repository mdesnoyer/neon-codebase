#!/usr/bin/env python
'''
This script launches the services server which hosts Services 
that neon web account uses.
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
import signal
import time
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
import traceback
import utils.neon

from StringIO import StringIO
from supportServices import neondata
from utils.inputsanitizer import InputSanitizer
from utils import statemon
import utils.sync
from utils.options import define, options
define("port", default=8083, help="run on the given port", type=int)
define("local", default=0, help="call local service", type=int)

import logging
_log = logging.getLogger(__name__)

def sig_handler(sig, frame):
    ''' signal handler'''
    _log.debug('Caught signal: %s'%sig)
    tornado.ioloop.IOLoop.instance().stop()

def CachePrimer():
    '''
    #TODO: On Bootstrap and periodic intervals, 
    #Load important blobs that don't change with TTL From storage in to cache
    '''
    pass

#Monitoring variables
statemon.define('bad_request', int) #HTTP 400s
statemon.define('bad_gateway', int) #HTTP 502s
statemon.define('internal_err', int) #HTTP 500s
statemon.define('total_requests', int) #all requests 

#Place holder images for processing
placeholder_images = [
                'http://cdn.neon-lab.com/webaccount/neon_processing_1.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_2.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_3.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_4.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_5.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_6.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_7.png',
                ]
################################################################################
# Helper classes  
################################################################################

class GetVideoStatusResponse(object):
    ''' VideoStatus response on *_integration calls '''
    def __init__(self, items, count, page_no=0, page_size=100,
            processing_count=0, recommended_count=0, published_count=0):
        self.items = items
        self.total_count = count 
        self.page_no = page_no
        self.page_size = page_size
        self.processing_count = processing_count
        self.recommended_count = recommended_count
        self.published_count = published_count

    def to_json(self):
        ''' to json''' 
        for item in self.items:
            for thumb in item['thumbnails']:
                score = thumb['model_score']
                if score == float('-inf') or score == '-inf' or score is None:
                    thumb['model_score'] =  -1 * sys.maxint

        return json.dumps(self, default=lambda o: o.__dict__)

################################################################################
# Account Handler
################################################################################

class AccountHandler(tornado.web.RequestHandler):
    ''' /api/v1/accounts handler '''
    
    def prepare(self):
        ''' Called before every request is processed '''
       
        # If POST or PUT, then decode the json arguments
        if not self.request.method == "GET":
            ctype = self.request.headers.get("Content-Type")
            if ctype is None:
                data = '{"error": "missing content type header use json or\
                urlencoded"}'
                self.send_json_response(data, 400)
                return

            ## Convert any json input in to argument dictionary
            if self.request.body and "application/json" in ctype:
                try:
                    json_data = json.loads(self.request.body)
                    for k, v in json_data.items():
                        # Tornado expects values in the argument dict to be lists.
                        # in tornado.web.RequestHandler._get_argument the last
                        # argument is returned.

                        # The value has to be unicode, so convert it
                        if not isinstance(v, basestring):
                            v = json.dumps(v)
                        json_data[k] = [v]
                    #clear the request body arguments from dict
                    #self.request.arguments.pop(self.request.body)
                    self.request.arguments.update(json_data)
                except ValueError, e:
                    self.send_json_response('{"error": "invalid json request"}', 400)

        self.api_key = self.request.headers.get('X-Neon-API-Key') 
        if self.api_key == None:
            if self.request.uri.split('/')[-1] == "accounts" \
                    and self.request.method == 'POST':
                #only account creation call can lack this header
                return
            else:
                _log.exception("key=initialize msg=api header missing")
                data = '{"error": "missing or invalid api key" }'
                self.send_json_response(data, 400)
                return

    @tornado.gen.engine
    def async_sleep(self, secs):
        ''' async sleep'''
        yield tornado.gen.Task(tornado.ioloop.IOLoop.current().add_timeout, 
                time.time() + secs)

    @tornado.gen.engine
    def delayed_callback(self, secs, callback):
        ''' delay a callback by x secs'''
        yield tornado.gen.Task(tornado.ioloop.IOLoop.current().add_timeout, 
                time.time() + secs)
        callback(secs)

    #### Support Functions #####

    @tornado.gen.engine 
    def verify_account(self, a_id, callback=None):
        ''' verify account '''

        api_key = yield tornado.gen.Task(neondata.NeonApiKey.get_api_key, a_id)
        if api_key == self.api_key:
            callback(True)
        else:
            data = '{"error":"invalid api_key or account id"}'
            _log.warning(("key=verify_account "
                          "msg=api key doesn't match for account %s") % a_id)
            self.send_json_response(data, 400)
            callback(False)
        
    ######## HTTP Methods #########

    def send_json_response(self, data, status=200):
        '''Send response to service client '''
       
        statemon.state.increment('total_requests')
        if status == 400 or status == 409:
            statemon.state.increment('bad_request')
        elif status == 502:
            statemon.state.increment('bad_gateway')
        elif status > 201:
            statemon.state.increment('internal_err')

        self.set_header("Content-Type", "application/json")
        self.set_status(status)
        self.write(data)
        self.finish()
    
    def method_not_supported(self):
        ''' unsupported method response'''
        data = '{"error":"api method not supported or REST URI is incorrect"}'
        self.send_json_response(data, 400)

    @tornado.gen.engine
    def get_platform_account(self, i_type, i_id, callback=None):
        
        #Get account/integration
        
        platform_account = None

        # Loose comparison
        if "brightcove" in i_type:
            platform_account = yield tornado.gen.Task(
                                        neondata.BrightcovePlatform.get_account,
                                        self.api_key, i_id)
        elif i_type == "ooyala":
            platform_account = yield tornado.gen.Task(
                                        neondata.OoyalaPlatform.get_account,
                                        self.api_key, i_id)
        elif i_type == "neon": 
            platform_account = yield tornado.gen.Task(
                                        neondata.NeonPlatform.get_account,
                                        self.api_key)

        callback(platform_account)


    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self, *args, **kwargs):
        ''' 
        GET /accounts/:account_id/status
        GET /accounts/:account_id/[brightcove_integrations|youtube_integrations] \
                /:integration_id/videos
        '''
        
        _log.info("Request %r" %self.request)

        uri_parts = self.request.uri.split('/')

        #NOTE: compare string in parts[-1] since get args aren't cleaned up
        if "accounts" in self.request.uri:
            #Get account id
            try:
                a_id = uri_parts[4]
                itype = uri_parts[5]
                i_id = uri_parts[6]
                method = uri_parts[7]
            except Exception, e:
                _log.error("key=get request msg=  %s" %e)
                self.set_status(400)
                self.finish()
                return
            
            #Verify Account
            is_verified = yield tornado.gen.Task(self.verify_account, a_id)
            if not is_verified:
                return

            if method == "status":
                #self.get_account_status(itype,i_id)
                self.send_json_response('{"error":"not yet impl"}',200)

            elif method == "tracker_account_id":
                self.get_tracker_account_id()

            elif method == "abteststate":
                video_id = uri_parts[-1].split('?')[0]
                self.get_abteststate(video_id)

            elif method == "videos" or "videos" in method:
                video_state = None
                #NOTE: Video ids here are external video ids
                ids = self.get_argument('video_ids', None)
                video_ids = None if ids is None else ids.split(',') 
                if len(uri_parts) == 9:
                    video_state = uri_parts[-1].split('?')[0] 

                if itype  == "neon_integrations":
                    #self.get_video_status_neon(video_ids, video_state)
                    self.get_video_status("neon", i_id, video_ids, video_state)
            
                elif itype  == "brightcove_integrations":
                    #self.get_video_status_brightcove(i_id, video_ids, video_state)
                    self.get_video_status("brightcove", i_id, video_ids, video_state)

                elif itype == "ooyala_integrations":
                    #self.get_video_status_ooyala(i_id, video_ids, video_state)
                    self.get_video_status("ooyala", i_id, video_ids, video_state)
                
                elif itype == "youtube_integrations":
                    self.get_youtube_videos(i_id)
            else:
                _log.warning(('key=account_handler '
                              'msg=Invalid method in request %s method %s') 
                              % (self.request.uri, method))
                self.send_json_response("API not supported", 400)

        else:
            _log.warning(('key=account_handler '
                          'msg=Account missing in request %s')
                          % self.request.uri)
            self.send_json_response("API not supported", 400)

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        ''' Post methods '''

        uri_parts = self.request.uri.split('/')
        a_id = None
        method = None
        itype = None
        i_id = None
        try:
            a_id = uri_parts[4]
            itype = uri_parts[5]
            i_id = uri_parts[6]
            method = uri_parts[7]
        except Exception,e:
            pass
      
        #POST /accounts ##Crete neon user account
        if a_id is None and itype is None:
            #len(ur_parts) == 4
            try:
                a_id = self.get_argument("account_id") 
                self.create_account_and_neon_integration(a_id)
            except:
                data = '{"error":"account id not specified"}'
                self.send_json_response(data, 400)                
            return

        #Account creation
        if method is None:
            #POST /accounts/:account_id/brightcove_integrations
            if "brightcove_integrations" in self.request.uri:
                self.create_brightcove_integration()
        
            #POST /accounts/:account_id/youtube_integrations
            elif "youtube_integrations" in self.request.uri:
                self.create_youtube_integration()
            
            elif "ooyala_integrations" in self.request.uri:
                self.create_ooyala_integration()

        #Video Request creation   
        elif method == 'create_video_request':
            if i_id is None:
                data = '{"error":"integration id not specified"}'
                self.send_json_response(data, 400)
                return

            if "brightcove_integrations" == itype:
                self.create_brightcove_video_request(i_id)
            elif "youtube_integrations" == itype:
                self.create_youtube_video_request(i_id)
            elif "neon_integrations" == itype:
                self.create_neon_video_request(i_id)
            #elif "ooyala_integrations" == itype:
            #    self.create_ooyala_video_request(i_id)
            else:
                self.method_not_supported()

    @tornado.web.asynchronous
    def put(self, *args, **kwargs):
        '''
        /accounts/:account_id/[brightcove_integrations|youtube_integrations] \
                /:integration_id/{method}
        '''
       
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

        #Update Accounts
        if method is None or method == "update":
            if "brightcove_integrations" == itype:
                self.update_brightcove_integration(i_id)
            elif "youtube_integrations" == itype:
                self.update_youtube_account(i_id)
            elif "ooyala_integrations" == itype:
                self.update_ooyala_integration(i_id)
            elif itype is None:
                #Update basic neon account
                self.method_not_supported()
            else:
                self.method_not_supported()

        #Update the thumbnail property
        elif method == "thumbnails":
            tid = uri_parts[-1].split('?')[0]
            self.update_thumbnail_property(tid)
        
        #Update the thumbnail
        elif method == "videos":
            if len(uri_parts) == 9:
                vid = uri_parts[-1]
                if vid == "null":
                    self.send_json_response('{"error": "video id null" }', 400)
                    return

                i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
                try:
                    new_tid = self.get_argument('current_thumbnail', None)
                    if new_tid is None:
                        # custom thumbnail upload
                        thumbs = json.loads(self.get_argument('thumbnails'))
                        self.upload_video_custom_thumbnail(itype, i_id, i_vid,
                                                            thumbs)
                except Exception, e:
                    data = '{"error": "missing thumbnail_id or thumbnails argument"}'
                    self.send_json_response(data, 400)
                    return

                if "brightcove_integrations" == itype:
                    self.update_video_brightcove(i_id, i_vid, new_tid)

                elif "youtube_integrations" == itype:
                    self.update_youtube_video(i_id, i_vid)
                    return
                
                elif "ooyala_integrations" == itype:
                    #Temp support for both arguments
                    if new_tid is None:
                        self.get_argument('current_thumbnail', None)
                        if new_tid is None:
                            data = '{"error": "missing thumbnail_id argument"}'
                            self.send_json_response(data, 400)
                            return
                    self.update_video_ooyala(i_id, i_vid, new_tid)
                    return
            else:
                self.method_not_supported()
        else:
            _log.error("Method not supported")
            self.set_status(400)
            self.finish()
    
    ############## User defined methods ###########

    def get_tracker_account_id(self):
        '''
        Return tracker account id associated with the neon user account
        '''
        nu = neondata.NeonUserAccount.get_account(self.api_key)
        if nu:
            data = ('{"tracker_account_id":"%s","staging_tracker_account_id":"%s"}'
                    %(nu.tracker_account_id, nu.staging_tracker_account_id))
            self.send_json_response(data, 200)
        else:
            data = '{"error":"account not found"}'
            self.send_json_response(data, 400)


    def get_neon_videos(self):
        ''' Get Videos which were called from the Neon API '''
        self.send_json_response('{"msg":"not yet implemented"}', 200)
   
    ## Submit a video request to Neon Video Server
    @tornado.gen.engine
    def submit_neon_video_request(self, api_key, video_id, video_url, 
                    video_title, topn, callback_url, default_thumbnail):

        request_body = {}
        request_body["topn"] = topn 
        request_body["api_key"] = api_key 
        request_body["video_id"] = video_id 
        request_body["video_title"] = \
                video_url.split('//')[-1] if video_title is None else video_title 
        request_body["video_url"] = video_url
        #client_url = 'http://thumbnails.neon-lab.com/api/v1/submitvideo/topn'
        client_url = 'http://50.19.216.114:8081/api/v1/submitvideo/topn' #use EIP
        if options.local == 1:
            client_url = 'http://localhost:8081/api/v1/submitvideo/topn'
            request_body["callback_url"] = callback_url 
        body = tornado.escape.json_encode(request_body)
        hdr = tornado.httputil.HTTPHeaders({"Content-Type": "application/json"})
        req = tornado.httpclient.HTTPRequest(url=client_url,
                                             method="POST",
                                             headers=hdr,
                                             body=body,
                                             request_timeout=30.0,
                                             connect_timeout=10.0)
        
        result = yield tornado.gen.Task(http_client.fetch, req)
        
        if result.code == 409:
            data = '{"error":"url already processed","video_id":"%s"}'%video_id
            self.send_json_response(data, 409)
            return
        if result.error:
            _log.error("key=create_neon_video_request_via_api "
                    "msg=thumbnail api error %s" %result.error)
            data = '{"error":"neon thumbnail api error"}'
            self.send_json_response(data, 502)
            return

        #Success
        self.send_json_response(result.body, 200)
    
    @tornado.gen.engine
    def create_neon_video_request_via_api(self):
        '''
        Endpoint for API calls to submit a video request
        '''
        video_id = self.get_argument('video_id', None)
        video_url = self.get_argument('video_url', "")
        video_url = video_url.replace("www.dropbox.com", 
                                "dl.dropboxusercontent.com")
        title = self.get_argument('video_title', None)
        neon_api_key = self.get_argument('api_key', None)
        topn = self.get_argument('topn', 1)
        callback_url = self.get_argument('callback_url', None)
        default_thumbnail = self.get_argument('default_thumbnail', None)
        
        if video_id is None or video_url == "" or neon_api_key is None:
            _log.error("key=create_neon_video_request_via_api "
                    "msg=malformed request or missing arguments")
            self.send_json_response('{"error":"missing video_url"}', 400)
            return
        
        #Create Neon API Request
        self.submit_neon_video_request(neon_api_key, video_id, video_url,
                            video_title, topn, callback_url, default_thumbnail)

    @tornado.gen.engine
    def create_neon_video_request(self, i_id):
        ''' neon platform request '''

        title = None
        try:
            video_url = self.get_argument('video_url')
            title = self.get_argument('title')
            #video_url = video_url.split('?')[0]
            video_url = video_url.replace("www.dropbox.com", 
                                "dl.dropboxusercontent.com")
        except:
            _log.error("key=create_neon_video_request "
                    "msg=malformed request or missing arguments")
            self.send_json_response('{"error":"missing video_url"}', 400)
            return
        
        invalid_url_links = ["youtube.com", "youtu.be"]
        for invalid_url_link in invalid_url_links:
            if invalid_url_link in video_url:
                data = '{"error":"link given is invalid or not a video file"}'
                self.send_json_response(data, 400)
                return

        #Validate link
        invalid_content_types = ['text/html', 'text/plain', 'application/json',
                    'application/x-www-form-urlencoded', 
                    'text/html; charset=utf-8', 'text/html;charset=utf-8']
        http_client = tornado.httpclient.AsyncHTTPClient()
        headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})
       
        req = tornado.httpclient.HTTPRequest(url=video_url, headers=headers,
                        use_gzip=False, request_timeout=1.5)
        vresponse = yield tornado.gen.Task(http_client.fetch, req)
       
        #If timeout, Ignore for now, may be a valid slow link.  
        if vresponse.code != 599:
            ctype = vresponse.headers.get('Content-Type')
            if vresponse.error or ctype is None or ctype.lower() in invalid_content_types:
                data = '{"error":"link given is invalid or not a video file"}'
                self.send_json_response(data, 400)
                return

        video_id = hashlib.md5(video_url).hexdigest()
        request_body = {}
        request_body["topn"] = 6 
        request_body["api_key"] = self.api_key 
        request_body["video_id"] = video_id 
        request_body["video_title"] = \
                video_url.split('//')[-1] if title is None else title 
        request_body["video_url"]   = video_url
        #client_url = 'http://thumbnails.neon-lab.com/api/v1/submitvideo/topn'
        client_url = 'http://50.19.216.114:8081/api/v1/submitvideo/topn' #use EIP
        if options.local == 1:
            client_url = 'http://localhost:8081/api/v1/submitvideo/topn'
            request_body["callback_url"] = "http://localhost:8081/testcallback"
        else:
            request_body["callback_url"] = \
                    "http://thumbnails.neon-lab.com/testcallback"
        body = tornado.escape.json_encode(request_body)
        hdr = tornado.httputil.HTTPHeaders({"Content-Type": "application/json"})
        req = tornado.httpclient.HTTPRequest(url=client_url,
                                             method="POST",
                                             headers=hdr,
                                             body=body,
                                             request_timeout=30.0,
                                             connect_timeout=10.0)
        
        result = yield tornado.gen.Task(http_client.fetch, req)
        
        if result.code == 409:
            data = '{"error":"url already processed","video_id":"%s"}'%video_id
            self.send_json_response(data, 409)
            return
        
        if result.error:
            _log.error("key=create_neon_video_request "
                    "msg=thumbnail api error %s" %result.error)
            data = '{"error":"neon thumbnail api error"}'
            self.send_json_response(data, 502)
            return

        #note: job id gets inserted into Neon platform account on video server
        t_urls = [] 
        thumbs = []
        im_index = int(hashlib.md5(video_id).hexdigest(), 16) \
                                        % len(placeholder_images)
        placeholder_url = placeholder_images[im_index] 
        t_urls.append(placeholder_url)
        ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tm = neondata.ThumbnailMetadata(0, video_id, t_urls, ctime, 0, 0,
                                        neondata.ThumbnailType.CENTERFRAME,
                                        0, 0)
        thumbs.append(tm.to_dict_for_video_response())
        vr = neondata.VideoResponse(video_id,
                            neondata.RequestState.PROCESSING,
                            "neon",
                            "0",
                            title,
                            None, #duration
                            time.time() * 1000,
                            0, 
                            thumbs)
        self.send_json_response(vr.to_json(), 200)

    ##### Generic get_video_status #####

    @tornado.gen.engine
    def get_video_status(self, i_type, i_id, vids, video_state=None):
        '''
         i_type : Integration type neon/brightcove/ooyala
         i_id   : Integration ID
         vids   : Platform video ids
         video_state: State of the videos to be requested 

         Get platform video to populate in the web account
         Get account details from db, including videos that have been
         processed so far.
         Multiget all video requests, using jobid 
         Check cached videos to reduce the multiget ( lazy load)
         Aggregrate results and format for the client
        '''
        #counters 
        c_published = 0
        c_processing = 0
        c_recommended = 0

        #videos by state
        p_videos = []
        r_videos = []
        a_videos = []
        f_videos = [] #failed videos 

        page_no = 0 
        page_size = 300
        try:
            page_no = int(self.get_argument('page_no'))
            page_size = min(int(self.get_argument('page_size')), 300)
        except:
            pass

        result = {}
        incomplete_states = [
            neondata.RequestState.SUBMIT, neondata.RequestState.PROCESSING,
            neondata.RequestState.REQUEUED]
        
        failed_states = [neondata.RequestState.INT_ERROR, 
                    neondata.RequestState.FAILED]
        
        #1 Get job ids for the videos from account, get the request status
        platform_account = yield tornado.gen.Task(self.get_platform_account, i_type, i_id)

        if not platform_account:
            _log.error("key=get_video_status_%s msg=account not found" %i_type)
            self.send_json_response("%s account not found"%i_type, 400)
            return
       
        #return all videos in the account
        if vids is None:
            vids = platform_account.get_videos()
       
        # No videos in the account
        if not vids:
            vstatus_response = GetVideoStatusResponse(
                        [], 0, page_no, page_size,
                        c_processing, c_recommended, c_published)
            data = vstatus_response.to_json() 
            self.send_json_response(data, 200)
            return
       
        total_count = len(vids)
        job_request_keys = [] 
        for vid in vids:
            try:
                jid = neondata.generate_request_key(self.api_key,
                                                    platform_account.videos[vid])
                job_request_keys.append(jid)
            except:
                pass #job id not found
 
        #2 Get Job status
        #jobs that have completed, used to reduce # of keys to fetch 
        completed_videos = [] 

        #get all requests and populate video response object in advance
        requests = yield tornado.gen.Task(neondata.NeonApiRequest.get_requests,
                    job_request_keys) 
        ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for request, vid in zip(requests, vids):
            if not request:
                result[vid] = None #indicate job not found
                continue

            thumbs = None
            status = neondata.RequestState.PROCESSING 
            if request.state in incomplete_states:
                t_urls = []
                thumbs = []
                t_type = i_type
                if i_type == "neon":
                    im_index = int(hashlib.md5(vid).hexdigest(), 16) \
                                        % len(placeholder_images)
                    placeholder_url = placeholder_images[im_index] 
                    t_urls.append(placeholder_url)
                else:
                    t_urls.append(request.previous_thumbnail)

                tm = neondata.ThumbnailMetadata(
                    0, #Create TID 0 as a temp id for previous thumbnail
                    neondata.InternalVideoID.generate(self.api_key, vid),
                    t_urls, ctime, 0, 0, t_type, 0, 0)
                thumbs.append(tm.to_dict_for_video_response())
                p_videos.append(vid)
            elif request.state in failed_states:
                status = "failed" 
                thumbs = None
            else:
                #Jobs have finished
                #append to completed_videos 
                #for backward compatibility with all videos api call 
                completed_videos.append(vid)
                status = "finished"
                thumbs = None
                if request.state == neondata.RequestState.FINISHED:
                    r_videos.append(vid) #finshed processing
                elif request.state == neondata.RequestState.ACTIVE:
                    a_videos.append(vid) #published /active 

            pub_date = None if not request.__dict__.has_key('publish_date') \
                            else request.publish_date
            pub_date = int(pub_date) if pub_date else None #type
            vr = neondata.VideoResponse(vid,
                              status,
                              request.request_type,
                              i_id,
                              request.video_title,
                              None, #duration
                              pub_date,
                              0, #current tid,add fake tid
                              thumbs)
            result[vid] = vr
         
        #2b Filter videos based on state as requested
        if video_state:
            if video_state == "published": #active
                vids = completed_videos = a_videos
            elif video_state == "recommended":
                vids = completed_videos = r_videos
            elif video_state == "processing":
                vids = p_videos
                completed_videos = []
            else:
                _log.error("key=get_video_status_%s" 
                        " msg=invalid state requested" %i_type)
                self.send_json_response('{"error":"invalid state request"}', 400)
                return

        #2c Pagination, case: There are more vids than page_size
        if len(vids) > page_size:
            #This means paging is valid
            #check if for the page_no request there are 
            #sort video ids
            s_index = page_no * page_size
            e_index = (page_no +1) * page_size
            vids = sorted(vids, reverse=True)
            vids = vids[s_index:e_index]
        
        #3. Populate Completed videos
        keys = [neondata.InternalVideoID.generate(
            self.api_key, vid) for vid in completed_videos] #get internal vids

        if len(keys) > 0:
            video_results = yield tornado.gen.Task(
                neondata.VideoMetadata.get_many,
                keys)
            tids = []
            for vresult in video_results:
                if vresult:
                    tids.extend(vresult.thumbnail_ids)
        
            #Get all the thumbnail data for videos that are done
            thumbnails = yield tornado.gen.Task(
                        neondata.ThumbnailMetadata.get_many, tids)
            for thumb in thumbnails:
                if thumb:
                    vid = neondata.InternalVideoID.to_external(thumb.video_id)
                    tdata = thumb.to_dict_for_video_response()
                    if not result.has_key(vid):
                        _log.debug("key=get_video_status_%s"
                                " msg=video deleted %s"%(i_type, vid))
                    else:
                        result[vid].thumbnails.append(tdata)

        #4. Set the default thumbnail for each of the video
        tid_key = "thumbnail_id"
        for res in result:
            vres = result[res]
            platform_thumb_id = None
            for thumb in vres.thumbnails:
                if thumb['chosen'] == True:
                    vres.current_thumbnail = thumb[tid_key]
                    if "neon" in thumb['type']:
                        vres.status = "active"

                if thumb['type'] == i_type:
                    platform_thumb_id = thumb[tid_key]

            if vres.status == "finished" and vres.current_thumbnail == 0:
                vres.current_thumbnail = platform_thumb_id
 
        
        #convert to dict and count total counts for each state
        vresult = []
        for res in result:
            vres = result[res]
            if vres and vres.video_id in vids: #filter videos by state 
                vresult.append(vres.to_dict())
            
        c_processing = len(p_videos)
        c_recommended = len(r_videos)
        c_published = len(a_videos)

        if i_type == "brightcove":
            #Sort brightcove videos by video_id, since publish_date 
            #is not currently
            s_vresult = sorted(vresult, key=lambda k: int(k['video_id']), reverse=True)
        else:
            s_vresult = sorted(vresult, key=lambda k: k['publish_date'], reverse=True)

        
        vstatus_response = GetVideoStatusResponse(
                        s_vresult, total_count, page_no, page_size,
                        c_processing, c_recommended, c_published)
        data = vstatus_response.to_json() 
        self.send_json_response(data, 200)

    def create_brightcove_video_request(self, i_id):
        ''' Create request for brightcove video 
        submit a job on neon server, update video in the brightcove account
        '''
        def job_created(result):
            ''' create job callback'''
            if not result: 
                data = '{"error": ""}'
                self.send_json_response(data, 200)  
            else:
                data = '{"error": "failed to create job, bad request"}'
                self.send_json_response(data, 400)  

        def get_account_callback(account):
            ''' get account cb '''
            if account:
                #submit job for processing
                account.create_job(vid, job_created)
            else:
                data = '{"error": "no such account"}'
                self.send_json_response(data, 400)

        #check video id
        try:
            vid = self.get_argument('video_id')
        except:
            data = '{"error": "video_id not set"}'
            self.send_json_response(data, 400)
            
        neondata.BrightcovePlatform.get_account(self.api_key,
                                                i_id,
                                                get_account_callback)
        
    @tornado.gen.engine
    def update_video_brightcove(self, i_id, i_vid, new_tid):
        ''' update thumbnail for a brightcove video '''
        #TODO : Check for the linked youtube account 
        
        p_vid = neondata.InternalVideoID.to_external(i_vid)
        #Get account/integration
        ba = yield tornado.gen.Task(neondata.BrightcovePlatform.get_account,
                self.api_key,i_id)

        if not ba:
            _log.error("key=update_video_brightcove" 
                    " msg=account doesnt exist api key=%s " 
                    "i_id=%s"%(self.api_key,i_id))
            data = '{"error": "no such account"}'
            self.send_json_response(data, 400)
            return

        result = yield tornado.gen.Task(ba.update_thumbnail, i_vid, new_tid)
        
        if result:
            _log.info("key=update_video_brightcove" 
                        " msg=thumbnail updated for video=%s tid=%s"\
                        %(p_vid, new_tid))
            data = ''
            self.send_json_response(data, 200)
        else:
            if result is None:
                data = '{"error": "internal error"}'
                self.send_json_response(data, 500)
            else:
                data = '{"error": "brightcove api failure"}'
                self.send_json_response(data, 502)

    @tornado.gen.engine
    def create_account_and_neon_integration(self, a_id):
        '''
        Create Neon user account and add neon integration
        '''
        user = neondata.NeonUserAccount(a_id)
        api_key = user.neon_api_key
        nuser_data = yield tornado.gen.Task(
                    neondata.NeonUserAccount.get_account, a_id)
        if not nuser_data:
            nplatform = neondata.NeonPlatform(a_id, api_key)
            user.add_platform(nplatform)
            res = yield tornado.gen.Task(user.save_platform, nplatform) 
            if res:
                tai_mapper = neondata.TrackerAccountIDMapper(
                                    user.tracker_account_id, api_key, 
                                    neondata.TrackerAccountIDMapper.PRODUCTION)
                tai_staging_mapper = neondata.TrackerAccountIDMapper(
                                    user.staging_tracker_account_id, api_key,
                                    neondata.TrackerAccountIDMapper.STAGING)
                staging_resp = yield tornado.gen.Task(tai_staging_mapper.save)
                resp = yield tornado.gen.Task(tai_mapper.save)
                if not (staging_resp and resp):
                    _log.error("key=create_neon_user "
                            " msg=failed to save tai %s" %user.tracker_account_id)

                # Set the default experimental strategy
                strategy = neondata.ExperimentStrategy(api_key)
                res = yield tornado.gen.Task(strategy.save)
                if not res:
                    _log.error('Bad database response when adding '
                               'the default strategy for Neon account %s'
                               % api_key)
                    
                data = ('{ "neon_api_key": "%s", "tracker_account_id":"%s",'
                            '"staging_tracker_account_id": "%s" }'
                            %(user.neon_api_key, user.tracker_account_id,
                            user.staging_tracker_account_id)) 
                self.send_json_response(data, 200)
            else:
                data = '{"error": "account not created"}'
                self.send_json_response(data, 500)

        else:
            data = '{"error": "integration/ account already exists"}'
            self.send_json_response(data, 409)

    @tornado.gen.engine
    def create_brightcove_integration(self):
        ''' Create Brightcove Account for the Neon user
        Add the integration in to the neon user account
        Extract params from post request --> create acccount in DB 
        --> verify tokens in brightcove -->
        send top 5 videos requests or appropriate error to client
        '''

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
            self.send_json_response(data, 400)
            return 

        na = yield tornado.gen.Task(neondata.NeonUserAccount.get_account,
                                    self.api_key)
        #Create and Add Platform Integration
        if na:
            
            #Check if integration exists
            if len(na.integrations) >0 and na.integrations.has_key(i_id):
                data = '{"error": "integration already exists"}'
                self.send_json_response(data, 409)
            else:
                curtime = time.time() #account creation time
                bc = neondata.BrightcovePlatform(
                    a_id, i_id, self.api_key, p_id, 
                    rtoken,wtoken, autosync, curtime) 
                na.add_platform(bc)
                #save & update acnt
                res = yield tornado.gen.Task(na.save_platform, bc)
                
                #Saved platform
                if res:
                    # Set the default experimental strategy for
                    # Brightcove Customers.
                    strategy = neondata.ExperimentStrategy(
                        self.api_key, only_exp_if_chosen=True)
                    res = yield tornado.gen.Task(strategy.save)
                    if not res:
                        _log.error('Bad database response when adding '
                                   'the default strategy for BC account %s'
                                   % self.api_key)
                    
                    response = bc.verify_token_and_create_requests_for_video(10)
                    
                    # TODO: investigate further, 
                    #ReferenceError: weakly-referenced object no longer exists
                    # (self.subscribed and cmd == 'PUBLISH')):
                    #Not Async due to tornado redis bug in neon server
                    #Task(bc.verify_token_and_create_requests_for_video,5)
                    
                    ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    #TODO : Add expected time of completion !
                    video_response = []
                    if not response:
                        #TODO : Distinguish between api call failure and bad tokens
                        _log.error("key=create brightcove account " 
                                    " msg=brightcove api call failed or token error")
                        data = '{"error": "Read token given is incorrect'  
                        data += ' or brightcove api failed"}'
                        self.send_json_response(data, 502)
                        return

                    for item in response:
                        t_urls =[]; thumbs = []
                        t_urls.append(item['videoStillURL'])
                        tm = neondata.ThumbnailMetadata(
                                0,
                                neondata.InternalVideoID.generate(self.api_key,
                                                                  item["id"]),
                                t_urls, ctime, 0, 0,
                                "brightcove", 0, 0)
                        thumbs.append(tm.to_dict_for_video_response())
                        vr = neondata.VideoResponse(item["id"],
                              "processing",
                              "brightcove",
                              i_id,
                              item['name'],
                              None,
                              None,
                              0, #current tid,add fake tid
                              thumbs)
                        video_response.append(vr.to_dict())
                        
                    vstatus_response = GetVideoStatusResponse(
                                        video_response, len(video_response))
                    data = vstatus_response.to_json() 
                    #data = tornado.escape.json_encode(video_response)
                    self.send_json_response(data, 201)
                else:
                    data = '{"error": "platform was not added,\
                                account creation issue"}'
                    self.send_json_response(data, 500)
                    return
        else:
            _log.error("key=create brightcove account " 
                        "msg= account not found %s" %self.api_key)

    @tornado.gen.engine
    def update_brightcove_integration(self, i_id):
        ''' Update Brightcove account details '''
        try:
            rtoken = InputSanitizer.to_string(self.get_argument("read_token"))
            wtoken = InputSanitizer.to_string(self.get_argument("write_token"))
            autosync = InputSanitizer.to_bool(self.get_argument("auto_update"))
        except Exception,e:
            _log.error("key=create brightcove account msg= %s" %e)
            data = '{"error": "API Params missing"}'
            self.send_json_response(data, 400)
            return

        uri_parts = self.request.uri.split('/')
        bc = yield tornado.gen.Task(neondata.BrightcovePlatform.get_account,
                                    self.api_key, i_id)
        if bc:
            bc.read_token = rtoken
            bc.write_token = wtoken
                
            #Auto publish all the previous thumbnails in the account
            if bc.auto_update == False and autosync == True:
                #self.autopublish_brightcove_videos(bc)
                bplatform_account = bc
                vids = bplatform_account.get_videos()
                
                # No videos in the account
                if not vids:
                    return
                
                keys = [neondata.InternalVideoID.generate(
                            self.api_key, vid) for vid in vids]
                video_results = yield tornado.gen.Task(
                        neondata.VideoMetadata.get_many, keys)
                tids = []
                video_thumb_mappings = {} #vid => [thumbnail metadata ...]
                update_videos = {} #vid => neon_tid
                #for all videos in account where status is not active
                for vresult in video_results:
                    if vresult:
                        tids.extend(vresult.thumbnail_ids)
                        video_thumb_mappings[vresult.get_id()] = []
                    
                    #Get all the thumbnail data for videos that are done
                    thumbnails = yield tornado.gen.Task(
                            neondata.ThumbnailMetadata.get_many, tids)
                    for thumb in thumbnails:
                        if thumb:
                            vid = thumb.video_id
                            #neondata.InternalVideoID.to_external(thumb.video_id)
                            tdata = thumb.to_dict_for_video_response()
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
                for vid, new_tid in update_videos.iteritems():
                    result = yield tornado.gen.Task(
                            bplatform_account.update_thumbnail, vid, new_tid)
                    if not result:
                        p_vid = neondata.InternalVideoID.to_external(vid)
                        _log.error("key=autopublish msg=update thumbnail" 
                                " failed for api_key=%s vid=%s tid=%s" 
                                %(self.api_key, p_vid, new_tid))

            bc.auto_update = autosync
            res = yield tornado.gen.Task(bc.save)
            if res:
                data = ''
                self.send_json_response(data, 200)
            else:
                data = '{"error": "account not updated"}'
                self.send_json_response(data, 500)
        else:
            _log.error("key=update_brightcove_integration " 
                    "msg=no such account %s integration id %s" %(self.api_key, i_id))
            data = '{"error": "Account doesnt exists"}'
            self.send_json_response(data, 400)
   
    ##################################################################
    # Ooyala Methods
    ##################################################################

    @tornado.gen.engine
    def create_ooyala_integration(self):
        '''
        Create Ooyala Integration
        '''

        try:
            a_id = self.request.uri.split('/')[-2]
            i_id = InputSanitizer.to_string(self.get_argument("integration_id"))
            partner_code = InputSanitizer.to_string(
                                self.get_argument("partner_code"))
            oo_api_key = InputSanitizer.to_string(self.get_argument("oo_api_key"))
            oo_secret_key = InputSanitizer.to_string(
                                self.get_argument("oo_secret_key"))
            autosync = InputSanitizer.to_bool(self.get_argument("auto_update"))

        except Exception,e:
            _log.error("key=create_ooyla_account msg= %s" %e)
            data = '{"error": "API Params missing"}'
            self.send_json_response(data, 400)
            return 

        na = yield tornado.gen.Task(neondata.NeonUserAccount.get_account,
                                    self.api_key)
        #Create and Add Platform Integration
        if na:
            
            #Check if integration exists
            if len(na.integrations) >0 and na.integrations.has_key(i_id):
                data = '{"error": "integration already exists"}'
                self.send_json_response(data, 409)
            else:
                curtime = time.time() #account creation time
                oo_account = neondata.OoyalaPlatform(a_id, i_id, self.api_key, 
                                                partner_code, 
                                                oo_api_key, oo_secret_key, autosync)
                na.add_platform(oo_account)
                #save & update acnt
                res = yield tornado.gen.Task(na.save_platform, oo_account)
                if res:
                    # Set the default experimental strategy for
                    # Ooyala Customers.
                    strategy = neondata.ExperimentStrategy(
                        self.api_key, only_exp_if_chosen=True)
                    res = yield tornado.gen.Task(strategy.save)
                    if not res:
                        _log.error('Bad database response when adding '
                                   'the default strategy for Ooyala account %s'
                                   % self.api_key)
                    
                    response = yield tornado.gen.Task(
                            oo_account.create_video_requests_on_signup, 10)
                    ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    video_response = []
                    if not response:
                        _log.error("key=create_ooyala_account " 
                                    " msg=ooyala api call failed or token error")
                        data = '{"error": "invalid api key or secret"}'
                        self.send_json_response(data, 502)
                        return
                   
                    #if reponse is empty? no videos in the account
                    #Build video response
                    for item in response:
                        t_urls = []
                        thumbs = []
                        t_urls.append(item['preview_image_url'])
                        tm = neondata.ThumbnailMetadata(
                                0,
                                neondata.InternalVideoID.generate(self.api_key,
                                                                  item["embed_code"]),
                                t_urls, ctime, 0, 0, "ooyala", 0, 0)
                        thumbs.append(tm.to_dict_for_video_response())
                        vr = neondata.VideoResponse(item["embed_code"],
                              "processing",
                              "ooyala",
                              i_id,
                              item['name'],
                              None,
                              None,
                              0, #current tid,add fake tid
                              thumbs)
                        video_response.append(vr.to_dict())
                        
                    vstatus_response = GetVideoStatusResponse(
                                        video_response, len(video_response))
                    data = vstatus_response.to_json() 
                    self.send_json_response(data, 201) 
                else:
                    data = '{"error": "platform was not added,\
                                account creation issue"}'
                    self.send_json_response(data, 500)

    #2. Update  the Account
    @tornado.gen.engine
    def update_ooyala_integration(self, i_id):
        ''' Update Ooyala account details '''
    
        try:
            partner_code = InputSanitizer.to_string(
                                self.get_argument("partner_code"))
            oo_api_key = InputSanitizer.to_string(self.get_argument("oo_api_key"))
            oo_secret_key = InputSanitizer.to_string(
                                self.get_argument("oo_secret_key"))
            autosync = InputSanitizer.to_bool(self.get_argument("auto_update"))
        except Exception,e:
            _log.error("key=update ooyala account msg= %s" %e)
            data = '{"error": "API Params missing"}'
            self.send_json_response(data, 400)
            return

        uri_parts = self.request.uri.split('/')

        oo = yield tornado.gen.Task(neondata.OoyalaPlatform.get_account,
                                    self.api_key, i_id)
        if oo:
            oo.partner_code = partner_code
            oo.ooyala_api_key = oo_api_key
            oo.api_secret = oo_secret_key 
            oo.autosync = autosync
            #TODO: Test autosync
            res = yield tornado.gen.Task(oo.save)
            if res:
                data = ''
                self.send_json_response(data, 200)
            else:
                data = '{"error": "account not updated"}'
                self.send_json_response(data, 500)
        else:
            _log.error("key=update_ooyala_integration msg=no such account ") 
            data = '{"error": "Account doesnt exists"}'
            self.send_json_response(data, 400)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def update_video_ooyala(self, i_id, i_vid, new_tid):
        ''' update thumbnail for a Ooyala video '''
        
        p_vid = neondata.InternalVideoID.to_external(i_vid)
        
        #Get account/integration
        oo = yield tornado.gen.Task(neondata.OoyalaPlatform.get_account,
                self.api_key, i_id)
        if not oo:
            _log.error("key=update_video_ooyala" 
                    " msg=account doesnt exist api key=%s " 
                    "i_id=%s"%(self.api_key,i_id))
            data = '{"error": "no such account"}'
            self.send_json_response(data, 400)
            raise tornado.gen.Return()

        result = yield tornado.gen.Task(oo.update_thumbnail, i_vid, new_tid)
        
        if result:
            _log.info("key=update_video_ooyala" 
                        " msg=thumbnail updated for video=%s tid=%s"\
                        %(p_vid, new_tid))
            data = ''
            self.send_json_response(data, 200)
        else:
            if result is None:
                data = '{"error": "ooyala api failure"}'
                self.send_json_response(data, 502)
            else:
                data = '{"error": "internal error"}'
                self.send_json_response(data, 500)


    ##################################################################
    # Youtube methods
    ##################################################################

    #'''
    #Cretate a Youtube Account

    #if brightcove account associated with the user, link them

    #validate the refresh token and retreive list of channels for the acccount    
    #'''
    #def create_youtube_integration(self):
    #    '''
    #    def saved_account(result):
    #        if result:
    #            data = '{"error" : ""}'
    #            self.send_json_response(data,201)
    #        else:
    #            data = '{"error": "account creation issue"}'
    #            self.send_json_response(data,500)
    #        return
    #   
    #    def neon_account(account):
    #        if account:
    #            account.add_platform(yt) 
    #            account.save_platofrm(yt,saved_account)
    #        else:
    #            _log.error("key=create_youtube_integration msg=neon account not found")
    #            data = '{"error": "account creation issue"}'
    #            self.send_json_response(data,500)

    #    def channel_callback(result):
    #        if result:
    #            neondata.NeonUserAccount.get_account(self.api_key,
    #                                                 neon_account)
    #        else:
    #            data = '{"error": "account creation issue"}'
    #            self.send_json_response(data,500)

    #    i_id = self.get_argument("integration_id")
    #    a_token = self.get_argument("access_token")
    #    r_token = self.get_argument("refresh_token")
    #    expires = self.get_argument("expires")    
    #    autosync = self.get_argument("auto_update")
    #    yt = neondata.YoutubePlatform(self.api_key,i_id,a_token,r_token,expires,autosync)
    #    #Add channel
    #    yt.add_channels(channel_callback)
    #    '''
    #    pass

    #'''
    #Update Youtube account
    #'''
    #def update_youtube_account(self,i_id):
    #    '''
    #    def saved_account(result):
    #        if result:
    #            data = ''
    #            self.send_json_response(data,200)
    #        else:
    #            data = '{"error": "account not updated"}'
    #            self.send_json_response(data,500)

    #    def update_account(account):
    #        if account:
    #            account.access_token = access_token
    #            account.refresh_token = refresh_token
    #            account.auto_update = auto_update
    #            account.save(saved_account)
    #        else:
    #            _log.error("key=update youtube account msg= no such account %s integration id %s" %(self.api_key,i_id))
    #            data = '{"error": "Account doesnt exists" }'
    #            self.send_json_response(data,400)
    #   
    #    try:
    #        access_token = self.request.get_argument('access_token')
    #        refresh_token = self.request.get_argument('refresh_token')
    #        auto_update = self.request.get_argument('auto_update')
    #        neondata.YoutubePlatform.get_account(self.api_key,
    #                                             i_id,
    #                                             update_account)
    #    except:
    #        data = '{"error": "missing arguments"}'
    #        self.send_json_response(data)
    #    '''
    #    data = '{"error": "not yet impl"}'
    #    self.send_json_response(data)

    #'''
    #Create a youtube video request 
    #'''
    #def create_youtube_video_request(self,i_id):
    #    '''
    #    def job_created(response):
    #        if not response.error:
    #            data = response.body 
    #            self.send_json_response(data,200)
    #        else:
    #            data = '{"error": "job not created"}'
    #            self.send_json_response(data,400)
    #    
    #    #Get params from request
    #    #Get account details   
    #   
    #    def get_account(account):
    #        if account:
    #            params = {}
    #            params["api_key"] = self.api_key
    #            params["video_id"] = self.get_argument("video_id")
    #            params["video_title"] = self.get_argument("video_id")
    #            params["video_url"] = self.get_argument("video_url") 
    #            params["topn"] = 5
    #            params["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"
    #            params["access_token"] = account.access_token
    #            params["refresh_token"] = account.refresh_token
    #            params["token_expiry"] = account.expires
    #            params["autosync"] = account.auto_update

    #            body = tornado.escape.json_encode(params)
    #            yt_request = "http://thumbnails.neon-lab.com/api/v1/submitvideo/youtube"
    #            yt_request = "http://localhost:8081/api/v1/submitvideo/youtube"
    #            http_client = tornado.httpclient.AsyncHTTPClient()
    #            req = tornado.httpclient.HTTPRequest(url = yt_request,
    #                                                method = "POST",
    #                                                body = body,
    #                                                request_timeout = 60.0,
    #                                                connect_timeout = 10.0)
    #            http_client.fetch(req,job_created)         
    #        else:
    #            data = '{"error" : "no such youtube account" }'
    #            self.send_json_response(data,400)

    #    neondata.YoutubePlatform.get_account(self.api_key, i_id, get_account)
    #    '''
    #    data = '{"error": "not yet impl"}'
    #    self.send_json_response(data)

    #'''
    #Populate youtube videos
    #'''
    #def get_youtube_videos(self,i_id):
    #    '''
    #    self.counter = 0
    #    self.yt_results = None
    #    self.video_results = None

    #    def format_result(response):
    #        if response and not response.error:
    #            vitems = tornado.escape.json_decode(response.body)
    #            items = vitems['items']
    #            videos = [] 
    #            for item in items:
    #                video = {}
    #                video['video_id'] = item['snippet']['resourceId']['videoId']
    #                video['title'] = item['snippet']['title']
    #                video['publish_date'] = item['snippet']['publishedAt']
    #                video['thumbnail_url'] = \
    #                        item['snippet']['thumbnails']['default']['url'] 
    #                videos.append(video)
    #            data = tornado.escape.json_encode(videos)
    #            self.send_json_response(data,200)
    #        else:
    #            data = '{"error": "youtube api issue"}'
    #            self.send_json_response(data,500)

    #    def process_youtube_results(yt_response):
    #        if not yt.response.error:
    #            self.yt_results = yt_response.body
    #        else:
    #            data = '{"error": "youtube api error"}'
    #            self.send_json_response(data,500)

    #    def process_video_results(vid_result):
    #        if vid_result:
    #            self.video_results = tornado.escape.json_decode(vid_result)
    #        else:
    #            data = '{"error": "database error"}'
    #            self.send_json_response(data,500)

    #    def format_response():
    #        def get_current_thumbnail(thumbnails):
    #                tid = None
    #                for t in thumbnails:
    #                    if t['enabled'] is not None:
    #                        tid = t['thumbnail_id']
    #                        return tid

    #        videos = []
    #        vitems = tornado.escape.json_decode(self.yt_results)
    #        items = vitems['items']
    #        for vid in items:
    #                video = {}
    #                video['video_id'] = item['snippet']['resourceId']['videoId']
    #                video['title'] = item['snippet']['title']
    #                video['publish_date'] = item['snippet']['publishedAt']
    #                video['thumbnail_url'] = item['snippet']['thumbnails']['default']['url'] 
    #                video['duration'] = None 
    #                
    #                #Fill from DB result
    #                if vid in self.video_results.keys():
    #                    video['status'] = self.video_results[vid]['state'] 
    #                    thumbs = self.video_results[vid]['thumbnails']
    #                    video['current_thumbnail_id'] = get_current_thumbnail(thumbs) 
    #                    video['thumbnails'] = thumbs
    #                else:
    #                    video['status'] = "unprocessed"
    #                    video['current_thumbnail_id'] = None 
    #                    video['thumbnails'] = None 
    #        videos.append(video)
    #          
    #        data = tornado.escape.json_encode(videos)
    #        return data

    #    def result_aggregator(result):
    #        self.bc_aggr += 1
    #        #check each discrete call
    #        
    #        if isinstance(result,tornado.httpclient.HTTPResponse):
    #            bcove_result = tornado.escape.json_decode(result.body)
    #            process_brightcove_results(bcove_result)
    #        else:
    #            process_video_results(result)

    #        #both calls have finished
    #        if self.bc_aggr == 2:

    #            data = format_response()
    #            self.set_header("Content-Type", "application/json")
    #            self.write(data)
    #            self.finish()
    #            #send response and terminate
    #            #self.send_json_response()

    #    def result_aggregator(result):
    #        self.counter += 1

    #        if isinstance(result,tornado.httpclient.HTTPResponse):
    #            yt_result = tornado.escape.json_decode(result.body)
    #            process_youtube_results(yt_result)
    #        else:
    #            process_video_results(result)
    #        
    #        if self.counter == 2:
    #            data = format_response()
    #            self.set_header("Content-Type", "application/json")
    #            self.write(data)
    #            self.finish()

    #    def account_callback(account):
    #        if account:                
    #            
    #            if (account.videos) > 0:
    #                #1.Get videos from youtube api
    #                account.get_videos(format_result)
    #                
    #                #2.Get videos that have been already processed from Neon Youtube account
    #                keys = [ neondata.generate_request_key(api_key,j_id) for j_id in account.videos.values()] 
    #                neondata.NeonApiRequest.multiget(keys, result_aggregator)
    #            else:
    #                raise Exception("NOT YET IMPL")
    #        else:
    #            data = '{"error": "no such account"}'
    #            self.send_json_response(data,500)

    #    uri_parts = self.request.uri.split('/')
    #    neondata.YoutubePlatform.get_account(self.api_key,
    #                                         i_id,
    #                                         account_callback)
    #    '''
    #    pass

    #''' Update the thumbnail for a particular video '''
    #def update_youtube_video(self,i_id,vid):
    #
    #    '''
    #    def update_thumbnail(t_result):
    #        if t_result:
    #            data = '{"error" :""}'
    #            self.send_json_response(data,200)
    #        else:
    #            data = '{"error": "thumbnail not updated"}'
    #            self.send_json_response(data,500)

    #    def get_request(r_result):
    #        if r_result:
    #            self.vid_request = YoutubeApiRequest.create(r_result) 
    #            thumbnail_url = self.vid_request.enable_thumbnail(tid)
    #            self.yt.update_thumbnail(vid,thumbnail_url,update_thumbnail)
    #        else:
    #            data = '{"error": "thumbnail not updated"}'
    #            self.send_json_response(data,500)

    #    def get_account_callback(account):
    #        if account:
    #            self.yt = account
    #            job_id = self.yt.videos[vid] 
    #            yt_request = YoutubeApiRequest.get_request(self.api_key,
    #                                                       job_id,
    #                                                       get_request) 
    #        else:
    #            data = '{"error": "no such account"}'
    #            self.send_json_response(data,500)

    #    try:
    #        thumbnail_id = self.get_argument('thumbnail_id')
    #    except Exception,e:
    #        _log.exception('type=update brightcove thumbnail' + e.message)
    #        self.set_status(400)
    #        self.finish()
    #        return

    #    neondata.YoutubePlatform.get_account(self.api_key,
    #                                         i_id,
    #                                         get_account_callback)
    #    '''
    #    pass


    @tornado.gen.engine
    def upload_video_custom_thumbnail(self, itype, i_id, i_vid, thumbs):

        p_vid = neondata.InternalVideoID.to_external(i_vid)
        platform_account = yield tornado.gen.Task(self.get_platform_account, itype, i_id)
        
        if not platform_account:
            _log.error("key=upload_video_custom_thumbnail msg=%s account not found" % itype)
            self.send_json_response("%s account not found" % itype, 400)
            return
     
        #TODO: handle multiple thumb uploads
        t_url = thumbs[0]["urls"][0]
        vmdata = yield tornado.gen.Task(neondata.VideoMetadata.get, i_vid)
       
        result = vmdata.add_custom_thumbnail(t_url)

        if result:
            _log.info("key=update_video_brightcove" 
                        " msg=thumbnail updated for video=%s tid=%s"\
                        %(p_vid, result))
            data = ''
            self.send_json_response(data, 202)
        else:
            data = '{"error": "internal error"}'
            self.send_json_response(data, 500)


    ### AB Test State #####
    
    @tornado.gen.engine
    def get_abteststate(self, vid):

        '''
        Return the A/B Test state of the video
        Possible status values are:
        (running, complete, disabled, override, unkown) 

        if state == complete, return all the serving URLs
       
        json response:
        {  
            "state": "running",  
            "data" : []
        }
        
        '''

        i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
        vmdata = yield tornado.gen.Task(neondata.VideoMetadata.get, i_vid)
        if not vmdata:
            self.send_json_response('{"error": "vid not found"}', 400)

        state = vmdata.experiment_state
        
        response = {}
        response['state'] = state
        response['data'] = []

        # If complete, then send all the URLs for a given tid
        if state == neondata.ExperimentState.COMPLETE:
            rdata = []
            # Find the winner tid

            # If serving fraction = 1.0 its the winner
            # If override == true, then pick highest
            # else filter all > exp_frac ; then max frac

            winner_tid = vmdata.get_winner_tid()
            if winner_tid:
                s_urls = neondata.ThumbnailServingURLs.get(winner_tid)
                for size_tup, url in s_urls.size_map.iteritems():
                    #Add urls to data section
                    s_url = {}
                    s_url['url'] = url 
                    s_url['width'] = size_tup[0]
                    s_url['height'] = size_tup[1]
                    rdata.append(s_url)
                response['data'] = rdata
            else:
                response['state'] = "unknown" #should we define error state? 
                _log.error("winner tid not found for video id %s" % i_vid)

        data = json.dumps(response)
        self.send_json_response(data, 200)

    def update_thumbnail_property(self, tid):
       
        invalid_msg = "invalid thumbnail id or thumbnail id not found" 
        if tid is None:
            self.send_json_response(invalid_msg, 400)

        tmdata = yield tornado.gen.Task(neondata.ThumbnailMetadata.get, tid)
        if tmdata is None:
            self.send_json_response(invalid_msg, 400)

        inp = json.loads(self.request.body)
        tmdata.enabled = inp['value']
        
        res = yield tornado.gen.Task(neondata.ThumbnailMetadata.save_all,
                                      [tmdata])
        if res:
            self.send_json_response('', 202)
        else:
            self.send_json_response('Internal DB error failed to save', 500)


######################################################################
## Brightcove support handler -- Mainly used by brigthcovecontroller 
######################################################################

class BcoveHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self, *args, **kwargs):
        ''' get '''
        self.finish()

    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self, *args, **kwargs):
        ''' post '''

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
        ''' /api/v1/brightcovecontroller/%s/updatethumbnail/%s '''

        try:
            new_tid = self.get_argument('thumbnail_id')
            nosave = self.get_argument('nosavedb', True)
        except:
            self.set_status(400)
            self.finish()
            return
        vmdata = yield tornado.gen.Task(
                 neondata.VideoMetadata.get, self.internal_video_id)
        if vmdata:
            i_id = vmdata.integration_id
            ba  = yield tornado.gen.Task(
                  neondata.BrightcovePlatform.get_account, self.a_id, i_id)
            if ba:
                bcove_vid = neondata.InternalVideoID.to_external(
                            self.internal_video_id)
                result = yield tornado.gen.Task(
                            ba.update_thumbnail,
                            self.internal_video_id, new_tid, True) #nosave true
                if result:
                    self.set_status(200)
                else:
                    _log.error("key=bcove_handler "
                            " msg=failed to update thumbnail for" 
                            " %s %s"%(self.internal_video_id, new_tid))
                    self.set_status(502)
            else:
                _log.error("key=bcove_handler msg=failed to fetch " 
                        " neondata.BrightcovePlatform %s i_id %s"%(self.a_id, i_id))
                self.set_status(502)
        else:
            _log.error("key=bcove_handler "
                    " msg=failed to fetch video metadata or video not present for "
                    "%s %s"%(self.internal_video_id, new_tid))
            self.set_status(502)
        self.finish()

################################################################
### MAIN
################################################################

application = tornado.web.Application([
        (r'/api/v1/accounts(.*)', AccountHandler),
        (r'/api/v1/brightcovecontroller(.*)', BcoveHandler)],
        gzip=True)

def main():
    
    global server
    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
