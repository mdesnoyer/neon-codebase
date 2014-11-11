#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)


import hashlib
import logging
import multiprocessing
import os
import properties
import Queue
import re
from supportServices import neondata
import time
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import threading
import utils.http
import utils.neon
import utils.ps
from utils import statemon

#Tornado options
from utils.options import define, options
define("port", default=8081, help="run on the given port", type=int)
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3

_log = logging.getLogger(__name__)

DIRNAME = os.path.dirname(__file__)

# Monitoring variables
statemon.define('server_queue', int)
statemon.define('duplicate_requests', int)
statemon.define('dequeue_requests', int)


# Constants
THUMBNAIL_RATE = "rate"
TOP_THUMBNAILS = "topn"
THUMBNAIL_SIZE = "size"
ABTEST_THUMBNAILS = "abtest"
THUMBNAIL_INTERVAL = "interval"
CALLBACK_URL = "callback_url"
VIDEO_ID = "video_id"
VIDEO_DOWNLOAD_URL = "video_url"
VIDEO_TITLE = "video_title"
BCOVE_READ_TOKEN = "read_token"
BCOVE_WRITE_TOKEN = "write_token"
REQUEST_UUID_KEY = "job_id"
API_KEY = "api_key"
JOB_SUBMIT_TIME = "submit_time"

class RequestData(object):
    '''
    Instance of this classs is stored in the Q
    '''

    def __init__(self, api_request):
        '''
        @api_request: NeonApiRequest Object
        '''
        self.api_request = api_request
        self.video_size = None # in bytes

    def get_request_json(self):
        return self.api_request.to_json()

    def get_video_size(self):
        return self.video_size

    def set_video_size(self, val):
        self.video_size = val

class GetCustomerPriority(object):
    priorities = {} 
    def __init__(self):
        pass


class SimpleThreadSafeDictQ(object):
    '''
    Threadsafe Q implementation using a dictionary
    '''
    
    def __init__(self):
        self.qdict = {}
        
        # Note that head index keeps monotonously increasing and is not reused
        # So during the lifecycle of the queue we should be able to use 
        # >>> sys.maxint = 9223372036854775807 many Q entries !!!

        self.head_index = 0 
        self.tail_index = None 
        self._lock = threading.Lock()
    
    def _is_empty(self):
        with self._lock:
            if self.head_index == 0 and self.tail_index is None:
                return True

        return False

    def _get_head(self):
        with self._lock:
            return self.head_index

    def size(self):
        if self.is_empty():
            return 0

        with self._lock:
            return self.tail_index - self.head_index + 1 

    def add(self, item):
        with self._lock:
            if self._is_empty():
                self.tail_index = 0
            else:
                self.tail_index += 1
            
            key = self.tail_index
            self.qdict[key] = item

    def peek(self, key=None):
        '''
        If key is None, return head of the Q
        else peek particular key 

        @returns: value or None, None if key not found    
        '''
        with self._lock:
            try:
                if key:
                    return self.qdict[key]
                else:
                    key = self._get_head()
                    return self.qdict[key]
            except KeyError, e:
                return None

    def get(self):
        with self._lock:
            key = self._get_head()
            ret = self.qdict.pop(key)
            self.head_index += 1
            return ret

    def modify(self, key, value):
        '''
        Modify item in the Q 
        Q item could have already been DQ'd, 
        skip modify in that case
        '''
        with self._lock:
            try:
                self.qdict[key]
                self.qdist[key] = value
            except KeyError, e:
                pass

class FairWeightedRequestQueue(object):
    '''
    FairWeighted requeust Q

    '''
    def __init__(self, nqueues=2, weights='pow2'):
        self.pqs = [SimpleThreadSafeDictQ() for x in range(nqueues)]
        self.max_priority = 1.0
        if weights == 'pow2':
            for i in range(nqueues -1):  
            self.max_priority += 1.0/2**(i+1) 

    def put(self):
        # Based on customer priority put in the appropriate Q
        # Spin off a thread to set the metadata
        pass

    def dequeue(self):
        # pick a random number in the interval (0, max_priority)
        pass

    @tornago.gen.engine
    def _add_metadata(self, pqid, key):
        '''
        Add metadata to the Q item
        '''
        pq = self.pqs[pqid]
        item = pq.peek(key)

        # Get content length of the video
        req = tornado.httpclient.HTTPRequest(method='HEAD',
                        url=video_url, request_timeout=5.0) 
        result = yield tornado.gen.Task(utils.http.send_request, req)

        if not result.error:
            headers = result.headers
            nbytes = int(headers.get('Content-Length'))

            # modify item
            pq.modify(key, item)

        # On error do nothing currently

    @tornago.gen.engine
    def _metadata_collector(self, pqid, key):
        t = threading.Thread(target=self._add_metadata, args=(pqid, key,))
        t.setDaemon(True)
        t.start()

    def qsize(self):
        pass

def _verify_neon_auth(value):
    #TODO: Implement the authentication token logic
    return True


#### GLOBALS #####
global_request_queue = FairWeightedRequestQueue()

class StatsHandler(tornado.web.RequestHandler):
    ''' Qsize handler '''
    def get(self, *args, **kwargs):
        size = global_request_queue.qsize()
        self.write(size)
        self.finish()

class DequeueHandler(tornado.web.RequestHandler):
    """ DEQUEUE JOB Handler - The queue stores data in json format already """
    def get(self, *args, **kwargs):
        if self.request.headers.has_key('X-Neon-Auth'):
            if not _verify_neon_auth(self.request.headers.get('X-Neon-Auth')):
                raise tornado.web.HTTPError(400)
        else:
            raise tornado.web.HTTPError(400)
        
        statemon.state.increment('dequeue_requests')
        statemon.state.server_queue = global_request_queue.qsize()
        element = global_request_queue.dequeue()
        if element:
            #send http response
            h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
            self.write(str(element))
        else: 
            #Send Queue empty message as a string {}
            self.write("{}")

        self.finish()

class RequeueHandler(tornado.web.RequestHandler):
    """ REQUEUE JOB Handler"""
    def post(self, *args, **kwargs):
        
        try:
            _log.info("key=requeue_handler msg=requeing ")
            data = self.request.body
            global_api_work_queue.put(data)
            statemon.state.server_queue = global_api_work_queue.qsize()
        except Exception,e:
            _log.error("key=requeue_handler msg=error " + e.__str__())
            raise tornado.web.HTTPError(500)

        self.finish()

## ===================== API ===========================================#
# External Handlers
## ===================== API ===========================================#

class GetThumbnailsHandler(tornado.web.RequestHandler):
    ''' Thumbnail API handler '''

    test_mode = False
    parsed_params = {}
    
    @tornado.web.asynchronous
    def send_json_response(self, data, status=200):
       
        self.set_header("Content-Type", "application/json")
        self.set_status(status)
        self.write(data)
        self.finish()

    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self, *args, **kwargs):
        #insert job in to user account
        
        try:
            params = tornado.escape.json_decode(self.request.body)
            uri = self.request.uri
            self.parsed_params = {}
            api_request = None 
            http_callback = None
            
            #Verify essential parameters
            try:
                api_key = params[API_KEY]
                vid = params[VIDEO_ID]
                if not re.match('^[a-zA-Z0-9-]+$', vid):
                    self.send_json_response(
                        '{"error":"video id contains invalid characters"}', 400)
                    return

                title = params[VIDEO_TITLE]
                url = params[VIDEO_DOWNLOAD_URL]
            except KeyError, e:
                self.send_json_response('{"error":"params not set"}', 400)
                return
           
            # Treat http_callback as an optional parameter
            try:
                http_callback = params[CALLBACK_URL]
            except KeyError, e:
                pass

            # compare with supported api methods
            if params.has_key(TOP_THUMBNAILS):
                api_method = "topn"
                api_param = min(int(params[TOP_THUMBNAILS]),
                        MAX_THUMBNAILS)
            else:
                self.send_json_response('{"error":"api method not supported"}', 400)
                return
           
            #Generate JOB ID  
            #Use Params that can change to generate UUID, support same
            #video to be processed with diff params
            intermediate = api_key + str(vid) + api_method + str(api_param) 
            job_id = hashlib.md5(intermediate).hexdigest()
          
            #Identify Request Type
            if "brightcove" in self.request.uri:
                pub_id  = params[PUBLISHER_ID] #publisher id
                p_thumb = params[PREV_THUMBNAIL]
                rtoken = params[BCOVE_READ_TOKEN]
                wtoken = params[BCOVE_WRITE_TOKEN]
                autosync = params["autosync"]
                request_type = "brightcove"
                i_id = params[INTEGRATION_ID]
                api_request = neondata.BrightcoveApiRequest(
                    job_id, api_key, vid, title, url,
                    rtoken, wtoken, pub_id, http_callback, i_id)
                api_request.previous_thumbnail = p_thumb 
                api_request.autosync = autosync

            elif "ooyala" in self.request.uri:
                request_type = "ooyala"
                oo_api_key = params["oo_api_key"]
                oo_secret_key = params["oo_secret_key"]
                autosync = params["autosync"]
                i_id = params[INTEGRATION_ID]
                p_thumb = params[PREV_THUMBNAIL]
                api_request = neondata.OoyalaApiRequest(job_id, api_key, 
                                                        i_id, vid, title, url,
                                                        oo_api_key,
                                                        oo_secret_key, 
                                                        p_thumb, http_callback)
                api_request.autosync = autosync

            else:
                request_type = "neon"
                api_request = neondata.NeonApiRequest(job_id, api_key, vid,
                                                      title, url,
                                                      request_type,
                                                      http_callback)
            
            #API Method
            api_request.set_api_method(api_method, api_param)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT

            #Validate Request & Insert in to Queue (serialized/json)
            #job_result = None #NeonApiRequest.blocking_conn.get(api_request.key)
            job_result = yield tornado.gen.Task(neondata.BrightcoveApiRequest.get,
                                api_request.api_key, api_request.job_id)

            if job_result is not None:
                response_data = '{"error":"duplicate job", "job_id": "%s" }' 
                                 % job_result.job_id 
                self.write(response_data)
                self.set_status(409)
                self.finish()
                statemon.state.increment('duplicate_requests')
                return
            
            json_data = api_request.to_json()
            global_request_queue.put(api_request)
            statemon.state.server_queue = global_request_queue.qsize()
            
            #Response for the submission of request
            response_data = "{\"job_id\":\"" + job_id + "\"}"
            
            result = yield tornado.gen.Task(api_request.save)

            if not result:
                _log.error("key=thumbnail_handler  msg=request save failed: ")
                self.set_status(502)
            else:
                
                # Only if this is a Neon request, save it to the DB. Other platform requests
                # get added on request creation cron 
                if request_type == 'neon':
                    nplatform = yield tornado.gen.Task(neondata.NeonPlatform.get_account, api_key)
                    if nplatform:
                        #TODO:refactor after moving platform accounts to stored object 
                        nplatform.add_video(vid, job_id)
                        res = yield tornado.gen.Task(nplatform, save)
                        if res:
                            self.write(response_data)
                            self.set_status(201)
                            self.finish()
                        else:
                            _log.error("key=thumbnail_handler update account " 
                                        "  msg=video not added to account")
                    else:
                        _log.error("account not found or api key error")
                        self.send_json_response('{}', 400)

                else:
                    self.set_status(201)
                    self.write(response_data)
                    self.finish()

        except Exception, e:
            _log.exception("key=thumbnail_handler msg= %s"%e)
            self.send_json_response('{"error":"%s"}' % e)
            return

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
    (r"/testcallback",TestCallback)
])

def main():
    utils.neon.InitNeon()
    global server
    server = tornado.httpserver.HTTPServer(application)
    utils.ps.register_tornado_shutdown(server)
    server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
    logging.getLogger('tornado.access').propagate = False
    main()
