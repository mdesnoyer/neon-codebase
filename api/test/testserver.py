#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                         '..', '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.httpclient
import tornado.gen
import time
import logging
import random
import errorlog
import shortuuid
import signal
import re
import multiprocessing
import Queue

import utils.neon

from utils.options import define, options
define("port", default=8082, help="run on the given port", type=int)

#global global_api_work_queue
#global_api_work_queue = multiprocessing.Queue()

_log = logging.getLogger(__name__)

global result_map
result_map = {} 

random.seed(2)
test_status = 0 ; # 0- in progress, 1 - pass , -1 - fail

def sig_handler(sig, frame):
    _log.debug('Caught signal: ' + str(sig) )
    tornado.ioloop.IOLoop.instance().stop()
    sys.exit(0)

''' 
Handler for web based testing and product demo
Takes in vimeo or direct download links
'''
class DemoHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        print self.request.headers
        url = self.get_argument('url')
        topn = self.get_argument('topn')
        if "vimeo" in url:
            self.vimeo_request(topn,url)
        else:
            self.create_neon_requests(topn,url)

    def vimeo_request(self,topn,url):
        def vimeo_callback(response):
            if response.error:
                self.set_status(500)
                self.finish()

            site = response.body
            requestSig = re.findall('signature":"(.*?)"', site)
            expireSig = re.findall('timestamp":([0-9]*)', site)
            vid = url.split('/')[-1]
            d_url = "http://player.vimeo.com/play_redirect?clip_id=" + vid +"&sig=" + requestSig[0] + "&time=" + expireSig[1] + "&quality=sd&codecs=H264,VP8i,VP6&type=moogaloop_local&embed_location="
            self.create_neon_requests(topn,d_url)
            return

        tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        h = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36'})
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url,headers =h , request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req,vimeo_callback)
        return

    def finish_callback(self):
        self.finish()
        shutdown()

    def create_neon_requests(self,topn,url):
        vid = shortuuid.uuid()  
        request_body = {}
        request_body["api_key"] = 'a63728c09cda459c3caaa158f4adff49' #neon user key 
        #request_body["api_key"] = '4a6715e07dfbc6a56487bf4eceba0dba'
        request_body["video_title"] = 'test-' + vid 
        request_body["video_id"] =  vid
        request_body["video_url"] = url 
        request_body["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"
        request_body["topn"] = topn 
        client_url = "http://thumbnails.neon-lab.com/api/v1/submitvideo/topn"
        client_url = "http://localhost:8081/api/v1/submitvideo/topn"
        body = tornado.escape.json_encode(request_body)
        tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",body = body, request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req,self.submit_req_callback)

    def submit_req_callback(self,response):
        data = tornado.escape.json_decode(response.body)
        job_id = data['job_id']
        
        def job_finish_callback(resp):
            jresponse = tornado.escape.json_decode(resp.body)
            result = jresponse['result']
            print result, len(result)
            if len(result) >1:
                r = tornado.escape.json_encode(result)
                self.set_header("Access-Control-Allow-Origin","*")
                self.set_header("Content-Type", "application/json")
                self.write(r)
                self.finish()
            else:
                time.sleep(5)
                check_status(job_id)

        def check_status(job_id):
            client_url = 'http://thumbnails.neon-lab.com/api/v1/jobstatus?api_key=a63728c09cda459c3caaa158f4adff49&job_id=' + job_id
            client_url = 'http://localhost:8081/jobstatus?api_key=a63728c09cda459c3caaa158f4adff49&job_id=' + job_id
            tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
            http_client = tornado.httpclient.AsyncHTTPClient()
            req = tornado.httpclient.HTTPRequest(url = client_url, method = "GET",request_timeout = 60.0, connect_timeout = 10.0)
            response = http_client.fetch(req,job_finish_callback)

        check_status(job_id)

class IntegrationTestHandler(tornado.web.RequestHandler):
    
    @tornado.gen.engine
    def register_timeout(self,timeout):
        yield tornado.gen.Task(tornado.ioloop.IOLoop.instance().add_timeout, time.time() + timeout)
        _log.error("Callbacks never came back, check server/client logs")
        sys.exit(1) 
    
    def initialize(self):
        self.test_videos = ['http://clips.vorwaerts-gmbh.de/big_buck_bunny.mp4','http://brightcove.vo.llnwd.net/pd16/media/2294876105001/2294876105001_2520426735001_PA210093.mp4?videoId=2520415927001', 'http://brightcove.vo.llnwd.net/e1/uds/pd/96980657001/96980657001_109379449001_Bird-CommonRedpoll-iStock-000006369683HD720.mp4?videoId=2296855886001'] 
        
    ''' submit a test request '''
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        
        #Register a 6 min timeout
        self.register_timeout(360)

        self.finish()

        types = ['neon']
        nreqs = 3
        try:
            test_type = str(self.get_argument('test'))
            if test_type in types:
                if test_type == 'neon':
                    for i in range(nreqs):
                        self.create_neon_requests()
            else:
                self.set_status(400)
                self.finish()
                return
        except Exception,e:
            raise tornado.web.HTTPError(400)
        self.finish()

    def create_neon_requests(self):
        vid = shortuuid.uuid()  
        request_body = {}
        request_body["api_key"] = 'a63728c09cda459c3caaa158f4adff49' #neon user key 
        request_body["video_title"] = 'test-' + vid 
        request_body["video_id"] =  vid
        request_body["video_url"] = random.choice(self.test_videos)  
        request_body["callback_url"] = "http://localhost:8082/integrationtest"
        request_body["topn"] = random.randint(1,5)
        client_url = "http://localhost:8081/api/v1/submitvideo/topn"
        body = tornado.escape.json_encode(request_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers = h,body = body, request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req,self.submit_req_callback)
        result_map[vid] = request_body["topn"]

    def submit_req_callback(self,reponse):
        return True

    ''' Verify Result on post callback'''
    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        
        try:
            result = tornado.escape.json_decode(self.request.body)
            _log.info("result " + self.request.body)
            data = result["data"]
            vid = result["video_id"]
            if len(data) != result_map[vid]:
                test_status = -1 # didn't return desired result
            result_map.pop(vid)

        except Exception,e:
            raise tornado.web.HTTPError(500)  
            _log.error("key=testcallback msg=error recieving message")
        self.finish()

class StatusHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        if len(result_map) != 0:
            self.set_status(502) #server busy  
        if test_status == -1:
            self.set_status(501) #internal server error  

        #if len(result_map) == 0, all requests complete
        self.finish()

if __name__ == "__main__":
    utils.neon.InitNeon()
    
    application = tornado.web.Application([
        (r'/integrationtest(.*)', IntegrationTestHandler),
        (r'/demo(.*)', DemoHandler),
        (r'/teststatus(.*)', StatusHandler)])
    
    global server
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
