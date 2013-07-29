#!/usr/bin/python
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.httpclient
import time
import sys
import random
sys.path.insert(0, "../")
import errorlog
import shortuuid
import signal

from tornado.options import define, options
define("port", default=8082, help="run on the given port", type=int)

global log
log = errorlog.FileLogger("server")

global result_map
result_map = {} 

random.seed(2)
test_status = 0 ; # 0- in progress, 1 - pass , -1 - fail

def sig_handler(sig, frame):
    log.debug('Caught signal: ' + str(sig) )
    tornado.ioloop.IOLoop.instance().stop()
    sys.exit(0)

class MetaIntegrationTestHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        pass

    def finish_callback(self):
        self.finish()
        shutdown()


class IntegrationTestHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.test_videos = ['http://clips.vorwaerts-gmbh.de/big_buck_bunny.mp4','http://brightcove.vo.llnwd.net/pd16/media/2294876105001/2294876105001_2520426735001_PA210093.mp4?videoId=2520415927001', 'http://brightcove.vo.llnwd.net/e1/uds/pd/96980657001/96980657001_109379449001_Bird-CommonRedpoll-iStock-000006369683HD720.mp4?videoId=2296855886001'] 
        
    ''' submit a test request '''
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
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
            log.info("result " + self.request.body)
            data = result["data"]
            vid = result["video_id"]
            if len(data) != result_map[vid]:
                test_status = -1 # didn't return desired result
            result_map.pop(vid)

        except Exception,e:
            raise tornado.web.HTTPError(500)  
            log.error("key=testcallback msg=error recieving message")
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
    application = tornado.web.Application([
        (r'/integrationtest(.*)', IntegrationTestHandler),
        (r'/teststatus(.*)', StatusHandler)])
    
    global server
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
