import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import json
import multiprocessing
import Queue
import signal
import time
import urllib
import urlparse
import errorlog
import youtube
import hashlib
import re
import properties

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from StringIO import StringIO

#async s3 connection
from botornado.s3.connection import AsyncS3Connection
from botornado.s3.bucket import AsyncBucket
from botornado.s3.key import AsyncKey

#for more info on stack context mgmt - http://www.tornadoweb.org/en/branch2.4/_modules/tornado/stack_context.html
import tornado.stack_context
import contextlib


#String constants
REQUEST_UUID_KEY = 'uuid'
JOB_ID = "job_id"

#Tornado options
from tornado.options import define, options
define("port", default=8081, help="run on the given port", type=int)
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3
    
global log
log = errorlog.FileLogger("server")


#=============== Global Handlers ======================================#

def sig_handler(sig, frame):
    log.debug('Caught signal: ' + str(sig) )
    tornado.ioloop.IOLoop.instance().add_callback(shutdown)

def shutdown():
    server.stop()

    #logging.info('Will shutdown in %s seconds ...', MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)
    io_loop = tornado.ioloop.IOLoop.instance()

    deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

    def stop_loop():
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.add_timeout(now + 1, stop_loop)
        else:
            io_loop.stop()
            log.info('Shutdown')
    stop_loop()


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


''' VERIFY API KEY '''
def verify_api_key(key):
    with open(properties.API_KEY_FILE, 'r') as f:
        json = f.readline()
        
    keys = tornado.escape.json_decode(json)
    if key not in keys.values():
        return False
        
    return True

#=============== Global Handlers =======================================#

# Keep alives
# Stats for tracking
# Logging


## ===================== API ===========================================#
## Internal Handlers and not be exposed externally
## ===================== API ===========================================#


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


class MetaDataHandler(tornado.web.RequestHandler):
    """ JOB Status Handler  """
    def get(self, *args, **kwargs):
        
        try:
            query = self.request.query
            params = urlparse.parse_qs(query)
            uri = self.request.uri
            api_key = params[properties.API_KEY][0]
            request_id = params[JOB_ID][0]
            s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
            s3bucket_name = properties.S3_BUCKET_NAME
            s3bucket = Bucket(name = s3bucket_name, connection = s3conn)
            k = Key(s3bucket)
            k.key = str(api_key) + "/" + str(request_id) + "/"+ 'video_metadata.txt'
            status_data = k.get_contents_as_string()
            self.write(resp)

        except S3ResponseError,e:
            resp = "no such job"
            self.write(resp)

        except Exception,e:
            log.error("key=metadata_handler msg=exception " + e.__str__())
            raise tornado.web.HTTPError(400)

        self.finish()

class DequeueHandler(tornado.web.RequestHandler):
    """ DEQUEUE JOB Handler - The queue stores data in json format already """
    def get(self, *args, **kwargs):
        
        try:
            element = global_api_work_queue.get_nowait()
            #send http response
            h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
            self.write(str(element))
            #self.write(str(json.dumps(element,skipkeys=True)))

        except Queue.Empty:
            #Send Queue empty message as a string {}
            self.write("{}")

        except Exception,e:
            log.error("key=dequeue_handler msg=error from work queue")
            raise tornado.web.HTTPError(500)

        self.finish()

class RequeueHandler(tornado.web.RequestHandler):
    """ REQUEUE JOB Handler"""
    def post(self, *args, **kwargs):
        
        try:
            log.info("key=requeue_handler msg=requeing ")
            #data = tornado.escape.url_unescape(self.request.body, encoding='utf-8')
            data = self.request.body
            #TODO Verify data Format
            global_api_work_queue.put(data)
        except Exception,e:
            log.error("key=requeue_handler msg=error " + e.__str__())
            raise tornado.web.HTTPError(500)

        self.finish()

## ===================== API ===========================================#
# External Handlers
## ===================== API ===========================================#

class GetResultsHandler(tornado.web.RequestHandler):
    """ Return results gzipped """
    
    def initialize(self):
        ''' init the s3 connection '''
        self.s3conn = AsyncS3Connection(aws_access_key_id=properties.S3_ACCESS_KEY,aws_secret_access_key = properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_BUCKET_NAME
        self.s3bucket = AsyncBucket(connection = self.s3conn, name = s3bucket_name)
    
    def s3_get_callback(self,data):
        self.write(data)
        self.finish()

    @contextlib.contextmanager
    def exp_handler(self,*args,**kwargs):
        #try:
        #    err = args[1]
        #    print isinstanceof(err,S3ResponseError)
        #    if isinstanceof(err,S3ResponseError):
        #        self.write("Results for the video are not ready yet")
        #except:
        #    pass #raise tornado.web.HTTPError(400)
       
        #either key is not present or some s3 error
        raise tornado.web.HTTPError(400)
        self.finish()

        
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        try:
            query = self.request.query
            params = urlparse.parse_qs(query)
            uri = self.request.uri
            api_key = params[properties.API_KEY][0]
            request_id = params[JOB_ID][0]
            k = AsyncKey(self.s3bucket)
            
            if not verify_api_key(api_key):
                raise Exception("API key invalid")
            
            k.key = str(api_key) + "/" + str(request_id) + "/"+ 'result.tar.gz'
            with tornado.stack_context.ExceptionStackContext(self.exp_handler):
                data = k.get_contents_as_string(callback=self.s3_get_callback)
    
        except S3ResponseError,e:
            log.exception("key=getresultshandler msg=traceback")
            self.write("Result not ready yet")
            self.finish()

        except:
            log.exception("key=getresultshandler msg=general traceback")
            raise tornado.web.HTTPError(400)
            self.finish()

class JobStatusHandler(tornado.web.RequestHandler):
    """ JOB Status Handler  """
    
    def initialize(self):
        ''' init the s3 connection '''
        self.s3conn = AsyncS3Connection(aws_access_key_id=properties.S3_ACCESS_KEY,aws_secret_access_key = properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_BUCKET_NAME
        self.s3bucket = AsyncBucket(connection = self.s3conn, name = s3bucket_name)

    @contextlib.contextmanager
    def exp_handler(self,*args,**kwargs):
        raise tornado.web.HTTPError(400)
        self.finish()

    ''' callback from s3, invoked only on success '''
    def s3_get_callback(self,data):
        self.write(data)
        self.finish()

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        
        try:
            query = self.request.query
            params = urlparse.parse_qs(query)
            uri = self.request.uri
            api_key = params[properties.API_KEY][0]
            request_id = params[JOB_ID][0]
            
            k = AsyncKey(self.s3bucket)
            k.key = str(api_key) + "/" + str(request_id) + "/"+ 'status.txt'
            with tornado.stack_context.ExceptionStackContext(self.exp_handler):
                data = k.get_contents_as_string(callback=self.s3_get_callback)
        
        except S3ResponseError,e:
            self.write("S3 resp error")

        except Exception,e:
            log.error("key=jobstatus_handler msg=exception " + e.__str__())
            raise tornado.web.HTTPError(400)
            self.finish()

class GetThumbnailsHandler(tornado.web.RequestHandler):
    test_mode = False
    parsed_params = {}
    
    def initialize(self):
        ''' init the s3 connection '''
        self.s3conn = AsyncS3Connection(aws_access_key_id=properties.S3_ACCESS_KEY,aws_secret_access_key = properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_BUCKET_NAME
        self.s3bucket = AsyncBucket(connection = self.s3conn, name = s3bucket_name)
        self.response_data = None
        self.queue_data = None
        self.state = 'start' #use enum 

    @contextlib.contextmanager
    def exp_handler(self,*args,**kwargs):
        log.error("key=thumbnail_handler api_key=" + self.parsed_params[properties.API_KEY] + " id=" +
                self.parsed_params[properties.REQUEST_UUID_KEY] + "msg=s3 save failed state=%s" %(self.state))
        raise tornado.web.HTTPError(400)
        self.finish()

    ''' Callback for status s3 save '''
    def save_status_s3_callback(self,s3response):
        self.state = 'savedStatusS3'
        global_api_work_queue.put(self.queue_data)
        self.write(self.response_data)
        self.finish()
       
    ''' Callback for queue data save '''   
    def save_data_s3_callback(self,s3response):
        self.state = 'savedDataS3'
        uuid = self.parsed_params[properties.REQUEST_UUID_KEY]
        api_key = self.parsed_params[properties.API_KEY]
        k = AsyncKey(self.s3bucket)
        k.key = api_key + "/" + uuid + "/" + "status.txt"
        ts = str(time.time())
        data = format_status_json("submitted",ts)
        with tornado.stack_context.ExceptionStackContext(self.exp_handler):
            k.set_contents_from_string(data,callback=self.save_status_s3_callback)

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        try:
            params = tornado.escape.json_decode(self.request.body)
            uri = self.request.uri
            self.parsed_params = {}

            #Verify essential parameters
            try:
                self.parsed_params[properties.API_KEY] = params[properties.API_KEY]
                self.parsed_params[properties.VIDEO_ID] = params[properties.VIDEO_ID]
                self.parsed_params[properties.VIDEO_TITLE] = params[properties.VIDEO_TITLE]
                self.parsed_params[properties.VIDEO_DOWNLOAD_URL] = params[properties.VIDEO_DOWNLOAD_URL]
                self.parsed_params[properties.CALLBACK_URL] = params[properties.CALLBACK_URL]
            except KeyError,e:
                raise Exception("param %s not set" %e.__str__())

            #Verify API Key
            api_key = params[properties.API_KEY]
            if not verify_api_key(api_key):
                raise Exception("API key invalid %s" %api_key)
            
            #compare with supported api methods
            if params.has_key(properties.TOP_THUMBNAILS):
                self.parsed_params[properties.TOP_THUMBNAILS] = min(int(params[properties.TOP_THUMBNAILS]),properties.MAX_THUMBNAILS)
            elif params.has_key(properties.THUMBNAIL_RATE):
                self.parsed_params[properties.THUMBNAIL_RATE] = params[properties.THUMBNAIL_RATE]
            elif params.has_key(properties.THUMBNAIL_INTERVAL):
                self.parsed_params[properties.THUMBNAIL_INTERVAL] = params[properties.THUMBNAIL_INTERVAL]
            
            elif params.has_key(properties.ABTEST_THUMBNAILS):
                #AB Test thumbnail request
                self.parsed_params[properties.ABTEST_THUMBNAILS]  = params[properties.ABTEST_THUMBNAILS]
                self.parsed_params[properties.THUMBNAIL_SIZE] = params[properties.THUMBNAIL_SIZE] #image size 
                
                #verify read and write tokens are specified in the request
                self.parsed_params[properties.BCOVE_READ_TOKEN] = params[properties.BCOVE_READ_TOKEN]
                self.parsed_params[properties.BCOVE_WRITE_TOKEN] = params[properties.BCOVE_WRITE_TOKEN]
            
            elif params.has_key(properties.BRIGHTCOVE_THUMBNAILS):
                self.parsed_params[properties.BRIGHTCOVE_THUMBNAILS]  = params[properties.BRIGHTCOVE_THUMBNAILS]
                self.parsed_params[properties.PUBLISHER_ID]  = params[properties.PUBLISHER_ID] #publisher id
                
                #verify read and write tokens are specified in the request
                self.parsed_params[properties.BCOVE_READ_TOKEN] = params[properties.BCOVE_READ_TOKEN]
                self.parsed_params[properties.BCOVE_WRITE_TOKEN] = params[properties.BCOVE_WRITE_TOKEN]
            
            else:
                #DEFAULT
                raise Exception("api method not supported")

            #update state
            self.state = 'parsedAPIParams'
            
            #Validate Request & Insert in to Queue (serialized/json)
            intermediate_json_data = tornado.escape.json_encode(self.parsed_params)

            #Generate UUID for the request
            ts = str(time.time())
            uuid = hashlib.md5(intermediate_json_data).hexdigest()
            self.parsed_params[properties.REQUEST_UUID_KEY] = uuid
            self.parsed_params[properties.JOB_SUBMIT_TIME] = ts
            json_data = tornado.escape.json_encode(self.parsed_params)
            self.queue_data = json_data 
            self.response_data = "{\"job_id\":\"" + uuid + "\"}"

            #### Write Job status and Requeust to S3
            k = AsyncKey(self.s3bucket)
            k.key = api_key + "/" + uuid + "/" + 'request.txt'
            with tornado.stack_context.ExceptionStackContext(self.exp_handler):
                k.set_contents_from_string(json_data,callback=self.save_data_s3_callback)
        
        except Exception,e:
            log.error("key=thumbnail_handler msg=" + e.__str__());
            self.set_status(400)
            self.finish("<html><body>Bad Request " + e.__str__() + " </body></html>")

    def __parse_common_params(self,params):

        try:
            self.parsed_params[properties.API_KEY] = params[properties.API_KEY][0]
            self.parsed_params[properties.VIDEO_ID] = params[properties.VIDEO_ID][0]
            self.parsed_params[properties.VIDEO_TITLE] = params[properties.VIDEO_TITLE][0]
            self.parsed_params[properties.VIDEO_DOWNLOAD_URL] = params[properties.VIDEO_DOWNLOAD_URL][0]
            self.parsed_params[properties.CALLBACK_URL] = params[properties.CALLBACK_URL][0]

        except KeyError,e:
            raise Exception("params not set") #convert to custom exception

    def __top_n_handler(self,params):
        if params.has_key(properties.TOP_THUMBNAILS):
            self.parsed_params[properties.TOP_THUMBNAILS] = params[properties.TOP_THUMBNAILS][0]      
        else:
            raise Exception("parmas not set")

    def __rate_handler(self,params):
        if params.has_key(properties.THUMBNAIL_RATE):
            self.parsed_params[properties.THUMBNAIL_RATE] = params[properties.THUMBNAIL_RATE][0]      
        else:
            raise Exception("parmas not set")

    def __interval_handler(self,params):
        if params.has_key(properties.THUMBNAIL_INTERVAL):
            self.parsed_params[properties.THUMBNAIL_INTERVAL] = params[properties.THUMBNAIL_INTERVAL][0]      
        else:
            raise Exception("parmas not set")

    def verify_api_key(self,key):
        with open(properties.API_KEY_FILE, 'r') as f:
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
            log.info("key=testcallback msg=output: " + self.request.body)
        except Exception,e:
            raise tornado.web.HTTPError(500)  
            log.error("key=testcallback msg=error recieving message")
        
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
    (r'/api/v1/videometadata',MetaDataHandler),    
    (r'/api/v1/getresults',GetResultsHandler),    
])

def main():

    global server
    tornado.options.parse_command_line()
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
	main()
