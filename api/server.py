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
            try:
                k.key = str(api_key) + "/" + str(request_id) + "/"+ 'result.tar.gz'
                data = k.get_contents_as_string()
                self.write(data)
            except Exception,e:
                log.exception("key=getresultshandler msg=traceback")
                raise tornado.web.HTTPError(400)
            self.finish()

        except:
            log.exception("key=getresultshandler msg=general traceback")

class JobStatusHandler(tornado.web.RequestHandler):
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
            k.key = str(api_key) + "/" + str(request_id) + "/"+ 'status.txt'
            status_data = k.get_contents_as_string()
            try:
                k.key = str(api_key) + "/" + str(request_id) + "/"+ 'response.txt'
                data = k.get_contents_as_string()
                decoded_data =  tornado.escape.json_decode(data)
                resp = format_status_json("finished",decoded_data["timestamp"],decoded_data)

            # Tried to get the response data from s3. If failed due to S3 Response error, file doesnt exist
            # The request is still being processed    
            except S3ResponseError,e:
                #resp = "{\"status\":\"submitted\",\"result\":" + tstamp + "}"
                resp =  status_data #"{\"status\":" + status_data + "}"

            self.write(resp)

        except S3ResponseError,e:
            resp = "{\"status\":\"no such job\"}"
            self.write(resp)
            #raise tornado.web.HTTPError(204)

        except Exception,e:
            log.error("key=jobstatus_handler msg=exception " + e.__str__())
            raise tornado.web.HTTPError(400)

        self.finish()

class GetThumbnailsHandler(tornado.web.RequestHandler):
    test_mode = False
    parsed_params = {}

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
                raise Exception("params not set") #convert to custom exception


            #Verify API Key
            if not self.verify_api_key(self.parsed_params[properties.API_KEY]):
                raise Exception("API key invalid")
            
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
            
            else:
                #DEFAULT
                raise Exception("api method not supported")

            #Validate Request & Insert in to Queue (serialized/json)
            intermediate_json_data = tornado.escape.json_encode(self.parsed_params)

            #Generate UUID for the request
            ts = str(time.time())
            uuid = hashlib.md5(intermediate_json_data).hexdigest()
            self.parsed_params[properties.REQUEST_UUID_KEY] = uuid
            self.parsed_params[properties.JOB_SUBMIT_TIME] = ts
            json_data = tornado.escape.json_encode(self.parsed_params)
            global_api_work_queue.put(json_data)
            response_data = "{\"job_id\":\"" + uuid + "\"}"

            #### Write Job status and Requeust to S3
            s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
            s3bucket_name = properties.S3_BUCKET_NAME
            s3bucket = Bucket(name = s3bucket_name, connection = s3conn)
            k = Key(s3bucket)

            #save request data 
            retries = 3
            for i in range(retries):
                try:
                    k.key = self.parsed_params[properties.API_KEY] + "/" + self.parsed_params[REQUEST_UUID_KEY] + "/" + 'request.txt'
                    k.set_contents_from_string(json_data)
                    break
                except S3ResponseError,e:
                    log.error("key=thumbnail_handler api_key=" + self.parsed_params[properties.API_KEY] + " id=" + uuid + " msg=request save failed: " + e.__str__())
                    continue

            #Respond for duplicate job; If result already stored, return it 
            #error = "", no retry --- Just reply with old time stamp 
            try:
                k.key = self.parsed_params[properties.API_KEY] + "/" + self.parsed_params[REQUEST_UUID_KEY] + "/" + "response.txt"
                old_response_data = k.get_contents_as_string()
                if old_response_data != 'requeued':
                    old_response = tornado.escape.json_decode(old_response_data)
                    if old_response.has_key('error'):
                        if len(old_response['error']) == 0:
                            response_data = "{\"job_id\":\"" + uuid + "\", \"prev_response\":" + old_response_data  + "}"

            #''' submit the jonb on s3 '''
            except S3ResponseError,e:
                try:
                    k.key = self.parsed_params[properties.API_KEY] + "/" + uuid + "/" + "status.txt"
                    data = format_status_json("submitted",ts)
                    k.set_contents_from_string(data)
                except:
                    pass

            self.write(response_data)

        except Exception,e:
            #TODO  Write appropriate error messages with error cocdes
            log.error("key=thumbnail_handler msg=" + e.__str__());
            #raise tornado.web.HTTPError(400,e.__str__())
            self.set_status(400)
            self.finish("<html><body>Bad Request " + e.__str__() + " </body></html>")
            return

        self.finish()

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

    def verify_api_key(selfi,key):
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
    #(r'/api/v1/get_youtube/(.*)',GetYoutube),
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
