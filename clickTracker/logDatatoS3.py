'''
Consumer client that drains the queue from the track logger and uploads data in to s3
Currently uses blocking http calls to drain Qs from localhost & boto to upload to s3

TODO: Integrate in to ioloop with delayed callbacks to drain Qs and upload data
TODO: Store data in binary format
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.escape
import multiprocessing
import Queue
import signal
import time
import os
import time
import random
import sys
import shortuuid
import utils.neon

import logging
_log = logging.getLogger(__name__)

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

#async s3 connection
from botornado.s3.connection import AsyncS3Connection
from botornado.s3.bucket import AsyncBucket
from botornado.s3.key import AsyncKey
#for more info on stack context mgmt - http://www.tornadoweb.org/en/branch2.4/_modules/tornado/stack_context.html
import tornado.stack_context
import contextlib

#Tornado options
from utils.options import define, options
define("port", default=9080, help="port to consume data from", type=int)
define("lines", default=1000, help="lines to aggregate", type=int)
define("fetch_count", default=100, help="# lines to fetch", type=int)
define("bucket_name", default='neon-tracker-logs',
       help='Bucket to store the logs on')
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3
    

global log_dir 
#log_dir = "/var/log/neon"
log_dir = os.getcwd() 

def sig_handler(sig, frame):
    _log.debug('Caught signal: ' + str(sig) )
    # Need to do this because tornado will catch a system exit exception
    os._exit(0)

class S3DataHandler(object):
    
    def __init__(self,s3_line_count,port,fetch_count,s3bucket=None):
        S3_ACCESS_KEY = 'AKIAJ5G2RZ6BDNBZ2VBA' 
        S3_SECRET_KEY = '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'
        self.s3conn = S3Connection(aws_access_key_id=S3_ACCESS_KEY,
                                   aws_secret_access_key =S3_SECRET_KEY)
        self.s3bucket = s3bucket or Bucket(connection=self.s3conn,
                                           name=options.bucket_name)
        
        #self.s3conn = AsyncS3Connection(aws_access_key_id=S3_ACCESS_KEY,aws_secret_access_key =S3_SECRET_KEY)
        #self.s3bucket = AsyncBucket(connection = self.s3conn, name = s3bucket_name)
        self.sleep = 1
        self.nlines = 0
        self.fetch_count = fetch_count
        self.fetch_url = ('http://localhost:%i/getlines?count=%i' 
                          % (port, fetch_count)) #go through the loadbalancer
        self.s3_line_count = s3_line_count
        self.lines_to_save = ''

    '''
    @contextlib.contextmanager
    def exception_handler(self,typ,value,tb):
        if isinstance(value,S3ResponseError):
            print "error"

        self.finish()

    #Async
    def upload_data_to_s3(data):
       
        def save_data(response):
            pass

        k = AsyncKey(self.s3bucket)
        k.key = 'todofilescheme-' + str(time.time())
        with tornado.stack_context.ExceptionStackContext(self.exception_handler):
            k.set_contents_as_string(data,callback=save_data)

    def do_work_async(self):
        lines = "" 
        counter = 0
        def get_lines(response):
            if not response.error:
                lines += response.body
            
            #upload data to s3
            if counter == len(self.fetch_urls):
                self.upload_data_to_s3(lines)

        http_client = tornado.httpclient.AsyncHTTPClient()
        for url in self.fetch_urls:
            req = tornado.httpclient.HTTPRequest(url = url,
                            method = "GET",request_timeout = 10.0, connect_timeout = 10.0)
            http_client.fetch(req,get_lines)
    '''

    def do_work(self):
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = self.fetch_url,
                                             method = "GET",
                                             request_timeout = 10.0, 
                                             connect_timeout = 10.0)
        try:
            response = http_client.fetch(req)
            self.lines_to_save += response.body
            self.nlines += response.body.count('\n')
        except:
            return

        #upload to s3
        if self.nlines >= self.s3_line_count: 
            k = Key(self.s3bucket)
            k.key = shortuuid.uuid() 
            try:
                k.set_contents_from_string(self.lines_to_save)
                self.nlines = 0
                self.lines_to_save = ''
                _log.info("key=do_work msg=saved to s3")
            except S3ResponseError,e:
                pass

    def run(self):
        while True:
            self.do_work()
            time.sleep(self.sleep)

def main():

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    #ioloop = tornado.ioloop.IOLoop.instance()
    #ioloop.start()
    s = S3DataHandler(options.lines,options.port,options.fetch_count)
    s.run()


# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
