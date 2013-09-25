'''
Consumer client that drains the queue from the track logger and uploads data in to s3
Currently uses blocking http calls to drain Qs from localhost & boto to upload to s3

TODO: Integrate in to ioloop with delayed callbacks to drain Qs and upload data
'''
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.escape
import multiprocessing
import Queue
import signal
import time
import errorlog
import os
import time
import random
import sys

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
from tornado.options import define, options
define("port", default=9080, help="run on the given port", type=int)
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3
    
global log
log = errorlog.FileLogger("server")

global log_dir 
#log_dir = "/var/log/neon"
log_dir = os.getcwd() 

def sig_handler(sig, frame):
    log.debug('Caught signal: ' + str(sig) )
    sys.exit(0)

class S3DataHandler(object):
    
    def __init__(self):
        S3_ACCESS_KEY = 'AKIAJ5G2RZ6BDNBZ2VBA'
        S3_SECRET_KEY = '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'
        s3bucket_name = 'neon-tracker-logs'
        self.s3conn = S3Connection(aws_access_key_id=S3_ACCESS_KEY,aws_secret_access_key =S3_SECRET_KEY)
        self.s3bucket = Bucket(connection = self.s3conn, name = s3bucket_name)
        
        #self.s3conn = AsyncS3Connection(aws_access_key_id=S3_ACCESS_KEY,aws_secret_access_key =S3_SECRET_KEY)
        #self.s3bucket = AsyncBucket(connection = self.s3conn, name = s3bucket_name)
        self.sleep = 1
        self.fetch_urls = ['http://localhost:9080/getlines']
        self.fetch_count = 1

    @contextlib.contextmanager
    def exception_handler(self,typ,value,tb):
        if isinstance(value,S3ResponseError):
            print "error"

        self.finish()

            
    def upload_data_to_s3(data):
       
        def save_data(response):
            pass

        k = AsyncKey(self.s3bucket)
        k.key = 'todofilescheme-' + str(time.time())
        with tornado.stack_context.ExceptionStackContext(self.exception_handler):
            k.set_contents_as_string(data,callback=save_data)

    def do_work_async(self):
        lines = ''
        counter = 0
        def get_lines(response):
            if not response.error:
                lines += response.body
                print lines

            #upload data to s3
            if counter == len(self.fetch_urls):
                self.upload_data_to_s3(lines)

        http_client = tornado.httpclient.AsyncHTTPClient()
        for url in self.fetch_urls:
            req = tornado.httpclient.HTTPRequest(url = url,
                            method = "GET",request_timeout = 10.0, connect_timeout = 10.0)
            http_client.fetch(req,get_lines)
    
    def do_work(self):
        lines = ''
        http_client = tornado.httpclient.HTTPClient()
        for url in self.fetch_urls:
            req = tornado.httpclient.HTTPRequest(url = url,
                            method = "GET",request_timeout = 10.0, connect_timeout = 10.0)
            response = http_client.fetch(req)
            lines += response.body
        
        #upload to s3
        k = Key(self.s3bucket)
        k.key = 'todofilescheme-' + str(time.time())
        try:
            k.set_contents_as_string(lines)
        except S3ResponseError,e:
            pass
    
    def run(self):
        while True:
            self.do_work()
            time.sleep(self.sleep)

def main():

    tornado.options.parse_command_line()
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    #ioloop = tornado.ioloop.IOLoop.instance()
    #ioloop.start()
    s = S3DataHandler()
    s.run()


# ============= MAIN ======================== #
if __name__ == "__main__":
	main()
