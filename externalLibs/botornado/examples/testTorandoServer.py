import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.options
import multiprocessing
import Queue
import signal
import time
import os
import time
import random
import botornado.s3
from botornado.s3.connection import AsyncS3Connection
from botornado.s3.bucket import AsyncBucket
from botornado.s3.key import AsyncKey

#Tornado options
from tornado.options import define, options
define("port", default=8081, help="run on the given port", type=int)
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3
    

class TestHandler(tornado.web.RequestHandler):
    
    def initialize(self):
        a = 'AKIAIHY5JHBE2BTFR4SQ'
        s = '1ByiJJdZIuKTUfH7bqfJ8spmopEB8AUgzjpdSyJf'
        self.s3 = AsyncS3Connection(aws_access_key_id= a,
                aws_secret_access_key=s)
        self.bucket = AsyncBucket(connection = self.s3, name = 'internal-test')

    def cb(self,response):
        b = response
        print "buckets", b 
        #self.bucket.get_all_keys(callback = self.all_keys)

    def test_save_key(self):
        print "save this test key"
        k = AsyncKey(bucket=self.bucket)
        k.key = "test.key"
        k.set_contents_from_string(str(time.time()),cb=self.save_callback,num_cb=2,callback=self.after_save)

    def save_callback(self,nbytes,size):
            #print nbytes,size 
            if nbytes == size:
                print "saved all bytes",time.time()

    def after_save(self,response):
            print response, time.time() #returns async http response
            self.write("success" + str(time.time()))
            self.finish()

    def get(self, *args, **kwargs):
        try:
            self.s3.get_all_buckets(callback = self.cb)
            self.test_save_key()

        except Exception,e:
            print e

    
# Create Tornado server application
###########################################

application = tornado.web.Application([
    (r"/",TestHandler),
])

def main():
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
	main()
