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

from boto.exception import S3ResponseError
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
    tornado.ioloop.IOLoop.instance().add_callback(shutdown)

def shutdown():
    server.stop()
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

class EventLogger(tornado.web.RequestHandler):
    
    def initialize(self):
        S3_ACCESS_KEY = 'AKIAJ5G2RZ6BDNBZ2VBA'
        S3_SECRET_KEY = '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'
        self.nlines = 0
        self.maxLines = 100000 
        self.flush_size = random.randint(100,200) #Randomize flush sizes so that not all clients are flusing at the same time
        self.event_log_file = self.getFileName() 
        self.s3conn = AsyncS3Connection(aws_access_key_id=S3_ACCESS_KEY,aws_secret_access_key =S3_SECRET_KEY)
        s3bucket_name = 'neon-tracker-logs1'
        self.s3bucket = AsyncBucket(connection = self.s3conn, name = s3bucket_name)


    def getFileName(self):
        return log_dir + "/" + "neon-events-" + str(time.time()) 
    
    @contextlib.contextmanager
    def exception_handler(self,typ,value,tb):
        if isinstance(value,S3ResponseError):
            print "error"

        self.finish()

    ''' Event logger '''
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        
        
        def save_status_s3_callback(result):
            print result
            self.finish()

        k = AsyncKey(self.s3bucket)
        k.key = "sometestkey"
        data = tornado.escape.json_encode(self.request.arguments)
        with tornado.stack_context.ExceptionStackContext(self.exception_handler):
            k.set_contents_from_string(data,callback=save_status_s3_callback)
        self.nlines +=1 
    
###########################################
# Create Tornado server application
###########################################

application = tornado.web.Application([
    (r"/",EventLogger),
    (r"/eventlog",EventLogger),
])

def main():

    global server
    global event_queue

    event_queue = multiprocessing.Queue()
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    

    #TODO : Start server with multiple ports 
    tornado.options.parse_command_line()
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
	main()
