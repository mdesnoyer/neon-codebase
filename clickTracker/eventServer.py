import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import multiprocessing
import Queue
import signal
import time
import errorlog
import os
import time
import random

#Tornado options
from tornado.options import define, options
define("port", default=8081, help="run on the given port", type=int)
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


class TempfsFile(file):
    ''' File class to create temp file on tmpfs and then copy to disk on close '''
    def __init__(self):
        pass
    def __delete__(self):
        pass


class EventLogger(tornado.web.RequestHandler):
    
    def __init__(self):
        self.nlines = 0
        self.maxLines = 100000 
        self.flush_size = random.randint(100,200) #Randomize flush sizes so that not all clients are flusing at the same time
        self.event_log_file = self.getFileName() 
   
    def getFileName(self):
        return log_dir + "/" + "neon-events-" + str(time.time()) 
    
    ''' Event logger '''
    def get(self, *args, **kwargs):
        try:
            data = tornado.escape.json_encode(self.request.arguments)
            event_queue.put(data)
            qsize = event_queue.qsize()
            if qsize >= self.flush_size:
                data = ""
                for i in range(self.flush_size):
                    try:
                        data += event_queue.get_nowait()
                        data += "\n"
                    except:
                        pass
                self.write_to_file(data)
        
        except Exception,e:
            log.exception("key=getException msg=trace" +  e.__str__() )

        self.nlines +=1 
        self.finish()
    
    def write_to_file(self,data):
        
        #get a new file
        if self.nlines > self.maxLines:
            self.nlines = 0
            self.event_log_file =  self.getFileName()

        with open(self.event_log_file,'a') as f :
            f.write(data)

    def check_file_size(self):
        #TODO : handle gracefully when the disk is full
        st    = os.statvfs('/')
        free  = st.f_bavail * st.f_frsize
        total = st.f_blocks * st.f_frsize
        used  = (st.f_blocks - st.f_bfree) * st.f_frsize
        free_space = float(free) / total
        return free_space

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
    

    tornado.options.parse_command_line()
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
	main()
