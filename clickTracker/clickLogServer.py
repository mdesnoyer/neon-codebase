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
import json

#logging
import logging
import logging.handlers
log = logging.getLogger(__name__)
log.setLevel(logging.WARNING)
formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler = logging.FileHandler("error.log")
log.addHandler(handler)

#Tornado options
from tornado.options import define, options
define("port", default=9080, help="run on the given port", type=int)
MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3
    
#############################################
#GLOBALS
#############################################

def sig_handler(sig, frame):
    log.warn('Caught signal: ' + str(sig) )
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

#############################################
#### DATA FORMAT ###
#############################################

class ClickData(object):
    '''
    Schema for click tracker data
    '''
    def __init__(self,action,id,aid,ttype,cts,sts,page,cip,imgs,cvid=None):
        self.a = action # load/ click
        self.id = id    # page load id
        self.aid = aid #account id
        self.ttype = ttype #tracker type
        self.ts = cts #client timestamp
        self.sts = sts #server timestamp
        self.cip = cip #client IP

        if isinstance(imgs,list):        
            self.imgs = imgs #image list
            self.cvid = cvid #current video in the player
        else:
            self.img = imgs  #clicked image
        
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

#############################################
#### WEB INTERFACE #####
#############################################

class LogLines(tornado.web.RequestHandler):
    
    ''' Track call logger '''
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        try:
            cvid = None
            action = self.get_argument('a')
            id = self.get_argument('id')
            aid = self.get_argument('aid') 
            ttype = self.get_argument('ttype')
            cts = self.get_argument('ts')
            sts = int(time.time())
            page = self.get_argument('page') #url decode
            if action == 'load':
                imgs = self.get_argument('imgs')
                cvid = self.get_argument('cvid')
            else:
                imgs = self.get_argument('img')
            cip = self.request.remote_ip

        except Exception,e:
            log.exception("key=get_track msg=%s" %e) 
        
        cd = ClickData(action,id,aid,ttype,cts,sts,page,cip,imgs,cvid)
        data = cd.to_json()

        try:
            event_queue.put(data)
        except Exception,e:
            log.exception("key=loglines msg=Q error %s" %e)
        self.finish()

    '''
    Method to check memory on the node
    '''
    def memory_check(self):
        return True

'''
Retrieve lines from the server
'''
class GetLines(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
       
        count = 1
        try:
            count = self.get_argument('count')
        except:
            pass

        qsize = event_queue.qsize()
        data = ''
        if qsize > count:
            for i in range(count):
                data += event_queue.get_nowait()  
                data += '\n'

        self.write(data)
        self.finish()

###########################################
# Create Tornado server application
###########################################

application = tornado.web.Application([
    (r"/",LogLines),
    (r"/track",LogLines),
    (r"/getlines",GetLines),
])

def main():

    global server
    global event_queue

    event_queue = multiprocessing.Queue()
    
    tornado.options.parse_command_line()
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    #server.bind(options.port)
    #server.start(0)
    tornado.ioloop.IOLoop.instance().start()

def test():
    QMAX = 5000
    tdata = '{"k1":"v1","k2":"v2"}'
    for i in range(QMAX):
        event_queue.put(tdata)

# ============= MAIN ======================== #
if __name__ == "__main__":
    main()
