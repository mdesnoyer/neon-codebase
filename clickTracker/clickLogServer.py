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
define("test", default=0, help="populate queue for test", type=int)
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
    def __init__(self,action,id,ttype,cts,sts,page,cip,imgs,cvid=None):
        self.a = action # load/ click
        self.id = id    # page load id
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
        
        cd = ClickData(action,id,ttype,cts,sts,page,cip,imgs,cvid)
        data = cd.to_json()
        print data
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
            count = int(self.get_argument('count'))
        except:
            pass

        qsize = event_queue.qsize()
        data = ''
        if qsize > count:
            for i in range(count):
                try:
                    data += event_queue.get_nowait()  
                    data += '\n'
                except:
                    log.error("key=GetLines msg=Q error")

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

    event_queue = Queue.Queue() #multiprocessing.Queue()
    tornado.options.parse_command_line()
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    if options.test:
        test()
    #server.bind(options.port)
    #server.start(0)
    tornado.ioloop.IOLoop.instance().start()

def test():
    QMAX = 5000
    tdata = '{"a": "load", "img": "[\"http://brightcove.vo.llnwd.net/d21/unsecured/media/2294876105001/201310/34/2294876105001_2727914703001_thumbnail-2296855887001.jpg\",\"http://brightcove.vo.llnwd.net/d21/unsecured/media/2294876105001/201310/354/2294876105001_2727881607001_thumbnail-2369368872001.jpg\",\"http://brightcove.vo.llnwd.net/e1/pd/2294876105001/2294876105001_2660525568001_thumbnail-2296855886001.jpg\",\"http://brightcove.vo.llnwd.net/e1/pd/2294876105001/2294876105001_2617231423001_thumbnail-2323153341001.jpg\"]", "ts": "1382056097928", "cip": "::1", "sts": 1382056097, "ttype": "flashonlyplayer", "id": "2787acedddac5e1c"}' 
    for i in range(QMAX):
        event_queue.put(tdata)

# ============= MAIN ======================== #
if __name__ == "__main__":
    main()
