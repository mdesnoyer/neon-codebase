#!/usr/bin/env python
''''
Server that logs data from the tracker in to S3

Tornado server listens for http requests and puts in to the Q a TrackerData json obj
A thread dequeues data and sends the data to s3
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
import Queue
import signal
import time
import os
import time
import json
import threading
import shortuuid
import utils.neon
import utils.ps

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

#logging
import logging
_log = logging.getLogger(__name__)

#Tornado options
from utils.options import define, options
define("port", default=9080, help="run on the given port", type=int)
define("test", default=0, help="populate queue for test", type=int)
define("batch_count", default=100, help="nlines to bacth to s3", type=int)
define("s3disk", default="/mnt/neon/s3diskbacklog",
        help="backup lines which failed to upload s3", type=str)
define("bucket_name", default='neon-tracker-logs',
       help='Bucket to store the logs on')

#############################################
#### DATA FORMAT ###
#############################################

class TrackerData(object):
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
            ttype = self.get_argument('ttype')
            action = self.get_argument('a')
            id = self.get_argument('id')
            cts = self.get_argument('ts')
            sts = int(time.time())
            page = self.get_argument('page') #url decode
            
            cvid = None

            #On load the current video loaded in the player is logged
            if action == 'load':
                imgs = self.get_argument('imgs')
                imgs = [e.strip('"\' ') for e in imgs.strip('[]').split(',')]
                if ttype != 'imagetracker':
                    cvid = self.get_argument('cvid')
            else:
                imgs = self.get_argument('img')

        except Exception,e:
            _log.exception("key=get_track msg=%s" %e) 
            self.finish()
            return

        cip = self.request.remote_ip
        cd = TrackerData(action,id,ttype,cts,sts,page,cip,imgs,cvid)
        data = cd.to_json()
        try:
            event_queue.put(data)
        except Exception,e:
            _log.exception("key=loglines msg=Q error %s" %e)
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
                    _log.error("key=GetLines msg=Q error")

        self.write(data)
        self.finish()

class TestTracker(tornado.web.RequestHandler):
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
            cb  = self.get_argument('callback') #json callback
            if action == 'load':
                imgs = self.get_argument('imgs')
                imgs = [e.strip('"\' ') for e in imgs.strip('[]').split(',')]
                try:
                    cvid = self.get_argument('cvid')
                except:
                    pass
            else:
                imgs = self.get_argument('img')

        except Exception,e:
            _log.exception("key=test msg=%s" %e) 
            self.finish()
            return

        cip = None
        cd = TrackerData(action,id,ttype,cts,sts,page,cip,imgs,cvid)
        data = cd.to_json()
        self.set_header("Content-Type", "application/json")
        self.write(cb + "("+ data + ")") #wrap json data in callback
        self.finish()

###########################################
# S3 Handler thread 
###########################################
class S3Handler(threading.Thread):
    def __init__(self,queue,s3_line_count,fetch_count,s3bucket=None):
        super(S3Handler, self).__init__()
        S3_ACCESS_KEY = 'AKIAJ5G2RZ6BDNBZ2VBA' 
        S3_SECRET_KEY = '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'
        self.s3conn = S3Connection(aws_access_key_id=S3_ACCESS_KEY,
                                   aws_secret_access_key =S3_SECRET_KEY)
        self.s3bucket = s3bucket or Bucket(connection=self.s3conn,
                                           name=options.bucket_name)
        self.dataQ = queue
        self.sleep = 5
        self.last_upload_time = time.time()
        self.upload_interval = 120 # flush every 2 mins
        self.nlines = 0
        self.fetch_count = fetch_count #count to fetch from Queue
        self.s3_batch_count = s3_line_count #Batch line count to s3
        self.lines_to_save = ''
        self.daemon = True

    def upload_to_s3(self):
        ret = 's3'
        k = Key(self.s3bucket)
        k.key = shortuuid.uuid() 
        try:
            k.set_contents_from_string(self.lines_to_save)
            self.last_upload_time = time.time()
        except S3ResponseError,e:
            _log.exception("key=upload_to_s3 msg=S3 Errror %s"%e)
            self.save_to_disk()
            ret = 'disk'

        self.nlines = 0
        self.lines_to_save = ''
        return ret

    def save_to_disk(self):
        if not os.path.exists(options.s3disk):
            os.makedirs(options.s3disk)
        
        fname = "s3backlog_%s" %time.time()
        with open(options.s3disk +'/' + fname,'a') as f:
            f.write(self.lines_to_save)

    def do_work(self):
        ret = False
        qsize = self.dataQ.qsize()
        data = ''

        if qsize >= self.fetch_count:
            for i in range(self.fetch_count):
                try:
                    data += self.dataQ.get_nowait()  
                    data += '\n'
                    self.nlines += 1
                except Exception,e:
                    _log.debug("key=do_work msg=%s" %e)
        self.lines_to_save += data
    
        #If time since the last upload has been long enough --upload
        if self.last_upload_time + self.upload_interval <= time.time() or self.nlines >= self.s3_batch_count: 
            if self.nlines >0:
                ret = self.upload_to_s3()
        return ret

    def run(self):
        while True:
            self.do_work()
            time.sleep(self.sleep)

###########################################
# Create Tornado server application
###########################################

application = tornado.web.Application([
    (r"/",LogLines),
    (r"/track",LogLines),
    (r"/test",TestTracker),
])

def main():
    global server
    global event_queue
    event_queue = Queue.Queue() 
    server = tornado.httpserver.HTTPServer(application)
    utils.ps.register_tornado_shutdown(server)
    server.listen(options.port)
    #server.bind(options.port)
    #server.start(0)
    
    s3handler = S3Handler(event_queue,options.batch_count,10)
    s3handler.start() 
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
