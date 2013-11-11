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
import json
import threading
import shortuuid
import utils.neon
import utils.ps

import boto.exception
from boto.s3.connection import S3Connection

#logging
import logging
_log = logging.getLogger(__name__)

#Tornado options
from utils.options import define, options
define("port", default=9080, help="run on the given port", type=int)
define("test", default=0, help="populate queue for test", type=int)
define("batch_count", default=100, type=int, 
       help="Number of lines to bacth to s3")
define("s3disk", default="/mnt/neon/s3diskbacklog",
        help="backup lines which failed to upload s3", type=str)
define("bucket_name", default='neon-tracker-logs',
       help='Bucket to store the logs on')
define("flush_interval", default=120, type=float,
       help='Interval in seconds to force a flush to S3')

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
        self.page = page # Page where the video is shown

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

class TrackerDataHandler(tornado.web.RequestHandler):
    def parse_tracker_data(self):
        '''Parses the tracker data from a GET request.

        returns:
        TrackerData object
        '''
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

        cip = self.request.remote_ip
        return TrackerData(action,id,ttype,cts,sts,page,cip,imgs,cvid)


class LogLines(TrackerDataHandler):

    def __init__(self, q):
        super(LogLines, self).__init__()
        self.q = q
    
    ''' Track call logger '''
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        try:
            tracker_data = self.parse_tracker_data()
        except Exception,e:
            _log.exception("key=get_track msg=%s" %e) 
            self.finish()
            return

        data = tracker_data.to_json()
        try:
            q.put(data)
        except Exception,e:
            _log.exception("key=loglines msg=Q error %s" %e)
        self.finish()

    '''
    Method to check memory on the node
    '''
    def memory_check(self):
        return True

class TestTracker(TrackerDataHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        try:
            tracker_data = self.parse_tracker_data()
        except Exception,e:
            _log.exception("key=test_track msg=%s" %e) 
            self.finish()
            return
        
        data = tracker_data.to_json()
        self.set_header("Content-Type", "application/json")
        self.write(cb + "("+ data + ")") #wrap json data in callback
        self.finish()

###########################################
# S3 Handler thread 
###########################################
class S3Handler(threading.Thread):
    def __init__(self, dataQ):
        super(S3Handler, self).__init__()
        S3_ACCESS_KEY = 'AKIAJ5G2RZ6BDNBZ2VBA' 
        S3_SECRET_KEY = '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'
        self.s3conn = S3Connection(aws_access_key_id=S3_ACCESS_KEY,
                                   aws_secret_access_key =S3_SECRET_KEY)
        self.dataQ = dataQ
        self.last_upload_time = time.time()
        self.daemon = True

        self.s3bucket = self.s3conn.lookup(options.bucket_name)
        if self.s3bucket is None:
            self.s3bucket = self.s3conn.create_bucket(options.bucket_name)

    def upload_to_s3(self, data):
        k = self.s3bucket.new_key(shortuuid.uuid())
        try:
            k.set_contents_from_string(data)
            self.last_upload_time = time.time()
        except boto.exception.S3ResponseError,e:
            _log.exception("key=upload_to_s3 msg=S3 Errror %s"%e)
            self.save_to_disk(data)

    def save_to_disk(self, data):
        if not os.path.exists(options.s3disk):
            os.makedirs(options.s3disk)
        
        fname = "s3backlog_%s" %time.time()
        with open(options.s3disk +'/' + fname,'a') as f:
            f.write(data)

    def run(self):
        data = ''
        nlines = 0
        while True:
            try:
                data += self.dataQ.get()
                data += '\n'
                nlines += 1

                # If we have enough data or it's been too long, upload
                if (nlines >= options.batch_count or 
                    (self.last_upload_time + options.flush_interval <= 
                     time.time())):
                    if nlines > 0:
                        _log.info('Uploading %i lines to S3' % nlines)
                        self.upload_to_s3(data)
                        data = ''
                        nlines = 0
            except Exception as e:
                _log.exception("key=do_work msg=%s" % e)
            self.dataQ.task_done()

###########################################
# Create Tornado server application
###########################################

def main():
    event_queue = Queue.Queue()

    application = tornado.web.Application([
        (r"/",LogLines),
        (r"/track",LogLines, dict(q=event_queue)),
        (r"/test",TestTracker),
        ])
    server = tornado.httpserver.HTTPServer(application)
    utils.ps.register_tornado_shutdown(server)
    server.listen(options.port)
    
    s3handler = S3Handler(event_queue)
    s3handler.start() 
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
