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


import json
import os
import Queue
import multiprocessing
import shortuuid
import threading
import time
import tornado.gen
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.escape
import utils.neon
import utils.ps

import boto.exception
from boto.s3.connection import S3Connection

#logging
import logging
_log = logging.getLogger(__name__)

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

from utils import statemon
statemon.define('qsize', int)
statemon.define('buffer_size', int)

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

    def initialize(self, q, watcher):
        self.q = q
        self.watcher = watcher
    
    ''' Track call logger '''
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        with self.watcher.activate():
            try:
                tracker_data = self.parse_tracker_data()
            except Exception, e:
                _log.exception("key=get_track msg=%s" %e) 
                self.set_status(500)
                self.finish()
                return

            data = tracker_data.to_json()
            try:
                self.q.put(data)
                statemon.state.qsize = self.q.qsize()
                self.set_status(200)
            except Exception, e:
                _log.exception("key=loglines msg=Q error %s" %e)
                self.set_status(500)
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
    def __init__(self, dataQ, watcher=utils.ps.ActivityWatcher()):
        super(S3Handler, self).__init__()
        s3conn = S3Connection()
        self.dataQ = dataQ
        self.last_upload_time = time.time()
        self.daemon = True
        self.watcher = watcher

        bucket = s3conn.lookup(options.bucket_name)
        if bucket is None:
            s3conn.create_bucket(options.bucket_name)

    def upload_to_s3(self, data):
        try:
            s3conn = S3Connection()
            bucket = s3conn.get_bucket(options.bucket_name)
            k = bucket.new_key(shortuuid.uuid())
            k.set_contents_from_string(data)
        except boto.exception.BotoServerError as e:
            _log.exception("key=upload_to_s3 msg=S3 Error %s" % e)
            self.save_to_disk(data)
        except IOError as e:
            _log.exception("key=upload_to_s3 msg=S3 I/O Error %s" % e)
            self.save_to_disk(data)

    def save_to_disk(self, data):
        if not os.path.exists(options.s3disk):
            os.makedirs(options.s3disk)
        
        fname = "s3backlog_%s.log" % time.time()
        with open(os.path.join(options.s3disk, fname),'a') as f:
            f.write(data)

    def run(self):
        data = ''
        nlines = 0
        while True:
            try:
                was_empty = False
                try:
                    new_data = self.dataQ.get(True, options.flush_interval)
                    if data != '':
                        data += '\n'
                    data += new_data
                    nlines += 1
                except Queue.Empty:
                    was_empty = True

                statemon.state.qsize = self.dataQ.qsize()
                statemon.state.buffer_size = nlines

                # If we have enough data or it's been too long, upload
                if (nlines >= options.batch_count or 
                    (self.last_upload_time + options.flush_interval <= 
                     time.time())):
                    if nlines > 0:
                        _log.info('Uploading %i lines to S3' % nlines)
                        with self.watcher.activate():
                            self.upload_to_s3(data)
                            data = ''
                            nlines = 0
                            self.last_upload_time = time.time()
            except Exception as e:
                _log.exception("key=s3_uploader msg=%s" % e)
            

            statemon.state.qsize = self.dataQ.qsize()
            statemon.state.buffer_size = nlines
            if not was_empty:
                self.dataQ.task_done()

###########################################
# Create Tornado server application
###########################################

class Server(threading.Thread):
    '''The server, which can be run as it's own thread.

    Or just call run() directly to have it startup and block.
    '''
    def __init__(self, watcher=utils.ps.ActivityWatcher()):
        '''Create the server. 

        Inputs:
        
        watcher - Optional synchronization object that can be used to
        know when the server is active.
        
        '''
        super(Server, self).__init__()
        self.event_queue = Queue.Queue()
        self.s3handler = S3Handler(self.event_queue, watcher)
        self.io_loop = tornado.ioloop.IOLoop()
        self._is_running = threading.Event()
        self._watcher = watcher

    def run(self):
        self.s3handler.start()
        self.io_loop.make_current()

        application = tornado.web.Application([
            (r"/", LogLines, dict(q=self.event_queue, 
                                  watcher=self._watcher)),
            (r"/track",LogLines, dict(q=self.event_queue,
                                      watcher=self._watcher)),
            (r"/test",TestTracker),
            ])
        server = tornado.httpserver.HTTPServer(application,
                                               io_loop=self.io_loop)
        utils.ps.register_tornado_shutdown(server)
        server.listen(options.port)
        

        self._is_running.set()
        self.io_loop.start()
        server.stop()

    @tornado.gen.engine
    def wait_until_running(self):
        '''Blocks until the server/io_loop is running.'''
        self._is_running.wait()
        yield tornado.gen.Task(self.io_loop.add_callback)

    def wait_for_processing(self):
        '''Blocks until the current requests are all processed.'''
        self.event_queue.join()

    def stop(self):
        self.io_loop.stop()

def main(watcher=utils.ps.ActivityWatcher()):
    server = Server(watcher)
    server.run()
    

# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
