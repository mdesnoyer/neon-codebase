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
import re
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
define("s3disk", default="/mnt/neon/s3diskbacklog", type=str,
        help="Location to store backup lines which failed to upload s3")
define("s3host", default=None, type=str,
       help='Host where the S3 server is. If None, uses the boto default.')
define("s3port", default=None, type=int,
       help='Optional port where the S3 server is.')
define("output", default='s3://neon-tracker-logs',
       help=('Location to store the output. Can be a local directory, '
             'or an S3 bucket of the form s3://<bucket_name>'))
define("flush_interval", default=120, type=float,
       help='Interval in seconds to force a flush to S3')
define("max_concurrent_uploads", default=100, type=int,
       help='Maximum number of concurrent uploads')

from utils import statemon
statemon.define('qsize', int)
statemon.define('buffer_size', int)
statemon.define('s3_connection_errors', int)

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
        self.dataQ = dataQ
        self.last_upload_time = time.time()
        self.daemon = True
        self.watcher = watcher

        self.s3conn = None
        self._mutex = threading.RLock()
        self._upload_limiter = \
          threading.Semaphore(options.max_concurrent_uploads)

        self.check_output_location()

    def check_output_location(self):
        # Make sure the s3 backup exists
        if not os.path.exists(options.s3disk):
            os.makedirs(options.s3disk)
        
        # Determine where the output will be and create the
        # directories if necessary
        s3pathRe = re.compile('s3://([0-9a-zA-Z_\-]+)')
        s3match = s3pathRe.match(options.output)
        self.use_s3 = False
        if s3match:
            self.bucket_name = s3match.groups()[0]
            self.use_s3 = True

            if self.s3conn is None:
                s3host = options.s3host
                if s3host is None:
                    s3host = S3Connection.DefaultHost
                self.s3conn = S3Connection(port=options.s3port, host=s3host)
                
            bucket = self.s3conn.lookup(self.bucket_name)
            if bucket is None:
                self.s3conn.create_bucket(self.bucket_name)
        else:
            if not os.path.exists(options.output):
                os.makedirs(options.output)
            if not os.path.isdir(options.output):
                raise IOError('The output directory: %s is not a directory' %
                              options.output)

    def generate_log_filename(self):
        return '%s_%s_clicklog.log' % (
            time.strftime('%S%M%H%d%m%Y', time.gmtime()),
            shortuuid.uuid())

    def send_to_output(self, data):
        '''Sends the data to the appropriate output location.'''
        filename = self.generate_log_filename()
        self.check_output_location()
        if self.use_s3:
            thread = threading.Thread(target=self.save_to_s3,
                                      args=('\n'.join(data), self.bucket_name,
                                            filename, len(data)))
        else:
            thread = threading.Thread(
                target=self.save_to_disk,
                args=('\n'.join(data),
                      os.path.join(options.output, filename),
                      len(data)))

        self._upload_limiter.acquire()
        thread.start()

    def save_to_s3(self, data, bucket_name, key_name, nlines):
        try:
            bucket = self.s3conn.get_bucket(bucket_name)
            key = bucket.new_key(key_name)
            key.set_contents_from_string(data)

            self.upload_temp_logs_to_s3(bucket_name)

            self._upload_limiter.release()
            
            for i in range(nlines):
                self.dataQ.task_done()
                
        except boto.exception.BotoServerError as e:
            _log.error("key=upload_to_s3 msg=S3 Error %s" % e)
            statemon.state.increment('s3_connection_errors')
            self.save_to_disk(data, os.path.join(options.s3disk, key_name),
                              nlines)
            return
        except IOError as e:
            _log.error("key=upload_to_s3 msg=S3 I/O Error %s" % e)
            statemon.state.increment('s3_connection_errors')
            self.save_to_disk(data, os.path.join(options.s3disk, key_name),
                              nlines)
            return

    def save_to_disk(self, data, path, nlines):
        try:
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))

            if self.use_s3:
                with self._mutex:
                    with open(path,'a') as f:
                        f.write(data)
            else:
                with open(path,'a') as f:
                    f.write(data)

            for i in range(nlines):
                self.dataQ.task_done()
        finally:
            self._upload_limiter.release()

    def upload_temp_logs_to_s3(self, bucket_name):
        '''Uploads temporary files to S3

        The temporary files were put there because the connection to
        S3 had a hiccup.
        
        '''
        with self._mutex:
            for filename in os.listdir(options.s3disk):
                full_path = os.path.join(options.s3disk, filename)
                if filename.endswith('clicklog.log'):
                    bucket = self.s3conn.get_bucket(bucket_name)
                    key = bucket.new_key(filename)
                    key.set_contents_from_filename(full_path)
                    os.remove(full_path)

    def run(self):
        data = []
        statemon.state.s3_connection_errors = 0
        while True:
            try:
                try:
                    data.append(self.dataQ.get(True, options.flush_interval))
                except Queue.Empty:
                    pass

                nlines = len(data)
                statemon.state.qsize = self.dataQ.qsize()
                statemon.state.buffer_size = nlines

                # If we have enough data or it's been too long, upload
                if (nlines >= options.batch_count or 
                    (self.last_upload_time + options.flush_interval <= 
                     time.time())):
                    if nlines > 0:
                        _log.info('Sending %i lines to output %s' % 
                                  (nlines, options.output))
                        with self.watcher.activate():
                            self.send_to_output(data)
                            data = []
                            self.last_upload_time = time.time()
            except Exception as e:
                _log.exception("key=s3_uploader msg=%s" % e)
            

            statemon.state.qsize = self.dataQ.qsize()
            statemon.state.buffer_size = nlines

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
