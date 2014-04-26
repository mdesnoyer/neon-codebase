#!/usr/bin/env python
''''
Server that logs data from the tracker and sends it to a local flume agent

Tornado server listens for http requests and sends them to flume. If
flume can't handle the load, the events are logged to disk and then
replayed when flume comes back.

'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)


import json
import os
import Queue
import re
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
from utils.s3 import S3Connection

#logging
import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("port", default=9080, help="run on the given port", type=int)
define("flume_port", default=6367, type=int,
       help='Port to talk to the flume agent running locally')
define("backup_disk", default="/mnt/neon/backlog", type=str,
        help="Location to store backup lines which failed to send to the flume agent")
define("backup_max_events_per_file", default=100000, type=int,
       help='Maximum events to allow backups on per file')
define("backup_flush_interval", default=100, type=int,
       help='Flush to disk after how many events?')

from utils import statemon
statemon.define('backup_qsize', int)
statemon.define('flume_errors', int)
statemon.define('messages_handled', int)
statemon.define('invalid_messages', int)

#############################################
#### DATA FORMAT ###
#############################################

class TrackerData(object):
    '''
    Schema for click tracker data
    '''
    def __init__(self, action, _id, ttype, cts, sts, page, cip, imgs, tai,
                 cvid=None, xy=None):
        #TODO: handle unicode data too 
        
        self.a = action # load/ click
        self.id = _id    # page load id
        self.ttype = ttype #tracker type
        self.ts = cts #client timestamp
        self.sts = sts #server timestamp
        self.cip = cip #client IP
        self.page = page # Page where the video is shown
        self.tai = tai # Tracker account id
        if isinstance(imgs, list):        
            self.imgs = imgs #image list
            self.cvid = cvid #current video in the player
        else:
            self.img = imgs  #clicked image
            if xy:
                self.xy = xy 
    
    def to_json(self):
        '''Converts the object to a json string.'''
        return json.dumps(self, default=lambda o: o.__dict__)

#############################################
#### WEB INTERFACE #####
#############################################

class TrackerDataHandler(tornado.web.RequestHandler):
    '''Common class to handle http requests to the tracker.'''
    
    def parse_tracker_data(self):
        '''Parses the tracker data from a GET request.

        returns:
        TrackerData object
        '''
        ttype = self.get_argument('ttype')
        action = self.get_argument('a')
        _id = self.get_argument('id')
        cts = self.get_argument('ts')
        sts = int(time.time())
        page = self.get_argument('page') #url decode
        tai = self.get_argument('tai') #tracker account id 
        cvid = None

        #On load the current video loaded in the player is logged
        if action == 'load':
            imgs = self.get_argument('imgs')
            imgs = [e.strip('"\' ') for e in imgs.strip('[]').split(',')]
            if ttype != 'imagetracker':
                cvid = self.get_argument('cvid')
        else:
            imgs = self.get_argument('img')

        xy = self.get_argument('xy', None) #click on image
        cip = self.request.remote_ip
        return TrackerData(action, _id, ttype, cts, sts, page, cip, imgs, tai,
                           cvid, xy)


class LogLines(TrackerDataHandler):
    '''Handler for real tracking data that should be logged.'''

    def initialize(self, q, watcher):
        '''Initialize the logger.'''
        self.watcher = watcher
        self.backup_q = q
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        '''Handle a tracking request.'''
        with self.watcher.activate():
            statemon.state.messages_handled.increment()
            
            try:
                tracker_data = self.parse_tracker_data()
            except tornado.web.HTTPError as e:
                _log.error('Invalid request: %s' % self.request.uri)
                statemon.state.invalid_messages.increment()
                raise
            except Exception, err:
                _log.exception("key=get_track msg=%s", err)
                self.set_status(500)
                self.finish()
                return

            data = tracker_data.to_json()
            try:
                request = tornado.httpclient.HTTPRequest(
                    'http://localhost:%i' % options.flume_port,
                    method='POST'
                    headers={'Content-Type': 'application/json'},
                    body=data)
                response = yield torndado.gen.Task(utils.http.send_request,
                                                   request)
                if response.error:
                    _log.error('Could not send message to flume')
                    statemon.state.flume_errors.increment()
                    self.backup_q.put(data)
                    self.set_status(response.error.code)
                else:
                    self.set_status(response.code)
            except Exception, err:
                _log.exception("key=loglines msg=Q error %s", err)
                self.set_status(500)
            self.finish()

    def memory_check(self):
        '''Method to check memory on the node'''
        return True

class TestTracker(TrackerDataHandler):
    '''Handler for test requests.'''
    
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        '''Handle a test tracking request.'''
        try:
            tracker_data = self.parse_tracker_data()
            cb = self.get_argument("callback")
        except Exception as err:
            _log.exception("key=test_track msg=%s", err) 
            self.finish()
            return
        
        data = tracker_data.to_json()
        self.set_header("Content-Type", "application/json")
        self.write(cb + "("+ data + ")") #wrap json data in callback
        self.finish()

###########################################
# File Backup Handler thread 
###########################################
class FileBackupHandler(threading.Thread):
    '''Thread that uploads data to S3.'''
    
    def __init__(self, dataQ, watcher=utils.ps.ActivityWatcher()):
        super(S3Handler, self).__init__()
        self.dataQ = dataQ
        self.daemon = True
        self.watcher = watcher
        self.backup_stream = None
        self.events_in_file = 0
        
        self._mutex = threading.RLock()
        self._upload_limiter = \
          threading.Semaphore(options.max_concurrent_uploads)

        statemon.state.qsize = self.dataQ.qsize()

        # Make sure the backup directory exists
        if not os.path.exists(options.backup_disk):
            os.makedirs(options.backup_disk)

    def __del__(self):
        if self.backup_stream is not None:
            self.backup_stream.close()

    def _generate_log_filename(self):
        '''Create a new log filename.'''
        return '%s_%s_clicklog.log' % (
            time.strftime('%S%M%H%d%m%Y', time.gmtime()),
            shortuuid.uuid())

    def _open_new_backup_file(self):
        '''Opens a new backup file and puts it on self.backup_stream.'''
        if not os.path.exists(options.backup_disk):
            os.makedirs(options.backup_disk)
            
        self.backup_stream = \
          open(os.path.join(options.backup_disk,
                            self._generate_log_filename()),
                            'a')

    def _prepare_backup_stream(self):
        '''Prepares the backup stream for writing to.

        This could mean flushing it to disk or closing this file and
        opening a new one.
        '''
        if self.backup_stream is None:
            self._open_new_backup_file()

        # See if the file should be flushed
        if self.events_in_file % options.backup_flush_interval == 0:
            self.backup_stream.flush()

        # Check to see if the file should be rolled over
        if self.events_in_file >= options.backup_max_events_per_file:
            self.backup_stream.close()
            self._open_new_backup_file()
            self.events_in_file = 0

    def run(self):
        '''Main runner for the handler.'''
        while True:
            try:
                try:
                    event = self.dataQ.get(True, 30)
                except Queue.Empty:
                    if self.backup_stream is not None:
                        self.backup_stream.flush()
                    continue

                statemon.state.qsize = self.dataQ.qsize()
                self._prepare_backup_stream()

                self.backup_stream.write('%s\n' % event)
                self.events_in_file += 1
            except Exception as err:
                _log.exception("key=file_backup_handler msg=%s", err)

class HealthCheckHandler(TrackerDataHandler):
    '''Handler for health check ''' 
    
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        '''Handle a test tracking request.'''

        self.write("<html> Server OK </html>")
        self.finish()

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
        self.backup_queue = Queue.Queue()
        self.backup_handler = FileBackupHandler(self.backup_queue, watcher)
        self.io_loop = tornado.ioloop.IOLoop()
        self._is_running = threading.Event()
        self._watcher = watcher

    def run(self):
        statemon.state.flume_errors = 0
        statemon.state.messages_handled = 0
        statemon.state.invalid_messages = 0
        
        with self._watcher.activate():
            self.backup_handler.start()
            self.io_loop.make_current()

            application = tornado.web.Application([
                (r"/", LogLines, dict(q=self.backup_queue,
                                      watcher=self._watcher)),
                (r"/track", LogLines, dict(q=self.backup_queue,
                                           watcher=self._watcher)),
                (r"/test", TestTracker),
                (r"/healthcheck", HealthCheckHandler),
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
        '''Stops the server'''
        self.io_loop.stop()

def main(watcher=utils.ps.ActivityWatcher()):
    '''Main function that runs the server.'''
    with watcher.activate():
        server = Server(watcher)
    server.run()
    

# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    #Turn off Access logs for tornado
    logging.getLogger('tornado.access').propagate = False
    main()
