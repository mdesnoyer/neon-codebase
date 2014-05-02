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
import utils.http
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
statemon.define('qsize', int)
statemon.define('flume_errors', int)
statemon.define('messages_handled', int)
statemon.define('invalid_messages', int)
statemon.define('internal_server_error', int)

# TODO(mdesnoyer): Remove version 1 code once it is phased out

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

    def to_flume_event(self):
        '''Coverts the data to a flume event.'''
        return json.dumps([
            {'headers': {
                'timestamp' : self.sts,
                'tai' : self.tai,
                'track_vers' : '1',
                },
            'body': self.__dict__
            }])

class BaseTrackerDataV2(object):
    '''
    Schema for V2 tracking data
    '''
    def __init__(self, request):
        self.pageid = request.get_argument('pageid') # page_id
        self.tai = request.get_argument('tai') # tracker_account_id
        # tracker_type (brightcove, ooyala, bcgallery, ign as of April 2014)
        self.ttype = request.get_argument('ttype')
        #TODO(Sunil): Mock this correctly and use default value
        self.page = request.get_argument('page') # page_url
        try:
            self.ref = request.get_argument('ref') # referral_url
        except:
            self.ref = None

        self.sts = int(time.time()) # Server time stamp
        self.cts = int(request.get_argument('cts')) # client_time
        self.cip = request.request.remote_ip # client_ip
        # Neon's user id
        self.uid = request.get_cookie('neonglobaluserid', default=None) 

    def to_flume_event(self):
        '''Coverts the data to a flume event.'''
        return json.dumps([
            {'headers': {
                'timestamp' : self.sts,
                'tai' : self.tai,
                'track_vers' : '2',
                },
            'body': self.__dict__
            }])

    @staticmethod
    def generate(request_handler):
        '''A Factory generator to make the event.

        Inputs:
        request_handler - The http request handler
        '''
        event_map = {
            'iv' : ImagesVisible,
            'il' : ImagesLoaded,
            'ic' : ImageClicked,
            'vp' : VideoPlay,
            'vc' : VideoClick,
            'ap' : AdPlay}

        action = request_handler.get_argument('a')
        try:
            return event_map[action](request_handler)
        except KeyError as e:
            _log.error('Invalid event: %s' % action)
            raise tornado.web.HTTPError(400)

class ImagesVisible(BaseTrackerDataV2):
    '''An event specifying that the image became visible.'''
    def __init__(self, request):
        super(ImagesVisible, self).__init__(request)
        self.event = 'iv'
        self.tids = [] #[Thumbnail_id1, Thumbnail_id2]
        self.tids = request.get_argument('tids').split(',')
        #for tup in request.get_argument('tids').split(','):
        #    elems = tup.split(' ')
        #    self.tids.append(elems[0])

class ImagesLoaded(BaseTrackerDataV2):
    '''An event specifying that the image were loaded.'''
    def __init__(self, request):
        super(ImagesLoaded, self).__init__(request)
        self.event = 'il'
        self.tids = [] # [(Thumbnail id, width, height)]
        for tup in request.get_argument('tids').split(','):
            elems = tup.split(' ')
            self.tids.append((elems[0], int(elems[1]), int(elems[2])))

class ImageClicked(BaseTrackerDataV2):
    '''An event specifying that the image was clicked.'''
    def __init__(self, request):
        super(ImageClicked, self).__init__(request)
        self.event = 'ic'
        self.tid = request.get_argument('tid') # Thumbnail id
        self.px = int(request.get_argument('x')) # Page X coordinate
        self.py = int(request.get_argument('y')) # Page Y coordinate
        self.wx = int(request.get_argument('wx')) # Window X
        self.wy = int(request.get_argument('wy')) # Window Y

class VideoClick(BaseTrackerDataV2):
    '''An event specifying that the image was clicked within the player'''
    def __init__(self, request):
        super(ImageClicked, self).__init__(request)
        self.event = 'vc'
        self.tid = request.get_argument('tid') # Thumbnail id
        self.adplay = request.get_argument('adplay') # ts when ad started to play
        self.mplay = request.get_argument('mplay') # ts when play buttion was pressed

class VideoPlay(BaseTrackerDataV2):
    '''An event specifying that the image were loaded.'''
    def __init__(self, request):
        super(VideoPlay, self).__init__(request)
        self.event = 'vp'
        self.tid = request.get_argument('tid') # Thumbnail id
        self.vid = request.get_argument('vid') # Video id
        self.playerid = request.get_argument('playerid') # Player id

class AdPlay(BaseTrackerDataV2):
    '''An event specifying that the image were loaded.'''
    def __init__(self, request):
        super(AdPlay, self).__init__(request)
        self.event = 'ap'
        # TODO(sunil): Define the rest of this message

#############################################
#### WEB INTERFACE #####
#############################################

class TrackerDataHandler(tornado.web.RequestHandler):
    '''Common class to handle http requests to the tracker.'''
    
    def parse_tracker_data(self, version):
        '''Parses the tracker data from a GET request.

        returns:
        TrackerData object
        '''
        if version == 1:
            return self._parse_v1_tracker_data()
        elif version == 2:
            return BaseTrackerDataV2.generate(self)
        else:
            _log.fatal('Invalid api version %s' % version)
            raise ValueError('Bad version %s' % version)
        

    def _parse_v1_tracker_data(self):
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

    def initialize(self, q, watcher, version):
        '''Initialize the logger.'''
        self.watcher = watcher
        self.backup_q = q
        self.version = version
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        '''Handle a tracking request.'''
        with self.watcher.activate():
            statemon.state.increment('messages_handled')
            
            try:
                tracker_data = self.parse_tracker_data(self.version)
            except tornado.web.HTTPError as e:
                _log.error('Invalid request: %s' % self.request.uri)
                statemon.state.increment('invalid_messages')
                self.set_status(400)
                raise
            except Exception, err:
                _log.exception("key=get_track msg=%s", err)
                statemon.state.increment('internal_server_error')
                self.set_status(500)
                self.finish()
                return

            data = tracker_data.to_flume_event()
            try:
                request = tornado.httpclient.HTTPRequest(
                    'http://localhost:%i' % options.flume_port,
                    method='POST',
                    headers={'Content-Type': 'application/json'},
                    body=data)
                response = yield tornado.gen.Task(utils.http.send_request,
                                                  request)
                if response.error:
                    _log.error('Could not send message to flume')
                    statemon.state.increment('flume_errors')
                    self.backup_q.put(data)
                    #Don't throw exception to client if it fails to write to flume
                    #self.set_status(response.error.code)
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

    def initialize(self, version):
        '''Initialize the logger.'''
        self.version = version
    
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        '''Handle a test tracking request.'''
        try:
            tracker_data = self.parse_tracker_data(self.version)
            cb = self.get_argument("callback")
        except Exception as err:
            _log.exception("key=test_track msg=%s", err) 
            self.finish()
            return
        
        data = tracker_data.to_flume_event()
        self.set_header("Content-Type", "application/json")
        self.write(cb + "("+ data + ")") #wrap json data in callback
        self.finish()

###########################################
# File Backup Handler thread 
###########################################
class FileBackupHandler(threading.Thread):
    '''Thread that uploads data to S3.'''
    
    def __init__(self, dataQ, watcher=utils.ps.ActivityWatcher()):
        super(FileBackupHandler, self).__init__()
        self.dataQ = dataQ
        self.daemon = True
        self.watcher = watcher
        self.backup_stream = None
        self.events_in_file = 0

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

                with self.watcher.activate():
                    statemon.state.qsize = self.dataQ.qsize()
                    self._prepare_backup_stream()

                    self.backup_stream.write('%s\n' % event)
                    self.events_in_file += 1
            except Exception as err:
                _log.exception("key=file_backup_handler msg=%s", err)

            self.dataQ.task_done()

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

        self.application = tornado.web.Application([
            (r"/", LogLines, dict(q=self.backup_queue,
                                  watcher=self._watcher,
                                  version=1)),
            (r"/v2", LogLines, dict(q=self.backup_queue,
                                    watcher=self._watcher,
                                    version=2)),
            (r"/track", LogLines, dict(q=self.backup_queue,
                                       watcher=self._watcher,
                                       version=1)),
            (r"/v2/track", LogLines, dict(q=self.backup_queue,
                                          watcher=self._watcher,
                                          version=2)),
            (r"/test", TestTracker, dict(version=1)),
            (r"/v2/test", TestTracker, dict(version=2)),
            (r"/healthcheck", HealthCheckHandler),
            ])

    def run(self):
        statemon.state.flume_errors = 0
        statemon.state.messages_handled = 0
        statemon.state.invalid_messages = 0
        
        with self._watcher.activate():
            self.backup_handler.start()
            self.io_loop.make_current()
            
            server = tornado.httpserver.HTTPServer(self.application,
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
