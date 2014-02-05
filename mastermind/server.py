#!/usr/bin/env python
'''
The mastermind server

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from mastermind.core import DistributionType, VideoInfo, ThumbnailInfo, \
     Mastermind
from datetime import datetime
from mastermind import directive_pusher
import json
import logging
import MySQLdb as sqldb
import stats.db
from supportServices import neondata
import time
import threading
import tornado.httpserver
import tornado.ioloop
import tornado.web
import utils.neon
from utils.options import define, options
import utils.ps

# This server's options
define('port', default=8080, help='Port to listen on', type=int)

# A/B Controller options
define('max_controller_connections', default=100, type=int,
       help='Maximum number of open connections to push changes to the '
       'AB controllers.')
define('bc_controller_url', default=None,
       help='URL to send the directives to the brightcove ab controller')
define('youtube_controller_url', default=None,
       help='URL to send the directives to the youtube ab controller')

# Stats database options
# TODO(mdesnoyer): Remove the default username and password after testing
define('stats_host',
       default='stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com',
       help='Host of the stats database')
define('stats_port', type=int, default=3306,
       help='Port to the stats database')
define('stats_user', default='mastermind',
       help='User for the stats database')
define('stats_pass', default='pignar4iuf434',
       help='Password for the stats database')
define('stats_db', default='stats_dev', help='Stats database to connect to')
define('stats_db_polling_delay', default=57, type=float,
       help='Number of seconds between polls of the video db')

# Video db options
define('video_db_polling_delay', default=300, type=float,
       help='Number of seconds between polls of the video db')

_log = logging.getLogger(__name__)

def initialize():
    '''Intializes the data structures needed by the server.

    Does this by reading from the video database.

    Returns (mastermind, ab_manager)
    '''
    _log.info('Initializing the server.')

    ab_manager = directive_pusher.Manager(options.max_controller_connections)
    if options.bc_controller_url is not None:
        _log.info('Brightcove directives will be sent to: %s' %
                  options.bc_controller_url)
        ab_manager.register_destination(DistributionType.BRIGHTCOVE,
                                        options.bc_controller_url)
    if options.youtube_controller_url is not None:
        _log.info('YouTube directives will be sent to: %s' %
                  options.youtube_controller_url)
        ab_manager.register_destination(DistributionType.YOUTUBE,
                                        options.youtube_controller_url)
    
    mastermind = Mastermind()

    return mastermind, ab_manager

class VideoDBWatcher(threading.Thread):
    '''This thread polls the video database for changes.'''
    def __init__(self, mastermind, ab_manager,
                 activity_watcher=utils.ps.ActivityWatcher()):
        super(VideoDBWatcher, self).__init__(name='VideoDBWatcher')
        self.mastermind = mastermind
        self.ab_manager = ab_manager
        self.daemon = True
        self.activity_watcher = activity_watcher

        # Is the initial data loaded
        self.is_loaded = threading.Event()

    def run(self):
        while True:
            try:
                with self.activity_watcher.activate():
                    self._process_db_data()

            except Exception as e:
                _log.exception('Uncaught video DB Error: %s' % e)

            # Now we wait so that we don't hit the database too much.
            time.sleep(options.video_db_polling_delay)

    def wait_until_loaded(self):
        '''Blocks until the data is loaded.'''
        self.is_loaded.wait()

    def _process_db_data(self):
        for platform in neondata.AbstractPlatform.get_all_instances():
            for video_id in platform.get_internal_video_ids():
                video_metadata = neondata.VideoMetadata.get(
                                            video_id)
                if video_metadata is None:
                    _log.error('Could not find information about video %s' %
                               video_id)
                    continue

                thumbnails = []
                data_missing = False
                for thumb_id in video_metadata.thumbnail_ids:
                    meta = neondata.ThumbnailIDMapper.get_thumb_metadata(
                        thumb_id)
                    if meta is None:
                        _log.error('Could not find metadata for thumb %s' %
                                   thumb_id)
                        data_missing = True
                    else:
                        thumbnails.append(ThumbnailInfo.from_db_data(meta))

                if data_missing:
                    continue

                self.ab_manager.register_video_distribution(
                    video_id, DistributionType.fromString(platform.get_ovp()))

                directive = self.mastermind.update_video_info(video_id,
                                                              platform.abtest,
                                                              thumbnails)
                if directive:
                    self.ab_manager.send(directive)

        
        self.is_loaded.set()

class StatsDBWatcher(threading.Thread):
    '''This thread polls the stats database for changes.'''
    def __init__(self, mastermind, ab_manager,
                 activity_watcher=utils.ps.ActivityWatcher()):
        super(StatsDBWatcher, self).__init__(name='StatsDBWatcher')
        self.mastermind = mastermind
        self.ab_manager = ab_manager
        self.video_id_cache = {} # Thumb_id -> video_id
        self.last_update = None
        self.daemon = True
        self.activity_watcher = activity_watcher

        # Is the initial data loaded
        self.is_loaded = threading.Event()

    def wait_until_loaded(self):
        '''Blocks until the data is loaded.'''
        self.is_loaded.wait()

    def run(self):
        _log.info('Statistics database is at host=%s port=%i database=%s' %
                  (options.stats_host, options.stats_port, options.stats_db))
        while True:
            try:
                with self.activity_watcher.activate():
                    self._process_db_data()            

            except Exception as e:
                _log.exception('Uncaught stats DB Error: %s' % e)

            # Now we wait so that we don't hit the database too much.
            time.sleep(options.stats_db_polling_delay)

    def _process_db_data(self):
        try:
            conn = sqldb.connect(
                user=options.stats_user,
                passwd=options.stats_pass,
                host=options.stats_host,
                port=options.stats_port,
                db=options.stats_db)
        except sqldb.Error as e:
            _log.exception('Error connecting to stats db: %s' % e)
            return

        cursor = conn.cursor()
        
        # See if there are any new entries
        stats.db.execute(
            cursor,
            '''SELECT logtime FROM last_update WHERE tablename = %s''',
            (stats.db.get_hourly_events_table(),))
        result = cursor.fetchall()
        if len(result) == 0:
            _log.error('Cannot determine when the database was last updated')
            self.is_loaded.set()
            return
        cur_update = result[0][0]
        if isinstance(cur_update, basestring):
            cur_update = datetime.strptime(cur_update, '%Y-%m-%d %H:%M:%S')
        if self.last_update is None or cur_update > self.last_update:
            _log.info('The stats database was updated at %s. Processing' 
                      % cur_update)

            # The database was updated, so process the new state.
            cursor.execute('''SELECT thumbnail_id,
                           sum(loads), sum(clicks) 
                           FROM %s group by thumbnail_id''' %
                stats.db.get_hourly_events_table())
            result = cursor.fetchall()
            if result:
                data = ((self._find_video_id(x[0]), x[0], x[1], x[2]) 
                        for x in result)
                    
                directives = self.mastermind.update_stats_info(
                    (cur_update - datetime(1970,1, 1)).total_seconds(),
                    data)
                for directive in directives:
                    self.ab_manager.send(directive)
                    
        self.last_update = cur_update
        self.is_loaded.set()

    def _find_video_id(self, thumb_id):
        '''Finds the video id for a thumbnail id.'''
        try:
            video_id = self.video_id_cache[thumb_id]
        except KeyError:
            video_id = neondata.ThumbnailIDMapper.get_video_id(thumb_id)
            self.video_id_cache[thumb_id] = video_id
        return video_id

class ApplyDelta(tornado.web.RequestHandler):
    '''Handle the request to apply a delta of the statistics.

    Expects a POST request containing json lines. Each line is of the form:

    {"d": [t, vid, tid, dload, dclick]}
    
    t - UTC timestamp of seconds since epoch
    vid - The video id
    tid - The thumbnail id
    dload - The number of loads in this delta
    dclick - The number of clicks in this delta
    '''
    def initialize(self, mastermind, ab_manager,
                   activity_watcher=utils.ps.ActivityWatcher()):
        self.mastermind = mastermind
        self.ab_manager = ab_manager
        self.activity_watcher = activity_watcher
        
    def post(self):
        with self.activity_watcher.activate():
            for line in self.request.body:
                try:
                    parsed_data = json.loads(line)
                    try:
                        parsed_args = parsed_data['d']
                    except KeyError:
                        _log.warn('Invalid delta format: %s' % line)
                        continue
                    directive = self.mastermind.incorporate_delta_stats(
                        *parsed_args)
                    if directive:
                        self.ab_manager.send(directive)
                except TypeError:
                    _log.warn('Problem using delta from: %s' % line)
            self.finish()
    

class GetDirectives(tornado.web.RequestHandler):
    '''Handle a request to receive all the serving directive we know about.

    The directives are streamed one per line as a json group of the form:
    {'d': (video_id, [(thumb_id, fraction)])}

    Expects a get request with the following optional parameters:
    vid - If set, returns the directive for a single video id. 
          Otherwise, return them all.
    push - If true, push to the known controllers. Default: True
    '''
    def initialize(self, mastermind, ab_manager,
                   activity_watcher=utils.ps.ActivityWatcher()):
        self.mastermind = mastermind
        self.ab_manager = ab_manager
        self.activity_watcher = activity_watcher

    def get(self):
        with self.activity_watcher.activate():
            video_id = self.get_argument('vid', None)
            push = self.get_argument('push', True)

            if video_id is not None:
                video_id = [video_id]

            firstLine = True
            for directive in self.mastermind.get_directives(video_id):
                if push:
                    self.ab_manager.send(directive)
                if firstLine:
                    firstLine = False
                else:
                    self.write('\n')
                self.write(json.dumps({'d': directive}))
                self.flush()
            self.finish()

def main(activity_watcher = utils.ps.ActivityWatcher()):
    with activity_watcher.activate():
        mastermind, ab_manager = initialize()

        videoDbThread = VideoDBWatcher(mastermind, ab_manager,
                                       activity_watcher)
        videoDbThread.start()
        videoDbThread.wait_until_loaded()
        statsDbThread = StatsDBWatcher(mastermind, ab_manager,
                                       activity_watcher)
        statsDbThread.start()
        statsDbThread.wait_until_loaded()

    _log.info('Starting server on port %i' % options.port)
    application = tornado.web.Application([
        (r'/delta', ApplyDelta,
         dict(mastermind=mastermind, ab_manager=ab_manager,
              activity_watcher=activity_watcher)),
        (r'/get_directives', GetDirectives,
         dict(mastermind=mastermind, ab_manager=ab_manager,
              activity_watcher=activity_watcher))])
    server = tornado.httpserver.HTTPServer(application)
    utils.ps.register_tornado_shutdown(server)
    server.listen(options.port)
    
    tornado.ioloop.IOLoop.instance().start()
    
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
