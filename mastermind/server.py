#!/usr/bin/env python
'''
The mastermind server

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from .core import DistributionType, VideoInfo, ThumbnailInfo, Mastermind
import datetime
from . import directive_pusher
import json
import logging
import mysql.connector as sqldb
from supportServices import neondata
import threading
import tornado.httpserver
import tornado.ioloop
from tornado.options import define, options
import tornado.web

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

# Video database options
define('video_db_host', default=None,
       help='Host where the video database resides')
define('video_db_port', default=None, type=int,
       help='Port where the video database resides')
define('video_db_polling_delay', default=120, type=float,
       help='Number of seconds between polls of the video db')

# Stats database options
# TODO(mdesnoyer): Remove the default username and password after testing
define('--stats_host',
       default='stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com',
       help='Host of the stats database')
define('--stats_port', type='int',default=3306,
       help='Port to the stats database')
define('--stats_user', default='mastermind',
       help='User for the stats database')
define('--stats_pass', default='pignar4iuf434',
       help='Password for the stats database')
define('--stats_db', default='stats_dev', help='Stats database to connect to')
define('--stats_table', default='hourly_events',
       help='Table in the stats database to write to')
define('stats_db_polling_delay', default=57, type=float,
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
    
    _log.info('Connecting to the video database at: %s:%i' %
              (options.video_db_host,options.video_db_port))
    conn = neondata.DBConnection(options.video_db_host,
                                 options.video_db_port)
    mastermind = Mastermind() 
    for platform in conn.get_all_external_platforms():
        for video_id in platform.videos.iterkeys():
            video_metadata = neondata.VideoMetadata.get(video_id, 
                                                        db_connection=conn)
            thumbnails = [core.ThumbnailInfo.from_db_data(
                neondata.ThumbnailIDMapper.get_id(thumb_id,
                                                  db_connection=conn
                                                  ).thumbnail_metadata) 
                for thumb_id in video_metadata.thumbnail_ids]

            mastermind.update_video_info(video_id, platform.abtest,
                                         thumbnails)
            ab_manager.register_video_distribution(
                video_id, DistributionType.fromString(platform.get_ovp()))

    return mastermind, ab_manager

class VideoDBWatcher(threading.Thread):
    '''This thread polls the video database for changes.'''
    def __init__(self, mastermind, ab_manager):
        super(VideoDBWatcher, self).__init__(name='VideoDBWatcher')
        self.mastermind = mastermind
        self.ab_manager = ab_manager

    def run(self):
        _log.info('Connecting to the video db at:%s:%i' %
                  (options.video_db_host, options.video_db_port))
        conn = neondata.DBConnection(options.video_db_host,
                                     options.video_db_port)
        while True:
            for platform in conn.get_all_external_platforms():
                for video_id in platform.videos.iterkeys():
                    video_metadata = neondata.VideoMetadata.get(
                        video_id, 
                        db_connection=conn)
                    thumbnails = [
                        core.ThumbnailInfo.from_db_data(
                            neondata.ThumbnailIDMapper.get_id(
                                thumb_id,db_connection=conn).thumbnail_metadata) 
                        for thumb_id in video_metadata.thumbnail_ids]

                    directive = mastermind.update_video_info(video_id,
                                                             platform.abtest,
                                                             thumbnails)
                    if directive:
                        ab_manager.send(directive)

            # Now we wait so that we don't hit the database too much.
            threading.Event().wait(options.video_db_polling_delay)

class StatsDBWatcher(threading.Thread):
    '''This thread polls the stats database for changes.'''
    def __init__(self, mastermind, ab_manager):
        super(StatsDBWatcher, self).__init__(name='StatsDBWatcher')
        self.mastermind = mastermind
        self.ab_manager = ab_manager

    def run(self):
        _log.info('Statistics database is at host=%s port=%i database=%s' %
                  (options.stats_host, options.stats_port, options.stats_db))
        last_update = None
        while True:
            # wait so that we don't hit the database too much.
            threading.Event().wait(options.stats_db_polling_delay)
            
            try:
                conn = sqldb.connect(
                    user=options.stats_user,
                    password=options.stats_pass,
                    host=options.stats_host,
                    port=options.stats_port,
                    database=options.stats_db)
            except sqldb.Error as e:
                _log.exception('Error connecting to stats db: %s' % e)
                continue

            cursor = conn.cursor()

            # See if there are any new entries
            cursor.execute('''SELECT UPDATE_TIME FROM 
                           information_schema.TABLES WHERE 
                           TABLE_SCHEMA = ? AND TABLE_NAME = ?''',
                           (options.stats_db, options.stats_table))
            result = cursor.fetchall()
            if len(result) == 0:
                _log.error('Cannot determine when the database was last updated')
                continue
            cur_update = result[0]
            if cur_update > last_update:
                _log.info('The database was updated at %s. Processing' 
                          % cur_update)

                # The database was updated, so process the new state.
                result = cursor.execute('''SELECT thumbnail_id, max(hour),
                                        sum(loads), sum(clicks) 
                                        FROM %s group by thumbnail_id''')
                data = ((x[0], x[0], x[2], x[3]) for x result)
                for directive in self.mastermind(
                        update_stats_info(cur_update, data)):
                    self.ab_manager.send(directive)
                    
            last_update = cur_update

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
    def initialize(self, mastermind, ab_manager):
        self.mastermind = mastermind,
        self.ab_manager = ab_manager
        
    def post(self, *args, **kwargs):
        for line in self.request.body:
            try:
                parsed_data = json.loads(line)
                directive = self.mastermind.incorporate_delta_stats(
                    *parsed_data['d'])
                if directive:
                    self.ab_manager.send(directive)
            except TypeError:
                _log.warn('Problem parsing: %s' % line)
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
    def initialize(self, mastermind, ab_manager):
        self.mastermind = mastermind,
        self.ab_manager = ab_manager

    def get(self, *args, **kwargs):
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

def main():
    tornado.options.parse_command_line()
   
    mastermind, ab_manager = initialize()

    videoDbThread = VideoDBWatcher()
    videoDbThread.start()
    statsDbThread = StatsDBWatcher()
    statsDbThread.start()

    _log.info('Starting server on port %i' % options.port)
    application = tornado.web.Application([
        (r'/delta', ApplyDelta,
         dict(mastermind=mastermind, ab_manager=ab_manager)),
        (r'/get_directives', GetDirectives,
         dict(mastermind=mastermind, ab_manager=ab_manager))])
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    
    tornado.ioloop.IOLoop.instance().start()
    
if __name__ == "__main__":
	main()
