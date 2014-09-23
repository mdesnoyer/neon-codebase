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

import atexit

from boto.s3.connection import S3Connection
from boto.emr.connection import EmrConnection
import boto
from guppy import hpy
import datetime
import impala.dbapi
import impala.error
import json
import logging
from mastermind.core import VideoInfo, ThumbnailInfo, Mastermind
import signal
import socket
import stats.cluster
from supportServices import neondata
import tempfile
import time
import threading
import tornado.ioloop
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon

# Stats database options. It is an Impala database
define('stats_cluster_type', default='video_click_stats',
       help='cluster-type tag on the stats cluster to use')
define('stats_port', type=int, default=21050,
       help='Port to the stats database')
define('stats_db_polling_delay', default=247, type=float,
       help='Number of seconds between polls of the stats db')

# Video db options
define('video_db_polling_delay', default=261, type=float,
       help='Number of seconds between polls of the video db')

# Publishing options
define('s3_bucket', default='neon-image-serving-directives-test',
       help='Bucket to publish the serving directives to')
define('directive_filename', default='mastermind',
       help='Filename in the S3 bucket that will hold the directive.')
define('publishing_period', type=int, default=300,
       help='Time in seconds between when the directive file is published.')
define('expiry_buffer', type=int, default=30,
       help='Buffer in seconds for the expiry of the directives file')

# Monitoring variables
statemon.define('time_since_stats_update', float) # Time since the last update
statemon.define('time_since_publish', float) # Time since the last publish
statemon.define('statsdb_error', int)
statemon.define('videodb_error', int)
statemon.define('publish_error', int)
statemon.define('serving_urls_missing', int)

_log = logging.getLogger(__name__)

class VideoDBWatcher(threading.Thread):
    '''This thread polls the video database for changes.'''
    def __init__(self, mastermind, directive_pusher,
                 activity_watcher=utils.ps.ActivityWatcher()):
        super(VideoDBWatcher, self).__init__(name='VideoDBWatcher')
        self.mastermind = mastermind
        self.daemon = True
        self.activity_watcher = activity_watcher
        self.directive_pusher = directive_pusher

        # Is the initial data loaded
        self.is_loaded = threading.Event()

        self._stopped = threading.Event()

    def run(self):
        while not self._stopped.is_set():
            try:
                with self.activity_watcher.activate():
                    self._process_db_data()

            except Exception as e:
                _log.exception('Uncaught video DB Error: %s' % e)
                statemon.state.increment('videodb_error')

            # Now we wait so that we don't hit the database too much.
            self._stopped.wait(options.video_db_polling_delay)

    def wait_until_loaded(self):
        '''Blocks until the data is loaded.'''
        self.is_loaded.wait()

    def stop(self):
        '''Stop this thread safely and allow it to finish what is is doing.'''
        self._stopped.set()

    def _process_db_data(self):
        _log.info('Polling the video database')

        # Get an update for the serving urls
        self.directive_pusher.update_serving_urls(
            dict(((str(x.get_thumbnail_id()), x.size_map) for x in
                  neondata.ThumbnailServingURLs.get_all())))

        # Get an update for the tracker id map
        self.directive_pusher.update_tracker_id_map(
            dict(((str(x.get_tai()), str(x.value)) for x in
                  neondata.TrackerAccountIDMapper.get_all())))

        # Get an update for the default widths
        self.directive_pusher.update_default_sizes(
            dict(((str(x.neon_api_key), x.default_size) for x in
                  neondata.NeonUserAccount.get_all_accounts())))

        # Update the video data
        for platform in neondata.AbstractPlatform.get_all_instances():
            # Update the experimental strategy for the account
            self.mastermind.update_experiment_strategy(
                platform.neon_api_key,
                neondata.ExperimentStrategy.get(platform.neon_api_key))
            
            #video_ids = platform.get_internal_video_ids()
            video_ids = platform.get_processed_internal_video_ids()
            all_video_metadata = neondata.VideoMetadata.get_many(video_ids)
            for video_id, video_metadata in zip(video_ids, all_video_metadata):
                if video_metadata is None:
                    _log.error('Could not find information about video %s' %
                               video_id)
                    continue

                if (platform.serving_enabled and 
                    video_metadata.serving_enabled):

                    thumbnails = []
                    data_missing = False
                    thumbs = neondata.ThumbnailMetadata.get_many(
                        video_metadata.thumbnail_ids)
                    for thumb_id, meta in zip(video_metadata.thumbnail_ids,
                                              thumbs):
                        if meta is None:
                            _log.error('Could not find metadata for thumb %s' %
                                       thumb_id)
                            data_missing = True
                        else:
                            thumbnails.append(meta)

                    if data_missing:
                        continue

                    self.mastermind.update_video_info(video_metadata,
                                                      thumbnails,
                                                      platform.abtest)
                else:                
                    self.mastermind.remove_video_info(video_id)
        
        self.is_loaded.set()

class StatsDBWatcher(threading.Thread):
    '''This thread polls the stats database for changes.'''
    def __init__(self, mastermind,
                 activity_watcher=utils.ps.ActivityWatcher()):
        super(StatsDBWatcher, self).__init__(name='StatsDBWatcher')
        self.mastermind = mastermind
        self.video_id_cache = {} # Thumb_id -> video_id
        self.last_update = None
        self.daemon = True
        self.activity_watcher = activity_watcher

        # Is the initial data loaded
        self.is_loaded = threading.Event()

        self._stopped = threading.Event()

    def wait_until_loaded(self):
        '''Blocks until the data is loaded.'''
        self.is_loaded.wait()

    def stop(self):
        '''Stop this thread safely and allow it to finish what is is doing.'''
        self._stopped.set()

    def run(self):
        while not self._stopped.is_set():
            try:
                with self.activity_watcher.activate():
                    self._process_db_data()

            except Exception as e:
                _log.exception('Uncaught stats DB Error: %s' % e)
                statemon.state.increment('statsdb_error')

            # Now we wait so that we don't hit the database too much.
            self._stopped.wait(options.stats_db_polling_delay)

    def _process_db_data(self):
        stats_host = self._find_cluster_ip()
        _log.info('Statistics database is at host=%s port=%i' %
                  (stats_host, options.stats_port))
        try:
            conn = impala.dbapi.connect(host=stats_host,
                                        port=options.stats_port)
        except Exception as e:
            _log.exception('Error connecting to stats db: %s' % e)
            statemon.state.increment('statsdb_error')
            return

        cursor = conn.cursor()

        _log.info('Polling the stats database')
        
        # See if there are any new entries
        curtime = datetime.datetime.utcnow()
        cursor.execute(
            ('SELECT max(serverTime) FROM videoplays WHERE '
             'yr >= {yr:d} or (yr = {yr:d} and mnth >= {mnth:d})').format(
            mnth=curtime.month, yr=curtime.year))
        result = cursor.fetchall()
        if len(result) == 0 or result[0][0] is None:
            _log.error('Cannot determine when the database was last updated')
            statemon.state.increment('statsdb_error')
            self.is_loaded.set()
            return
        cur_update = datetime.datetime.utcfromtimestamp(result[0][0])
        statemon.state.time_since_stats_update = (
            datetime.datetime.now() - cur_update).total_seconds()
        if self.last_update is None or cur_update > self.last_update:
            _log.info('Found a newer entry in the stats database from %s. '
                      'Processing' % cur_update.isoformat())
            last_month = cur_update - datetime.timedelta(weeks=4)
            col_map = {
                neondata.MetricType.LOADS: 'imloadclienttime',
                neondata.MetricType.VIEWS: 'imvisclienttime',
                neondata.MetricType.CLICKS: 'imclickclienttime',
                neondata.MetricType.PLAYS: 'videoplayclienttime'
                }

            # We are going to walk through the db by tracker id
            # because it is partitioned that way and it makes the
            # calls faster
            for tai_info in neondata.TrackerAccountIDMapper.get_all():
                if (tai_info.itype != 
                    neondata.TrackerAccountIDMapper.PRODUCTION):
                    continue

                # Build the query for all the data in the last month
                strategy = neondata.ExperimentStrategy.get(tai_info.value)
                if strategy is None:
                    _log.error('Could not find experimental strategy for '
                               'account %s. Skipping' % tai_info.value)
                    continue
                if strategy.conversion_type == neondata.MetricType.PLAYS:
                    query = (
                        ("select thumbnail_id, count({imp_type}), "
                         "sum(cast(imclickclienttime is not null and " 
                         "(adplayclienttime is not null or "
                         "videoplayclienttime is not null) as int)) "
                         "from EventSequences where tai='{tai}' and "
                         "{imp_type} is not null "
                         "and (yr >= {yr:d} or "
                         "(yr = {yr:d} and mnth >= {mnth:d})) "
                         "group by thumbnail_id").format(
                            imp_type=col_map[strategy.impression_type],
                            tai=tai_info.get_tai(),
                            yr=last_month.year,
                            mnth=last_month.month))
                else:
                    query = (
                        ("select thumbnail_id, count({imp_type}), "
                         "count({conv_type}) "
                         "from EventSequences where tai='{tai}' and "
                         "{imp_type} is not null "
                         "and yr >= {yr:d} and mnth >= {mnth:d} "
                         "group by thumbnail_id").format(
                            imp_type=col_map[strategy.impression_type],
                            conv_type=col_map[strategy.conversion_type],
                            tai=tai_info.get_tai(),
                            yr=last_month.year,
                            mnth=last_month.month))
                cursor.execute(query)

                data = [(self._find_video_id(x[0]), x[0], x[1], x[2])
                        for x in cursor]
                    
                self.mastermind.update_stats_info(data)
                    
        self.last_update = cur_update
        self.is_loaded.set()

    def _find_cluster_ip(self):
        '''Finds the private ip of the stats cluster.'''
        _log.info('Looking for cluster of type: %s' % 
                  options.stats_cluster_type)
        cluster = stats.cluster.Cluster(options.stats_cluster_type)
        try:
            cluster.find_cluster()
        except stats.cluster.ClusterInfoError as e:
            _log.error('Could not find the cluster.')
            statemon.state.increment('statsdb_error')
            raise
        return cluster.master_ip

    def _find_video_id(self, thumb_id):
        '''Finds the video id for a thumbnail id.'''
        try:
            video_id = self.video_id_cache[thumb_id]
        except KeyError:
            video_id = neondata.ThumbnailMetadata.get_video_id(thumb_id)
            self.video_id_cache[thumb_id] = video_id
        return video_id

class DirectivePublisher(threading.Thread):
    '''Manages the publishing of the Masermind directive files.

    The files are published to S3.

    The files are a list of json entries, one per line. The entries
    can be of two types. The first is a mapping from the publisher id
    (aka TrackerAccountId) to the account id (aka api key). Those
    entries look like:

    {"type" : "pub", "pid":"publisher1_prod", "aid":"account1"}

    The second type of file is a serving directive for a video. The
    reference json is:
    {
    "type":"dir",
    "aid":"account1",
    "vid":"vid1",
    "sla":"2014-03-27T23:23:02Z",
    "fractions":
    [
      {
        "pct":0.8,
        "tid": "thumb1",
        "default_url" : "http://neon/thumb1_480_640.jpg", 
        "imgs":
        [
          {
            "h":480,
            "w":640,
            "url":"http://neon/thumb1_480_640.jpg"
          },
          {
            "h":600,
            "w":800,
            "url":"http://neon/thumb1_600_800.jpg"
          }
        ]
      },
      {
        "pct":0.2,
        "tid": "thumb2",
        "default_url" : "http://neon/thumb2_480_640.jpg",
        "imgs":
        [
          {
            "h":480,
            "w":640,
            "url":"http://neon/thumb2_480_640.jpg"
          },
          {
            "h":600,
            "w":800,
            "url":"http://neon/thumb2_600_800.jpg"
          }
        ]
      }
    ]
    }
    '''
    def __init__(self, mastermind, tracker_id_map={}, serving_urls={},
                 default_sizes={},
                 activity_watcher=utils.ps.ActivityWatcher()):
        '''Creates the publisher.

        Inputs:
        mastermind - The mastermind.core.Mastermind object that has the logic
        tracker_id_map - A map of tracker_id -> account_id
        serving_urls - A map of thumbnail_id -> { (width, height) -> url }
        default_widths - A map of account_id (aka api_key) -> 
                                             default thumbnail width
        '''
        super(DirectivePublisher, self).__init__(name='DirectivePublisher')
        self.mastermind = mastermind
        self.tracker_id_map = tracker_id_map
        self.serving_urls = serving_urls
        self.default_sizes = default_sizes
        self.activity_watcher = activity_watcher

        self.last_publish_time = datetime.datetime.utcnow()

        self.lock = threading.RLock()
        self._stopped = threading.Event()

        # For some reason, when testing on some machines, the patch
        # for the S3Connection doesn't work in a separate thread. So,
        # we grab the reference to the S3Connection on initialization
        # instead of relying on the import statement.
        self.S3Connection = S3Connection

    def run(self):
        self._stopped.clear()
        while not self._stopped.is_set():
            last_woke_up = datetime.datetime.now()

            try:
                with self.activity_watcher.activate():
                    self._publish_directives()
            except Exception as e:
                _log.exception('Uncaught exception when publishing %s' %
                               e)
                statemon.state.increment('publish_error')

            self._stopped.wait(options.publishing_period -
                               (datetime.datetime.now() - 
                                last_woke_up).total_seconds())

    def stop(self):
        '''Stop this thread safely and allow it to finish what is is doing.'''
        self._stopped.set()

    def update_tracker_id_map(self, new_map):
        with self.lock:
            self.tracker_id_map = new_map

    def update_serving_urls(self, new_map):
        with self.lock:
            self.serving_urls = new_map

    def update_default_sizes(self, new_map):
        with self.lock:
            self.default_sizes = new_map

    def _publish_directives(self):
        '''Publishes the directives to S3'''
        # Create the directives file
        curtime = datetime.datetime.utcnow()
        directive_file = tempfile.TemporaryFile()
        valid_length = options.expiry_buffer + options.publishing_period
        directive_file.write(
            'expiry=%sZ' % 
            (curtime + datetime.timedelta(seconds=valid_length))
            .isoformat())
        with self.lock:
            self._write_directives(directive_file)
        directive_file.write('\nend')

        filename = '%s.%s' % (curtime.strftime('%Y%m%d%H%M%S'),
                              options.directive_filename)
        _log.info('Publishing directive to s3://%s/%s' %
                  (options.s3_bucket, filename))

        # Create the connection to S3
        s3conn = self.S3Connection()
        try:
            bucket = s3conn.get_bucket(options.s3_bucket)
        except boto.exception.BotoServerError as e:
            _log.error('Could not get bucket %s: %s' % (options.s3_bucket,
                                                         e))
            statemon.state.increment('publish_error')
            return
        except boto.exception.BotoClientError as e:
            _log.error('Could not get bucket %s: %s' % (options.s3_bucket,
                                                         e))
            statemon.state.increment('publish_error')
            return
        except socket.error as e:
            _log.error('Error connecting to S3: %s' % e)
            statemon.state.increment('publish_error')
            return

        # Write the file that is timestamped
        key = bucket.new_key(filename)
        key.content_type = 'text/plain'
        directive_file.seek(0)
        key.set_contents_from_file(directive_file,
                                   encrypt_key=True)

        # Copy the file to the REST endpoint
        key.copy(bucket.name, options.directive_filename, encrypt_key=True,
                 preserve_acl=True)

        self.last_publish_time = curtime

        statemon.state.time_since_publish = (
            curtime - datetime.datetime(1970,1,1)).total_seconds()

    def _write_directives(self, stream):
        '''Write the current directives to the stream.'''
        # First write out the tracker id maps
        for tracker_id, account_id in self.tracker_id_map.iteritems():
            stream.write('\n' + json.dumps({'type': 'pub', 'pid': tracker_id,
                                            'aid': account_id}))

        # Now write the directives
        serving_urls_missing = 0
        for key, directive in self.mastermind.get_directives():
            account_id, video_id = key
            fractions = []
            missing_urls = False
            for thumb_id, frac in directive:
                try:
                    fractions.append({
                        'pct': frac,
                        'tid': thumb_id,
                        'default_url': self._get_default_url(account_id,
                                                             thumb_id),
                        'imgs': [ 
                            {
                                'w': k[0],
                                'h': k[1],
                                'url': v
                            } 
                            for k, v in 
                            self.serving_urls[thumb_id].iteritems()
                            ]
                    })
                except KeyError:
                    if frac > 1e-7:
                        _log.error('Could not find all serving URLs for '
                                   'video: %s. The directives will not '
                                   'be published.' % video_id)
                        missing_urls = True
                        serving_urls_missing += 1
                        break

            if missing_urls:
                continue
            
            data = {
                'type': 'dir',
                'aid': account_id,
                'vid': video_id,
                'sla': datetime.datetime.utcnow().isoformat() + 'Z',
                'fractions': fractions
            }
            stream.write('\n' + json.dumps(data))

        statemon.state.serving_urls_missing = serving_urls_missing

    def _get_default_url(self, account_id, thumb_id):
        '''Returns the default url for this thumbnail id.'''
        # Figure out the default width for this account
        default_size = self.default_sizes.get(account_id, None)
        if default_size is None:
            default_size = (160, 90)
        
        serving_urls = self.serving_urls[thumb_id]
        try:
            return serving_urls[tuple(default_size)]
        except KeyError:
            # We couldn't find that exact size, so pick the one with the minimum 
            mindiff = min([abs(x[0] - default_size[0]) +
                           abs(x[1] - default_size[1]) 
                           for x in serving_urls.keys()])
            closest_size = [x for x in serving_urls.keys() if
                            (abs(x[0] - default_size[0]) +
                             abs(x[1] - default_size[1])) == mindiff][0]
            _log.warn('There is no serving thumb of size (%i, %i) for thumb'
                      '%s. Using (%i, %i) instead'
                      % (default_size[0], default_size[1], thumb_id,
                         closest_size[0], closest_size[1]))
            return serving_urls[closest_size]
        
def main(activity_watcher = utils.ps.ActivityWatcher()):    
    with activity_watcher.activate():
        mastermind = Mastermind()
        publisher = DirectivePublisher(mastermind, 
                                       activity_watcher=activity_watcher)

        videoDbThread = VideoDBWatcher(mastermind, publisher,
                                       activity_watcher)
        videoDbThread.start()
        videoDbThread.wait_until_loaded()
        statsDbThread = StatsDBWatcher(mastermind, activity_watcher)
        statsDbThread.start()
        statsDbThread.wait_until_loaded()

        publisher.start()

    atexit.register(tornado.ioloop.IOLoop.current().stop)
    atexit.register(publisher.stop)
    atexit.register(videoDbThread.stop)
    atexit.register(statsDbThread.stop)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))

    def update_publish_time():
        statemon.state.last_publish_time = (
            datetime.datetime.utcnow() -
            publisher.last_publish_time).total_seconds()
    tornado.ioloop.PeriodicCallback(update_publish_time, 10000)
    tornado.ioloop.IOLoop.current().start()
    
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
