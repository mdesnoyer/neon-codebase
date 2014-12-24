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
import cPickle as pickle
import datetime
import happybase
import impala.dbapi
import impala.error
import json
import logging
from mastermind.core import VideoInfo, ThumbnailInfo, Mastermind
import signal
import socket
import stats.cluster
import struct
from supportServices import neondata
import tempfile
import time
import thrift
import thrift.Thrift
import threading
import tornado.ioloop
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon
import zlib

# Stats database options. It is an Impala database
define('stats_cluster_type', default='video_click_stats',
       help='cluster-type tag on the stats cluster to use')
define('stats_port', type=int, default=21050,
       help='Port to the stats database')
define('stats_db_polling_delay', default=247, type=float,
       help='Number of seconds between polls of the stats db')

# Incremental stats database options. It is hbase
define('incr_stats_host', default='localhost',
       help='Host to connect to the hbase server with incremental stats updates')
define('incr_stats_col_family', default='evts',
       help='Column family to grab in the incremental stats db')

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
statemon.define('incr_statsdb_error', int)
statemon.define('videodb_error', int)
statemon.define('publish_error', int)
statemon.define('serving_urls_missing', int)

_log = logging.getLogger(__name__)

def pack_obj(x):
    '''Package an object so that it is smaller in memory'''
    return zlib.compress(pickle.dumps(x))

def unpack_obj(x):
    '''Unpack an object that was compressed by pack_obj'''
    return pickle.loads(zlib.decompress(x))

class VideoIdCache(object):
    '''Cache to figure out the video id from a thumbnail id.'''
    def __init__(self):
        self.cache = {} # thumb_id -> video_id
        self._lock = threading.RLock()

    def find_video_id(self, thumb_id):
        '''Returns the internal video id for a given thumbnail id.'''
        # First try to extract the video id because thumbnail ids are
        # usually in the form <api_key>_<external_video_id>_<thumb_id>
        split = thumb_id.split('_')
        if len(split) == 3:
            return '_'.join(split[0:2])
        else:
            _log.warn('Unexpected thumbnail id format %s. '
                      'Looking it up in the database' % thumb_id)
            try:
                with self._lock:
                    video_id = self.cache[thumb_id]
            except KeyError:
                video_id = neondata.ThumbnailMetadata.get_video_id(thumb_id)
                if video_id is None:
                    _log.error('Could not find video id for thumb %s' %
                               thumb_id)
                else:
                    with self._lock:
                        self.cache[thumb_id] = video_id
            return video_id

class ExperimentStrategyCache(object):
    '''Cache to map from an account_id to an experiment strategy.'''
    def __init__(self):
        self.cache = {} # account_id -> experiment_strategy
        self._lock = threading.RLock()

    def get(self, account_id):
        '''Return the experiment strategy for a given account.'''
        try:
            with self._lock:
                strategy = self.cache[account_id]
        except KeyError:
            strategy = neondata.ExperimentStrategy.get(account_id)
            with self._lock:
                self.cache[account_id] = strategy
        return strategy

    def from_thumb_id(self, thumb_id):
        '''Return the experiment strategy for a given thumb.'''
        thumb = neondata.ThumbnailMetadata(thumb_id)
        return self.get(thumb.get_account_id())

class VideoDBWatcher(threading.Thread):
    '''This thread polls the video database for changes.'''
    def __init__(self, mastermind, directive_pusher,
                 video_id_cache=VideoIdCache(),
                 activity_watcher=utils.ps.ActivityWatcher()):
        super(VideoDBWatcher, self).__init__(name='VideoDBWatcher')
        self.mastermind = mastermind
        self.daemon = True
        self.activity_watcher = activity_watcher
        self.directive_pusher = directive_pusher
        self.video_id_cache = video_id_cache

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

        # Get an update for the serving urls
        self.directive_pusher.update_serving_urls(
            { thumb_id: size_map for thumb_id, size_map in
              ((x.get_thumbnail_id(), x.size_map) for x in
              neondata.ThumbnailServingURLs.get_all()) if
              self.mastermind.is_serving_video(
                  self.video_id_cache.find_video_id(thumb_id))})
        
        self.is_loaded.set()

def hourtimestamp(dt):
    'Converts a datetime to a timestamp and rounds down to the nearest hour'
    return (dt.replace(minute=0, second=0, microsecond=0) - 
            datetime.datetime(1970, 1, 1)).total_seconds()

class StatsDBWatcher(threading.Thread):
    '''This thread polls the stats database for changes.'''
    def __init__(self, mastermind, video_id_cache=VideoIdCache(),
                 activity_watcher=utils.ps.ActivityWatcher()):
        super(StatsDBWatcher, self).__init__(name='StatsDBWatcher')
        self.mastermind = mastermind
        self.video_id_cache = video_id_cache
        self.last_update = None
        self.impala_conn = None
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

    def _connect_to_stats_db(self):
        '''Connects to the stats impala database.'''
        try:
            stats_host = self._find_cluster_ip()
            _log.info('Statistics database is at host=%s port=%i' %
                  (stats_host, options.stats_port))
            self.impala_conn = impala.dbapi.connect(host=stats_host,
                                                    port=options.stats_port)
            return self.impala_conn
        except Exception as e:
            _log.exception('Error connecting to stats db: %s' % e)
            statemon.state.increment('statsdb_error')
            raise

    def _process_db_data(self):
        strategy_cache = ExperimentStrategyCache()

        _log.info('Polling the stats database')
        if self._is_newer_batch_data():
            _log.info('Found a newer entry in the stats database from %s. '
                      'Processing' % self.last_update.isoformat())
            cursor = self.impala_conn.cursor()
            last_month = self.last_update - datetime.timedelta(weeks=4)
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
                strategy = strategy_cache.get(tai_info.value)
                if strategy.conversion_type == neondata.MetricType.PLAYS:
                    query = (
                        ("select thumbnail_id, count({imp_type}), "
                         "sum(cast(imclickclienttime is not null and " 
                         "(adplayclienttime is not null or "
                         "videoplayclienttime is not null) as int)) "
                         "from EventSequences where tai='{tai}' and "
                         "{imp_type} is not null "
                         "and servertime < {update_hour:f} "
                         "and (yr >= {yr:d} or "
                         "(yr = {yr:d} and mnth >= {mnth:d})) "
                         "group by thumbnail_id").format(
                            imp_type=col_map[strategy.impression_type],
                            tai=tai_info.get_tai(),
                            update_hour=hourtimestamp(self.last_update),
                            yr=last_month.year,
                            mnth=last_month.month))
                else:
                    query = (
                        ("select thumbnail_id, count({imp_type}), "
                         "count({conv_type}) "
                         "from EventSequences where tai='{tai}' and "
                         "{imp_type} is not null "
                         "and servertime < {update_hour:f} "
                         "and yr >= {yr:d} and mnth >= {mnth:d} "
                         "group by thumbnail_id").format(
                            imp_type=col_map[strategy.impression_type],
                            conv_type=col_map[strategy.conversion_type],
                            tai=tai_info.get_tai(),
                            update_hour=hourtimestamp(self.last_update),
                            yr=last_month.year,
                            mnth=last_month.month))
                cursor.execute(query)

                data = []
                for thumb_id, base_imp, base_conv in cursor:
                    incr_counts = self._get_incremental_stat_data(
                        strategy_cache,
                        thumb_id=thumb_id)[thumb_id]
                    data.append((self.video_id_cache.find_video_id(thumb_id),
                                 thumb_id,
                                 base_imp,
                                 incr_counts[0],
                                 base_conv,
                                 incr_counts[1]))
                    
                self.mastermind.update_stats_info(data)
        else:
            self.mastermind.update_stats_info([
                (self.video_id_cache.find_video_id(thumb_id),
                 thumb_id,
                 None, # base impression
                 counts[0], # incr impressions
                 None, # base conversions
                 counts[1]) # incr conversions
                 for thumb_id, counts in 
                 self._get_incremental_stat_data(strategy_cache)
                 .iteritems()])
                    
        self.is_loaded.set()

    def _is_newer_batch_data(self):
        '''Returns true if there is newer batch data.

        Also resets the self.last_update parameter if the impala database 
        could be reached.
        '''
        try:
            self._connect_to_stats_db()
        except Exception as e:
            return False
        
        cursor = self.impala_conn.cursor()
        
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
            return False
            
        cur_update = datetime.datetime.utcfromtimestamp(result[0][0])
        statemon.state.time_since_stats_update = (
            datetime.datetime.now() - cur_update).total_seconds()

        is_newer = self.last_update is None or cur_update > self.last_update
        self.last_update = cur_update
        return is_newer
        
 
    def _get_incremental_stat_data(self, strategy_cache, thumb_id=None):
        '''Looks up the incremental stats data from the database.

        Inputs:
        thumb_id - If set, only those counts for this thumb will be returned
        strategy_cache - Cache for retrieving the ExperimentStrategy objects.

        Returns: dictionary of thumbnail_id =>  [incr_imp, incr_conv]
        '''
        retval = {} 
        if thumb_id is not None:
            retval[thumb_id] = [0, 0]
        try:
            conn = happybase.Connection(options.incr_stats_host)
            col_family = options.incr_stats_col_family
            col_map = {
                neondata.MetricType.LOADS : '%s:il' % col_family,
                neondata.MetricType.VIEWS : '%s:iv' % col_family,
                neondata.MetricType.CLICKS : '%s:ic' % col_family,
                neondata.MetricType.PLAYS : '%s:vp' % col_family}
                
            row_start = None
            row_end = None
            table = None
            if thumb_id is None:
                # We want all the data from a given time onwards for all
                # thumbnails
                table = conn.table('TIMESTAMP_THUMBNAIL_EVENT_COUNTS')
                if self.last_update is not None:
                    # There is a time to start at
                    row_start = self.last_update.strftime('%Y-%m-%dT%H')
            else:
                # We only want data from a specific thumb
                table = conn.table('THUMBNAIL_TIMESTAMP_EVENT_COUNTS')
                row_end = thumb_id + 'a'
                if self.last_update is None:
                    row_start = thumb_id
                else:
                    row_start = '_'.join([
                        thumb_id, self.last_update.strftime('%Y-%m-%dT%H')])
            
            for key, row in table.scan(row_start=row_start,
                                       row_end=row_end,
                                       columns=[col_family]):
                if thumb_id is None:
                    tid = key.partition('_')[2]
                else:
                    tid = thumb_id
                if tid == '':
                    _log.warn_n('Invalid thumbnail id in key %s' % key, 100)
                    continue

                strategy = strategy_cache.from_thumb_id(tid)

                counts = retval.get(tid, [0, 0])

                try:
                    impr_col = col_map[strategy.impression_type]
                    conv_col = col_map[strategy.conversion_type]
                except KeyError as e:
                    _log.error_n('Unexpected event type in the experiment '
                                 'strategy for account %s: %s' % 
                                 (strategy.get_id(), e), 100)
                    continue

                try:
                    
                    incr_imp = struct.unpack('>q',
                                             row.get(impr_col, '\x00'*8))[0]
                    incr_conv = struct.unpack('>q',
                                              row.get(conv_col,'\x00'*8))[0]
                    counts[0] += incr_imp
                    counts[1] += incr_conv
                except struct.error as e:
                    _log.warn_n('Invalid value found for key %s: %s' %
                                (key, e), 100)
                    continue

                retval[tid] = counts
        except thrift.Thrift.TException as e:
            _log.error('Error connecting to incremental stats database: %s'
                       % e)
            statemon.state.increment('incr_statsdb_error')

        return retval            
            

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
        # Keep track of videos for which the directive has been published
        # This map is used to keep track of video_ids for which serving urls
        # state has been updated.
        self.video_id_serving_map = {} # video_id => serving_state
        self.serving_urls = serving_urls
        self.default_sizes = default_sizes
        self.activity_watcher = activity_watcher

        self.last_publish_time = datetime.datetime.utcnow()
        self._update_publish_timer = None
        self._update_time_since_publish()

        self.lock = threading.RLock()
        self._stopped = threading.Event()

        # For some reason, when testing on some machines, the patch
        # for the S3Connection doesn't work in a separate thread. So,
        # we grab the reference to the S3Connection on initialization
        # instead of relying on the import statement.
        self.S3Connection = S3Connection

    def __del__(self):
        if self._update_publish_timer and self._update_publish_timer.is_alive():
            self._update_publish_timer.cancel()
        super(DirectivePublisher, self).__del__()

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
            del self.serving_urls
            self.serving_urls = {}
            for k, v in new_map.iteritems():
                self.serving_urls[k] = pack_obj(v)

    def update_default_sizes(self, new_map):
        with self.lock:
            self.default_sizes = new_map

    def _update_time_since_publish(self):
        statemon.state.time_since_publish = (
            datetime.datetime.utcnow() -
            self.last_publish_time).total_seconds()

        self._update_publish_timer = threading.Timer(
            10.0, self._update_time_since_publish)
        self._update_publish_timer.daemon = True
        self._update_publish_timer.start()
    
    def _add_video_id_to_serving_map(self, vid):
        try:
            self.video_id_serving_map[vid]
        except KeyError, e:
            # new video is inserted, by default its serving state
            # should be false. i.e request state not updated
            self.video_id_serving_map[vid] = False
        
    def _update_request_state_to_serving(self):
        ''' update all the new video's serving state 
            i.e ISP URLs are ready
        '''
        vids = []
        for key, value in self.video_id_serving_map.iteritems():
            if value == False:
                vids.append(key)
        requests = neondata.VideoMetadata.get_video_requests(vids)
        for vid, request in zip(vids, requests):
            # TODO(Sunil) : Bulk update
            if request:
                if request.state in [neondata.RequestState.ACTIVE, 
                        neondata.RequestState.SERVING_AND_ACTIVE]:
                    request.state = neondata.RequestState.SERVING_AND_ACTIVE
                else:
                    request.state = neondata.RequestState.SERVING
                if request.save():
                    self.video_id_serving_map[vid] = True

    def _publish_directives(self):
        '''Publishes the directives to S3'''
        # Create the directives file
        _log.info("Building directives file")
        curtime = datetime.datetime.utcnow()
        directive_file = tempfile.TemporaryFile()
        valid_length = options.expiry_buffer + options.publishing_period
        directive_file.write(
            'expiry=%s' % 
            (curtime + datetime.timedelta(seconds=valid_length))
            .strftime('%Y-%m-%dT%H:%M:%SZ'))
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

        # The serving directives for videos are active, update the request state
        # for videos
        self._update_request_state_to_serving()

    def _write_directives(self, stream):
        '''Write the current directives to the stream.'''
        # First write out the tracker id maps
        _log.info("Writing tracker id maps")
        for tracker_id, account_id in self.tracker_id_map.iteritems():
            stream.write('\n' + json.dumps({'type': 'pub', 'pid': tracker_id,
                                            'aid': account_id}))

        # Now write the directives
        _log.info("Writing directives")
        serving_urls_missing = 0
        for key, directive in self.mastermind.get_directives():
            account_id, video_id = key
            # keep track of video_ids in directive file
            self._add_video_id_to_serving_map(video_id)
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
                            unpack_obj(self.serving_urls[thumb_id]).iteritems()
                            ]
                    })
                except KeyError:
                    if frac > 1e-7:
                        _log.error_n('Could not find all serving URLs for '
                                     'video: %s . The directives will not '
                                     'be published.' % video_id, 5)
                        missing_urls = True
                        serving_urls_missing += 1
                        break

            if missing_urls:
                continue
            
            data = {
                'type': 'dir',
                'aid': account_id,
                'vid': video_id,
                'sla': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
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
        
        serving_urls = unpack_obj(self.serving_urls[thumb_id])
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
        video_id_cache = VideoIdCache()
        publisher = DirectivePublisher(mastermind, 
                                       activity_watcher=activity_watcher)

        videoDbThread = VideoDBWatcher(mastermind, publisher, video_id_cache,
                                       activity_watcher)
        videoDbThread.start()
        videoDbThread.wait_until_loaded()
        statsDbThread = StatsDBWatcher(mastermind, video_id_cache,
                                       activity_watcher)
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
