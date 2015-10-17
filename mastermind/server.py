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
from contextlib import closing
from cmsdb import neondata
import cPickle as pickle
import datetime
import dateutil.parser
import gzip
import happybase
import impala.dbapi
import impala.error
import simplejson as json
import logging
from mastermind.core import VideoInfo, ThumbnailInfo, Mastermind
import multiprocessing
import os
import signal
import socket
import stats.cluster
import struct
from StringIO import StringIO
import tempfile
import time
import thrift
import thrift.Thrift
import threading
import tornado.ioloop
import utils.neon
import utils
from utils.options import define, options
import utils.ps
import utils.sqsmanager
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
define('video_db_polling_delay', default=1967, type=float,
       help='Number of seconds between batch polls of the video db')

# Publishing options
define('s3_bucket', default='neon-image-serving-directives-test',
       help='Bucket to publish the serving directives to')
define('directive_filename', default='mastermind',
       help='Filename in the S3 bucket that will hold the directive.')
define('publishing_period', type=int, default=300,
       help='Time in seconds between when the directive file is published.')
define('expiry_buffer', type=int, default=30,
       help='Buffer in seconds for the expiry of the directives file')
define('serving_update_delay', type=int, default=240,
       help='delay in seconds to update new videos to serving state')

# Callback options
define('send_callbacks', default=1, help='If 1, callbacks are sent')

# Script running options
define('tmp_dir', default='/tmp', help='Temp directory to work in')


# Monitoring variables
statemon.define('time_since_stats_update', float) # Time since the last update
statemon.define('time_since_last_batch_event', float) # Time since the most recent event in the stats db
statemon.define('time_since_publish', float) # Time since the last publish
statemon.define('unexpected_statsdb_error', int) # 1 if there was an error last cycle
statemon.define('has_newest_statsdata', int) # 1 if we have the newest data
statemon.define('good_connection_to_impala', int)
statemon.define('good_connection_to_hbase', int)
statemon.define('initialized_directives', int)
statemon.define('videodb_batch_update', int) # Count of the nubmer of batch updates from the video db
statemon.define('videodb_error', int) # error connecting to the video DB
statemon.define('publish_error', int) # error publishing directive to s3
statemon.define('serving_urls_missing', int) # missing serving urls for videos
statemon.define('need_full_urls', int) # Num of thumbs where full urls had to be sent in the directive file
statemon.define('account_default_serving_url_missing', int) # mising default
statemon.define('no_videometadata', int) # mising videometadata 
statemon.define('no_platform', int) # mising platform information 
statemon.define('no_thumbnailmetadata', int) # mising thumb metadata 
statemon.define('unexpected_video_handle_error', int) # Error when handling video
statemon.define('default_serving_thumb_size_mismatch', int) # default thumb size missing 
statemon.define('pending_modifies', int)
statemon.define('directive_file_size', int) # file size in bytes 
statemon.define('unexpected_callback_error', int)
statemon.define('unexpected_db_update_error', int)

statemon.define('accounts_subscribed_to', int)
statemon.define('video_push_updates_received', int)
statemon.define('thumbnails_serving', int)

_log = logging.getLogger(__name__)

def pack_obj(x):
    '''Package an object so that it is smaller in memory'''
    return zlib.compress(pickle.dumps(x), 4)

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

class VideoUpdater(threading.Thread):
    '''This thread processes queued video changes.

    We use this so that a number of changes can get merged into a
    single update.
    
    '''
    def __init__(self, video_db_watcher):
        super(VideoUpdater, self).__init__(name='VideoUpdater')
        self.video_db_watcher = video_db_watcher
        self.daemon = True
        self._stopped = threading.Event()

    def run(self):
        while not self._stopped.is_set():
            try:
                self.video_db_watcher.wait_for_queued_videos()
                self.video_db_watcher.process_queued_video_updates()
            except Exception as e:
                _log.error('Unexpected error when processing queued video '
                           'updates %s' % e)
                statemon.state.increment('unexpected_video_handle_error')

    def stop(self):
        '''Stop this thread safely and allow it to finish what is is doing.'''
        self._stopped.set()

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

        # Objects to subscribe to changes in the database
        self._table_subscribers = []
        self._account_subscribers = {}
        self._subscribe_lock = threading.RLock()

        self._vid_lock = threading.RLock()
        # Set of videos to update
        self._vids_to_update = set()
        self._vids_waiting = threading.Event()
        self._vid_processing_done = threading.Event()
        self._video_updater = VideoUpdater(self)
        self._platform_options_lock = threading.RLock()
        # Options for the platform
        # (api_key, integration_id) -> (abtest, serving_enabled)
        self._platform_options = {} 

    def __del__(self):
        self.stop()
        del self._video_updater
        for sub in self._table_subscribers:
            if sub is not None:
                sub.close()
        for subs in self._account_subscribers.itervalues():
            for sub in subs:
                if sub is not None:
                    sub.close()

    def run(self):
        is_initialized = False
        while not self._stopped.is_set():
            try:
                with self.activity_watcher.activate():
                    if not is_initialized:
                        self._initialize_serving_directives()
                        is_initialized = True
                        statemon.state.initialized_directives = 1
                    self._process_db_data()

                    if not self._video_updater.is_alive():
                        self._video_updater.start()

            except Exception as e:
                _log.exception('Uncaught video DB Error: %s' % e)
                statemon.state.increment('videodb_error')

            # Now we wait so that we don't hit the database too much.
            self._stopped.wait(options.video_db_polling_delay)

    def wait_until_loaded(self, timeout=None):
        '''Blocks until the data is loaded.'''
        if not self.is_loaded.wait(timeout):
            raise TimeoutException("Waiting too long for video data to load")

    def stop(self):
        '''Stop this thread safely and allow it to finish what is is doing.'''
        self._stopped.set()
        self._video_updater.stop()

    def _initialize_serving_directives(self):
        '''Save current experiment state and serving fracs to mastermind
         
        When Mastermind server starts, the experiment_states and current
        serving directives are loaded from the database. If experiment
        is already complete, we will keep its complete state and not
        changing its serving directives.
        '''
        _log.info('Loading current experiment info and updating in mastermind')

        for platform in neondata.AbstractPlatform.iterate_all():
            if not platform.serving_enabled:
                continue
            for video_id in platform.get_internal_video_ids():
                video_metadata = neondata.VideoMetadata.get(video_id)
                account_id = video_metadata.get_account_id()
                if not video_metadata.serving_enabled:
                    continue
                video_status = neondata.VideoStatus.get(video_id,
                                                        log_missing=False)
                if video_status is None:
                    continue

                # Get all thumbnails
                thumbnail_status_list = neondata.ThumbnailStatus.get_many(
                    set(video_metadata.thumbnail_ids), log_missing=False)
                thumbnail_status_list = [x for x in thumbnail_status_list if
                                         x is not None]

                self.mastermind.update_experiment_state_directive(
                    video_id,
                    video_status,
                    thumbnail_status_list)

    def _process_db_data(self):
        _log.info('Polling the video database for a full batch update')

        # Get an update for the tracker id map
        self.directive_pusher.update_tracker_id_map(
            dict(((str(x.get_tai()), str(x.value)) for x in
                  neondata.TrackerAccountIDMapper.iterate_all())))

        # Get an update for the default widths and thumbnail ids
        account_tups = [(str(x.neon_api_key), x.default_size,
                         x.default_thumbnail_id) for x in
                         neondata.NeonUserAccount.iterate_all()]
        self.directive_pusher.update_default_sizes(
            dict((x[0], x[1]) for x in account_tups))
        self.directive_pusher.update_default_thumbs(
            dict((x[0], x[2]) for x in account_tups if x[2]))

        # Update the serving urls for the default account thumbs
        default_thumb_ids = [x[2] for x in account_tups if x[2]]
        for url_obj in neondata.ThumbnailServingURLs.get_many(
                default_thumb_ids):
            if url_obj is not None:
                self.directive_pusher.add_serving_urls(
                    url_obj.get_thumbnail_id(),
                    url_obj)

        # Update the platform, which updates the video data
        for platform in neondata.AbstractPlatform.iterate_all():
            # Update the experimental strategy for the account
            self.mastermind.update_experiment_strategy(
                platform.neon_api_key,
                neondata.ExperimentStrategy.get(platform.neon_api_key))
            
            self._handle_platform_change(platform.get_id(), platform, 'set',
                                         update_videos=False)

            # Force the videos to update
            for internal_video_id in platform.get_internal_video_ids():
                self._schedule_video_update(internal_video_id)
            self.process_queued_video_updates()

        statemon.state.increment('videodb_batch_update')
        self.is_loaded.set()

    def subscribe_to_db_changes(self):
        '''Subscribe to all the changes we care about in the database.'''
        with self._subscribe_lock:
            self._table_subscribers.append(
                neondata.NeonUserAccount.subscribe_to_changes(
                    self._handle_account_change))

            self._table_subscribers.append(
                neondata.AbstractPlatform.subscribe_to_changes(
                    self._handle_platform_change))

            self._table_subscribers.append(
                neondata.ExperimentStrategy.subscribe_to_changes(
                    lambda key, obj, op: 
                    self.mastermind.update_experiment_strategy(key, obj)))

            self._table_subscribers.append(
                neondata.TrackerAccountIDMapper.subscribe_to_changes(
                    lambda key, obj, op: 
                    self.directive_pusher.add_to_tracker_id_map(
                        str(obj.get_tai()), str(obj.value))))

            def _update_serving_url(key, obj, op):
                if op == 'del':
                    try:
                        self.directive_pusher.del_serving_urls(key)
                    except KeyError:
                        pass
                elif op == 'set':
                    if self.mastermind.is_serving_video(
                            self.video_id_cache.find_video_id(key)):
                        self.directive_pusher.add_serving_urls(key, obj)
            self._table_subscribers.append(
                neondata.ThumbnailServingURLs.subscribe_to_changes(
                _update_serving_url))

            if not self._video_updater.is_alive():
                self._video_updater.start()

    def _subscribe_to_video_changes(self, account_id):
        '''Subscribe to changes to video and thumbnail objects for a given 
           account.
        '''
        with self._subscribe_lock:
            if account_id not in self._account_subscribers:
                thumb_pubsub = neondata.ThumbnailMetadata.subscribe_to_changes(
                    lambda key, obj, op: self._schedule_video_update(
                        '_'.join(key.split('_')[0:2]), is_push_update=True),
                    pattern='%s_*' % account_id,
                    get_object=False)
                video_pubsub = neondata.VideoMetadata.subscribe_to_changes(
                    lambda key, obj, op: self._schedule_video_update(
                        key, is_push_update=True),
                    pattern='%s_*' % account_id,
                    get_object=False)
                self._account_subscribers[account_id] = (video_pubsub,
                                                         thumb_pubsub)
                statemon.state.accounts_subscribed_to = \
                  len(self._account_subscribers)

    def _unsubscribe_from_video_changes(self, account_id):
        '''Unsubscribe from changes to videos in a given account'''
        pubsubs = []
        with self._subscribe_lock:
            try:
                pubsubs = self._account_subscribers.pop(account_id)
                statemon.state.accounts_subscribed_to = \
                  len(self._account_subscribers)
            except KeyError:
                return
        
        for sub in pubsubs:
            if sub:
                sub.close()

    def _schedule_video_update(self, video_id, is_push_update=False):
        '''Add a video to the queue to update in the mastermind core.'''
        if neondata.InternalVideoID.is_no_video(video_id):
            return
        with self._vid_lock:
            self._vids_to_update.add(video_id)
            self._vid_processing_done.clear()
            self._vids_waiting.set()
        if is_push_update:
            statemon.state.increment('video_push_updates_received')

    def _handle_platform_change(self, key, platform, operation,
                                update_videos=True):
        '''Handler for when a platform object changes in the database'''
        # TODO: Handle deletion
        if operation != 'set' or platform is None:
            return

        # If there are new settings, then trigger a bunch of updates 
        new_options = (platform.abtest, platform.serving_enabled)
        plat_tup = tuple(key)
        with self._platform_options_lock:
            old_options = self._platform_options.get(plat_tup, None)
            if new_options != old_options:
                self._platform_options[plat_tup] = new_options
            else:
                return

        if platform.serving_enabled:
            self._subscribe_to_video_changes(platform.neon_api_key)
        else:
            self._unsubscribe_from_video_changes(platform.neon_api_key)

        if update_videos:
            for internal_video_id in platform.get_internal_video_ids():
                self._schedule_video_update(internal_video_id)

    def _handle_account_change(self, account_id, account, operation):
        '''Handler for when a NeonUserAccount object changes in the database.'''

        if operation == 'set' and account is not None:
            # Subscribe to this account if we aren't subscribed yet
            self._subscribe_to_video_changes(account_id)

            # Update default size and default thumbs
            self.directive_pusher.default_sizes[account_id] = \
              account.default_size
            if account.default_thumbnail_id is not None:
                self.directive_pusher.default_thumbs[account_id] = \
                  account.default_thumbnail_id
            else:
                try:
                    del self.directive_pusher.default_thumbs[account_id]
                except KeyError: pass

    def _handle_video_update(self, video_id, video_metadata):
        '''Processes a new video state for a single video.'''
        if video_metadata is None:
            statemon.state.increment('no_videometadata')
            _log.error('Could not find information about video %s' % video_id)
            return

        
        thumb_ids = sorted(set(video_metadata.thumbnail_ids))

        try:
            abtest, serving_enabled = self._platform_options[
                (video_metadata.get_account_id(),
                 str(video_metadata.integration_id))]
        except KeyError:
            statemon.state.increment('no_platform')
            _log.error('Could not find platform information for video %s' %
                       video_id)
            return

        if serving_enabled and video_metadata.serving_enabled:
            thumbnails = []
            thumbs = neondata.ThumbnailMetadata.get_many(thumb_ids)
            for thumb_id, meta in zip(thumb_ids, thumbs):
                if meta is None:
                    statemon.state.increment('no_thumbnailmetadata')
                    _log.error('Could not find metadata for thumb %s' %
                               thumb_id)
                    return
                else:
                    thumbnails.append(meta)

            serving_urls = neondata.ThumbnailServingURLs.get_many(thumb_ids)
            for url_obj in serving_urls:
                if url_obj is not None:
                    self.directive_pusher.add_serving_urls(
                        url_obj.get_thumbnail_id(),
                        url_obj)

            self.mastermind.update_video_info(video_metadata,
                                              thumbnails,
                                              abtest)
        else:
            self.mastermind.remove_video_info(video_id)
            for thumb_id in thumb_ids:
                self.directive_pusher.del_serving_urls(thumb_id)

    def process_queued_video_updates(self):
        try:
            # Get the list of video ids to process now
            with self._vid_lock:
                video_ids = list(self._vids_to_update)
                self._vids_to_update = set()
                self._vids_waiting.clear()

            if len(video_ids) == 0:
                return

            _log.debug('Processing %d video updates' % len(video_ids))

            for video_id, video_metadata in zip(*(
                    video_ids,
                    neondata.VideoMetadata.get_many(video_ids))):
                try:
                    self._handle_video_update(video_id, video_metadata)
                except Exception as e:
                    _log.error('Error when updating video %s: %s'
                               % (video_id, e))
                    statemon.state.increment('unexpected_video_handle_error')
        finally:
            with self._vid_lock:
                if len(self._vids_to_update) == 0:
                    self._vid_processing_done.set()

    def wait_for_queued_videos(self, timeout=None):
        return self._vids_waiting.wait(timeout)

    def wait_for_video_processing(self, timeout=None):
        return self._vid_processing_done.wait(timeout)
        

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
        self.last_update = None # Time of the most recent data
        self.last_table_build = None # Time when the tables were last built
        self._update_stats_timer = None
        self.impala_conn = None
        self.daemon = True
        self.activity_watcher = activity_watcher

        # Is the initial data loaded
        self.is_loaded = threading.Event()
        self._stopped = threading.Event()

        self._update_time_since_stats_update()

    def __del__(self):
        if self._update_stats_timer and self._update_stats_timer.is_alive():
            self._update_stats_timer.cancel()
        super(StatsDBWatcher, self).__del__()

    def _update_time_since_stats_update(self):
        if self.last_update is not None:
            statemon.state.time_since_last_batch_event = (
                datetime.datetime.now() - self.last_update).total_seconds()

        if self.last_table_build is not None:
            statemon.state.time_since_stats_update = (
                datetime.datetime.now() -
                self.last_table_build).total_seconds()

        self._update_stats_timer = threading.Timer(
            10.0, self._update_time_since_stats_update)
        self._update_stats_timer.daemon = True
        self._update_stats_timer.start()

    def wait_until_loaded(self, timeout=None):
        '''Blocks until the data is loaded.'''
        if not self.is_loaded.wait(timeout):
            raise TimeoutException("Waiting too long for stats data to load")

    def stop(self):
        '''Stop this thread safely and allow it to finish what is is doing.'''
        self._stopped.set()

    def run(self):
        while not self._stopped.is_set():
            try:
                with self.activity_watcher.activate():
                    self._process_db_data()
                statemon.state.unexpected_statsdb_error = 0

            except Exception as e:
                _log.exception('Uncaught stats DB Error: %s' % e)
                statemon.state.unexpected_statsdb_error = 1

            finally:
                if self.impala_conn is not None:
                    self.impala_conn.close()
                    self.impala_conn = None

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
            statemon.state.good_connection_to_impala = 0
            raise

    def _process_db_data(self):
        strategy_cache = ExperimentStrategyCache()

        _log.info('Polling the stats database')
        if self._is_newer_batch_data():
            _log.info('Found a newer entry in the stats database from %s. '
                      'Processing' % self.last_update.isoformat())
            cursor = self.impala_conn.cursor()
            try:
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
                for tai_info in neondata.TrackerAccountIDMapper.iterate_all():
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
                             "and (yr > {yr:d} or "
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
                             "and (yr > {yr:d} or "
                             "(yr = {yr:d} and mnth >= {mnth:d})) "
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
                _log.info('Finished processing batch stats update')
                statemon.state.has_newest_statsdata = 1
            finally:
                cursor.close()
        else:
            _log.info('Looking for incremental stats update from host %s' %
                      options.incr_stats_host)
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

        try:

            # First look at table_build_times to find if the last
            # table was built.
            try:
                cursor.execute('select max(done_time) from table_build_times')
                table_build_result = cursor.fetchall()

                if (len(table_build_result) == 0 or 
                    table_build_result[0][0] is None):
                    _log.error('Cannot determine when the database was last '
                               'updated')
                    statemon.state.good_connection_to_impala = 0
                    self.is_loaded.set()
                    return False

                cur_table_build = table_build_result[0][0]
                if not isinstance(cur_table_build, datetime.datetime):
                    cur_table_build = \
                      dateutil.parser.parse(table_build_result[0][0])

                statemon.state.time_since_stats_update = (
                    datetime.datetime.now() - cur_table_build).total_seconds()

                is_newer = (self.last_table_build is None or 
                            cur_table_build > self.last_table_build)
                statemon.state.good_connection_to_impala = 1
                if not is_newer:
                    return False
            except impala.error.RPCError as e:
                _log.error('SQL Error. Probably a table is not available yet. '
                           '%s' % e)
                statemon.state.good_connection_to_impala = 0
                self.is_loaded.set()
                return False


            try:
                # Now, find out when the last event we knew about was
                curtime = datetime.datetime.utcnow()
                cursor.execute(
                    ('SELECT max(serverTime) FROM videoplays WHERE '
                     'yr >= {yr:d} or (yr = {yr:d} and mnth >= {mnth:d})'
                     ).format(
                         mnth=curtime.month, yr=curtime.year))
                play_result = cursor.fetchall()
                if len(play_result) == 0 or play_result[0][0] is None:
                    _log.error('Cannot find any videoplay events')
                    statemon.state.good_connection_to_impala = 0
                    self.is_loaded.set()
                    return False
            except impala.error.RPCError as e:
                _log.error('SQL Error. Probably a table is not available yet. '
                           '%s' % e)
                statemon.state.good_connection_to_impala = 0
                self.is_loaded.set()
                return False
        finally:
            cursor.close()

        # Update the state variables
        self.last_update = datetime.datetime.utcfromtimestamp(
            play_result[0][0])
        self.last_table_build = cur_table_build
        
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
            conn = happybase.Connection(options.incr_stats_host,
                                        timeout=300000)
            try:
                col_family = options.incr_stats_col_family
                col_map = {
                    neondata.MetricType.LOADS : '%s:il' % col_family,
                    neondata.MetricType.VIEWS : '%s:iv' % col_family,
                    neondata.MetricType.CLICKS : '%s:ic' % col_family,
                    neondata.MetricType.PLAYS : '%s:vp' % col_family}

                row_start = None
                row_stop = None
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
                    row_stop = thumb_id + 'a'
                    if self.last_update is None:
                        row_start = thumb_id
                    else:
                        row_start = '_'.join([
                            thumb_id,
                            self.last_update.strftime('%Y-%m-%dT%H')])

                for key, row in table.scan(row_start=row_start,
                                           row_stop=row_stop,
                                           columns=[col_family]):
                    if thumb_id is None:
                        tid = key.partition('_')[2]
                    else:
                        tid = thumb_id
                    if tid == '':
                        _log.warn_n('Invalid thumbnail id in key %s' % key,
                                    100)
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

                        incr_imp = struct.unpack(
                            '>q',
                            row.get(impr_col, '\x00'*8))[0]
                        incr_conv = struct.unpack(
                            '>q',
                            row.get(conv_col,'\x00'*8))[0]
                        counts[0] += incr_imp
                        counts[1] += incr_conv
                    except struct.error as e:
                        _log.warn_n('Invalid value found for key %s: %s' %
                                    (key, e), 100)
                        continue

                    retval[tid] = counts
                statemon.state.good_connection_to_hbase = 1
            finally:
                conn.close()
        except thrift.Thrift.TException as e:
            _log.error('Error connecting to incremental stats database: %s'
                       % e)
            statemon.state.good_connection_to_hbase = 0

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

    The second type of file is a default thumbnail for an account.

    {
    "type":"default_thumb",
    "aid":"account1",
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
    }

    The third type of file is a serving directive for a video. The
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
    def __init__(self, mastermind, tracker_id_map=None, serving_urls=None,
                 default_sizes=None, default_thumbs=None,
                 activity_watcher=utils.ps.ActivityWatcher()):
        '''Creates the publisher.

        Inputs:
        mastermind - The mastermind.core.Mastermind object that has the logic
        tracker_id_map - A map of tracker_id -> account_id
        serving_urls - A map of thumbnail_id -> { (width, height) -> url }
        default_sizes - A map of account_id (aka api_key) -> 
                                             default thumbnail (w,h)
        default_thumbs - A map of account_id (aka api_key) ->
                                             default thumbnail id
        '''
        super(DirectivePublisher, self).__init__(name='DirectivePublisher')
        self.mastermind = mastermind
        self.tracker_id_map = tracker_id_map or {}
        self.serving_urls = serving_urls or {}
        self.default_sizes = default_sizes or {}
        self.default_thumbs = default_thumbs or {}
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

        # Set of last video ids in the directive file
        self.last_published_videos = set([])

        self.callback_manager = utils.sqsmanager.CustomerCallbackManager()
        self._callback_thread = None

        # Counter for the number of pending modify calls to the database
        self.pending_modifies = multiprocessing.Value('i', 0)
        statemon.state.pending_modifies = 0
        self._lock = multiprocessing.RLock()
        self.modify_waiter = multiprocessing.Condition()

    def __del__(self):
        if self._update_publish_timer and self._update_publish_timer.is_alive():
            self._update_publish_timer.cancel()
        super(DirectivePublisher, self).__del__()

    def _incr_pending_modify(self, count):
        '''Safely increment the number of pending modifies by count.'''
        with self._lock:
            self.pending_modifies.value += count

        with self.modify_waiter:
            self.modify_waiter.notify_all()
        statemon.state.pending_modifies = self.pending_modifies.value

    def wait_for_pending_modifies(self):
        with self.modify_waiter:
            while self.pending_modifies.value > 0:
                self.modify_waiter.wait()

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
            
    def add_to_tracker_id_map(self, tracker_id, account_id):
        self.tracker_id_map[tracker_id] = account_id

    def add_serving_urls(self, thumbnail_id, urls_obj):
        '''Add serving urls for a given thumbnail

        Inputs:
        thumbnail_id - The thumbnail id
        urls_obj - A ThumbnailServingURLs objec
        '''
        with self.lock:
            self.serving_urls[thumbnail_id] = pack_obj(urls_obj.__dict__)
        statemon.state.thumbnails_serving = len(self.serving_urls)

    def del_serving_urls(self, thumbnail_id):
        try:
            with self.lock:
                del self.serving_urls[thumbnail_id]
        except KeyError as e:
            pass
        statemon.state.thumbnails_serving = len(self.serving_urls)

    def get_serving_urls(self, thumbnail_id):
        obj = neondata.ThumbnailServingURLs('')
        obj.__dict__ = unpack_obj(self.serving_urls[thumbnail_id])
        return obj

    def update_default_sizes(self, new_map):
        with self.lock:
            self.default_sizes = new_map

    def update_default_thumbs(self, new_map):
        with self.lock:
            self.default_thumbs = new_map

    def _update_time_since_publish(self):
        statemon.state.time_since_publish = (
            datetime.datetime.utcnow() -
            self.last_publish_time).total_seconds()

        self._update_publish_timer = threading.Timer(
            10.0, self._update_time_since_publish)
        self._update_publish_timer.daemon = True
        self._update_publish_timer.start()

    def _publish_directives(self):

        '''Publishes the directives to S3'''
        # Create the directives file
        _log.info("Building directives file")
        if not os.path.exists(options.tmp_dir):
            os.makedirs(options.tmp_dir)
            
        with closing(tempfile.NamedTemporaryFile(
                'w+b', dir=options.tmp_dir)) as directive_file:
            # Create the space for the expiry
            self._write_expiry(directive_file)

            # Write the data
            with self.lock:
                written_video_ids = self._write_directives(directive_file)

            directive_file.write('\nend')

            # Overwrite the expiry because write_directives can take a while
            self._write_expiry(directive_file)
            directive_file.flush()
            directive_file.seek(0)
            curtime = datetime.datetime.utcnow()

            with closing(tempfile.NamedTemporaryFile(
                    'w+b', dir=options.tmp_dir)) as gzip_file:
                gzip_stream = gzip.GzipFile(mode='wb',
                                            compresslevel=7,
                                            fileobj=gzip_file)
                gzip_stream.writelines(directive_file)
                gzip_stream.close()
                gzip_file.flush()

                filename = '%s.%s' % (curtime.strftime('%Y%m%d%H%M%S'),
                                      options.directive_filename)
                _log.info('Publishing directive to s3://%s/%s' %
                          (options.s3_bucket, filename))

                # Create the connection to S3
                s3conn = self.S3Connection()
                try:
                    bucket = s3conn.get_bucket(options.s3_bucket)
                except boto.exception.BotoServerError as e:
                    _log.error('Could not get bucket %s: %s' % 
                               (options.s3_bucket, e))
                    statemon.state.increment('publish_error')
                    return
                except boto.exception.BotoClientError as e:
                    _log.error('Could not get bucket %s: %s' % 
                               (options.s3_bucket, e))
                    statemon.state.increment('publish_error')
                    return
                except socket.error as e:
                    _log.error('Error connecting to S3: %s' % e)
                    statemon.state.increment('publish_error')
                    return

                # Write the file that is timestamped
                key = bucket.new_key(filename)
                gzip_file.seek(0)
                data_size = key.set_contents_from_file(
                    gzip_file,
                    encrypt_key=True,
                    headers={'Content-Type': 'application/x-gzip'},
                    replace=True)
                statemon.state.directive_file_size = data_size

                # Copy the file to the REST endpoint
                key.copy(bucket.name, options.directive_filename,
                         encrypt_key=True,
                         preserve_acl=True)

                # Schedule updates to the database with the video request state
                new_serving_videos = (written_video_ids - \
                                      self.last_published_videos)
                just_stopped_videos = (self.last_published_videos - \
                                       written_video_ids)
                self.last_published_videos = written_video_ids
                if len(new_serving_videos) > 0:
                    self._incr_pending_modify(len(new_serving_videos))
                    t = threading.Timer(
                        options.serving_update_delay,
                        self._update_video_serving_state,
                        (new_serving_videos,
                         neondata.RequestState.SERVING))
                    t.daemon = True
                    t.start()
                if len(just_stopped_videos) > 0:
                    self._incr_pending_modify(len(just_stopped_videos))
                    t = threading.Timer(
                        options.serving_update_delay,
                        self._update_video_serving_state,
                        (just_stopped_videos,
                         neondata.RequestState.FINISHED))
                    t.daemon = True
                    t.start()

                # Send the callbacks for new videos after a delay
                if self._callback_thread is None:
                    self._callback_thread = threading.Timer(
                        options.serving_update_delay,
                        self._send_callbacks)
                    self._callback_thread.daemon = True
                    self._callback_thread.start()

                self.last_publish_time = curtime

    def _write_directives(self, stream):
        '''Write the current directives to the stream.

        Returns the video ids of the directives that were sucessfully written.
        '''
        written_video_ids = set([])
        
        # First write out the tracker id maps
        _log.info("Writing tracker id maps")
        for tracker_id, account_id in self.tracker_id_map.iteritems():
            stream.write('\n' + json.dumps({'type': 'pub', 'pid': tracker_id,
                                            'aid': account_id}))

        # Next write the default thumbnails for each account that has them
        _log.info("Writing default thumbnails")
        missing_account_default_serving = 0
        for account_id, thumb_id in self.default_thumbs.iteritems():
            try:
                default_thumb_directive = self._get_url_fields(account_id,
                                                               thumb_id)
                default_thumb_directive['type'] = 'default_thumb'
                default_thumb_directive['aid'] = account_id
                stream.write('\n' + json.dumps(default_thumb_directive))
            except KeyError:
                _log.error_n('Could not find serving url for thumb %s, '
                             'which is the default on account %s . Skipping' %
                             (thumb_id, account_id))
                missing_account_default_serving += 1
        statemon.state.account_default_serving_url_missing = \
          missing_account_default_serving

        # Now write the directives
        _log.info("Writing directives")
        serving_urls_missing = 0
        need_full_urls = 0
        for key, directive in self.mastermind.get_directives():
            account_id, video_id = key
            fractions = []
            missing_urls = False
            for thumb_id, frac in directive:
                try:
                    serving_urls = unpack_obj(self.serving_urls[thumb_id])
                    frac_obj = self._get_url_fields(account_id, thumb_id)
                    frac_obj['pct'] = frac
                    frac_obj['tid'] = thumb_id
                    fractions.append(frac_obj)
                    if 'default_url' in frac_obj:
                        need_full_urls += 1
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
            if len(fractions) > 1:
                # If the default thumb is there, we want to serve it,
                # but not flag that it is serving yet. So, we need
                # more than one thumb associated with the video.  THIS
                # IS A HACK
                # TODO(mdesnoyer): Keep the video state
                # around and do this properly.
                written_video_ids.add(video_id)

        statemon.state.serving_urls_missing = serving_urls_missing
        statemon.state.need_full_urls = need_full_urls
        return written_video_ids

    def _get_default_size(self, account_id, url_obj):
        '''Returns the default url size (w,h) for this thumbnail id.'''
        default_size = self.default_sizes.get(account_id, None)
        if default_size is None:
            default_size = (160, 90)

        # Make sure the default size exists
        if url_obj.is_valid_size(*default_size):
            return default_size

        # We couldn't find the exact size so pick the one with the
        # minimum size difference.
        valid_sizes = url_obj.sizes.union(url_obj.size_map.iterkeys())
        if len(valid_sizes) == 0:
            _log.warn('No valid sizes to serve for thumb %s' 
                      % url_obj.get_id())
            raise KeyError('No valid sizes to serve')
        mindiff = min([abs(x[0] - default_size[0]) +
                           abs(x[1] - default_size[1]) 
                           for x in valid_sizes])
        closest_size = [x for x in valid_sizes if
                        (abs(x[0] - default_size[0]) +
                         abs(x[1] - default_size[1])) == mindiff][0]
        _log.warn('There is no serving thumb of size (%i, %i) for thumb'
                  '%s. Using (%i, %i) instead'
                  % (default_size[0], default_size[1],
                     url_obj.get_thumbnail_id(),
                     closest_size[0], closest_size[1]))
        statemon.state.increment('default_serving_thumb_size_mismatch')
        return closest_size

    def _write_expiry(self, fp):
        '''Writes the expiry line at the beginning of the file point fp.

        Seeks if necessary.
        '''
        fp.seek(0)
        valid_length = options.expiry_buffer + options.publishing_period
        fp.write('expiry=%s' % 
                 (datetime.datetime.utcnow() +
                  datetime.timedelta(seconds=valid_length))
                 .strftime('%Y-%m-%dT%H:%M:%SZ'))
        fp.flush()

    def _get_url_fields(self, account_id, thumb_id):
        '''Returns a dictionary of the url fields for a thumbnail.

        Does an older version, where each serving url is specified for
        each size and a newer version where we just specify the base
        url and a list of valid sizes.
        '''
        urls = self.get_serving_urls(thumb_id)
        if len(urls.size_map) > 0:
            # We have some urls with a different base, so it's the old style
            return {
                'default_url': urls.get_serving_url(*self._get_default_size(
                    account_id, urls)),
                'imgs' : [
                    {
                        'w': k[0],
                        'h': k[1],
                        'url': v
                    }
                    for k, v in urls]
                    }
        else:
            # All the urls can be generated from a single base url and
            # different sizes so use that for the directive.
            return {
                'base_url' : urls.base_url,
                'default_size': dict(zip(*[('w','h'),
                                           self._get_default_size(account_id,
                                                                  urls)])),
                'img_sizes' : [ { 'h': h, 'w': w} for w, h in urls.sizes]
                }

    def _update_video_serving_state(self, video_ids, new_state):
        '''Updates a list of video ids with a new serving state.'''
        MAX_VIDS_PER_CALL = 100
        video_ids = list(video_ids)
        for startI in range(0, len(video_ids), MAX_VIDS_PER_CALL):
            cur_vid_ids = video_ids[startI:(startI+MAX_VIDS_PER_CALL)]
            try:
                def _set_serving_url(videos_dict):
                    for vidobj in videos_dict.itervalues():
                        if vidobj is not None:
                            if new_state == neondata.RequestState.SERVING:
                                vidobj.serving_url = vidobj.get_serving_url(
                                    save=False)
                            else:
                                vidobj.serving_url = None
                videos = neondata.VideoMetadata.modify_many(cur_vid_ids,
                                                            _set_serving_url)
                
                request_keys = [(video.job_id, video.get_account_id()) for
                                video in videos.itervalues()
                                if video is not None]
                def _set_state(request_dict):
                    for obj in request_dict.itervalues():
                        if obj is not None:
                            if obj.state not in [
                                    neondata.RequestState.CUSTOMER_ERROR,
                                    neondata.RequestState.FAILED,
                                    neondata.RequestState.INT_ERROR]:
                                obj.state = new_state
                neondata.NeonApiRequest.modify_many(request_keys, _set_state)
                
            except Exception as e:
                statemon.state.increment('unexpected_db_update_error')
                _log.exception('Unexpected error when updating serving state in '
                               'database %s' % e)
                # We didn't update the database so don't say that the
                # videos were published. This will trigger a retry next
                # time the directives are pushed.
                with self.lock:
                    self.last_published_videos = \
                      self.last_published_videos - set(cur_vid_ids)
            finally:
                self._incr_pending_modify(-len(cur_vid_ids))

    def _send_callbacks(self):
        try:
            if options.send_callbacks:
                self.callback_manager.schedule_all_callbacks(
                    self.last_published_videos)
        except Exception as e:
            _log.warn('Unexpected error when sending a customer '
                      'callback: %s' % e)
            statemon.state.increment('unexpected_callback_error')
        finally:
            self._callback_thread = None
        
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
        videoDbThread.subscribe_to_db_changes()
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

    publisher.join(300)
    videoDbThread.join(30)
    statsDbThread.join(30)
    
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
