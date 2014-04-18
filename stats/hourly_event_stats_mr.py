#!/usr/bin/env python
'''Map reduce jobs for processing the stats

Aggregates stats on an hourly basis for each thumbnail. Also converts
the external url to an internal thumbnail id.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
    
from datetime import datetime
import json
import logging
from mrjob.job import MRJob
import mrjob.protocol
import mrjob.util
import MySQLdb as sqldb
import re
import redis.exceptions
import stats.db
from supportServices import neondata
import utils.neon
import utils.options

_log = logging.getLogger(__name__)

class HourlyEventStats(MRJob):
    '''MapReduce job that gets statistics on an hourly basis for each thumb.'''
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def __init__(self, *args, **kwargs):
        super(HourlyEventStats, self).__init__(*args, **kwargs)
        self.statsdb = None
        self.statscursor = None

    def configure_options(self):
        super(HourlyEventStats, self).configure_options()
        self.add_passthrough_option(
            '--stats_host',
            default='stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com',
            help='Host of the stats database')
        self.add_passthrough_option(
            '--stats_port', type='int', default=3306,
            help='Port to the stats database')
        self.add_passthrough_option(
            '--stats_user', default='mrwriter',
            help='User for the stats database')
        self.add_passthrough_option('--stats_pass', default='kjge8924qm',
                                    help='Password for the stats database')
        self.add_passthrough_option('--stats_db', default='stats_dev',
                                    help='Stats database to connect to')
        self.add_passthrough_option(
            '--increment_stats', type='int', default=0,
            help=('If true, stats are incremented. Otherwise, they are '
                  'overwritten'))
        self.add_passthrough_option(
            '--stats_table', default='hourly_events',
            help='Table in the stats database to write to')
        self.add_file_option(
            '--neon_config', default=None,
            help='Config file to parse for Neon options.')

    def load_options(self, args):
        super(HourlyEventStats, self).load_options(args)

        if (self.options.neon_config is not None and 
            not utils.options.options_loaded()):
            with open(self.options.neon_config) as f:
                utils.options.parse_options(args=[], config_stream=f)

    def mapper_get_events(self, line, _):
        '''Reads the json log and outputs event data.

        In particular, the output is:

        ((event_type, page_load_id, img_url, tracker_id), (hour, 1))
        For counting on an hourly basis

        or
        
        ('latest', time)
        For tracking the last known event
        '''
        try:
            data = json.loads(line)
            if data['ttype'] == 'html5':
                self.increment_counter(
                    'HourlyEventStatsErrors',
                    'HTML5_bc_click' if data['a'] == 'click' else 
                    'HTML5_bc_load',
                    1)
                return

            tracker_id = data['tai']
            page_load_id = data['id']
            
            hour = data['sts'] / 3600
            if data['a'] == 'load':
                if isinstance(data['imgs'], basestring):
                    raise KeyError('imgs')
                for img in data['imgs']:
                    if img is not None:
                        yield (('load', page_load_id, img, tracker_id),
                               (hour, 1))
            elif data['a'] == 'click':
                yield(('click', page_load_id, data['img'], tracker_id), 
                      (hour, 1))

            yield ('latest', data['sts'])
        except ValueError as e:
            _log.error('JSON could not be parsed: %s' % line)
            self.increment_counter('HourlyEventStatsErrors',
                                   'JSONParseErrors', 1)
        except KeyError as e:
            _log.error('Input data was missing a necessary field (%s): %s' % 
                       (e, line))
            self.increment_counter('HourlyEventStatsErrors',
                                   'JSONFieldMissing', 1)

    def combiner_merge_latest_time(self, event, values):
        '''Combines the latest time for entries in the log.

        If the entries are of the form:
        ('latest', time)
        They are combined, otherwise, they are ignored.
        '''
        if event == 'latest':
            yield (event, max(values))
        else:
            for value in values:
                yield event, value

    def reducer_filter_duplicate_events(self, event, values):
        '''Filters duplicate events in the log into a single one.

        Clicks and loads for a given image could appear twice in the
        log, so if it's the same image and same page load id, we
        should combine into a single event.

        Also identifies the latest known event.

        Input:
        ((event_type, page_load_id, img_url, tracker_id), (hour, 1))

        or

        ('latest', time)

        Output:
        ((event_type, img_url, tracker_id, hour), 1)

        or

        ('latest', time)
        '''
        if event == 'latest':
            yield (event, max(values))
            return

        event_type, page_load_id, img_url, tracker_id = event
        yield ((event_type, img_url, tracker_id, min([x[0] for x in values])),
               1)

    def reducer_count_events(self, event, counts):
        '''Sums up the counts for the events.

        Expects entries of the form

        ((event_type, img_url, tracker_id, hour), count)

        or

        ('latest', time)

        and outputs the same, where count is the sum of all the counts.
        '''
        if event == 'latest':
            yield (event, max(counts))
        else:
            yield (event, sum(counts))

    def map_thumbnail_url2id(self, event, count):
        '''Maps from the external thumbnail url to our internal id.

        Also filters those entries from non-production tracker ids.

        Input:
        ((event_type, img_url, tracker_id, hour), count)

        Output:
        ((event_type, thumb_id, hour), count)

        or
        
        ('latest', time)
        For tracking the last known event
        '''
        if event == 'latest':
            yield (event, count)
            return

        try:
            img_url = event[1]
            tracker_id = event[2]
            account_info = neondata.TrackerAccountIDMapper.get_neon_account_id(
                tracker_id)
            if account_info is None:
                _log.warn('Tracker ID %s unknown', tracker_id)
                self.increment_counter('HourlyEventStatsErrors',
                                       'UnknownTrackerId', 1)
                return
            elif account_info[1] != neondata.TrackerAccountIDMapper.PRODUCTION:
                # The tracker ID isn't flagged for production, so drop
                # this entry
                return
            
            thumb_id = neondata.ThumbnailURLMapper.get_id(img_url)

            if thumb_id is None:
                # Look for the thumbnail id in the url
                thumb_id = self._extract_thumbnail_id_from_url(img_url,
                                                               account_info[0])
                if thumb_id is None:
                    _log.warn('Unknown Thumbnail URL %s' % img_url)
                    self.increment_counter('HourlyEventStatsErrors',
                                           'UnknownThumbnailURL', 1)
                    return
            if len(thumb_id) < 5:
                _log.warn('ID for URL %s is too short: %s' % (img_url,
                                                              thumb_id))
                self.increment_counter('HourlyEventStatsErrors',
                                       'ThumbIDTooShort', 1)
                return
                
            yield (event[0], thumb_id, event[3]), count
        except redis.exceptions.RedisError as e:
            _log.exception('Error getting data from Redis server: %s' % e)
            self.increment_counter('HourlyEventStatsErrors',
                                   'RedisErrors', 1)

    def _extract_thumbnail_id_from_url(self, img_url, account_id):
        '''Tries to extract the thumbnail id from the url itself.

        Returns None if it can't be found
        '''
        # This is used to extract the data we put in the brightcove url
        bc_url_tag_extractor = \
          re.compile(r'https*:\/\/brightcove.*/[0-9]+_[0-9]+_([\w\-]+)\.jpg')
        # This is used to parse the thumbnail id
        tidRe = re.compile(r'([0-9a-z]+)_(\d+)_([a-f0-9]+)')
        # These are used to identify old format brightcove or
        # neonthumbnail urls. TODO(mdesnoyer) remove this hack
        oldBcRe = re.compile(r'neonthumbnailbc_([0-9]+)')
        oldNeonRe = re.compile(r'neonthumbnail_([0-9]+)')

        bc_tag_found = bc_url_tag_extractor.search(img_url)
        if bc_tag_found:
            bc_tag = bc_tag_found.group(1).replace('-', '_')
            
            tid_found = tidRe.match(bc_tag)
            if tid_found:
                # The thumbnail is just the tag we threw into brightcove
                return bc_tag
            else:
                # TODO(mdesnoyer): Remove this hack
                oldBcMatch = oldBcRe.match(bc_tag)
                oldNeonMatch = oldNeonRe.match(bc_tag)
                if oldBcMatch:
                    # It's the default brightcove thumbnail
                    # for this video
                    ext_video_id = oldBcMatch.group(1)
                    video_data = neondata.VideoMetadata.get(
                        neondata.InternalVideoID.generate(
                            account_id, ext_video_id))
                    if video_data is None:
                        return None
                    thumbs = neondata.ThumbnailMetadata.get_many(
                        video_data.thumbnail_ids)
                    for thumb in thumbs:
                        if thumb.type == 'brightcove':
                            return thumb.key
                elif oldNeonMatch:
                    # It's a Neon thumbnail, so take the chosen one
                    # Hopefully it hasn't changed
                    ext_video_id = oldNeonMatch.group(1)
                    video_data = neondata.VideoMetadata.get(
                        neondata.InternalVideoID.generate(
                            account_id, ext_video_id))
                    if video_data is None:
                        return None
                    thumbs = neondata.ThumbnailMetadata.get_many(
                        video_data.thumbnail_ids)
                    for thumb in thumbs:
                        if thumb.type == 'neon' and thumb.chosen:
                            return thumb.key
        return None

    def merge_events(self, event, count):
        if event == 'latest':
            yield (event, count)
            return
        yield ((event[1], event[2]), (count, event[0]))

    def write_latest2db(self, time):
        '''Writes the latest log time to the database.'''
        stats.db.execute(self.statscursor,
            '''REPLACE INTO last_update (tablename, logtime) VALUES (%s, %s)''',
            (self.options.stats_table, datetime.utcfromtimestamp(time)))

    def reducer_write2db(self, img_hr, count_events):
        '''Writes the event counts to the database.

        Inputs:
        img_hr - (img_id, hours since epoch)
        count_events - [(count, event name)]
        '''
        if img_hr == 'latest':
            return self.write_latest2db(count_events.next())
        
        img_id, hours = img_hr
        counts = {}
        for count, event in count_events:
            counts[event] = count
        loads = counts.setdefault('load', 0)
        clicks = counts.setdefault('click', 0)
        hourdate = datetime.utcfromtimestamp(hours * 3600)

        if self.options.increment_stats:
            stats.db.execute(self.statscursor,
                '''SELECT loads, clicks from %s 
                where thumbnail_id = %%s and hour = %%s''' %
              self.options.stats_table, (img_id, hourdate))
            result = self.statscursor.fetchone()
            if result is None:
                stats.db.execute(self.statscursor,
                    '''INSERT INTO %s (thumbnail_id, hour, loads, clicks)
                    VALUES (%%s, %%s, %%s, %%s) ''' % self.options.stats_table,
                    (img_id, hourdate, loads, clicks))
            else:
                stats.db.execute(self.statscursor,
                    '''UPDATE %s set loads=%%s, clicks=%%s where
                    thumbnail_id = %%s and hour = %%s''' %
                    self.options.stats_table,
                    (loads + result[0], clicks + result[1], img_id, hourdate))
        else:
            stats.db.execute(self.statscursor,
                '''REPLACE INTO %s (thumbnail_id, hour, loads, clicks) 
                VALUES (%%s, %%s, %%s, %%s) ''' % self.options.stats_table,
                (img_id, hourdate, loads, clicks))

    def statsdb_connect(self):
        '''Create connection to the stats DB and create tables if necessary.'''
        try:
            self.statsdb = sqldb.connect(
                user=self.options.stats_user,
                passwd=self.options.stats_pass,
                host=self.options.stats_host,
                port=self.options.stats_port,
                db=self.options.stats_db)
        except sqldb.Error as e:
            _log.exception('Error connecting to stats db: %s' % e)
            raise
        self.statscursor = self.statsdb.cursor()
        
        stats.db.create_tables(self.statscursor)

    def statsdb_disconnect(self):
        '''Disconnect from the stats DB'''
        self.statscursor.close()
        self.statsdb.commit()
        self.statsdb.close()

    def steps(self):
        return [
            self.mr(mapper=self.mapper_get_events,
                    combiner=self.combiner_merge_latest_time,
                    reducer=self.reducer_filter_duplicate_events),
            self.mr(combiner=self.reducer_count_events,
                    reducer=self.reducer_count_events),
            self.mr(mapper=self.map_thumbnail_url2id,
                    reducer=self.reducer_count_events),
            self.mr(mapper=self.merge_events,
                    reducer_init=self.statsdb_connect,
                    reducer=self.reducer_write2db,
                    reducer_final=self.statsdb_disconnect)]

def main():
    '''Main function for when running from the command line.'''
    # Setup a logger for dumping errors to stderr
    utils.logs.CreateLogger(__name__, stream=sys.stderr,
                            level=logging.WARNING)
    
    HourlyEventStats.run()

if __name__ == '__main__':
    main()
