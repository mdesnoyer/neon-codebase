#!/usr/bin/env python
'''Map reduce jobs for processing the stats

Aggregates stats on an hourly basis for each thumbnail. Also converts
the external url to an internal thumbnail id.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
from datetime import datetime
import json
import logging
from mrjob.job import MRJob
import mrjob.protocol
import mrjob.util
import MySQLdb as sqldb
import stats.db
import time
import urllib
import urllib2
import utils.neon

_log = logging.getLogger(__name__)

from utils.options import define, options

define('stats_host', default='stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com',
       help='Host of the stats database')
define('stats_port', type=int, default=3306,
       help='Port to the stats database')
define('stats_user', default='mrwriter', help='User for the stats database')
define('stats_pass', default='kjge8924qm',
       help='Password for the stats database')
define('stats_db', default='stats_dev', help='Stats database to connect to')
define('increment_stats', type=int, default=0,
       help='If true, stats are incremented. Otherwise, they are overwritten')
define('stats_table', default='hourly_events',
       help='Table in the stats database to write to')
define('videodb_url', default='http://localhost:8080',
       help='url for the video database call')


class HourlyEventStats(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol            

    def mapper_get_events(self, line, _):
        '''Reads the json log and outputs event data.

        In particular, the output is:

        ((event_type, img_url, hour), 1)
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
            
            hour = data['sts'] / 3600
            if data['a'] == 'load':
                if isinstance(data['imgs'], basestring):
                    raise KeyError('imgs')
                for img in data['imgs']:
                    if img is not None:
                        yield (('load', img, hour),  1)
            elif data['a'] == 'click':
                yield(('click', data['img'], hour), 1)

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

    def reducer_count_events(self, event, counts):
        if event == 'latest':
            yield (event, max(counts))
        else:
            yield (event, sum(counts))

    def videodb_connect(self):
        # We're not talking to a true database at the moment
        pass

    def videodb_disconnect(self):
        # We're not talking to a true database at the moment
        pass

    def map_thumbnail_url2id(self, event, count):
        '''Maps from the external thumbnail url to our internal id.'''
        if event == 'latest':
            yield (event, count)
            return
        
        try:
            stream = urllib2.urlopen(options.videodb_url,
                                     urllib.urlencode({'url':event[1]}),
                                     60)
            data = json.load(stream)
            if data['tid'] is None:
                _log.error('Could not find the thumbnail id for url: %s' %
                           event[1])
                self.increment_counter('HourlyEventStatsErrors',
                                       'UnknownThumbnailURL', 1)
                return
                
            if len(data['tid']) < 5:
                raise ValueError('Thumbnail ID is too short: %s' % data['tid'])
            yield (event[0], data['tid'], event[2]), count
        except urllib2.URLError as e:
            _log.exception('Error connecting to: %s' % 
                           options.videodb_url)
            self.increment_counter('HourlyEventStatsErrors',
                                   'VideoDBConnectionError', 1)
        except KeyError as e:
            _log.error('Data format incorrect: %s' % e)
            self.increment_counter('HourlyEventStatsErrors',
                                   'TIDFieldMissing', 1)
        except ValueError as e:
            _log.error('Could not parse response')
            self.increment_counter('HourlyEventStatsErrors',
                                   'TIDParseError', 1)
        except IOError as e:
            _log.exception(
                'Error reading data from videodb for thumbnail url %s: %s' % 
                (event[1], e))
            self.increment_counter('HourlyEventStatsErrors',
                                   'TIDParseError', 1)

    def merge_events(self, event, count):
        if event == 'latest':
            yield (event, count)
            return
        yield ((event[1], event[2]), (count, event[0]))

    def write_latest2db(self, time):
        '''Writes the latest log time to the database.'''
        self.statscursor.execute(
            '''REPLACE INTO last_update (tablename, logtime) VALUES (?, ?)''',
            (options.stats_table, datetime.utcfromtimestamp(time)))

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

        if options.increment_stats:
            self.statscursor.execute(
                '''SELECT loads, clicks from %s 
                where thumbnail_id = ? and hour = ?''' %
              options.stats_table, (img_id, hourdate))
            result = self.statscursor.fetchone()
            if result is None:
                self.statscursor.execute(
                    '''INSERT INTO %s (thumbnail_id, hour, loads, clicks)
                    VALUES (?, ?, ?, ?) ''' % options.stats_table,
                    (img_id, hourdate, loads, clicks))
            else:
                self.statscursor.execute(
                    '''UPDATE %s set loads=?, clicks=? where
                    thumbnail_id = ? and hour = ?''' %
                    options.stats_table,
                    (loads + result[0], clicks + result[1], img_id, hourdate))
        else:
            self.statscursor.execute(
                '''REPLACE INTO %s (thumbnail_id, hour, loads, clicks) 
                VALUES (?, ?, ?, ?) ''' % options.stats_table,
                (img_id, hourdate, loads, clicks))

    def statsdb_connect(self):
        try:
            self.statsdb = sqldb.connect(
                user=options.stats_user,
                passwd=options.stats_pass,
                host=options.stats_host,
                port=options.stats_port,
                db=options.stats_db)
        except sqldb.Error as e:
            _log.exception('Error connecting to stats db: %s' % e)
            raise
        self.statscursor = self.statsdb.cursor()

        stats.db.create_tables(self.statscursor)

    def statsdb_disconnect(self):
        self.statscursor.close()
        self.statsdb.commit()
        self.statsdb.close()

    def steps(self):
        return [
            self.mr(mapper=self.mapper_get_events,
                    combiner=self.reducer_count_events,
                    reducer=self.reducer_count_events),
            self.mr(mapper=self.map_thumbnail_url2id,
                    mapper_init=self.videodb_connect,
                    mapper_final=self.videodb_disconnect,
                    reducer=self.reducer_count_events),
            self.mr(mapper=self.merge_events,
                    reducer_init=self.statsdb_connect,
                    reducer=self.reducer_write2db,
                    reducer_final=self.statsdb_disconnect)]
