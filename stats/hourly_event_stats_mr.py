#!/usr/bin/env python
'''Map reduce jobs for processing the stats

Aggregates stats on an hourly basis for each thumbnail. Also converts
the external url to an internal thumbnail id.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import json
from mrjob.job import MRJob
import mysql.connector as sqldb
import time
import urllib
import urllib2

class HourlyEventStats(MRJob):
    INPUT_PROTOCOL = RawValueProtocol
    INTERNAL_PROTOCOL = PickleProtocol

    def __init__(self):
        super(HourlyEventStats, self).__init__()
        self.statsdb = None
    
    def configure_options(self):
        super(HourlyEventStats, self).configure_options()
        self.add_passthrough_option( '--stats_host',
            default='stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com',
            help='Host of the stats database')
        self.add_passthrough_option( '--stats_port', type='int',
                                     default=3306,
                                     help='Port to the stats database')
        self.add_passthrough_option('--stats_user', default='mrwriter',
                                    help='User for the stats database')
        self.add_passthrough_option('--stats_pass', default='kjge8924qm',
                                    help='Password for the stats database')
        self.add_passthrough_option('--stats_db', default='stats',
                                    help='Stats database to connect to')
        self.add_passthrough_option('--increment_stats', action='store_true',
            default='False',
            help='If true, stats are incremented. Otherwise, they are overwritten')
        self.add_passthrough_option('--stats_table', default='hourly_events',
                                    help='Table in the stats database to write to')
        self.add_passthrough_option('--videodb_url',
                                    default='http://localhost:8080',
                                    help='url for the video database call')
        
            

    def mapper_get_events(self, _, line):
        data = json.load(line)
        hour = data.s_ts / 3600
        if data.a == 'load':
            for img in data.imgs:
                if img is not None:
                    yield (('load', img, hour),  1)
        elif data.a == 'click':
            yield(('click', data.img, hour), 1)

    def reducer_count_events(self, event, counts):
        yield (event, sum(counts))

    def videodb_connect(self):
        # We're not talking to a true database at the moment
        pass

    def videodb_disconnect(self):
        # We're not talking to a true database at the moment
        pass

    def map_thumbnail_url2id(self, event, count):
        '''Maps from the external thumbnail url to our internal id.'''
        stream = urllib2.urlopen(self.options.videodb_url,
                                 urllib.urlencode({'url':event[1]}),
                                 60)
        event[1] = stream.read().strip()
        yield event, count

    def merge_events(self, event, count):
        yield ((event[1], event[2]), (count, event[0]))

    def reducer_write2db(self, img_hr, count_events):
        '''Writes the event counts to the database.

        Inputs:
        img_hr - (img_id, hours since epoch)
        count_events - [(count, event name)]
        '''
        img_id, hours = img_hr
        counts = {}
        for count, event in count_events:
            counts[event] = count
        loads = counts.setdefault('load', 0)
        clicks = counts.setdefault('click', 0)
        hourdate = time.gmtime(hours * 3600)

        self.statscursor.execute(
            'INSERT INTO ? (thumbnail_id, hour, loads, clicks) '
            'VALUES (?, ?, ?, ?) '
            'ON DUPLICATE KEY UPDATE loads=loads+?, clicks=clicks+?',
            (self.options.stats_table,
             img_id, hourdate, loads, clicks, loads, clicks))

    def statsdb_connect(self):
        self.statsdb = sqldb.connect(
            user=self.options.stats_user,
            password=self.options.stats_pass,
            host=self.options.stats_host,
            port=self.options.stats_port,
            database=self.options.stats_db)
        self.statscursor = self.statsdb.cursor()

        if not self.options.increment_stats:
            self.statscursor.execute('DROP TABLE IF EXISTS ?',
                                     (self.options.stats_table,))

        self.statscursor.execute('CREATE TABLE IF NOT EXISTS ? ('
                                 'thumbail_id VARCHAR(32) NOT NULL,'
                                 'hour DATETIME NOT NULL,'
                                 'loads INT NOT NULL DEFAULT 0,'
                                 'clicks INT NOT NULL DEFAULT 0,'
                                 'UNIQUE KEY (thumbnail_id, hour))',
                                 (self.options.stats_table,))

    def statsdb_disconnect(self):
        self.statscursor.commit()
        self.statscursor.close()
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
            
        

if __name__ == '__main__':
    HourlyEventStats.run()
