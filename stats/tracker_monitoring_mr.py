#!/usr/bin/env python
'''Map reduce jobs to monitor the state of the tracker on customers sites

This set of jobs will produce two things.

1) A mysql table where each row is the page and the last time we saw
traffic from it in the logs

2) Each time we see traffic from a new Neon Account, we report that to
an external system

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
import redis.exceptions
import stats.db
from supportServices import neondata
import time
import utils.neon
import utils.options
import urllib2
import urlparse

_log = logging.getLogger(__name__)

class TrackerMonitoring(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol 

    def configure_options(self):
        super(TrackerMonitoring, self).configure_options()
        self.add_passthrough_option(
            '--host',
            default='stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com',
            help='Host of the database')
        self.add_passthrough_option(
            '--port', type='int', default=3306,
            help='Port to the database')
        self.add_passthrough_option(
            '--user', default='mrwriter',
            help='User for the database')
        self.add_passthrough_option('--password', default='kjge8924qm',
                                    help='Password for the database')
        self.add_passthrough_option('--db', default='stats_dev',
                                    help='Database to connect to')
        self.add_passthrough_option('--notify_host',
                                    default='api.neon-lab.com',
                                    help='Host to notify of new analytics')
        self.add_file_option(
            '--neon_config', default=None,
            help='Config file to parse for Neon options.')

    def load_options(self, args):
        super(TrackerMonitoring, self).load_options(args)

        if (self.options.neon_config is not None and 
            not utils.options.options_loaded()):
            with open(self.options.neon_config) as f:
                utils.options.parse_options(args=[], config_stream=f)

    def map_parse_logs(self, line, _):
        '''Reads the json log and outputs the time of the event.

        In particular, the output is:

        ((event_type, page, tracker_account), time)
        '''
        try:
            data = json.loads(line)
            # Ignore old html5 bc player because the data isn't
            # complete from it
            if data['ttype'] == 'html5':
                self.increment_counter(
                    'TrackerMonitoringErrors',
                    'HTML5_bc_click' if data['a'] == 'click' else 
                    'HTML5_bc_load',
                    1)
                return

            # Verify that the data is valid
            if data['a'] not in ['click', 'load']:
                raise TypeError('Invalid action: %s' % data['a'])

            # Normalize the page
            url_chunks = urlparse.urlparse(data['page'])
            if url_chunks.scheme == '':
                url = url_chunks.path
            else:
                url = ''.join((url_chunks.netloc, url_chunks.path))

            yield ((data['a'], url, data['tai']), int(data['sts']))
            
        except ValueError as e:
            _log.error('JSON could not be parsed: %s' % line)
            self.increment_counter('TrackerMonitoringErrors',
                                   'JSONParseErrors', 1)
        except KeyError as e:
            _log.error('Input data was missing a necessary field (%s): %s' % 
                       (e, line))
            self.increment_counter('TrackerMonitoringErrors',
                                   'JSONFieldMissing', 1)

        except TypeError as e:
            _log.error('Data is invalid %s' % e)
            self.increment_counter('TrackerMonitoringErrors',
                                    'InvalidData', 1)

    def reduce_choose_latest(self, key, times):
        yield (key, max(times))

    def map_find_neon_account_id(self, key, time):
        '''Converts the tracker account id to the neon account id.

        Expects: ((event_type, page, tracker_account), time)

        Yields: ((event_type, page, neon_account_id), time)
        '''
        try:
            neon_account_id = \
              neondata.TrackerAccountIDMapper.get_neon_account_id(key[2])
        except redis.exceptions.RedisError as e:
            _log.exception('Error getting data from Redis server: %s' % e)
            self.increment_counter('TrackerMonitoringErrors',
                                   'RedisErrors', 1)
            return
        
        if neon_account_id:
            yield ((key[0], key[1], neon_account_id), time)
        else:
            _log.warn('Invalid tracker account id %s. Skipping' % key[2])
            self.increment_counter('TrackerMonitoringErrors',
                                   'InvalidTAI', 1)

    def connect_to_db(self):
        try:
            self.db_conn = sqldb.connect(
                user=self.options.user,
                passwd=self.options.password,
                host=self.options.host,
                port=self.options.port,
                db=self.options.db)
        except sqldb.Error as e:
            _log.exception('Error connecting to stats db: %s' % e)
            raise
        self.cursor = self.db_conn.cursor()

        stats.db.create_tables(self.cursor)

    def map_update_database(self, key, time):
        '''Update the database of when we last saw logs from a page.

        Expects: ((event_type, page, neon_account_id), time)
        '''
        event_type, page, neon_account_id = key
        time = datetime.utcfromtimestamp(time)
        event_col = 'last_click' if event_type == 'click' else 'last_load'
        
        stats.db.execute(self.cursor,
                         '''SELECT last_click, last_load from %s
                         where neon_acct_id = %%s and page = %%s''' %
                         stats.db.get_pages_seen_table(),
                         (neon_account_id, page))
        result = self.cursor.fetchone()
        if result is None:
            # There isn't an entry for this page yet so see if there
            # is an entry from this account id
            stats.db.execute(self.cursor,
                             '''SELECT * from %s where neon_acct_id = %%s
                             limit 1''' %
                             stats.db.get_pages_seen_table(),
                             (neon_account_id, ))
            found_account_entry = self.cursor.fetchone()
            if found_account_entry is None:
                # Notify the external system that this is a new
                # account we're getting data from.
                try:
                    response = urllib2.urlopen(urllib2.Request(
                        'http://%s/accounts/%s/analytics_received' %
                        (self.options.notify_host, neon_account_id),
                        headers={'X-Neon-API-Key': 
                                 neondata.NeonApiKey.generate(
                                     neon_account_id)}))
                    if response.getcode() != 200:
                        raise urllib2.URLError(
                            'Problem notifying. Error code: %i' %
                            response.getcode())
                except urllib2.URLError as e:
                    _log.exception('Error notifying external system we have '
                                   'a new account: %s' % e)
                    self.increment_counter('TrackerMonitoringErrors',
                                           'NoNotificationSent', 1)

                # Turn on A/B testing for this account
                try:
                    account = neondata.NeonUserAccount.get_account(
                        neondata.NeonApiKey.generate(neon_account_id))
                    for platform in account.get_platforms():
                        platform.abtest = True
                        platform.save()
                except redis.exceptions.RedisError as e:
                    _log.error('Error turning on the A/B test for account %s'
                               ': %s' % (neon_account_id, e))
                    self.increment_counter('TrackerMonitoringErrors',
                                           'RedisError', 1)

            # Insert a new entry into the database
            stats.db.execute(self.cursor,
                             '''INSERT INTO %s (%s, neon_acct_id, page)
                             VALUES (%%s, %%s, %%s)''' %
                             (stats.db.get_pages_seen_table(), event_col),
                             (time, neon_account_id, page))

        else:
            # There is already an entry from this page and account id,
            # so update it.
            stats.db.execute(self.cursor,
                             '''UPDATE %s set %s=%%s where neon_acct_id=%%s
                             and page=%%s''' % 
                             (stats.db.get_pages_seen_table(), event_col),
                             (time, neon_account_id, page))
                

    def disconnect_db(self):
        self.cursor.close()
        self.db_conn.commit()
        self.db_conn.close()

    def steps(self):
        return [
            self.mr(mapper=self.map_parse_logs,
                    combiner=self.reduce_choose_latest,
                    reducer=self.reduce_choose_latest),
            self.mr(mapper=self.map_find_neon_account_id),
            self.mr(mapper=self.map_update_database,
                    mapper_init=self.connect_to_db,
                    mapper_final=self.disconnect_db)]

def main():
    # Setup a logger for dumping errors to stderr
    utils.logs.CreateLogger(__name__, stream=sys.stderr,
                            level=logging.WARNING)
    
    TrackerMonitoring.run()

if __name__ == '__main__':
    main()
