#!/usr/bin/env python
'''This script fires up the serving system and runs end to end tests.

***WARNING*** This test does not run completely locally. It interfaces
   with some Amazon services and will thus incure some fees. Also, it
   cannot be run from multiple locations at the same time.

TODO(mdesnoyer): Enable a version of this to run locally with out
Amazon dependencies.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import atexit
import clickTracker.clickLogServer
import clickTracker.logDatatoS3
from datetime import datetime
import json
import logging
import mastermind.server
import MySQLdb as sqldb
import multiprocessing
import os
import re
import signal
import SimpleHTTPServer
import SocketServer
import stats.db
import stats.stats_processor
import subprocess
import supportServices.services
from supportServices import neondata
import tempfile
import time
import tornado.httpserver
import tornado.ioloop
import tornado.web
import unittest
import utils.neon
import utils.ps

from utils.options import define, options

define('stats_db', help='Name of the stats database to talk to',
       default='serving_tester')
define('stats_db_user', help='User for the stats db connection',
       default='neon')
define('stats_db_pass', help='Password for the stats db connection',
       default='neon')
define('bc_directive_port', default=7212,
       help='Port where the brightcove directives will be output')

_log = logging.getLogger(__name__)

class TestServingSystem(unittest.TestCase):

    def setUp(self):
        _log.info('Starting the directive capture server on port %i' 
                  % options.bc_directive_port)
        directive_cap = DirectiveCaptureProc(options.bc_directive_port)
        self.directive_q = directive_cap.q
        directive_cap.start()
        directive_cap.wait_until_running()

        # Clear the stats database
        conn = sqldb.connect(user=options.stats_db_user,
                             passwd=options.stats_db_pass,
                             host='localhost',
                             db=options.stats_db)
        self.statscursor = conn.cursor()
        stats.db.execute(self.statscursor,
                         '''DELETE from hourly_events''')
        stats.db.execute(self.statscursor,
                         '''DETELE from last_update''')
        stats.db.execute(
            self.statscursor,
            '''REPLACE INTO last_update (tablename, logtime) VALUES (?, ?)''',
            ('hourly_events', datetime.utcfromtimestamp(0)))
        
        #TODO: Clear the video database

        #TODO: Add a couple of bare bones entries to the video db?

    def tearDown(self):
        pass

    def simulateLoads(self, n_loads, thumbs, target_ctr):
        '''Simulate a set of loads and clicks

        n_loads - Number of player-like loads to generate
        thumbs - List of thumbnail urls
        target_str - List of target CTR for each thumb.
        '''
        pass

    def assertDirectiveCaptured(self, directive, timeout=30):
        '''Verifies that a given directive is received.

        Inputs:
        directive - (video_id, [(thumb_id, frac)])
        timeout - How long to wait for the directive
        '''
        pass

    def getStats(self, video_id, thumb_id):
        '''Retrieves the statistics about a given thumb.

        Inputs:
        video_id - Video id
        thumb_id - Thumbnail id

        Outputs:
        (loads, clicks)
        '''
        pass
        

    #TODO: Add helper functions to add stuff to the video database?

    #TODO: Write the actual tests

def DirectiveCaptureProc(multiprocessing.Process):
    '''A mini little http server that captures mastermind directives.

    The directives are shoved into a multiprocess Queue after being parsed.
    '''
    def __init__(self, port):
        super(DirectiveCapture, self).__init__()
        self.port = port
        self.q = multiprocessing.Queue()
        self.is_running = multiprocessing.Event()

    def wait_until_running(self):
        '''Blocks until the data is loaded.'''
        self.is_running.wait()

    def run():
        application = tornado.web.Application([
            (r'/', DirectiveCaptureHandler, dict(q=self.q))])
        server = tornado.httpserver.HTTPServer(application)
        utils.ps.register_tornado_shutdown(server)
        server.listen(self.port)
        self.is_running.set()
        tornado.ioloop.IOLoop.instance().start()

def DirectiveCaptureHandler(tornado.web.RequestHandler):
    def initialize(self, q):
        self.q = q

    def post(self):
        data = json.loads(self.request.body)
        self.q.put(data['d'])

def LaunchStatsDb():
    '''Launches the stats db, which is a mysql interface.

    Makes sure that the database is up.
    '''
    _log.info('Connecting to stats db')
    try:
        conn = sqldb.connect(user=options.stats_db_user,
                             passwd=options.stats_db_pass,
                             host='localhost',
                             db=options.stats_db)
    except sqldb.Error as e:
        _log.error(('Error connection to stats db. Make sure that you '
                    'have a mysql server running locally and that it has '
                    'a database name: %s with user: %s and pass: %s. ') 
                    % (options.stats_db, options.stats_db_user,
                       options.stats_db_pass))
        raise

    cursor = conn.cursor()
    stats.db.create_tables(cursor)
    
    _log.info('Connection to stats db is good')

def LaunchVideoDb():
    '''Launches the video db.'''

    _log.info('Launching video db')
    proc = subprocess.Popen([
        '/usr/bin/env', 'redis-server',
        os.path.join(os.path.dirname(__file__), 'test_video_db.conf')],
        stdout=subprocess.PIPE)

    # Wait until the db is up correctly
    upRe = re.compile('The server is now ready to accept connections on port')
    video_db_log = []
    while proc.poll() is None:
        line = proc.stdout.readline()
        video_db_log.append(line)
        if upRe.search(line):
            break

    if proc.poll() is not None:
        raise Exception('Error starting video db. Log:\n%s' %
                        '\n'.join(video_db_log))

    _log.info('Video db is up')

def LaunchSupportServices():
    proc = multiprocessing.Process(target=supportServices.services.main)
    proc.start()
    _log.info('Launching Support Services with pid %i' % proc.pid)

def LaunchMastermind():
    proc = multiprocessing.Process(target=mastermind.server.main)
    proc.start()
    _log.info('Launching Mastermind with pid %i' % proc.pid)

def LaunchClickLogServer():
    proc = multiprocessing.Process(
        target=clickTracker.clickLogServer.main)
    proc.start()
    _log.info('Launching click log server with pid %i' % proc.pid)

    proc = multiprocessing.Process(
        target=clickTracker.logDatatoS3.main)
    proc.start()
    _log.info('Launching s3 data pusher with pid %i' % proc.pid)

def LaunchStatsProcessor():
    proc = multiprocessing.Process(
        target=stats.stats_processor.main)
    proc.start()
    _log.info('Launching stats processor with pid %i' % proc.pid)

def main():
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    atexit.register(utils.ps.shutdown_children)

    LaunchStatsDb()
    LaunchVideoDb()
    LaunchSupportServices()
    LaunchMastermind()
    LaunchClickLogServer()
    LaunchStatsProcessor()

    time.sleep(1)

    suite = unittest.TestLoader().loadTestsFromTestCase(TestServingSystem)
    result = unittest.TextTestRunner().run(suite)

    if result.wasSuccessful():
        sys.exit(0)
    else:
        sys.exit(1)
    

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

