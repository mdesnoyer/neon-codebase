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
import controllers.brightcove_controller
import logging
import mastermind.server
import mysql.connector as sqldb
import multiprocessing
import os
import re
import signal
import stats.stats_processor
import subprocess
import supportServices.services
from supportServices import neondata
import tempfile
import time
import unittest
import utils.neon

from utils.options import define, options

define('stats_db', help='Name of the stats database to talk to',
       default='serving_tester')
define('stats_db_user', help='User for the stats db connection',
       default='neon')
define('stats_db_pass', help='Password for the stats db connection',
       default='neon')

_log = logging.getLogger(__name__)

class TestServingSystem(unittest.TestCase):

    def setUp(self):
        #TODO: Clear the databases

        #TODO: Add a couple of bare bones entries to the video db?

        #TODO: Setup endpoint to capture directives from mastermind
        pass

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

def MultiprocWrapper(object):
    '''A wrapper to make a multiprocess.Process look like a subprocess.Process.

    '''
    def __init__(self, proc):
        self.proc = proc

    def __getattr__(self, name):
        return self.proc.__getattr__(name)

    def poll(self):
        return self.proc.exitcode

    def kill(self):
        os.kill(self.proc.pid, signal.SIGKILL)

def LaunchStatsDb():
    '''Launches the stats db, which is a mysql interface.

    Makes sure that the database is up.
    '''
    _log.info('Connecting to stats db')
    try:
        conn = sqldb.connect(user=options.stats_db_user,
                             password=options.stats_db_pass,
                             host='localhost',
                             database=options.stats_db)
    except sqldb.Error as e:
        _log.error(('Error connection to stats db. Make sure that you '
                    'have a mysql server running locally and that it has '
                    'a database name: %s with user: %s and pass: %s. ') 
                    % (options.stats_db, options.stats_db_user,
                       options.stats_db_pass))
        raise
    _log.info('Connection to stats db is good')

def LaunchVideoDb():
    '''Launches the video db.'''

    _log.info('Launching video db')
    proc = subprocess.Popen([
        '/usr/bin/env', 'redis-server',
        os.path.join(os.path.dirname(__file__), 'test_video_db.conf')],
        stdout=subprocess.PIPE)
    _subprocs.append(proc)

    # Wait until the db is up correctly
    upRe = re.compile('The server is now ready to accept connections on port')
    video_db_log = []
    for line in proc.stdout:
        video_db_log.append(line)
        if upRe.search(line):
            break

    if proc.poll() is not None:
        raise Exception('Error starting video db. Log:\n%s' %
                        '\n'.join(video_db_log))

    _log.info('Video db is up')

def LaunchSupportServices():
    _log.info('Launching Support Services')
    proc = multiprocessing.Process(target=supportServices.services.main)
    proc.daemon = True
    proc.start()

def LaunchMastermind():
    _log.info('Launching Mastermind')
    proc = multiprocessing.Process(target=mastermind.server.main)
    proc.daemon = True
    proc.start()

def LaunchABControllers():
    _log.info('Launching brightcove controller')
    proc = multiprocessing.Process(
        target=controllers.brightcove_controller.main)
    proc.daemon = True
    proc.start()

def LaunchClickLogServer():
    _log.info('Launching click log server')
    proc = multiprocessing.Process(
        target=clickTracker.clickLogServer.main)
    proc.daemon = True
    proc.start()

def LaunchStatsProcessor():
    _log.info('Launching stats processor')
    proc = multiprocessing.Process(
        target=stats.stats_processor.main)
    _subprocs.append(MultiprocWrapper(proc))
    proc.start()

_subprocs = []
def shutdown(subprocs):
    import logging
    import time
    _log = logging.getLogger(__name__)
    _log.warn('Shutting down now')
    for proc in subprocs:
        try:
            proc.terminate()
        except OSError as e:
            if e.errno <> 3:
                raise

    still_running = True
    count = 0
    while still_running and count < 20:
        still_running = False
        for proc in subprocs:
            if proc.poll() is None:
                still_running = True
        time.sleep(1)
        count += 1

    if still_running:
        for proc in subprocs:
            if proc.poll() is None:
                _log.error('Process %i not down. Killing' % proc.pid)
                proc.kill()
    _log.info('Done shutting down')
    sys.exit(0)

def main():    
    signal.signal(signal.SIGTERM, sys.exit)
    atexit.register(shutdown, _subprocs)

    LaunchStatsDb()
    LaunchVideoDb()
    LaunchSupportServices()
    LaunchMastermind()
    LaunchABControllers()
    LaunchClickLogServer()
    LaunchStatsProcessor()

    time.sleep(100000)
    unittest.main()
    

if __name__ == "__main__":
    utils.neon.InitNeonTest()
    main()

