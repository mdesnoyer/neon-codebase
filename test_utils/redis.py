'''
Utilities to deal with redis in tests

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import logging
from . import net
import random
import re
import signal
import subprocess
from supportServices import neondata
import tempfile
import utils.ps
from utils.options import define, options

_log = logging.getLogger(__name__)

class RedisServer:
    '''A redis serving running in its own process.

    To use, create the object and then start() and stop() it. e.g.

    def setUp(self):
      self.redis = RedisServer(port)
      self.redis.start()

    def tearDown(self):
      self.redis.stop()

    def test_something(self):
      connect_to_redis(self.redis.port)
    '''
    
    def __init__(self, port=None):
        self.port = port
        if self.port is None:
            self.port = net.find_free_port()

    def start(self):
        ''' Start on a random port and set supportServices.neondata.dbPort '''

        # Clear the singleton instance
        # This is required so that we can use a new connection(port) 
        neondata.DBConnection.clear_singleton_instance()

        self.config_file = tempfile.NamedTemporaryFile()
        self.config_file.write('port %i\n' % self.port)
        self.config_file.flush()

        _log.info('Redis server on port %i' % self.port)

        self.proc = subprocess.Popen([
            '/usr/bin/env', 'redis-server',
            self.config_file.name],
            stdout=subprocess.PIPE)

        upRe = re.compile('The server is now ready to accept connections on '
                          'port')
        video_db_log = []
        while self.proc.poll() is None:
            line = self.proc.stdout.readline()
            video_db_log.append(line)
            if upRe.search(line):
                break

        if self.proc.poll() is not None:
            raise Exception('Error starting video db. Log:\n%s' %
                            '\n'.join(video_db_log))

        # Set the port for the most common place we use redis. If it's
        # not being used in the test, it won't hurt anything.
        self.old_port = options.get('supportServices.neondata.dbPort')
        options._set('supportServices.neondata.dbPort', self.port)
        

    def stop(self):
        ''' stop redis instance '''

        self.config_file.close()
        options._set('supportServices.neondata.dbPort', self.old_port)
        still_running = utils.ps.send_signal_and_wait(signal.SIGTERM,
                                                      [self.proc.pid],
                                                      timeout=8)
        if still_running:
            utils.ps.send_signal_and_wait(signal.SIGKILL,
                                          [self.proc.pid],
                                          timeout=10)
        
        self.proc.wait()
        _log.info('Redis server on port %i stopped' % self.port)

        # Clear the singleton instance
        # This is required so that the next test can use a new connection(port) 
        neondata.DBConnection.clear_singleton_instance()
