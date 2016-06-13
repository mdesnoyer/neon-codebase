#!/usr/bin/env python
'''
A script that can be used to get a console that talks to the production
database.
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
from cmsdb.neondata import *
import code
import ipdb
import random
import signal
import socket
import subprocess
import utils.neon
import utils.prod
import utils.ps

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("write", default=0, type=int,
       help='If 1, then the database is writeable')
define("id", default="postgres-prod",
       help="id of the database to talk to")
define("hopserver", default="hopserver_us_east",
       help="Hop server to use")

def forward_port(local_port, db_host, db_port):
    cmd = ('ssh -L {local}:{db_host}:{db_port} {hopserver}').format(
               local=local_port,
               db_host=db_host,
               db_port=db_port,
               hopserver=options.hopserver)
    _log.info('Forwarding port %s to the database' % local_port)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            stdin=subprocess.PIPE)
    # Wait until we can connect to the db
    for i in range(12):
        try:
            sock = socket.create_connection(('localhost', local_port), 3600)
        except socket.error:
            time.sleep(5)
    if sock is None:
        raise Exception('Could not connect to the database')
    
    _log.info('Connection made to the database')
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
    
    return proc


if __name__ == '__main__':
    utils.neon.InitNeon()
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    dbhost, dbport = utils.prod.find_rds_host(options.id)

    local_port = random.randint(10000, 12000)
    proc = forward_port(local_port, dbhost, dbport)

    try:
        # Set the options for connecting to the db
        options._set('cmsdb.neondata.db_address', 'localhost')
        options._set('cmsdb.neondata.db_port', local_port)

        if options.id == 'postgres-test':
            user_var = 'CMSDB_TEST_USER'
            pass_var = 'CMSDB_TEST_PASS'
        elif options.write:
            user_var = 'CMSDB_WRITE_USER'
            pass_var = 'CMSDB_WRITE_PASS'
        else:
            user_var = 'CMSDB_READ_USER'
            pass_var = 'CMSDB_READ_PASS'
        
        if os.environ.get(user_var):
            options._set('cmsdb.neondata.db_user',
                         os.environ.get(user_var))

        if os.environ.get(pass_var):
            options._set('cmsdb.neondata.db_password',
                         os.environ.get(pass_var))

        # Enter the console
        ipdb.set_trace()

    finally:
        proc.terminate()


    
