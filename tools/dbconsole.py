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


if __name__ == '__main__':
    utils.neon.InitNeon()
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    host, port = utils.prod.find_rds_host(options.id)
    
    # Set the options for connecting to the db
    options._set('cmsdb.neondata.db_address', host)
    options._set('cmsdb.neondata.db_port', port)

    if options.write:
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
    #code.interact(local=locals())


    
