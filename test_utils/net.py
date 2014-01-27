'''
Utilities to deal with networking in tests

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import random
import socket

_log = logging.getLogger(__name__)

def find_free_port():
    '''Finds a free port which can safely be used to bind to.'''
    rand_state = random.getstate()

    random.seed()

    free_port = None
    while free_port is None:
        port = random.randint(10000, 11000)

        # Check if the port is free by trying to connect to it
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', port))
        if result == 111:
            free_port = port
        else:
            sock.close()

    random.setstate(rand_state)
    return free_port
