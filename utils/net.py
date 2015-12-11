'''
Utilities to deal with networking

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import socket

_log = logging.getLogger(__name__)

def get_local_ip():
    '''Returns the local ip address that can be used to reach this machine.

    Will not return 127.0.x.x, but will return a 10. address for example.

    The machine must be connected to the internet for this to work.
    '''

    # Connecting to a UDP address doesn't send packets
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(('8.8.8.8', 0))
        return sock.getsockname()[0]
    except socket.error as e:
        if e.errno == 101:
            # Network unreachable
            return '127.0.0.1'
        raise
    finally:
        sock.close()
