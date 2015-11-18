#!/usr/bin/env python
'''
A script that can be used to more easily ssh to a production machine using
its hostname.


Author: Mark Desnoyer (desnoyer@neon-lab.com)
Date: July 2015
Copyright 2015 Neon Labs Inc.
'''
USAGE='%proc [options] <hostname>'

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
import boto.opsworks
import signal
import subprocess
import utils.neon
import utils.prod
import utils.ps

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("aws_region", default="us-east-1", type=str,
       help="Region to look for the production db")
define("stack_name", default=None, type=str,
       help="Name of the stack")

def ssh_to_host(hostname):
    cmd = ('ssh {ip_addr}').format(
        ip_addr=utils.prod.find_host_private_address(hostname,
                                                     options.stack_name,
                                                     options.aws_region))

    proc = subprocess.Popen(cmd, shell=True)
    proc.wait()
    

if __name__ == '__main__':
    argv = utils.neon.InitNeon(usage=USAGE)
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    ssh_to_host(argv[0])
