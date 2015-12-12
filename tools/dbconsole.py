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
import boto.opsworks
from cmsdb.neondata import *
import code
import ipdb
import random
import signal
import socket
import subprocess
import utils.neon
import utils.ps

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("aws_region", default="us-east-1", type=str,
       help="Region to look for the production db")
define("layer_name", default="dbslave", type=str,
       help="Layer shortname for the db")
define("stack_name", default="Neon Serving Stack V2",
       help="Name of the stack")
define("redis_port", default=6379,
       help="Port where redis is listening on the host")
define("forward_port", default=1, help="If 1, then setup port forwarding")

def find_db_address():
    conn = boto.opsworks.connect_to_region(options.aws_region)

    # Find the stack
    stack_id = None
    for stack in conn.describe_stacks()['Stacks']:
        if stack['Name'] == options.stack_name:
            stack_id = stack['StackId']
            break
    if stack_id is None:
        raise Exception('Could not find stack %s' % options.stack_name)

    # Find the layer
    layer_id = None
    for layer in conn.describe_layers(stack_id = stack_id)['Layers']:
        if layer['Shortname'] == options.layer_name:
            layer_id = layer['LayerId']
            break
    if layer_id is None:
        raise Exception('Could not find layer %s in stack %s' %
                        (options.layer_name, options.stack_name))

    # Find the instance ip
    ip = None
    for instance in conn.describe_instances(layer_id=layer_id)['Instances']:
        try:
            ip = instance['PrivateIp']
            break
        except KeyError:
            pass
    _log.info('Found db at %s' % ip)
    return '10.0.89.251'

def forward_port(local_port):
    cmd = ('ssh -L {local}:localhost:{rem} {db_addr}').format(
               local=local_port,
               rem=options.redis_port,
               db_addr=find_db_address())
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

    random.seed()
    proc = None
    if options.forward_port:
        db_address = 'localhost'
        db_port = random.randint(10000, 12000)
        proc = forward_port(db_port)
    else:
        db_address = find_db_address()
        db_port = options.redis_port
    try:
        # Set the options for connecting to the db
        options._set('cmsdb.neondata.dbPort', db_port)
        options._set('cmsdb.neondata.accountDB', db_address)
        options._set('cmsdb.neondata.thumbnailDB', db_address)
        options._set('cmsdb.neondata.videoDB', db_address)

        # Enter the console
        ipdb.set_trace()
        #code.interact(local=locals())

    finally:
        if proc is not None:
            proc.terminate()
    
