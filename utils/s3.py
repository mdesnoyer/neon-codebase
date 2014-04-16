''' Utilities for talking to S3

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import boto.s3.connection
from boto.exception import S3ResponseError
import logging
import time
from utils.options import define, options

define('s3port', type=int, default=None, help='Port to connect to S3 on')
define('s3host', default=boto.s3.connection.S3Connection.DefaultHost,
       help='Host where S3 is being served from')

_log = logging.getLogger(__name__)

def S3Connection(*args, **kwargs):
    '''Same as boto's S3Connection except using the neon defined parameters.'''
    
    if options.s3host == 'localhost':
        # We are talking to a fake S3 server, which needs a couple of
        # extra options.
        kwargs['is_secure'] = False
        kwargs['calling_format'] = boto.s3.connection.OrdinaryCallingFormat()

    kwargs['port'] = options.s3port
    kwargs['host'] = options.s3host
    return boto.s3.connection.S3Connection(*args, **kwargs)

def set_contents_from_string(key, data, headers, retries=3):
    '''
    @returns: None on s3 error, <n on partial write, n bytes written on success 
    @key : boto.s3.key.Key object

    usage:
    s3bucket = s3conn.get_bucket(s3bucket_name)
    k = s3bucket.new_key(keyname)
    s3.set_contents_from_string(k, data, headers)
    '''
    cur_try = 0
    done = False
    ret = None
    while (cur_try < retries):
        cur_try += 1
        try:
            ret = key.set_contents_from_string(data, headers)
            if not ret: 
                time.sleep(0.2)
                continue
            else:
                return ret
        except S3ResponseError, e:
            #Retry if there is an exception
            continue
        #TODO:Sunil : Are there any other exceptions possible ?    
