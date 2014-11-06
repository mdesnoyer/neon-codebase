'''
Tools for using boto to interact with s3

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
    
import concurrent.futures
import logging
from utils.options import define, options

_log = logging.getLogger(__name__)

define('aws_pool_size', type=int, default=10,
       help='Number of processes that can talk simultaneously to aws')

__boto_thread_pool = concurrent.futures.ThreadPoolExecutor(
    options.aws_pool_size)

def run_async(func, *args, **kwargs):
    '''Runs a boto function asynchronously.

    e.g. in tornado
    s3conn = S3Connection()
    bucket = yield utils.boto.run_async(s3conn.get_bucket, 'my_bucket')

    Returns a future that can be used to watch the async results
    '''
    return __boto_thread_pool.submit(func, *args, **kwargs)
