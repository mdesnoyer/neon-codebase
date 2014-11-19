'''
Tools for using boto to interact with s3

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
    
import concurrent.futures
import logging
import multiprocessing
import os
from utils.options import define, options

_log = logging.getLogger(__name__)

define('aws_pool_size', type=int, default=10,
       help='Number of processes that can talk simultaneously to aws')

class AsyncManager(object):
    def __init__(self):
        self.pool = concurrent.futures.ThreadPoolExecutor(
            options.aws_pool_size)

    def run_async(self, func, *args, **kwargs):
        '''Runs a boto function asynchronously.

        e.g. in tornado
        s3conn = S3Connection()
        bucket = yield utils.boto.run_async(s3conn.get_bucket, 'my_bucket')

        Returns a future that can be used to watch the async results
        '''
        return self.pool.submit(func, *args, **kwargs)

# Dictionary of async managers, one per pid
_async_managers = {}
_manager_lock = multiprocessing.RLock()

def run_async(func, *args, **kwargs):
    '''Runs a boto function asynchronously.

    e.g. in tornado
    s3conn = S3Connection()
    bucket = yield utils.boto.run_async(s3conn.get_bucket, 'my_bucket')

    Returns a future that can be used to watch the async results
    '''
    with _manager_lock:
        try:
            manager = _async_managers[os.getpid()]
        except KeyError:
            manager = AsyncManager()
            _async_managers[os.getpid()] = manager
    return manager.run_async(func, *args, **kwargs)
