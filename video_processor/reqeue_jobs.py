#!/usr/bin/env python
'''
Script that manually reqeues the jobs that are still processing
according to the database

NOTE: Use this script only to requeue all requests that have failed
to process. It will requeue request that are in the following state
SUBMIT, PROCESSING, FAILED, Requeue 

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
from cmsdb import neondata
import urllib2
import utils.neon

from utils.options import define, options
define('host', default='localhost',
       help='Host where the video processing server is')
define('port', default=8081, type=int,
       help='Port where the video processing server is')
define('api_key', default=None, type=str, help='account api key')

_log = logging.getLogger(__name__)

def main():
    if options.api_key is not None:
        accounts = [neondata.NeonUserAccount.get(options.api_key)]
    else:
        accounts = neondata.NeonUserAccount.get_all()


    for account in accounts:
        for request in account.iterate_all_jobs():
            if request.state in [neondata.RequestState.SUBMIT,
                                 neondata.RequestState.PROCESSING, 
                                 neondata.RequestState.REQUEUED,
                                 neondata.RequestState.FAILED,
                                 neondata.RequestState.INT_ERROR]:
                url = 'http://%s:%s/requeue' % (options.host, options.port)

                try:
                    response = urllib2.urlopen(url, request.to_json())
                    if response.code == 200:
                        _log.info('Requeued request %s for account %s' %
                              (request.job_id, request.api_key))
                    else:
                        _log.error('Could not requeue %s for account %s' %
                                   (request.job_id, request.api_key))
                except urllib2.HTTPError as e:
                    _log.error('Could not requeue %s for account %s: %s' %
                                   (request.job_id, request.api_key, e))

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
