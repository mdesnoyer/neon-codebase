#!/usr/bin/env python
'''
Script that manually reprocesses many jobs for an account.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
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
define('states', default='failed,internal_error', type=str, 
       help=('Comma separated list of states of videos to reprocess. '
             'A special value of "all" will make all them be reprocessed'))
define('excluded_jobs', default=None, type=str,
       help='File with job ids, one per line to exclude')

_log = logging.getLogger(__name__)

def main():
    # TODO(mdesnoyer): Use boto to lookup server addresses and setup a tunnel
    valid_states = options.states.split(',')
    _log.info('Will reprocess all videos in account %s with states %s' %
              (options.api_key, valid_states))

    if options.excluded_jobs is None:
        excluded_jobs = []
    else:
        excluded_jobs = set([x.trim() for x in open(options.excluded_jobs)])
    
    acct = neondata.NeonUserAccount.get(options.api_key)
    for platform in acct.get_platforms():
        for job_id in platform.videos.values():
            if job_id in excluded_jobs:
                continue
            
            request = neondata.NeonApiRequest.get(job_id, options.api_key)
            if request is None:
                _log.error('Could not find job %s for account %s' % 
                           (options.job_id, options.api_key))
                continue

            if options.states != 'any' and request.state not in valid_states:
                continue
        
            url = 'http://%s:%s/reprocess' % (options.host, options.port)

            try:
                response = urllib2.urlopen(url, request.to_json())
                if response.code == 200:
                    _log.info('Reprocessing %s for account %s' %
                              (request.job_id, request.api_key))
                else:
                    _log.error('Could not reprocess %s for account %s' %
                               (request.job_id, request.api_key))
            except urllib2.HTTPError as e:
                _log.error('Could not reprocess %s for account %s: %s' %
                           (request.job_id, request.api_key, e))

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
