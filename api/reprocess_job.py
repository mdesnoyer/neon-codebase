#!/usr/bin/env python
'''
Script that manually reprocesses a job.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
from supportServices import neondata
import urllib2
import utils.neon

from utils.options import define, options
define('host', default='localhost',
       help='Host where the video processing server is')
define('port', default=8081, type=int,
       help='Port where the video processing server is')
define('api_key', default=None, type=str, help='account api key')
define('job_id', default=None, type=str, help='job id')

_log = logging.getLogger(__name__)

def main():
    # TODO(mdesnoyer): Use boto to lookup server addresses
    
    request = neondata.NeonApiRequest.get(options.job_id, options.api_key)
    if request is None:
        _log.error('Could not find job %s for account %s' % 
                   (options.job_id, options.api_key))
        
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
