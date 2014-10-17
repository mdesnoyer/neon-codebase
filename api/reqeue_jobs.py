#!/usr/bin/env python
'''Script that manually reqeues the jobs that are still processing
according to the database

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import logging
from supportServices import neondata
import urllib2
import utils.neon

from utils.options import define, options
define('host', default='localhost',
       help='Host where the video processing server is')
define('port', default=8081, type=int,
       help='Port where the video processing server is')

_log = logging.getLogger(__name__)

def main():
    db_connection = neondata.DBConnection('NeonApiRequest')

    # Get the request keys
    keys = db_connection.blocking_conn.keys('request_*')
    for key in keys:
        request_json = db_connection.blocking_conn.get(key)
        request = neondata.NeonApiRequest.create(request_json)

        #If request state is submitted, being processed, Requeued or Failed
        if request.state in [neondata.RequestState.SUBMIT, neondata.RequestState.PROCESSING, 
                neondata.RequestState.REQUEUED, neondata.RequestState.FAILED]:
            url = 'http://%s:%s/requeue' % (options.host, options.port)
            response = urllib2.urlopen(url, request.to_json())

            if response.code != 200:
                _log.error('Could not requeue %s' % request.__dict__)

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
