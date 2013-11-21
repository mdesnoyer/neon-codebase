#!/usr/bin/python
'''
This script repopulate the server queue
- Get all request keys from persisten DB, iterate through states 
    and find unprocessed ones. 

v2 - Persistent DB to maintain a queue(in redis) of the request IDs 
    that are unprocessed, to enable fast recovery
'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import redis as blockingRedis
import os
from supportServices.neondata import *
import urllib2 
import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define('dbhost', default='127.0.0.1', type=str, help='database host')
define('dbport', default=6379, type=int, help='database port')
define('server_requeue_url', default="http://localhost:8081/requeue", type=str, help='server url')
try:
    host = options.dbhost 
    port = options.dbport
    rclient = blockingRedis.StrictRedis(host,port)
    requests = rclient.keys('request*')
    for request_data in requests:
        api_key = request_data.split('_')[-2]
        jid = request_data.split('_')[-1]
        _log.debug("key=repopulate_server_q msg= internal account %s jid %s"
                %(api_key,jid))
        jdata = rclient.get(request_data) 
        request = NeonApiRequest.create(jdata)

        if request.state in [RequestState.SUBMIT,RequestState.PROCESSING,
                RequestState.REQUEUED,RequestState.FAILED]:
            #Put them back in the server queue
            url = options.server_requeue_url 
            req = urllib2.Request(url, jdata)
            try:
                response = urllib2.urlopen(req)
                resp = response.read()
            except urllib2.HTTPError as e:
                _log.exception('key=repopulate_server_q msg=http error %s' %e)
                continue

except Exception as e:
    _log.exception('key=repopulate_server_q msg=Unhandled exception %s'
                   % e)
