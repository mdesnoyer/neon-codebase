#!/usr/bin/python
''' Create api requests for the brightcove customers '''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
from brightcove_api import BrightcoveApi
import redis as blockingRedis
import os
from supportServices.neondata import *
import json
import logging
logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%m-%d %H:%M',
        filename='/mnt/logs/neon/brightcovecron.log',
        filemode='a')
_log = logging.getLogger(__name__)

try:
    # Get all Brightcove accounts
    host = '127.0.0.1'
    port = 6379
    rclient = blockingRedis.StrictRedis(host,port)
    accounts = rclient.keys('brightcoveplatform*')
    for accnt in accounts:
        api_key = accnt.split('_')[-2]
        i_id = accnt.split('_')[-1]
        _log.debug("key=brightcove_request msg= internal account %s i_id %s" %(api_key,i_id))
        #retrieve the blob and create the object
        jdata = rclient.get(accnt) 
        bc = BrightcovePlatform.create(jdata)
        bc.check_feed_and_create_api_requests()

except Exception as e:
    _log.error('key=create_brightcove_requests msg=Unhandled exception %s' %
               e)

