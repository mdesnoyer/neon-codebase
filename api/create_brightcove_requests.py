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

# Get all Brightcove accounts
host = '127.0.0.1'
port = 6379
rclient = blockingRedis.StrictRedis(host,port)
accounts = rclient.keys('brightcoveaccount_*')
for accnt in accounts:
    api_key = accnt.split('_')[-2]
    i_id = accnt.split('_')[-1]
    #retrieve the blob and create the object
    jdata = rclient.get(accnt) 
    bc = BrightcoveAccount.create(jdata)
    bc.check_feed_and_create_api_requests()

