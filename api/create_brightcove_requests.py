#!/usr/bin/python

''' Create api requests for the brightcove customers '''
from brightcove_api import BrightcoveApi
import redis as blockingRedis
import os
import sys
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../supportServices')))
from neondata import *
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

