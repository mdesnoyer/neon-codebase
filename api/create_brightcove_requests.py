#!/usr/bin/python
'''
BRIGHTCOVE CRON 

Parse Brightcove Feed for all customers and 
Create api requests for the brightcove customers 

'''
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
import utils.neon
from utils.options import define, options

import logging
logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%m-%d %H:%M',
        filename='/mnt/logs/neon/brightcovecron.log',
        filemode='a')
_log = logging.getLogger(__name__)
skip_accounts = ["brightcoveplatform_4b33788e970266fefb74153dcac00f94_31", "brightcoveplatform_8bda0ee38d1036b46d07aec4040af69c_26"
            "brightcoveplatform_5329143981226ef6593f3762b636bd44_23"
        ]

if __name__ == "__main__":
    utils.neon.InitNeon()
    pid = str(os.getpid())
    pidfile = "/tmp/brightcovecron.pid"
    if os.path.isfile(pidfile):
        with open(pidfile, 'r') as f:
            pid = f.readline().rstrip('\n')
            if os.path.exits('/proc/%s' %pid):
                print "%s already exists, exiting" % pidfile
                sys.exit()
            else:
                os.unlink(pidfile)

    else:
        file(pidfile, 'w').write(pid)

        try:
            # Get all Brightcove accounts
            host = "127.0.0.1" 
            port = 6379
            rclient = blockingRedis.StrictRedis(host, port)
            accounts = rclient.keys('brightcoveplatform*')
            for accnt in accounts:
                if accnt in skip_accounts:
                    continue
                api_key = accnt.split('_')[-2]
                i_id = accnt.split('_')[-1]
                _log.debug("key=brightcove_request msg= internal account %s i_id %s" %(api_key,i_id))
                #retrieve the blob and create the object
                jdata = rclient.get(accnt) 
                bc = BrightcovePlatform.create(jdata)
                bc.check_feed_and_create_api_requests()

        except Exception as e:
            _log.exception('key=create_brightcove_requests msg=Unhandled exception %s'
                           % e)
        os.unlink(pidfile)

