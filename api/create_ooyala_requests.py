#!/usr/bin/python
'''
OOYALA CRON

Create api requests for the OOYALA customers 

'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
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
        filename='/mnt/logs/neon/ooyala.log',
        filemode='a')
_log = logging.getLogger(__name__)

if __name__ == "__main__":
    utils.neon.InitNeon()
    pid = str(os.getpid())
    pidfile = "/tmp/ooyalacron.pid"
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
            skip_accounts = []
            # Get all Brightcove accounts
            host = "10.249.34.227"
            port = 6379
            rclient = blockingRedis.StrictRedis(host, port)
            accounts = rclient.keys('ooyalaplatform*')
            for accnt in accounts:
                if accnt in skip_accounts:
                    continue
                api_key = accnt.split('_')[-2]
                i_id = accnt.split('_')[-1]
                _log.debug("key=ooyala_request msg= internal account %s i_id %s" %(api_key,i_id))
                #retrieve the blob and create the object
                jdata = rclient.get(accnt)
                oo = OoyalaPlatform.create(jdata)
                oo.check_feed_and_create_requests()

        except Exception as e:
            _log.exception('key=create_ooyala_requests msg=Unhandled exception %s'
                           % e)
        os.unlink(pidfile)
