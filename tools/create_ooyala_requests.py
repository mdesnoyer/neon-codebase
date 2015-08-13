#!/usr/bin/python
'''
OOYALA CRON

Create api requests for the OOYALA customers 

'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
    
import redis as blockingRedis
import os
from cmsdb.neondata import *
import json
import utils.neon
from utils.options import define, options

import logging
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
            dbconn = DBConnection(OoyalaPlatform)
            keys = dbconn.blocking_conn.keys('ooyalaplatform*')
            accounts = []
            for k in keys:
                parts = k.split('_')
                op = OoyalaPlatform.get(parts[-2], parts[-1])
                accounts.append(op)
            for accnt in accounts:
                if accnt.enabled == False:
                    continue
                api_key = accnt.neon_api_key
                i_id = accnt.integration_id 
                _log.debug("key=ooyala_request msg= internal account %s i_id %s" %(api_key,i_id))
                accnt.check_feed_and_create_requests()

        except Exception as e:
            _log.exception('key=create_ooyala_requests msg=Unhandled exception %s'
                           % e)
        os.unlink(pidfile)
