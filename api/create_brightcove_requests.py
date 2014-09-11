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
import urllib2
import utils.neon
import utils.monitor
from utils.options import define, options
from utils import statemon

import logging
_log = logging.getLogger(__name__)

skip_accounts = ["brightcoveplatform_4b33788e970266fefb74153dcac00f94_31", "brightcoveplatform_8bda0ee38d1036b46d07aec4040af69c_26"
            "brightcoveplatform_5329143981226ef6593f3762b636bd44_23",
            "brightcoveplatform_gmlrsi27487ocd0wwbr9y66z_38"
        ]

statemon.define('cron_finished', int)
statemon.define('cron_error', int)
statemon.define('accnt_delay', int)

def check_single_brightcove_account_delay(self, api_key='6d3d519b15600c372a1f6735711d956e', i_id='52'):
    '''
    Maintains a counter to help understand the delay between api call and request creation
    '''
    ba = BrighcovePlatform.get_account(api_key, i_id) 
    req = 'http://api.brightcove.com/services/library?command=find_all_videos&token=%s&media_delivery=http&output=json&sort_by=publish_date' %ba.read_token
    response = urllib2.urlopen(req)
    resp = json.loads(resp.read())
    for item in vitems['items']:
        vid = str(item['id'])
        if not ba.video.has_key(vid):
            statemon.state.increment('accnt_delay')
            return #return on a single delay detection

if __name__ == "__main__":
    utils.neon.InitNeon()
    pid = str(os.getpid())
    pidfile = "/tmp/brightcovecron.pid"
    if os.path.isfile(pidfile):
        with open(pidfile, 'r') as f:
            pid = f.readline().rstrip('\n')
            if os.path.exists('/proc/%s' %pid):
                print "%s already exists, exiting" % pidfile
                sys.exit()
            else:
                os.unlink(pidfile)

    else:
        file(pidfile, 'w').write(pid)

        try:
            # Get all Brightcove accounts
            accounts = BrightcovePlatform.get_all_instances()
            for accnt in accounts:
                if accnt.neon_api_key in skip_accounts:
                    continue
                api_key = accnt.neon_api_key
                i_id = accnt.integration_id 
                _log.info("key=brightcove_request msg= internal account %s i_id %s" %(api_key, i_id))
                #retrieve the blob and create the object
                accnt.check_feed_and_create_api_requests()

        except Exception as e:
            _log.exception('key=create_brightcove_requests msg=Unhandled exception %s'
                           % e)
            statemon.state.increment('cron_error')

        os.unlink(pidfile)
    statemon.state.increment('cron_finished')
    utils.monitor.send_statemon_data()
