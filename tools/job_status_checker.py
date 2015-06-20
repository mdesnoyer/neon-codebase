#!/usr/bin/env python

'''
Check Status of each video in the logger file and if serving, write the time
'''
import os
import time
import urllib2
import json
import logging
import sys
import StringIO
import smtplib

#NEON_CMS_URL = "http://services.neon-lab.com"
NEON_CMS_URL = "http://cmsapi-test-1988789777.us-east-1.elb.amazonaws.com
"

logging.basicConfig(filename='/home/mdesnoyer/tmp/job_serving_times.log',level=logging.DEBUG, format='%(message)s')
_log = logging.getLogger(__name__)

def get_neon_video(account_id, api_key, video_id):
    '''
    Get video metadata object from Neon Account
    ''' 

    video_api_formater = "%s/api/v1/accounts/%s/neon_integrations/0/videos/%s"
    headers = {"X-Neon-API-Key" : API_KEY }
    request_url = video_api_formater % (NEON_CMS_URL, account_id, video_id)

    req = urllib2.Request(request_url, headers=headers)
    res = urllib2.urlopen(req)
    data = json.loads(res.read())
    return data["items"][0]["status"]


account_id = "159" # Turner Sports account
API_KEY = "3yd7b8vmrj67b99f7a8o1n30"

with open('/home/mdesnoyer/tmp/job_submit_times.log') as f:
    for line in f:
        values = line.split(",")
        if len(values) < 3:
            status = get_neon_video(account_id, API_KEY, values[1])
            if status == "serving":
                starttime = int(values[1].strip())
                curtime = int(time.time())
                _log.info('%s,%s,%s,%s,%s' % (values[0], status, starttime, curtime, curtime-starttime))

