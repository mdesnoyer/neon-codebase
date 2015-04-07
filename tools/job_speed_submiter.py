#!/usr/bin/env python

'''
Create requests for Neon via Vimeo QS changing URL
'''
import os
import time
import urllib2
import json
import logging
import sys
import StringIO
import smtplib

NEON_CMS_URL = "http://services.neon-lab.com"

logging.basicConfig(filename='job_submit_times.log',level=logging.DEBUG, format='%(message)s')
_log = logging.getLogger(__name__)


def create_neon_api_request(account_id, api_key, video_id, video_title, video_url):
    '''
    Send video processing request to Neon
    '''
    
    video_api_formater = "%s/api/v1/accounts/%s/neon_integrations/0/create_thumbnail_api_request"
    headers = {"X-Neon-API-Key" : API_KEY, "Content-Type" : "application/json"}
    request_url = video_api_formater % (NEON_CMS_URL, account_id)

    data =     { 
        "video_id": video_id,
        "video_url": video_url, 
        "video_title": video_title,
        "callback_url": None 
    }

    req = urllib2.Request(request_url, headers=headers)
    res = urllib2.urlopen(req, json.dumps(data))
    api_resp = json.loads(res.read())

    return api_resp["job_id"]


account_id = "159" # Turner Sports account
API_KEY = "3yd7b8vmrj67b99f7a8o1n30"

video_id = int(time.time())
url =  "https://www.dropbox.com/s/r0zte3nt97zaeib/GOPR0140.MP4?dl=0&test=%s" % int(time.time())
job_id = create_neon_api_request(account_id, API_KEY, video_id, video_id, url) 

_log.info('%s,%s' % (job_id, video_id))
