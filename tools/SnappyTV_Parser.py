#!/usr/bin/env python

'''
Create Neon API requests from a SnappyTV Feed 
'''
import os
import urllib2
import json
import logging
import sys
import StringIO
import smtplib
from email.mime.text import MIMEText

NEON_CMS_URL = "http://cmsapi-202500328.us-east-1.elb.amazonaws.com"
#NEON_CMS_URL = "http://54.225.235.97:8083"

logging.basicConfig(filename='snappy.log',level=logging.DEBUG, format='%(asctime)s %(message)s')
_log = logging.getLogger(__name__)

### Check if its already running
pid = str(os.getpid())
pidfile = "/tmp/snappycron.pid"
if os.path.isfile(pidfile):
    with open(pidfile, 'r') as f:
        pid = f.readline().rstrip('\n')
        if os.path.exists('/proc/%s' %pid):
            print "%s already exists, exiting" % pidfile
            sys.exit()
        else:
            os.unlink(pidfile)
                


_log.info('\n\n-----Checking SnappyTV for new videos-----\n')

def send_email(data):
    print "sending email"
    output = StringIO.StringIO()
    output.write(data)
    output.seek(0)
    msg = MIMEText(output.read())
    frm = "create_requests@neon-lab.com" 
    to = "mallya@neon-lab.com" 
    msg['Subject'] = 'Snapy video requests'
    msg['From'] = "abtest@neon-lab.com" 
    msg['To'] = "mallya@neon-lab.com" 

    s = smtplib.SMTP('localhost')
    s.sendmail(frm, [to, "lea@neon-lab.com"], msg.as_string())
    s.quit()

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

def get_neon_account_videos(account_id, api_key):
    '''
    Get video metadata object from Neon Account
    ''' 
    video_ids = []
    video_api_formater = "%s/api/v1/accounts/%s/neon_integrations/0/videos"
    headers = {"X-Neon-API-Key" : API_KEY }
    request_url = video_api_formater % (NEON_CMS_URL, account_id)

    req = urllib2.Request(request_url, headers=headers)
    res = urllib2.urlopen(req)
    data = json.loads(res.read())

    for item in data["items"]:
        video_ids.append(item["video_id"])
        
    return video_ids


#account_id = "999"
#API_KEY = "abc123"

#_log.info('Existing video_ids: %s \n' % video_ids)

def make_snappy_api_request(url):
    req = urllib2.Request(url);
    res = urllib2.urlopen(req)
    data = json.loads(res.read())
    return data


#array of feeds to check from Snappy.  For Ryder Cup, there was 1 unique JSON for each day of the tournament
var feeds = ["http://api.snappytv.com/partner_api/v1/timeline/25336934.json","http://api.snappytv.com/partner_api/v1/timeline/25336937.json","http://api.snappytv.com/partner_api/v1/timeline/25336938.json","http://api.snappytv.com/partner_api/v1/timeline/25336935.json","http://api.snappytv.com/partner_api/v1/timeline/25336936.json","http://api.snappytv.com/partner_api/v1/timeline/25336939.json","http://api.snappytv.com/partner_api/v1/timeline/25336940.json","http://api.snappytv.com/partner_api/v1/timeline/25336942.json","http://api.snappytv.com/partner_api/v1/timeline/25337239.json"]

video_requests = []
 
for feed in feeds:

    feed_data = make_snappy_api_request(feed)
    video_ids = get_neon_account_videos(account_id, API_KEY)
    
    for items in feed_data["tracks"].values():
        for item in items:
            if item['type'] == 'snap':
                snappy_id = item["id"]
                video_title = item["title"]
                video_url = item["downloads"][0]["url"]
                _log.info('Got a snap item : %s %s %s \n' % (snappy_id, video_title, video_url))
                # check if snappy video id exists in the video_ids list
                # import pdb; pdb.set_trace()

                if str(snappy_id) in video_ids:
                    _log.info('Snap already exists in the Neon account\n')
                else:
                    video_requests.append(snappy_id)
                    job_id = create_neon_api_request(account_id, API_KEY, snappy_id, video_title, video_url) 
                    _log.info('Item added to Neon with Job ID: %s \n' % job_id)
# Remove PID file                    
os.unlink(pidfile)
if len(video_requests) > 0:
    send_email("".join(video_requests))
