#!/usr/bin/env python

'''
Create requests for Eurogamer via YouTube Feed 
'''
import os
import os.path
import urllib2
import json
import logging
import subprocess
import sys
import StringIO
import smtplib
import xml.etree.ElementTree
import boto
import boto.s3.connection
from email.mime.text import MIMEText

NEON_CMS_URL = "http://services.neon-images.com"
#NEON_CMS_URL = "http://54.225.235.97:8083"

TMP_DIR="/mnt/neon/eurogamer"

access_key = "AKIAJLSUET5GCSSUW5WQ"
secret_key = "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"

conn = boto.connect_s3(
       aws_access_key_id = access_key,
       aws_secret_access_key = secret_key)

if not os.path.exists(TMP_DIR):
    os.mkdirs(TMP_DIR)

logging.basicConfig(filename='%s/yt_for_eurogamer.log' % TMP_DIR,level=logging.DEBUG, format='%(asctime)s %(message)s')
_log = logging.getLogger(__name__)

### Check if its already running
#pid = str(os.getpid())
#pidfile = "/tmp/snappycron.pid"
#if os.path.isfile(pidfile):
#    with open(pidfile, 'r') as f:
#        pid = f.readline().rstrip('\n')
#        if os.path.exists('/proc/%s' %pid):
#            print "%s already exists, exiting" % pidfile
#            sys.exit()
#        else:
#            os.unlink(pidfile)
                


_log.info('\n\n-----Checking YT for new Eurogamer videos-----\n')

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
        "callback_url": "https://zapier.com/hooks/catch/b8lbva/" 
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


account_id = "319" # David Lea account
API_KEY = "e61fk07rstxjiufbpaxd2gu2"

#account_id = "159" # David Lea account
#API_KEY = "3yd7b8vmrj67b99f7a8o1n30"
#_log.info('Existing video_ids: %s \n' % video_ids)

def make_yt_api_request(url, videos):

    xmlStream = urllib2.urlopen(url)
    try:
        xmlDoc = xml.etree.ElementTree.parse(xmlStream)
        root = xmlDoc.getroot()

        for media_group in root.iter('{http://search.yahoo.com/mrss/}group'):
            title = media_group.find('{http://search.yahoo.com/mrss/}title').text
            #import pdb; pdb.set_trace()
            yt_url = media_group.find('{http://search.yahoo.com/mrss/}content').attrib['url']
            stripped_url = yt_url.replace("https://www.youtube.com/v/", "")
            real_id = stripped_url.replace("?version=3", "")
            print real_id

            if str(real_id) in videos:
                _log.info('YT video already exists in our account\n')
            else:
                videos.append(real_id)
                video_fn = '%s/%s.mp4' % (TMP_DIR, real_id)
                subprocess.call('youtube-dl --id %s -o %s' %
                                (yt_url, video_fn), shell=True)
                bucket = conn.get_bucket("neon-test")
                fname = "/dlea/eurogamer/%s.mp4" % real_id
                key = bucket.new_key(fname)
                key.set_contents_from_filename(video_fn)
                os.remove(video_fn)
                
                fullpath = "https://s3.amazonaws.com/neon-test/dlea/eurogamer/%s.mp4" % real_id
                job_id = create_neon_api_request(account_id, API_KEY, real_id, title, fullpath) 
                _log.info('Item added to Neon with Job ID: %s \n' % job_id)

    finally:
        xmlStream.close()



#OLD ONE WITH TEST DATA feed_data = make_snappy_api_request("http://api.snappytv.com/partner_api/v1/timeline/25209371.json")


#var feeds = ["https://www.youtube.com/feeds/videos.xml?channel_id=UCKk076mm-7JjLxJcFSXIPJA"]
video_requests = []
feed = "https://www.youtube.com/feeds/videos.xml?channel_id=UCKk076mm-7JjLxJcFSXIPJA"
 
#for feed in feeds:


video_ids = get_neon_account_videos(account_id, API_KEY)
feed_data = make_yt_api_request(feed, video_ids)

#    for items in feed_data["tracks"].values():
#        for item in items:
#            if item['type'] == 'snap':
#                snappy_id = item["id"]
#                video_title = item["title"]
#                video_url = item["downloads"][0]["url"]
#                _log.info('Got a snap item : %s %s %s \n' % (snappy_id, video_title, video_url))
#                # check if snappy video id exists in the video_ids list
#                # import pdb; pdb.set_trace()

#                if str(snappy_id) in video_ids:
#                    _log.info('Snap already exists in our account\n')
#                else:
#                    video_requests.append(snappy_id)
#                    job_id = create_neon_api_request(account_id, API_KEY, snappy_id, video_title, video_url) 
                    #print job_id
#                    _log.info('Item added to Neon with Job ID: %s \n' % job_id)
# Remove PID file                    
#os.unlink(pidfile)
#if len(video_requests) > 0:
#    send_email("".join(video_requests))
