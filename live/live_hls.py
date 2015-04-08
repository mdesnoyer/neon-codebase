#!/usr/bin/python
import urllib2
import urllib
import math
import json
import m3u8
import shutil
import subprocess
import os
import time
from glob import iglob
import boto
import boto.s3.connection

access_key = "AKIAJLSUET5GCSSUW5WQ"
secret_key = "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"

conn = boto.connect_s3(
       aws_access_key_id = access_key,
       aws_secret_access_key = secret_key)


NEON_CMS_URL = "http://services.neon-lab.com"
account_id = "159" # David Lea Account
API_KEY = "3yd7b8vmrj67b99f7a8o1n30"

# USE ACCOUNT 257 for NAB
# KEY 8gnmm5kkmzwrgw89xekwl84n
#

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

    print data

    req = urllib2.Request(request_url, headers=headers)

    print req

    res = urllib2.urlopen(req, json.dumps(data))
    api_resp = json.loads(res.read())

    return api_resp["job_id"]


def download_and_save_segment(ts_url):
    print ts_url
    fname = ts_url
    new_url = "http://bcoveliveios-i.akamaihd.net/hls/live/215156/livemod_hls_trial/account=1845599807001/ba93e11685d24c39b6081cb985bc3bf2/%s" % ts_url
    urllib.urlretrieve(new_url, fname)

def cat_and_ffmpeg():
	PATH = r'/Users/davidlea/Desktop/code/'
	destination = open('input.ts', 'wb')

	# TODO, CAT THEM IN ORDER
	for filename in iglob(os.path.join(PATH, '*.ts')):
		shutil.copyfileobj(open(filename,'rb'), destination)
	destination.close

	subprocess.call('ffmpeg -i /Users/davidlea/Desktop/code/input.ts -bsf:a aac_adtstoasc -vcodec copy -acodec copy output.mp4', shell=True)
    #subprocess.call('ffmpeg -i output.mp4 -b 800k output2.mp4')

global count
count = 30

try:
    os.remove("/Users/davidlea/Desktop/code/output.mp4")
    for x in range(1,30):
        os.remove("/Users/davidlea/Desktop/code/segment_%s.ts" % x)
        print sys.exc_info()[0]
except:
    pass

#m3u8_obj = m3u8.load('/Users/davidlea/Desktop/code/m3u8-master/tests/playlists/simple-playlist.m3u8')  # this could also be an absolute filename
#m3u8_obj = m3u8.load('http://bcoveliveios-i.akamaihd.net/hls/live/215156/livemod_hls_trial/account=1845599807001/7014dcbf4ce03eda2ab2ef5239572a43/1845599807001_8888_0e6_910.m3u8')  # this could also be an absolute filename

m3u8_obj = m3u8.load('http://bcoveliveios-i.akamaihd.net/hls/live/215156/livemod_hls_trial/account=1845599807001/ba93e11685d24c39b6081cb985bc3bf2/1845599807001_8888_470_910.m3u8')
#print m3u8_obj.files

# for AK live, we need to download and save the last 12 in the list
for segment in reversed(m3u8_obj.files):
    # Check to see if this first segment (the most recent) exists in file system.  If so BAIL.
    if count == 30 and os.path.exists(segment):
        break
	if count > 0:        
		#download_and_save_segment("%s" % segment)
		count = count - 1

#cat_and_ffmpeg()

bucket = conn.get_bucket("neon-test")

fname = "/dlea/live/output%s.mp4" % int(time.time())
key = bucket.new_key(fname)

#key.set_contents_from_filename('/Users/davidlea/Desktop/code/output.mp4')
#key.set_acl('public-read')
#fullpath = "https://s3.amazonaws.com/neon-test%s" % fname

#create_neon_api_request(account_id, API_KEY, "video%s" % int(time.time()), "video%s" % int(time.time()), fullpath)


# http://azubuhdlive-lh.akamaihd.net/control/video25892CHFaker_1_2048@131269?cmd=log,rebufferevent,0.069&v=3.4.0.132&r=ETOAY&g=LNHXUJPOKXTL
#rtmp://live29.us-va.zencoder.io:1935/live

#steps
#1. download manifest
#2. Check to make sure there are at least 12 segments, download each segment
#3. cat the last 12 segments (2 minutes) into 1 .ts
##  os.system(my_cmd)
#4. Submit that new file as a job

