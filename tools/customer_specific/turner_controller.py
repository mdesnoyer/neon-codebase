#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
	sys.path.insert(0, __base_path__)

import logging
import urllib2
import urllib
import math
import json
import shutil
import subprocess
import os
import time
from glob import glob
import boto
import boto.s3.connection
import utils.monitor
import utils.neon

from utils.options import define, options
define("access_key", default=None)
define("secret_key", default=None)
define("account_id", default="159")
define("api_key", default="3yd7b8vmrj67b99f7a8o1n30")
define("working_dir", default="/mnt/neon/vids")
define("program_guide"), default="http://data.tntdrama.com/processors/TNTE.json")

from utils import statemon
statemon.define('live_errors', int)

_log = logging.getLogger(__name__)

def create_neon_api_request(account_id, api_key, video_id, video_title, video_url):
	'''
	Send video processing request to Neon
	'''
	video_api_formater = "http://services.neon-lab.com/api/v1/accounts/%s/neon_integrations/0/create_thumbnail_api_request"
	headers = {"X-Neon-API-Key" : api_key, "Content-Type" : "application/json"}
	request_url = video_api_formater % (account_id)

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

def get_neon_account_video(account_id, api_key, video_id):
    '''
    Get video metadata object from Neon Account
    ''' 
    video_api_formater = "%s/api/v1/accounts/%s/neon_integrations/0/videos/%s"
    headers = {"X-Neon-API-Key" : API_KEY }
    request_url = video_api_formater % (NEON_CMS_URL, account_id, video_id)

    req = urllib2.Request(request_url, headers=headers)
    res = urllib2.urlopen(req)
    data = json.loads(res.read())

    for item in data["items"]:
    	if item.get('serving'):
        	return True

    return False

def cat_and_ffmpeg():

	#first get the program guide
	data = json.load(urllib2.urlopen(options.program_guide))
	for items in feed_data["SchedItem"].values():
		for item in items:
			#check to see if we already have this video (Airing ID)
			if not get_neon_account_video(options.account_id, options.api_key, item["AiringID"]):
				#Look at the EstimatedEndTime and EstimatedStartTime to see if we have those chunks
				


	with open(os.path.join(options.working_dir, 'input.ts'), 'wb') as destination:

		# TODO, CAT THEM IN ORDER
		valid_files = glob(os.path.join(options.working_dir, '[0-9]*.ts'))
		for filename in sorted(valid_files):
			_log.info('Catting %s' % filename)
			shutil.copyfileobj(open(filename,'rb'), destination)


	subprocess.check_call('ffmpeg -i %s -bsf:a aac_adtstoasc -vcodec copy '
						  '-acodec copy %s' % (
							  os.path.join(options.working_dir, 'input.ts'),
							  os.path.join(options.working_dir, 'output.mp4')),
							  shell=True)

if __name__ == '__main__':
	try:
        utils.neon.InitNeon()

        # TODO: Check EPG JSON and process accordingly
        # http://data.tntdrama.com/processors/TNTE.json

        cat_and_ffmpeg()

		bucket = conn.get_bucket("neon-test")

		fname = "/dlea/live/output%s.mp4" % int(time.time())
		key = bucket.new_key(fname)

		key.set_contents_from_filename(
		    os.path.join(options.working_dir, 'output.mp4'))
		key.set_acl('public-read')
		fullpath = "https://s3.amazonaws.com/neon-test%s" % fname

		create_neon_api_request(options.account_id, options.api_key,
		                        "video%s" % int(time.time()),
		                        "video%s" % int(time.time()), fullpath)

		# now that we've used up the segments DELETE THEM
		for fn in os.listdir(options.working_dir):
			os.remove(os.path.join(options.working_dir, fn))


    except Exception as e:
        _log.exception('Error running video')
        statemon.state.increment('live_errors')
        utils.monitor.send_statemon_data()
        exit(1)

    finally:
        utils.monitor.send_statemon_data()
