#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import binascii
import logging
import urllib2
import urllib
import math
import json
import m3u8
import shutil
import subprocess
import os
from Crypto.Cipher import AES
import time
from glob import glob
import boto
import boto.s3.connection
import urlparse
import utils.monitor
import utils.neon

from utils.options import define, options
define("access_key", default=None)
define("secret_key", default=None)
define("account_id", default="159")
define("api_key", default="3yd7b8vmrj67b99f7a8o1n30")
define("working_dir", default="/mnt/neon/vids")
define("lookback_count", default=36)
define("input", default='http://bcoveliveios-i.akamaihd.net/hls/live/215156/livemod_hls_trial/account=1845599807001/ba93e11685d24c39b6081cb985bc3bf2/1845599807001_8888_470_910.m3u8')
define("title", default='Test Video')

from utils import statemon
statemon.define('live_errors', int)

# USE ACCOUNT 257 for NAB
# KEY 8gnmm5kkmzwrgw89xekwl84n
#

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


def download_and_save_segment(base_path, ts_url, cipher=None):
    urlparsed = urlparse.urlparse(ts_url)
    local_fn = '%s/%s' % (options.working_dir, urlparsed.path)
    local_dir = os.path.dirname(local_fn)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    if not os.path.exists(local_fn):
        _log.info('downloading %s' % ts_url)
        if not urlparsed.netloc:
            ts_url = '%s/%s' % (base_path, ts_url)
        urlstream = urllib2.urlopen(ts_url)
        data = urlstream.read()
        if cipher is not None:
            data = cipher.decrypt(data)
        with open(local_fn, 'wb') as out_file:
            out_file.write(data)
    return local_fn

def cat_and_ffmpeg(segment_files):
    with open(os.path.join(options.working_dir, 'input.ts'), 'wb') as destination:

        # TODO, CAT THEM IN ORDER
        for filename in reversed(segment_files):
            _log.info('Catting %s' % filename)
            shutil.copyfileobj(open(filename,'rb'), destination)


	subprocess.check_call('/usr/bin/ffmpeg -i %s -absf aac_adtstoasc -vcodec copy '
                          '-acodec copy %s' % (
                              os.path.join(options.working_dir, 'input.ts'),
                              os.path.join(options.working_dir, 'output.mp4')),
                              shell=True)

if __name__ == '__main__':
    try:
        utils.neon.InitNeon()
        
        last_segment=None
        segment_file = os.path.join(options.working_dir, 'segments.log')
        if os.path.exists(segment_file):
            with open(segment_file) as f_stream:
            	for line in f_stream:
            	    val = line.strip()
            	    if val != '':
            	    	last_segment = val

        if not os.path.exists(options.working_dir):
            os.makedirs(options.working_dir)
        else:
            for fn in os.listdir(options.working_dir):
                path = os.path.join(options.working_dir, fn)
                if os.path.isfile(path):
                    os.remove(os.path.join(options.working_dir, fn))

        conn = boto.connect_s3(options.access_key, options.secret_key)

        variant_m3u8 = m3u8.load(options.input)
        variant_m3u8.is_variant

        bandwidth = 0
        hdurl = ""

        for playlist in variant_m3u8.playlists:
            if playlist.stream_info.bandwidth > bandwidth:
                bandwidth = playlist.stream_info.bandwidth
                hdurl = playlist.uri

        hdurl = '%s/%s' % (os.path.dirname(options.input), hdurl)
        #m3u8_obj = m3u8.load(hdurl)
        m3u8_obj = m3u8.load(options.input)
        cipher=None
        if m3u8_obj.key is not None:
            encr_key = urllib2.urlopen(m3u8_obj.key.uri)
            iv = binascii.unhexlify(m3u8_obj.key.iv.split('X')[1])
            cipher = AES.new(encr_key.read(), AES.MODE_CBC, IV=iv)

        # for AK live, we need to download and save the last 12 in the list
        idx = 0
        segment_files = []
        for segment in reversed(m3u8_obj.files):
            local_fn = os.path.join(options.working_dir, segment)

            # If the first segment already exists, then exit because we've
            # done this file
            if idx == 0:
                if last_segment is not None and last_segment == segment:
                    exit(1)
                else:
                    with open(segment_file, 'w') as f_stream:
                        f_stream.write(segment)

            if idx < options.lookback_count:
                local_fn = download_and_save_segment(os.path.dirname(hdurl),
                                                     segment, cipher)
                segment_files.append(local_fn)
            elif os.path.exists(local_fn):
                # Delete older segments
                os.remove(local_fn)
            idx += 1

        cat_and_ffmpeg(segment_files)

        bucket = conn.get_bucket("neon-test")

        fname = "/dlea/live/output%s.mp4" % int(time.time())
        key = bucket.new_key(fname)

        key.set_contents_from_filename(
            os.path.join(options.working_dir, 'output.mp4'))
        key.set_acl('public-read')
        fullpath = "https://s3.amazonaws.com/neon-test%s" % fname

        create_neon_api_request(options.account_id, options.api_key,
                                "video%s" % int(time.time()),
                                options.title, fullpath)
    except Exception as e:
        _log.exception('Error running video')
        statemon.state.increment('live_errors')
        utils.monitor.send_statemon_data()
        exit(1)

    finally:
        utils.monitor.send_statemon_data()

#steps
#1. download manifest
#2. Check to make sure there are at least 12 segments, download each segment
#3. cat the last 12 segments (2 minutes) into 1 .ts
##  os.system(my_cmd)
#4. Submit that new file as a job

