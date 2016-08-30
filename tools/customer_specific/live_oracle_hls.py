#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import binascii
import cmsapiv2.client
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
import tornado.gen
import urlparse
import utils.monitor
import utils.neon

from utils.options import define, options
define("access_key", default=None)
define("secret_key", default=None)
define("cmsapi_user", default=None) 
define("cmsapi_pass", default=None) 
define("account_id", default="bfxsu6vp8utmmb57hoqcu55j")
define("working_dir", default="/mnt/neon/vids")
define("lookback_count", default=36)
#define("input", default='http://oracleuniversal2-lh.akamaihd.net/i/oracle_hd2@134506/master.m3u8')
#define("input", default='http://iphone-cnn.cdn.turner.com/cnn/iphone2/cnn_live/cnn_live_2_ipad.m3u8')
define("input", default='http://oracleuniversal2-lh.akamaihd.net/i/oracle_hd2@134506/master.m3u8')
define("title", default='oracle_20160813')

from utils import statemon
statemon.define('live_errors', int)

_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def create_neon_api_request(account_id, video_id, video_title, video_url):
    '''
    Send video processing request to Neon
    '''
    custom_data = { "feed" : options.input } 
    body = {
        'external_video_ref': video_id,
        'url': video_url,
        'title': video_title,
        'callback_url': None,
        'custom_data': custom_data,
    }

    headers = {"Content-Type": "application/json"}

    url = '/api/v2/%s/videos' % (account_id)

    req = tornado.httpclient.HTTPRequest(
        url,
        method='POST',
        headers=headers,
        body=json.dumps(dict((k, v)
            for k, v in body.iteritems() if v or v is 0)))

    client = cmsapiv2.client.Client(
        options.cmsapi_user,
        options.cmsapi_pass)

    res = yield client.send_request(
        req,
        no_retry_codes=[402,409,429],
        ntries=3)

    if res.error:
        if res.error.code == 409:
            _log.warn('Video %s for account %s already exists' %
                (video_id, self.neon_api_key))
        elif res.error.code == 402 or res.error.code == 429:
            _log.warn('Rate limit for account %s has been met' %
                (self.neon_api_key))
        else:
            statemon.state.increment('job_submission_error')
            _log.error('Error submitting video: %s' % res.error)

        return

    raise tornado.gen.Return(json.loads(res.body))  

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

@tornado.gen.coroutine
def main(): 
    try:
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

        #hdurl = '%s/%s' % (os.path.dirname(options.input), hdurl)
        m3u8_obj = m3u8.load(hdurl)
        #m3u8_obj = m3u8.load(options.input)
        cipher=None
        if m3u8_obj.key is not None:
            encr_key = urllib2.urlopen(m3u8_obj.key.uri)
            iv = binascii.unhexify(m3u8_obj.key.iv.split('X')[1])
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
                try:
                    local_fn = download_and_save_segment(
                        os.path.dirname(hdurl),
                        segment, cipher)
                    segment_files.append(local_fn)
                except urllib2.HTTPError as e:
                    _log.warning("Error downloading segment %s: %s" % 
                                 (segment,e))
            elif os.path.exists(local_fn):
                # Delete older segments
                os.remove(local_fn)
            idx += 1

        if len(segment_files) == 0:
            raise Exception("Could not download any video")
        cat_and_ffmpeg(segment_files)

        bucket = conn.get_bucket("neon-test")

        fname = "/dlea/live/output%s.mp4" % int(time.time())
        key = bucket.new_key(fname)

        key.set_contents_from_filename(
            os.path.join(options.working_dir, 'output.mp4'))
        key.set_acl('public-read')
        fullpath = "https://s3.amazonaws.com/neon-test%s" % fname

        yield create_neon_api_request(options.account_id,
                                "video%s" % int(time.time()),
                                options.title, fullpath)
    except Exception as e:
        _log.exception('Error running video')
        statemon.state.increment('live_errors')
        utils.monitor.send_statemon_data()
        exit(1)

    finally:
        utils.monitor.send_statemon_data()

if __name__ == '__main__':
    utils.neon.InitNeon()
    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.run_sync(main)

#steps
#1. download manifest
#2. Check to make sure there are at least 12 segments, download each segment
#3. cat the last 12 segments (2 minutes) into 1 .ts
##  os.system(my_cmd)
#4. Submit that new file as a job
