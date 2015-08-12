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
import m3u8
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
define("lookback_count", default=36)
define("input", default='http://androidhlslive-secure.cdn.turner.com/tnt/hls/tvetnt/east/stream.m3u8?hdnea=expires%3D1440454134%7Eaccess%3D%2F*%7Emd5%3D6b89107fe1e27229a8f4e8f48eb93ca6')

from utils import statemon
statemon.define('live_errors', int)


_log = logging.getLogger(__name__)


def download_and_save_segment(base_path, ts_url):
    local_fn = os.path.join(options.working_dir, ts_url)
    if not os.path.exists(local_fn):
        
        _log.info('downloading %s' % ts_url)
        urllib.urlretrieve('%s/%s' % (base_path, ts_url), local_fn)


if __name__ == '__main__':
    try:
        utils.neon.InitNeon()

        if not os.path.exists(options.working_dir):
            os.makedirs(options.working_dir)

        conn = boto.connect_s3(options.access_key, options.secret_key)

        variant_m3u8 = m3u8.load(options.input)
        variant_m3u8.is_variant

        bandwidth = 0
        hdurl = ""

        for playlist in variant_m3u8.playlists:
            if playlist.stream_info.bandwidth > bandwidth:
                bandwidth = playlist.stream_info.bandwidth
                hdurl = playlist.uri

        m3u8_obj = m3u8.load(hdurl)

        # for Turner we need to download any segments that we don't have already
        for segment in m3u8_obj.files:
            
            #segment.program_date_time example: 2015-06-30T19:04:51Z
            prog_time = segment.program_date_time + '.ts'
            ts_start_time = prog_time.replace('-', '_').replace(':', '_')
            local_fn = os.path.join(options.working_dir, ts_start_time)

            if not os.path.exists(local_fn):

                # if this isn't an advertisement segment, keep it
                # TODO: GO BACK AND RENAME OR DELETE THE PREV 6 FILES SINCE THEY MIGHT HAVE LOWER THIRDS
                # TODO: When we start processing after the break, skip the first 6 files.
                if not segment.cue_out:
                    download_and_save_segment(os.path.dirname(options.input), segment)


    except Exception as e:
        _log.exception('Error running video')
        statemon.state.increment('live_errors')
        utils.monitor.send_statemon_data()
        exit(1)

    finally:
        utils.monitor.send_statemon_data()

#steps
#1. Download all new segments that aren't ad break, name them based on segment start time
#2. Once an hour, at the top of the hour + 5 minutes of buffer, check to see what programs have
#   aired.  If there is a new program (as defined in the TNT JSON, stitch the program together
#   and submit as a job to Neon with video ID = airing ID.
