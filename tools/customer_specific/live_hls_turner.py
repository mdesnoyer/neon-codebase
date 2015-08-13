#!/usr/bin/env python
'''Script to download segments from the Turner live stream'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import binascii
from Crypto.Cipher import AES
import dateutil.parser
from glob import glob
import logging
import os
import requests
import requests.exceptions
import m3u8
import shutil
import time
import urlparse
import utils.monitor
import utils.neon

from utils.options import define, options
define("working_dir", default="/mnt/neon/vids")
define("input", default='http://androidhlslive-secure.cdn.turner.com/tnt/hls/tvetnt/east/stream.m3u8?hdnea=expires%3D1440454134%7Eaccess%3D%2F*%7Emd5%3D6b89107fe1e27229a8f4e8f48eb93ca6')

from utils import statemon
statemon.define('turner_live_errors', int)


_log = logging.getLogger(__name__)



def download_segment(session, ts_url, local_fn, cipher=None):
    if not os.path.exists(local_fn):
        _log.info('Downloading %s' % ts_url)
        data = session.get(ts_url)
        data.raise_for_status()
        data = data.content
        if cipher is not None:
            data = cipher.decrypt(data)
        with open(local_fn, 'wb') as out_file:
            out_file.write(data)

def get_cipher(session, key):
    if key is None:
        return None
    encr_key = session.get(key.uri)
    iv = binascii.unhexlify(key.iv.split('x')[1])
    return AES.new(encr_key.content, AES.MODE_CBC, IV=iv)

def main():
    if not os.path.exists(options.working_dir):
        os.makedirs(options.working_dir)

    with requests.Session() as session:
        master_response = session.get('%s&cache_buster=%d' % (
            options.input, int(time.time())))
        master_response.raise_for_status()
        master_m3u8 = m3u8.loads(master_response.content)

        # Find the variant to download
        bandwidth = 0
        variant_url = None
        for playlist in master_m3u8.playlists:
            if playlist.stream_info.bandwidth > bandwidth:
                bandwidth = playlist.stream_info.bandwidth
                variant_url = playlist.uri

        variant_url = urlparse.urljoin(options.input, variant_url)
        variant_response = session.get(variant_url)
        variant_response.raise_for_status()
        m3u8_obj = m3u8.loads(variant_response.content)

        # Get the cipher if necessary
        global_cipher = get_cipher(session, m3u8_obj.key)

        for segment in m3u8_obj.segments:
            #segment.program_date_time example: 2015-06-30T19:04:51Z
            ts_fn = 'segment_%s.ts' % segment.program_date_time.strftime('%Y-%m-%dT%H:%M:%SZ')

            local_fn = os.path.join(options.working_dir, ts_fn)
            if os.path.exists(local_fn):
                continue

            cipher = get_cipher(session, segment.key) or global_cipher

            if not segment.cue_out:
                # It's not an ad, so download it
                segment_url = urlparse.urljoin(variant_url, segment.uri)
                download_segment(session, segment_url, local_fn, cipher)

if __name__ == '__main__':
    utils.neon.InitNeon()
    try:
        main()

    except Exception as e:
        _log.exception('Error running video')
        statemon.state.increment('turner_live_errors')
        utils.monitor.send_statemon_data()
        exit(1)

    finally:
        utils.monitor.send_statemon_data()

#steps
#1. Download all new segments that aren't ad break, name them based on segment start time
#2. Once an hour, at the top of the hour + 5 minutes of buffer, check to see what programs have
#   aired.  If there is a new program (as defined in the TNT JSON, stitch the program together
#   and submit as a job to Neon with video ID = airing ID.
