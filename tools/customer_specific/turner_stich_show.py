#!/usr/bin/env python
'''Script to stich together an turner episode after segments were downloaded.

This script is paired with live_hls_turner.py, which downloads the videos
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto
import boto.s3.connection
import copy
import datetime
import dateutil.parser
from glob import glob
import json
import logging
import math
import re
import requests
import shutil
import subprocess
import utils.monitor
import utils.neon

from utils.options import define, options
define("working_dir", default="/mnt/neon/vids")
define("schedule", default="http://data.tntdrama.com/processors/TNTE.json")
define("account_id", default="325")
define("api_key", default="u21ep2m88rapp50dmbpjj2un")
define("segment_length", default=10.0, type=float,
       help='Length in seconds of each segment')
define("break_buffer", default=30.0, type=float,
       help=('Number of seconds to ignore at around breaks, wether it is an '
             'ad or the beginning and end of the episode.'))


from utils import statemon
statemon.define('turner_stich_errors', int)


_log = logging.getLogger(__name__)

def parse_schedule(uri):
    '''Parses the json data from a uri.

    Returns a list of (start_time, end_time, name, airingid)
    '''
    _log.info('Downloading schedule from %s' % uri)
    response = requests.get(uri)
    response.raise_for_status()
    data = response.json()

    retval = []
    for episode in data['LiveFeed']['tveLiveSched']['SchedItem']:
        start_time = dateutil.parser.parse(episode['Program']['StartTime'])
        end_time = dateutil.parser.parse(episode['Program']['EndTime'])
        airing_id = episode['AiringID']
        name = '%s - %s' % (episode['Program']['SeriesName'],
                            episode['Program']['Name'])

        retval.append((start_time, end_time, name, airing_id))

    return sorted(retval, key=lambda x: x[0], reverse=True)

def get_segment_list(working_dir):
    '''Gets a list of (start_time, filename) of segments in the directory.

    List will be ordered by start time
    '''
    retval = []
        
    dateRe = re.compile('segment_([0-9TZ\-:\.]+)\.ts')
    for fn in glob(os.path.join(working_dir, 'segment_*.ts')):
        dateMatch = dateRe.search(fn)
        if dateMatch:
            start_time = dateutil.parser.parse(dateMatch.group(1))
            retval.append((start_time, fn))

    retval = sorted(retval, key=lambda x: x[0])
    _log.info('Found %d segments starting at %s and ending %s' %
              (len(retval), retval[0][0], retval[-1][0]))

    return retval

def process_episode(episode, segments):
    start_time, end_time, name, airing_id = episode

    # Look to see if we have a segment after the end of the episode
    # and we have at least 50% of the episode.
    if segments[-1][0] < end_time:
        return

    ep_segments = [x for x in segments if
                   x[0] > start_time and x[0] < end_time]
    segment_time = len(ep_segments) * options.segment_length
    ep_time = (end_time - start_time).total_seconds()

    if segment_time < 0.5 * ep_time:
        return

    # Throw out segments around an ad break or at the beginning and
    # end of the video.
    n_seg_cut = int(math.ceil(options.break_buffer / options.segment_length))
    filt_segments = []
    skip_count = n_seg_cut
    for seg_time, seg_fn in ep_segments[n_seg_cut:-n_seg_cut]:
        if len(filt_segments) == 0 or skip_count == 0:
            filt_segments.append((seg_time, seg_fn))
            skip_count = n_seg_cut
            continue

        time_diff = (seg_time - filt_segments[-1][0]).total_seconds()
        if time_diff > (options.segment_length + 0.1):
            # We found a discontinuity
            if skip_count == n_seg_cut:
                # It's the start of the discontinuty so throw out the
                # most recent segments.
                filt_segments = filt_segments[:-n_seg_cut]

            # Don't take this segment, but start counting down
            skip_count -= 1
            continue
        filt_segments.append((seg_time, seg_fn))

    _log.info('Stiching together segments for episode %s' % airing_id)

    full_ts_fn = os.path.join(options.working_dir, '%s.ts' % airing_id)
    episode_fn = '%s.mp4' % airing_id
    episode_full_path = os.path.join(options.working_dir, episode_fn)
    with open(full_ts_fn, 'wb') as out_stream:
        for seg_time, seg_fn in filt_segments:
            with open(seg_fn, 'rb') as in_stream:
                out_stream.writelines(in_stream)

    _log.info('Transmuxing file to mp4')

    subprocess.check_call('/usr/bin/ffmpeg -i %s  -absf aac_adtstoasc '
                          '-vcodec copy -acodec copy %s' % 
                          (full_ts_fn, episode_full_path),
                          shell=True)
    

    _log.info('Uploading episode to s3://neon-test/dlea/live/turner/%s' % 
              episode_fn)
    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('neon-test')
    key = bucket.new_key('/dlea/live/turner/%s' % episode_fn)
    key.set_contents_from_filename(
        os.path.join(options.working_dir, episode_fn),
        headers = {'Content-Type' : 'video/mp4'},
        policy='public-read',
        cb=lambda x, y: _log.info('Uploaded %d of %d to S3' % (x, y)))

    _log.info('Submitting job')
    submit_neon_job(airing_id, name,
                    's3://neon-test/dlea/live/turner/%s' % episode_fn)

    _log.info('Erasing segments')
    for seg_time, seg_fn in ep_segments:
        os.remove(seg_fn)
    os.remove(episode_full_path)

def submit_neon_job(video_id, video_title, video_url):
    request_url = ('http://services.neon-lab.com/api/v1/accounts/'
                   '%s/neon_integrations/0/create_thumbnail_api_request' %
                   options.account_id)
    headers = {"X-Neon-API-Key" : options.api_key,
               "Content-Type" : "application/json"}

    data = { 
        "video_id": video_id,
        "video_url": video_url, 
        "video_title": video_title,
        "topn" : 10
    }
    response = requests.post(request_url, json.dumps(data), headers=headers)
    if response.status_code == 409:
        _log.warning('Job %i was previously submitted' % video_id)
    else:    
        response.raise_for_status()

    _log.info('Successfully submitted video %s as job %s' %
              (video_id, response.json()['job_id']))
    

def main():
    schedule = parse_schedule(options.schedule)

    segments = get_segment_list(options.working_dir)

    for episode in schedule:
        process_episode(episode, segments)

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
    try:
        main()

    except Exception as e:
        _log.exception('Stiching video')
        statemon.state.increment('turner_stich_errors')
        utils.monitor.send_statemon_data()
        exit(1)

    finally:
        utils.monitor.send_statemon_data()
