#!/usr/bin/env python
'''
Script that submits video processing jobs from a json file.

The json format should be
{ 'videos': [
    { 'video_id' : some id
      'video_url' : url to the video
      'video_title' : optional video title
      'callback' : optional callback url
    }
  ]
}

Authors: Mark Desnoyer (desnoyer@neon-lab.com)
         David Lea (lea@neon-lab.com)
Copyright 2015 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import json
import logging
import urllib2
import utils.neon

from utils.options import define, options
define('host', default='localhost', help='Host where the cmsapi is')
define('port', default=80, type=int, help='Port where the cmsapi is')
define('api_key', default=None, type=str, help='account api key')
define('account_id', default=None, type=str, help='account id')
define('input', default=None, type=str, help='Input json file')

_log = logging.getLogger(__name__)

def create_neon_api_request(video_id, video_title, video_url, callback_url):
    ''' Send video processing request to Neon '''
    video_api_formater = "http://%s:%s/api/v1/accounts/%s/neon_integrations/0/create_thumbnail_api_request"
    headers = {"X-Neon-API-Key" : options.api_key,
               "Content-Type" : "application/json"}
    request_url = video_api_formater % (options.host, options.port,
                                        options.account_id)

    data =     { 
        "video_id": video_id,
        "video_url": video_url, 
        "video_title": video_title,
        "callback_url": callback_url
    }

    req = urllib2.Request(request_url, headers=headers)
    try:
        res = urllib2.urlopen(req, json.dumps(data))
    except urllib2.HTTPError, e:
        if e.code != 409:
            _log.error('urllib2 HTTPError caught for video %s: %s' % 
                       (video_id, e))
        return "error"
    
    api_resp = json.loads(res.read())
    _log.info('Added job %s for video %s' % (api_resp['job_id'], video_id))
    return api_resp

def get_registered_videos():
    '''Return a list of video ids in the account already.'''
    plat = neondata.NeonPlatform.get(options.api_key, '0')
    return plat.videos.keys()

def main():    
    # Get the json feed
    json_data = json.load(open(options.input))

    known_videos = get_registered_videos()

    _log.info('Adding %d videos to account %s with api_key %s' %
              (len(json_data['videos']), options.account_id, options.api_key))

    count = 0
    for item in json_data['videos']:
        if count % 10 == 0:
            _log.info('Processed %d videos' % count)
        count += 1

        video_id = item['video_id']
        video_title = item.get('video_title', video_id)
        url = item['video_url']
        callback_url = item.get('callback_url', None)

        if video_id in known_videos:
            continue

        create_neon_api_request(video_id, video_title, url, callback_url) 

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
