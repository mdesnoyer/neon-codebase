#!/usr/bin/env python
'''Script to extract scores from an account for each video.

Output is a json of:
{ 'results' : 
  [
    { 
      'video_id' :
      'model' :
      'thumbnails' : 
      [
        { 
          'rank' :
          'score' :
          'url' :
          'frameno' : 
        }
      ]
    }
 ]
}
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import json
import logging
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

define('api_key', default='uwe10loni5x4mbjcqwh4k3ol')
define('output', default='video_scores.json')
define('valid_model', default='20130924_crossfade')
define('model_type', default='automated')

def main():

    data_map = { 'results' : [] }
    
    platform = neondata.NeonPlatform.get(options.api_key, '0')

    video_count = 0
    for video in neondata.VideoMetadata.get_many(
            [neondata.InternalVideoID.generate(options.api_key, x) for x in
             platform.videos.keys()]):
        if video_count % 100 == 0:
            _log.info('Processing video %i' % video_count)
        video_count += 1

        if video is None:
            continue

        thumb_data = []

        for thumb in neondata.ThumbnailMetadata.get_many(
                video.thumbnail_ids):
            if thumb.type != neondata.ThumbnailType.NEON:
                continue
            if thumb.model_version != options.valid_model:
                continue
            if thumb.frameno < 0:
                continue

            thumb_data.append({
                'rank' : thumb.rank,
                'score' : thumb.model_score,
                'url' : thumb.urls[0],
                'frameno' : thumb.frameno
                })
            
        if len(thumb_data) > 0:
            data_map['results'].append({
                'video_id' : neondata.InternalVideoID.to_external(video.key),
                'model' : options.model_type,
                'thumbnails' : sorted(thumb_data, key=lambda x: x['rank'])
                })

    _log.info('Writing data to %s' % options.output)
    with open(options.output, 'w') as outstream:
        json.dump(data_map, outstream)

    

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()

    
    
