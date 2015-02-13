#!/usr/bin/env python
'''Script to extract scores from an account for each video.

Output is a csv of:
<video_id>,<best_thumb_score>,<frameno>,<best_image_url>
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import logging
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

define('api_key', default='uwe10loni5x4mbjcqwh4k3ol')
define('output', default='video_scores.txt')

def main():
    _log.info('Writing data to %s' % options.output)
    with open(options.output, 'w') as outstream:
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

            best_thumb = None
            best_score = float('-inf')

            for thumb in neondata.ThumbnailMetadata.get_many(
                    video.thumbnail_ids):
                if thumb.type != neondata.ThumbnailType.NEON:
                    continue
                if thumb.model_score > best_score:
                    best_score = thumb.model_score
                    best_thumb = thumb

            if best_thumb is not None:
                outstream.write('%s,%.5f,%i,%s\n' % (
                    neondata.InternalVideoID.to_external(video.key),
                    best_score,
                    best_thumb.frameno,
                    best_thumb.urls[0]))
                    

    

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()

    
    
