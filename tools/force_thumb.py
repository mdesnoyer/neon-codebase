#!/usr/bin/env python

'''
Script that disables all the thumbs in a video except one.
'''
USAGE='%prog [options] <thumbnail_id0> <thumbnail_id1>....'

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

define('keep_default', default=1, help='if 1, keep the default thumb on')

def main(thumb_ids):
    for thumb_id in thumb_ids:
        _log.info('Forcing thumb %s' % thumb_id)
        keep_thumb = neondata.ThumbnailMetadata.get(thumb_id)
        video = neondata.VideoMetadata.get(keep_thumb.video_id)

        def _disable_thumbs(objs):
            for thumb in objs.itervalues():
                if (options.keep_default and 
                    thumb.type == neondata.ThumbnailType.DEFAULT):
                    continue
                if thumb.key == keep_thumb.key:
                    continue
                thumb.enabled = False
        thumbs = neondata.ThumbnailMetadata.modify_many(video.thumbnail_ids,
                                                        _disable_thumbs)

if __name__ == "__main__":
    args = utils.neon.InitNeon(USAGE)
    main(args)
