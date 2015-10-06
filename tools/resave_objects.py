#!/usr/bin/env python
'''
Script that resaves a bunch of objects in order to migrate the database.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__),  '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import datetime
import logging
import time
import tornado.ioloop
import utils.http
import utils.neon
from utils.options import options, define

define("types", default=None, type=str,
       help="CSV list of object types to resave in the neondata namespace")

_log = logging.getLogger(__name__)


BATCH_SIZE = 100
def _update_time(d):
    for x in d.itervalues():
        x.updated = str(datetime.datetime.utcnow())

def resave_namespaced_objects(cls):
    _log.info('Resave all objects of type %s' % cls.__name__)

    keys = cls.get_all_keys()
    for i in range(0, len(keys), BATCH_SIZE):
        cls.modify_many(keys[i:(i+BATCH_SIZE)], _update_time)
        if i % 1000 == 0:
            _log.info('Processed %i %s objects' % (i, cls.__name__))
        # Don't hurt the database
        time.sleep(0.1)

    _log.info('Done saving all objects of type %s' % cls.__name__)

def resave_videos_and_thumbs(save_videos, save_thumbs):
    _log.info('Iterating through accounts to save videos and thumbs')

    for platform in neondata.AbstractPlatform.get_all():
        _log.info('Processing platform %s for account %s' % 
                  (platform.integration_id, platform.neon_api_key))
        
        video_ids = platform.get_internal_video_ids()  
        for i in range(0, len(video_ids), BATCH_SIZE):
            cur_vids = video_ids[i:(i+BATCH_SIZE)]
            if save_videos:
                videos = neondata.VideoMetadata.modify_many(cur_vids,
                                                            _update_time)
                videos = videos.values()
            else:
                videos = neondata.VideoMetadata.get_many(cur_vids)

            if save_thumbs:
                for video in videos:
                    neondata.ThumbnailMetadata.modify_many(video.thumbnail_ids,
                                                           _update_time)
            if i % 1000 == 0:
                _log.info('Processed %i videos for account %s and platform %s'
                          % (i, platform.neon_api_key, platform.integration_id))
            # Don't hurt the database
            time.sleep(0.1)

    _log.info('Done saving videos and thumbs')

def main():
    types = options.types.split(',')

    save_videos = 'VideoMetadata' in types
    save_thumbs = 'ThumbnailMetadata' in types
    if save_videos or save_thumbs:
        try:
            types.remove('VideoMetadata')
            types.remove('ThumbnailMetadata')
        except ValueError:
            pass
        resave_videos_and_thumbs(save_videos, save_thumbs)
    
    for typename in types:
        resave_namespaced_objects(getattr(neondata, typename))


if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
