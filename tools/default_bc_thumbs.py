#!/usr/bin/env python

'''
Script that changes any thumbnails with brightcove type to default

This script should not be necessary beyond Aug 2015

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import logging
import utils.neon
from utils.options import define, options

define('api_key', default=None,
       help='If set, only do this account')

_log = logging.getLogger(__name__)

def bc_type_to_default(obj_dict):
    for key, thumb in obj_dict.iteritems():
        if (thumb is not None and 
            thumb.type == neondata.ThumbnailType.BRIGHTCOVE):
            thumb.type = neondata.ThumbnailType.DEFAULT

def main():
    if options.api_key is None:
        accts = neondata.NeonUserAccount.iterate_all()
    else:
        accts = neondata.NeonUserAccount.get_many([options.api_key])
    for acct in accts:
        _log.info('Processing account %s' % acct.get_id())
        n_processed = 0
        for video in neondata.VideoMetadata.get_many(
                acct.get_internal_video_ids()):
            if video is not None:
                neondata.ThumbnailMetadata.modify_many(video.thumbnail_ids,
                                                       bc_type_to_default)
            n_processed += 1
            if n_processed % 1000 == 0:
                _log.info('Processed %i of %i videos for this account' %
                          (n_processed, len(plat.videos)))
            

                

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
