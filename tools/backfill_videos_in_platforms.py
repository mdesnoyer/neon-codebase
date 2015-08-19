#!/usr/bin/env python

'''
Script that adds any videos that are missing from the *Platform objects

This script should not be necessary beyond June 2015

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

def main():
    if options.api_key is None:
        accts = neondata.NeonUserAccount.iterate_all()
    else:
        accts = neondata.NeonUserAccount.get_many([options.api_key])
    for acct in accts:
        _log.info('Processing account %s' % acct.get_id())
        for plat in acct.get_platforms():
            vids_to_add = [] # (video_id, job_id)
            vids_to_change_iid = []
            for video in acct.iterate_all_videos():
                external_id = neondata.InternalVideoID.to_external(video.key)
                if video.integration_id != plat.integration_id:
                    # Not the right platform for this video
                    continue

                elif external_id not in plat.videos:
                    vids_to_add.append((external_id,
                                        video.job_id))

            if len(vids_to_add) > 0:
                _log.info('Adding %i videos to platform %s' % 
                          (len(vids_to_add), plat.get_id()))
                def _add_vids(x):
                    for vid, job_id in vids_to_add:
                        x.add_video(vid, job_id)
                plat.__class__.modify(plat.neon_api_key, plat.integration_id,
                                      _add_vids)

                

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
