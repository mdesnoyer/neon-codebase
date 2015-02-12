#!/usr/bin/python

'''
DB consistency check for all brightcvoe & neon platform videos

- video check
verify that videos present in the account.videos dictionary is 
valid and present in the database

- thumbnail check
verify that the thumbnails present in the videometadata object
is present in the database

By default the script runs in a dry_run mode and prints out all the 
inconsistent videos and thumbnails ids

'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import os
from cmsdb import neondata 
import json
import urllib2
import utils.neon
import utils.monitor
from utils.options import define, options
from utils import statemon

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define('dry_run', default=True, help='dry run?')


def main():
    try:
        # Get all Brightcove & Neon accounts
        accounts = neondata.BrightcovePlatform.get_all_instances()
        accounts.extend(neondata.NeonPlatform.get_all_instances())
        for accnt in accounts:
            mod = False # account modified ?
            videos = accnt.get_videos()
            if not videos:
                continue
            for vid in videos:
                i_vid = accnt.neon_api_key + '_' + vid
                vm = neondata.VideoMetadata.get(i_vid)
                if not vm:
                    _log.info("video not found %s" % i_vid)
                    accnt.videos.pop(vid)
                    mod = True
                else:
                    tds = neondata.ThumbnailMetadata.get_many(
                        vm.thumbnail_ids)
                    for td, tid in zip(tds, vm.thumbnail_ids):
                        if not td:
                            _log.info("no tid %s for vid %s" % (tid, i_vid))
                            vm.thumbnail_ids.remove(tid)
                    if options.dry_run == False :
                        vm.save()
            if mod and options.dry_run == False:
                accnt.save()

    except Exception as e:
        print e

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
