#!/usr/bin/env python
'''
Script that submits a lot of Discovery jobs for backprocessing

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cmsdb.cdnhosting
from cmsdb import neondata
import utils.neon
import cvutils.imageutils
import utils.neon

API_KEY = 'gvs3vytvg20ozp78rolqmdfa'

def main():
    for line in open('/tmp/ids2.txt'):
        vid = neondata.VideoMetadata.get(neondata.InternalVideoID.generate(
            'gvs3vytvg20ozp78rolqmdfa', line.strip()))

        thumbs = neondata.ThumbnailMetadata.get_many(vid.thumbnail_ids)

        default_thumbs = [x for x in thumbs if x.type == 'default']
        default_thumbs = sorted(default_thumbs)

        def_thumb = default_thumbs[0]

        serving_urls = neondata.ThumbnailServingURLs.get(def_thumb.get_id())

        print serving_urls.get_serving_url(337, 337)

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
