#!/usr/bin/env python
'''
Script that changes the integration id for all the video objects in an account

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import logging
import itertools
import time
import tornado.ioloop
import utils.http
import utils.neon

from utils.options import define, options
define('api_key', default=None, type=str,
       help='The account to change the videos for')
define('integration_id', default=None, type=str,
       help='The integration id')

_log = logging.getLogger(__name__)

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

@tornado.gen.coroutine
def main():
    plat = neondata.BrightcovePlatform.get(options.api_key,
                                           options.integration_id)

    internal_video_ids = plat.get_internal_video_ids()
    n_processed = 0
    GROUP_SIZE = 20

    for video_ids in grouper(internal_video_ids, GROUP_SIZE):
        def _setiid(d):
            for k, v in d.iteritems():
                v.integration_id = plat.integration_id

        neondata.VideoMetadata.modify_many(
            [x for x in video_ids if x is not None],
            _setiid)

        n_processed += GROUP_SIZE
        if n_processed % (GROUP_SIZE*10) == 0:
            _log.info('Processed %d videos' % n_processed)

if __name__ == '__main__':
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop.current().run_sync(main)
