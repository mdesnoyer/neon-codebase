#!/usr/bin/env python
'''
Script that submits old brightcove videos to be processed

Takes an input file with one brightcove video id per line

Authors: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
USAGE='%prog [options] <input_file>'

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from api import brightcove_api
from cmsdb import neondata
import integrations.brightcove
import json
import logging
import time
import tornado.gen
import tornado.ioloop
import utils.http
import utils.neon
from utils.options import define, options

define("max_submit_rate", default=10, help="Max job submit rate per min",
       type=int)
define("api_key", default=None, help="Account api key where the videos are",
       type=str)
define("integration_id", default=None, type=str, 
       help="Integration id of the Brightcove account")

_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def main(input_file):
    with open(input_file) as in_stream:
        bcove_vids = [line.strip() for line in in_stream]

    account = neondata.NeonUserAccount.get(options.api_key)
    platform = neondata.BrightcovePlatform.get(options.api_key,
                                               options.integration_id)
    done_vids = set(platform.get_videos())
    bcove_vids = [x for x in bcove_vids if x not in done_vids]
    bc_integration = integrations.brightcove.BrightcoveIntegration(
        account.account_id,
        platform)

    _log.info('Found %i video ids to process for api key %s integration id %s'
              % (len(bcove_vids), options.api_key, options.integration_id))

    while len(bcove_vids) > 0:
        cur_vids = bcove_vids[0:10]
        bcove_vids = bcove_vids[10:]

        yield bc_integration.lookup_and_submit_videos(cur_vids)
        _log.info('%i videos left' % len(bcove_vids))
        time.sleep(60.0)
            

if __name__ == "__main__":
    args = utils.neon.InitNeon(USAGE)
    input_file = args[0]

    tornado.ioloop.IOLoop.current().run_sync(lambda: main(input_file))
