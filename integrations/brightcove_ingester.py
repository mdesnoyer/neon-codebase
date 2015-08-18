#!/usr/bin/env python
'''
Ingests changes from Brightcove into our system

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
from cmsdb import neondata
import datetime
import logging
import integrations.brightcove
import integrations.exceptions
import signal
import multiprocessing
import os.path
import re
import tornado.ioloop
import tornado.gen
import urlparse
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon

define("poll_period", default=300, help="Period (s) to poll brightcove",
       type=int)

statemon.define('cycles_complete', int)
statemon.define('unexpected_exception', int)
statemon.define('unexpected_processing_error', int)
statemon.define('cycle_runtime', float)

_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def process_one_account(platform):
    '''Processes one Brightcove account.'''
    _log.debug('Processing Brightcove platform for account %s, integration %s'
               % (platform.neon_api_key, platform.integration_id))

    account_id = platform.account_id
    if account_id is None or account_id == platform.neon_api_key:
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                                      platform.neon_api_key)
        account_id = acct.account_id

    integration = integrations.brightcove.BrightcoveIntegration(
        account_id, platform)
    try:
        yield integration.process_publisher_stream()
    except integrations.exceptions.IntegrationError as e:
        # Exceptions are logged in the integration object already
        pass
    except Exception as e:
        _log.error('Unexpected exception when processing publisher stream %s'
                   % e)
        statemon.state.increment('unexpected_processing_error')

@tornado.gen.coroutine
def run_one_cycle():
    platforms = yield tornado.gen.Task(
        neondata.BrightcovePlatform.get_all)
    yield [process_one_account(x) for x in platforms if 
           x is not None and x.enabled]

@tornado.gen.coroutine
def main(run_flag):
    while run_flag.is_set():
        start_time = datetime.datetime.now()
        try:
            yield run_one_cycle()
            statemon.state.increment('cycles_complete')
        except Exception as e:
            _log.exception('Unexpected exception when ingesting from '
                           'Brightcove')
            statemon.state.increment('unexpected_exception')
        cycle_runtime = (datetime.datetime.now() - start_time).total_seconds()
        statemon.state.cycle_runtime = cycle_runtime

        if cycle_runtime < options.poll_period:
            yield tornado.gen.sleep(options.poll_period - cycle_runtime)
            
    _log.info('Finished program')

if __name__ == "__main__":
    utils.neon.InitNeon()
    run_flag = multiprocessing.Event()
    run_flag.set()
    atexit.register(utils.ps.shutdown_children)
    atexit.register(run_flag.clear)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    
    tornado.ioloop.IOLoop.current().run_sync(lambda: main(run_flag))
