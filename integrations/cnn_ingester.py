#!/usr/bin/env python
'''
Takes all CNNIntegration accounts and 
grabs the videos, which end up in our system

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
import functools
import logging
import integrations.brightcove
import integrations.exceptions
import signal
import multiprocessing
import os.path
import re
import time
import tornado.ioloop
import tornado.gen
import urlparse
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon
import utils.sync

define("poll_period", default=300.0, help="Period (s) to poll cnn",
       type=float)

statemon.define('unexpected_exception', int)
statemon.define('unexpected_processing_error', int)
statemon.define('platform_missing', int)
statemon.define('n_integrations', int)
statemon.define('integrations_finished', int)
statemon.define('slow_update', int)

_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def process_one_account(api_key, integration_id, slow_limit=600.0):
    '''Processes one CNN account.'''
    _log.debug('Processing CNN integration for account %s, integration %s'
               % (api_key, integration_id))
    start_time = datetime.datetime.now()

    platform = yield tornado.gen.Task(neondata.CNNIntegration.get, integration_id)

    if platform is None:
        _log.error('Could not find platform %s for account %s' %
                   (integration_id, api_key))
        statemon.state.increment('platform_missing')
        return

    integration = integrations.cnn.CNNIntegration(platform.account_id, platform)

    try:
        yield integration.process_publisher_stream()
    except integrations.exceptions.IntegrationError as e:
        # Exceptions are logged in the integration object already
        pass
    except Exception as e:
        _log.exception(
            'Unexpected exception when processing publisher stream %s'
            % e)
        statemon.state.increment('unexpected_processing_error')

    statemon.state.increment('integrations_finished')
    runtime = (datetime.datetime.now() - start_time).total_seconds()
    log_func = _log.debug
    if runtime > slow_limit:
        statemon.state.increment('slow_update')
        log_func = _log.warn
    log_func('Finished processing account %s, integration %s. Time was %f' %
             (platform.account_id, platform.integration_id, runtime))

class Manager(object):
    def __init__(self):
        self._timers = {} # (api_key, integration_id) -> periodic callback timer
        self.integration_checker = utils.sync.PeriodicCoroutineTimer(
            self.check_integration_list,
            options.poll_period * 1000.)

    def start(self):
        self.integration_checker.start()

    def stop(self):
        self.integration_checker.stop()
                
    @tornado.gen.coroutine
    def check_integration_list(self):
        '''Polls the database for the active integrations.'''
        orig_keys = set(self._timers.keys())

        platforms = yield tornado.gen.Task(
            neondata.CNNIntegration.get_all)
        cur_keys = set([(x.account_id, x.integration_id) for x in platforms
                        if x is not None and x.enabled])
        
        # Schedule callbacks for new integration objects
        new_keys = cur_keys - orig_keys
        for key in new_keys:
            _log.info('Turning on integration (%s,%s)' % key)
            timer = utils.sync.PeriodicCoroutineTimer(
                functools.partial(process_one_account, *key),
                options.poll_period * 1000.)
            timer.start()
            self._timers[key] = timer

        # Remove callbacks for integration objects we are no long watching
        dropped_keys = orig_keys - cur_keys
        for key in dropped_keys:
            _log.info('Turning off integration (%s,%s)' % key)
            timer = self._timers[key]
            timer.stop()
            del self._timers[key]

        statemon.state.n_integrations = len(self._timers)

def main():    
    ioloop = tornado.ioloop.IOLoop.current()
    manager = Manager()
    manager.start()
    
    atexit.register(ioloop.stop)
    atexit.register(manager.stop)
    _log.info('Starting CNN ingester')
    ioloop.start()
    _log.info('Finished program')

if __name__ == "__main__":
    utils.neon.InitNeon()
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    main()
