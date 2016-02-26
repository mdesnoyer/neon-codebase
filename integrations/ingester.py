#!/usr/bin/env python
'''
Ingests changes from Any Service into our system

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
import integrations.cnn
import integrations.fox
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

define("poll_period", default=300.0, help="Period (s) to poll service", type=float)
define("service_name", default=None, help="Which service to start", type=str)

statemon.define('unexpected_exception', int)
statemon.define('unexpected_processing_error', int)
statemon.define('platform_missing', int)
statemon.define('n_integrations', int)
statemon.define('integrations_finished', int)
statemon.define('slow_update', int)

_log = logging.getLogger(__name__)

def get_platform_class(): 
    '''Takes the service_name option and returns 
         the correct neondata.platform/integration object 
    '''
    types = { 
              'fox' : neondata.FoxIntegration, 
              'cnn' : neondata.CNNIntegration, 
              'brightcove' : neondata.BrightcovePlatform 
            } 
    try: 
        return types[options.service_name.lower()] 
    except KeyError as e: 
        _log.error('Service not available for Ingestion : %s' % e)
        raise     

def get_integration_class(): 
    '''Takes the service_name option and returns 
         the correct integrations.integration object 
    ''' 
    types = { 
              'fox' : integrations.fox.FoxIntegration, 
              'cnn' : integrations.cnn.CNNIntegration, 
              'brightcove' : integrations.brightcove.BrightcoveIntegration 
            } 
    try: 
        return types[options.service_name.lower()] 
    except KeyError as e: 
        _log.error('Service not available for Ingestion : %s' % e)
        raise     

@tornado.gen.coroutine
def process_one_account(api_key, integration_id, slow_limit=600.0):
    '''Processes one account.'''
    _log.debug('Processing %s platform for account %s, integration %s'
               % (options.service_name, api_key, integration_id))
    start_time = datetime.datetime.now()
    pi_class = get_platform_class() 
    # TODO hack once these platform objects go away
    # import pdb; pdb.set_trace()
    if options.service_name.lower() == 'brightcove': 
        platform = yield tornado.gen.Task(pi_class.get, api_key, integration_id)
    else: 
        platform = yield tornado.gen.Task(pi_class.get, integration_id)

    if platform is None:
        _log.error('Could not find platform %s for account %s' %
                   (integration_id, api_key))
        statemon.state.increment('platform_missing')
        return

    integration_class = get_integration_class()
    account_id = platform.account_id
    try: 
        if account_id is None or account_id == platform.neon_api_key:
            acct = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                                          platform.neon_api_key)
            account_id = acct.account_id
    except AttributeError: 
        # this is an cmsd_integration object that does not have a neon_api_key
        pass 

    integration = integration_class(account_id, platform)
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
             (api_key, platform.integration_id, runtime))


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
        pi_class = get_platform_class() 
        platforms = yield tornado.gen.Task(pi_class.get_all)
        
        if options.service_name.lower() == 'brightcove': 
            cur_keys = set([(x.neon_api_key, x.integration_id) for x in platforms
                             if x is not None and x.enabled and isinstance(x, pi_class)])
        else: 
            cur_keys = set([(x.account_id, x.integration_id) for x in platforms
                             if x is not None and x.enabled and isinstance(x, pi_class)])
        
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
    _log.info('Starting Ingester')
    ioloop.start()

    _log.info('Finished program')

if __name__ == "__main__":
    utils.neon.InitNeon()
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    main()
