#!/usr/bin/env python
'''
    Script that listens for changes in our Redis store and 
        moves it to Postgres  
''' 
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import logging
import datetime
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
from tornado.locks import Semaphore
from tornado.queues import Queue
import utils 
from utils.options import define, options
#import Queue 

_log = logging.getLogger(__name__)
local_queue = Queue() 
sem = Semaphore(1)
 
@tornado.gen.coroutine
def producer(queue):
    @tornado.gen.coroutine 
    def change_handler_platform(key, obj, op):
        _log.info_n('Adding platform object', 10)
        try:  
            yield queue.put({'type' : 'platform', 'key' : key, 'obj' : obj, 'op' : op})
        except Exception as e: 
            _log.error('exception occured puting to local queue') 
            yield tornado.gen.sleep(0.01)
         
    @tornado.gen.coroutine 
    def change_handler_apirequest(key, obj, op): 
        _log.info_n('Adding request object', 10)
        try:  
            yield queue.put({'type' : 'apirequest', 'key' : key, 'obj' : obj, 'op' : op})  
        except Exception as e: 
            _log.error('exception occured puting to local queue') 
            yield tornado.gen.sleep(0.01)

    @tornado.gen.coroutine 
    def change_handler_normal(key, obj, op):
        _log.info_n('Adding normal object', 10)
        try: 
            yield queue.put({'type' : 'normal', 'key' : key, 'obj' : obj, 'op' : op})
        except Exception as e: 
            _log.error('exception occured puting to local queue') 
            yield tornado.gen.sleep(0.01)
     
    options._set('cmsdb.neondata.wants_postgres', 0)
    yield neondata.AbstractIntegration.subscribe_to_changes(change_handler_normal, async=True)
    yield neondata.CDNHostingMetadataList.subscribe_to_changes(change_handler_normal, async=True)
    yield neondata.ExperimentStrategy.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.NeonApiKey.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.OoyalaIntegration.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.NeonApiRequest.subscribe_to_changes(change_handler_apirequest, async=True) 
    yield neondata.ThumbnailMetadata.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.ThumbnailServingURLs.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.ThumbnailStatus.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.VideoMetadata.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.VideoStatus.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.AbstractPlatform.subscribe_to_changes(change_handler_platform, async=True)
    yield neondata.TrackerAccountIDMapper.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.User.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.NeonUserAccount.subscribe_to_changes(change_handler_normal, async=True) 
    yield neondata.ProcessingStrategy.subscribe_to_changes(change_handler_normal, async=True) 
    options._set('cmsdb.neondata.wants_postgres', 1)
