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
def subscribe_to_changes():
     
    @tornado.gen.coroutine 
    def change_handler_platform(key, obj, op):
        _log.info('Adding platform object')
        #yield sem.acquire() 
        yield local_queue.put({'type' : 'platform', 'key' : key, 'obj' : obj, 'op' : op})
        #sem.release()  
        yield tornado.gen.sleep(0.01)
         
    @tornado.gen.coroutine 
    def change_handler_apirequest(key, obj, op): 
        _log.info('Adding request object')
        #yield sem.acquire() 
        yield local_queue.put({'type' : 'apirequest', 'key' : key, 'obj' : obj, 'op' : op})  
        #sem.release()  
        yield tornado.gen.sleep(0.01)

    @tornado.gen.coroutine 
    def change_handler_normal(key, obj, op):
        _log.info('Adding normal object')
        #yield sem.acquire() 
        yield local_queue.put({'type' : 'normal', 'key' : key, 'obj' : obj, 'op' : op})
        #sem.release()
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
    options._set('cmsdb.neondata.wants_postgres', 1)

@tornado.gen.coroutine
def producer(queue):
    tornado.ioloop.IOLoop.current().add_callback(lambda: subscribe_to_changes()) 
    while True:  
        current_counter = 0 
        #yield sem.acquire()
        try:
            #_log.info('blam checking local_queue size = %s' % local_queue.qsize())
            obj = local_queue.get_nowait() 
            yield queue.put(obj)   
        except tornado.queues.QueueEmpty as e: 
            yield tornado.gen.sleep(0.01)
          
        #while not local_queue.empty():
        #    obj = yield local_queue.get() 
        #    yield queue.put(obj)
        #    current_counter += 1
        #sem.release()  
        #current_counter = 0
        #yield tornado.gen.sleep(0.01)
        #print 'test' 
