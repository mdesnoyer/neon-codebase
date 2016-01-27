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
from contextlib import closing
import copy 
import logging
import psycopg2
from subprocess import call
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
from tornado.locks import Semaphore
import utils
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

sem = Semaphore(1) 

@tornado.gen.coroutine 
def subscribe_to_db_changes(): 
    @tornado.gen.coroutine
    def change_handler_normal(key, obj, op):
        # if we already have the data in the database 
        # modify this thing, otherwise just save it
        # we will always overwrite on save, because modifies 
        # should take precedence over the move_data script 
        yield sem.acquire()  
        options._set('cmsdb.neondata.wants_postgres', 1)
        def modify_me(x): 
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key
        if op == 'set':
            try:
                #get_obj = yield obj.get(key, async=True)
                #if get_obj: 
                if obj:  
                    yield obj.modify(key, modify_me, create_missing=True, async=True) 
                    _log.info('saving changing object %s' % obj.__class__.__name__)  
                #else: 
                #    yield obj.save(async=True) 
            except Exception as e: 
                _log.error('exception while saving changing key %s : %s' % (key, e))
            finally:  
                options._set('cmsdb.neondata.wants_postgres', 0)
                sem.release() 
    
    @tornado.gen.coroutine 
    def change_handler_apirequest(key, obj, op): 
        def modify_me(x): 
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key 
        yield sem.acquire()  
        options._set('cmsdb.neondata.wants_postgres', 1)
        if op == 'set':
            try:
                yield obj.modify(obj.api_key, 
                                 obj.job_id, 
                                 modify_me, 
                                 create_missing=True,
                                 async=True)
                _log.info('saving changing object neonapirequest')  
            except Exception as e: 
                _log.error('exception while saving changing request %s : %s' % (obj, e))
                pass
        options._set('cmsdb.neondata.wants_postgres', 0)
        sem.release() 
        
    @tornado.gen.coroutine
    def change_handler_platform(key, obj, op):
        # since platforms are keyed differently we will 
        # just use another handler  
        obj.key.replace('brightcoveplatform', 'abstractplatform') 
        obj.key.replace('neonplatform', 'abstractplatform') 
        obj.key.replace('ooyalaplatform', 'abstractplatform') 
        obj.key.replace('youtubeplatform', 'abstractplatform')
        yield sem.acquire()  
        options._set('cmsdb.neondata.wants_postgres', 1)
        def modify_me(x):
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key
            x.videos = {}  
        if op == 'set':
            try:
                yield obj.modify(obj.neon_api_key, 
                                 obj.integration_id, 
                                 modify_me, 
                                 create_missing=True,
                                 async=True) 
                _log.info('saving changing object platform')  
            except Exception as e: 
                _log.error('exception while saving changing platform %s : %s' % (obj, e))
                pass
        options._set('cmsdb.neondata.wants_postgres', 0)
        sem.release() 
   
     
    yield neondata.NeonUserAccount.subscribe_to_changes(change_handler_normal, async=True)
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
     
def main():
    subscribe_to_db_changes()  
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
