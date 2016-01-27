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
import utils 
from utils.options import define, options

_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def consumer(queue):
    @tornado.gen.coroutine
    def change_handler_normal(key, obj, op):
        # if we already have the data in the database 
        # modify this thing, otherwise just save it
        # we will always overwrite on save, because modifies 
        # should take precedence over the move_data script 
        def modify_me(x): 
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key
        if op == 'set':
            try:
                if obj: 
                    options._set('cmsdb.neondata.wants_postgres', 1)  
                    yield obj.modify(key, modify_me, create_missing=True, async=True) 
                    options._set('cmsdb.neondata.wants_postgres', 0)  
                    _log.info('saving changing object %s' % obj.__class__.__name__)  
            except Exception as e: 
                _log.error('exception while saving changing key %s : %s' % (key, e))
    
    @tornado.gen.coroutine 
    def change_handler_apirequest(key, obj, op): 
        def modify_me(x): 
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key 
        if op == 'set':
            try:
                options._set('cmsdb.neondata.wants_postgres', 1)  
                yield obj.modify(obj.api_key, 
                                 obj.job_id, 
                                 modify_me, 
                                 create_missing=True,
                                 async=True)
                options._set('cmsdb.neondata.wants_postgres', 0)  
                _log.info('saving changing object neonapirequest')  
            except Exception as e: 
                _log.error('exception while saving changing request %s : %s' % (obj, e))
                pass
        
    @tornado.gen.coroutine
    def change_handler_platform(key, obj, op):
        # since platforms are keyed differently we will 
        # just use another handler  
        obj.key.replace('brightcoveplatform', 'abstractplatform') 
        obj.key.replace('neonplatform', 'abstractplatform') 
        obj.key.replace('ooyalaplatform', 'abstractplatform') 
        obj.key.replace('youtubeplatform', 'abstractplatform')
        def modify_me(x):
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key
            x.videos = {}  
        if op == 'set':
            try:
                options._set('cmsdb.neondata.wants_postgres', 1)  
                yield obj.modify(obj.neon_api_key, 
                                 obj.integration_id, 
                                 modify_me, 
                                 create_missing=True,
                                 async=True) 
                options._set('cmsdb.neondata.wants_postgres', 0)  
                _log.info('saving changing object platform')  
            except Exception as e: 
                _log.error('exception while saving changing platform %s : %s' % (obj, e))
                pass

    while True: 
        item = yield queue.get()
        try: 
            _log.info('this is the item %s' % (item))
            if item['type'] is 'normal': 
                yield change_handler_normal(item['key'], item['obj'], item['op']) 
            if item['type'] is 'apirequest': 
                yield change_handler_apirequest(item['key'], item['obj'], item['op']) 
            if item['type'] is 'platform': 
                yield change_handler_platform(item['key'], item['obj'], item['op']) 
            yield tornado.gen.sleep(0.01)  
        finally: 
            queue.task_done()   
