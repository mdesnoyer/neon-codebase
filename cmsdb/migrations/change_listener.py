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
import utils
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

@tornado.gen.coroutine 
def subscribe_to_db_changes(): 
    @tornado.gen.coroutine
    def change_handler_normal(key, obj, op):
        # if we already have the data in the database 
        # modify this thing, otherwise leave it alone
        options._set('cmsdb.neondata.wants_postgres', 1)
        def modify_me(x): 
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key
        if op == 'set':
            try: 
                yield obj.modify(key, modify_me, async=True) 
            except Exception as e: 
                _log.error('exception while saving changing object %s : %s' % (obj, e))
                pass  
        options._set('cmsdb.neondata.wants_postgres', 0)

    @tornado.gen.coroutine
    def change_handler_platform(key, obj, op):
        # since platforms are keyed differently we will 
        # just use another handler  
        obj.key.replace('brightcoveplatform', 'abstractplatform') 
        obj.key.replace('neonplatform', 'abstractplatform') 
        obj.key.replace('ooyalaplatform', 'abstractplatform') 
        obj.key.replace('youtubeplatform', 'abstractplatform') 
        options._set('cmsdb.neondata.wants_postgres', 1)
        def modify_me(x):
            current_key = x.key 
            x.__dict__ = obj.__dict__
            x.key = current_key
        if op == 'set':
            try: 
                yield obj.modify(obj.neon_api_key, 
                                 obj.integration_id, 
                                 modify_me, 
                                 create_missing=True,
                                 async=True) 
            except Exception as e: 
                _log.error('exception while saving changing platform %s : %s' % (obj, e))
                pass
        options._set('cmsdb.neondata.wants_postgres', 0)

    neondata.NeonUserAccount.subscribe_to_changes(change_handler_normal)
    neondata.AbstractIntegration.subscribe_to_changes(change_handler_normal)
    neondata.CDNHostingMetadataList.subscribe_to_changes(change_handler_normal)
    neondata.ExperimentStrategy.subscribe_to_changes(change_handler_normal) 
    neondata.NeonApiKey.subscribe_to_changes(change_handler_normal) 
    neondata.OoyalaIntegration.subscribe_to_changes(change_handler_normal) 
    neondata.NeonApiRequest.subscribe_to_changes(change_handler_normal) 
    neondata.ThumbnailMetadata.subscribe_to_changes(change_handler_normal) 
    neondata.ThumbnailServingURLs.subscribe_to_changes(change_handler_normal) 
    neondata.ThumbnailStatus.subscribe_to_changes(change_handler_normal) 
    neondata.VideoMetadata.subscribe_to_changes(change_handler_normal) 
    neondata.VideoStatus.subscribe_to_changes(change_handler_normal) 

    neondata.AbstractPlatform.subscribe_to_changes(change_handler_platform)
     
def main():
    subscribe_to_db_changes()  
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
