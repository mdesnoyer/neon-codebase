#!/usr/bin/env python
''' 
''' 
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
from contextlib import closing
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

define("pg_db_name", default="cmsdb", type=str, help="PG Database Name")
define("pg_db_user", default="pgadmin", type=str, help="PG Database User")
#define("pg_db_host", default="10.0.51.11", type=str, help="PG Database Hostname")
define("pg_db_port", default=5432, type=int, help="PG Database Port")

#define("redis_db_host", default="10.0.77.74", type=str, help="Redis Database Host") 
#define("redis_db_port", default=6379, type=int, help="Redis Database Port") 

#@tornado.gen.coroutine
def move_neon_user_accounts():
    accts = neondata.NeonUserAccount.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1) 
    #import pdb; pdb.set_trace() 
    for acct in accts:
        acct.save()  
        print acct 
        
    print 'moving neon user accounts' 

def move_neon_videos_and_thumbnails(): 
    accts = neondata.NeonUserAccount.get_all()
    for acct in accts: 
        videos = list(acct.iterate_all_videos())
        for v in videos:
            tnails = neondata.ThumbnailMetadata.get_many(v.thumbnail_ids)
            options._set('cmsdb.neondata.wants_postgres', 1) 
            for t in tnails: 
                t.save() 
            v.save()
            print v  
            options._set('cmsdb.neondata.wants_postgres', 0) 
    #tnails = neondata.VideoMetadata.iterate_all()
    #for tnail in tnails:
    #    import pdb; pdb.set_trace() 
    #    tnail.save() 
    #    options._set('cmsdb.neondata.wants_postgres', 0) 
    #    print tnail  

def main(): 
    #move_neon_user_accounts()
    move_neon_videos_and_thumbnails()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
