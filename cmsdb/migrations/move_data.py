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

#define("pg_db_name", default="cmsdb", type=str, help="PG Database Name")
#define("pg_db_user", default="pgadmin", type=str, help="PG Database User")
#define("pg_db_port", default=5432, type=int, help="PG Database Port")

def move_abstract_integrations(): 
    ''' 
         moves all the Integrations, saves them as 
         abstractintegration in postgres 
    '''  
    integrations = neondata.AbstractIntegration.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    for i in integrations: 
        try: 
            i.save()
        except Exception as e: 
            _log.exception('Error saving integration %s to postgres %s' % (i,e))  
            pass 

def move_abstract_platforms(): 
    ''' 
        moves all the platforms, saves them as the platform class 
        name in postgres 
    '''  
    platforms = neondata.AbstractPlatform.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    for p in platforms: 
        try: 
            p.modify(p.key, p.integration_id, lambda x: x, create_missing=True)
        except Exception as e:
            _log.exception('Error saving platform %s to postgres %s' % (p,e)) 
            pass  

def move_experiment_strategies(): 
    ''' 
        moves all the ExperimentStrategies
    '''  
    strategies = neondata.ExperimentStrategy.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    for s in strategies: 
        try: 
            s.save()
        except Exception as e:
            _log.exception('Error saving exp stratgey %s to postgres %s' % (p,e)) 
            pass  

def move_neon_user_accounts():
    ''' 
        moves all the NeonUserAccounts, NeonApiKeys
    '''  
    accts = neondata.NeonUserAccount.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1) 
    for acct in accts:
        try: 
            acct.save() 
            api_key = neondata.NeonApiKey(acct.account_id, 
                                          acct.neon_api_key) 
            api_key.save() 
        except Exception as e: 
            _log.exception('Error saving account %s to postgres %s' % (acct,e))
            pass  

def move_cdn_hosting_metadata_lists(): 
    ''' 
        moves all the CDNHostingMetadataLists 
    '''  
    cdns = neondata.CDNHostingMetadataList.get_all() 
    options._set('cmsdb.neondata.wants_postgres', 1)
    for c in cdns: 
        try: 
            c.save() 
        except Exception as e:
            _log.exception('Error saving cdnhostingmetadatalist %s to postgres %s' % (p,e)) 
            pass  
 
def move_neon_videos_and_thumbnails():
    ''' 
        move VideoMetadata, ThumbnailMetadata, ThumbnailStatus 
             VideoStatus
    '''  
    accts = neondata.NeonUserAccount.get_all()
    for acct in accts: 
        videos = list(acct.iterate_all_videos())
        for v in videos:
            options._set('cmsdb.neondata.wants_postgres', 0) 
            try: 
                tnails = neondata.ThumbnailMetadata.get_many(v.thumbnail_ids)
                api_request = neondata.NeonApiRequest.get(v.job_id, acct.neon_api_key)
                tnail_statuses = neondata.ThumbnailStatus.get_many(v.thumbnail_ids)
                video_status = neondata.VideoStatus.get(v.key)  
                options._set('cmsdb.neondata.wants_postgres', 1) 
                for t in tnails: 
                    t.save() 
                for ts in tnail_statuses: 
                    ts.save() 
                video_status.save() 
                v.save()
                api_request.save() 
            except Exception as e: 
                _log.exception('Error saving video %s to postgres %s' % (v,e)) 
                pass

def main():
    move_neon_user_accounts()
    move_neon_videos_and_thumbnails()
    move_abstract_integrations()
    move_abstract_platforms()
    move_cdn_hosting_metadata_lists()
    move_experiment_strategies()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
