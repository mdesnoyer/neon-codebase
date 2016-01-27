#!/usr/bin/env python
'''
    Script responsible for moving everything in our Redis store to 
       Postgres on Amazon RDS  
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
import utils
import utils.neon
from utils.options import define, options

_log = logging.getLogger(__name__)

def move_abstract_integrations(): 
    ''' 
         moves all the Integrations, saves them as 
         abstractintegration in postgres 
    '''  
    integrations = neondata.AbstractIntegration.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    _log.info('Processing %d AbstractIntegrations...' % len(integrations))
    counter = 0
    for i in integrations: 
        try: 
            i.save(overwrite_existing_object=False)
        except Exception as e: 
            _log.exception('Error saving integration %s to postgres %s' % (i,e))  
            pass
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing integration %d...' % counter)
    _log.info('All Done Processing AbstractIntegrations')  
    options._set('cmsdb.neondata.wants_postgres', 0)

def move_abstract_platforms(): 
    ''' 
        moves all the platforms, saves them as the platform class 
        name in postgres 
    '''  
    platforms = neondata.AbstractPlatform.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    def modify_me(x):
        current_key = x.key  
        x.__dict__ = p.__dict__ 
        x.key = current_key
        x.__dict__['videos'] = {}  
    _log.info('Processing %d AbstractPlatforms...' % len(platforms))
    counter = 0  
    for p in platforms:
        try:
            p.videos = {}  
            p.modify(p.key, p.integration_id, modify_me, create_missing=True)
        except Exception as e:
            _log.exception('Error saving platform %s to postgres %s' % (p,e)) 
            pass  
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing platform %d...' % counter)
    _log.info('All Done Processing AbstractPlatforms')  
    options._set('cmsdb.neondata.wants_postgres', 0)

def move_experiment_strategies(): 
    ''' 
        moves all the ExperimentStrategies
    '''  
    strategies = neondata.ExperimentStrategy.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    for s in strategies: 
        try: 
            s.save(overwrite_existing_object=False)
        except Exception as e:
            _log.exception('Error saving exp stratgey %s to postgres %s' % (p,e)) 
            pass  
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing experiment strategy %d...' % counter)
    _log.info('All Done Processing ExperimentStrategies')  
    options._set('cmsdb.neondata.wants_postgres', 0)

def move_neon_user_accounts():
    ''' 
        moves all the NeonUserAccounts, NeonApiKeys
    '''  
    accts = neondata.NeonUserAccount.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1) 
    _log.info('Processing %d NeonUserAccounts ...' % len(accts))
    counter = 0
    for acct in accts:
        try:
            acct.save(overwrite_existing_object=False) 
            api_key = neondata.NeonApiKey(acct.account_id, 
                                          acct.neon_api_key) 
            api_key.save(overwrite_existing_object=False) 
        except Exception as e: 
            _log.exception('Error saving account %s to postgres %s' % (acct,e))
            pass 
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing neonuseraccount %d...' % counter)
    _log.info('All Done Processing NeonUserAccounts')  
    options._set('cmsdb.neondata.wants_postgres', 0)

def move_users(): 
    users = neondata.User.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    _log.info('Processing %d Users...' % len(users))
    counter = 0
    for user in users:
        try: 
            user.save(overwrite_existing_object=False) 
        except Exception as e: 
            _log.exception('Error saving User %s to postgres %s' % (user,e))
            pass
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing user %d...' % counter)
    _log.info('All Done Processing Users')  
    options._set('cmsdb.neondata.wants_postgres', 0)

def move_tracker_account_id_mappers(): 
    taidms = neondata.TrackerAccountIDMapper.get_all() 
    options._set('cmsdb.neondata.wants_postgres', 1)
    _log.info('Processing %d TrackerAccountIDMappers ...' % len(taidms))
    counter = 0
    for taidm in taidms: 
        try: 
            taidm.save(overwrite_existing_object=False) 
        except Exception as e: 
            _log.exception('Error saving trackeraccountidmapper %s to postgres %s' % (taidm,e))
            pass
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing tracker account id mapper %d...' % counter)
    _log.info('All Done Processing TrackerAccountIDMappers')  
    options._set('cmsdb.neondata.wants_postgres', 0)

def move_processing_strategies(): 
    pss = neondata.ProcessingStrategy.get_all()
    options._set('cmsdb.neondata.wants_postgres', 1)
    _log.info('Processing %d ProcessingStrategies ...' % len(pss))
    counter = 0
    for ps in pss: 
        try: 
            ps.save(overwrite_existing_object=False) 
        except Exception as e: 
            _log.exception('Error saving processing strategy %s to postgres %s' % (ps,e))
            pass
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing processing strategy %d...' % counter)
    _log.info('All Done Processing ProcessingStrategies')  
    options._set('cmsdb.neondata.wants_postgres', 0)
    

def move_cdn_hosting_metadata_lists(): 
    ''' 
        moves all the CDNHostingMetadataLists 
    '''  
    cdns = neondata.CDNHostingMetadataList.get_all() 
    options._set('cmsdb.neondata.wants_postgres', 1)
    _log.info('Processing %d CDNHostingMetadataLists ...' % len(cdns))
    counter = 0
    for c in cdns: 
        try: 
            c.save(overwrite_existing_object=False) 
        except Exception as e:
            _log.exception('Error saving cdnhostingmetadatalist %s to postgres %s' % (p,e)) 
            pass  
        counter += 1 
        if counter % 10 is 0:
            _log.info('Processing CDN Hosting Metadata List %d...' % counter)
    _log.info('All Done Processing CDNHostingMetadataLists')  
    options._set('cmsdb.neondata.wants_postgres', 0)
 
def move_neon_videos_and_thumbnails():
    ''' 
        move VideoMetadata, ThumbnailMetadata, ThumbnailStatus 
             VideoStatus
    '''  
    accts = neondata.NeonUserAccount.get_all()
    _log.info('Processing Videos for %d Accounts ...' % len(accts))
    account_counter = 0 
    for acct in accts:
        video_counter = 0
        _log.info('Processing Videos for Account : %s ...' % acct.neon_api_key)
        account_counter += 1 
        _log.info('Currently processing account %d ...' % account_counter)
        for v in acct.iterate_all_videos():
            options._set('cmsdb.neondata.wants_postgres', 0)
            video_counter += 1
            if video_counter % 5 is 0:
                _log.info('Done Processing %d videos for Account : %s ...' % (video_counter, acct.neon_api_key))
            try:
                tnails = neondata.ThumbnailMetadata.get_many(v.thumbnail_ids)
                if v.job_id: 
                    api_request = neondata.NeonApiRequest.get(v.job_id, acct.neon_api_key)
                tnail_statuses = neondata.ThumbnailStatus.get_many(v.thumbnail_ids)
                tnail_serving_urls = neondata.ThumbnailServingURLs.get_many(v.thumbnail_ids) 
                video_status = neondata.VideoStatus.get(v.key)  
                options._set('cmsdb.neondata.wants_postgres', 1) 
                for t in tnails:
                    try:  
                        if t: 
                            t.save(overwrite_existing_object=False) 
                    except Exception as e: 
                        _log.exception('Error saving thumbnail %s to postgres %s' % (t,e)) 
                        pass 
                for ts in tnail_statuses:
                    try: 
                        if ts:  
                            ts.save(overwrite_existing_object=False) 
                    except Exception as e: 
                        _log.exception('Error saving thumbnail_status %s to postgres %s' % (ts,e)) 
                        pass 
                for tsu in tnail_serving_urls:
                    try: 
                        if tsu:  
                            tsu.save(overwrite_existing_object=False) 
                    except Exception as e:
                        _log.exception('Error saving thumbnail_serving_url %s to postgres %s' % (tsu,e)) 
                        pass
                try:  
                    video_status.save(overwrite_existing_object=False) 
                except Exception as e: 
                    _log.exception('Error saving video_status %s to postgres %s' % (video_status,e)) 
                    pass
                
                try: 
                    v.save(overwrite_existing_object=False)
                except Exception as e:
                    _log.exception('Error saving video %s to postgres %s' % (v,e)) 
                    pass

                try: 
                    api_request.save(overwrite_existing_object=False) 
                except Exception as e: 
                    _log.exception('Error saving api_request %s to postgres %s' % (api_request,e)) 
                    pass
                options._set('cmsdb.neondata.wants_postgres', 0)
            except Exception as e: 
                _log.exception('Error pulling information for video %s while saving to postgres %s' % (v,e)) 
                pass
    options._set('cmsdb.neondata.wants_postgres', 0)

def main():
    move_processing_strategies() 
    move_tracker_account_id_mappers() 
    move_users() 
    move_neon_user_accounts()
    move_neon_videos_and_thumbnails()
    move_abstract_integrations()
    move_abstract_platforms()
    move_cdn_hosting_metadata_lists()
    move_experiment_strategies()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
