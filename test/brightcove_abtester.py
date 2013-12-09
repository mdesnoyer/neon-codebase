#!/usr/bin/env python

'''
Brightcove A/B tester with Neon demo account

real: actual videos from account, 
      non mocked network calls to change thumbnails

simulated: load & click data on the videos/ thumbnails
'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
import datetime
import json
import logging
import multiprocessing
import random
import subprocess
import time
import urllib
import urllib2

from api import server
from supportServices import services,neondata
from test_utils import redis

_log = logging.getLogger(__name__)

class TestBrightcove(object):

    def __init__(self):
        pass

    def get_url(self,url):
        return "http://localhost:8083" + url 

    def get_request(self,url):
        try:
            req = urllib2.Request(url)
            response = urllib2.urlopen(req)
            return response.read()
        except:
            pass

    def post_request(self,url,vals,apikey=None):
        try:
            data = urllib.urlencode(vals)
            headers = {}
            if apikey is not None:
                headers = {'X-Neon-API-Key' : apikey }
            req = urllib2.Request(url, data, headers)
            response = urllib2.urlopen(req)
            return response.read()
        except Exception,e:
            print e
            _log.exception("exception %s"%e)
   
    def process_video_requests(self,api_key,i_id,video_responses):

        #Create thumbnail metadata
        N_THUMBS = 5
        IMG_PREFIX = 'http://localhost:8083/api/v1/utils'
        for video_response in video_responses:
            video_id = str(video_response.video_id)
            thumbnails = []
            for t in range(N_THUMBS):
                urls = [] ; url = IMG_PREFIX + "/thumb-%s"%t
                imgdata = self.get_request(url) #get image
                tid = neondata.ThumbnailID.generate(imgdata,
                                neondata.InternalVideoID.generate(api_key,
                                video_id))
                urls.append(url)
                created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if t == N_THUMBS -1:
                    tdata = neondata.ThumbnailMetaData(tid,urls,created,480,360,
                        "brightcove",2,"test",enabled=True,rank=0)
                else:
                    tdata = neondata.ThumbnailMetaData(tid,urls,created,480,360,
                        "neon",2,"test",enabled=True,rank=t+1)
                thumbnails.append(tdata)
        
            i_vid = neondata.InternalVideoID.generate(api_key,video_id)
            thumbnail_mapper_list = []
            thumbnail_url_mapper_list = []
            for thumb in thumbnails:
                tid = thumb.thumbnail_id
                for t_url in thumb.urls:
                    uitem = neondata.ThumbnailURLMapper(t_url,tid)
                    thumbnail_url_mapper_list.append(uitem)
                    item = neondata.ThumbnailIDMapper(tid,i_vid,thumb)
                    thumbnail_mapper_list.append(item)
            retid = neondata.ThumbnailIDMapper.save_all(thumbnail_mapper_list)
            returl = neondata.ThumbnailURLMapper.save_all(thumbnail_url_mapper_list)
            assert(retid)
            assert(returl)

            tids = []
            for thumb in thumbnails:
                tids.append(thumb.thumbnail_id)
        
            vmdata = neondata.VideoMetadata(i_vid,tids,"job_id",url,10,5,"test",i_id)
            assert(vmdata.save())

    def setup_test_accounts(
            self,a_id="bcovetestaccount",
            i_id="i12345",
            rtoken="cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..",
            wtoken="vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..",
            n_vids = 1,
            n_thumbs = 2):
        
        # create neon user account
        vals = { 'account_id' : a_id }
        uri = self.get_url('/api/v1/accounts')
        response = self.post_request(uri,vals)
        api_key = json.loads(response)["neon_api_key"]
       
        #Signup for a brightcove account
        url = self.get_url('/api/v1/accounts/' + a_id + '/brightcove_integrations')
        vals = { 'integration_id' : i_id, 'publisher_id' : 'testpubid123',
                'read_token' : rtoken, 'write_token': wtoken,'auto_update': False}
        resp = self.post_request(url,vals,api_key)
        video_response = json.loads(resp)
        vrs = []
        for item in video_response:
            vr = services.VideoResponse(None,None,None,None,None,None,None,
                                        None,None)
            vr.__dict__ = item
            vrs.append(vr)

        #enable abtest for account
        bdata = neondata.BrightcovePlatform.get_account(api_key,i_id)
        bp = neondata.BrightcovePlatform.create(bdata)
        bp.abtest = True
        bp.save()

        #produce consistent database state
        #For n videos generate A & B thumbnails with unique tids
        self.process_video_requests(api_key,i_id,vrs)

        #Trigger and send data to Mastermind

        #Mastermind sends data to ab controller

        #Simulated data stream is sent to the trackserver

        #What about delta streams ? 
        print "done"

def LaunchSupportServices():
    #proc = multiprocessing.Process(target=services.main)
    #temp
    proc = subprocess.Popen([
            '/usr/bin/env', 'python',
            '../supportServices/services.py'],
            stdout=subprocess.PIPE)
    _log.warn('Launching Support Services with pid %i' % proc.pid)
    return proc

def LaunchVideoServer():
    #proc = multiprocessing.Process(target=server.main)
    #proc.start()
    proc = subprocess.Popen([
            '/usr/bin/env', 'python',
            '../api/server.py'],
            stdout=subprocess.PIPE)
    _log.warn('Launching Server with pid %i' % proc.pid)
    return proc

if __name__ == "__main__":
    #Temp setup
    port =  10121
    redis = redis.RedisServer(port)
    redis.start()
    service = LaunchSupportServices()
    vserver = LaunchVideoServer()
    time.sleep(2)
    
    try:
        TestBrightcove().setup_test_accounts()
    except:
        pass
    
    #cleanup;
    service.terminate()
    service.wait()
    vserver.terminate()
    vserver.wait()
    redis.stop()
