#!/usr/bin/env python

'''
Brightcove A/B tester with Neon demo account

real: actual videos from account, 
      non mocked network calls to change thumbnails

simulated: load & click data on the videos/ thumbnails
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
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
from cmsapi import services
from cmsdb import neondata
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
   
    def setup_test(self, a_id, api_key, n_vids=1, n_thumbs=2):
        
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

if __name__ == "__main__":
    #Temp setup
