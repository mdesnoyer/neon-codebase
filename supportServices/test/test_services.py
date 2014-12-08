#!/usr/bin/env python
'''
Unit Test for Support services api calls

Note: get_new_ioloop() is overridden so that the test code and
tornado server share the same io_loop
'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
        sys.path.insert(0, base_path)

from api import brightcove_api
import datetime
import json
from mock import MagicMock, patch 
import random
import re
from PIL import Image
from StringIO import StringIO
from supportServices import services, neondata
import test_utils.mock_boto_s3 as boto_mock
import test_utils.redis
import time
import tornado.gen
import tornado.ioloop
import tornado.testing
import tornado.httpclient
import unittest
import urllib
from utils.imageutils import PILImageUtils
from utils.options import define, options
import utils.neon
import logging
_log = logging.getLogger(__name__)

import bcove_responses
import ooyala_responses


### Global helper methods
TIME_OUT = 10
mock_image_url_prefix = "http://servicesunittest.mock.com/"

def create_random_image_response():
    '''http image response''' 
    request = tornado.httpclient.HTTPRequest("http://someimageurl/image.jpg")
    im = utils.imageutils.PILImageUtils.create_random_image(360, 480)
    imgstream = StringIO()
    im.save(imgstream, "jpeg", quality=100)
    imgstream.seek(0)

    response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=imgstream)
    return response

###############################################
# Test Services
###############################################

def process_neon_api_requests(api_requests, api_key, i_id, t_type):
    #Create thumbnail metadata
    images = {}
    thumbnail_url_to_image = {}
    N_THUMBS = 5
    for api_request in api_requests:
        video_id = api_request.video_id
        internal_video_id = neondata.InternalVideoID.generate(api_key,
                                                              video_id)
        job_id = api_request.job_id
        thumbnails = []
        for t in range(N_THUMBS):
            image =  utils.imageutils.PILImageUtils.create_random_image(360,
                                                                        480)
            filestream = StringIO()
            image.save(filestream, "JPEG", quality=100) 
            filestream.seek(0)
            imgdata = filestream.read()
            tid = neondata.ThumbnailID.generate(imgdata, internal_video_id)
            images[tid] = image
            urls = [] ; url = mock_image_url_prefix + "/thumb-%i" % t
            urls.append(url)
            thumbnail_url_to_image[url] = imgdata
            created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if t == N_THUMBS -1:
                tdata = neondata.ThumbnailMetadata(
                    tid, internal_video_id,
                    urls, created, 480, 360, t_type, 2,
                    "test", enabled=True, rank=0)
            else:
                tdata = neondata.ThumbnailMetadata(
                    tid, internal_video_id, urls, created, 480, 360,
                    "neon", 2, "test", enabled=True, rank=t+1)
            thumbnails.append(tdata)

        thumbnail_url_mapper_list = []
        for thumb in thumbnails:
            tid = thumb.key
            for t_url in thumb.urls:
                uitem = neondata.ThumbnailURLMapper(t_url, tid)
                thumbnail_url_mapper_list.append(uitem)
        retid = neondata.ThumbnailMetadata.save_all(thumbnails)
        returl = neondata.ThumbnailURLMapper.save_all(thumbnail_url_mapper_list)

        # standard mock response
        response_body = {}
        response_body["job_id"] = job_id 
        response_body["video_id"] = video_id 
        response_body["data"] = [] 
        response_body["thumbnails"] = [] 
        response_body["timestamp"] = str(time.time())
        response_body["serving_url"] =\
            "http://i1.neon-images.com/v1/client/%s/neonvid_%s" % ("tai", video_id)
        response_body["error"] = "" 

        #Update request state to FINISHED
        api_request.state = neondata.RequestState.FINISHED
        api_request.response = response_body
        api_request.save()
        tids = []
        for thumb in thumbnails:
            tids.append(thumb.key)

        vmdata = neondata.VideoMetadata(internal_video_id, tids, job_id,
                                        url, 10, 5, "test", i_id)
        vmdata.save()

    return images, thumbnail_url_to_image

class TestServices(tornado.testing.AsyncHTTPTestCase):
    ''' Services Test '''
        
    @classmethod
    def setUpClass(cls):
        super(TestServices, cls).setUpClass()

    def setUp(self):
        super(TestServices, self).setUp()
        #NOTE: Make sure that you don't repatch objects

        #Http Connection pool Mock
        self.cp_sync_patcher = \
          patch('utils.http.tornado.httpclient.HTTPClient')
        self.cp_async_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.cp_mock_client = self.cp_sync_patcher.start()
        self.cp_mock_async_client = self.cp_async_patcher.start()

        self.api_key = "" # filled later
        self.a_id = "unittester-0"
        self.rtoken = "rtoken"
        self.wtoken = "wtoken"
        self.b_id = "i12345" #i_id bcove
        self.pub_id = "p124"
        self.thumbnail_url_to_image = {} # mock url => raw image buffer data
        self.job_ids = [] #ordered list
        self.video_ids = []
        self.images = {} 

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        
        random.seed(19449)
        
    def tearDown(self):
        #self.bapi_sync_patcher.stop()
        #self.bapi_async_patcher.stop()
        self.cp_sync_patcher.stop()
        self.cp_async_patcher.stop()
        self.redis.stop()
        super(TestServices, self).tearDown()
    
    def get_app(self):
        ''' return services app '''
        return services.application

    # TODO: It should be possible to run this with an IOLoop for each
    # test, but it's not running. Need to figure out why.
    #def get_new_ioloop(self):
    #    return tornado.ioloop.IOLoop.instance()

    def post_request(self, url, vals, apikey, jsonheader=False):
        ''' post request to the app '''

        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded'}
        body = urllib.urlencode(vals)
        
        if jsonheader: 
            headers = {'X-Neon-API-Key' : apikey, 
                    'Content-Type':'application/json'}
            body = json.dumps(vals)

        self.http_client.fetch(url,
                               callback=self.stop,
                               method="POST",
                               body=body,
                               headers=headers)
        response = self.wait()
        return response

    def put_request(self, url, vals, apikey, jsonheader=False):
        ''' put request to the app '''

        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded' }
        body = urllib.urlencode(vals)
        
        if jsonheader: 
            headers = {'X-Neon-API-Key' : apikey, 
                    'Content-Type':'application/json'}
            body = json.dumps(vals)
        
        self.http_client.fetch(url, self.stop,method="PUT", body=body,
                               headers=headers)
        response = self.wait(timeout=10)
        return response

    def get_request(self, url, apikey):
        ''' get request to the app '''

        headers = {'X-Neon-API-Key' :apikey} 
        self.http_client.fetch(url, self.stop, headers=headers)
        resp = self.wait()
        return resp

    ### Helper methods

    ##Brightcove response parser methods
    def _get_videos(self):
        ''' get all videos from brightcove response'''

        vitems = json.loads(bcove_responses.find_all_videos_response)
        videos = []
        for item in vitems['items']:
            vid = str(item['id'])
            videos.append(vid)
        return videos
    
    def _get_thumbnails(self, vid):
        ''' get all thumbnails for a video'''

        i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
        vmdata= neondata.VideoMetadata.get(i_vid)
        tids = vmdata.thumbnail_ids
        return tids

    def _process_brightcove_neon_api_requests(self, api_requests):
        self.images, self.thumbnail_url_to_image = process_neon_api_requests(
            api_requests,self.api_key, self.b_id, 'brightcove')

    def _create_neon_api_requests(self):
        ''' create neon api requests '''

        api_requests = []
        vitems = json.loads(bcove_responses.find_all_videos_response)
        items = vitems['items']
        i = 0
        for item in items:
            vid = str(item['id'])                              
            title = item['name']
            video_download_url = item['FLVURL']
            job_id = str(self.job_ids[i]) #str(random.random())
            p_thumb = item['videoStillURL']
            api_request = neondata.BrightcoveApiRequest(
                job_id, self.api_key, vid,
                title, video_download_url, self.rtoken,
                self.wtoken, self.pub_id, "http://callback", self.b_id)
            api_request.previous_thumbnail = p_thumb 
            api_request.autosync = False
            api_request.set_api_method("topn", 5)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT
            self.assertTrue(api_request.save())
            api_requests.append(api_request)
            i += 1

        return api_requests

    def _get_video_status_brightcove(self):
        ''' get video status for all videos in brightcove '''

        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                            "/%s/videos" %(self.a_id, self.b_id))
        headers = {'X-Neon-API-Key' : self.api_key} 
        self.http_client.fetch(url, self.stop, headers=headers)
        resp = self.wait()
        items = json.loads(resp.body)
        return items

    def _check_video_status_brightcove(self, vstatus):
        ''' assert video status for brightcove videos'''
        
        items = self._get_video_status_brightcove()
        for item in items['items']:
            vr = neondata.VideoResponse(None, None, None, None, None, None, 
                                    None, None, None, None)
            vr.__dict__ = item
            status = vr.status
            self.assertEqual(status, vstatus)

    def _check_neon_default_chosen(self, videos): 
        ''' validate neon rank 1 is chosen on default publish '''

        for vid in videos:
            i_vid = neondata.InternalVideoID.generate(self.api_key, vid) 
            vmdata = neondata.VideoMetadata.get(i_vid)
            thumbnails = neondata.ThumbnailMetadata.get_many(
                vmdata.thumbnail_ids)
            for thumb in thumbnails:
                if thumb.chosen == True and thumb.type == 'neon':
                    self.assertEqual(thumb.rank, 1)

    def create_neon_account(self):
        ''' create neon user account '''

        vals = { 'account_id' : self.a_id }
        uri = self.get_url('/api/v1/accounts') 
        response = self.post_request(uri, vals, "")
        api_key = json.loads(response.body)["neon_api_key"]
        tai = json.loads(response.body)["tracker_account_id"]
        return api_key

    def create_brightcove_account(self):
        ''' create brightcove platform account '''

        #create a neon account first
        self.api_key = self.create_neon_account()
        self.assertEqual(self.api_key, 
                neondata.NeonApiKey.get_api_key(self.a_id))

        url = self.get_url('/api/v1/accounts/' + self.a_id + \
                            '/brightcove_integrations')
        vals = {'integration_id' : self.b_id, 'publisher_id' : 'testpubid123',
                'read_token' : self.rtoken, 'write_token': self.wtoken, 
                'auto_update': False}
        resp = self.post_request(url, vals, self.api_key)
        return resp.body

    def update_brightcove_account(self, rtoken=None, wtoken=None, autoupdate=None):
        ''' update brightcove account '''

        if rtoken is None: rtoken = self.rtoken
        if wtoken is None: wtoken = self.wtoken
        if autoupdate == None: autoupdate = False

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/%s' \
                            %(self.a_id, self.b_id))
        vals = {'read_token' : rtoken, 'write_token': wtoken, 
                'auto_update': autoupdate}
        return self.put_request(url, vals, self.api_key)

    def update_brightcove_thumbnail(self, vid, tid):
        ''' update thumbnail for a brightcove video given thumbnail id'''

        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        vals = {'current_thumbnail' : tid }
        return self.put_request(url, vals, self.api_key)
  
    ## HTTP Side efffect for all Tornado HTTP Requests

    def _success_http_side_effect(self, *args, **kwargs):
        ''' generic sucess http side effects for all patched http calls 
            for this test ''' 

        def _neon_submit_job_response():
            ''' video server response on job submit '''
            job_id = str(random.random())
            self.job_ids.append(job_id)
            request = tornado.httpclient.HTTPRequest('http://thumbnails.neon-lab.com')
            response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"%s"}'%job_id))
            return response

        def _add_image_response(req): 
            ''' image response '''
            itype = "THUMBNAIL"
            if "VIDEO_STILL" in req.body:
                itype = "VIDEO_STILL"

            request = tornado.httpclient.HTTPRequest("http://api.brightcove.com/services/post")
            response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO
                    ('{"result": {"displayName":"test","id":123,'
                    '"referenceId":"test_ref_id","remoteUrl":null,"type":"%s"},'
                    '"error": null, "id": null}'%itype))
            return response
        
        #################### HTTP request/responses #################
        #mock brightcove api call
        bcove_request = tornado.httpclient.HTTPRequest('http://api.brightcove.com/services/library?'
            'get_item_count=true&command=find_all_videos&page_size=5&sort_by='
            'publish_date&token=rtoken&page_number=0&output=json&media_delivery=http') #build the string
        bcove_response = tornado.httpclient.HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_all_videos_response))
        
        #mock neon api call
        request = tornado.httpclient.HTTPRequest('http://google.com')
        response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"neon error"}'))
        
        #################### HTTP request/responses #################
        http_request = args[0]
        if kwargs.has_key("callback"):
            callback = kwargs["callback"]
        else:
            callback = args[1] if len(args) >=2 else None

        if "/services/library?command=find_video_by_id" in http_request.url:
            request = tornado.httpclient.HTTPRequest(http_request.url)
            response = tornado.httpclient.HTTPResponse(request, 200,
                    buffer=StringIO(bcove_responses.find_video_by_id_response))
            if kwargs.has_key("callback"):
                callback = kwargs["callback"]
                return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                     response)
            else:
                if len(args) > 1:
                    callback = args[1]
                    return tornado.ioloop.IOLoop.current().add_callback(
                        callback, response)
                else:
                    return response

        elif "http://api.brightcove.com/services/library" in http_request.url:
            return bcove_response
           
        #add_image api call 
        elif "http://api.brightcove.com/services/post" in http_request.url:
            return _add_image_response(http_request) 

        #Download image from brightcove CDN
        elif "http://brightcove.vo.llnwd.net" in http_request.url:
            return create_random_image_response()
            
        #Download image from mock unit test url ; This is done async in the code
        elif mock_image_url_prefix in http_request.url:
            request = tornado.httpclient.HTTPRequest(http_request.url)
            response = tornado.httpclient.HTTPResponse(request, 200,
                    buffer=StringIO(self.thumbnail_url_to_image[http_request.url]))
            #on async fetch, callback is returned
            #check if callable -- hasattr(obj, '__call__')
            if kwargs.has_key("callback"):
                callback  = kwargs["callback"]
            else:
                callback = args[1] 
            return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                               response)

        #neon api request
        elif "api/v1/submitvideo" in http_request.url:
            response = _neon_submit_job_response()            
            

        elif ".mp4" in http_request.url:
            headers = {"Content-Type": "video/mp4"}
            response = tornado.httpclient.HTTPResponse(request, 200,
                                                       headers=headers,
                                                       buffer=StringIO('videodata'))
            
        else:
            headers = {"Content-Type": "text/plain"}
            response = tornado.httpclient.HTTPResponse(request, 200, headers=headers,
                buffer=StringIO('someplaindata'))
            
        if callback:
            return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                response)
        return response

        

    def _setup_initial_brightcove_state(self):
        '''
        Setup the state of a brightcove account with 5 processed videos 
        '''
        #Setup Side effect for the http clients
        #self.bapi_mock_client().fetch.side_effect = \
        #  self._success_http_side_effect
        self.cp_mock_client().fetch.side_effect = \
          self._success_http_side_effect 
        #self.bapi_mock_async_client().fetch.side_effect = \
        #self._success_http_side_effect
        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect
    
        #set up account and video state for testing
        self.api_key = self.create_neon_account()
        json_video_response = self.create_brightcove_account()
        self.assertNotEqual(json_video_response, '{}') # !empty json response
        
        #verify account id added to Neon user account
        nuser = neondata.NeonUserAccount.get_account(self.api_key)
        self.assertTrue(self.b_id in nuser.integrations.keys())

        #Verifty that there is an experiment strategy for the account
        strategy = neondata.ExperimentStrategy.get(self.api_key)
        self.assertIsNotNone(strategy)
        self.assertTrue(strategy.only_exp_if_chosen)
        
        reqs = self._create_neon_api_requests()
        self._process_brightcove_neon_api_requests(reqs)


    ################################################################
    # Unit Tests
    ################################################################

    def test_invalid_get_rest_uri(self):
        ''' test uri parsing, invalid requests '''
        api_key = self.create_neon_account()

        url = self.get_url('/api/v1/accounts/')
        resp = self.get_request(url, api_key)
        self.assertEqual(resp.code, 400)
        
        url = self.get_url('/api/v1/accounts/123/invalid_aid')
        resp = self.get_request(url, api_key)
        self.assertEqual(resp.code, 400)
        
        url = self.get_url('/api/v1/accounts/%s/dummy_integration' %self.a_id)
        resp = self.get_request(url, api_key)
        self.assertEqual(resp.code, 400)
        
        url = self.get_url('/api/v1/accounts/invalid_api_key/'\
                            'neon_integrations/0/videos')
        resp = self.get_request(url, api_key)
        self.assertEqual(resp.code, 400)
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations'\
                            '/0/bad_method' %self.a_id)
        resp = self.get_request(url, api_key)
        self.assertEqual(resp.code, 400)

    def test_invalid_put_rest_uri(self):
        ''' put requests'''
        
        api_key = self.create_neon_account()

        url = self.get_url('/api/v1/accounts/%s/neon_integrations'\
                            '/0/videos' %self.a_id)
        resp = self.put_request(url, {}, api_key)
        self.assertEqual(resp.code, 400)
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations'\
                            '/0/invalid_method' %self.a_id)
        resp = self.put_request(url, {}, api_key)
        self.assertEqual(resp.code, 400)


    def test_create_update_brightcove_account(self):
        ''' updation of brightcove account '''

        #create neon account
        self.api_key = self.create_neon_account()
        self.assertEqual(self.api_key, 
                neondata.NeonApiKey.get_api_key(self.a_id))
        
        #Verify that there is an experiment strategy for the account
        strategy = neondata.ExperimentStrategy.get(self.api_key)
        self.assertIsNotNone(strategy)
        self.assertFalse(strategy.only_exp_if_chosen)

        #Setup Side effect for the http clients
        #self.bapi_mock_client().fetch.side_effect = \
        #  self._success_http_side_effect
        self.cp_mock_client().fetch.side_effect = \
          self._success_http_side_effect 
        #self.bapi_mock_async_client().fetch.side_effect = \
        #  self._success_http_side_effect
        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect

        #create brightcove account
        json_video_response = self.create_brightcove_account()
        video_response = json.loads(json_video_response)['items']
        self.assertEqual(len(video_response), 5)

        # Verify actual contents
        platform = neondata.BrightcovePlatform.get_account(self.api_key,
                                                           self.b_id)
        self.assertFalse(platform.abtest) # Should default to False
        self.assertEqual(platform.neon_api_key, self.api_key)
        self.assertEqual(platform.integration_id, self.b_id)
        self.assertEqual(platform.account_id, self.a_id)
        self.assertEqual(platform.publisher_id, 'testpubid123')
        self.assertEqual(platform.read_token, self.rtoken)
        self.assertEqual(platform.write_token, self.wtoken)
        self.assertFalse(platform.auto_update)
        

        #update brightcove account
        new_rtoken = ("newrtoken")
        update_response = self.update_brightcove_account(new_rtoken)
        self.assertEqual(update_response.code, 200)
        platform = neondata.BrightcovePlatform.get_account(self.api_key,
                                                           self.b_id)
        self.assertEqual(platform.read_token, "newrtoken")
        self.assertFalse(platform.auto_update)
        self.assertEqual(platform.write_token, self.wtoken)

    def test_brightcove_web_account_flow(self):
        #Create Neon Account --> Bcove Integration --> update Integration --> 
        #query videos --> autopublish --> verify autopublish
        with options._set_bounded('supportServices.neondata.dbPort',
                                  self.redis.port):
        
            #create neon account
            api_key = self.create_neon_account()
            self.assertEqual(api_key, neondata.NeonApiKey.get_api_key(self.a_id))

            #Setup Side effect for the http clients
            #self.bapi_mock_client().fetch.side_effect = \
            #                    self._success_http_side_effect
            self.cp_mock_client().fetch.side_effect = \
                                self._success_http_side_effect 
            #self.bapi_mock_async_client().fetch.side_effect = \
                                #self._success_http_side_effect
            self.cp_mock_async_client().fetch.side_effect = \
                                self._success_http_side_effect

            #create brightcove account
            json_video_response = self.create_brightcove_account()
            video_response = json.loads(json_video_response)['items']
            self.assertEqual(len(video_response), 5) 
        
            #process requests
            reqs = self._create_neon_api_requests()
            self._process_brightcove_neon_api_requests(reqs)
            
            videos = []
            vitems = json.loads(bcove_responses.find_all_videos_response)
            for item in vitems['items']:
                videos.append(str(item['id']))                             

            #update a thumbnail
            new_tids = [] 
            for vid, job_id in zip(videos, self.job_ids):
                i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
                vmdata= neondata.VideoMetadata.get(i_vid)
                tids = vmdata.thumbnail_ids
                new_tids.append(tids[1])
                #set neon rank 2 
                resp = self.update_brightcove_thumbnail(vid, tids[1])
                self.assertEqual(resp.code, 200)
                
                #assert request state
                vid_request = neondata.NeonApiRequest.get(job_id,
                                                          self.api_key)
                self.assertEqual(vid_request.state,neondata.RequestState.ACTIVE)

            thumbs = []
            items = self._get_video_status_brightcove()
            for item, tid in zip(items['items'], new_tids):
                vr = neondata.VideoResponse(None, None, None, None, None,
                                        None, None, None, None, None)
                vr.__dict__ = item
                thumbs.append(vr.current_thumbnail)
            self.assertItemsEqual(thumbs, new_tids)

    #Failure test cases
    #Database failure on account creation, updation
    #Brightcove API failures

    def test_update_thumbnail_fails(self):
        with options._set_bounded('supportServices.neondata.dbPort',
                                  self.redis.port):
            self._setup_initial_brightcove_state()
            self._test_update_thumbnail_fails()
    
    def _test_update_thumbnail_fails(self):
        def _failure_http_side_effect(*args, **kwargs):
            http_request = args[0]
            if mock_image_url_prefix in http_request.url:
                request = tornado.httpclient.HTTPRequest(http_request.url)
                response = tornado.httpclient.HTTPResponse(request, 500,
                    buffer=StringIO("Server error"))
                if kwargs.has_key("callback"):
                    callback = kwargs["callback"]
                else:
                    callback = args[1]
                return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                     response)

            if "http://api.brightcove.com/services/post" in http_request.url:
                itype = "THUMBNAIL"
                if "VIDEO_STILL" in http_request.body:
                    itype = "VIDEO_STILL"

                request = tornado.httpclient.HTTPRequest("http://api.brightcove.com/services/post")
                response = tornado.httpclient.HTTPResponse(request, 500,
                    buffer=StringIO
                        ('{"result": {"displayName":"test","id":123,'
                        '"referenceId":"test_ref_id","remoteUrl":null,"type":"%s"},'
                        '"error": "mock error", "id": null}'%itype))
                return response

        vids = self._get_videos()
        vid  = vids[0]
        tids = self._get_thumbnails(vid)
        
        #Failed to download Image; Expect internal error 500
        self.cp_mock_async_client().fetch.side_effect = \
                _failure_http_side_effect
        resp = self.update_brightcove_thumbnail(vid, tids[1]) 
        self.assertEqual(resp.code, 500) 

        #Brightcove api error, gateway error 502
        self.cp_mock_async_client().fetch.side_effect = \
                self._success_http_side_effect
        self.cp_mock_client().fetch.side_effect =\
                _failure_http_side_effect 
        resp = self.update_brightcove_thumbnail(vid, tids[1]) 
        self.assertEqual(resp.code, 502) 

        #Successful update of thumbnail
        self.cp_mock_client().fetch.side_effect =\
                self._success_http_side_effect
        resp = self.update_brightcove_thumbnail(vid, tids[1]) 
        self.assertEqual(resp.code, 200) 

        #Induce Failure again, bcove api error
        self.cp_mock_client().fetch.side_effect =\
                _failure_http_side_effect 
        resp = self.update_brightcove_thumbnail(vid, tids[1]) 
        self.assertEqual(resp.code, 502) 

    #TODO: Test creation of individual request

    ######### BCOVE HANDLER Test cases ##########################

    def test_bh_update_thumbnail(self):
        ''' Brightcove support handler tests (check thumb/update thumb) '''
        
        self._setup_initial_brightcove_state()
        vids = self._get_videos()
        vid  = vids[0]
        job_id = self.job_ids[0]
        tids = self._get_thumbnails(vid)
        i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
        tid  = tids[0]
        self.cp_mock_client().fetch.side_effect =\
            self._success_http_side_effect
        url = self.get_url('/api/v1/brightcovecontroller/%s/updatethumbnail/%s' 
                        %(self.api_key, i_vid))
        vals = {'thumbnail_id' : tid }
        resp = self.post_request(url, vals, self.api_key)
        self.assertEqual(resp.code, 200)

        #assert request state is not updated to active
        vid_request = neondata.NeonApiRequest.get(job_id, self.api_key)
        self.assertEqual(vid_request.state, neondata.RequestState.FINISHED)
        
        #assert the previous thumbnail is still the thumb in DB
        self.update_brightcove_thumbnail(vid, tid)
        tid2 = tids[1]
        vals = {'thumbnail_id' : tid2}
        resp = self.post_request(url, vals, self.api_key)
        self.assertEqual(resp.code, 200)
        tids = neondata.ThumbnailMetadata.get_many([tid,tid2])
        self.assertTrue(tids[0].chosen)
        self.assertFalse(tids[1].chosen)
        

    def test_pagination_videos_brighcove(self):
        ''' test pagination of brightcove integration '''

        self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        
        #get videos in pages
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size)
        result_vids = [x['video_id'] for x in items]
        
        self.assertEqual(ordered_videos[:page_size],
                result_vids)

        #test page no (initial # of vids populated =5)
        page_no = 1
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id,self.b_id,page_no,page_size))
        resp = self.get_request(url,self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size, "page number did not match")
        result_vids = [x['video_id'] for x in items]
        self.assertItemsEqual(
                ordered_videos[page_no*page_size:(page_no+1)*page_size],
                result_vids)

        #request page_size such that it more than #of vids in account 
        page_no = 0
        page_size = 1000
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = self.get_request(url, self.api_key)
        response = json.loads(resp.body)
        items = response['items']
        result_vids = [x['video_id'] for x in items]
        
        #Check videos are sorted by publish date or video ids ? 
        self.assertEqual(len(ordered_videos),len(result_vids),
                "number of videos returned dont match")
    
        #re-create response
        self.assertEqual(response['published_count'], 0)
        self.assertEqual(response['processing_count'], 0)
        self.assertEqual(response['recommended_count'], len(ordered_videos))

        #request last page with page_size > #of videos available in the page
        page_no = 1 
        page_size = 3 
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertEqual(len(ordered_videos) - (page_no*page_size),
                        len(result_vids))

    def test_request_by_video_ids_brightcove(self):
        ''' test video ids of brightcove integration '''

        self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        test_video_ids = ordered_videos[:2]
        video_ids = ",".join(test_video_ids)

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?video_ids=%s'
                %(self.a_id, self.b_id, video_ids))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertItemsEqual(result_vids, test_video_ids)
        
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos/?video_ids=%s'
                %(self.a_id, self.b_id, video_ids))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertItemsEqual(result_vids, test_video_ids)
    
    def test_invalid_model_scores(self):
        ''' test filtering of invalid model scores like -inf, nan '''

        self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        tids = self._get_thumbnails(vid)
        
        #update in database the thumbnail to have -inf score
        td = neondata.ThumbnailMetadata.get_many(tids)
        td[0].model_score = float('-inf')
        td[1].model_score = float('nan')
        td[2].model_score = None 
        neondata.ThumbnailMetadata.save_all(td)
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id,self.b_id,0,100))
        resp = self.get_request(url,self.api_key)
        response = json.loads(resp.body)
       
        model_scores = []
        for r in response['items']:
            for t in r['thumbnails']:
                model_scores.append(t['model_score'])

        self.assertFalse(float('-inf') in model_scores)    
        self.assertFalse(float('nan') in model_scores)    
        self.assertFalse(None in model_scores)    
   
    def test_get_brightcove_video_requests_by_state(self):
        '''
        Test you can query brightcove videos by their video state
        including requesting them in pages
        '''
        self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        
        #recommended videos
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos/recommended?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = self.get_request(url,self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertEqual(ordered_videos[:page_size],
                         result_vids)

        #publish a couple of videos
        vids = self._get_videos()[:page_size]
        for vid in vids:
            tid = self._get_thumbnails(vid)[0] 
            update_response = self.update_brightcove_thumbnail(vid, tid)
            self.assertEqual(update_response.code, 200)

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos/published?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertItemsEqual(vids,
                result_vids)

    def test_tracker_account_id_mapper(self):
        '''
        Test mapping between tracker account id => neon account id
        '''
        #account creation
        vals = { 'account_id' : self.a_id }
        uri = self.get_url('/api/v1/accounts') 
        response = self.post_request(uri, vals, '')
        api_key = json.loads(response.body)["neon_api_key"]
        tai = json.loads(response.body)["tracker_account_id"]
        s_tai = json.loads(response.body)["staging_tracker_account_id"]
        a_id, itype = neondata.TrackerAccountIDMapper.get_neon_account_id(tai)   
        self.assertEqual(api_key, a_id)
        self.assertEqual(itype,neondata.TrackerAccountIDMapper.PRODUCTION)
        a_id,itype = neondata.TrackerAccountIDMapper.get_neon_account_id(s_tai)   
        self.assertEqual(api_key, a_id)
        self.assertEqual(itype, neondata.TrackerAccountIDMapper.STAGING)

        #query tai
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/tracker_account_id'%(self.a_id, self.b_id))
        response = self.get_request(url, api_key)
        tai = json.loads(response.body)["tracker_account_id"]
        s_tai = json.loads(response.body)["staging_tracker_account_id"]
        a_id, itype = neondata.TrackerAccountIDMapper.get_neon_account_id(tai)   
        self.assertEqual(api_key, a_id)
        self.assertEqual(itype,neondata.TrackerAccountIDMapper.PRODUCTION)
        
        a_id, itype = neondata.TrackerAccountIDMapper.get_neon_account_id(s_tai)   
        self.assertEqual(api_key, a_id)
        self.assertEqual(itype, neondata.TrackerAccountIDMapper.STAGING)

        r_a_id,r_itype = neondata.TrackerAccountIDMapper.get_neon_account_id(tai)
        self.assertEqual(r_a_id, a_id)
        self.assertEqual(r_itype,neondata.TrackerAccountIDMapper.PRODUCTION)
        
        r_a_id,r_itype = neondata.TrackerAccountIDMapper.get_neon_account_id(s_tai)
        self.assertEqual(r_a_id, a_id)
        self.assertEqual(r_itype, neondata.TrackerAccountIDMapper.STAGING)

    def _test_gzip_response(self):
        pass
        #response = self.fetch("/chunk", use_gzip=False,
        #        headers={"Accept-Encoding": "gzip"})
        #self.assertEqual(response.headers["Content-Encoding"], "gzip")
    
    def test_create_neon_integration(self):
        api_key = self.create_neon_account()
        nuser = neondata.NeonUserAccount.get_account(api_key)
        neon_integration_id = "0"
        self.assertTrue(neon_integration_id in nuser.integrations.keys()) 

    def test_create_neon_video_request(self):
        ''' verify that video request creation via services  ''' 
        
        api_key = self.create_neon_account()
        vals = { 'video_url' : "http://test.mp4", "title": "test_title" }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_video_request'%(self.a_id, "0"))

        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect

        response = self.post_request(uri, vals, api_key)
        self.assertTrue(response.code, 200)
        response = json.loads(response.body)
        self.assertIsNotNone(response["video_id"])  
        self.assertEqual(response["status"], neondata.RequestState.PROCESSING)
    
    def test_create_neon_video_request_via_api(self):
        ''' verify that video request creation via services  ''' 
        
        api_key = self.create_neon_account()
        vals = { 'video_url' : "http://test.mp4", "video_title": "test_title", 
                 'video_id'  : "vid1", "callback_url" : "http://callback"
                }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "0"))

        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect
        
        vid = "vid1"
        response = self.post_request(uri, vals, api_key)
        self.assertTrue(response.code, 201)
        jresponse = json.loads(response.body)
        job_id = jresponse['job_id']
        self.assertIsNotNone(job_id)
        
        # add video to account
        np = neondata.NeonPlatform.get_account(api_key)
        np.add_video(vid, job_id)
        np.save()

        # Test duplicate request
        request = tornado.httpclient.HTTPRequest('http://thumbnails.neon-lab.com')
        response = tornado.httpclient.HTTPResponse(request, 409,
                buffer=StringIO('{"error":"already processed","video_id":"vid", "job_id":"%s"}' % job_id))
        self.cp_mock_async_client().fetch.side_effect = \
        response = self.post_request(uri, vals, api_key)
        self.assertTrue(response.code, 409)
        self.assertTrue(json.loads(response.body)["job_id"], job_id)


    def test_video_request_in_submit_state(self):
        '''
        Create video request and then query it via Neon API
        '''

        api_key = self.create_neon_account()
        vals = { 'video_url' : "http://test.mp4", "video_title": "test_title", 
                 'video_id'  : "vid1", "callback_url" : "http://callback"
                }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "0"))

        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect
        
        vid = "vid1"
        response = self.post_request(uri, vals, api_key)
        self.assertTrue(response.code, 201)
        jresponse = json.loads(response.body)
        job_id = jresponse['job_id']
        self.assertIsNotNone(job_id)
        
        # add video to account
        np = neondata.NeonPlatform.get_account(api_key)
        np.add_video(vid, job_id)
        np.save()
        
        # Query a video that was just submitted 
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/%s'
                % (self.a_id, "0", vid))
        resp = self.get_request(url, api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['video_id'], vid)



    def test_create_neon_video_request_invalid_url(self):
        ''' invalid url test '''
        api_key = self.create_neon_account()
        vals = { 'video_url' : "http://not_a_video_link", "title": "test_title" }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_video_request'%(self.a_id, "0"))

        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect
        response = self.post_request(uri, vals, api_key)
        self.assertTrue(response.code, 200)
        response = json.loads(response.body)
        self.assertEqual(response['error'], 
                'link given is invalid or not a video file')

    def test_empty_get_video_status_neonplatform(self):
        ''' empty videos '''
        api_key = self.create_neon_account()
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(items,[])

    def test_get_video_status_neonplatform(self):
        '''
        Test retreiving video responses for neonplatform
        '''

        self.api_key = self.create_neon_account()
        nplatform = neondata.NeonPlatform.get_account(self.api_key)
        nvids = 10 
        api_requests = [] 
        for i in range(nvids):
            vid = "neonvideo%s"%i 
            title = "title%s"%i 
            video_download_url = "http://video%s.mp4" %i 
            job_id = "job_id%s"%i 
            api_request = neondata.NeonApiRequest(job_id, self.api_key, vid,
                    title, video_download_url, "neon", "http://callback")
            api_request.set_api_method("topn", 5)
            api_request.publish_time = str(time.time() *1000)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT
            self.assertTrue(api_request.save())
            api_requests.append(api_request)
            nplatform.add_video(vid, job_id)

        nplatform.save()
        random.seed(1123)

        self._process_brightcove_neon_api_requests(api_requests[:-1])

        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size)
        result_vids = [x['video_id'] for x in items]
        
        # recommended
        page_size = 5
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/recommended?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size)
        result_vids = [ x['video_id'] for x in items]

        # processing
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/processing?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), 1) #1 video in processing
       
        # failed state
        api_requests[0].state = neondata.RequestState.FAILED
        api_requests[0].save()
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/failed?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), 1) #1 video failed 
        self.assertEqual(items[0]['status'], 'failed')
        self.assertEqual(items[0]['job_id'], api_requests[0].job_id)
       
        # invalid state
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/invalid?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, self.api_key)
        self.assertEqual(resp.code, 400)

        # TODO (Sunil) More test cases on states
        # get videos with serving state
        api_requests[-1].state = neondata.RequestState.SERVING
        api_requests[-1].save()

        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos'  %(self.a_id, "0"))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        status = [item['status'] for item in items]
        self.assertEqual(status.count("serving"), 1)

    def _setup_neon_account_and_request_object(self, vid="testvideo1",
                                            job_id = "j1"):
        self.api_key = self.create_neon_account()
        nplatform = neondata.NeonPlatform.get_account(self.api_key)
        title = "title"
        video_download_url = "http://video.mp4" 
        api_request = neondata.NeonApiRequest(job_id, self.api_key, vid,
                    title, video_download_url, "neon", "http://callback")
        api_request.set_api_method("topn", 5)
        api_request.publish_time = str(time.time() *1000)
        api_request.submit_time = str(time.time())
        api_request.state = neondata.RequestState.SUBMIT
        self.assertTrue(api_request.save())
        nplatform.add_video(vid, job_id)

        nplatform.save()
        self._process_brightcove_neon_api_requests([api_request])
        
        # set the state to serving
        api_request = neondata.NeonApiRequest.get(job_id, self.api_key)
        api_request.state = neondata.RequestState.SERVING
        api_request.save()

    def test_video_response_object(self):
        '''
        Test expected fields of a video response object
        '''
        vid = "testvideo1"
        job_id = "j1"
        title = "title"
        self._setup_neon_account_and_request_object(vid, job_id)
        
        i_vid = neondata.InternalVideoID.generate(self.api_key, vid) 
        TMD = neondata.ThumbnailMetadata
        thumbs = [
            TMD('%s_t1' % i_vid, i_vid, ['t1.jpg'], None, None, None,
                              None, None, None, serving_frac=0.8),
            TMD('%s_t2' % i_vid, i_vid, ['t2.jpg'], None, None, None,
                              None, None, None, serving_frac=0.2),
            ]
        TMD.save_all(thumbs)
        
        #Save VideoMetadata
        tids = [thumb.key for thumb in thumbs]
        v = neondata.VideoMetadata(i_vid, tids, job_id, 'v0.mp4', 0, 0,
                None, 0, (120, 90), True)
        v.save()

        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?video_id=%s'
                %(self.a_id, "0", vid))
        resp = self.get_request(url, self.api_key)
        vresponse = json.loads(resp.body)["items"][0]

        pub_id = neondata.NeonUserAccount.get_account(self.api_key).tracker_account_id
        serving_url = 'neon-images.com/v1/client/%s/neonvid_%s.jpg' \
                        % (pub_id, vid)

        self.assertEqual(vresponse["video_id"], vid)
        self.assertEqual(vresponse["title"], title)
        self.assertEqual(vresponse["integration_type"], "neon")
        self.assertEqual(vresponse["status"], "serving")
        self.assertEqual(vresponse["abtest"], True)
        self.assertTrue(serving_url in vresponse["serving_url"])
        self.assertEqual(vresponse["winner_thumbnail"], None)

    def test_get_abtest_state(self):
        '''
        A/B test state response
        '''

        self.api_key = self.create_neon_account()
        
        ext_vid = 'vid1'
        vid = neondata.InternalVideoID.generate(self.api_key, ext_vid) 
        
        #Set experiment strategy
        es = neondata.ExperimentStrategy(self.api_key)
        es.chosen_thumb_overrides = True
        es.save()
        
    def test_winner_thumbnail_in_video_response(self):
        '''
        Test winner thumbnail, after A/B test is complete
        '''

        self.api_key = self.create_neon_account()
        nplatform = neondata.NeonPlatform.get_account(self.api_key)
        vid = "testvideo1"
        title = "title"
        video_download_url = "http://video.mp4" 
        job_id = "j1" 
        api_request = neondata.NeonApiRequest(job_id, self.api_key, vid,
                    title, video_download_url, "neon", "http://callback")
        api_request.set_api_method("topn", 5)
        api_request.publish_time = str(time.time() *1000)
        api_request.submit_time = str(time.time())
        api_request.state = neondata.RequestState.SUBMIT
        self.assertTrue(api_request.save())
        nplatform.add_video(vid, job_id)

        nplatform.save()
        self._process_brightcove_neon_api_requests([api_request])
        
        i_vid = neondata.InternalVideoID.generate(self.api_key, vid) 
        TMD = neondata.ThumbnailMetadata
        #Save thumbnails 
        thumbs = [
            TMD('%s_t1' % i_vid, i_vid, ['t1.jpg'], None, None, None,
                              None, None, None, serving_frac=0.8),
            TMD('%s_t2' % i_vid, i_vid, ['t2.jpg'], None, None, None,
                              None, None, None, serving_frac=0.2),
            ]
        TMD.save_all(thumbs)
        
        #Save VideoMetadata
        tids = [thumb.key for thumb in thumbs]
        v = neondata.VideoMetadata(i_vid, tids, job_id, 'v0.mp4', 0, 0,
                None, 0, (120, 90), True, neondata.ExperimentState.COMPLETE)
        v.save()
       
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?video_id=%s'
                %(self.a_id, "0", vid))
        resp = self.get_request(url, self.api_key)
        vresponse = json.loads(resp.body)["items"][0]

        expected_vresponse = neondata.VideoResponse(vid, job_id, "finished", "neon",
                "0", title, None, None, None, thumbs, True, thumbs[0].key)

        self.assertEqual(vresponse["winner_thumbnail"],
                expected_vresponse.winner_thumbnail)

    def test_get_abtest_state(self):
        '''
        A/B test state response
        '''

        self.api_key = self.create_neon_account()
        
        ext_vid = 'vid1'
        vid = neondata.InternalVideoID.generate(self.api_key, ext_vid) 
        
        #Set experiment strategy
        es = neondata.ExperimentStrategy(self.api_key)
        es.chosen_thumb_overrides = True
        es.save()
       
        TMD = neondata.ThumbnailMetadata

        #Save thumbnails 
        thumbs = [
            TMD('%s_t1' % vid, vid, ['t1.jpg'], None, None, None,
                              None, None, None, serving_frac=0.8),
            TMD('%s_t2' % vid, vid, ['t2.jpg'], None, None, None,
                              None, None, None, serving_frac=0.15),
            TMD('%s_t3' % vid, vid, ['t3.jpg'], None, None, None,
                              None, None, None, serving_frac=0.01),
            TMD('%s_t4' % vid, vid, ['t4.jpg'], None, None, None,
                              None, None, None, serving_frac=0.04),
            ]
        TMD.save_all(thumbs)
        
        #Save VideoMetadata
        tids = [thumb.key for thumb in thumbs]
        v0 = neondata.VideoMetadata(vid, tids, 'reqid0', 'v0.mp4', 0, 0,
                None, 0, (120, 90), True, neondata.ExperimentState.RUNNING)
        v0.save()
       
        #Set up Serving URLs 
        for thumb in thumbs:
            inp = neondata.ThumbnailServingURLs('%s' % thumb.key)
            inp.add_serving_url('http://servingurl_800_600.jpg', 800, 600) 
            inp.add_serving_url('http://servingurl_120_90.jpg', 120, 90) 
            inp.save()
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                            '%s/abteststate/%s' %(self.a_id, "0", ext_vid))  
        resp = self.get_request(url, self.api_key)
        res = json.loads(resp.body)
        
        # AB test running
        self.assertEqual(resp.code, 200)
        self.assertEqual(res['state'], "running") 
        self.assertEqual(res['data'], []) 
        
        v0.experiment_state = neondata.ExperimentState.COMPLETE
        v0.save()
        
        # AB test complete 
        expected_data = json.loads('{"state": "complete", "data": [{"url":\
        "http://servingurl_800_600.jpg", "width": 800, "height": 600}, {"url":\
        "http://servingurl_120_90.jpg", "width": 120, "height": 90}]}')
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                            '%s/abteststate/%s' %(self.a_id, "0", ext_vid))  
        resp = self.get_request(url, self.api_key)
       
        res = json.loads(resp.body)
        self.assertEqual(resp.code, 200)
        self.assertEqual(res['state'], "complete") 
        self.assertEqual(res['data'], expected_data['data'])
        self.assertEqual(res['original_thumbnail'],
                            "http://servingurl_120_90.jpg")

        

    @patch('utils.imageutils.utils.http.send_request')
    @patch('api.cdnhosting.urllib2')
    @patch('api.cdnhosting.S3Connection')
    def test_upload_video_custom_thumbnail(self, mock_conntype, mock_urllib2,
                                           mock_img_download):
        '''
        Test uploading a custom thumbnail for a video
        PUT
        /api/v1/accounts/{account_id}/{integration_type}/{integration_id}/videos/{video_id}

        {"thumbnails":[
        {
            created_time: 12345,
            type: custom_upload,
            urls: [
                http://example.com/images/1.jpg
            ]
        }
        ]}
        '''
        
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('n3.neon-images.com')
        mock_conntype.return_value = conn
        
        #cloudinary mock
        mresponse = MagicMock()
        mresponse.read.return_value = '{"url": "http://cloudinary.jpg"}' 
        mock_urllib2.urlopen.return_value = mresponse 

        # Mock the image download
        def _handle_img_download(request, callback=None, *args, **kwargs):
            if "jpg" in request.url or "jpeg" in request.url:
                if "error_image" in request.url:
                    response = tornado.httpclient.HTTPResponse(request, 500)
                else:
                    #downloading any image (create a random image response)
                    response = create_random_image_response()

            elif ".png" in request.url:
                # Open an RGBA image
                im = Image.open(os.path.join(
                    os.path.dirname(__file__),
                    os.path.basename(request.url)))
                self.assertEqual(im.mode, "RGBA")
                imgstream = StringIO()
                im.save(imgstream, "png")
                imgstream.seek(0)
                response = tornado.httpclient.HTTPResponse(request, 200,
                                                           buffer=imgstream)
            else:
                response = self._success_http_side_effect(request,
                                                          callback=callback,
                                                          *args, **kwargs)
            if callback:
                return self.io_loop.add_callback(callback, response)
            else:
                return response
        mock_img_download.side_effect = _handle_img_download 
        
        self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "custom_upload",
                "urls": ["http://custom_thumbnail.jpg"]
                }

        vals = {'thumbnails' : [data]}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 202) 

        # Get all thumbnails, check custom_upload & verify in DB
        # Verify Thumbnail in videometadata obj
        i_vid = self.api_key + "_" + vid
        vmdata = neondata.VideoMetadata.get(i_vid)
        thumbs = neondata.ThumbnailMetadata.get_many(vmdata.thumbnail_ids)
        c_thumb = None
        for thumb in thumbs:
            if thumb.type == neondata.ThumbnailType.CUSTOMUPLOAD:
                c_thumb = thumb
        self.assertIsNotNone(c_thumb)
        self.assertEqual(c_thumb.type, neondata.ThumbnailType.CUSTOMUPLOAD)
        self.assertIsNotNone(c_thumb.phash)
        self.assertEqual(c_thumb.urls, 
                         ['https://s3.amazonaws.com/host-thumbnails/%s.jpg' %
                          re.sub('_', '/', c_thumb.key),
                          'http://custom_thumbnail.jpg'])
        
        s_url = neondata.ThumbnailServingURLs.get(c_thumb.key)
        self.assertIsNotNone(s_url)
        s3httpRe = re.compile('http://n[0-9].neon-images.com/([a-zA-Z0-9\-\._/]+)')
        serving_url = s_url.get_serving_url(160, 120)
        self.assertRegexpMatches(serving_url, s3httpRe)
        serving_key = s3httpRe.search(serving_url).group(1)

        # Make sure that image is in S3 both for serving and main
        self.assertIsNotNone(conn.get_bucket('host-thumbnails').get_key(
            re.sub('_', '/', c_thumb.key) + '.jpg'))
        self.assertIsNotNone(conn.get_bucket('host-thumbnails').get_key(
            "%s/%s/customupload0.jpg" % (self.api_key, vid)))
        self.assertIsNotNone(conn.get_bucket('n3.neon-images.com').get_key(
            serving_key))

        # RGBA image
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "custom_upload",
                "urls": ["http://rgba.png"]
                }
        vals = {'thumbnails' : [data]}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 202)

        # Check that the image is converted to RGB
        i_vid = self.api_key + "_" + vid
        vmdata = neondata.VideoMetadata.get(i_vid)
        thumbs = neondata.ThumbnailMetadata.get_many(vmdata.thumbnail_ids)
        for thumb in thumbs:
            if thumb.type == neondata.ThumbnailType.CUSTOMUPLOAD:
                s3key = conn.get_bucket('host-thumbnails').get_key(
                    re.sub('_', '/', thumb.key) + '.jpg')
                self.assertIsNotNone(s3key)
                buf = StringIO()
                s3key.get_contents_to_file(buf)
                buf.seek(0)
                im = Image.open(buf)
                self.assertEqual(im.mode, 'RGB')

        # Check that the ranks decrease for custom uploads
        self.assertIsNotNone(conn.get_bucket('host-thumbnails').get_key(
            "%s/%s/customupload-1.jpg" % (self.api_key, vid)))
        
    
        # Image download error
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "custom_upload",
                "urls": ["http://error_image.jpg"]
                }

        vals = {'thumbnails' : [data]}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 400)

        # cloudinary error 
        mresponse.read.return_value =  '{"error": "fake error"}' 
        mock_urllib2.urlopen.return_value = mresponse 
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "custom_upload",
                "urls": ["http://custom_thumbnail.jpg"]
                }

        vals = {'thumbnails' : [data]}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 202) 

        # Make sure that there are 3 custom thumbs now
        i_vid = self.api_key + "_" + vid
        vmdata = neondata.VideoMetadata.get(i_vid)
        thumbs = neondata.ThumbnailMetadata.get_many(vmdata.thumbnail_ids)
        self.assertEqual(
            len([x for x in thumbs 
                 if x.type == neondata.ThumbnailType.CUSTOMUPLOAD]), 3)

    def test_disable_thumbnail(self):
        '''
        Test disable thumbnail
        '''

        self._setup_initial_brightcove_state()
        vids = self._get_videos()
        vid  = vids[0]
        job_id = self.job_ids[0]
        tids = self._get_thumbnails(vid)
        tid = tids[0]
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/thumbnails/%s" %(self.a_id, self.b_id, tid))
        vals = {'property' : "enabled", "value" : False}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 202) 

    def test_job_status(self):
        '''
        Get Job Status 
        '''

        self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        job_id = self.job_ids[0]
        url = self.get_url("/api/v1/jobs/%s/" % job_id)
        response = self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
        jresponse = json.loads(response.body)
        self.assertEqual(jresponse["job_id"], job_id)
        self.assertEqual(jresponse["video_id"], vid)

    def test_update_video_abtest_state(self):
        '''
        Test udpating video abtest state
        '''

        self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        url = self.get_url("/api/v1/accounts/%s/neon_integrations"
                    "/%s/videos/%s" %(self.a_id, "0", vid))
        vals = {"abtest" : False}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 202)

        # not a boolean, invalid value
        vals = {"abtest" : "abe"}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 400)
        err_msg = '{"error": "invalid data type or not boolean"}'
        self.assertEqual(response.body, err_msg)
    
    def test_get_video(self):
        '''
        Get Video via videos/:video_id endpoint
        '''

        self._setup_initial_brightcove_state()
        vids = self._get_videos()
        vid  = vids[0]
        job_id = self.job_ids[0]
        tids = self._get_thumbnails(vid)
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        response = self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
        resp = json.loads(response.body)
        self.assertEqual(resp['items'][0]['video_id'], vid)

    def test_get_video_ids(self):
        ''' /videoids api '''
        self._setup_initial_brightcove_state()
        vids = self._get_videos()
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videoids" %(self.a_id, self.b_id))
        response = self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
        resp = json.loads(response.body)
        r_vids = resp['videoids']
        self.assertListEqual(sorted(r_vids), sorted(vids))
   
    @patch('supportServices.services.utils.http') 
    def test_healthcheck(self):
        url = self.get_url("/healthcheck")
        response = self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
       
        request = tornado.httpclient.HTTPRequest(url="http://test")
        response = tornado.httpclient.HTTPResponse(
                    request, 200, buffer=StringIO())
        mock_http.send_request.side_effect = lambda x, callback:\
            callback(response)

        url = self.get_url("/healthcheck/video_server")
        response = self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)

##### OOYALA PLATFORM TEST ######

class TestOoyalaServices(tornado.testing.AsyncHTTPTestCase):
    ''' Ooyala services Test '''
        
    @classmethod
    def setUpClass(cls):
        super(TestOoyalaServices, cls).setUpClass()

    def setUp(self):
        super(TestOoyalaServices, self).setUp()

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        
        random.seed(1949)

        #Ooyala api http mock
        #Http Connection pool Mock
        self.cp_sync_patcher = \
          patch('utils.http.tornado.httpclient.HTTPClient')
        self.cp_async_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.cp_mock_client = self.cp_sync_patcher.start()
        self.cp_mock_async_client = self.cp_async_patcher.start()
        
        self.cp_mock_client().fetch.side_effect = \
          self._success_http_side_effect 
        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect

        self.oo_api_key = 'oo_api_key_now'
        self.oo_api_secret = 'oo_secret'
       
        self.a_id = "oo_test"
        self.i_id = "oo_iid_1"
        self.job_ids = [] 
        
    def tearDown(self):
        self.cp_sync_patcher.stop()
        self.cp_async_patcher.stop()
        self.redis.stop()
    
    def get_app(self):
        ''' return services app '''
        return services.application

    #def get_new_ioloop(self):
    #    return tornado.ioloop.IOLoop.instance()

    def get_request(self, url, apikey):
        ''' get request to the app '''

        headers = {'X-Neon-API-Key' :apikey} 
        self.http_client.fetch(url, self.stop, headers=headers)
        resp = self.wait()
        return resp
    
    def post_request(self, url, vals, apikey):
        ''' post request to the app '''

        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded'}
        body = urllib.urlencode(vals)
        self.http_client.fetch(url,
                               callback=self.stop,
                               method="POST",
                               body=body,
                               headers=headers)
        response = self.wait()
        return response
    
    def put_request(self, url, vals, apikey, jsonheader=False):
        ''' put request to the app '''

        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded'}
        body = urllib.urlencode(vals)
        
        if jsonheader: 
            headers = {'X-Neon-API-Key' : apikey, 
                    'Content-Type':'application/json'}
            body = json.dumps(vals)
        
        self.http_client.fetch(url, self.stop, method="PUT", body=body,
                               headers=headers)
        response = self.wait()
        return response

    def create_neon_account(self):
        ''' create neon user account '''

        vals = { 'account_id' : self.a_id }
        uri = self.get_url('/api/v1/accounts') 
        response = self.post_request(uri, vals, "")
        api_key = json.loads(response.body)["neon_api_key"]
        tai = json.loads(response.body)["tracker_account_id"]
        return api_key

    def create_ooyala_account(self):
        ''' create ooyala platform account '''

        #create a neon account first
        self.api_key = self.create_neon_account()
        self.assertEqual(self.api_key, 
                neondata.NeonApiKey.get_api_key(self.a_id))

        url = self.get_url('/api/v1/accounts/' + self.a_id + \
                            '/ooyala_integrations')

        vals = {'integration_id' : self.i_id, 'partner_code' : 'partner123',
                'oo_api_key' : self.oo_api_key, 'oo_secret_key': self.oo_api_secret, 
                'auto_update': False}
        resp = self.post_request(url, vals, self.api_key)
        return resp.body

    def _success_http_side_effect(self, *args, **kwargs):
        ''' generic sucess http side effects for all patched http calls 
            for this test ''' 
        
        def _neon_submit_job_response():
            ''' video server response on job submit '''
            job_id = str(random.random())
            self.job_ids.append(job_id)
            request = tornado.httpclient.HTTPRequest('http://thumbnails.neon-lab.com')
            response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"%s"}'%job_id))
            return response
        
        #################### HTTP request/responses #################
        #mock ooyala api call
        ooyala_request = tornado.httpclient.HTTPRequest('http://api.ooyala.com')
        ooyala_response = tornado.httpclient.HTTPResponse(ooyala_request, 200,
                buffer=StringIO(ooyala_responses.assets))
        
        #mock neon api call
        request = tornado.httpclient.HTTPRequest('http://neon-lab.com')
        response = tornado.httpclient.HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"j123"}'))
        
        #################### HTTP request/responses #################
        http_request = args[0]
        if kwargs.has_key("callback"):
            callback = kwargs["callback"]
        else:
            callback = args[1] if len(args) >=2 else None
       
        #print "----> ",http_request.url, callback

        #video stream call
        if "/streams" in http_request.url:
            request = tornado.httpclient.HTTPRequest(http_request.url)
            response = tornado.httpclient.HTTPResponse(request, 200,
                    buffer=StringIO(ooyala_responses.streams))
            if callback:
                return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                     response)
            else:
                return response

        #PUT call to set primary image or POST call to upload image
        elif "primary_preview_image" in http_request.url or "/preview_image_files" in http_request.url:
            request = tornado.httpclient.HTTPRequest('http://ooyala.com')
            response = tornado.httpclient.HTTPResponse(request, 200,
                    buffer=StringIO(''))
        
            if callback:
                return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                     response)
            else:
                return response
       
        #generic asset call
        elif "/v2/assets" in http_request.url:
            if callback:
                return tornado.ioloop.IOLoop.current().add_callback(
                    callback,
                    ooyala_response)
            else:
                return ooyala_response
        
        elif "jpg" in http_request.url or "jpeg" in http_request.url or \
                    "http://servicesunittest.mock.com" in http_request.url:
            #downloading any image (create a random image response)
            response = create_random_image_response()
            if callback:
                return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                     response)
            else:
                return response

        #Download image from Ooyala CDN
        #elif "http://ak.c.ooyala" in http_request.url:
        #    return create_random_image_response()
            
        #neon api request
        elif "api/v1/submitvideo" in http_request.url:
            response = _neon_submit_job_response()
            if callback:    
                return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                     response)
            return response

        else:
            headers = {"Content-Type": "text/plain"}
            response = tornado.httpclient.HTTPResponse(request, 200, headers=headers,
                buffer=StringIO('someplaindata'))
            if callback:
                return tornado.ioloop.IOLoop.current().add_callback(callback,
                                                                     response)
            return response

    def _create_request_from_feed(self):
        '''
        Create requests from ooyala feed 
        '''
        self.create_ooyala_account()

        #Get ooyala account 
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key,
                                                         self.i_id)
        
        #create feed request
        oo_account.check_feed_and_create_requests()
  
    def _process_ooyala_neon_api_requests(self):
        '''
        Mock process the neon api requests
        '''
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key,
                                                         self.i_id)
        api_request_keys = []
        for vid, job_id in oo_account.videos.iteritems():
            api_request_keys.append((job_id, self.api_key))
            api_request = neondata.OoyalaApiRequest(job_id, self.api_key, 
                                            self.i_id, vid, 'title', 'url',
                                            'oo_api_key', 'oo_secret_key', 
                                            'p_thumb', 'http_callback')
            api_request.autosync = False
            api_request.set_api_method("topn", 5)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT
            self.assertTrue(api_request.save())
        
        api_requests = neondata.NeonApiRequest.get_many(api_request_keys)
        process_neon_api_requests(api_requests, self.api_key,
                                  self.i_id, "ooyala")

    def test_create_ooyala_requests(self):

        self._create_request_from_feed()

        #Verify that there is an experiment strategy for the account
        strategy = neondata.ExperimentStrategy.get(self.api_key)
        self.assertIsNotNone(strategy)
        self.assertTrue(strategy.only_exp_if_chosen)

        #Assert the job ids in the ooyala account
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key, self.i_id)
        self.assertTrue(len(oo_account.videos) >0)

    def _test_ooyala_signup_flow(self):
        '''
        Test account creations and creation of requests for first n videos
        '''
         
        signup_response = self.create_ooyala_account()
        vresponse = json.loads(signup_response)
        self.assertTrue(vresponse["total_count"] > 0)
        #verify that all items are in processing state, integration_type etc


    def _test_pagination_videos_ooyala(self):
        ''' test pagination of ooyala integration '''

        self._create_request_from_feed()
        self._process_ooyala_neon_api_requests()

        #get videos in pages
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/ooyala_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, self.i_id, page_no, page_size))
        resp = self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertEqual(len(result_vids), page_size)

    def _update_ooyala_thumbnail(self, vid, tid):    
        '''
        Services request to update the thumbnail 
        '''
        url = self.get_url("/api/v1/accounts/%s/ooyala_integrations"
                    "/%s/videos/%s" %(self.a_id, self.i_id, vid))
        vals = {'current_thumbnail' : tid}
        return self.put_request(url, vals, self.api_key)
    
    def test_update_thumbnail(self):
        '''
        Test updating thumbnail in ooyala account
        '''

        self._create_request_from_feed()
        self._process_ooyala_neon_api_requests()
        
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key,
                                                         self.i_id)
        
        new_tids = [] 
        for vid, job_id in oo_account.videos.iteritems(): 
            i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
            vmdata= neondata.VideoMetadata.get(i_vid)
            tids = vmdata.thumbnail_ids
            new_tids.append(tids[1])
            #set neon rank 2 
        resp = self._update_ooyala_thumbnail(vid, tids[1])
        self.assertEqual(resp.code, 200)

    def test_ooyala_account_update(self):
        '''
        Update the ooyala account 
        '''
            
        self._create_request_from_feed()
        self._process_ooyala_neon_api_requests()
        url = self.get_url('/api/v1/accounts/%s/ooyala_integrations/%s' \
                            %(self.a_id, self.i_id))
        vals = {'oo_secret_key' : 'sec', 'oo_api_key': 'okey', 
                'auto_update': 'false', 'partner_code': 'part'}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 200)

        oo_account = neondata.OoyalaPlatform.get_account(self.api_key,
                                                         self.i_id)
        self.assertEqual(oo_account.ooyala_api_key, 'okey') 
       
        #Test Missing an argument
        vals = {'oo_secret_key' : 'sec', 'oo_api_key': 'okey', 
                'auto_update': False}
        response = self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 400)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
