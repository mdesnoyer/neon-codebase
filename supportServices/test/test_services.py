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

import Image
import json
import random
import unittest
import urllib
import test_utils.redis
import tornado.gen
import tornado.ioloop
import utils.neon
import time
import datetime
from StringIO import StringIO
from mock import patch 
from supportServices import services, neondata
from api import brightcove_api
#from api import ooyala_api 
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase, AsyncHTTPClient
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from utils.options import define, options
import logging
_log = logging.getLogger(__name__)

import api.properties
import bcove_responses
import ooyala_responses


### Global helper methods
TIME_OUT = 100
mock_image_url_prefix = "http://servicesunittest.mock.com/"

def create_random_image():
    ''' create random image data for http response'''
    h = 360
    w = 480
    pixels = [(0, 0, 0) for _w in range(h*w)] 
    r = random.randrange(0, 255)
    g = random.randrange(0, 255)
    b = random.randrange(0, 255)
    pixels[0] = (r, g, b)
    im = Image.new("RGB", (h, w))
    im.putdata(pixels)
    return im

def create_random_image_response():
        '''http image response''' 
        
        request = HTTPRequest("http://someimageurl/image.jpg")
        im = create_random_image()
        imgstream = StringIO()
        im.save(imgstream, "jpeg", quality=100)
        imgstream.seek(0)

        response = HTTPResponse(request, 200,
                    buffer=imgstream)
        return response
    
def process_neon_api_requests(api_requests, api_key, i_id, t_type):
    #Create thumbnail metadata
    N_THUMBS = 5
    images = {} 
    thumbnail_url_to_image = {}
    for api_request in api_requests:
        video_id = api_request.video_id
        job_id = api_request.job_id
        thumbnails = []
        for t in range(N_THUMBS):
            image = create_random_image() 
            filestream = StringIO()
            image.save(filestream, "JPEG", quality=100) 
            filestream.seek(0)
            imgdata = filestream.read()
            tid = neondata.ThumbnailID.generate(imgdata,
                            neondata.InternalVideoID.generate(api_key,
                            video_id))
            images[tid] = image
            urls = []
            url = mock_image_url_prefix + "/thumb-%s"%t
            urls.append(url)
            thumbnail_url_to_image[url] = imgdata
            created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if t == N_THUMBS -1:
                tdata = neondata.ThumbnailMetaData(tid, urls, created, 480, 360,
                    t_type, 2, "test", enabled=True, rank=0)
            else:
                tdata = neondata.ThumbnailMetaData(tid, urls, created, 480, 360,
                    "neon", 2, "test", enabled=True, rank=t+1)
            thumbnails.append(tdata)
    
        i_vid = neondata.InternalVideoID.generate(api_key, video_id)
        thumbnail_mapper_list = []
        thumbnail_url_mapper_list = []
        for thumb in thumbnails:
            tid = thumb.thumbnail_id
            for t_url in thumb.urls:
                uitem = neondata.ThumbnailURLMapper(t_url, tid)
                thumbnail_url_mapper_list.append(uitem)
                item = neondata.ThumbnailIDMapper(tid, i_vid, thumb)
                thumbnail_mapper_list.append(item)
        retid = neondata.ThumbnailIDMapper.save_all(thumbnail_mapper_list)
        returl = neondata.ThumbnailURLMapper.save_all(thumbnail_url_mapper_list)

        #Update request state to FINISHED
        api_request.state = neondata.RequestState.FINISHED 
        api_request.save()
        tids = []
        for thumb in thumbnails:
            tids.append(thumb.thumbnail_id)
    
        vmdata = neondata.VideoMetadata(i_vid, tids, job_id, url,
                    10, 5, "test", i_id)
        vmdata.save()

###############################################
# Test Services
###############################################


class TestServices(AsyncHTTPTestCase):
    ''' Services Test '''
        
    @classmethod
    def setUpClass(cls):
        super(TestServices, cls).setUpClass()
        random.seed(19449)

    def setUp(self):
        super(TestServices, self).setUp()
        #NOTE: Make sure that you don't repatch objects

        #Brightcove api http mock
        self.bapi_sync_patcher = \
          patch('api.brightcove_api.tornado.httpclient.HTTPClient')
        self.bapi_async_patcher = \
          patch('api.brightcove_api.tornado.httpclient.AsyncHTTPClient')
        self.bapi_mock_client = self.bapi_sync_patcher.start()
        self.bapi_mock_async_client = self.bapi_async_patcher.start()

        #Http Connection pool Mock
        self.cp_sync_patcher = \
          patch('utils.http.tornado.httpclient.HTTPClient')
        self.cp_async_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.cp_mock_client = self.cp_sync_patcher.start()
        self.cp_mock_async_client = self.cp_async_patcher.start()

        self.a_id = "unittester-0"
        self.rtoken = "rtoken"
        self.wtoken = "wtoken"
        self.b_id = "i12345" #i_id bcove
        self.pub_id = "p124"
        self.mock_image_url_prefix = "http://servicesunittest.mock.com/"
        self.thumbnail_url_to_image = {} # mock url => raw image buffer data
        self.job_ids = [] #ordered list
        self.video_ids = []
        self.images = {} 

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        
    def tearDown(self):
        self.bapi_sync_patcher.stop()
        self.bapi_async_patcher.stop()
        self.cp_sync_patcher.stop()
        self.cp_async_patcher.stop()
        self.redis.stop()
        super(TestServices, self).tearDown()
    
    def get_app(self):
        ''' return services app '''
        return services.application

    # TODO: It should be possible to run this with an IOLoop for each
    # test, but it's not running. Need to figure out why.
    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()

    def post_request(self, url, vals, apikey):
        ''' post request to the app '''

        http_client = AsyncHTTPClient(self.io_loop)
        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded'}
        body = urllib.urlencode(vals)
        http_client.fetch(url,
                     callback=self.stop,
                     method="POST",
                     body=body,
                     headers=headers)
        response = self.wait(timeout=10)
        return response

    def put_request(self, url, vals, apikey):
        ''' put request to the app '''

        http_client = AsyncHTTPClient(self.io_loop)
        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded' }
        body = urllib.urlencode(vals)
        http_client.fetch(url, self.stop,method="PUT", body=body, headers=headers)
        response = self.wait(timeout=10)
        return response

    def get_request(self, url, apikey):
        ''' get request to the app '''

        headers = {'X-Neon-API-Key' :apikey} 
        http_client = AsyncHTTPClient(self.io_loop)
        http_client.fetch(url, self.stop, headers=headers)
        resp = self.wait(timeout=10)
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
            api_request = neondata.BrightcoveApiRequest(job_id, self.api_key, vid,
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

    def _process_neon_api_requests(self, api_requests):
        #Create thumbnail metadata
        N_THUMBS = 5
        for api_request in api_requests:
            video_id = api_request.video_id
            job_id = api_request.job_id
            thumbnails = []
            for t in range(N_THUMBS):
                image = create_random_image() 
                filestream = StringIO()
                image.save(filestream, "JPEG", quality=100) 
                filestream.seek(0)
                imgdata = filestream.read()
                tid = neondata.ThumbnailID.generate(imgdata,
                                neondata.InternalVideoID.generate(self.api_key,
                                video_id))
                self.images[tid] = image
                urls = [] ; url = self.mock_image_url_prefix + "/thumb-%s"%t
                urls.append(url)
                self.thumbnail_url_to_image[url] = imgdata
                created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if t == N_THUMBS -1:
                    tdata = neondata.ThumbnailMetaData(tid, urls, created, 480, 360,
                        "brightcove", 2, "test", enabled=True, rank=0)
                else:
                    tdata = neondata.ThumbnailMetaData(tid, urls, created, 480, 360,
                        "neon", 2, "test", enabled=True, rank=t+1)
                thumbnails.append(tdata)
        
            i_vid = neondata.InternalVideoID.generate(self.api_key, video_id)
            thumbnail_mapper_list = []
            thumbnail_url_mapper_list = []
            for thumb in thumbnails:
                tid = thumb.thumbnail_id
                for t_url in thumb.urls:
                    uitem = neondata.ThumbnailURLMapper(t_url, tid)
                    thumbnail_url_mapper_list.append(uitem)
                    item = neondata.ThumbnailIDMapper(tid, i_vid, thumb)
                    thumbnail_mapper_list.append(item)
            retid = neondata.ThumbnailIDMapper.save_all(thumbnail_mapper_list)
            returl = neondata.ThumbnailURLMapper.save_all(thumbnail_url_mapper_list)
            self.assertTrue(retid)
            self.assertTrue(returl)

            #Update request state to FINISHED
            api_request.state = neondata.RequestState.FINISHED 
            api_request.save()
            tids = []
            for thumb in thumbnails:
                tids.append(thumb.thumbnail_id)
        
            vmdata = neondata.VideoMetadata(i_vid, tids, job_id, url,
                        10, 5, "test", self.b_id)
            self.assertTrue(vmdata.save())


    def _get_video_status_brightcove(self):
        ''' get video status for all videos in brightcove '''

        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                            "/%s/videos" %(self.a_id, self.b_id))
        headers = {'X-Neon-API-Key' : self.api_key} 
        client = AsyncHTTPClient(self.io_loop)
        client.fetch(url, self.stop, headers=headers)
        resp = self.wait()
        items = json.loads(resp.body)
        return items

    def _check_video_status_brightcove(self, vstatus):
        ''' assert video status for brightcove videos'''
        
        items = self._get_video_status_brightcove()
        for item in items['items']:
            vr = services.VideoResponse(None, None, None, None, None, 
                                    None, None, None, None)
            vr.__dict__ = item
            status = vr.status
            self.assertEqual(status, vstatus)

    def _check_neon_default_chosen(self, videos): 
        ''' validate neon rank 1 is chosen on default publish '''

        for vid in videos:
            i_vid = neondata.InternalVideoID.generate(self.api_key, vid) 
            vmdata = neondata.VideoMetadata.get(i_vid)
            thumbnails = neondata.ThumbnailIDMapper.get_thumb_mappings(
                            vmdata.thumbnail_ids)
            for thumbnail in thumbnails:
                thumb = thumbnail.get_metadata()
                if thumb["chosen"] == True and thumb["type"] == 'neon':
                    self.assertEqual(thumb["rank"],1)

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
        vals = {'thumbnail_id' : tid }
        return self.put_request(url, vals, self.api_key)
   
    def _success_http_side_effect(self, *args, **kwargs):
        ''' generic sucess http side effects for all patched http calls 
            for this test ''' 

        def _neon_submit_job_response():
            ''' video server response on job submit '''
            job_id = str(random.random())
            self.job_ids.append(job_id)
            request = HTTPRequest('http://thumbnails.neon-lab.com')
            response = HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"%s"}'%job_id))
            return response

        def _add_image_response(req): 
            ''' image response '''
            itype = "THUMBNAIL"
            if "VIDEO_STILL" in req.body:
                itype = "VIDEO_STILL"

            request = HTTPRequest("http://api.brightcove.com/services/post")
            response = HTTPResponse(request, 200,
                buffer=StringIO
                    ('{"result": {"displayName":"test","id":123,'
                    '"referenceId":"test_ref_id","remoteUrl":null,"type":"%s"},'
                    '"error": null, "id": null}'%itype))
            return response
        
        #################### HTTP request/responses #################
        #mock brightcove api call
        bcove_request = HTTPRequest('http://api.brightcove.com/services/library?'
            'get_item_count=true&command=find_all_videos&page_size=5&sort_by='
            'publish_date&token=rtoken&page_number=0&output=json&media_delivery=http') #build the string
        bcove_response = HTTPResponse(bcove_request, 200,
                buffer=StringIO(bcove_responses.find_all_videos_response))
        
        #mock neon api call
        request = HTTPRequest('http://google.com')
        response = HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"neon error"}'))
        
        #################### HTTP request/responses #################
        http_request = args[0]
        if kwargs.has_key("callback"):
            callback = kwargs["callback"]
        else:
            callback = args[1] if len(args) >=2 else None

        if "/services/library?command=find_video_by_id" in http_request.url:
            request = HTTPRequest(http_request.url)
            response = HTTPResponse(request, 200,
                    buffer=StringIO(bcove_responses.find_video_by_id_response))
            if kwargs.has_key("callback"):
                callback = kwargs["callback"]
                return self.io_loop.add_callback(callback, response)
            else:
                if len(args) > 1:
                    callback = args[1]
                    return self.io_loop.add_callback(callback, response)
                else:
                    return response

        elif "http://api.brightcove.com/services/library" in http_request.url:
            return bcove_response
           
        #add_image api call 
        elif "http://api.brightcove.com/services/post" in http_request.url:
            return _add_image_response(http_request) 

        #Download image from brightcove CDN
        elif "http://brightcove.vo.llnwd.net" in http_request.url:
            return self._create_random_image_response()
            
        #Download image from mock unit test url ; This is done async in the code
        elif self.mock_image_url_prefix in http_request.url:
            request = HTTPRequest(http_request.url)
            response = HTTPResponse(request, 200,
                    buffer=StringIO(self.thumbnail_url_to_image[http_request.url]))
            #on async fetch, callback is returned
            #check if callable -- hasattr(obj, '__call__')
            if kwargs.has_key("callback"):
                callback  = kwargs["callback"]
            else:
                callback = args[1] 
            return self.io_loop.add_callback(callback, response)

        #neon api request
        elif "api/v1/submitvideo" in http_request.url:
            response = _neon_submit_job_response()
            if callback:    
                return self.io_loop.add_callback(callback, response)
            return response

        elif "jpg" in http_request.url or "jpeg" in http_request.url:
            #downloading any image (create a random image response)
            response = self._create_random_image_response()
            if callback:
                return self.io_loop.add_callback(callback, response)
            else:
                return response

        elif ".mp4" in http_request.url:
            headers = {"Content-Type": "video/mp4"}
            response = HTTPResponse(request, 200, headers=headers,
                buffer=StringIO('videodata'))
            if callback:
                return self.io_loop.add_callback(callback, response)
        else:
            headers = {"Content-Type": "text/plain"}
            response = HTTPResponse(request, 200, headers=headers,
                buffer=StringIO('someplaindata'))
            if callback:
                return self.io_loop.add_callback(callback, response)
            return response

    def _setup_initial_brightcove_state(self):
        '''
        Setup the state of a brightcove account with 5 processed videos 
        '''
        #Setup Side effect for the http clients
        self.bapi_mock_client().fetch.side_effect = \
          self._success_http_side_effect
        self.cp_mock_client().fetch.side_effect = \
          self._success_http_side_effect 
        self.bapi_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect
        self.cp_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect
    
        #set up account and video state for testing
        self.api_key = self.create_neon_account()
        json_video_response = self.create_brightcove_account()
        self.assertNotEqual(json_video_response, '{}') # !empty json response
        
        #verify account id added to Neon user account
        nuser = neondata.NeonUserAccount.get_account(self.api_key)
        self.assertTrue(self.b_id in nuser.integrations.keys()) 
        
        reqs = self._create_neon_api_requests()
        self._process_neon_api_requests(reqs)
   
    def _create_random_image_response(self):
        '''http image response''' 
        
        request = HTTPRequest("http://someimageurl/image.jpg")
        im = self.images.values()[0]#self._create_random_image()
        imgstream = StringIO()
        im.save(imgstream, "jpeg", quality=100)
        imgstream.seek(0)

        response = HTTPResponse(request, 200,
                    buffer=imgstream)
        return response

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

        #Setup Side effect for the http clients
        self.bapi_mock_client().fetch.side_effect = \
          self._success_http_side_effect
        self.cp_mock_client().fetch.side_effect = \
          self._success_http_side_effect 
        self.bapi_mock_async_client().fetch.side_effect = \
          self._success_http_side_effect
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

    def test_autopublish_brightcove_account(self):
        with options._set_bounded('supportServices.neondata.dbPort',
                                  self.redis.port):

            #Setup Side effect for the http clients
            self.bapi_mock_client().fetch.side_effect = \
              self._success_http_side_effect
            self.cp_mock_client().fetch.side_effect = \
              self._success_http_side_effect 
            self.bapi_mock_async_client().fetch.side_effect = \
              self._success_http_side_effect
            self.cp_mock_async_client().fetch.side_effect = \
              self._success_http_side_effect

            #create neon account first & create brightcove account
            json_video_response = self.create_brightcove_account()
            video_response = json.loads(json_video_response)['items']
            self.assertEqual(len(video_response), 5)

            #update brightcove account
            new_rtoken = ("newrtoken")
            update_response = self.update_brightcove_account(new_rtoken)
            self.assertEqual(update_response.code, 200)
        
            #auto publish test
            reqs = self._create_neon_api_requests()
            self._process_neon_api_requests(reqs)
            self._check_video_status_brightcove(
                vstatus=neondata.RequestState.FINISHED)
        
            update_response = self.update_brightcove_account(autoupdate=True)
            self.assertEqual(update_response.code, 200)

            #Check Neon rank 1 thumbnail is the new thumbnail for the videos
            vitems = json.loads(bcove_responses.find_all_videos_response)
            videos = []
            for item in vitems['items']:
                videos.append(str(item['id']))                              
            self._check_neon_default_chosen(videos)

    def test_brightcove_web_account_flow(self):
        #Create Neon Account --> Bcove Integration --> update Integration --> 
        #query videos --> autopublish --> verify autopublish
        with options._set_bounded('supportServices.neondata.dbPort',
                                  self.redis.port):
        
            #create neon account
            api_key = self.create_neon_account()
            self.assertEqual(api_key, neondata.NeonApiKey.get_api_key(self.a_id))

            #Setup Side effect for the http clients
            self.bapi_mock_client().fetch.side_effect = \
                                self._success_http_side_effect
            self.cp_mock_client().fetch.side_effect = \
                                self._success_http_side_effect 
            self.bapi_mock_async_client().fetch.side_effect = \
                                self._success_http_side_effect
            self.cp_mock_async_client().fetch.side_effect = \
                                self._success_http_side_effect

            #create brightcove account
            json_video_response = self.create_brightcove_account()
            video_response = json.loads(json_video_response)['items']
            self.assertEqual(len(video_response), 5) 
        
            #process requests
            reqs = self._create_neon_api_requests()
            self._process_neon_api_requests(reqs)
            
            videos = []
            vitems = json.loads(bcove_responses.find_all_videos_response)
            for item in vitems['items']:
                videos.append(str(item['id']))                             

            #update a thumbnail
            new_tids = [] 
            for vid,job_id in zip(videos, self.job_ids):
                i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
                vmdata= neondata.VideoMetadata.get(i_vid)
                tids = vmdata.thumbnail_ids
                new_tids.append(tids[1])
                #set neon rank 2 
                resp = self.update_brightcove_thumbnail(vid, tids[1])
                self.assertEqual(resp.code, 200)
                
                #assert request state
                req_data = neondata.NeonApiRequest.get_request(self.api_key,job_id)
                vid_request = neondata.NeonApiRequest.create(req_data)
                self.assertEqual(vid_request.state,neondata.RequestState.ACTIVE)

            thumbs = []
            items = self._get_video_status_brightcove()
            for item,tid in zip(items['items'], new_tids):
                vr = services.VideoResponse(None, None, None, None,
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
            if self.mock_image_url_prefix in http_request.url:
                request = HTTPRequest(http_request.url)
                response = HTTPResponse(request, 500,
                    buffer=StringIO("Server error"))
                if kwargs.has_key("callback"):
                    callback = kwargs["callback"]
                else:
                    callback = args[1]
                return self.io_loop.add_callback(callback, response)

            if "http://api.brightcove.com/services/post" in http_request.url:
                itype = "THUMBNAIL"
                if "VIDEO_STILL" in http_request.body:
                    itype = "VIDEO_STILL"

                request = HTTPRequest("http://api.brightcove.com/services/post")
                response = HTTPResponse(request, 500,
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
        req_data = neondata.NeonApiRequest.get_request(self.api_key, job_id)
        vid_request = neondata.NeonApiRequest.create(req_data)
        self.assertEqual(vid_request.state, neondata.RequestState.FINISHED)
        
        #assert the previous thumbnail is still the thumb in DB
        self.update_brightcove_thumbnail(vid, tid)
        tid2 = tids[1]
        vals = {'thumbnail_id' : tid2}
        resp = self.post_request(url, vals, self.api_key)
        self.assertEqual(resp.code, 200)
        tids = neondata.ThumbnailIDMapper.get_thumb_mappings([tid, tid2])
        self.assertTrue(tids[0].thumbnail_metadata["chosen"])
        self.assertFalse(tids[1].thumbnail_metadata["chosen"])

    def test_bh_check_thumbnail(self):
        ''' Brightcove support handler tests (check thumb/update thumb) '''
        
        self._setup_initial_brightcove_state()
        
        vids = self._get_videos()
        vid  = vids[0]
        tids = self._get_thumbnails(vid)
        i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
        tid  = tids[0]
        url = self.get_url('/api/v1/brightcovecontroller/%s/checkthumbnail/%s' 
                        %(self.api_key, i_vid))
        vals = {}
        
        # thumbnail md5 shouldnt be found since random image was returned
        # while creating the response for get_image()
        resp = self.post_request(url, vals, self.api_key)
        self.assertEqual(resp.code, 200) 

        #NOTE: ImageMD5 gets saved as part of image upload to brightcove
        #hence simulate that so as to create DB entry for check thumbnail run

        #TODO: Return image that was saved so that thumbnail check succeceds
        #HACK: all potential tids here are associated with single video 
        md5_objs = []
        for tid,image in self.images.iteritems():
            t_md5 = neondata.ImageMD5Mapper(vid, image, tid)
            md5_objs.append(t_md5) 
        res = neondata.ImageMD5Mapper.save_all(md5_objs)
        

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
    
    def test_invalid_model_scores(self):
        ''' test filtering of invalid model scores like -inf, nan '''

        self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        tids = self._get_thumbnails(vid)
        
        #update in database the thumbnail to have -inf score
        td = neondata.ThumbnailIDMapper.get_thumb_mappings(tids)
        td[0].thumbnail_metadata['model_score'] = float('-inf')
        td[1].thumbnail_metadata['model_score'] = float('nan')
        td[2].thumbnail_metadata['model_score'] = None 
        neondata.ThumbnailIDMapper.save_all(td)
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
        a_id,itype = neondata.TrackerAccountIDMapper.get_neon_account_id(tai)   
        self.assertEqual(self.a_id, a_id)
        self.assertEqual(itype,neondata.TrackerAccountIDMapper.PRODUCTION)
        a_id,itype = neondata.TrackerAccountIDMapper.get_neon_account_id(s_tai)   
        self.assertEqual(self.a_id, a_id)
        self.assertEqual(itype, neondata.TrackerAccountIDMapper.STAGING)

        #query tai
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/tracker_account_id'%(self.a_id, self.b_id))
        response = self.get_request(url, api_key)
        tai = json.loads(response.body)["tracker_account_id"]
        s_tai = json.loads(response.body)["staging_tracker_account_id"]
        a_id,itype = neondata.TrackerAccountIDMapper.get_neon_account_id(tai)   
        self.assertEqual(self.a_id, a_id)
        self.assertEqual(itype,neondata.TrackerAccountIDMapper.PRODUCTION)
        
        a_id,itype = neondata.TrackerAccountIDMapper.get_neon_account_id(s_tai)   
        self.assertEqual(self.a_id, a_id)
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

        api_key = self.create_neon_account()
        nplatform = neondata.NeonPlatform.get_account(api_key)
        nvids = 10 
        api_requests = [] 
        for i in range(nvids):
            vid = "neonvideo%s"%i 
            title = "title%s"%i 
            video_download_url = "http://video%s.mp4" %i 
            job_id = "job_id%s"%i 
            api_request = neondata.NeonApiRequest(job_id, api_key, vid,
                    title, video_download_url, "neon", "http://callback")
            api_request.set_api_method("topn",5)
            api_request.publish_time = str(time.time() *1000)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT
            self.assertTrue(api_request.save())
            api_requests.append(api_request)
            nplatform.add_video(vid, job_id)

        nplatform.save()
        random.seed(1123)
        #Create thumbnail metadata
        N_THUMBS = 5
        for api_request in api_requests[:-1]:
            video_id = api_request.video_id
            job_id = api_request.job_id
            thumbnails = []
            for t in range(N_THUMBS):
                image = create_random_image() 
                filestream = StringIO()
                image.save(filestream, "JPEG", quality=100) 
                filestream.seek(0)
                imgdata = filestream.read()
                tid = neondata.ThumbnailID.generate(imgdata,
                                neondata.InternalVideoID.generate(api_key,
                                video_id))
                self.images[tid] = image
                urls = [] ; url = self.mock_image_url_prefix + "/thumb-%s"%t
                urls.append(url)
                self.thumbnail_url_to_image[url] = imgdata
                created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                score = random.random()
                if t == N_THUMBS -1:
                    tdata = neondata.ThumbnailMetaData(tid, urls, created,
                            480, 360, neondata.ThumbnailType.CENTERFRAME,
                            score, "test", enabled=True, rank=0)
                else:
                    tdata = neondata.ThumbnailMetaData(tid, urls,
                            created, 480, 360,
                            neondata.ThumbnailType.NEON, score,
                            "test", enabled=True, rank=t+1)
                thumbnails.append(tdata)
        
            i_vid = neondata.InternalVideoID.generate(api_key, video_id)
            thumbnail_mapper_list = []
            thumbnail_url_mapper_list = []
            for thumb in thumbnails:
                tid = thumb.thumbnail_id
                for t_url in thumb.urls:
                    uitem = neondata.ThumbnailURLMapper(t_url, tid)
                    thumbnail_url_mapper_list.append(uitem)
                    item = neondata.ThumbnailIDMapper(tid, i_vid, thumb)
                    thumbnail_mapper_list.append(item)
            retid = neondata.ThumbnailIDMapper.save_all(thumbnail_mapper_list)
            returl = neondata.ThumbnailURLMapper.save_all(
                    thumbnail_url_mapper_list)
            self.assertTrue(retid)
            self.assertTrue(returl)

            #Update request state to FINISHED
            api_request.state = neondata.RequestState.FINISHED 
            api_request.save()
            tids = []
            for thumb in thumbnails:
                tids.append(thumb.thumbnail_id)
        
            vmdata = neondata.VideoMetadata(i_vid, tids, job_id, url,
                        10, 5, "test", self.b_id)
            self.assertTrue(vmdata.save())

        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size)
        result_vids = [x['video_id'] for x in items]
        
        #recommended
        page_size = 5
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/recommended?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size)
        result_vids = [ x['video_id'] for x in items]

        #processing
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/processing?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), 1) #1 video in processing
        
        #invalid state
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/invalid?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = self.get_request(url, api_key)
        self.assertEqual(resp.code, 400)
    
    def test_utils_handler(self):
        ''' random image utils handler test '''
        url = self.get_url('/api/v1/utils/get_random_image')
        resp = self.get_request(url, '')
        self.assertEqual(resp.code, 200)
        image = Image.open(StringIO(resp.body))
        self.assertIsNotNone(image)


    ##### OOYALA PLATFORM TEST ######

class TestOoyalaServices(AsyncHTTPTestCase):
    ''' Ooyala services Test '''
        
    @classmethod
    def setUpClass(cls):
        super(TestOoyalaServices, cls).setUpClass()
        random.seed(1949)

    def setUp(self):
        super(TestOoyalaServices, self).setUp()

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

        self.oo_api_key = 's0Y3YxOp0XTCL2hFlfFS1S2MRmaY.nxNs0'
        self.oo_api_secret = 'uwTrMevYq54eani8ViRn6Ar5-rwmmmvKwq1HDtCn'
       
        self.a_id = "oo_test"
        self.i_id = "oo_iid_1"
        self.job_ids = [] 

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        
    def tearDown(self):
        self.cp_sync_patcher.stop()
        self.cp_async_patcher.stop()
        self.redis.stop()
    
    def get_app(self):
        ''' return services app '''
        return services.application

    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()

    def get_request(self, url, apikey):
        ''' get request to the app '''

        headers = {'X-Neon-API-Key' :apikey} 
        http_client = AsyncHTTPClient(self.io_loop)
        http_client.fetch(url, self.stop, headers=headers)
        resp = self.wait(timeout=TIME_OUT)
        return resp
    
    def post_request(self, url, vals, apikey):
        ''' post request to the app '''

        http_client = AsyncHTTPClient(self.io_loop)
        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded'}
        body = urllib.urlencode(vals)
        http_client.fetch(url,
                     callback=self.stop,
                     method="POST",
                     body=body,
                     headers=headers)
        response = self.wait(timeout=TIME_OUT)
        return response
    
    def put_request(self, url, vals, apikey):
        ''' put request to the app '''

        http_client = AsyncHTTPClient(self.io_loop)
        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded' }
        body = urllib.urlencode(vals)
        http_client.fetch(url, self.stop,method="PUT", body=body, headers=headers)
        response = self.wait(timeout=TIME_OUT)
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
            request = HTTPRequest('http://thumbnails.neon-lab.com')
            response = HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"%s"}'%job_id))
            return response
        
        #################### HTTP request/responses #################
        #mock ooyala api call
        ooyala_request = HTTPRequest('http://api.ooyala.com')
        ooyala_response = HTTPResponse(ooyala_request, 200,
                buffer=StringIO(ooyala_responses.assets))
        
        #mock neon api call
        request = HTTPRequest('http://neon-lab.com')
        response = HTTPResponse(request, 200,
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
            request = HTTPRequest(http_request.url)
            response = HTTPResponse(request, 200,
                    buffer=StringIO(ooyala_responses.streams))
            if callback:
                return self.io_loop.add_callback(callback, response)
            else:
                return response

        #PUT call to set primary image or POST call to upload image
        elif "primary_preview_image" in http_request.url or "/preview_image_files" in http_request.url:
            request = HTTPRequest('http://ooyala.com')
            response = HTTPResponse(request, 200,
                    buffer=StringIO(''))
        
            if callback:
                return self.io_loop.add_callback(callback, response)
            else:
                return response
       
        #generic asset call
        elif "/v2/assets" in http_request.url:
            if callback:
                return self.io_loop.add_callback(callback, ooyala_response)
            else:
                return ooyala_response
        
        elif "jpg" in http_request.url or "jpeg" in http_request.url or \
                    "http://servicesunittest.mock.com" in http_request.url:
            #downloading any image (create a random image response)
            response = create_random_image_response()
            if callback:
                return self.io_loop.add_callback(callback, response)
            else:
                return response

        #Download image from Ooyala CDN
        #elif "http://ak.c.ooyala" in http_request.url:
        #    return create_random_image_response()
            
        #neon api request
        elif "api/v1/submitvideo" in http_request.url:
            response = _neon_submit_job_response()
            if callback:    
                return self.io_loop.add_callback(callback, response)
            return response

        else:
            headers = {"Content-Type": "text/plain"}
            response = HTTPResponse(request, 200, headers=headers,
                buffer=StringIO('someplaindata'))
            return response

    def _create_request_from_feed(self):
        '''
        Create requests from ooyala feed 
        '''
        self.create_ooyala_account()

        #Get ooyala account 
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key, self.i_id)
        
        
        #create feed request
        oo_account.check_feed_and_create_requests()
  
    def _process_neon_api_requests(self):
        '''
        Mock process the neon api requests
        '''
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key, self.i_id)
        api_request_keys = []
        for vid, job_id in oo_account.videos.iteritems():
            key = neondata.generate_request_key(self.api_key, job_id)
            api_request_keys.append(key)
            api_request = neondata.OoyalaApiRequest(job_id, self.api_key, 
                                            self.i_id, vid, 'title', 'url',
                                            'oo_api_key', 'oo_secret_key', 
                                            'p_thumb', 'http_callback')
            api_request.autosync = False
            api_request.set_api_method("topn", 5)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT
            self.assertTrue(api_request.save())
        
        api_requests = neondata.NeonApiRequest.get_requests(api_request_keys)
        process_neon_api_requests(api_requests, self.api_key, self.i_id, "ooyala")

    def test_create_ooyala_requests(self):

        self._create_request_from_feed()

        #Assert the job ids in the ooyala account
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key, self.i_id)
        self.assertTrue(len(oo_account.videos) >0)

    def test_ooyala_signup_flow(self):
        '''
        Test account creations and creation of requests for first n videos
        '''
         
        signup_response = self.create_ooyala_account()
        vresponse = json.loads(signup_response)
        self.assertTrue(vresponse["total_count"] > 0)
        #verify that all items are in processing state, integration_type etc


    def test_pagination_videos_ooyala(self):
        ''' test pagination of ooyala integration '''

        self._create_request_from_feed()
        self._process_neon_api_requests()

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
        vals = {'thumbnail_id' : tid}
        return self.put_request(url, vals, self.api_key)
    
    def test_update_thumbnail(self):
        '''
        Test updating thumbnail in ooyala account
        '''

        self._create_request_from_feed()
        self._process_neon_api_requests()
        
        oo_account = neondata.OoyalaPlatform.get_account(self.api_key, self.i_id)
        
        new_tids = [] 
        for vid, job_id in oo_account.videos.iteritems(): 
            i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
            vmdata= neondata.VideoMetadata.get(i_vid)
            tids = vmdata.thumbnail_ids
            new_tids.append(tids[1])
            #set neon rank 2 
        resp = self._update_ooyala_thumbnail(vid, tids[1])
        self.assertEqual(resp.code, 200)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
