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
        sys.path.insert(0,base_path)

import unittest
import urllib
import tornado.gen
import tornado.ioloop
import tornado.gen
import json
import random
import time
import Image
import datetime
from StringIO import StringIO
from mock import patch, MagicMock
from supportServices import services,neondata
from api import client,server,brightcove_api
from tornado.concurrent import Future
from tornado.testing import AsyncHTTPTestCase,AsyncTestCase,AsyncHTTPClient
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError

import api.properties

import bcove_responses
class TestBrightcoveServices(AsyncHTTPTestCase):

    def setUp(self):
        #TODO: Spin up new redis instance
        super(TestBrightcoveServices, self).setUp()
        self.real_httpclient = tornado.httpclient.HTTPClient()
        self.real_asynchttpclient = tornado.httpclient.AsyncHTTPClient(self.io_loop)
        self.sync_patcher = \
          patch('supportServices.services.tornado.httpclient.HTTPClient')
        self.async_patcher = \
          patch('supportServices.services.tornado.httpclient.AsyncHTTPClient')
        self.mock_client = self.sync_patcher.start()
        self.mock_async_client = self.async_patcher.start()

        #Brightcove api http mock
        self.bapi_sync_patcher = \
          patch('api.brightcove_api.tornado.httpclient.HTTPClient')
        self.bapi_async_patcher = \
          patch('api.brightcove_api.tornado.httpclient.AsyncHTTPClient')
        self.bapi_mock_client = self.bapi_sync_patcher.start()
        self.bapi_mock_async_client = self.bapi_async_patcher.start()

        #Http Connection pool Mock
        self.cp_sync_patcher = \
          patch('utils.connection_pool.tornado.httpclient.HTTPClient')
        self.cp_async_patcher = \
          patch('utils.connection_pool.tornado.httpclient.AsyncHTTPClient')
        self.cp_mock_client = self.cp_sync_patcher.start()
        self.cp_mock_async_client = self.cp_async_patcher.start()

        self.a_id = "unittester-0"
        self.api_key = neondata.NeonApiKey.generate(self.a_id) 
        self.rtoken = "rtoken"
        self.wtoken = "wtoken"
        self.b_id   = "i12345" #i_id bcove
        self.pub_id = "p124"
        self.mock_image_url_prefix = "http://servicesunittest.mock.com/"
        self.thumbnail_url_to_image = {} # mock url => raw image buffer data
        self.job_ids = [] #ordered list
        self.video_ids = []

    def get_app(self):
        return services.application
    
    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()

    def tearDown(self):
        self.sync_patcher.stop()
        self.async_patcher.stop()
        self.bapi_sync_patcher.stop()
        self.bapi_async_patcher.stop()
        self.delete_test_data()

    def delete_test_data(self):
        import redis
        client = redis.StrictRedis()
        keys = client.keys("*%s*"%self.api_key)
        for key in keys:
            client.delete(key)

    def post_request(self,url,vals,apikey):
        client = AsyncHTTPClient(self.io_loop)
        headers = {'X-Neon-API-Key' : apikey, 'Content-Type':'application/x-www-form-urlencoded' }
        body = urllib.urlencode(vals)
        client.fetch(url,self.stop,method="POST",body=body,headers=headers)
        response = self.wait()
        return response

    def put_request(self,url,vals,apikey):
        client = AsyncHTTPClient(self.io_loop)
        headers = {'X-Neon-API-Key' : apikey, 'Content-Type':'application/x-www-form-urlencoded' }
        body = urllib.urlencode(vals)
        client.fetch(url,self.stop,method="PUT",body=body,headers=headers)
        response = self.wait(timeout=6100)
        return response


    ### Helper methods
    
    def _create_random_image(self):
        h = 360
        w = 480
        pixels = [(0,0,0) for _w in range(h*w)] 
        r = random.randrange(0,255)
        g = random.randrange(0,255)
        b = random.randrange(0,255)
        pixels[0] = (r,g,b)
        im = Image.new("RGB",(h,w))
        im.putdata(pixels)
        return im

    def _create_neon_api_requests(self):

        api_requests = []
        vitems = json.loads(bcove_responses.find_all_videos_response)
        items = vitems['items']
        i=0
        for item in items:
            vid = str(item['id'])                              
            title = item['name']
            video_download_url = item['FLVURL']
            job_id = str(self.job_ids[i]) #str(random.random())
            p_thumb = item['videoStillURL']
            api_request = neondata.BrightcoveApiRequest(job_id,self.api_key,vid,title,
                    video_download_url,
                    self.rtoken,self.wtoken,self.pub_id,"http://callback",self.b_id)
            api_request.previous_thumbnail = p_thumb 
            api_request.autosync = False
            api_request.set_api_method("topn",5)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT
            self.assertTrue(api_request.save())
            api_requests.append(api_request)
            i += 1

        return api_requests

    def _process_neon_api_requests(self,api_requests):
        random.seed(194)
        #Create thumbnail metadata
        N_THUMBS = 5
        for api_request in api_requests:
            video_id = api_request.video_id
            job_id = api_request.job_id
            thumbnails = []
            for t in range(N_THUMBS):
                image = self._create_random_image() 
                filestream = StringIO()
                image.save(filestream, "JPEG", quality=100) 
                filestream.seek(0)
                imgdata = filestream.read()
                tid = neondata.ThumbnailID.generate(imgdata,
                                neondata.InternalVideoID.generate(self.api_key,
                                video_id))
                urls = [] ; url = self.mock_image_url_prefix + "/thumb-%s"%t
                urls.append(url)
                self.thumbnail_url_to_image[url] = imgdata
                created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if t == N_THUMBS -1:
                    tdata = neondata.ThumbnailMetaData(tid,urls,created,480,360,
                        "brightcove",2,"test",enabled=True,rank=0)
                else:
                    tdata = neondata.ThumbnailMetaData(tid,urls,created,480,360,
                        "neon",2,"test",enabled=True,rank=t+1)
                thumbnails.append(tdata)
        
            i_vid = neondata.InternalVideoID.generate(self.api_key,video_id)
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
            self.assertTrue(retid)
            self.assertTrue(returl)

            #Update request state to FINISHED
            api_request.state = neondata.RequestState.FINISHED 
            api_request.save()
            tids = []
            for thumb in thumbnails:
                tids.append(thumb.thumbnail_id)
        
            vmdata = neondata.VideoMetadata(i_vid,tids,job_id,url,10,5,"test",self.b_id)
            self.assertTrue(vmdata.save())


    def _get_video_status_brightcove(self):
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/%s/videos' %(self.a_id,self.b_id))
        headers = {'X-Neon-API-Key' : self.api_key} 
        client = AsyncHTTPClient(self.io_loop)
        client.fetch(url, self.stop, headers=headers)
        resp = self.wait()
        items = json.loads(resp.body)
        return items

    def _check_video_status_brightcove(self,vstatus):
        def check_video_status(status,expected_status):
            self.assertEqual(status,expected_status)

        items = self._get_video_status_brightcove()
        for item in items:
            vr = services.VideoResponse(None,None,None,None,None,None,None,None,None)
            vr.__dict__ = item
            status =  vr.status
            check_video_status(status,vstatus)

    def _check_neon_default_chosen(self,videos):
        for vid in videos:
            i_vid = neondata.InternalVideoID.generate(self.api_key,vid) 
            vmdata= neondata.VideoMetadata.get(i_vid)
            thumbnails = neondata.ThumbnailIDMapper.get_thumb_mappings(vmdata.thumbnail_ids)
            for thumbnail in thumbnails:
                thumb = thumbnail.get_metadata()
                if thumb["chosen"] == True and thumb["type"] == 'neon':
                    self.assertEqual(thumb["rank"],1)


    def create_neon_account(self):
        vals = { 'account_id' : self.a_id }
        uri = self.get_url('/api/v1/accounts') 
        response = self.post_request(uri,vals,self.api_key)
        api_key = json.loads(response.body)["neon_api_key"]
        return api_key

    def create_brightcove_account(self):
        url = self.get_url('/api/v1/accounts/' + self.a_id + '/brightcove_integrations')
        vals = { 'integration_id' : self.b_id, 'publisher_id' : 'testpubid123',
                'read_token' : self.rtoken, 'write_token': self.wtoken,'auto_update': False}
        resp = self.post_request(url,vals,self.api_key)
        return resp.body

    def update_brightcove_account(self,rtoken=None,wtoken=None,autoupdate=None):
        if rtoken is None: rtoken = self.rtoken
        if wtoken is None: wtoken = self.wtoken
        if autoupdate == None: autoupdate = False

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/%s' %(self.a_id,self.b_id))
        vals = {'read_token' : rtoken, 'write_token': wtoken,'auto_update': autoupdate}
        return self.put_request(url,vals,self.api_key)

    ################################################################
    # Unit Tests
    ################################################################

    def test_brightcove_web_account_flow(self):
        #Create Neon Account --> Bcove Integration --> update Integration --> 
        #query videos --> autopublish --> verify autopublish
        
        random.seed(1234)
    
        def _create_random_image_response():
            return image_buffer

        def _neon_submit_job_response():
            job_id = random.random()
            self.job_ids.append(job_id)
            request = HTTPRequest('http://google.com')
            response = HTTPResponse(request, 200,
                buffer=StringIO('{"job_id":"%s"}'%job_id))
            return response

        def _add_image_response(req):
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

        def _http_side_effect(*args,**kwargs):
            http_request = args[0]
            if "http://api.brightcove.com/services/library" in http_request.url:
                return bcove_response
           
            #add_image api call 
            elif "http://api.brightcove.com/services/post" in http_request.url:
                return _add_image_response(http_request) 

            #Download image from brightcove CDN
            elif "http://brightcove.vo.llnwd.net" in http_request.url:
                return _create_random_image_response()
            
            #Download image from mock unit test url
            elif self.mock_image_url_prefix in http_request.url:
                request = HTTPRequest(http_request.url)
                response = HTTPResponse(request, 200,
                    buffer=StringIO(self.thumbnail_url_to_image[http_request.url]))
                #on async fetch, callback is returned
                callback = args[1] #,kwargs
                return callback(response)

            #neon request
            elif "api/v1/submitvideo/brightcove" in http_request.url:
                return _neon_submit_job_response() 
            else:
                raise

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
        
        #create neon account
        api_key = self.create_neon_account()
        self.assertEqual(api_key,neondata.NeonApiKey.generate(self.a_id))

        #Setup Side effect for the http clients
        self.bapi_mock_client().fetch.side_effect = _http_side_effect 
        self.cp_mock_client().fetch.side_effect = _http_side_effect  
        self.bapi_mock_async_client().fetch.side_effect = _http_side_effect
        self.cp_mock_async_client().fetch.side_effect = _http_side_effect

        #create brightcove account
        json_video_response = self.create_brightcove_account()
        video_response = json.loads(json_video_response)
        self.assertEqual(len(video_response),5) #TODO: Verify actual contents

        #update brightcove account
        new_rtoken = ("newrtoken")
        update_response = self.update_brightcove_account(new_rtoken)
        self.assertEqual(update_response.code,200)
        
        #auto publish test
        reqs = self._create_neon_api_requests()
        self._process_neon_api_requests(reqs)
        self._check_video_status_brightcove(vstatus=neondata.RequestState.FINISHED)
        
        update_response = self.update_brightcove_account(autoupdate = True)
        self.assertEqual(update_response.code,200)
        
        #Check Neon rank 1 thumbnail is the new thumbnail for the videos
        vitems = json.loads(bcove_responses.find_all_videos_response)
        videos = []
        for item in vitems['items']:
            videos.append(str(item['id']))                              
        self._check_neon_default_chosen(videos)
    
        #update a thumbnail
        new_tids = [] 
        for vid in videos:
            i_vid = neondata.InternalVideoID.generate(self.api_key,vid)
            vmdata= neondata.VideoMetadata.get(i_vid)
            tids = vmdata.thumbnail_ids
            new_tids.append(tids[1])
            resp = self.update_brightcove_thumbnail(vid,tids[1]) #set neon rank 2 
            self.assertEqual(resp.code,200)

        thumbs = []
        items = self._get_video_status_brightcove()
        for item,tid in zip(items,new_tids):
            vr = services.VideoResponse(None,None,None,None,None,None,None,None,None)
            vr.__dict__ = item
            thumbs.append(vr.current_thumbnail)
        self.assertItemsEqual(thumbs,new_tids)

    def update_brightcove_thumbnail(self,vid,tid):
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/%s/videos/%s' %(self.a_id,self.b_id,vid))
        vals = {'thumbnail_id' : tid }
        return self.put_request(url,vals,self.api_key)
    
    def test_check_brightcove_thumbnail(self):
        pass

    #Failure test cases
    #Database failure on account creation, updation
    #Brightcove API failures

if __name__ == '__main__':
    unittest.main()
