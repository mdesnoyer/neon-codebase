#!/usr/bin/env python
'''
Unit Test for Support services api calls

Note: get_new_ioloop() is overridden so that the test code and
tornado server share the same io_loop
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from api import brightcove_api
from cmsapi import services
from cmsdb import neondata
import datetime
import json
from mock import MagicMock, patch 
import random
import re
from PIL import Image
from StringIO import StringIO
import test_utils.mock_boto_s3 as boto_mock
import test_utils.neontest
import test_utils.postgresql
import time
import tornado.gen
import tornado.ioloop
import tornado.testing
import tornado.httpclient
from tornado.httpclient import HTTPError
import unittest
import urllib
from cvutils.imageutils import PILImageUtils
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
    im = PILImageUtils.create_random_image(360, 480)
    imgstream = StringIO()
    im.save(imgstream, "jpeg", quality=100)
    imgstream.seek(0)

    response = tornado.httpclient.HTTPResponse(request, 200,
                            buffer=imgstream)
    return response

###############################################
# Test Services
###############################################

def process_neon_api_requests(api_requests, api_key, i_id, t_type,
                              plattype=neondata.BrightcoveIntegration):
    #Create thumbnail metadata
    images = {}
    thumbnail_url_to_image = {}
    N_THUMBS = 5
    video_map = {}
    for api_request in api_requests:
        video_id = api_request.video_id
        internal_video_id = neondata.InternalVideoID.generate(api_key,
                                                              video_id)
        job_id = api_request.job_id
        video_map[video_id] = job_id
        thumbnails = []
        for t in range(N_THUMBS):
            image =  PILImageUtils.create_random_image(360,
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

class TestServices(test_utils.neontest.AsyncHTTPTestCase):
    def setUp(self):
        super(TestServices, self).setUp()
        options._set('cmsdb.neondata.wants_postgres', 1)
        #Http Connection pool Mock
        self.cp_async_patcher = \
          patch('utils.http.tornado.httpclient.AsyncHTTPClient')
        self.cp_mock_async_client = self._future_wrap_mock(
            self.cp_async_patcher.start()().fetch)

        self.cmsapiv2_patcher = \
          patch('cmsapi.services.cmsapiv2.client.Client')
        self.mock_cmsapiv2 = self._future_wrap_mock(
            self.cmsapiv2_patcher.start()().send_request)
        self.mock_cmsapiv2.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, 200, buffer=StringIO(json.dumps({
                  "job_id" : "job1",
                  'video' : {
                      'state' : neondata.RequestState.PROCESSING,
                      'title' : 'my_title'
                      }
                  })))

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

        random.seed(19449)
        
    def tearDown(self):
        self.cp_async_patcher.stop()
        self.cmsapiv2_patcher.stop()
        self.postgresql.clear_all_tables() 
        options._set('cmsdb.neondata.wants_postgres', 0)
        super(TestServices, self).tearDown()

    @classmethod
    def setUpClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 1)
        dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
        cls.postgresql = test_utils.postgresql.Postgresql(dump_file=dump_file)
        super(TestServices, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        options._set('cmsdb.neondata.wants_postgres', 0)
        cls.postgresql.stop()
        super(TestServices, cls).tearDownClass()

    
    def write_side_effect(self, priority, message, timeout):
        return message

    def get_app(self):
        ''' return services app '''
        return services.application

    # TODO: It should be possible to run this with an IOLoop for each
    # test, but it's not running. Need to figure out why.
    #def get_new_ioloop(self):
    #    return tornado.ioloop.IOLoop.instance()

    @tornado.gen.coroutine
    def post_request(self, url, vals, apikey, jsonheader=False):
        ''' post request to the app '''        
        if jsonheader: 
            headers = {'X-Neon-API-Key' : apikey, 
                    'Content-Type':'application/json'}
            body = json.dumps(vals)
        else:        
            headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded'}
            body = urllib.urlencode(
                dict([(k, v if not isinstance(v, basestring) else 
                   v.encode('utf-8')) for k, v in vals.iteritems()]))

        response = yield self.http_client.fetch(url,
                                                method="POST",
                                                body=body,
                                                headers=headers)
        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def put_request(self, url, vals, apikey, jsonheader=False):
        ''' put request to the app '''

        headers = {'X-Neon-API-Key' : apikey, 
                'Content-Type':'application/x-www-form-urlencoded' }
        body = urllib.urlencode(vals)
        
        if jsonheader: 
            headers = {'X-Neon-API-Key' : apikey, 
                    'Content-Type':'application/json'}
            body = json.dumps(vals)
        
        response = yield self.http_client.fetch(url, method="PUT", body=body,
                                                headers=headers)
        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def get_request(self, url, apikey):
        ''' get request to the app '''

        headers = {'X-Neon-API-Key' :apikey} 
        resp = yield self.http_client.fetch(url, headers=headers)
        raise tornado.gen.Return(resp)

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
        self.job_ids = [x.job_id for x in api_requests]

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
            job_id = str(random.random())
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

    @tornado.gen.coroutine
    def _get_video_status_brightcove(self):
        ''' get video status for all videos in brightcove '''

        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                            "/%s/videos" %(self.a_id, self.b_id))
        headers = {'X-Neon-API-Key' : self.api_key} 
        resp = yield self.http_client.fetch(url, headers=headers)
        items = json.loads(resp.body)
        raise tornado.gen.Return(items)

    @tornado.gen.coroutine
    def _check_video_status_brightcove(self, vstatus):
        ''' assert video status for brightcove videos'''
        
        items = yield self._get_video_status_brightcove()
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

    @tornado.gen.coroutine
    def create_neon_account(self):
        ''' create neon user account '''

        vals = { 'account_id' : self.a_id }
        uri = self.get_url('/api/v1/accounts') 
        response = yield self.post_request(uri, vals, "")
        api_key = json.loads(response.body)["neon_api_key"]
        tai = json.loads(response.body)["tracker_account_id"]
        raise tornado.gen.Return(api_key)

    @tornado.gen.coroutine
    def create_brightcove_account(self, expected_code=200):
        ''' create brightcove platform account '''

        #create a neon account first
        self.api_key = yield self.create_neon_account()
        self.assertEqual(self.api_key, 
                         neondata.NeonApiKey.get_api_key(self.a_id))

        url = self.get_url('/api/v1/accounts/' + self.a_id + \
                            '/brightcove_integrations')
        vals = {'integration_id' : self.b_id, 'publisher_id' : 'testpubid123',
                'read_token' : self.rtoken, 'write_token': self.wtoken, 
                'auto_update': False}
        resp = yield self.post_request(url, vals, self.api_key)
        self.assertEquals(resp.code, expected_code)
        json_body = json.loads(resp.body)

        try: 
            self.b_id = json_body['integration_id'] 
        except KeyError: 
            pass 

        raise tornado.gen.Return(resp.body)

    @tornado.gen.coroutine
    def update_brightcove_account(self, rtoken=None, wtoken=None, autoupdate=None):
        ''' update brightcove account '''

        if rtoken is None: rtoken = self.rtoken
        if wtoken is None: wtoken = self.wtoken
        if autoupdate is None: autoupdate = False

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/%s' \
                            %(self.a_id, self.b_id))
        vals = {'read_token' : rtoken, 'write_token': wtoken, 
                'auto_update': autoupdate}
        resp = yield self.put_request(url, vals, self.api_key)
        raise tornado.gen.Return(resp)

  
    ## HTTP Side effect for all Tornado HTTP Requests

    def _success_http_side_effect(self, *args, **kwargs):
        ''' generic success http side effects for all patched http calls 
            for this test ''' 

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

        if "/services/library?command=search_videos" in http_request.url:
            request = tornado.httpclient.HTTPRequest(http_request.url)
            response = tornado.httpclient.HTTPResponse(request, 200,
                    buffer=StringIO(bcove_responses.search_videos_response))
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
            return response            
            

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

        

    @tornado.gen.coroutine
    def _setup_initial_brightcove_state(self):
        '''
        Setup the state of a brightcove account with 5 processed videos 
        '''
        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
    
        #set up account and video state for testing
        self.api_key = yield self.create_neon_account()
        json_video_response = yield self.create_brightcove_account()
        #verify account id added to Neon user account
        nuser = neondata.NeonUserAccount.get(self.api_key)
        self.assertIn(self.b_id, nuser.integrations.keys())
         
        reqs = self._create_neon_api_requests()
        self._process_brightcove_neon_api_requests(reqs)


    ################################################################
    # Unit Tests
    ################################################################

    @tornado.testing.gen_test
    def test_bad_brightcove_tokens(self):
        self.cp_mock_async_client.side_effect = [
            brightcove_api.BrightcoveApiError("Oops")
            ]
        
        #set up account and video state for testing
        self.api_key = yield self.create_neon_account()
        json_video_response = yield self.create_brightcove_account(502)
        vr = json.loads(json_video_response)
        self.assertEqual(vr['error'], 
            "Read token given is incorrect or brightcove api failed")


    @tornado.testing.gen_test
    def test_invalid_get_rest_uri(self):
        ''' test uri parsing, invalid requests '''
        api_key = yield self.create_neon_account()
        
        
        url = self.get_url('/api/v1/accounts/')
        with self.assertRaises(HTTPError) as e:
            resp = yield self.get_request(url, api_key)
        self.assertEqual(e.exception.code, 400)
        
        url = self.get_url('/api/v1/accounts/123/invalid_aid')
        with self.assertRaises(HTTPError) as e:
            resp = yield self.get_request(url, api_key)
        self.assertEqual(e.exception.code, 400)
        
        url = self.get_url('/api/v1/accounts/%s/dummy_integration' %self.a_id)
        with self.assertRaises(HTTPError) as e:
            resp = yield self.get_request(url, api_key)
        self.assertEqual(e.exception.code, 400)
        
        url = self.get_url('/api/v1/accounts/invalid_api_key/'\
                            'neon_integrations/0/videos')
        with self.assertRaises(HTTPError) as e:
            resp = yield self.get_request(url, api_key)
        self.assertEqual(e.exception.code, 400)
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations'\
                            '/0/bad_method' %self.a_id)
        with self.assertRaises(HTTPError) as e:
            resp = yield self.get_request(url, api_key)
        self.assertEqual(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_invalid_put_rest_uri(self):
        ''' put requests'''
        
        api_key = yield self.create_neon_account()

        url = self.get_url('/api/v1/accounts/%s/neon_integrations'\
                            '/0/videos' %self.a_id)
        with self.assertRaises(HTTPError) as e:
            resp = yield self.put_request(url, {}, api_key)
        self.assertEqual(e.exception.code, 400)
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations'\
                            '/0/invalid_method' %self.a_id)
        with self.assertRaises(HTTPError) as e:
                resp = yield self.put_request(url, {}, api_key)
        self.assertEqual(e.exception.code, 400)

    @tornado.testing.gen_test
    def test_get_account_info(self):
        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        
        yield self.create_brightcove_account()
        url = self.get_url('/api/v1/accounts/%s/neon_integrations'\
                            '/0' % self.a_id)
        resp = yield self.get_request(url, self.api_key)
        self.assertEqual(resp.code, 200)
        data = json.loads(resp.body)
        self.assertEqual(data['neon_api_key'], self.api_key)
        self.assertEqual(data['integration_id'], '0')

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations'\
                            '/%s' % (self.a_id, self.b_id))
        resp = yield self.get_request(url, self.api_key)
        self.assertEqual(resp.code, 200)
        data = json.loads(resp.body)
        self.assertEqual(data['integration_id'], self.b_id)

    @tornado.testing.gen_test
    def test_create_update_brightcove_account(self):
        ''' updation of brightcove account '''

        #create neon account
        self.api_key = yield self.create_neon_account()
        self.assertEqual(self.api_key, 
                neondata.NeonApiKey.get_api_key(self.a_id))
        
        #Verify that there is an experiment strategy for the account
        strategy = neondata.ExperimentStrategy.get(self.api_key)
        self.assertIsNotNone(strategy)
        self.assertFalse(strategy.only_exp_if_chosen)

        #Setup Side effect for the http clients
        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect

        #create brightcove account
        yield self.create_brightcove_account()

        # Verify actual contents
        platform = neondata.BrightcoveIntegration.get(
            self.b_id)
        self.assertEqual(platform.integration_id, self.b_id)
        self.assertEqual(platform.account_id, self.a_id)
        self.assertEqual(platform.publisher_id, 'testpubid123')
        self.assertEqual(platform.read_token, self.rtoken)
        self.assertEqual(platform.write_token, self.wtoken)
        self.assertFalse(platform.auto_update)
        

        #update brightcove account
        new_rtoken = ("newrtoken")
        update_response = yield self.update_brightcove_account(new_rtoken)
        self.assertEqual(update_response.code, 200)
        platform = neondata.BrightcoveIntegration.get(self.b_id)
        self.assertEqual(platform.read_token, "newrtoken")
        self.assertFalse(platform.auto_update)
        self.assertEqual(platform.write_token, self.wtoken)

    ######### BCOVE HANDLER Test cases ##########################
        
    @tornado.testing.gen_test
    def test_pagination_videos_brighcove(self):
        ''' test pagination of brightcove integration '''

        yield self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        
        #get videos in pages
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
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
        resp = yield self.get_request(url,self.api_key)
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
        resp = yield self.get_request(url, self.api_key)
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
        self.assertEqual(response['serving_count'], 0)

        #request last page with page_size > #of videos available in the page
        page_no = 1 
        page_size = 3 
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertEqual(len(ordered_videos) - (page_no*page_size),
                        len(result_vids))

    @tornado.testing.gen_test
    def test_request_by_video_ids_brightcove(self):
        ''' test video ids of brightcove integration '''

        yield self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        test_video_ids = ordered_videos[:2]
        video_ids = ",".join(test_video_ids)

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?video_ids=%s'
                %(self.a_id, self.b_id, video_ids))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertItemsEqual(result_vids, test_video_ids)

    @tornado.testing.gen_test
    def test_request_invalid_video(self):
        ''' invalid video id '''

        yield self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        test_video_ids = ordered_videos[:2]

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos?video_ids=invalidvideoID'
                %(self.a_id, self.b_id))
        with self.assertRaises(HTTPError) as e:
            resp = yield self.get_request(url, self.api_key)
        self.assertEquals(e.exception.code, 400)
        items = json.loads(e.exception.response.body)['items']
        self.assertItemsEqual(items[0], {})

    @tornado.testing.gen_test
    def test_invalid_video_ids_request(self):
        yield self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        test_video_ids = ordered_videos[:2]
        video_ids = ",".join(test_video_ids)
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos/?video_ids=15238901589,%s'
                %(self.a_id, self.b_id, video_ids))
        jresp = yield self.get_request(url, self.api_key)
        resp = json.loads(jresp.body)
        self.assertEqual(resp["total_count"], 2)
        self.assertEqual(len(resp["items"]), 2)

        result_vids = [x['video_id'] for x in resp["items"]]
        self.assertItemsEqual(result_vids, test_video_ids)

    @tornado.testing.gen_test
    def test_invalid_model_scores(self):
        ''' test filtering of invalid model scores like -inf, nan '''

        yield self._setup_initial_brightcove_state()
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
        resp = yield self.get_request(url,self.api_key)
        response = json.loads(resp.body)
       
        model_scores = []
        for r in response['items']:
            for t in r['thumbnails']:
                model_scores.append(t['model_score'])

        self.assertFalse(float('-inf') in model_scores)    
        self.assertFalse(float('nan') in model_scores)    
        self.assertFalse(None in model_scores)    

    @tornado.testing.gen_test
    def test_get_brightcove_video_requests_by_state(self):
        '''
        Test you can query brightcove videos by their video state
        including requesting them in pages
        '''
        yield self._setup_initial_brightcove_state()

        ordered_videos = sorted(self._get_videos(), reverse=True)
        
        #recommended videos
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos/recommended?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = yield self.get_request(url,self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertEqual(ordered_videos[:page_size],
                         result_vids)

        #fail a couple of videos
        def _fail_video(x):
            x.state = neondata.RequestState.FAILED
        vids = []
        for item in items[:page_size]:
            neondata.NeonApiRequest.modify(item['job_id'], self.api_key,
                                           _fail_video)
            vids.append(item['video_id'])

        url = self.get_url('/api/v1/accounts/%s/brightcove_integrations/'
                '%s/videos/failed?page_no=%s&page_size=%s'
                %(self.a_id, self.b_id, page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        result_vids = [x['video_id'] for x in items]
        self.assertItemsEqual(vids, result_vids)

    @tornado.testing.gen_test
    def test_tracker_account_id_mapper(self):
        '''
        Test mapping between tracker account id => neon account id
        '''
        #account creation
        vals = { 'account_id' : self.a_id }
        uri = self.get_url('/api/v1/accounts') 
        response = yield self.post_request(uri, vals, '')
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
        response = yield self.get_request(url, api_key)
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

    @tornado.testing.gen_test
    def test_create_neon_integration(self):
        api_key = yield self.create_neon_account()
        nuser = neondata.NeonUserAccount.get(api_key)
        neon_integration_id = "0"
        self.assertIn(neon_integration_id, nuser.integrations.keys()) 

    @tornado.testing.gen_test
    def test_create_neon_video_request(self):
        ''' verify that video request creation via services  '''        
        api_key = yield self.create_neon_account()
        vals = { 'video_url' : "http://test.mp4", "title": "test_title" }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_video_request'%(self.a_id, "0"))

        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect

        response = yield self.post_request(uri, vals, api_key)

        self.assertEqual(response.code, 201)
        response = json.loads(response.body)
        self.assertIsNotNone(response["video_id"])  
        self.assertEqual(response["status"], neondata.RequestState.PROCESSING)

    @tornado.testing.gen_test
    def test_create_neon_video_request_via_api(self):
        ''' verify that video request creation via services  ''' 
        api_key = yield self.create_neon_account()

        vals = { 'video_url' : "http://test.mp4", 
                 "video_title": "test_title", 
                 'video_id'  : "vid1", 
                 "callback_url" : "http://callback"
                }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "0"))

        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        
        vid = "vid1"
        response = yield self.post_request(uri, vals, api_key)
        self.assertEqual(response.code, 201)
        jresponse = json.loads(response.body)
        job_id = jresponse['job_id']
        self.assertIsNotNone(job_id)

        # Make sure that the video was submitted to cmsapiv2
        self.assertEquals(self.mock_cmsapiv2.call_count, 1)
        cargs, kwargs = self.mock_cmsapiv2.call_args
        self.assertEquals(cargs[0].url, '/api/v2/%s/videos' % api_key)
        self.assertEquals(cargs[0].method, 'POST')
        self.maxDiff = None
        self.assertEquals(json.loads(cargs[0].body),
                          { 'external_video_ref': vid,
                            'title' : 'test_title',
                            'url' : 'http://test.mp4',
                            'default_thumbnail_url' : None,
                            'thumbnail_ref' : None,
                            'callback_url' : 'http://callback',
                            'integration_id' : '0',
                            'publish_date' : None,
                            'custom_data' : {},
                            'duration' : None
                            })
        
        

        # Test duplicate request
        self.mock_cmsapiv2.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(
              x, 409, buffer=StringIO('{"job_id" : "job1"}'))

        with self.assertRaises(tornado.httpclient.HTTPError) as e:
            response = yield self.post_request(uri, vals, api_key)
        self.assertEqual(e.exception.code, 409)
        self.assertEqual(json.loads(e.exception.response.body)["job_id"],
                         job_id)

    @tornado.testing.gen_test
    def test_create_neon_video_request_videoid_size(self):
        ''' verify video id length check ''' 
        
        api_key = yield self.create_neon_account()
        vals = { 'video_url' : "http://test.mp4", "video_title": "test_title", 
                 'video_id'  : "vid1"*100, "callback_url" : "http://callback"
                }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "0"))
        with self.assertRaises(HTTPError) as e:
            response = yield self.post_request(uri, vals, api_key)
        self.assertEqual(e.exception.code, 400)
        self.assertEqual(e.exception.response.body, 
            '{"error":"video id greater than 128 chars"}')

    @tornado.testing.gen_test
    def test_create_video_request_with_custom_data(self):
        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        api_key = yield self.create_neon_account()
        vals = {
            'video_url' : "http://test.mp4",
            "video_title": "test_title", 
            'video_id'  : 654321,
            "callback_url" : "null",
            'custom_data' : { 'my_id' : 123456, 'my_string': 'string'},
            'duration' : 123456.5,
            'publish_date' : '2015-06-03T13:04:33Z'
            }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request' % (self.a_id, "61"))
        response = yield self.post_request(uri, vals, api_key, jsonheader=True)
        
        self.assertEqual(response.code, 201)

        # Make sure that the video was submitted to cmsapiv2
        self.assertEquals(self.mock_cmsapiv2.call_count, 1)
        cargs, kwargs = self.mock_cmsapiv2.call_args
        self.assertEquals(cargs[0].url, '/api/v2/%s/videos' % api_key)
        self.assertEquals(cargs[0].method, 'POST')
        self.maxDiff = None
        self.assertEquals(json.loads(cargs[0].body),
                          { 'external_video_ref': '654321',
                            'title' : 'test_title',
                            'url' : 'http://test.mp4',
                            'default_thumbnail_url' : None,
                            'thumbnail_ref' : None,
                            'callback_url' : None,
                            'integration_id' : '61',
                            'publish_date' : '2015-06-03T13:04:33+00:00',
                            'custom_data' : vals['custom_data'],
                            'duration' : 123456.5
                            })

    @tornado.testing.gen_test
    def test_create_video_with_default_thumb(self):
        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        api_key = yield self.create_neon_account()
        vals = {
            'video_url' : "http://test.mp4",
            "video_title": "test_title", 
            'video_id'  : "vid1",
            'default_thumbnail' : 'default_thumb.jpg',
            'external_thumbnail_id' : 'ext_tid',
            'callback_url' : 'http://callback'
            }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request' % (self.a_id, "61"))
        response = yield self.post_request(uri, vals, api_key, jsonheader=True)
        
        self.assertEqual(response.code, 201)

        # Make sure that the video was submitted to cmsapiv2
        self.assertEquals(self.mock_cmsapiv2.call_count, 1)
        cargs, kwargs = self.mock_cmsapiv2.call_args
        self.assertEquals(cargs[0].url, '/api/v2/%s/videos' % api_key)
        self.assertEquals(cargs[0].method, 'POST')
        self.assertEquals(json.loads(cargs[0].body),
                          { 'external_video_ref': 'vid1',
                            'title' : 'test_title',
                            'url' : 'http://test.mp4',
                            'default_thumbnail_url' : 'default_thumb.jpg',
                            'thumbnail_ref' : 'ext_tid',
                            'callback_url' : 'http://callback',
                            'integration_id' : '61',
                            'publish_date' : None,
                            'duration' : None,
                            'custom_data' : {}
                            })

    @tornado.testing.gen_test
    def test_create_video_request_custom_data_via_url_string(self):
        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        api_key = yield self.create_neon_account()
        vals = {
            'video_url' : "http://test.mp4",
            "video_title": "test_title", 
            'video_id'  : "vid1",
            "callback_url" : "http://callback",
            'custom_data' : json.dumps(
                { 'my_id' : 123456, 'my_string': 'string'}),
            'duration' : 123456.5
            }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "61"))
        response = yield self.post_request(uri, vals, api_key)
        
        self.assertEqual(response.code, 201)

        # Make sure that the video was submitted to cmsapiv2
        self.assertEquals(self.mock_cmsapiv2.call_count, 1)
        cargs, kwargs = self.mock_cmsapiv2.call_args
        self.assertEquals(cargs[0].url, '/api/v2/%s/videos' % api_key)
        self.assertEquals(cargs[0].method, 'POST')
        self.assertEquals(json.loads(cargs[0].body),
                          { 'external_video_ref': 'vid1',
                            'title' : 'test_title',
                            'url' : 'http://test.mp4',
                            'default_thumbnail_url' : None,
                            'thumbnail_ref' : None,
                            'callback_url' : 'http://callback',
                            'integration_id' : '61',
                            'publish_date' : None,
                            'custom_data' : {
                                'my_id' : 123456,
                                'my_string' : 'string'
                                },
                            'duration' : 123456.5
                            })

    @tornado.testing.gen_test
    def test_create_video_request_bad_custom_data(self):
        api_key = yield self.create_neon_account()
        vals = {
            'video_url' : "http://test.mp4",
            "video_title": "test_title", 
            'video_id'  : "vid1",
            "callback_url" : "http://callback",
            'custom_data' : "mega man",
            'duration' : 123456.5
            }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "61"))
        with self.assertRaises(HTTPError) as e:
            response = yield self.post_request(uri, vals, api_key,
                                               jsonheader=True)
        
        self.assertEqual(e.exception.code, 400)

        self.assertEqual(e.exception.response.body, 
            '{"error":"custom data must be a dictionary"}')

    @tornado.testing.gen_test
    def test_create_video_request_utf8(self):
        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        api_key = yield self.create_neon_account()
        vals = {
            'video_url' : "http://%stest.mp4" % unichr(40960),
            "video_title": unichr(40960) + u'abcd' + unichr(1972), 
            'video_id'  : "vid1"
            }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "61"))

        response = yield self.post_request(uri, vals, api_key, jsonheader=True)
        
        self.assertEqual(response.code, 201)

        self.assertEquals(self.mock_cmsapiv2.call_count, 1)
        cargs, kwargs = self.mock_cmsapiv2.call_args
        self.assertEquals(cargs[0].url, '/api/v2/%s/videos' % api_key)
        self.assertEquals(cargs[0].method, 'POST')
        self.maxDiff = None
        self.assertEquals(json.loads(cargs[0].body),
                          { 'external_video_ref': 'vid1',
                            'title' : vals['video_title'],
                            'url' : vals['video_url'],
                            'default_thumbnail_url' : None,
                            'thumbnail_ref' : None,
                            'callback_url' : None,
                            'integration_id' : '61',
                            'publish_date' : None,
                            'custom_data' : {},
                            'duration' : None
                            })

    @tornado.testing.gen_test
    def test_video_request_in_submit_state(self):
        '''
        Create video request and then query it via Neon API
        '''

        api_key = yield self.create_neon_account()
        vals = { 'video_url' : "http://test.mp4",
                 "video_title": "test_title", 
                 'video_id'  : "vid1",
                 "callback_url" : "http://callback"
                }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_thumbnail_api_request'%(self.a_id, "0"))

        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        
        vid = "vid1"
        response = yield self.post_request(uri, vals, api_key)
        self.assertEqual(response.code, 201)
        jresponse = response.body
        job_id = json.loads(jresponse)['job_id']
        self.assertIsNotNone(job_id)

        # Make sure that the video was submitted to cmsapiv2
        self.assertEquals(self.mock_cmsapiv2.call_count, 1)
        cargs, kwargs = self.mock_cmsapiv2.call_args
        self.assertEquals(cargs[0].url, '/api/v2/%s/videos' % api_key)
        self.assertEquals(cargs[0].method, 'POST')
        self.assertEquals(json.loads(cargs[0].body),
                          { 'external_video_ref': vid,
                            'title' : 'test_title',
                            'url' : 'http://test.mp4',
                            'default_thumbnail_url' : None,
                            'thumbnail_ref' : None,
                            'callback_url' : 'http://callback',
                            'integration_id' : '0',
                            'publish_date' : None,
                            'duration' : None,
                            'custom_data' : {}
                            })

    @tornado.testing.gen_test
    def test_create_neon_video_request_invalid_url(self):
        ''' invalid url test '''
        api_key = yield self.create_neon_account()
        vals = { 'video_url' : "http://not_a_video_link", "title": "test_title" }
        uri = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/create_video_request'%(self.a_id, "0"))

        self.cp_mock_async_client.side_effect = \
          self._success_http_side_effect
        with self.assertRaises(HTTPError) as e:
            response = yield self.post_request(uri, vals, api_key)
        self.assertEqual(e.exception.code, 400)
        response = json.loads(e.exception.response.body)
        self.assertEqual(response['error'], 
                'link given is invalid or not a video file')

    @tornado.testing.gen_test
    def test_empty_get_video_status_neonplatform(self):
        ''' empty videos '''
        api_key = yield self.create_neon_account()
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = yield self.get_request(url, api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(items,[])

    @tornado.testing.gen_test
    def test_get_video_status_neonplatform(self):
        '''
        Test retreiving video responses for neonplatform
        '''

        self.api_key = yield self.create_neon_account()
        nvids = 10 
        api_requests = [] 
        vids_added = []
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
            vids_added.append((vid, job_id))

        def _add_videos(x):
            for vid, job_id in vids_added:
                x.add_video(vid, job_id)
        nplatform = neondata.NeonPlatform.modify(
            self.api_key, '0',
            _add_videos)
        random.seed(1123)
        self._process_brightcove_neon_api_requests(api_requests)
        
        page_no = 0
        page_size = 2
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size)
        result_vids = [x['video_id'] for x in items]

        # recommended
        page_size = 5
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/recommended?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), page_size)
        result_vids = [ x['video_id'] for x in items]

        # processing
        page_size = 5
        api_requests[0].state = neondata.RequestState.SUBMIT
        api_requests[0].save()
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/processing?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), 1) #1 video in processing
       
        # failed state
        api_requests[0].state = neondata.RequestState.FAILED
        api_requests[0].save()
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/failed?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), 1) #1 video failed 
        self.assertEqual(items[0]['status'], 'failed')
        self.assertEqual(items[0]['job_id'], api_requests[0].job_id)
       
        # invalid state
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/invalid?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        with self.assertRaises(HTTPError) as e:
            resp = yield self.get_request(url, self.api_key)
        self.assertEqual(e.exception.code, 400)

        # serving state
        api_requests[-1].state = neondata.RequestState.SERVING
        api_requests[-1].save()
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos/serving?page_no=%s&page_size=%s'
                %(self.a_id, "0", page_no, page_size))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        self.assertEqual(len(items), 1) #1 video serving
        self.assertEqual(items[0]['status'], 'serving')
        self.assertEqual(items[0]['job_id'], api_requests[-1].job_id)

        # TODO (Sunil) More test cases on states
        # get videos with serving state
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos'  %(self.a_id, "0"))
        resp = yield self.get_request(url, self.api_key)
        items = json.loads(resp.body)['items']
        status = [item['status'] for item in items]
        self.assertEqual(status.count("serving"), 1)

    @tornado.gen.coroutine
    def _setup_neon_account_and_request_object(self, vid="testvideo1",
                                            job_id = "j1"):
        self.api_key = yield self.create_neon_account()
        title = "title"
        video_download_url = "http://video.mp4" 
        api_request = neondata.NeonApiRequest(job_id, self.api_key, vid,
                    title, video_download_url, "neon", "http://callback")
        api_request.set_api_method("topn", 5)
        api_request.publish_time = str(time.time() *1000)
        api_request.submit_time = str(time.time())
        api_request.state = neondata.RequestState.SUBMIT
        self.assertTrue(api_request.save())
        neondata.NeonPlatform.modify(
            self.api_key, '0',
            lambda x: x.add_video(vid, job_id),
            create_missing=True)
        self._process_brightcove_neon_api_requests([api_request])
        
        # set the state to serving
        api_request = neondata.NeonApiRequest.get(job_id, self.api_key)
        api_request.state = neondata.RequestState.SERVING
        api_request.save()

    @tornado.testing.gen_test
    def test_video_response_object(self):
        '''
        Test expected fields of a video response object
        '''
        vid = "testvideo1"
        job_id = "j1"
        title = "title"
        yield self._setup_neon_account_and_request_object(vid, job_id)
        
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
        # NOTE: In the lifecycle of a video, this will be called by mastermind 
        v.get_serving_url()
        v.save()

        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?video_id=%s'
                %(self.a_id, "0", vid))
        resp = yield self.get_request(url, self.api_key)
        vresponse = json.loads(resp.body)["items"][0]

        pub_id = neondata.NeonUserAccount.get(self.api_key).tracker_account_id
        serving_url = 'neon-images.com/v1/client/%s/neonvid_%s.jpg' \
                        % (pub_id, vid)

        self.assertEqual(vresponse["video_id"], vid)
        self.assertEqual(vresponse["title"], title)
        self.assertEqual(vresponse["integration_type"], "neon")
        self.assertEqual(vresponse["status"], "serving")
        self.assertEqual(vresponse["abtest"], True)
        self.assertIn(serving_url, vresponse["serving_url"])
        self.assertEqual(vresponse["winner_thumbnail"], None)

    @unittest.skip('Incomplete test. TODO: fill out when Ooyala is used')
    @tornado.testing.gen_test
    def test_get_abtest_state(self):
        '''
        A/B test state response
        '''

        self.api_key = yield self.create_neon_account()
        
        ext_vid = 'vid1'
        vid = neondata.InternalVideoID.generate(self.api_key, ext_vid) 
        
        #Set experiment strategy
        es = neondata.ExperimentStrategy(self.api_key)
        es.chosen_thumb_overrides = True
        es.save()

    @tornado.testing.gen_test
    def test_winner_thumbnail_in_video_response(self):
        '''
        Test winner thumbnail, after A/B test is complete
        '''

        self.api_key = yield self.create_neon_account()
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
        nplatform = neondata.NeonPlatform.modify(
            self.api_key, '0',
            lambda x: x.add_video(vid, job_id),
            create_missing=True)
        self._process_brightcove_neon_api_requests([api_request])
        
        i_vid = neondata.InternalVideoID.generate(self.api_key, vid) 
        TMD = neondata.ThumbnailMetadata
        #Save thumbnails 
        thumbs = [
            TMD('%s_t1' % i_vid, i_vid, ['t1.jpg']),
            TMD('%s_t2' % i_vid, i_vid, ['t2.jpg']),
            ]
        TMD.save_all(thumbs)
        TS = neondata.ThumbnailStatus
        TS('%s_t1' % i_vid, 0.8, 0.02).save()
        TS('%s_t2' % i_vid, 0.2, 0.01).save()
        
        #Save VideoMetadata
        tids = [thumb.key for thumb in thumbs]
        v = neondata.VideoMetadata(i_vid, tids, job_id, 'v0.mp4')
        v.save()
        neondata.VideoStatus(
            i_vid,
            experiment_state=neondata.ExperimentState.COMPLETE,
            winner_tid=thumbs[0].key).save()
       
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                '%s/videos?video_id=%s'
                %(self.a_id, "0", vid))
        resp = yield self.get_request(url, self.api_key)
        vresponse = json.loads(resp.body)["items"][0]

        self.assertEqual(vresponse["winner_thumbnail"], thumbs[0].key)
        self.assertEqual(vresponse["status"], "finished")
        self.assertEqual(vresponse["thumbnails"][0]["thumbnail_id"],
                         '%s_t1' % i_vid)
        self.assertAlmostEqual(vresponse["thumbnails"][0]["serving_frac"], 0.8)
        self.assertAlmostEqual(vresponse["thumbnails"][0]["ctr"], 0.02)
        self.assertEqual(vresponse["thumbnails"][1]["thumbnail_id"],
                         '%s_t2' % i_vid)
        self.assertAlmostEqual(vresponse["thumbnails"][1]["serving_frac"], 0.2)
        self.assertAlmostEqual(vresponse["thumbnails"][1]["ctr"], 0.01)
        self.assertNotIn('key', vresponse["thumbnails"][0])

    @tornado.testing.gen_test
    def test_get_abtest_state(self):
        '''
        A/B test state response
        '''

        self.api_key = yield self.create_neon_account()
        
        ext_vid = 'vid1'
        vid = neondata.InternalVideoID.generate(self.api_key, ext_vid)
        
        #Save thumbnails 
        TMD = neondata.ThumbnailMetadata
        thumbs = [
            TMD('%s_t1' % vid, vid, ['t1.jpg'], None, None, None,
                              None, None, None, serving_frac=0.8),
            TMD('%s_t2' % vid, vid, ['t2.jpg'], None, None, None,
                              None, None, None, serving_frac=0.15)
            ]
        TMD.save_all(thumbs)
        
        #Save VideoMetadata
        tids = [thumb.key for thumb in thumbs]
        v0 = neondata.VideoMetadata(vid, tids, 'reqid0', 'v0.mp4',
                                    frame_size=(120,90))
        v0.save()
        vid_status = neondata.VideoStatus(
            vid,
            experiment_state=neondata.ExperimentState.RUNNING)
        vid_status.save()
       
        #Set up Serving URLs 
        for thumb in thumbs:
            inp = neondata.ThumbnailServingURLs('%s' % thumb.key)
            inp.add_serving_url('http://%s_800_600.jpg' % thumb.key, 800, 600) 
            inp.add_serving_url('http://%s_120_90.jpg' % thumb.key, 120, 90) 
            inp.save()
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                            '%s/abteststate/%s' %(self.a_id, "0", ext_vid))  
        resp = yield self.get_request(url, self.api_key)
        res = json.loads(resp.body)
        
        # AB test running
        self.assertEqual(resp.code, 200)
        self.assertEqual(res['state'], "running") 
        self.assertEqual(res['data'], []) 
        
        vid_status.experiment_state = neondata.ExperimentState.COMPLETE
        vid_status.winner_tid = '%s_t2' % vid
        vid_status.save()
        
        # AB test complete 
        expected_data = json.loads('{"state": "complete", "data": [{"url":\
        "http://%s_t2_800_600.jpg", "width": 800, "height": 600}, {"url":\
        "http://%s_t2_120_90.jpg", "width": 120, "height": 90}]}' %
            (vid, vid))
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                            '%s/abteststate/%s' %(self.a_id, "0", ext_vid))  
        resp = yield self.get_request(url, self.api_key)
       
        res = json.loads(resp.body)
        self.assertEqual(resp.code, 200)
        self.assertEqual(res['state'], "complete") 
        self.assertEqual(res['data'], expected_data['data'])
        self.assertEqual(res['original_thumbnail'],
                            "http://%s_t2_120_90.jpg" % vid)

    @tornado.testing.gen_test
    def test_get_abtest_state_new_serving_urls(self):
        '''
        A/B test state response
        '''

        self.api_key = yield self.create_neon_account()
        
        ext_vid = 'vid1'
        vid = neondata.InternalVideoID.generate(self.api_key, ext_vid)
        
        #Save thumbnails 
        TMD = neondata.ThumbnailMetadata
        thumbs = [
            TMD('%s_t1' % vid, vid, ['t1.jpg'], None, None, None,
                              None, None, None, serving_frac=0.8),
            TMD('%s_t2' % vid, vid, ['t2.jpg'], None, None, None,
                              None, None, None, serving_frac=0.15)
            ]
        TMD.save_all(thumbs)
        
        #Save VideoMetadata
        tids = [thumb.key for thumb in thumbs]
        v0 = neondata.VideoMetadata(vid, tids, 'reqid0', 'v0.mp4',
                                    frame_size=(54,54))
        v0.save()
        vid_status = neondata.VideoStatus(
            vid,
            experiment_state=neondata.ExperimentState.COMPLETE)
        vid_status.winner_tid = '%s_t2' % vid
        vid_status.save()
       
        #Set up Serving URLs 
        for thumb in thumbs:
            inp = neondata.ThumbnailServingURLs('%s' % thumb.key, base_url='a')
            inp.add_serving_url('a/neontn123_14234ab_t1_w800_h600.jpg', 800, 600) 
            inp.add_serving_url('a/neontn1234_124ab_t2_w120_h90.jpg', 120, 90) 
            inp.save()
        
        url = self.get_url('/api/v1/accounts/%s/neon_integrations/'
                            '%s/abteststate/%s' %(self.a_id, "0", ext_vid))  
        resp = yield self.get_request(url, self.api_key)
        res = json.loads(resp.body)
        self.assertEqual(res['data'][0]['url'], 'a/neontnpx6iaz6um8e8u1sk5pyknk60_vid1_t2_w800_h600.jpg') 
        self.assertEqual(res['data'][0]['width'], 800)
        self.assertEqual(res['data'][0]['height'], 600)

    @patch('cvutils.imageutils.utils.http')
    @patch('cmsdb.cdnhosting.S3Connection')
    @patch('cmsapi.services.neondata.cmsdb.cdnhosting.utils.http')
    @tornado.testing.gen_test
    def test_upload_video_custom_thumbnail(self, mock_cloudinary,
                                           mock_conntype,
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
        mock_img_download = self._future_wrap_mock(
            mock_img_download.send_request)
        mock_cloudinary = self._future_wrap_mock(mock_cloudinary.send_request)
        #s3mocks to mock host_thumbnails_to_s3
        conn = boto_mock.MockConnection()
        conn.create_bucket('host-thumbnails')
        conn.create_bucket('n3.neon-images.com')
        mock_conntype.return_value = conn

        # Mock out the cloudinary call
        mock_cloudinary.side_effect = \
          lambda x: tornado.httpclient.HTTPResponse(x, 200)

        # Mock the image download
        def _handle_img_download(request, *args, **kwargs):
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
                                                          *args, **kwargs)
            return response
        
        mock_img_download.side_effect = _handle_img_download 
        yield self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "custom_upload",
                "urls": ["http://custom_thumbnail.jpg"]
                }

        vals = {'thumbnails' : [data]}
        response = yield self.put_request(url, vals, self.api_key, jsonheader=True)
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
                         ['http://s3.amazonaws.com/host-thumbnails/%s.jpg' %
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
        # TODO(Sunil) Enable when redirection is fixed
        #self.assertIsNotNone(conn.get_bucket('host-thumbnails').get_key(
        #    "%s/%s/customupload0.jpg" % (self.api_key, vid)))
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
        response = yield self.put_request(url, vals, self.api_key, jsonheader=True)
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
        # TODO(Sunil) Enable when redirection is fixed
        #self.assertIsNotNone(conn.get_bucket('host-thumbnails').get_key(
        #    "%s/%s/customupload-1.jpg" % (self.api_key, vid)))
        
    
        # Image download error
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "custom_upload",
                "urls": ["http://error_image.jpg"]
                }

        vals = {'thumbnails' : [data]}
        with self.assertRaises(HTTPError) as e:
            yield self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(e.exception.code, 400)

        # cloudinary error 
        mock_cloudinary.send_request.side_effect = \
          lambda x, callback: callback(
              tornado.httpclient.HTTPResponse(x, 200, buffer=StringIO(
                  '{"error": "fake error"}')))
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "custom_upload",
                "urls": ["http://custom_thumbnail.jpg"]
                }

        vals = {'thumbnails' : [data]}
        response = yield self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 202) 

        # Make sure that there are 3 custom thumbs now
        i_vid = self.api_key + "_" + vid
        vmdata = neondata.VideoMetadata.get(i_vid)
        thumbs = neondata.ThumbnailMetadata.get_many(vmdata.thumbnail_ids)
        self.assertEqual(
            len([x for x in thumbs 
                 if x.type == neondata.ThumbnailType.CUSTOMUPLOAD]), 3)

    @tornado.testing.gen_test
    def test_invalid_upload_custom_thumbnail(self):
        vid = self._get_videos()[0]
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        data = {
                "created_time": time.time(),
                "type": "default",
                "urls": ["http://some_image.jpg"]
                }
        vals = {'thumbnails' : [data]}
        with self.assertRaises(HTTPError) as e:
            yield self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(e.exception.code, 400)
        self.assertRegexpMatches(
            json.loads(e.exception.response.body)['error'],
            'no valid thumbnail found')

    @tornado.testing.gen_test
    def test_disable_thumbnail(self):
        '''
        Test disable thumbnail
        '''

        yield self._setup_initial_brightcove_state()
        vids = self._get_videos()
        vid  = vids[0]
        job_id = self.job_ids[0]
        tids = self._get_thumbnails(vid)
        tid = tids[0]
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/thumbnails/%s" %(self.a_id, self.b_id, tid))
        vals = {'property' : "enabled", "value" : False}
        response = yield self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(response.code, 202) 
        self.assertFalse(neondata.ThumbnailMetadata.get(tid).enabled)

        # Now test enabling the thumb using a form encoded request
        vals = {'property' : "enabled", "value" : 'true'}
        response = yield self.put_request(url, vals, self.api_key)
        self.assertEqual(response.code, 202)
        self.assertTrue(neondata.ThumbnailMetadata.get(tid).enabled)

    @tornado.testing.gen_test
    def test_change_invalid_thumb_property(self):
        yield self._setup_initial_brightcove_state()
        vids = self._get_videos()
        vid  = vids[0]
        job_id = self.job_ids[0]
        tids = self._get_thumbnails(vid)
        tid = tids[0]
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/thumbnails/%s" %(self.a_id, self.b_id, tid))
        vals = {'property' : 'chosen', "value" : False}
        with self.assertRaises(HTTPError) as e:
            yield self.put_request(url, vals, self.api_key, jsonheader=True)
        self.assertEqual(e.exception.code, 400) 

    @tornado.testing.gen_test
    def test_job_status(self):
        '''
        Get Job Status 
        '''

        yield self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        job_id = self.job_ids[0]
        url = self.get_url("/api/v1/jobs/%s/" % job_id)
        response = yield self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
        jresponse = json.loads(response.body)
        self.assertEqual(jresponse["job_id"], job_id)
        self.assertEqual(jresponse["video_id"], vid)

    @tornado.testing.gen_test
    def test_update_video_abtest_state(self):
        '''
        Test udpating video abtest state
        '''

        yield self._setup_initial_brightcove_state()
        vid = self._get_videos()[0]
        url = self.get_url("/api/v1/accounts/%s/neon_integrations"
                    "/%s/videos/%s" %(self.a_id, "0", vid))
        vals = {"abtest" : False}
        response = yield self.put_request(url, vals, self.api_key,
                                          jsonheader=True)
        self.assertEqual(response.code, 202)

        # not a boolean, invalid value
        vals = {"abtest" : "abe"}
        with self.assertRaises(HTTPError) as e:
            yield self.put_request(url, vals, self.api_key,
                                          jsonheader=True)
        self.assertEqual(e.exception.code, 400)
        err_msg = '{"error": "invalid data type or not boolean"}'
        self.assertEqual(e.exception.response.body, err_msg)

    @tornado.testing.gen_test
    def test_get_video(self):
        '''
        Get Video via videos/:video_id endpoint
        '''

        yield self._setup_initial_brightcove_state()
        vids = self._get_videos()
        vid  = vids[0]
        job_id = self.job_ids[0]
        tids = self._get_thumbnails(vid)
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videos/%s" %(self.a_id, self.b_id, vid))
        response = yield self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
        resp = json.loads(response.body)
        self.assertEqual(resp['items'][0]['video_id'], vid)

    @tornado.testing.gen_test
    def test_get_video_ids(self):
        ''' /videoids api '''
        yield self._setup_initial_brightcove_state()
        vids = self._get_videos()
        url = self.get_url("/api/v1/accounts/%s/brightcove_integrations"
                    "/%s/videoids" %(self.a_id, self.b_id))
        response = yield self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
        resp = json.loads(response.body)
        r_vids = resp['videoids']
        self.assertListEqual(sorted(r_vids), sorted(vids))
   
    @patch('cmsapi.services.utils.http') 
    @tornado.testing.gen_test
    def test_healthcheck(self, mock_http):
        url = self.get_url("/healthcheck")
        response = yield self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)
       
        request = tornado.httpclient.HTTPRequest(url="http://test")
        response = tornado.httpclient.HTTPResponse(
                    request, 200, buffer=StringIO())
        mock_http.send_request.side_effect = lambda x, callback:\
            callback(response)

        url = self.get_url("/healthcheck/video_server")
        response = yield self.get_request(url, self.api_key)
        self.assertEqual(response.code, 200)

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
