import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)

from cmsapiv2 import controllers
import json
import tornado.gen
import tornado.ioloop
import tornado.testing
import tornado.httpclient
import test_utils.redis
import unittest
import utils.neon
import utils.http
import urllib
import test_utils.neontest
from mock import patch
from cmsdb import neondata
from StringIO import StringIO
from utils.imageutils import PILImageUtils
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse 
from tornado.httputil import HTTPServerRequest

#from tornado.testing import AsyncHTTPTestCase

class TestNewAccountHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application

    @tornado.testing.gen_test 
    def test_create_new_account(self):
        url = '/api/v2/accounts?customer_name=meisnew'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], 'meisnew') 

    @tornado.testing.gen_test
    def test_get_new_acct_not_implemented(self):
        try: 
            url = '/api/v2/accounts' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method="GET")
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
	    pass 


class TestAccountHandler(tornado.testing.AsyncHTTPTestCase):
    def get_app(self): 
        return controllers.application
    def setUp(self):
        self.user = neondata.NeonUserAccount(customer_name='testingaccount')
        self.user.save() 
        super(TestAccountHandler, self).setUp()

    @tornado.testing.gen_test
    def test_get_acct_does_not_exist(self):
        try: 
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method="GET")
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 400) 
	    pass
 
    @tornado.testing.gen_test
    def test_post_acct_not_implemented(self):
        try: 
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='abc123', 
                                                    method="POST")
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
	    pass 
    
    @tornado.testing.gen_test
    def test_delete_acct_not_implemented(self):
        try: 
            url = '/api/v2/124abc' 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='DELETE')
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
	    pass 
    
    @tornado.testing.gen_test 
    def test_get_acct_does_exist(self):
        try: 
            url = '/api/v2/accounts?customer_name=123abc'
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    body='', 
                                                    method='POST', 
                                                    allow_nonstandard_methods=True)
	    self.assertEquals(response.code, 200)
            rjson = json.loads(response.body)
            self.assertEquals(rjson['customer_name'], '123abc') 
            url = '/api/v2/%s' % (rjson['neon_api_key']) 
            response = yield self.http_client.fetch(self.get_url(url), 
                                                    method="GET")
            rjson2 = json.loads(response.body) 
            self.assertEquals(rjson['account_id'],rjson2['account_id']) 
        except tornado.httpclient.HTTPError as e:
            pass

    @tornado.testing.gen_test 
    def test_basic_post_account(self):
        url = '/api/v2/accounts?customer_name=123abc'
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='POST', 
                                                allow_nonstandard_methods=True)
	self.assertEquals(response.code, 200)
        rjson = json.loads(response.body)
        self.assertEquals(rjson['customer_name'], '123abc') 

    @tornado.testing.gen_test 
    def test_update_acct_base(self): 
        url = '/api/v2/accounts/%s?default_height=1200&default_width=1500' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        rjson = json.loads(response.body)
        default_size = rjson['default_size']
        self.assertEquals(default_size[0],1500)
        self.assertEquals(default_size[1],1200)

    @tornado.testing.gen_test 
    def test_update_acct_height_only(self): 
        # do an extra get here, in case we've been modifying this user elsewhere
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        orig_user = json.loads(response.body)
        default_size_old = orig_user['default_size'] 

        url = '/api/v2/accounts/%s?default_height=1200' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        new_user = json.loads(response.body)
        default_size_new = new_user['default_size']
        self.assertEquals(default_size_new[1],1200)
        self.assertEquals(default_size_new[0],default_size_old[0])

    @tornado.testing.gen_test 
    def test_update_acct_width_only(self): 
        # do a get here to test and make sure the height wasn't messed up
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        orig_user = json.loads(response.body)
        default_size_old = orig_user['default_size'] 

        url = '/api/v2/accounts/%s?default_width=1200' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                body='', 
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
         
        url = '/api/v2/%s' % (self.user.neon_api_key) 
        response = yield self.http_client.fetch(self.get_url(url), 
                                                method="GET")
        new_user = json.loads(response.body)
        default_size_new = new_user['default_size']
        self.assertEquals(default_size_new[0],1200)
        self.assertEquals(default_size_new[1],default_size_old[1])
 
class TestOoyalaIntegrationHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application
    def setUp(self):
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testiid' 
        defop = neondata.OoyalaPlatform.modify(self.account_id_api_key, self.test_i_id, lambda x: x, create_missing=True) 
        super(TestOoyalaIntegrationHandler, self).setUp()

    @tornado.testing.gen_test 
    def test_post_integration(self):
        url = '/api/v2/%s/integrations/ooyala?publisher_id=123123abc' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                          rjson['neon_api_key'], 
                                          rjson['integration_id'])

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_get_integration(self):
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s' % (self.account_id_api_key, self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_put_integration(self):
        ooyala_api_key = 'testapikey' 
        url = '/api/v2/%s/integrations/ooyala?integration_id=%s&ooyala_api_key=%s' % (self.account_id_api_key, self.test_i_id, ooyala_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 200)
        platform = yield tornado.gen.Task(neondata.OoyalaPlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(platform.ooyala_api_key, ooyala_api_key)  

class TestBrightcoveIntegrationHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application
    def setUp(self):
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testbciid' 
        defop = neondata.BrightcovePlatform.modify(self.account_id_api_key, self.test_i_id, lambda x: x, create_missing=True) 
        super(TestBrightcoveIntegrationHandler, self).setUp()

    @tornado.testing.gen_test 
    def test_post_integration(self):
        url = '/api/v2/%s/integrations/brightcove?publisher_id=123123abc' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST',
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          rjson['neon_api_key'], 
                                          rjson['integration_id'])

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_get_integration(self):
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s' % (self.account_id_api_key, self.test_i_id)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        self.assertEquals(response.code, 200)
        rjson = json.loads(response.body) 
        platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(rjson['integration_id'], platform.integration_id) 
 
    @tornado.testing.gen_test 
    def test_put_integration(self):
        read_token = 'readtoken' 
        url = '/api/v2/%s/integrations/brightcove?integration_id=%s&read_token=%s' % (self.account_id_api_key, self.test_i_id, read_token)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)

        self.assertEquals(response.code, 200)
        platform = yield tornado.gen.Task(neondata.BrightcovePlatform.get, 
                                          self.account_id_api_key, 
                                          self.test_i_id)

        self.assertEquals(platform.read_token, read_token)  

class TestVideoHandler(tornado.testing.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application
    def setUp(self):
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        self.test_i_id = 'testvideohiid'
        thumbnail_one = neondata.ThumbnailMetadata('testing_vtid_one', width=500)
        thumbnail_two = neondata.ThumbnailMetadata('testing_vtid_two', width=500)
        thumbnail_one.save()
        thumbnail_two.save()
        defop = neondata.BrightcovePlatform.modify(self.account_id_api_key, self.test_i_id, lambda x: x, create_missing=True) 
        user.modify(self.account_id_api_key, lambda p: p.add_platform(defop))
        super(TestVideoHandler, self).setUp()
    
    @unittest.skip("not implemented yet") 
    @patch('api.brightcove_api.BrightcoveApi.read_connection.send_request') 
    @tornado.testing.gen_test
    def test_post_video(self, http_mock):
        url = '/api/v2/%s/videos?integration_id=%s&external_video_ref=1234ascs' % (self.account_id_api_key, self.test_i_id)
        #send_request_mock = self._callback_wrap_mock(http_mock.send_request)
        #send_request_mock.fetch().side_effect = [HTTPResponse(HTTPRequest('http://test_bc'), 200, buffer=StringIO('{"job_id":"j123"}'))]
        #request = HTTPServerRequest(uri=self.get_url(url), method='POST') 
        #response = yield controllers.VideoHelper.addVideo(request, self.account_id_api_key)
        #rv = yield self.http_client.fetch(self.get_url(url),
        #                                   body='',
        #                                   method='POST',
        #                                   allow_nonstandard_methods=True)
    @tornado.testing.gen_test
    def test_get_without_video_id(self):
        try: 
            vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
            vm.save()
            url = '/api/v2/%s/videos' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
        except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 400) 

    @tornado.testing.gen_test
    def test_get_single_video(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_get_two_videos(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid2'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1,vid2' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 2)

    @tornado.testing.gen_test
    def test_get_single_video_with_fields(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
       
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_update_video_testing_enabled(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'))
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&testing_enabled=0' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        rjson = json.loads(response.body)
        self.assertFalse(rjson['testing_enabled'])
        self.assertEquals(response.code, 200)

        url = '/api/v2/%s/videos?video_id=vid1&testing_enabled=1' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        rjson = json.loads(response.body)
        self.assertTrue(rjson['testing_enabled'])
        self.assertEquals(response.code, 200)

    @tornado.testing.gen_test
    def test_get_single_video_with_thumbnails_field(self):
        vm = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,'vid1'), 
                                    tids=['testing_vtid_one', 'testing_vtid_two'])
        vm.save()
        url = '/api/v2/%s/videos?video_id=vid1&fields=created,thumbnails' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(response.code, 200)
        self.assertEquals(rjson['video_count'], 1)

        thumbnail_array = rjson['videos'][0]['thumbnails']
        self.assertEquals(len(thumbnail_array), 2)
        self.assertEquals(thumbnail_array[0]['width'], 500)

    @tornado.testing.gen_test
    def test_update_video_does_not_exist(self):
        try: 
            url = '/api/v2/%s/videos?video_id=vid_does_not_exist&testing_enabled=0' % (self.account_id_api_key)
            response = yield self.http_client.fetch(self.get_url(url),
                                                    body='',
                                                    method='PUT', 
                                                    allow_nonstandard_methods=True)
	except Exception as e:
            self.assertEquals(e.code, 400)

class TestThumbnailHandler(test_utils.neontest.AsyncHTTPTestCase): 
    def get_app(self): 
        return controllers.application

    def setUp(self):
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        user = neondata.NeonUserAccount(customer_name='testingme')
        user.save() 
        self.account_id_api_key = user.neon_api_key
        neondata.ThumbnailMetadata('testingtid', width=500).save()
        self.test_video = neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,
                             'tn_test_vid1')).save()
        neondata.VideoMetadata(neondata.InternalVideoID.generate(self.account_id_api_key,
                             'tn_test_vid2')).save()

        self.cdn_mocker = patch('cmsdb.cdnhosting.CDNHosting')
        self.cdn_mock = self._future_wrap_mock(
            self.cdn_mocker.start().create().upload)
        self.cdn_mock.return_value = [('some_cdn_url.jpg', 640, 480)]
        self.im_download_mocker = patch(
            'utils.imageutils.PILImageUtils.download_image')
        self.random_image = PILImageUtils.create_random_image(480, 640)
        self.im_download_mock = self._future_wrap_mock(
            self.im_download_mocker.start())
        self.im_download_mock.side_effect = [self.random_image] 
        super(TestThumbnailHandler, self).setUp()

    def tearDown(self): 
        self.redis.stop()
        self.cdn_mocker.stop()
        self.im_download_mocker.stop()
    
    @tornado.testing.gen_test
    def test_add_new_thumbnail(self):
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid1&thumbnail_location=blah.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code,202)
        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'tn_test_vid1')
        video = neondata.VideoMetadata.get(internal_video_id)
         
        self.assertEquals(len(video.thumbnail_ids), 1)

    @tornado.testing.gen_test
    def test_add_two_new_thumbnails(self):
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid2&thumbnail_location=blah.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code,202)
        url = '/api/v2/%s/thumbnails?video_id=tn_test_vid2&thumbnail_location=blah.jpg' % (self.account_id_api_key)
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='POST', 
                                                allow_nonstandard_methods=True)
        self.assertEquals(response.code,202)
        internal_video_id = neondata.InternalVideoID.generate(self.account_id_api_key,'tn_test_vid2')
        video = neondata.VideoMetadata.get(internal_video_id)
        self.assertEquals(len(video.thumbnail_ids), 2)

    @tornado.testing.gen_test
    def test_get_thumbnail_exists(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        rjson = json.loads(response.body)
        self.assertEquals(rjson['width'],500)
        self.assertEquals(rjson['key'],'testingtid')

    @tornado.testing.gen_test
    def test_get_thumbnail_does_not_exist(self):
        try: 
            url = '/api/v2/%s/thumbnails?thumbnail_id=testingtiddoesnotexist' % (self.account_id_api_key) 
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='GET')
	except Exception as e:
            self.assertEquals(e.code, 400)

    @tornado.testing.gen_test
    def test_thumbnail_update_enabled(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        old_tn = json.loads(response.body)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid&enabled=0' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body)
        self.assertEquals(new_tn['enabled'],False)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid&enabled=1' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body) 
        self.assertEquals(new_tn['enabled'],True)

    @tornado.testing.gen_test
    def test_thumbnail_update_no_params(self):
        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                method='GET')
        old_tn = json.loads(response.body)

        url = '/api/v2/%s/thumbnails?thumbnail_id=testingtid' % (self.account_id_api_key) 
        response = yield self.http_client.fetch(self.get_url(url),
                                                body='',
                                                method='PUT', 
                                                allow_nonstandard_methods=True)
        new_tn = json.loads(response.body) 
        self.assertEquals(response.code,200)
        self.assertEquals(new_tn['enabled'],old_tn['enabled'])

    @tornado.testing.gen_test
    def test_delete_thumbnail_not_implemented(self):
        try: 
            url = '/api/v2/%s/thumbnails?thumbnail_id=12234' % (self.account_id_api_key)  
            response = yield self.http_client.fetch(self.get_url(url),
                                                    method='DELETE')
	except tornado.httpclient.HTTPError as e:
	    self.assertEquals(e.code, 501) 
	    pass 

if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
