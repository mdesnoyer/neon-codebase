'''
OOYALA API Interface 
'''

import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

import base64
import json
import hashlib
import logging
import properties
import urllib
from utils.http import RequestPool
import utils.http
import time
import tornado.escape
import tornado.gen
import tornado.httpclient
import utils.logs
import utils.neon
_log = utils.logs.FileLogger(__name__)

HTTP_METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']    
DEFAULT_EXPIRATION_WINDOW = 15
DEFAULT_ROUND_UP_TIME = 300
API_VERSION = 'v2'
DEFAULT_BASE_URL = 'api.ooyala.com'
DEFAULT_CACHE_BASE_URL = 'cdn-api.ooyala.com'
API_URL = 'http://%s'%DEFAULT_BASE_URL

logging.basicConfig(format='',level=logging.INFO)

class OoyalaAPI(object):
    def __init__(self, 
                api_key,
                secret_key,
                base_url=DEFAULT_BASE_URL,
                cache_base_url=DEFAULT_CACHE_BASE_URL,
                expiration=DEFAULT_EXPIRATION_WINDOW,
                local=True):
        """OoyalaAPI Constructor
        
        Type signature:
            (str, str, str:DEFAULT_BASE_URL, int:DEFAULT_EXPIRATION_WINDOW) -> OoyalaAPI
        Parameters:
            api_key    - The API key
            secret_key - The secret key
            base_url   - the url's base
            expiration - the expiration window, in seconds
        Example:
            api = OoyalaAPI("..", "..")
        """
        self._secret_key = secret_key
        self._api_key = api_key
        self._base_url = base_url
        self._cache_base_url = cache_base_url
        self._expiration_window = expiration
        self._response_headers = [()]
        self.http_request_pool = RequestPool(5, 3)
        self.local = local 
        if self.local:
            self.neon_uri = "http://localhost:8081/api/v1/submitvideo/"
        else:
            self.neon_uri = "http://thumbnails.neon-lab.com/api/v1/submitvideo/" 

    def send_request(self, http_method, relative_path, body=None, params={}, callback=None):
        """Send a request.
        
        Type signature:
            (str, str, str:None, dict:{}) -> json str | None
        Parameters:
            http_method   - the http method
            relative_path - the url's relative path
            body          - the request's body
            params        - the query parameters
        Example:
            api = OoyalaAPI(secret_key, api_key)
            response = api.send_request('GET', 'players/2kj390')
            response = api.send_request('PATCH', 'players/2kj390', "{'name': 'my new player name'}")
        """

        ### Helper method ###
        def response_callback(response):
            '''
            Response callback from http.send_request
            '''
            data = response.body
            if (response.code >= 400): 
                return None
            else:
                if (len(data)==0):
                    data = "true"
                return json.loads(data)

        # create the url
        path = '/%s/%s' %(API_VERSION, relative_path)
        
        # Convert the body to JSON format
        json_body = ''
        if (body is not None):
            json_body = json.dumps(body) if type(body) is not str else body

        url = self.build_path_with_authentication_params(
                                http_method, path, params, json_body)
        if url is None:
            return None
       
        base_url = self.base_url if http_method != 'GET' else self.cache_base_url
        
        #hack needed when a PUT request has an empty body
        headers = {}
        if (body is None and http_method == 'PUT'):
            headers['Content-length'] = 0
       
        url = "https://%s%s" %(base_url, url)

        #Make the request
        if http_method == 'GET':
            json_body = None
        request = tornado.httpclient.HTTPRequest(url=url,
                                             method=http_method,
                                             headers=headers,
                                             body=json_body,
                                             request_timeout=60.0,
                                             connect_timeout=5.0)

        if callback:
            #http_client = tornado.httpclient.AsyncHTTPClient()
            self.http_request_pool.send_request(request, 
                                    lambda x: callback(response_callback(x)))
        else:
            response = self.http_request_pool.send_request(request)
            return response_callback(response)

    def get(self, path, query={}, callback=None):
        """Send a GET request.

        Type signature:
            (str, srt:{}) -> json str | None
        Parameters:
            path   - the url's path
            query - the query parameters
        Example:
            api = Ooyala(...)
            response = api.get('players/2kj390')
        """
        return self.send_request('GET', path, None, query, callback)

    def put(self, path, body, callback=None):
        """Send a PUT request.

        Type signature:
            (str, object) -> json str | None
        Parameters:
            path - the url's path
            body - the request's body
        Example:
            api = Ooyala(...)
            response = api.put('players/2kj390/metadata', {'skin' : 'branded'})
        """
        return self.send_request('PUT', path, body, {}, callback)

    def post(self, path, body, callback=None):
        """Send a POST request.

        Type signature:
            (str, object) -> json str | None
        Parameters:
            path - the url's path
            body - the request's body
        Example:
            api = Ooyala(...)
            response = api.post('players/', {'name' : 'sample player'})
        """
        return self.send_request('POST', path, body, {}, callback)

    def patch(self, path, body, callback=None):
        """Send a PATCH request.

        Type signature:
            (str, object) -> json str | None
        Parameters:
            path - the url's path
            body - the request's body
        Example:
            api = Ooyala(...)
            response = api.patch('players/2kj390', {'name' : 'renamed player'})
        """
        return self.send_request('PATCH', path, body, {}, callback)

    def delete(self, path):
        """Send a DELETE request.

        Type signature:
            (str) -> json str | None
        Key Parameters:
            'path' - the url's path
        Example:
            api = Ooyala(...)
            response = api.delete('players/2kj390')
        """
        return self.send_request('DELETE', path)

    def generate_signature(self, http_method, path, params, body=''):
        """Generates the signature for a request.

        Type signature:
            (str, str, dict, str:'') -> str
        Parameters:
            http_method - the http method
            path        - the url's path
            body        - the request's body (a JSON encoded string)
            params      - query parameters
        """
        signature = str(self.secret_key) + http_method.upper() + path
        for key, value in sorted(params.iteritems()):
            signature += key + '=' + str(value)
        # This is neccesary on python 2.7. if missing, 
        # signature+=body with raise an exception when body are bytes (image data)
        signature = signature.encode('ascii')
        signature += body
        signature = base64.b64encode(hashlib.sha256(signature).digest())[0:43]
        signature = urllib.quote_plus(signature)
        return signature

    def build_path(self, path, params):
        """Build the path for a request.
        
        Type signature:
            (str, dict) -> str
        Parameters:
            path        - the url's path
            params      - the query parameters
        """
        # a local function which check if item is a query param
        f = lambda k: k == 'where' or k == 'orderby' or k == 'limit' or k == 'page_token'
        url = path + '?'
        url += "&".join(["%s=%s" % (key, urllib.quote_plus(str(value)) if f(key) else value) for key, value in params.items()])
        return url

    def build_path_with_authentication_params(self, http_method, path, params, body):
        """Build the path for a Ooyala API request, including authentication parameters.
        
        Type signature:
            (str, str, str, dict, str) -> str
        Parameters:
            http_method - the http method
            path        - the url's path
            params      - the query parameters
            body        - the request's body
        """
        if (http_method not in HTTP_METHODS) or (self.api_key is None) or (self.secret_key is None):
            return None
        authentication_params = dict(params)
        #add the ooyala authentication params
        authentication_params['api_key'] = str(self.api_key)
        authentication_params['expires'] = str(self.expires)
        authentication_params['signature'] = self.generate_signature(http_method, path, authentication_params, body)
        return self.build_path(path, authentication_params)

    @property
    def response_headers(self):
        """Get the response's header from last request made.

        Type signature:
            () -> [(header,value)]
        """
        return self._response_headers

    def get_secret_key(self):
        """Secret Key getter.

        Type signature:
            () -> str
        Example:
            api = OoyalaAPI(...)
            print 'the secret key is ', api.secret_key
        """
        return self._secret_key

    def set_secret_key(self, value):
        """Secret Key setter.

        Type signature:
            (str) -> None
        Parameters:
            value - the secret key to be set
        Example:
            api = OoyalaAPI(...)
            api.secret_key = "83jja"
            print 'the new secret key is ', api.secret_key
        """
        self._secret_key = value

    secret_key = property(get_secret_key, set_secret_key)

    def get_api_key(self):
        """API Key getter.

        Type signature:
            () -> str
        Example:
            api = OoyalaAPI(...)
            print 'the API key is ', api.api_key
        """
        return self._api_key

    def set_api_key(self, value):
        """API Key setter.

        Type signature:
            (str) -> None
        Parameters:
            value - the API key to be set
        Example:
            api = OoyalaAPI(...)
            api.api_key = "332kka"
            print 'the new API key is ', api.api_key
        """
        self._api_key = value

    api_key = property(get_api_key, set_api_key)

    def get_base_url(self):
        """Base url getter.

        Type signature:
            () -> str
        Example:
            api = OoyalaAPI(...)
            print 'the base url is ', api.base_url
        """
        return self._base_url

    def set_base_url(self, value):
        """Base url setter.

        Type signature:
            (str) -> None
        Parameters:
            value - the url's base to be set
        Example:
            api = OoyalaAPI(...)
            api.base_url = "cache.api.ooyala.com"
            print 'the new url's base is ', api.base_url
        """
        self._base_url = value

    base_url = property(get_base_url, set_base_url)
    
    def get_cache_base_url(self):
        """Cache base url getter.
            
            Type signature:
            () -> str
            Example:
            api = OoyalaAPI(...)
            print 'the cache base url is ', api.cache_base_url
            """
        return self._cache_base_url
    
    def set_cache_base_url(self, value):
        """Cache base url setter.
            
            Type signature:
            (str) -> None
            Parameters:
            value - the url's base to be set
            Example:
            api = OoyalaAPI(...)
            api.base_url = "cache.api.ooyala.com"
            print 'the new url's base is ', api.cache_base_url
            """
        self._cache_base_url = value
    
    cache_base_url = property(get_cache_base_url, set_cache_base_url)

    def get_expiration_window(self): return self._expiration_window
    def set_expiration_window(self, value): self._expiration_window = value
    def del_expiration_window(self): del self._expiration_window
    expiration_window = property(get_expiration_window, set_expiration_window, del_expiration_window)

    @property
    def expires(self):
        """Computes the expiration's time

        Type signature:
            () -> int
        """
        now_plus_window = int(time.time()) + self.expiration_window
        return now_plus_window + DEFAULT_ROUND_UP_TIME - (now_plus_window % DEFAULT_ROUND_UP_TIME)

    
    def get_video_url(self, video_id, callback=None):
        '''
        Get video url to download the video from the asset streams response
        
        #TODO: find the muxing format to choose, is MP4 always available?
        '''
        #http://ak.c.ooyala.com/l2djJvazrgOdtAiOtaWrZejpYgsdH8zc/DOcJ-FxaFrRg4gtDEwOmY1OjBrO_V8SW

        video_data = self.get('assets/%s/streams'%video_id)
        #profiles = {} 
        for p in video_data:
            #prof = p['profile']
            #profiels[prof] = p['url']
            if p['muxing_format'] != "NA":
                return p['url']

    @tornado.gen.engine
    def _create_video_requests_on_signup(self, oo_account, limit=10, callback=None):
        '''
        http://support.ooyala.com/developers/documentation/api/query.html
        '''

        qs = {} 
        qs["orderby"] = "created_at+DESCENDING"
        feed = yield tornado.gen.Task(self.get, 'assets/', qs)
        items = feed["items"]
        response = self.create_neon_request_from_asset_feed(oo_account, items, limit)
        if callback:
            callback(response)
        else:
            raise Exception("Sync method not permitted currently")

    def process_publisher_feed(self, oo_account):
        ''' 
        process ooyala feed and create neon requests 
        '''
       
        #Get all assets for the customer
        qs = {} 
        qs["orderby"] = "created_at+DESCENDING"
        feed = self.get('assets/', qs)
        items = feed["items"]
        self.create_neon_request_from_asset_feed(oo_account, items)

    def create_neon_request_from_asset_feed(self, oo_account, items, limit=None): 
        '''
        oo_account: ooyala platform account
        items: [] of dict objects with asset information
        limit: # of videos to create requests

        No async http calls in this method. There was a bug with creating async calls 
        to the neon server in brightcove api, hence resorting to sync http calls here
        The bug to be re-explored soon and the code to be made synci & async compatible
        '''

        items_processed = [] 
        videos_processed = oo_account.get_videos() 
        if videos_processed is None:
            videos_processed = {} 
        
        #parse and get video ids to process
        count = 0
        for item in items:
            if item['asset_type'] == "video":
                count += 1
                #limit the #of videos to process
                if limit is not None and count >= limit:
                    break
                
                vid = str(item['embed_code']) #video id is embed_code
                if vid in videos_processed:
                    continue
                title = item['name']
                length = item['duration']
                d_url = self.get_video_url(vid)
                if d_url is None:
                    _log.error('msg=Download url was empty for video %s'%vid)
                    continue

                thumb_url = item['preview_image_url']
                if thumb_url is None or thumb_url == '':
                    thumb_url = "https://images.puella-magi.net/thumb/2/27/No_Image_Wide.svg/800px-No_Image_Wide.svg.png"

                _log.info("Creating request for video [topn] %s" % vid)
                resp = self.send_neon_api_request(oo_account.neon_api_key,
                                                  vid,
                                                  title,
                                                  d_url,
                                                  prev_thumbnail=thumb_url,
                                                  request_type='topn',
                                                  i_id=oo_account.integration_id,
                                                  autosync=oo_account.auto_update)
                if resp is not None and not resp.error:
                    items_processed.append(item)
                    #update customer inbox
                    #TODO: get ooyala account, rather using local copy  
                    response = tornado.escape.json_decode(resp.body)
                    j_id = response['job_id']
                    oo_account.videos[vid] = j_id
                    oo_account.save()

        return items_processed 

    def send_neon_api_request(self, neon_api_key, vid, title, video_download_url, 
                                prev_thumbnail=None, request_type='topn',
                                i_id=None, autosync=False, callback=None):
        ''' Format and submit reuqest to neon thumbnail api '''

        request_body = {}
        request_body["oo_api_key"] = self._api_key
        request_body["oo_secret_key"] = self._secret_key 
        request_body["api_key"] = neon_api_key 
        request_body["video_id"] = str(vid)
        request_body["video_title"] = str(vid) if title is None else title 
        request_body["video_url"] = video_download_url
        if self.local:
            request_body["callback_url"] = "http://localhost:8081/testcallback"
        else:
            request_body["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"
        request_body["autosync"] = autosync
        request_body["topn"] = 1
        request_body["integration_id"] = i_id 

        if request_type == 'topn':
            client_url = self.neon_uri + "ooyala"
            request_body["ooyala"] = 1
            if prev_thumbnail is not None:
                _log.debug("key=format_neon_api_request "
                        " msg=ooyala preview thumbnail not set")
            request_body[properties.PREV_THUMBNAIL] = prev_thumbnail
        else:
            return
        
        body = tornado.escape.json_encode(request_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        req = tornado.httpclient.HTTPRequest(url=client_url,
                                             method="POST",
                                             headers=h,
                                             body=body,
                                             request_timeout=30.0,
                                             connect_timeout=10.0)
        response = self.http_request_pool.send_request(req)
        if response.code == 409:
            _log.error("Duplicate Neon api request")
        elif response.code >= 400:
            _log.error("Neon api request failed")
        return response

    @tornado.gen.engine 
    def update_thumbnail_from_url(self, video_id, image_url, 
                                        tid, fsize, callback):
        '''
        Async method, no sync version currently

        DOC: http://support.ooyala.com/developers/documentation/api/asset_video.html
        Upload Custom Preview Image
            [POST] /v2/assets/asset_id/preview_image_files
            <file_contents>
           
        Set Primary Preview Image Configuration
        Set the type of the primary preview image of an asset to one of the following
        
        generated: use the autogenerated preview image
        uploaded_file: use the uploaded custom preview image
        remote_url: URL for the preview image
            [PUT] /v2/assets/asset_id/primary_preview_image
            {
               "type" : "generated" | "uploaded_file" | "remote_url"
            }

        '''

        def handle_response(resp):
            '''
            resp is json response 
            common response handler for the states below
            '''
            current_state = state
            if resp is None:
                msg = "failed to %s for video %s" %(current_state, video_id)        
                _log.error("update_thumbnail_from_url msg=%s"%msg)
                callback(None)
                return
            else:
                if isinstance(resp, tornado.httpclient.HTTPResponse):
                    return resp.body
                return resp
 
        states = ["download_image", "upload_thumbnail", "set_primary"] 
 
        state = states[0] 
        req = tornado.httpclient.HTTPRequest(url=image_url,
                                             method="GET",
                                             request_timeout=60.0,
                                             connect_timeout=10.0)
        image_response = yield tornado.gen.Task(
                            self.http_request_pool.send_request, req) 
        image_body = handle_response(image_response)
        
        #Upload the image
        state = states[1] 
        upload_response = yield tornado.gen.Task(
                            self.post, 
                            'assets/%s/preview_image_files'%video_id, 
                            image_body)

        up_response = handle_response(upload_response)
        #verify body ? doc on this missing --       
 
        #Set as primary image
        state = states[2] 
        body = {}
        body["type"] = "uploaded_file"
        set_response = yield tornado.gen.Task(
                                self.put, 
                                'assets/%s/primary_preview_image'%video_id, 
                                body)

        set_resp = handle_response(set_response)
        #if set_resp["status"] == "uploading":
        #    callback(True)
        #    return
        if set_resp:
            callback(True)
            return

        #something went wrong ?
        callback(False) 
