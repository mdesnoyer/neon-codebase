''' Submit jobs to the server '''

import tornado.httpclient
import tornado.ioloop
import tornado.httputil
import tornado.escape
import sys
from StringIO import StringIO
from poster.encode import multipart_encode
from PIL import Image
import poster.encode
import urllib

class BrightcoveApi(object):
    def __init__(self,neon_api_key,publisher_id=None,read_token=None,write_token=None):
        self.publisher_id = publisher_id
        self.neon_api_key = neon_api_key
        self.read_token = read_token
        self.write_token = write_token 
        self.read_url = "http://api.brightcove.com/services/library"
        self.write_url = "http://api.brightcove.com/services/post"
    
    def format_get(self, url, data=None):
        if data is not None:
            if isinstance(data, dict):
                data = urllib.urlencode(data)
            if '?' in url:
                url += '&amp;%s' % data
            else:
                url += '?%s' % data
        return url

    def add_image(self,video_id,im, **kwargs):
        #http://help.brightcove.com/developer/docs/mediaapi/add_image.cfm
        
        reference_id = kwargs.get('reference_id', None)
        remote_url = kwargs.get('remote_url', None)
        display_name = kwargs.get('display_name', None)
        
        outer = {}
        params = {}
        params["token"] = self.write_token 
        params["video_id"] = video_id 
        image = {} 
        if reference_id is not None:
            image["referenceId"] = reference_id
        if display_name is not None:
            image["displayName"] = display_name 
        image["type"] = "THUMBNAIL"
        params["image"] = image
        outer["params"] = params
        outer["method"] = "add_image"

        body = tornado.escape.json_encode(outer)
        
        #save image
        filestream = StringIO()
        im.save(filestream, 'png')
        filestream.seek(0)
        image_data = filestream.getvalue()
        post_param = []
        fileparam = poster.encode.MultipartParam("filePath",value= image_data,filetype='image/png',filename='thumb.png')
        args = poster.encode.MultipartParam("JSONRPC", value=body)
        post_param.append(args)
        post_param.append(fileparam)
        datagen, headers = multipart_encode(post_param)
        body = "".join([data for data in datagen])

        client_url = "http://api.brightcove.com/services/post"
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers =headers, body = body, request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        #print response.body
    
    def update_abtest_custom_thumbnail_video(self,video_id,neona,neonb,neonc, **kwargs):
        
        reference_id = kwargs.get('reference_id', None)
        display_name = kwargs.get('name', None)
        
        outer = {}
        params = {}
        params["token"] = self.write_token 
        video = {}
        video["id"] = video_id
        #video["thumbnailURL"] = thumbnail_url 
        
        if display_name is not None:
            video["name"] = display_name 
        cf = {}
        cf["neona"] = neona
        cf["neonb"] = neonb 
        cf["neonc"] = neonc
        
        video["customFields"] = cf 
        params["video"] = video
        outer["params"] = params
        outer["method"] = "update_video"

        body = tornado.escape.json_encode(outer)
        
        post_param =[]
        args = poster.encode.MultipartParam("JSONRPC", value=body)
        post_param.append(args)
        datagen, headers = multipart_encode(post_param)
        body = "".join([data for data in datagen])
        
        #headers = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        #body = tornado.escape.url_escape(body)
        #headers = tornado.httputil.HTTPHeaders({"content-type": "application/x-www-form-urlencoded"})
        
        client_url = "http://api.brightcove.com/services/post"
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST", headers = headers, body = body, request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        #print body
        #print response.body
    
   
    def get_publisher_feed(self,command='find_all_videos',output='json'):
        data = {}
        data['command'] = command
        data['token'] = self.read_token
        data['media_delivery'] = 'http'
        data['output'] = output
        data['video_fields'] = 'customFields,id,tags,FLVURL'
        data['any'] = 'tag:neon'

        url = self.format_get(self.read_url,data)
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        return response.body

    def process_publisher_feed(self,result):
        #   http://api.brightcove.com/services/library?command=search_videos&token=XnqvEfjmnharPqj9Ob_sLFtkkoltcoGmd4pvMSsyq8qXOscO0MoouA..&any=tag:neon&media_delivery=http&output=json&video_fields=customFields,id,tags,FLVURL

        vids_to_process = [] 
        json = tornado.escape.json_decode(result)
        items = json['items']

        #parse and get video ids to process
        for item in items:
            tags = item['tags']
            to_process = False

            #check if neon tagged
            if "neon" in tags or "Neon" in tags:
                #if vid not processed yet, then add to list
                #print item
                if item.has_key("customFields"):
                    #check if the custom field data is set 
                    if item["customFields"] is None or item["customFields"].has_key("neona") == False:
                        to_process = True
                    else:
                        #check if the field is empty
                        neonthumbnail = item["customFields"]["neona"]
                        if neonthumbnail is None or len(neonthumbnail) == 0 :
                            to_process = True
               
                #if the video is to be processed
                if to_process == True:
                    vid = item['id']
                    if item.has_key('FLVURL') == False:
                        print "ERROR http delivery not enabled", vid
                        return
                    
                    d_url = item['FLVURL']
                    print "processing ", vid
                    self.format_neon_api_request(vid,d_url) 


    def format_neon_api_request(self,id,download_url):
        request_body = {}
        
        #brightcove tokens
        request_body["write_token"] = self.write_token
        request_body["read_token"] = self.read_token
        request_body["size"] = 480 
        
        request_body["api_key"] = self.neon_api_key 
        request_body["video_id"] = str(id)
        request_body["video_title"] = str(id) 
        request_body["video_url"] = download_url
        request_body["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"
        client_url = "http://thumbnails.neon-lab.com/api/v1/submitvideo/abtest"
        #client_url = "http://localhost:8081/api/v1/submitvideo/abtest"
        request_body["abtest"] = 1

        body = tornado.escape.json_encode(request_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers = h,body = body, request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        #verify response 200 OK

    def create_neon_api_requests(self):
        
        #Get publisher feed
        response = self.get_publisher_feed(command='search_videos')

        #Parse publisher feed
        self.process_publisher_feed(response)

        return

    def create_request_by_video_id(self,video_id):
        
        url = 'http://api.brightcove.com/services/library?command=find_video_by_id&token='+ self.read_token +'&media_delivery=http&output=json&video_id=' + video_id + '&video_fields=FLVURL,id'
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        resp = tornado.escape.json_decode(response.body)
        self.format_neon_api_request(resp['id'] ,resp['FLVURL'])


if __name__ == "__main__" :

    #Test publisher feed with neon api key
    #bc = BrightcoveApi('a63728c09cda459c3caaa158f4adff49',read_token='cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..',write_token='cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLC1siJaM9THyqC2wMlIuBJg..')
    
    #post gazette
    bc = BrightcoveApi('1a1887842e4da19de2980538b1ae72d4',read_token='hLGCV_uw2wWjyVxq6wgMMPHhLf3RjQbjeBWFnRgfxBFGsCaSAPYepg..',write_token='XnqvEfjmnhbg62iUQn_fBIgJK4HJpzrdYIqnz9KdzV49IzVEKY7EGg..')
    bc.create_neon_api_requests()
    #bc.create_request_by_video_id('2472092942001')
