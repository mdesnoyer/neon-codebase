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
import properties

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.s3.bucket import Bucket

class BrightcoveApi(object):
    def __init__(self,neon_api_key,publisher_id=0,read_token=None,write_token=None,s3init=True):
        self.publisher_id = publisher_id
        self.neon_api_key = neon_api_key
        self.read_token = read_token
        self.write_token = write_token 
        self.read_url = "http://api.brightcove.com/services/library"
        self.write_url = "http://api.brightcove.com/services/post"

        if s3init:
            self.s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
            s3bucket_name = properties.S3_CUSTOMER_ACCOUNT_BUCKET_NAME
            self.s3bucket = Bucket(name = s3bucket_name, connection = self.s3conn)
            self.video_inbox = {}
            self.get_customer_video_inbox()

    def format_get(self, url, data=None):
        if data is not None:
            if isinstance(data, dict):
                data = urllib.urlencode(data)
            if '?' in url:
                url += '&amp;%s' % data
            else:
                url += '?%s' % data
        return url
   
    ''' Get the customer inbox from s3 
        the inbox is organized as follows  map of video_id with 3 possible status
        Queued (0) , Failed (-1) , Success (1)
    '''
    def get_customer_video_inbox(self):
        k = Key(self.s3bucket)
        k.key = str(self.publisher_id) + '_inbox.json'
        try:
            data = k.get_contents_as_string()
            self.video_inbox = tornado.escape.json_decode(data)
        except S3ResponseError,e:
            pass

    ''' check video has been processed already '''
    def should_process(self,vid):
        ret = True
        if self.video_inbox.has_key(vid):
            status = self.video_inbox[vid]
            if status == 1 or status ==0 : #Success or Queued
                ret = False
        return ret

    def update_customer_video_inbox(self,vids,status=0):
        for vid in vids:
            self.video_inbox[vid] = status
        
        k = Key(self.s3bucket)
        k.key = str(self.publisher_id) + '_inbox.json'

        data = tornado.escape.json_encode(self.video_inbox)
        
        #TODO Retries
        try:
            data = k.set_contents_from_string(data)
        except S3ResponseError,e:
            print "failed to save customer inbox",self.publisher_id
            return False

        return True


    ###### Brightcove media api update method ##########
    ''' add thumbnail and videostill in to brightcove account '''
    def update_thumbnail_and_videostill(self,video_id,image):
        rt = self.add_image(video_id,image,atype='thumbnail')
        rv = self.add_image(video_id,image,atype='videostill')
        
        if rt and rv:
            return True
        return False

    def add_image(self,video_id,im,atype='thumbnail', **kwargs):
        #http://help.brightcove.com/developer/docs/mediaapi/add_image.cfm
        #http://support.brightcove.com/en/video-cloud/docs/adding-images-videos-media-api#upload

        ''' helper method to send request to brightcove'''
        def send_add_image_request(headers,body):
            client_url = "http://api.brightcove.com/services/post"
            http_client = tornado.httpclient.HTTPClient()
            retries = 5
            ret = False
            for i in range(retries):
                try:
                    req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers =headers, body = body, request_timeout = 60.0, connect_timeout = 10.0)
                    response = http_client.fetch(req)
                    ret = True
                    break
                except tornado.httpclient.HTTPError, e:
                    continue
            return ret
        
        reference_id = kwargs.get('reference_id', None)
        remote_url = kwargs.get('remote_url', None)
        
        outer = {}
        params = {}
        params["token"] = self.write_token 
        params["video_id"] = video_id 
        image = {} 
        if reference_id is not None:
            image["referenceId"] = reference_id
        
        if atype == 'thumbnail':    
            image["type"] = "THUMBNAIL"
            image["displayName"] = str(self.publisher_id) + '-neon-thumbnail-for-video-' + str(video_id)
        else:
            image["type"] = "VIDEO_STILL"
            image["displayName"] = str(self.publisher_id) + '-neon-video-still-for-video-' + str(video_id) 

        params["image"] = image
        outer["params"] = params
        outer["method"] = "add_image"

        body = tornado.escape.json_encode(outer)
        
        #save image
        filestream = StringIO()
        im.save(filestream, 'jpeg')
        filestream.seek(0)
        image_data = filestream.getvalue()
        post_param = []
        fileparam = poster.encode.MultipartParam("filePath",value= image_data,filetype='image/jpeg',filename='thumbnail-' + str(video_id) + '.jpeg')
        args = poster.encode.MultipartParam("JSONRPC", value=body)
        post_param.append(args)
        post_param.append(fileparam)
        datagen, headers = multipart_encode(post_param)
        body = "".join([data for data in datagen])
        
        #send request
        ret = send_add_image_request(headers,body)
        return ret 

    ''' update_video implementation '''
    def update_brightcove_thumbnail(self,video_id, **kwargs):
        reference_id = kwargs.get('reference_id', None)
        display_name = kwargs.get('name', None)
        thumbnail_url = 'https://neon-lab-blog-content.s3.amazonaws.com/uploads/neonglogogreen2.png'

        outer = {}
        params = {}
        params["token"] = self.write_token 
        video = {}
        video["id"] = video_id
        video["thumbnailURL"] = thumbnail_url 
        video["videostillURL"] = thumbnail_url 
        
        if display_name is not None:
            video["name"] = display_name 
        
        params["video"] = video
        
        image = {}
        image["type"] = "THUMBNAIL"
        display_name = 'thumbnail-' + str(video_id)  #kwargs.get('display_name', None)
        params["image"] = image
        
        outer["params"] = params
        outer["method"] = "update_video"

        body = tornado.escape.json_encode(outer)
        post_param = []
        args = poster.encode.MultipartParam("JSONRPC", value=body)
        post_param.append(args)
        datagen, headers = multipart_encode(post_param)
        body = "".join([data for data in datagen])
        
        client_url = "http://api.brightcove.com/services/post"
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST", headers = headers, body = body, request_timeout = 600.0, connect_timeout = 20.0)
        response = http_client.fetch(req)
        
        #TODO Retry
        print response.body
        return


    ############# Brightcove media api feed method ####################
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
        data['video_fields'] = 'customFields,id,tags,FLVURL,thumbnailURL,videostillURL'
        data['any'] = 'tag:neon'

        url = self.format_get(self.read_url,data)
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        return response.body

    ''' process publisher feed for neon tags and generate brightcove thumbnail/still requests '''
    def process_publisher_feed(self,feed):
        json = tornado.escape.json_decode(feed)
        vids_to_process = [] 
        
        try:
            items = json['items']
        except:
            print json
            return
        
        #parse and get video ids to process
        for item in items:
            tags = item['tags']
            to_process = False
            
            #check if neon tagged
            if "neon" in tags or "Neon" in tags:
                #video id
                vid = str(item['id'])
                
                #if video has been processed for abtest <-- ppg
                if item.has_key("customFields"):
                    #check if the custom field data is set 
                    if item["customFields"] is not None and item["customFields"].has_key("neona"):
                        neonthumbnail = item["customFields"]["neona"]
                        #check if the field is empty
                        if neonthumbnail is not None and len(neonthumbnail) > 0:
                            #print item['id'],neonthumbnail
                            continue

                #Check if neon has selected thumbnail/ videoStill
                thumb = item['thumbnailURL'] 
                still = item['videoStillURL']
                #if 'neon' in thumb and 'neon' in still:
                #    pass #the video has already been processed
                
                if self.should_process(vid):
                    if item.has_key('FLVURL') == False:
                        print "ERROR http delivery not enabled", vid
                        return
                    d_url = item['FLVURL']
                    if d_url is None:
                        print "FLV URL Missing", vid
                        continue

                    print "creating request for video [topn] ", vid
                    self.format_neon_api_request(vid,d_url,prev_thumbnail=still,request_type='topn')

                    #add to process list   
                    vids_to_process.append(vid)

        #Update all the videos in customer inbox
        self.update_customer_video_inbox(vids_to_process)

    ''' Process publisher feed and generate abtest requests'''
    def process_publisher_feed_for_abtest(self,result):
        #   http://api.brightcove.com/services/library?command=search_videos&token=XnqvEfjmnharPqj9Ob_sLFtkkoltcoGmd4pvMSsyq8qXOscO0MoouA..&any=tag:neon&media_delivery=http&output=json&video_fields=customFields,id,tags,FLVURL

        vids_to_process = [] 
        json = tornado.escape.json_decode(result)
        try:
            items = json['items']
        except:
            print json
            return
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
                    self.format_neon_api_request(vid,d_url,request_type='abtest') 


    def format_neon_api_request(self,id,video_download_url,prev_thumbnail=None,request_type='topn'):
        request_body = {}
        
        #brightcove tokens
        request_body["write_token"] = self.write_token
        request_body["read_token"] = self.read_token
        request_body["size"] = 480 
        
        request_body["api_key"] = self.neon_api_key 
        request_body["video_id"] = str(id)
        request_body["video_title"] = str(id) 
        request_body["video_url"] = video_download_url
        request_body["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"

        if request_type == 'topn':
            client_url = "http://thumbnails.neon-lab.com/api/v1/submitvideo/brightcove"
            #client_url = "http://localhost:8081/api/v1/submitvideo/brightcove"
            request_body["brightcove"] =1
            request_body["publisher_id"] = self.publisher_id
            if prev_thumbnail is not None:
                request_body[properties.PREV_THUMBNAIL] = prev_thumbnail

        elif request_type == 'abtest':
            client_url = "http://thumbnails.neon-lab.com/api/v1/submitvideo/abtest"
            request_body["abtest"] = 1
        else:
            return
        
        #client_url = "http://localhost:8081/api/v1/submitvideo/abtest"

        body = tornado.escape.json_encode(request_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        http_client = tornado.httpclient.HTTPClient()
        
        retries = 1
        for i in range(retries):
            try:
                req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers = h,body = body, request_timeout = 60.0, connect_timeout = 10.0)
                response = http_client.fetch(req)
                #verify response 200 OK
                break
            except tornado.httpclient.HTTPError, e:
                continue

    def create_neon_api_requests(self,request_type='default'):
        
        #Get publisher feed
        response = self.get_publisher_feed(command='search_videos')

        #Parse publisher feed and create requests
        if request_type == 'abtest':
            self.process_publisher_feed_for_abtest(response)
        else:
            self.process_publisher_feed(response)
            
        return

    def create_request_by_video_id(self,video_id):
        
        url = 'http://api.brightcove.com/services/library?command=find_video_by_id&token='+ self.read_token +'&media_delivery=http&output=json&video_id=' + video_id + '&video_fields=FLVURL,id'
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        resp = tornado.escape.json_decode(response.body)
        print url
        print resp
        #self.format_neon_api_request(resp['id'] ,resp['FLVURL'])


if __name__ == "__main__" :
    print 'test'
    #Test publisher feed with neon api key
    #bc = BrightcoveApi('a63728c09cda459c3caaa158f4adff49',read_token='cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..',write_token='vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..')

    #Get videos to abtest
    #bc.create_neon_api_requests(request_type='abtest')
    
    #Test replacing default thumbnail and still for the video
    #im = Image.open('test.jpg')
    #bc.add_image('2369368872001',im,atype='thumbnail')
    #bc.add_image('2369368872001',im,atype='videostill')

    #sutter
    bc = BrightcoveApi('7f61cc2b1dead42fc05a0c87cc04eff4' ,publisher_id=817826402001,read_token='mDhucGOjGVIKggOnmbWqSOGeea1Xn08HQZfg3c1HRdu9fg5PvhLZWg..',write_token='rn-NufCTuxvQguygktpFtFEaro4tOYIp0rhSRUue1yujogl3HNtVlw..')
    bc.create_request_by_video_id('819903089001')
