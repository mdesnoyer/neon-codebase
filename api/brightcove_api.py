''' Submit jobs to the server '''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import tornado.httpclient
import tornado.ioloop
import tornado.httputil
import tornado.escape
import sys
from StringIO import StringIO
from poster.encode import multipart_encode
import poster.encode
import urllib
import properties
from PIL import Image

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.s3.bucket import Bucket

import time
import os

import supportServices.neondata 

import utils.logs
import utils.neon
_log = utils.logs.FileLogger("brighcove_api")

class BrightcoveApi(object):
    def __init__(self,neon_api_key,publisher_id=0,read_token=None,write_token=None,autosync=False,publish_date=None,local=True):
        self.publisher_id = publisher_id
        self.neon_api_key = neon_api_key
        self.read_token = read_token
        self.write_token = write_token 
        self.read_url = "http://api.brightcove.com/services/library"
        self.write_url = "http://api.brightcove.com/services/post"
        self.autosync = autosync
        self.last_publish_date = publish_date if publish_date else time.time()
        self.local = local
        if local:
            self.neon_uri = "http://localhost:8081/api/v1/submitvideo/"
        else:
            self.neon_uri = "http://thumbnails.neon-lab.com/api/v1/submitvideo/" 

    def format_get(self, url, data=None):
        if data is not None:
            if isinstance(data, dict):
                data = urllib.urlencode(data)
            if '?' in url:
                url += '&amp;%s' % data
            else:
                url += '?%s' % data
        return url

    ###### Brightcove media api update method ##########
    
    ''' add thumbnail and videostill in to brightcove account '''
    def update_thumbnail_and_videostill(self,video_id,image,ref_id):
        
        #If url is passed, then set thumbnail using the remote url
        if isinstance(image,basestring):
            rt = self.add_image(video_id,remote_url = image,atype='thumbnail')
            rv = self.add_image(video_id,remote_url = image,atype='videostill')
        else:
            rt = self.add_image(video_id,image,atype='thumbnail',reference_id = ref_id)
            rv = self.add_image(video_id,image,atype='videostill',reference_id = ref_id)
        
        tref_id = None ; vref_id = None
        #Get thumbnail name, referenceId params
        if rt:
            add_image_val = tornado.escape.json_decode(rt)
            tref_id = add_image_val["result"]["referenceId"]
        if rv:
            add_image_val = tornado.escape.json_decode(rv)
            vref_id = add_image_val["result"]["referenceId"]

        return ((rt is not None and rv is not None),tref_id,vref_id)

    '''
    Update the thumbnail for a given video given the ReferenceID an existing image asset 
    '''

    def update_image_with_refid(self,video_id,refid,videostill=False):
        outer = {}
        params = {}
        params["token"] = self.write_token 
        params["video_id"] = video_id 
        image = {} 
        image["referenceId"] = refid
        if videostill:
            image["type"] = "VIDEO_STILL"
        params["image"] = image
        outer["params"] = params
        outer["method"] = "add_image"
        body = tornado.escape.json_encode(outer)
        
        post_param = []
        args = poster.encode.MultipartParam("JSONRPC", value=body)
        post_param.append(args)
        datagen, headers = multipart_encode(post_param)
        body = "".join([data for data in datagen])

        client_url = "http://api.brightcove.com/services/post"
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers =headers,
                body = body, request_timeout = 60.0, connect_timeout = 10.0)

        http_client = tornado.httpclient.HTTPClient()
        response = http_client.fetch(req)
        if not response.error:
            return True

    '''
    Add Image brightcove api call
    : remote_url sets the url to 3rd party url 
    : image creates a new asset and the url is on brightcove servers
    '''
    def add_image(self,video_id,im=None,atype='thumbnail',async_callback=None, **kwargs):
        #http://help.brightcove.com/developer/docs/mediaapi/add_image.cfm
        #http://support.brightcove.com/en/video-cloud/docs/adding-images-videos-media-api#upload

        ''' helper method to send request to brightcove'''
        def send_add_image_request(headers,body):
            client_url = "http://api.brightcove.com/services/post"
            req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers =headers, 
                            body = body, request_timeout = 60.0, connect_timeout = 10.0)
            
            # If Async call requested  
            if async_callback:
                http_client = tornado.httpclient.AsyncHTTPClient()
                response = http_client.fetch(req,async_callback)
                return
            
            http_client = tornado.httpclient.HTTPClient()
            retries = 5
            ret = False
            for i in range(retries):
                try:
                    response = http_client.fetch(req)
                    #ret = True
                    ret = response.body
                    break
                except tornado.httpclient.HTTPError, e:
                    _log.error("type=add_image msg=" + e.message)
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
        
        if remote_url:
            image["remoteUrl"] = remote_url

        params["image"] = image
        outer["params"] = params
        outer["method"] = "add_image"

        body = tornado.escape.json_encode(outer)
        
        if remote_url:
            post_param = []
            args = poster.encode.MultipartParam("JSONRPC", value=body)
            post_param.append(args)
            datagen, headers = multipart_encode(post_param)
            body = "".join([data for data in datagen])

        else:
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

    '''
    Enable a particular thumbnail in the brightcove account
    '''
    def enable_thumbnail_from_url(self,video_id,url,**kwargs):
        http_client = tornado.httpclient.HTTPClient()
        headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})
        req = tornado.httpclient.HTTPRequest(url = url,
                                                method = "GET",headers = headers,
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)
        response = http_client.fetch(req)
        imfile = StringIO(response.body)
        try:
            image =  Image.open(imfile)
        except Exception,e:
            _log.exception("Image format error %s" %e )

        reference_id = kwargs.get('reference_id', None)
        rt = self.add_image(video_id,image,atype='thumbnail',reference_id = reference_id)
        rv = self.add_image(video_id,image,atype='videostill',reference_id = reference_id if not reference_id else "still-" + reference_id)
       
        tref_id = None ; vref_id = None
        #Get thumbnail name, referenceId params
        if rt:
            add_image_val = tornado.escape.json_decode(rt)
            tref_id = add_image_val["result"]["referenceId"]
        if rv:
            add_image_val = tornado.escape.json_decode(rv)
            vref_id = add_image_val["result"]["referenceId"]

        return ((rt is not None and rv is not None),tref_id,vref_id)
    
    '''
    Enable thumbnail async
    '''

    def async_enable_thumbnail_from_url(self,video_id,img_url,callback):
        self.img_result = []  
        reference_id = kwargs.get('reference_id', None)
        
        def add_image_callback(result):
            if not result.error and len(result.body) > 0:
                self.img_result.append(tornado.escape.json_decode(result.body))
            else:
                self.img_result.append({})

            if len(self.img_result) == 2:
                callback_value = False
                try:
                    if not self.img_result[0]["error"] and not self.img_result[1]["error"]:
                        callback_value = True
                        callback_value = (self.img_result[0]["result"]["referenceId"],self.img_result[1]["result"]["referenceId"]) 
                except:
                    pass

                callback(callback_value)

        def image_data_callback(image_response):
            if not image_response.error:
                imfile = StringIO(image_response.body)
                image =  Image.open(imfile)
                srefid = reference_id if not reference_id else "still-" + reference_id
                self.add_image(video_id,image,atype='thumbnail', reference_id = reference_id, async_callback = add_image_callback)
                self.add_image(video_id,image,atype='videostill',reference_id = srefid, async_callback = add_image_callback)
            else:
                callback(False)

        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)
        http_client.fetch(req,image_data_callback)


    ##################################################################################

    def get_publisher_feed(self,command='find_all_videos',output='json',page_no=0,page_size=100,async_callback=None):
    
        '''Get videos after the signup date, Iterate until you hit video the publish date
        optimize with the latest video processed which is stored in the account
        '''

        data = {}
        data['command'] = command
        data['token'] = self.read_token
        data['media_delivery'] = 'http'
        data['output'] = output
        #data['video_fields'] = 'customFields,id,tags,FLVURL,thumbnailURL,videostillURL,publishedDate,name,videoFullLength' #creates api delay
        data['page_number'] = page_no 
        data['page_size'] = page_size
        data['sort_by'] = 'publish_date'
        data['get_item_count'] = "true"

        url = self.format_get(self.read_url,data)
        if async_callback:
            http_client = tornado.httpclient.AsyncHTTPClient()
            req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
            response = http_client.fetch(req,async_callback)
            return

        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        return response.body

    ''' process publisher feed for neon tags and generate brightcove thumbnail/still requests '''
    def process_publisher_feed(self,items,i_id):
        vids_to_process = [] 
        bc_json = supportServices.neondata.BrightcovePlatform.get_account(self.neon_api_key,i_id)
        bc = supportServices.neondata.BrightcovePlatform.create(bc_json)
        videos_processed = bc.get_videos() 
        if videos_processed is None:
            videos_processed = {} 
        
        #parse and get video ids to process
        '''
        - Get videos after a particular date
        - Check if they have already been queued up, else queue it 
        '''
        for item in items:
            to_process = False
            vid = str(item['id'])

            #Check if neon has processed the videos already 
            if vid not in videos_processed:
                thumb = item['thumbnailURL'] 
                still = item['videoStillURL']
                d_url = item['FLVURL']
                length = item['length']

                if thumb is None or still is None or length <0:
                    _log.info("key=process_publisher_feed msg=%s is a live feed" %vid)
                    continue

                if d_url is None:
                    _log.info("key=process_publisher_feed msg=flv url missing for %s" %vid)
                    continue

                resp = self.format_neon_api_request(vid,d_url,prev_thumbnail=still,request_type='topn',i_id=i_id)
                print "creating request for video [topn] ", vid
                if resp is not None and not resp.error:
                    #Update the videos in customer inbox
                    bc_json = supportServices.neondata.BrightcovePlatform.get_account(self.neon_api_key,i_id)
                    bc = supportServices.neondata.BrightcovePlatform.create(bc_json)
                    r = tornado.escape.json_decode(resp.body)
                    bc.videos[vid] = r['job_id']
                    bc.last_process_date = int(item['publishedDate']) / 1000
                    bc.save()

    def format_neon_api_request(self,id,video_download_url,prev_thumbnail=None,request_type='topn',i_id=None,title=None,callback=None):
        request_body = {}
    
        #brightcove tokens
        request_body["write_token"] = self.write_token
        request_body["read_token"] = self.read_token
        request_body["api_key"] = self.neon_api_key 
        request_body["video_id"] = str(id)
        request_body["video_title"] = str(id) if title is None else title 
        request_body["video_url"] = video_download_url
        if self.local:
            request_body["callback_url"] = "http://localhost:8081/testcallback"
        else:
            request_body["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"
        request_body["autosync"] = self.autosync
        request_body["topn"] = 1
        request_body["integration_id"] = i_id 

        if request_type == 'topn':
            client_url = self.neon_uri + "brightcove"
            request_body["brightcove"] =1
            request_body["publisher_id"] = self.publisher_id
            if prev_thumbnail is not None:
                request_body[properties.PREV_THUMBNAIL] = prev_thumbnail
        else:
            return
        
        body = tornado.escape.json_encode(request_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers = h,
                body = body, request_timeout = 30.0, connect_timeout = 10.0)
        
        #async
        if callback:
            http_client = tornado.httpclient.AsyncHTTPClient()
            http_client.fetch(req,callback) 
            return

        http_client = tornado.httpclient.HTTPClient()
        retries = 1
        for i in range(retries):
            try:
                response = http_client.fetch(req)
                #verify response 200 OK
                return response

            except tornado.httpclient.HTTPError, e:
                continue
        return

    '''
    Create Neon Brightcove API Requests
    '''
    def create_neon_api_requests(self,i_id,request_type='default'):
        
        #Get publisher feed
        items_to_process = []  
        done = False
        page_no = 0

        while not done: 
            count = 0
            response = self.get_publisher_feed(command='find_all_videos',page_no = page_no)
            json = tornado.escape.json_decode(response)
            page_no += 1
            try:
                items = json['items']
                total = json['total_count']
                psize = json['page_size']
                pno   = json['page_number']

            except Exception,e:
                print json
                return
        
            for item in items:
                pdate = int(item['publishedDate']) / 1000
                if pdate > self.last_publish_date:
                    items_to_process.append(item)
                    count += 1

            #if we have seen all items or if we have seen all the new videos since last pub date 
            if count < total or psize * (pno +1) > total:
                done = True

        if len(items_to_process) < 1 :
            return

        self.process_publisher_feed(items_to_process,i_id)
        return

    ''' Brightcove api request to get info about a videoid '''
    def find_video_by_id(self,video_id,find_vid_callback=None):
        url = 'http://api.brightcove.com/services/library?command=find_video_by_id&token='+ self.read_token + '&media_delivery=http&output=json&video_id=' + video_id
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        
        if find_vid_callback:
            http_client = tornado.httpclient.AsyncHTTPClient()
            http_client.fetch(req,find_vid_callback)
        else:
            http_client = tornado.httpclient.HTTPClient()
            response = http_client.fetch(req)
            return response.body

    '''
    Create neon api request for the particular video
    '''

    def create_video_request(self,video_id,i_id,create_callback):

        def get_vid_info(response):
            if not response.error and "error" not in response.body:
                data = tornado.escape.json_decode(response.body)
                v_url = data["FLVURL"]
                still = data['videoStillURL']
                vid = str(data["id"])
                self.format_neon_api_request(vid,v_url,still,request_type='topn',i_id=i_id,callback = create_callback)
            else:
                create_callback(False)

        self.find_video_by_id(video_id,get_vid_info)

    def create_request_by_video_id(self,video_id):
        
        url = 'http://api.brightcove.com/services/library?command=find_video_by_id&token='+ self.read_token +'&media_delivery=http&output=json&video_id=' + video_id + '&video_fields=FLVURL,id'
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)
        resp = tornado.escape.json_decode(response.body)
        print url
        print resp
        #self.format_neon_api_request(resp['id'] ,resp['FLVURL'])

    def async_get_n_videos(self,n,async_callback):
        self.get_publisher_feed(command='find_all_videos',page_size = n, async_callback = async_callback)
        return

    def create_brightcove_request_by_tag(self,i_id):
        
        url = 'http://api.brightcove.com/services/library?command=search_videos&token=' + self.read_token + '&media_delivery=http&output=json&sort_by=publish_date:DESC&any=tag:neon'
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = url, method = "GET", request_timeout = 60.0, connect_timeout = 10.0)
        response = http_client.fetch(req)

        #Get publisher feed
        items_to_process = []  
        done = False
        page_no = 0

        while not done: 
            count = 0
            #TODO Keep requesting pages of tagged videos to iterate through, for now just look at 1 page (100 vids)
            json = tornado.escape.json_decode(response.body)
            page_no += 1
            try:
                items = json['items']
                total = json['total_count']
                psize = json['page_size']
                pno   = json['page_number']

            except Exception,e:
                print json
                return
        
            for item in items:
                tags = item['tags']
                if "neon" in tags or "Neon" in tags:
                    items_to_process.append(item)
                    count += 1

            #if we have seen all items or if we have seen all the new videos since last pub date 
            #if count < total or psize * (pno +1) > total:
            #    done = True
            done = True  #temp hack !

        if len(items_to_process) < 1 :
            return

        self.process_publisher_feed(items_to_process,i_id)
        return


    #Verify Tokens and Create Neon requests
    def async_verify_token_and_create_requests(self,i_id,n,callback=None):

        @tornado.gen.engine
        def verify_brightcove_tokens(result):
            if not result.error and "error" not in result.body:
                bc_json = yield tornado.gen.Task(supportServices.neondata.BrightcovePlatform.get_account,self.neon_api_key,i_id)
                if not bc_json:
                    log.error("key=verify_brightcove_tokens msg=account not found %s"%i_id)
                    callback(None)
                    return

                bc = supportServices.neondata.BrightcovePlatform.create(bc_json)
                vitems = tornado.escape.json_decode(result.body)
                items = vitems['items']
                keys = []
                #create request for each video 
                for item in items:
                    vid = str(item['id'])                              
                    title = item['name']
                    video_download_url = item['FLVURL']
                    prev_thumbnail = item['videoStillURL'] #item['thumbnailURL']
                    keys.append("key"+vid)
                    self.format_neon_api_request(vid,video_download_url,prev_thumbnail,
                            'topn',i_id,title,
                            callback=(yield tornado.gen.Callback("key" + vid)))

                result = [] 
                responses = yield tornado.gen.WaitAll(keys)
                for response,item in zip(responses,items):
                    if not response.error:
                        vid = str(item['id'])
                        jid = tornado.escape.json_decode(response.body)
                        job_id = jid["job_id"]
                        item['job_id'] = job_id 
                        bc.videos[vid] = job_id 
                        result.append(item)
               
                print bc.key
                #Update the videos in customer inbox
                res = yield tornado.gen.Task(bc.save)
                if not res:
                    log.error("key=async_verify_token_and_create_requests msg=customer inbox not updated %s" %i_id)

                #send result back with job_id
                callback(result)
            else:
                log.error("key=async_verify_token_and_create_requests msg=brightcove api failed for %s" %i_id)
                callback(None)
        self.async_get_n_videos(5,verify_brightcove_tokens)

if __name__ == "__main__" :
    utils.neon.InitNeon()
    print 'test'
    #Test publisher feed with neon api key
    #bc = BrightcoveApi('a63728c09cda459c3caaa158f4adff49',read_token='cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..',write_token='vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..')
