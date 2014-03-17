'''
Brightcove API Interface class
'''

import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import datetime
from poster.encode import multipart_encode
import poster.encode
import properties
from PIL import Image
from StringIO import StringIO
import supportServices.neondata 
import time
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import tornado.escape
import urllib

from utils.http import RequestPool
import utils.http
from utils.imageutils import ImageUtils
import utils.logs
import utils.neon
_log = utils.logs.FileLogger("brighcove_api")

from utils.options import define, options
#define("local", default=1, help="create neon requests locally", type=int)
define('max_write_connections', default=1, type=int, 
       help='Maximum number of write connections to Brightcove')
define('max_read_connections', default=50, type=int, 
       help='Maximum number of read connections to Brightcove')
define('max_retries', default=5, type=int,
       help='Maximum number of retries when sending a Brightcove error')

class BrightcoveApi(object): 
    ''' Brighcove API Interface class
    All video ids used in the class refer to the Brightcove platform VIDEO ID
    '''

    write_connection = RequestPool(options.max_write_connections,
                                   options.max_retries)
    read_connection = RequestPool(options.max_read_connections,
                                  options.max_retries)
    
    def __init__(self, neon_api_key, publisher_id=0, read_token=None,
                 write_token=None, autosync=False, publish_date=None,
                 local=True, account_created=None):
        self.publisher_id = publisher_id
        self.neon_api_key = neon_api_key
        self.read_token = read_token
        self.write_token = write_token 
        self.read_url = "http://api.brightcove.com/services/library"
        self.write_url = "http://api.brightcove.com/services/post"
        self.autosync = autosync
        self.last_publish_date = publish_date if publish_date else time.time()
        self.local = local 
        if self.local:
            self.neon_uri = "http://localhost:8081/api/v1/submitvideo/"
        else:
            self.neon_uri = "http://thumbnails.neon-lab.com/api/v1/submitvideo/" 
        
        self.THUMB_SIZE = 120, 90
        self.STILL_SIZE = 480, 360
        self.account_created = account_created

    def update_still_width(self, sz):
        '''
        Set the still width
        Ignore the height as we scale the image based on aspect ratio of the 
        original image
        '''
        self.STILL_SIZE = (width, self.STILL_SIZE[1])

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
    
    def find_video_by_id(self, video_id, find_vid_callback=None):
        ''' Brightcove api request to get info about a videoid '''

        url = ('http://api.brightcove.com/services/library?command=find_video_by_id'
                    '&token=%s&media_delivery=http&output=json&' 
                    'video_id=%s' %(self.read_token, video_id)) 

        req = tornado.httpclient.HTTPRequest(url=url, method="GET", 
                request_timeout=60.0, connect_timeout=10.0)

        if not find_vid_callback:
            return BrightcoveApi.read_connection.send_request(req)
        else:
            BrightcoveApi.read_connection.send_request(req, find_vid_callback)

    def add_image(self, video_id, im=None, atype='thumbnail', callback=None,
                  **kwargs):
        '''
        Add Image brightcove api helper method
        : remote_url sets the url to 3rd party url 
        : image creates a new asset and the url is on brightcove servers
        
        #NOTE: When uploading an image with a reference ID that already
        #exists, the image is not
        updated. Although other metadata like displayName is updated.

        '''
        #help.brightcove.com/developer/docs/mediaapi/add_image.cfm
        
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
            image["displayName"] = str(self.publisher_id) + \
                    'neon-thumbnail-for-video-%s'%video_id
        else:
            image["type"] = "VIDEO_STILL"
            image["displayName"] = str(self.publisher_id) + \
                    'neon-video-still-for-video-%s'%video_id 
        
        if remote_url:
            image["remoteUrl"] = remote_url

        params["image"] = image
        outer["params"] = params
        outer["method"] = "add_image"

        body = tornado.escape.json_encode(outer)
        
        #use this keyword to identify if it is a neon image or not
        image_suffix = kwargs.get('image_suffix', '')
        image_fname = 'neonthumbnail%s-%s.jpg'%(image_suffix, video_id) 
        
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
            fileparam = poster.encode.MultipartParam(
                "filePath",
                value=image_data,
                filetype='image/jpeg',
                filename=image_fname)
            args = poster.encode.MultipartParam("JSONRPC", value=body)
            post_param.append(args)
            post_param.append(fileparam)
            datagen, headers = multipart_encode(post_param)
            body = "".join([data for data in datagen])
        
        #send request
        client_url = "http://api.brightcove.com/services/post"
        req = tornado.httpclient.HTTPRequest(url=client_url,
                                             method="POST",
                                             headers=headers, 
                                             body=body,
                                             request_timeout=60.0,
                                             connect_timeout=10.0)
            
        return BrightcoveApi.write_connection.send_request(req, callback)
    
    def update_thumbnail_and_videostill(self, video_id, image, ref_id, 
            frame_size=None): 
    
        ''' add thumbnail and videostill in to brightcove account.  used
            by neon client to update thumbnail, Image gets sent to the
            method call

            ref_id = tid
        '''
        
        #If url is passed, then set thumbnail using the remote url 
        #(not used currently)

        if isinstance(image, basestring):
            rt = self.add_image(video_id, remote_url=image, atype='thumbnail')
            rv = self.add_image(video_id, remote_url=image, atype='videostill')
        else:
            #Always save the Image with the aspect ratio of the video
            if frame_size is None:
                #resize to brightcove default size
                bcove_thumb = image.resize(self.THUMB_SIZE)
                bcove_still = image.resize(self.STILL_SIZE)
            else:
                bcove_thumb = ImageUtils.resize(image, im_w=self.THUMB_SIZE[0])
                bcove_still = ImageUtils.resize(image, im_w=self.STILL_SIZE[0])

            t_md5 = supportServices.neondata.ImageMD5Mapper(video_id,
                                                            bcove_thumb,
                                                            ref_id)
            s_md5 = supportServices.neondata.ImageMD5Mapper(video_id,
                                                            bcove_still,
                                                            ref_id)
            md5_objs = []
            md5_objs.append(t_md5); md5_objs.append(s_md5)
            res = supportServices.neondata.ImageMD5Mapper.save_all(md5_objs)
            if not res:
                _log.error('key=update_thumbnail msg=failed to' 
                            'save ImageMD5Mapper for %s' %video_id)
            
            rt = self.add_image(video_id,
                                bcove_thumb,
                                atype='thumbnail',
                                reference_id=ref_id)
            rv = self.add_image(video_id,
                                bcove_still,
                                atype='videostill',
                                reference_id='still-' + ref_id)
        
        tref_id = None ; vref_id = None
        #Get thumbnail name, referenceId params
        if rt and not rt.error:
            add_image_val = tornado.escape.json_decode(rt.body)
            tref_id = add_image_val["result"]["referenceId"]
        if rv and not rv.error:
            add_image_val = tornado.escape.json_decode(rv.body)
            vref_id = add_image_val["result"]["referenceId"]

        return ((rt is not None and rv is not None), tref_id, vref_id)

    def update_thumbnail_and_still_refid(self, video_id, refid, callback):
        '''
        update both thumbnail and video still  
        async only
        '''
        self.img_result = []
        def updated_image(result):
            if not result.error and len(result.body) > 0:
                self.img_result.append(tornado.escape.json_decode(result.body))
            else:
                self.img_result.append(None)

            if len(self.img_result) == 2:
                thumb = False
                still = False
                try:
                    for res in self.img_result:
                        if res and not res["error"]:
                            if res["result"]["type"] == 'THUMBNAIL':
                                thumb = res["result"]["referenceId"]
                            elif res["result"]["type"] == 'VIDEO_STILL':
                                still = res["result"]["referenceId"]
                except:
                    pass
                callback_value = (thumb, still)
                callback(callback_value)

            self.update_image_with_refid(video_id,
                                         refid,
                                         callback=updated_image)
            self.update_image_with_refid(video_id,
                                         'still-'+refid,
                                         callback=updated_image)


    def update_image_with_refid(self, video_id, refid, videostill=False,
                                callback=None):
        '''
        Update the thumbnail for a given video given the ReferenceID 
        of an existing image asset 
        '''

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

        req = tornado.httpclient.HTTPRequest(url=self.write_url,
                                             method="POST",
                                             headers=headers,
                                             body=body,
                                             request_timeout=60.0,
                                             connect_timeout=10.0)

        return BrightcoveApi.write_connection.send_request(req, callback)
        

    def enable_thumbnail_from_url(self, video_id, url, frame_size=None,
                                  reference_id=None):
        '''
        Enable a particular thumbnail in the brightcove account
        '''

        headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})
        req = tornado.httpclient.HTTPRequest(url=url,
                                             method="GET",
                                             headers=headers,
                                             request_timeout=60.0,
                                             connect_timeout=10.0)
        response = utils.http.send_request(req)
        imfile = StringIO(response.body)
        try:
            image =  Image.open(imfile)
        except Exception,e:
            _log.exception("Image format error %s" %e )

        #TODO: Resize to the aspect ratio of video; key by width
        thumbnail_id = reference_id
        if frame_size is None:
            #resize to brightcove default size
            bcove_thumb = image.resize(self.THUMB_SIZE)
            bcove_still = image.resize(self.STILL_SIZE)
        else:
            bcove_thumb = ImageUtils.resize(image, im_w=self.THUMB_SIZE[0])
            bcove_still = ImageUtils.resize(image, im_w=self.STILL_SIZE[0])

        t_md5 = supportServices.neondata.ImageMD5Mapper(video_id,
                                                        bcove_thumb,
                                                        thumbnail_id)
        s_md5 = supportServices.neondata.ImageMD5Mapper(video_id,
                                                        bcove_still,
                                                        thumbnail_id)
        md5_objs = []
        md5_objs.append(t_md5); md5_objs.append(s_md5)
        res = supportServices.neondata.ImageMD5Mapper.save_all(md5_objs)
        if not res:
            _log.error('key=update_thumbnail msg=failed to save ImageMD5Mapper for %s' %video_id)

        rt = self.add_image(video_id,image, atype='thumbnail',
                            reference_id=reference_id)
        rv = self.add_image(video_id,image, atype='videostill',
                            reference_id=reference_id if not reference_id else "still-" + reference_id)
       
        tref_id = None ; vref_id = None
        #Get thumbnail name, referenceId params
        if rt and not rt.error:
            add_image_val = tornado.escape.json_decode(rt.body)
            tref_id = add_image_val["result"]["referenceId"]
        if rv and not rv.error:
            add_image_val = tornado.escape.json_decode(rv.body)
            vref_id = add_image_val["result"]["referenceId"]

        return ((rt is not None and rv is not None),tref_id,vref_id)
    

    def async_enable_thumbnail_from_url(self, video_id, img_url, 
                                       thumbnail_id, frame_size=None,
                                       image_suffix="",
                                       callback=None):
        '''
        Enable thumbnail async
        '''

        self.img_result = []  
        reference_id = thumbnail_id
       
        def add_image_callback(result):
            if not result.error and len(result.body) > 0:
                self.img_result.append(tornado.escape.json_decode(result.body))
            else:
                self.img_result.append(None)

            if len(self.img_result) == 2:
                thumb = False
                still = False 
                try:
                    for res in self.img_result:
                        if res and not res["error"]:
                            if res["result"]["type"] == 'THUMBNAIL':
                                thumb = res["result"]["referenceId"]
                            elif res["result"]["type"] == 'VIDEO_STILL':
                                still = res["result"]["referenceId"]
                        else:
                            _log.error("key=async_update_thumbnail"
                                    " msg=brightcove api error for %s %s" 
                                    %(video_id, res["error"]))
                except:
                    pass

                callback_value = (thumb, still)
                callback(callback_value)

        @tornado.gen.engine
        def image_data_callback(image_response):
            if not image_response.error:
                imfile = StringIO(image_response.body)
                image =  Image.open(imfile)
                srefid = reference_id if not reference_id else "still-" + reference_id
                
                if frame_size is None:
                    #resize to brightcove default size
                    bcove_thumb = image.resize(self.THUMB_SIZE)
                    bcove_still = image.resize(self.STILL_SIZE)
                else:
                    bcove_thumb = ImageUtils.resize(image, im_w=self.THUMB_SIZE[0])
                    bcove_still = ImageUtils.resize(image, im_w=self.STILL_SIZE[0])

                t_md5 = supportServices.neondata.ImageMD5Mapper(video_id,
                                                                bcove_thumb,
                                                                thumbnail_id)
                s_md5 = supportServices.neondata.ImageMD5Mapper(video_id,
                                                                bcove_still,
                                                                thumbnail_id)
                md5_objs = []
                md5_objs.append(t_md5)
                md5_objs.append(s_md5)
                res = yield tornado.gen.Task(
                    supportServices.neondata.ImageMD5Mapper.save_all,
                    md5_objs)
                if not res:
                    _log.error('key=async_update_thumbnail' 
                        'msg=failed to save ImageMD5Mapper for %s' %thumbnail_id)
                
                #TODO : use generator task. Don't you dare. This code
                #is complicated enough as is
                self.add_image(video_id,
                               bcove_thumb,
                               atype='thumbnail', 
                               reference_id=reference_id,
                               image_suffix=image_suffix,
                               callback=add_image_callback)
                self.add_image(video_id,
                               bcove_still,
                               atype='videostill',
                               reference_id=srefid,
                               image_suffix=image_suffix,
                               callback=add_image_callback)
            else:
                _log.error('key=async_update_thumbnail' 
                        'msg=failed to download image for %s' %thumbnail_id)
                callback(None)

        req = tornado.httpclient.HTTPRequest(url = img_url,
                                             method = "GET",
                                             request_timeout = 60.0,
                                             connect_timeout = 5.0)
        utils.http.send_request(req, callback=image_data_callback)

    ################################################################################
    # Feed Processors
    ################################################################################

    def get_video_url_to_download(self, b_json_item, frame_width=None):
        '''
        Return a video url to download from a brightcove json item 
        
        if frame_width is specified, get the closest one  
        '''

        video_urls = {}
        d_url  = b_json_item['FLVURL']
        renditions = b_json_item['renditions']
        for rend in renditions:
            f_width = rend["frameWidth"]
            url = rend["url"]
            video_urls[f_width] = url 
        
        if frame_width:
            if video_urls.has_key(frame_width):
                return video_urls[frame_width] 
            closest_f_width = min(video_urls.keys(),
                                key=lambda x:abs(x-frame_width))
            return video_urls[closest_f_width]
        else:
            return d_url

    def get_publisher_feed(self, command='find_all_videos', output='json',
                           page_no=0, page_size=100, callback=None):
    
        '''Get videos after the signup date, Iterate until you hit 
           video the publish date.
        
        Optimize with the latest video processed which is stored in the account

        NOTE: using customFields or specifying video_fields creates API delays
        '''

        data = {}
        data['command'] = command
        data['token'] = self.read_token
        data['media_delivery'] = 'http'
        data['output'] = output
        data['page_number'] = page_no 
        data['page_size'] = page_size
        data['sort_by'] = 'publish_date'
        data['get_item_count'] = "true"

        url = self.format_get(self.read_url, data)
        req = tornado.httpclient.HTTPRequest(url=url,
                                             method="GET",
                                             request_timeout=60.0,
                                             connect_timeout=10.0)
        return BrightcoveApi.read_connection.send_request(req, callback)

    def process_publisher_feed(self, items, i_id):
        ''' process publisher feed for neon tags and generate brightcove
        thumbnail/still requests '''
        
        vids_to_process = [] 
        bc = supportServices.neondata.BrightcovePlatform.get_account(
            self.neon_api_key, i_id)
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
            vid   = str(item['id'])
            title = item['name']

            #Check if neon has processed the videos already 
            if vid not in videos_processed:
                thumb  = item['thumbnailURL'] 
                still  = item['videoStillURL']
                d_url  = item['FLVURL']
                length = item['length']

                #if frame width is set, select the resolution to process
                if bc.rendition_frame_width:
                    d_url = self.get_video_url_to_download(item, 
                                    bc.rendition_frame_width)

                if still is None:
                    still = thumb

                if thumb is None or still is None or length <0:
                    _log.info("key=process_publisher_feed" 
                                " msg=%s is a live feed" %vid)
                    continue

                if d_url is None:
                    _log.info("key=process_publisher_feed"
                                " msg=flv url missing for %s" %vid)
                    continue

                resp = self.format_neon_api_request(vid,
                                                    d_url,
                                                    prev_thumbnail=still,
                                                    request_type='topn',
                                                    i_id=i_id,
                                                    title=title)
                _log.info("creating request for video [topn] %s" % vid)
                if resp is not None and not resp.error:
                    #Update the videos in customer inbox
                    bc = supportServices.neondata.BrightcovePlatform.get_account(
                            self.neon_api_key, i_id)
                    r = tornado.escape.json_decode(resp.body)
                    bc.videos[vid] = r['job_id']
                    #publishedDate may be null, if video is unscheduled
                    bc.last_process_date = int(item['publishedDate']) /1000 if item['publishedDate'] else None
                    bc.save()

            else:
                #Sync the changes in brightcove account to NeonDB
                #TODO: Sync not just the latest 100 videos
                job_id = bc.videos[vid]
                req_data = supportServices.neondata.NeonApiRequest.get_request(
                        self.neon_api_key,job_id)
                vid_request = supportServices.neondata.NeonApiRequest.create(
                        req_data)
                pub_date = int(item['publishedDate']) if item['publishedDate'] else None
                vid_request.publish_date = pub_date 
                vid_request.video_title = title
                vid_request.save()

    def sync_neondb_with_brightcovedb(self, items, i_id):
        ''' sync neondb with brightcove metadata '''        
        bc = supportServices.neondata.BrightcovePlatform.get_account(
            self.neon_api_key, i_id)
        videos_processed = bc.get_videos() 
        if videos_processed is None:
            videos_processed = [] 
        
        for item in items:
            vid = str(item['id'])
            title = item['name']
            if vid in videos_processed:
                job_id = bc.videos[vid]
                req_data = supportServices.neondata.NeonApiRequest.get_request(
                        self.neon_api_key,job_id)
                vid_request = supportServices.neondata.NeonApiRequest.create(
                        req_data)
                pub_date = int(item['publishedDate']) if item['publishedDate'] else None
                vid_request.publish_date = pub_date 
                vid_request.video_title = title
                vid_request.save()

    def format_neon_api_request(self, id, video_download_url, 
                                prev_thumbnail=None, request_type='topn',
                                i_id=None, title=None, callback=None):
        ''' Format and submit reuqest to neon thumbnail api '''

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
                _log.debug("key=format_neon_api_request "
                        " msg=brightcove prev thumbnail not set")
            request_body[properties.PREV_THUMBNAIL] = prev_thumbnail
        else:
            return
        
        body = tornado.escape.json_encode(request_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        req = tornado.httpclient.HTTPRequest(url = client_url,
                                             method = "POST",
                                             headers = h,
                                             body = body,
                                             request_timeout = 30.0,
                                             connect_timeout = 10.0)

        response = utils.http.send_request(req, ntries=3, callback=callback)
        if response and response.error:
            _log.error(('key=format_neon_api_request '
                        'msg=Error sending Neon API request: %s')
                        % response.error)

        return response

    def create_neon_api_requests(self, i_id, request_type='default'):
        ''' Create Neon Brightcove API Requests '''
        
        #Get publisher feed
        items_to_process = []  
        items_processed = [] #videos that Neon has processed
        done = False
        page_no = 0

        while not done: 
            count = 0
            response = self.get_publisher_feed(command='find_all_videos',
                                               page_no = page_no)
            if response.error:
                break
            json = tornado.escape.json_decode(response.body)
            page_no += 1
            try:
                items = json['items']
                total = json['total_count']
                psize = json['page_size']
                pno   = json['page_number']

            except Exception,e:
                _log.exception('key=create_neon_api_requests msg=%s' % e)
                return
            
            for item in items:
                pdate = int(item['publishedDate']) / 1000
                check_date = self.account_created if \
                        self.account_created is not None else self.last_publish_date
                if pdate > check_date:
                    items_to_process.append(item)
                    count += 1
                else:
                    items_processed.append(item)

            #if we have seen all items or if we have seen all the new
            #videos since last pub date
            if count < total or psize * (pno +1) > total:
                done = True

        #Sync video metadata of processed videos
        self.sync_neondb_with_brightcovedb(items_processed, i_id)

        if len(items_to_process) < 1 :
            return

        self.process_publisher_feed(items_to_process, i_id)
        return


    ## TODO: potentially replace find_all_videos by find_modified_videos ? 

    def create_requests_unscheduled_videos(self, i_id, page_no=0, page_size=25):
   
        '''
        ## Find videos scheduled in the future and process them
        ## Use this method as an additional call to check for videos 
        ## that are scheduled in the future
        # http://docs.brightcove.com/en/video-cloud/media/reference.html
        '''

        data = {}
        data['command'] = "find_modified_videos" 
        data['token'] = self.read_token
        data['media_delivery'] = 'http'
        data['output'] = 'json' 
        data['page_number'] = page_no 
        data['page_size'] = page_size
        #data['sort_by'] = 'modified_date'
        #data['sort_order'] = 'DESC'
        data['get_item_count'] = "true"
        data['video_fields'] = "id,name,length,endDate,startDate,creationDate,publishedDate,lastModifiedDate,thumbnailURL,videoStillURL,FLVURL"
        data["from_date"] = 21492000
        data["filter"] = "UNSCHEDULED,INACTIVE"

        url = self.format_get(self.read_url, data)
        req = tornado.httpclient.HTTPRequest(url = url,
                                             method = "GET",
                                             request_timeout = 60.0
                                             )
        response = BrightcoveApi.read_connection.send_request(req)
        items = tornado.escape.json_decode(response.body)

        #Logic to determine videos that may be not scheduled to run yet
        #publishedDate is null, and creationDate is recent (last 24 hrs) 
        #publishedDate can be null for inactive videos too

        #NOTE: There may be videos which are marked as not to use
        # by renaming the title  
        
        #check if requests for these videos have been created
        items_to_process = []

        for item in items['items']:
            if item['publishedDate'] is None or len(item['publishedDate']) ==0:
                items_to_process.append(item)
                _log.debug("key=create_requests_unscheduled_videos" 
                        " msg=creating request for vid %s" %item['id'])
        self.process_publisher_feed(items_to_process,i_id)

    def sync_individual_video_metadata(self,i_id):
        '''
        Sync the video metadata from brightcove to neon db

        #TODO: make this more efficient
        '''
        bc = supportServices.neondata.BrightcovePlatform.get_account(
            self.neon_api_key, i_id)
        videos_processed = bc.get_videos() 

        for vid in videos_processed:
            response = self.find_video_by_id(vid,i_id)
            item = tornado.escape.json_decode(response.body)
            title = item['name']
            job_id = bc.videos[vid]
            req_data = supportServices.neondata.NeonApiRequest.get_request(
                    self.neon_api_key,job_id)
            vid_request = supportServices.neondata.NeonApiRequest.create(
                    req_data)
            pub_date = int(item['publishedDate']) if item['publishedDate'] else None
            vid_request.publish_date = pub_date 
            vid_request.title = title
            #TODO: May be even save current thumbnail
            vid_request.save()

    ############## NEON API INTERFACE ########### 


    def create_video_request(self, video_id, i_id, create_callback):
        ''' Create neon api request for the particular video '''

        def get_vid_info(response):
            ''' vid info callback '''
            if not response.error and "error" not in response.body:
                data = tornado.escape.json_decode(response.body)
                v_url = data["FLVURL"]
                still = data['videoStillURL']
                vid = str(data["id"])
                title = data["name"]
                self.format_neon_api_request(vid,
                                             v_url,
                                             still,
                                             request_type='topn',
                                             i_id=i_id,
                                             title=title,
                                             callback = create_callback)
            else:
                create_callback(response)

        self.find_video_by_id(video_id, get_vid_info)

    def create_request_by_video_id(self, video_id, i_id):
        ''' create thumbnail api request given a video id '''

        url = 'http://api.brightcove.com/services/library?command=find_video_by_id' \
                '&token=%s&media_delivery=http&output=json&video_id=%s' %(self.read_token,video_id) 
        req = tornado.httpclient.HTTPRequest(url = url,
                                             method = "GET",
                                             request_timeout = 60.0,
                                             connect_timeout = 10.0)
        response = BrightcoveApi.read_connection.send_request(req)
        if response.error:
            _log.error('key=create_request_by_video_id msg=Unable to get %s'
                       % url)
            return
            
        resp = tornado.escape.json_decode(response.body)
        still = resp['videoStillURL']
        response = self.format_neon_api_request(resp['id'],
                                                resp['FLVURL'], 
                                                still, 
                                                request_type='topn', 
                                                i_id=i_id,
                                                title=resp['name'])
        if not response or response.error:
            _log.error(('key=create_request_by_video_id '
                        'msg=Unable to create request for video %s')
                        % video_id)
            return
        
        jid = tornado.escape.json_decode(response.body)
        job_id = jid["job_id"]
        bc = supportServices.neondata.BrightcovePlatform.get_account(
            self.neon_api_key, i_id)
        bc.videos[video_id] = job_id
        bc.save()

    def async_get_n_videos(self, n, callback):
        ''' async get n vids '''
        self.get_publisher_feed(command='find_all_videos',
                                page_size = n,
                                callback = callback)
        return
    
    def get_n_videos(self, n):
        ''' sync get n vids '''
        return self.get_publisher_feed(command='find_all_videos',
                                       page_size = n)

    def create_brightcove_request_by_tag(self,i_id):
        ''' create requests from brightcove tags ''' 
        url = 'http://api.brightcove.com/services/library?command=' \
                'search_videos&token=%s&media_delivery=http&output=json' \
                '&sort_by=publish_date:DESC&any=tag:neon' %self.read_token
        req = tornado.httpclient.HTTPRequest(url = url,
                                             method = "GET",
                                             request_timeout = 60.0,
                                             connect_timeout = 10.0)
        
        response = BrightcoveApi.read_connection.send_request(req)
        if response.error:
            return

        #Get publisher feed
        items_to_process = []  
        done = False
        page_no = 0

        while not done: 
            count = 0
            #TODO: Keep requesting pages of tagged videos to iterate through, 
            #for now just look at 1 page (100 vids)
            
            json = tornado.escape.json_decode(response.body)
            page_no += 1
            try:
                items = json['items']
                total = json['total_count']
                psize = json['page_size']
                pno   = json['page_number']

            except Exception,e:
                _log.exception("key=create_brightcove_request_by_tag msg=json error")
                return
        
            for item in items:
                tags = item['tags']
                if "neon" in tags or "Neon" in tags:
                    items_to_process.append(item)
                    count += 1

            #if we have seen all items or
            #if we have seen all the new videos since last pub date 
            #if count < total or psize * (pno +1) > total:
            #    done = True
            done = True  #temp hack !

        if len(items_to_process) < 1 :
            return

        self.process_publisher_feed(items_to_process, i_id)
        return

    #### Verify Read token and create Requests during signup #####

    def verify_token_and_create_requests(self, i_id, n):
        '''
        Initial call when the brightcove account gets created
        verify the read token and create neon api requests
        #Sync version
        '''
        result = self.get_publisher_feed(command='find_all_videos',
                                       page_size = n) #get n videos

        if result and not result.error:
            bc = supportServices.neondata.BrightcovePlatform.get_account(
                self.neon_api_key, i_id)
            if not bc:
                _log.error("key=verify_brightcove_tokens" 
                            " msg=account not found %s"%i_id)
                return
            vitems = tornado.escape.json_decode(result.body)
            items = vitems['items']
            keys = []
            #create request for each video 
            result = [] 
            for item in items:
                vid = str(item['id'])                              
                title = item['name']
                video_download_url = item['FLVURL']
                
                #TODO: Try searching for highest renedition to get quality thumb

                #NOTE: If a video doesn't have a thumbnail 
                #Perhaps a signle renedition video or a live feed
                if not item.has_key('videoStillURL'):
                    continue

                prev_thumbnail = item['videoStillURL'] #item['thumbnailURL']
                response = self.format_neon_api_request(vid,
                                                        video_download_url,
                                                        prev_thumbnail,
                                                        'topn',
                                                        i_id,
                                                        title)
                if not response.error:
                    vid = str(item['id'])
                    jid = tornado.escape.json_decode(response.body)
                    job_id = jid["job_id"]
                    item['job_id'] = job_id 
                    bc.videos[vid] = job_id 
                    result.append(item)
            #Update the videos in customer inbox
            res = bc.save()
            if not res:
                _log.error("key=async_verify_token_and_create_requests" 
                        " msg=customer inbox not updated %s" %i_id)
            return result

    def async_verify_token_and_create_requests(self, i_id, n, callback=None):
        '''#Verify Tokens and Create Neon requests'''

        @tornado.gen.engine
        def verify_brightcove_tokens(result):
            if not result.error:
                bc = yield tornado.gen.Task(
                    supportServices.neondata.BrightcovePlatform.get_account,
                    self.neon_api_key, i_id)
                if not bc:
                    _log.error("key=verify_brightcove_tokens "
                                " msg=account not found %s"%i_id)
                    callback(None)
                    return
                
                
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
                    self.format_neon_api_request(vid,
                                                 video_download_url,
                                                 prev_thumbnail,
                                                 'topn',
                                                 i_id,
                                                 title,
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
               
                #Update the videos in customer inbox
                res = yield tornado.gen.Task(bc.save)
                if not res:
                    _log.error("key=async_verify_token_and_create_requests"
                            " msg=customer inbox not updated %s" %i_id)

                #send result back with job_id
                callback(result)
            else:
                _log.error("key=async_verify_token_and_create_requests" 
                        " msg=brightcove api failed for %s" %i_id)
                callback(None)
        self.async_get_n_videos(5, verify_brightcove_tokens)
    
    #################################################################
    # Brightcove Thumbnail check methods 
    #################################################################
    
    def get_image(self, image_url, callback=None):
        '''Returns the raw image stream from a given URL.'''
        def process_response(response):

            if response.error:
                _log.error('key=get_image msg=Error getting %s' % image_url)
                raise response.error
            return response.body
            
        req = tornado.httpclient.HTTPRequest(url=image_url,
                                             method="GET",
                                             request_timeout=60.0,
                                             connect_timeout=10.0)

        #TODO: Not a brightcove api call, switch to normal http fetch
        if callback:
            #callback = lambda x: callback(process_response(x))#investigate recursion
            return BrightcoveApi.read_connection.send_request(req, callback)
        return process_response(
            BrightcoveApi.read_connection.send_request(req))


    def async_check_thumbnail(self, video_id, callback):
        '''Method to check the current thumbnail for a video on brightcove
        
        Used by AB Test to keep track of any uploaded image to
        brightcove, its MD5 & URL

        Returns True if the current thumbnail is on brighcove.
        
        '''
        thumb_url = None
        still_url = None

        @tornado.gen.engine
        def check_image_md5_db(thumb_url, thumbnail, callback):
            ''' check if imagemd5 already is in the DB'''
            if thumbnail:
                #valid image
                t_md5 = supportServices.neondata.ThumbnailMD5.generate(
                    thumbnail)
                tid = yield tornado.gen.Task(
                    supportServices.neondata.ImageMD5Mapper.get_tid,
                    video_id,
                    t_md5)
                if tid:
                    url_mapper = yield tornado.gen.Task(
                        supportServices.neondata.ThumbnailURLMapper.get_id,
                        thumb_url)
                    if not url_mapper:
                        #entry for the given thumbnail url doesn't exist, Save it !
                        mapper = supportServices.neondata.ThumbnailURLMapper(
                            thumb_url, tid)
                        res = yield tornado.gen.Task(mapper.save)
                        if res:
                            callback(True)
                            return
                        else:
                            _log.error("key=async_check_thumbnail" 
                                    " msg=failed to save ThumbnailURLMapper url " 
                                    " %s tid %s" %(thumb_url, tid))
                    else:
                        _log.info("key=async_check_thumbnail"
                                " msg=entry for url %s exists already"%thumb_url)
                        callback(False) #indicate thumb not saved
                        return
                else:
                    #Should this entry be saved, perhaps its a new image 
                    #that the customer may have overriden ? 
                    #-- should this be saved in db as chosen=true?
                    i_vid = supportServices.neondata.InternalVideoID.generate(
                                self.neon_api_key, video_id) 
                    tid = supportServices.neondata.ThumbnailID.generate(
                                thumbnail, i_vid)
                    created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    urls = [thumb_url]
                    td = supportServices.neondata.ThumbnailMetaData(
                                tid, urls, created, 480, 360, "brightcove", 0, 0, rank=0)
                    tm = supportServices.neondata.ThumbnailIDMapper(
                                tid, i_vid, td.to_dict())
                    ret = supportServices.neondata.ThumbnailIDMapper.save_all([tm])
                    if ret:
                        tmap = supportServices.neondata.ThumbnailURLMapper(thumb_url, tid)
                        ret = supportServices.neondata.ThumbnailURLMapper.save_all([tmap])
                    _log.info("key=async_check_thumbnail"
                            " msg=saved mapping for image url" 
                            " %s md5 %s"%(thumb_url, t_md5)) 
                    callback(False)
                    return
            else:
                _log.error("key=async_check_thumbnail" 
                        " msg=thumbnail not downloaded %s" %thumb_url)
            
            callback(None)

        @tornado.gen.engine
        def result_callback(response):
            '''
            Make a call to Brightcove, Download the image and process the result
            '''
            if not response.error:
                resp = tornado.escape.json_decode(response.body)
                try:
                    thumb_url = resp['thumbnailURL'].split('?')[0]
                    still_url = resp['videoStillURL'].split('?')[0]
                except:
                    _log.error("key=async_check_thumbnail "
                            " msg=thumbnail url not found for %s"%video_id)
                    callback(None)
                    return
                thumbnail = yield tornado.gen.Task(self.get_image, thumb_url)
                videostill = yield tornado.gen.Task(self.get_image, still_url)
                
                tret = yield tornado.gen.Task(
                        check_image_md5_db,thumb_url, thumbnail.body)
                stret = yield tornado.gen.Task(
                        check_image_md5_db,still_url, videostill.body)
                callback(tret and stret)
            else:
                callback(None)

        url = 'http://api.brightcove.com/services/library?' \
                'command=find_video_by_id&token=%s&media_delivery=http'\
                '&output=json&video_id=%s' %(self.read_token, video_id)

        req = tornado.httpclient.HTTPRequest(url=url,
                                             method="GET", 
                                             request_timeout=60.0,
                                             connect_timeout=10.0)
        response = BrightcoveApi.read_connection.send_request(req,
                                                              result_callback)

    

if __name__ == "__main__" :
    utils.neon.InitNeon()
