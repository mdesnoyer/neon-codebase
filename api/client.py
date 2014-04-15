#!/usr/bin/env python
'''
Video Processing client, no longer a multiprocessing client

VideoClient class has a run loop which uses httpdownload object to
download the video file after dequeueing job from video-server

Sync and Async options are available to download the video file in httpdownload

If Async, on the streaming_callback video processing is done partially.

ProcessVideo class has all the methods to deal with video processing and
post processing

'''

USAGE = '%prog [options] <model_file> <local>'

import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import cv2
import datetime
import json
import hashlib
import model
import properties
import Queue
import random
import signal
import struct
import tempfile
import tornado.web
import tornado.gen
import tornado.escape
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import time
import urllib
import numpy as np
from PIL import Image

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from StringIO import StringIO

import brightcove_api
import ooyala_api 
from supportServices import neondata

import logging
_log = logging.getLogger(__name__)

import utils.neon
from utils.http import RequestPool
from utils import statemon

#Monitoring
statemon.define('processed_video', int)
statemon.define('processing_error', int)
statemon.define('dequeue_error', int)
statemon.define('save_tmdata_error', int)
statemon.define('save_vmdata_error', int)

# ======== Parameters  =======================#
from utils.options import define, options
define('local', type=int, default=0,
      help='If set, use the localproperties file for config')
define('model_file', default=None, help='File that contains the model')
define('debug', default=0, type=int, help='If true, runs in debug mode')
define('profile', default=0, type=int, help='If true, runs in debug mode')
define('sync', default=0, type=int,
       help='If true, runs http client in async mode')

# ======== API String constants  =======================#

MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 2
INTERNAL_PROCESSING_ERROR = "internal error"

#=============== Global Handlers =======================#
def sig_handler(sig, frame):
    ''' signal handler '''
    _log.debug('Caught signal: ' + str(sig) )
    try:
        vc.kill_received = True
    except Exception, e:
        sys.exit(0)


###########################################################################
# Global Helper functions
###########################################################################

def callback_response_builder(job_id, vid, data, 
                            thumbnails, client_url, error=None):
        '''
        build callback response
        '''
        response_body = {}
        response_body["job_id"] = job_id 
        response_body["video_id"] = vid 
        response_body["data"] = data
        response_body["thumbnails"] = thumbnails
        response_body["timestamp"] = str(time.time())
        response_body["error"] = error 

        #CREATE POST REQUEST
        body = tornado.escape.json_encode(response_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        cb_response_request = tornado.httpclient.HTTPRequest(
                                url=client_url, 
                                method="POST",
                                headers=h,body=body, 
                                request_timeout=60.0, 
                                connect_timeout=10.0)
        return cb_response_request 

def notification_response_builder(a_id, i_id, video_json, 
                                event="processing_complete"):
        '''
        Notification to rails end point
        '''
        r = {}

        notification_url = 'http://www.neon-lab.com/api/accounts/%s/events'%a_id
        r["api_key"] = properties.NOTIFICATION_API_KEY
        r["video"] = video_json
        r["event"] = event
        body = urllib.urlencode(r)
        nt_response_request = tornado.httpclient.HTTPRequest(
                                url=notification_url, 
                                method = "POST",
                                body=body, 
                                request_timeout=60.0, 
                                connect_timeout=10.0)
        return nt_response_request
    
def host_images_s3(api_key, video_id, img_and_scores, base_filename, 
            model_version=0, ttype=neondata.ThumbnailType.NEON):
    ''' 
    Host images on s3 which is available publicly 
    @input
    img_and_scores: list of tuples (image objects, score)

    @return
    (thumbnails, s3_urls) : tuple of list(thumbnail metadata), s3 urls
    '''
  
    thumbnails = [] 
    s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
    s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
    s3bucket = s3conn.get_bucket(s3bucket_name)
    
    fname_prefix = 'neon'
    fmt = 'jpeg'
    s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
    s3_urls = []
    
    i_vid = neondata.InternalVideoID.generate(api_key, video_id)

    #upload the images to s3
    rank = 0 
    for image, score in img_and_scores:
        #image = Image.fromarray(imdata)
        keyname = base_filename + "/" + fname_prefix + str(rank) + "." + fmt
        s3fname = s3_url_prefix + "/%s/%s%s.jpeg" %(base_filename, fname_prefix, rank)
        tdata = save_thumbnail_to_s3_and_metadata(
                                    i_vid, image, score, 
                                    s3bucket, keyname,
                                    s3fname, ttype, rank, model_version)
        thumbnails.append(tdata)
        rank = rank+1
        s3_urls.append(s3fname)

    return (thumbnails, s3_urls)

def save_thumbnail_to_s3_and_metadata(i_vid, image, score, s3bucket,
                keyname, s3fname, ttype, rank=0, model_version=0):
    '''
    Save a thumbnail to s3 and its metadata in the DB

    @return thumbnail metadata object
    '''

    fmt = 'jpeg'
    filestream = StringIO()
    image.save(filestream, fmt, quality=90) 
    filestream.seek(0)
    imgdata = filestream.read()
    k = s3bucket.new_key(keyname)
    k.set_contents_from_string(imgdata,{"Content-Type":"image/jpeg"})
    s3bucket.set_acl('public-read', keyname)
    
    urls = []
    tid = neondata.ThumbnailID.generate(imgdata, i_vid)

    #If tid already exists, then skip saving metadata
    if neondata.ThumbnailMetadata.get(tid) is not None:
        _log.warn('Already have thumbnail id: %s' % tid)
        return

    urls.append(s3fname)
    created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    width   = image.size[0]
    height  = image.size[1] 

    #populate thumbnails
    tdata = neondata.ThumbnailMetadata(tid, i_vid, urls, created, width, height,
                              ttype, score, model_version, rank=rank)
    tdata.update_phash(image)
    return tdata

###########################################################################
# Process Video File
####################################################################################

class VideoProcessor(object):
    ''' 
    Download the video
    Process the video
    Finalize according to the API request
    '''

    retry_codes = [403, 500, 502, 503, 504]
    http_request_pool = RequestPool(3, 3) 

    def __init__(self, params, model, model_version):
        '''
        @input
        params: dict of request
        model: model obj
        '''

        self.timeout = 300000.0 #long running tasks ## -- is this necessary ???
        self.job_params = params
        self.video_url = self.job_params[properties.VIDEO_DOWNLOAD_URL]
        vsuffix = self.video_url.split('/')[-1]  #get the video file extension
        self.tempfile = tempfile.NamedTemporaryFile(
                                    suffix='_%s'%vsuffix, delete=False)
        self.headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})

        self.base_filename = self.job_params[properties.API_KEY] + "/" + \
                self.job_params[properties.REQUEST_UUID_KEY]  
        
        self.error = None # Error message is filled as str in this variable
        self.requeue_url = properties.BASE_SERVER_URL + "/requeue"

        self.video_metadata = {} 
        self.video_metadata['codec_name'] = None
        self.video_metadata["process_time"] = str(time.time())
        self.video_metadata['duration'] = None
        self.video_metadata['framerate'] = None
        self.video_metadata['bitrate'] = None
        self.video_metadata['frame_size'] = None

        self.debug_timestamps = {}
    
        #Video vars
        self.model = model
        self.model_version = model_version
        self.frames = []
        self.data_map = {} #frameNo -> (score, image_rgb)
        self.attr_map = {}
        self.valence_scores = [[], []] #x,y 
        self.timecodes = {}
        self.thumbnails = [] 
        self.sec_to_extract = 1 
        self.sec_to_extract_offset = 1

    def download_video_file(self):
        '''
        Download the video file 
        '''

        req = tornado.httpclient.HTTPRequest(url=self.video_url, 
                                            headers=self.headers,
                                            request_timeout=self.timeout)
        http_client = tornado.httpclient.HTTPClient()
        response = http_client.fetch(req)
        if response.error:
            #Do we need to handle timeout for long running http calls ? 
            self.error = "http error downloading file"
            return

        if not response.headers.has_key('Content-Length'):
            self.error = "url not a video file"
            return

        #Write contents to the temp file 
        self.tempfile.write(response.body)
     
    def start(self):
        '''
        Actual work done here
        '''

        self.download_video_file()
       
        #Error downloading the file
        if self.error:
            self.finalize_request()
            return

        #Process the video
        n_thumbs = 10 # Dummy
        if self.job_params.has_key(properties.TOP_THUMBNAILS):
            n_thumbs = 2*int(self.job_params[properties.TOP_THUMBNAILS])
        self.process_video(self.tempfile.name, n_thumbs=n_thumbs)

        #gather stats
        end_time = time.time()
        total_request_time = \
                end_time - float(self.video_metadata[properties.VIDEO_PROCESS_TIME])
        self.video_metadata[properties.VIDEO_PROCESS_TIME] = str(total_request_time)
        self.video_metadata[properties.JOB_SUBMIT_TIME] = \
                        self.job_params[properties.JOB_SUBMIT_TIME]
        self.video_metadata[properties.JOB_END_TIME] = str(end_time)
        
        #Delete the temp video file which was downloaded
        self.tempfile.flush()
        self.tempfile.close()
        if os.path.exists(self.tempfile.name):
            os.unlink(self.tempfile.name)
       
        #finalize request, if success send client and notification response
        #error cases are handled in finalize_request
        self.finalize_request()

    def process_video(self, video_file, n_thumbs=1):
        ''' process all the frames from the partial video downloaded '''
        start_process = time.time()
        try:
            mov = cv2.VideoCapture(video_file)
            if self.video_metadata['codec_name'] is None:
                fps = mov.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
                self.video_metadata['codec_name'] = \
                  struct.pack('I', mov.get(cv2.cv.CV_CAP_PROP_FOURCC))
                self.video_metadata['duration'] = \
                  fps * mov.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)
                self.video_metadata['framerate'] = fps
                self.video_metadata['bitrate'] = None # Can't get this in OpenCV
                width = mov.get(cv2.cv.CV_CAP_PROP_FRAME_WIDTH)
                height = mov.get(cv2.cv.CV_CAP_PROP_FRAME_HEIGHT)
                self.video_metadata['frame_size'] = (width, height)
                self.video_size = None # Can't get this in OpenCV
        except Exception, e:
            _log.error("key=process_video worker " 
                        " msg=%s "  % (e.message))
            return False

        duration = self.video_metadata['duration']

        if duration <= 1e-3:
            _log.error("key=process_video worker[%s] "
                       "msg=video %s has no length. skipping." %
                       (self.pid, video_file))
            #return #NOTE: cv2 doesn't return this reliably, investigate why? 

        #If a really long video, then increase the sampling rate
        if duration > 1800:
            self.sec_to_extract_offset = 2 

        # >1 hr
        if duration > 3600:
            self.sec_to_extract_offset = 4
        results, self.sec_to_extract = \
                self.model.choose_thumbnails(mov,
                                       n=n_thumbs,
                                       sample_step=self.sec_to_extract_offset,
                                       start_time=self.sec_to_extract)

        for image, score, frame_no, timecode, attribute in results:
            if attribute is not None and attribute == '':
                self.valence_scores[0].append(timecode)
                self.valence_scores[1].append(score)
                self.timecodes[frame_no] = timecode
                self.data_map[frame_no] = (score, image[:, :, ::-1])
                self.attr_map[frame_no] = attribute
        
        end_process = time.time()
        _log.info("key=process_video msg=debug " 
                    "time_processing=%s" %(end_process - start_process))
        return True

    def get_center_frame(self, video_file):
        '''approximation of brightcove logic 
         #Note: Its unclear the exact nature of brighcove thumbnailing,
         the images are close but this is not the exact frame
        '''
        try:
            mov = cv2.VideoCapture(video_file)
            duration = mov.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)
            mov.set(cv2.cv.CV_CAP_PROP_POS_FRAMES, int(duration / 2))
            read_sucess, image = mov.read()
            if read_sucess:
                return image[:,:,::-1]
            else:
                _log.error('key=get_center_frame '
                           'msg=Error reading middle frame of video %s'
                           % video_file)
        except Exception,e:
            _log.debug("key=get_center_frame msg=%s"%e)

    def valence_score(self, image):
        ''' valence of pil.image '''

        im_array = np.array(image)
        im = im_array[:, :, ::-1]
        #TODO: Test flann global data corruption
        #score, attr = self.model.score(im)
        score = 0
        return str(score)
    
    ### Thumbnail methods ### 
        
    def get_topn_thumbnails(self, n):
        ''' topn '''
        res = self.top_thumbnails_per_interval(nthumbnails=n)
        #return at least 1 thumb, 
        #if self.data_map may be empty if there was an internal error
        if len(res) == 0 and self.data_map != {}:
            res = self.data_map.items()[0]
        return res

    def top_thumbnails_per_interval(self, nthumbnails =1, interval =0):
        ''' Get top n thumbnails per interval - used by topn '''

        data_slice = self.data_map.items()
        if interval != 0:
            data_slice = self.data_map.items()  #TODO slice the interval

        #Randomize if the scores are the same, generate hash to use as
        #the secondary key
        secondary_sorted_list = sorted(data_slice, 
                                       key=lambda tup: hashlib.md5(str(tup[0])).hexdigest(),
                                       reverse=True)
        result = sorted(secondary_sorted_list,
                        key=lambda tup: tup[1][0],
                        reverse=True)
      
        if len(result) < nthumbnails: 
            nthumbnails = min(len(result), nthumbnails)
            return result[:nthumbnails]
        else:
            # Fiter duplicates
            # TODO(mdesnoyer): Specify the number of thumbnails to return.
            filtered = self.model.filter_duplicates(
                [(x[1][1], x) for x in result],
                n=None)
            filt_result = [x[1] for x in filtered]

            # TODO(mdesnoyer): Remove this hack. This forces the
            # thumbnails to be at least 5s apart in the video in
            # order to avoid duplicates.
            spread_result = []
            filt_result.reverse()
            while len(spread_result) < nthumbnails and len(filt_result) > 0:
                too_close = False
                cur_entry = filt_result.pop()
                cur_frameno = cur_entry[0]
                for chosen in spread_result:
                    if abs(cur_frameno - chosen[0]) < 150:
                        too_close = True
                        break
                if not too_close:
                    spread_result.append(cur_entry)
                    
            return spread_result

    def finalize_request(self):
        '''
        Finalize request after video has been processed
        '''
        
        api_key = self.job_params[properties.API_KEY]  
        job_id  = self.job_params[properties.REQUEST_UUID_KEY]
        video_id = self.job_params[properties.VIDEO_ID]
        callback_url = self.job_params[properties.CALLBACK_URL]
        request_type = self.job_params['request_type']
        api_method = self.job_params['api_method'] 
        api_param =  self.job_params['api_param']
      
        if self.error:
            #If Internal error, requeue and dont send response to client yet
            #Send response to client that job failed due to the last reason
            #And Log the response we send to the client
            statemon.state.increment('processing_error')
            if not self.requeue_job():
                cb_request = callback_response_builder(job_id, video_id, [], 
                            [], callback_url, error=self.error)
                self.send_client_callback_response(cb_request)
                #update error state
                api_request = neondata.NeonApiRequest.get(api_key, job_id)
                api_request.state = neondata.RequestState.INT_ERROR
                api_request.save()
                return
           
            #requeued
            api_request = neondata.NeonApiRequest.get(api_key, job_id)
            api_request.state = neondata.RequestState.REQUEUED
            api_request.save()
            return

        # Prcocessed video successfully
        statemon.state.increment('processed_video') 

        # API Specific client response
        MAX_T = 6
        
        ''' Neon API section 
        '''
        if  api_method == properties.TOP_THUMBNAILS:
            n = topn = int(api_param)
            '''
            Always save MAX_T thumbnails for any request and host them on s3 
            '''
            if topn < MAX_T:
                n = MAX_T

            res = self.get_topn_thumbnails(n)
            img_score_list = [] 
            for tup in res:
               score, imgdata = tup[1]
               image = Image.fromarray(imgdata)
               img_score_list.append((image, score))
            ranked_frames = [x[0] for x in res]
            data = ranked_frames[:topn]
            
            #host top MAX_T images on s3
            thumbnails, s3_urls = host_images_s3(api_key, video_id, img_score_list, 
                                            self.base_filename, model_version=0, 
                                            ttype=neondata.ThumbnailType.NEON)
            self.thumbnails.extend(thumbnails)
            cr_request = callback_response_builder(job_id, video_id, data, 
                            s3_urls[:topn], callback_url, error=self.error)
            self.send_client_callback_response(cr_request)
            VideoProcessor.http_request_pool.send_request(cr_request)

            ## Neon section
            if request_type == "neon":
                #Save response that was created for the callback to client 
                self.finalize_api_request(cr_request.body, "neon")
                return

            ## Brightcove secion 
            elif request_type == "brightcove":
                self.finalize_api_request(cr_request.body, "brightcove")
                self.send_notifiction_response()

            ## Ooyala section
            elif request_type == "ooyala":
                self.finalize_api_request(cr_request.body, "ooyala")

            else:
                _log.error("request type not supported")
        else:
            _log.error("request param not supported")

    
    def finalize_api_request(self, result, request_type):
        '''
        - host neon thumbs and also save bcove previous thumbnail in s3
        - Get Account settings and replace default thumbnail if enabled 
        - update request object with the thumbnails
        '''
        
        api_key = self.job_params[properties.API_KEY]  
        job_id = self.job_params[properties.REQUEST_UUID_KEY]
        video_id = self.job_params[properties.VIDEO_ID]
        title = self.job_params[properties.VIDEO_TITLE]
        i_id = 0
        if request_type != "neon":
            i_id = self.job_params[properties.INTEGRATION_ID]
        
        api_request = neondata.NeonApiRequest.get(api_key, job_id)
        api_request.response = tornado.escape.json_decode(result)
        api_request.publish_date = time.time() *1000.0 #ms

        #previous thumbnail
        if hasattr(api_request, "previous_thumbnail"):
            self.save_previous_thumbnail()
        else:
            _log.info("key=finalize_api_request "
                       " msg=no previous thumbnail for %s %s" %(api_key, video_id))
       
        #autosync    
        if hasattr(api_request, "autosync"):
            if api_request.autosync:
                fno = api_request.response["data"][0]
                img = Image.fromarray(self.data_map[fno][1])
                self.autosync(api_request, img)

        api_request.state = neondata.RequestState.FINISHED 
        if api_request.save():
            # Save the Thumbnail URL and ID to Mapper DB
            if not self.save_thumbnail_metadata(request_type, i_id):
                _log.error("save_thumbnail_metadata failed")
                statemon.state.increment('save_tmdata_error')

            if not self.save_video_metadata():
                _log.error("save_video_metadata")
                statemon.state.increment('save_vmdata_error')
        else:
            _log.error("key=finalize_api_request msg=failed to save request")
    
    def save_previous_thumbnail(self, api_request):
        '''
        Save the previous thumbnail / default thumbnail in the request
        '''

        if not api_request.previous_thumbnail:
            return 

        p_url = api_request.previous_thumbnail.split('?')[0]
        video_id = self.job_params[properties.VIDEO_ID]

        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url=p_url,
                                            method="GET",
                                            request_timeout=60.0,
                                            connect_timeout=10.0)

        response = http_client.fetch(req)
        if not response.error:
            imgdata = response.body
            image = Image.open(StringIO(imgdata))
            score = self.valence_score(image) 
            s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
            s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
            s3bucket = s3conn.get_bucket(s3bucket_name)
            s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
            keyname =  self.base_filename + "/brightcove.jpeg" 
            s3fname = s3_url_prefix + "/" + keyname
            if api_request.request_type == "brightcove":
                ttype = neondata.ThumbnailType.BRIGHTCOVE
            if api_request.request_type == "ooyala":
                ttype = neondata.ThumbnailType.OOYALA
            tdata = save_thumbnail_to_s3_and_metadata(
                                            video_id, image, 
                                            score, s3bucket,
                                            keyname, s3fname, 
                                            ttype, rank=1)
            self.thumbnails.append(tdata)
            api_request.previous_thumbnail = s3fname
            return tdata
        else:
            _log.error("key=save_previous_thumbnail msg=failed to download image")

    def autosync(self, api_request, image):
        '''
        Autosync Thumbnail
        '''
        api_key = self.job_params[properties.API_KEY]  
        video_id = self.job_params[properties.VIDEO_ID]  
        i_id = self.job_params[properties.INTEGRATION_ID] 
        frame_size = self.video_metadata['frame_size']
        tid = self.thumbnails[0].key #TOP Neon thumbnail TID
        if api_request.request_type == "brightcove":
        
            rtoken = self.job_params[properties.BCOVE_READ_TOKEN]
            wtoken = self.job_params[properties.BCOVE_WRITE_TOKEN]
            pid = self.job_params[properties.PUBLISHER_ID]
            bcove = brightcove_api.BrightcoveApi(
                        neon_api_key=api_key,
                        publisher_id=pid,
                        read_token=rtoken,
                        write_token=wtoken)
            
            ret = bcove.update_thumbnail_and_videostill(
                                video_id, image, tid, frame_size)

            if ret[0]:
                #NOTE: By default Neon rank 1 is always uploaded
                self.thumbnails[0].chosen = True 
            else:
                _log.error("autosync failed for video %s" % video_id)
   
        elif api_request.request_type == "ooyala":
            oo_api_key = self.job_params["oo_api_key"]
            oo_secret_key = self.job_params["oo_secret_key"]
            oo = ooyala_api.OoyalaAPI(oo_api_key, oo_secret_key)
            update_result = oo.update_thumbnail(video_id,
                                               image,
                                               tid,
                                               frame_size)
            if update_result:
                self.thumbnails[0].chosen = True 
            else:
                _log.error("autosync failed for ooyala video %s" % video_id)

    def save_thumbnail_metadata(self, platform, i_id):
        '''Save the Thumbnail URL and ID to Mapper DB '''

        api_key = self.job_params[properties.API_KEY] 
        vid = self.job_params[properties.VIDEO_ID]
        job_id = self.job_params[properties.REQUEST_UUID_KEY]
        i_vid = neondata.InternalVideoID.generate(api_key, vid)

        thumbnail_url_mapper_list = []
        if len(self.thumbnails) > 0:
            for thumb in self.thumbnails:
                tid = thumb.key
                for t_url in thumb.urls:
                    uitem = neondata.ThumbnailURLMapper(t_url, tid)
                    thumbnail_url_mapper_list.append(uitem)

            retid = neondata.ThumbnailMetadata.save_all(self.thumbnails)
            returl = neondata.ThumbnailURLMapper.save_all(thumbnail_url_mapper_list)
            
            return (retid and returl)
    
    def save_video_metadata(self):
        '''
        Method to save video metadata in to the videoDB
        contains list of thumbnail ids 
        '''
        
        api_key = self.job_params[properties.API_KEY] 
        vid = self.job_params[properties.VIDEO_ID]
        i_vid = neondata.InternalVideoID.generate(api_key, vid)
        i_id = self.job_params[properties.INTEGRATION_ID] if self.job_params.has_key(properties.INTEGRATION_ID) else 0 
        job_id = self.job_params[properties.REQUEST_UUID_KEY]
        duration = self.video_metadata["duration"]
        video_valence = "%.4f" %float(np.mean(self.valence_scores[1])) 
        url = self.job_params[properties.VIDEO_DOWNLOAD_URL]
        model_version = self.model_version 
        frame_size = self.video_metadata['frame_size']

        tids = [x.key for x in self.thumbnails]

        vmdata = neondata.VideoMetadata(i_vid, tids, job_id, url, duration,
                    video_valence, model_version, i_id, frame_size)
        return vmdata.save()

    def requeue_job(self):
        """ Requeue the api request on failure 
            @job_params : dict of api request
        """

        if self.job_params.has_key("requeue_count"):
            rc = self.job_params["requeue_count"]
            if rc > 3:
                _log.error("key=requeue_job msg=exceeded max requeue job_id=%s"
                                % self.job_params["job_id"])
                return False

            self.job_params["requeue_count"] = rc + 1
        else:
            self.job_params["requeue_count"] = 1

        body = tornado.escape.json_encode(self.job_params)
        requeue_request = tornado.httpclient.HTTPRequest(
                            url=self.requeue_url, 
                            method="POST",
                            body=body, 
                            request_timeout=60.0, 
                            connect_timeout=10.0)
        response = VideoProcessor.http_request_pool.send_request(requeue_request)
        if response.error:
            return False
        return True
 
    def send_client_callback_response(self, request):
        '''
        Send client response
        '''
        response = VideoProcessor.http_request_pool.send_request(request)
        if response.error:
            return False
        return True

    def send_notifiction_response(self):
        '''
        Send Notification to endpoint
        '''

        api_key = self.job_params[properties.API_KEY] 
        video_id = self.job_params[properties.VIDEO_ID]
        title = self.job_params[properties.VIDEO_TITLE]
        ba = neondata.BrightcovePlatform.get_account(api_key, i_id) 
        thumbs = [t.to_dict_for_video_response() for t in self.thumbnails]
        vr = neondata.VideoResponse(video_id,
                                    "processed",
                                    "brightcove",
                                    i_id,
                                    title,
                                    None, 
                                    None,
                                    0, #current_tid
                                    thumbs)
        request = notification_response_builder(ba.account_id, i_id, vr.to_json()) 
        response = VideoProcessor.http_request_pool.send_request(request)

class VideoClient(object):
   
    '''
    Video Client processor
    '''
    def __init__(self, model_file, debug=False, sync=False):
        self.model_file = model_file
        self.SLEEP_INTERVAL = 10
        self.kill_received = False
        self.dequeue_url = properties.BASE_SERVER_URL + "/dequeue"
        self.requeue_url = properties.BASE_SERVER_URL + "/requeue"
        self.state = "start"
        self.model_version = -1
        self.model = None
        self.debug = debug
        self.sync = sync
        self.pid = os.getpid()
        self.http_pool = RequestPool(2, 3)

    def dequeue_job(self):
        ''' Blocking http call to global queue to dequeue work
            Change state to PROCESSING after dequeue
        '''
        
        http_client = tornado.httpclient.AsyncHTTPClient()
        headers = {'X-Neon-Auth' : properties.NEON_AUTH} 
        result = None
        try:
            req = tornado.httpclient.HTTPRequest(
                                            url=self.dequeue_url,
                                            method="GET",
                                            headers=headers,
                                            request_timeout=60.0,
                                            connect_timeout=10.0)

            response = self.http_pool.send_request(req)
            result = response.body
        except tornado.httpclient.HTTPError, e:
            _log.error("Dequeue Error %s" %e)
            statemon.state.increment('dequeue_error')
             
        if result is not None and result != "{}":
            try:
                job_params = tornado.escape.json_decode(result)
                #Change Job State
                api_key = job_params[properties.API_KEY]
                job_id  = job_params[properties.REQUEST_UUID_KEY]
                api_request = neondata.NeonApiRequest.get(api_key, job_id)
                if api_request.state == neondata.RequestState.SUBMIT:
                    api_request.state = neondata.RequestState.PROCESSING
                    api_request.model_version = self.model_version
                    api_request.save()
                _log.info("key=worker [%s] msg=processing request %s "
                          %(self.pid,job_params[properties.REQUEST_UUID_KEY]))
            except Exception,e:
                _log.error("key=worker [%s] msg=db error %s" %(self.pid,e.message))
        return result
    

    ##### Model Methods #####

    def load_model(self):
        ''' load model '''
        parts = self.model_file.split('/')[-1]
        version = parts.split('.model')[0]
        self.model_version = version
        _log.info('Loading model from %s version %s'
                  % (self.model_file,self.model_version))
        self.model = model.load_model(self.model_file)
        if not self.model:
            _log.error('Error loading the Model')
            exit(1)

    def run(self):
        ''' run/start method '''
        _log.info("starting worker [%s] " %(self.pid))
        while not self.kill_received:
            self.do_work()

    def do_work(self):   
        ''' do actual work here'''
        try:
            job = self.dequeue_job()
            if not job or job == "{}": #string match
                raise Queue.Empty
           
            self.load_model()
            jparams = json.loads(job)
            vprocessor = VideoProcessor(jparams, self.model, self.model_version)
            vprocessor.start()

        except Queue.Empty:
            _log.debug("Q,Empty")
            time.sleep(self.SLEEP_INTERVAL * random.random())
        
        except Exception,e:
            _log.exception("key=worker [%s] "
                    " msg=exception %s" %(self.pid, e.message))
            time.sleep(self.SLEEP_INTERVAL)

def main():
    utils.neon.InitNeon()
    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    
    if options.local:
        _log.info("Running locally")
        properties.BASE_SERVER_URL = properties.LOCALHOST_URL

    global vc
    vc = VideoClient(options.model_file,
                     options.debug, options.sync)
    vc.run()

if __name__ == "__main__":
    main()
