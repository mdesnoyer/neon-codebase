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

import brightcove_api
import cv2
import datetime
import ffvideo
import hashlib
import json
import model
import numpy as np
import ooyala_api 
import properties
from PIL import Image
import psutil
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
import urllib2
import utils.neon
import utils.pycvutils
import utils.http
import utils.sqsmanager
from utils import statemon

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.exception import SQSError
from utils.imageutils import PILImageUtils
from StringIO import StringIO
from supportServices import neondata

import utils.s3

import logging
_log = logging.getLogger(__name__)

random.seed(232315)

#Monitoring
statemon.define('processed_video', int)
statemon.define('processing_error', int)
statemon.define('dequeue_error', int)
statemon.define('save_tmdata_error', int)
statemon.define('save_vmdata_error', int)
statemon.define('customer_callback_schedule_error', int)
statemon.define('no_thumbs', int)

# ======== Parameters  =======================#
from utils.options import define, options
define('local', type=int, default=0,
      help='If set, use the localproperties file for config')
define('model_file', default=None, help='File that contains the model')
define('debug', default=0, type=int, help='If true, runs in debug mode')
define('profile', default=0, type=int, help='If true, runs in debug mode')
define('sync', default=0, type=int,
       help='If true, runs http client in async mode')
define('video_server', default="http://localhost:8081", type=str, help="video server")
define('serving_url_format',
        default="http://i%s.neon-images.com/v1/client/%s/neonvid_%s", type=str)

# ======== API String constants  =======================#
INTERNAL_PROCESSING_ERROR = "internal error"

###########################################################################
# Global Helper functions
###########################################################################

def callback_response_builder(job_id, vid, data, 
                              thumbnails, client_url,
                              serving_url=None, error=None):
        '''
        build callback response that will be sent to the callback url
        which was specified when the request was created

        @data: frame numbers in rank order
        @thumbnails: s3 urls of the recommended images
        @client_url: the callback url to send the data to

        '''

        response_body = {}
        response_body["job_id"] = job_id 
        response_body["video_id"] = vid 
        response_body["data"] = data
        response_body["thumbnails"] = thumbnails
        response_body["timestamp"] = str(time.time())
        response_body["serving_url"] = serving_url 
        response_body["error"] = error 

        #CREATE POST REQUEST
        body = tornado.escape.json_encode(response_body)
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        cb_response_request = tornado.httpclient.HTTPRequest(
                                url=client_url, 
                                method="POST",
                                headers=h, 
                                body=body, 
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
                                method="POST",
                                body=body, 
                                request_timeout=60.0, 
                                connect_timeout=10.0)
        return nt_response_request
    
def host_images_s3(vmdata, api_key, img_and_scores, base_filename, 
                   model_version=0, ttype=neondata.ThumbnailType.NEON,
                   cdn_metadata=None):
    ''' 
    Host images on s3 which is available publicly 
    @input
    img_and_scores: list of tuples (image objects, score)
    
    @cdn_metadata: CDNMetadata object with customer cdn access info

    @return
    (thumbnails, s3_urls) : tuple of list(thumbnail metadata), s3 urls
    '''
  
    thumbnails = [] 
    s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
    s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
    s3bucket = s3conn.get_bucket(s3bucket_name)
    
    fname_prefix = 'neon'
    if ttype == neondata.ThumbnailType.CENTERFRAME:
        fname_prefix = 'centerframe'

    fmt = 'jpeg'
    s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
    s3_urls = []
    
    i_vid = vmdata.key 

    #upload the images to s3
    rank = 0 
    for image, score in img_and_scores:
        keyname = base_filename + "/" + fname_prefix + str(rank) + "." + fmt
        s3fname = s3_url_prefix + "/%s/%s%s.jpeg" % (base_filename, fname_prefix, rank)
        #for center frame skip rank
        if ttype == neondata.ThumbnailType.CENTERFRAME:
            keyname = base_filename + "/" + fname_prefix + "." + fmt
            s3fname = s3_url_prefix + "/%s/%s.jpeg" % (base_filename,
                                                        fname_prefix)

        tdata = vmdata.save_thumbnail_to_s3_and_store_metadata(image, score, 
                                                                keyname,
                                                                s3fname,
                                                                ttype, 
                                                                rank, 
                                                                model_version,
                                                                cdn_metadata)
        thumbnails.append(tdata)
        rank = rank+1
        s3_urls.append(s3fname)

    return (thumbnails, s3_urls)

###########################################################################
# Process Video File
###########################################################################

class VideoProcessor(object):
    ''' 
    Download the video
    Process the video
    Finalize according to the API request
    '''

    retry_codes = [403, 500, 502, 503, 504]

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
                                    suffix='_%s'%vsuffix, delete=True)
        self.headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})

        self.base_filename = self.job_params[properties.API_KEY] + "/" + \
                self.job_params[properties.REQUEST_UUID_KEY]  
        
        self.error = None # Error message is filled as str in this variable
        self.requeue_url = options.video_server + "/requeue"

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
        self.center_frame = None
        self.sec_to_extract = 1 
        self.sec_to_extract_offset = 1

    def download_video_file_urllib2(self):
        '''
        Download file using urllib2
        Tornado throws errors in bizzare cases
        '''

        req = urllib2.Request(self.video_url, headers=self.headers)
        res = urllib2.urlopen(req)
        data = res.read()
        self.tempfile.write(data)
        return

    def download_video_file(self):
        '''
        Download the video file 
        '''

        req = tornado.httpclient.HTTPRequest(url=self.video_url, 
                                            headers=self.headers,
                                            request_timeout=self.timeout)
        http_client = tornado.httpclient.HTTPClient()
        
        try:
            response = http_client.fetch(req)
        except Exception, e:
            #Enable fallback is needed for tornado errors, explore a single solution
            try:
                self.download_video_file_urllib2()
                return
            except urllib2.HTTPError, e:
                self.error = "http error conencting to url"
                return

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
       
        # Error downloading the file
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
       
        #finalize request, if success send client and notification response
        #error cases are handled in finalize_request
        self.finalize_request()

    def process_video(self, video_file, n_thumbs=1):
        ''' process all the frames from the partial video downloaded '''
        start_process = time.time()

        try:
            # OpenCV doesn't return metadata reliably, so use ffvideo
            # to get that information.
            fmov = ffvideo.VideoStream(video_file)
            self.video_metadata['codec_name'] = fmov.codec_name
            self.video_metadata['duration'] = fmov.duration
            self.video_metadata['framerate'] = fmov.framerate
            self.video_metadata['bitrate'] = fmov.bitrate
            self.video_metadata['frame_size'] = fmov.frame_size
            self.video_size = fmov.duration * fmov.bitrate / 8 # in bytes
        except Exception, e:
            _log.error("key=process_video msg=FFVIDEO error %s" % e)
            self.error = "processing_error"
            return False

        #Try to open the video file using openCV
        try:
            mov = cv2.VideoCapture(video_file)
        except Exception, e:
            _log.error("key=process_video worker " 
                        " msg=%s "  % e)
            self.error = "processing_error"
            return False

        duration = self.video_metadata['duration']

        if duration <= 1e-3:
            _log.error("key=process_video worker "
                       "msg=video %s has no length. skipping." %
                       (video_file)) 

        #If a really long video, then increase the sampling rate
        if duration > 1800:
            self.sec_to_extract_offset = 2 

        # >1 hr
        if duration > 3600:
            self.sec_to_extract_offset = 4

        try:
            results, self.sec_to_extract = \
              self.model.choose_thumbnails(
                  mov,
                  n=n_thumbs,
                  sample_step=self.sec_to_extract_offset,
                  start_time=self.sec_to_extract)
        except model.VideoReadError:
            _log.error("Error using OpenCV to read video. Trying ffvideo")
            results, self.sec_to_extract = \
              self.model.ffvideo_choose_thumbnails(
                  fmov,
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

        # Get the center frame of the video
        self.center_frame = self.get_center_frame(video_file)
        return True

    def get_center_frame(self, video_file, nframes=None):
        '''approximation of brightcove logic 
         #Note: Its unclear the exact nature of brighcove thumbnailing,
         the images are close but this is not the exact frame
        '''
        
        try:
            mov = cv2.VideoCapture(video_file)
            if nframes is None:
                nframes = mov.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)
            read_sucess, image = utils.pycvutils.seek_video(mov, int(nframes /
                2))
            if read_sucess:
                #Now grab the frame
                read_sucess, image = mov.read()
                if read_sucess:
                    return utils.pycvutils.to_pil(image)
            _log.error('key=get_center_frame '
                        'msg=Error reading middle frame of video %s'
                        % video_file)
        except Exception,e:
            _log.debug("key=get_center_frame msg=%s" % e)

    def valence_score(self, image):
        ''' valence of pil.image '''

        im_array = np.array(image)
        im = im_array[:, :, ::-1]
        #TODO(Sunil): Test flann global data corruption
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
        
        # get api request object
        api_request = neondata.NeonApiRequest.get(api_key, job_id)
      
        if self.error:
            #If Internal error, requeue and dont send response to client yet
            #Send response to client that job failed due to the last reason
            #And Log the response we send to the client
            statemon.state.increment('processing_error')
            
            #TODO(Sunil): Re-enable Requeuing, its not been helpful so far
            # Cron Requeue is more helpful

            api_request = neondata.NeonApiRequest.get(api_key, job_id)
            api_request.state = neondata.RequestState.INT_ERROR
            api_request.save()
            return

        # Prcocessed video successfully
        statemon.state.increment('processed_video') 

        # API Specific client response
        MAX_T = 6
        reprocess = False 
        if api_request.state == neondata.RequestState.REPROCESS:
            reprocess = True 

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
           
            # Create the video metadata object
            vmdata = self.save_video_metadata(reprocess)
            if not vmdata:
                _log.error("save_video_metadata failed to save")
                statemon.state.increment('save_vmdata_error')
                # if can't save vmdata, probably a DB error
                api_request.state = neondata.RequestState.INT_ERROR
                api_request.save()
                return

            # CDN Metadata
            cdn_metadata = None
            if api_request.request_type == "neon":
                np = neondata.NeonPlatform.get_account(api_key)
                cdn_metadata = np.cdn_metadata
            else:
                #TODO(Sunil): Implement for other platforms as we move forward
                pass

            #host top MAX_T images on s3
            thumbnails, s3_urls = host_images_s3(vmdata, api_key, img_score_list, 
                                            self.base_filename, model_version=0, 
                                            ttype=neondata.ThumbnailType.NEON,
                                            cdn_metadata=cdn_metadata)
            self.thumbnails.extend(thumbnails)
            # TODO(Sunil): If no thumbs, should the state be indicated
            # differently & not be serving enabled ?  
            if len(self.thumbnails) < 1:
                statemon.state.increment('no_thumbs')
                _log.error("no thumbnails extracted for video %s url %s"\
                        % (vmdata.key, vmdata.url))

            #host Center Frame on s3
            if self.center_frame is not None:
                cthumbnail, s3_url = host_images_s3(vmdata, api_key, [(self.center_frame, None)], 
                                            self.base_filename, model_version=0, 
                                            ttype=neondata.ThumbnailType.CENTERFRAME,
                                            cdn_metadata=cdn_metadata)
                self.thumbnails.extend(cthumbnail)
            else:
                _log.error("centerframe was None for %s" % video_id)

            # Save videometadata 
            if reprocess == False:
                if not vmdata.save():
                    _log.error("failed to save vmdata for %s" % vmdata.key)
                    statemon.state.increment('save_vmdata_error')
                    # What do we do if we have a DB Failure ?

            # Generate the Serving URL
            # TODO(Sunil): Pass TAI as part of the request?
            serving_url = None
            subdomain_index = random.randint(1, 4)
            na = neondata.NeonUserAccount.get_account(api_key)
            if na:
                neon_publisher_id = na.tracker_account_id  
                serving_url = options.serving_url_format % (subdomain_index,
                               neon_publisher_id, video_id)

            cr_request = callback_response_builder(job_id, video_id, data, 
                                                   s3_urls[:topn], callback_url,
                                                   serving_url,
                                                   error=self.error)

            # TODO(Sunil): Do we need to send the callback again? 
            # On reprocess, the serving URL doesn't change
            if reprocess == False:
                self.send_client_callback_response(video_id, cr_request)
            
            if request_type in ["neon", "brightcove", "ooyala"]:
                self.finalize_api_request(cr_request.body, request_type,
                        reprocess, vmdata)

                # SAVE VMDATA atomically
                def _modify_vmdata_atomically(video_obj):
                    video_obj.frame_size = vmdata.frame_size
                    video_obj.thumbnail_ids = vmdata.thumbnail_ids
                    video_obj.video_valence = vmdata.video_valence
                    video_obj.model_version = vmdata.model_version
            
                if reprocess:
                    neondata.VideoMetadata.modify(vmdata.key, _modify_vmdata_atomically)

                ## Notification enabled for brightcove only 
                if request_type == "brightcove":
                    self.send_notifiction_response()
            else:
                _log.error("request type not supported")
        else:
            _log.error("request param not supported")
    
    def finalize_api_request(self, result, request_type, reprocess=False,
                            vmdata=None):
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

        # Save the previous thumbnail to VMData
        if hasattr(api_request, "previous_thumbnail"):
            tdata = self.save_previous_thumbnail(api_request)
            # If reprocess, save to the vmdata passed
            if reprocess and vmdata is not None:
                vmdata.thumbnail_ids.append(tdata.key)

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
            if not self.save_thumbnail_metadata(request_type, i_id, reprocess):
                _log.error("save_thumbnail_metadata failed")
                statemon.state.increment('save_tmdata_error')
        else:
            
            # Need to reprocess the request
            _log.error("key=finalize_api_request msg=failed to save request %s"
                    % api_request.key)
    
    def save_previous_thumbnail(self, api_request):
        '''
        Save the previous thumbnail / default thumbnail in the request
        '''

        if not api_request.previous_thumbnail:
            return 

        p_url = api_request.previous_thumbnail.split('?')[0]
        api_key = self.job_params[properties.API_KEY]  
        video_id = self.job_params[properties.VIDEO_ID]
        i_vid = neondata.InternalVideoID.generate(api_key, video_id)

        score = 0 #no score for previous thumbnails
        s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        if api_request.request_type == "brightcove":
            keyname =  self.base_filename + "/brightcove.jpeg" 
            ttype = neondata.ThumbnailType.BRIGHTCOVE
        elif api_request.request_type == "ooyala":
            keyname =  self.base_filename + "/ooyala.jpeg" 
            ttype = neondata.ThumbnailType.OOYALA
        else:
            ttype = neondata.ThumbnailType.DEFAULT
            keyname =  self.base_filename + "/default.jpeg" 

        s3fname = s3_url_prefix + "/" + keyname

        #get vmdata
        vmdata = neondata.VideoMetadata.get(i_vid)

        #get neon pub id
        tdata = vmdata.download_and_add_thumbnail(p_url, keyname, s3fname, 
                                                  ttype, rank=1)
        if tdata:
            self.thumbnails.append(tdata)
            if not vmdata.save():
                _log.error("key=save_previous_thumbnail msg=failed to save videometadata")
                #TODO: requeue the video ? 

        api_request.previous_thumbnail = s3fname
        return tdata

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

    def save_thumbnail_metadata(self, platform, i_id, reprocess=False):
        '''Save the Thumbnail URL and ID to Mapper DB '''

        api_key = self.job_params[properties.API_KEY] 
        vid = self.job_params[properties.VIDEO_ID]
        job_id = self.job_params[properties.REQUEST_UUID_KEY]
        i_vid = neondata.InternalVideoID.generate(api_key, vid)

        thumbnail_url_mapper_list = []
        tids = [thumb.key for thumb in self.thumbnails]

        if len(self.thumbnails) > 0:
            for thumb in self.thumbnails:
                tid = thumb.key
                for t_url in thumb.urls:
                    uitem = neondata.ThumbnailURLMapper(t_url, tid)
                    thumbnail_url_mapper_list.append(uitem)

            # Merge specific fields if its being reprocesed
            def _atomically_merge_tmdata(t_objs):
                for thumb in self.thumbnails:
                    t_obj = t_objs[thumb.key]
                    if t_obj is not None:
                        thumb.chosen = t_obj.chosen
                        thumb.enabled = t_obj.enabled
                        thumb.serving_frac = t_obj.serving_frac
                    t_objs[thumb.key] = thumb

            if reprocess:
                retid = neondata.ThumbnailMetadata.modify_many(tids,
                    _atomically_merge_tmdata)
            else:
                retid = neondata.ThumbnailMetadata.save_all(self.thumbnails)
            
            returl = neondata.ThumbnailURLMapper.save_all(thumbnail_url_mapper_list)
            
            return (retid and returl)
    
    def save_video_metadata(self, reprocess=False):
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
        video_valence = "%.4f" % float(np.mean(self.valence_scores[1])) 
        url = self.job_params[properties.VIDEO_DOWNLOAD_URL]
        model_version = self.model_version 
        frame_size = self.video_metadata['frame_size']

        tids = [x.key for x in self.thumbnails]
        vmdata = neondata.VideoMetadata(i_vid, tids, job_id, url, duration,
                    video_valence, model_version, i_id, frame_size)
        
        # If reprocess mode, return the unsaved video metadata obj, We'll
        # atomically save this at the end
        if reprocess:   
            return vmdata 

        if vmdata.save():
            return vmdata

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
        response = utils.http.send_request(requeue_request)
        if response.error:
            return False
        return True
 
    def send_client_callback_response(self, video_id, request):
        '''
        Schedule client response to be sent to the client
        '''
        # Check if url in request object is empty, if so just return True

        if request.url is None or request.url == "null":
            return True

        '''
        # NOTE: Disabling SQS Callback since IGN doesn't need it

        # Schedule the callback in SQS
        try:
            sqsmgr = utils.sqsmanager.CustomerCallbackManager()
            r = sqsmgr.add_callback_response(video_id, request.url, request.body)
        except SQSError, e:
            _log.error('SQS Error %s' % e)
            _log.info('Tried to send response: %s' % request.body)
            return False 

        if not r:
            statemon.state.increment('customer_callback_schedule_error')
            _log.error("Callback schedule failed for video %s" % video_id)
            return False
        '''

        return True

    def send_notifiction_response(self):
        '''
        Send Notification to endpoint
        '''

        api_key = self.job_params[properties.API_KEY] 
        video_id = self.job_params[properties.VIDEO_ID]
        title = self.job_params[properties.VIDEO_TITLE]
        i_id = self.job_params[properties.INTEGRATION_ID]
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
        response = utils.http.send_request(request)
        if response.error:
            _log.error("Notification response not sent to %r " % request)

class VideoClient(object):
   
    '''
    Video Client processor
    '''
    def __init__(self, model_file, debug=False, sync=False):
        self.model_file = model_file
        self.SLEEP_INTERVAL = 10
        self.kill_received = False
        self.dequeue_url = options.video_server + "/dequeue"
        self.requeue_url = options.video_server + "/requeue"
        self.state = "start"
        self.model_version = -1
        self.model = None
        self.debug = debug
        self.sync = sync
        self.pid = os.getpid()

    def dequeue_job(self):
        ''' Blocking http call to global queue to dequeue work
            Change state to PROCESSING after dequeue
        '''
        
        headers = {'X-Neon-Auth' : properties.NEON_AUTH} 
        result = None
        req = tornado.httpclient.HTTPRequest(
                                            url=self.dequeue_url,
                                            method="GET",
                                            headers=headers,
                                            request_timeout=60.0,
                                            connect_timeout=10.0)

        response = utils.http.send_request(req)
        if not response.error:
            result = response.body
             
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
                    _log.info("key=worker [%s] msg=processing request %s for %s "
                              % (self.pid, job_id, api_key))
                except Exception,e:
                    _log.error("key=worker [%s] msg=db error %s" %(self.pid, e.message))
            return result
        else:
            _log.error("Dequeue Error")
            statemon.state.increment('dequeue_error')

    ##### Model Methods #####

    def load_model(self):
        ''' load model '''
        parts = self.model_file.split('/')[-1]
        version = parts.split('.model')[0]
        self.model_version = version
        _log.info('Loading model from %s version %s'
                  % (self.model_file, self.model_version))
        self.model = model.load_model(self.model_file)
        if not self.model:
            _log.error('Error loading the Model')
            exit(1)

    def run(self):
        ''' run/start method '''
        _log.info("starting worker [%s] " %(self.pid))
        while not self.kill_received:
            # Check if memory has been exceeded & exit
            cur_mem_usage = psutil.virtual_memory()[2] # in %
            if cur_mem_usage > 85:
                sys.exit(0)

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
   
    # TODO(sunil): Add signal handler for SIGTERM, do a clean
    # shutdown after finishing the current request

    if options.local:
        _log.info("Running locally")

    vc = VideoClient(options.model_file,
                     options.debug, options.sync)
    vc.run()

if __name__ == "__main__":
    main()
