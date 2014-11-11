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
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
import api.ooyala_api
from api import properties
import atexit
import boto.exception
from boto.s3.connection import S3Connection
import cv2
import ffvideo
import hashlib
import json
import model
import multiprocessing
import numpy as np 
from PIL import Image
import psutil
import Queue
import random
import signal
from supportServices import neondata
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
statemon.define('model_load_error', int)
statemon.define('unknown_exception', int)
statemon.define('video_download_error', int)
statemon.define('ffvideo_metadata_error', int)
statemon.define('video_duration_30m', int)
statemon.define('video_duration_60m', int)
statemon.define('video_read_error', int)
statemon.define('extract_frame_error', int)
statemon.define('running_workers', int)

# ======== Parameters  =======================#
from utils.options import define, options
define('model_file', default=None, help='File that contains the model')
define('video_server', default="http://localhost:8081", type=str,
       help="video server")
define('serving_url_format',
        default="http://i%s.neon-images.com/v1/client/%s/neonvid_%s", type=str)
define('max_videos_per_proc', default=100,
       help='Maximum number of videos a process will handle before respawning')
define('dequeue_period', default=10,
       help='Number of seconds between dequeues on a worker')


class BadVideoError(IOError): pass
class VideoReadError(IOError): pass
class DBError(IOError): pass

    
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

    def __init__(self, params, model, model_version, cv_semaphore):
        '''
        @input
        params: dict of request
        model: model obj
        '''

        self.timeout = 300000.0 #long running tasks ## -- is this necessary ???
        self.job_params = params
        self.video_url = self.job_params[properties.VIDEO_DOWNLOAD_URL]
        vsuffix = self.video_url.split('/')[-1]  #get the video file extension
        vsuffix = vsuffix.strip("!@#$%^&*[]^()+~")
        self.tempfile = tempfile.NamedTemporaryFile(
                                    suffix='_%s' % vsuffix, delete=True)
        self.headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})

        self.base_filename = self.job_params[properties.API_KEY] + "/" + \
                self.job_params[properties.REQUEST_UUID_KEY]

        self.cv_semaphore = cv_semaphore

        integration_id = self.job_params[properties.INTEGRATION_ID] \
          if self.job_params.has_key(properties.INTEGRATION_ID) else 0
        self.video_metadata = VideoMetadata(
            neondata.InternalVideoID.generate(
                job_params[properties.API_KEY],
                self.job_params[properties.VIDEO_ID]),
            model_version=self.model_version,
            request_id=self.job_params[properties.REQUEST_UUID_KEY],
            video_url=self.job_params[properties.VIDEO_DOWNLOAD_URL],
            i_id=integration_id)
    
        #Video vars
        self.model = model
        self.model_version = model_version
        self.thumbnails = [] # List of (ThumbnailMetadata, pil_image)
        self.sec_to_extract = 1 

    def start(self):
        '''
        Actual work done here
        '''
        try:
            self.download_video_file()

            #Process the video
            n_thumbs = 5
            if self.job_params.has_key(properties.TOP_THUMBNAILS):
                n_thumbs = int(self.job_params[properties.TOP_THUMBNAILS])
                
            with self.cv_semaphore:
                self.process_video(self.tempfile.name, n_thumbs=n_thumbs)

            #finalize request, if success send client and notification
            #response
            self.finalize_response()

        except Exception as e:
            # Flag that the job failed in the database
            statemon.state.increment('processing_error')

            # TODO(sunil): Make this an atomic modify operation once
            # neondata is refactored.
            api_request = neondata.NeonApiRequest.get(api_key, job_id)
            api_request.state = neondata.RequestState.FAILED
            api_request.save()
       
        finally:
            #Delete the temp video file which was downloaded
            self.tempfile.close()

    def download_video_file(self):
        '''
        Download the video file 
        '''
        CHUNK_SIZE = 4*1024*1024 # 4MB
        _log.info('Starting download of video %s' % self.video_url)
        req = urllib2.Request(self.video_url, headers=self.headers)
        try:
            response = urllib2.urlopen(req, timeout=self.timeout)
            data = response.read(CHUNK_SIZE)
            while data != '':
                self.tempfile.write(data)
                self.tempfile.flush()
                data = response.read(CHUNK_SIZE)

            _log.info('Finished downloading video %s' % self.video_url)
        except urllib2.URLError as e:
            _log.error("Error downloading video from %s: %s" % 
                       (self.video_url, e))
            statemon.state.increment('video_download_error')
            raise

        except socket.error as e:
            _log.error("Error downloading video from %s: %s" % 
                       (self.video_url, e))
            statemon.state.increment('video_download_error')
            raise

        except IOError as e:
            _log.error("Error saving video to disk: %s" % e)
            statemon.state.increment('video_download_error')
            raise

    def process_video(self, video_file, n_thumbs=1):
        ''' process all the frames from the partial video downloaded '''
        _log.info('Starting to process video %s' % self.video_url)
        start_process = time.time()

        try:
            # OpenCV doesn't return metadata reliably, so use ffvideo
            # to get that information.
            fmov = ffvideo.VideoStream(video_file)            
            self.video_metadata.duration = fmov.duration
            self.video_metadata.frame_size = fmov.frame_size
            
        except Exception, e:
            _log.error("Error reading ffvideo metadata of %s: %s" %
                       (self.video_url, e))
            statemon.state.increment('ffvideo_metadata_error')
            raise

        #Try to open the video file using openCV
        try:
            mov = cv2.VideoCapture(video_file)
        except Exception, e:
            _log.error("Error opening video file %s: %s"  % 
                       (self.video_url, e))
            statemon.state.increment('video_read_error')
            raise

        duration = self.video_metadata.duration or 0.0

        if duration <= 1e-3:
            _log.error("Video %s has no length" % (self.video_url))
            statemon.state.increment('video_read_error')
            raise BadVideoError("Video has no length")

        #Log long videos
        if duration > 1800:
            statemon.state.increment('video_duration_30m')
        if duration > 3600:
            statemon.state.increment('video_duration_60m')

        try:
            results, self.sec_to_extract = \
              self.model.choose_thumbnails(
                  mov,
                  n=n_thumbs,
                  start_time=self.sec_to_extract,
                  video_name=self.video_url)
        except model.VideoReadError:
            _log.error("Error using OpenCV to read video. Trying ffvideo")
            statemon.state.increment('video_read_error')
            try:
                results, self.sec_to_extract = \
                  self.model.ffvideo_choose_thumbnails(
                      fmov,
                      n=n_thumbs,
                      sample_step=1.0,
                      start_time=self.sec_to_extract)
            except Exception:
                raise

        exists_unfiltered_images = np.any([x[4] is not None and x[4] == ''
                                           for x in results])
        rank=0
        for image, score, frame_no, timecode, attribute in results:
            # Only return unfiltered images unless they are all
            # filtered, in which case, return them all.
            if not exists_unfiltered_images or (
                    attribute is not None and attribute == ''):
                meta = neondata.ThumbnailMetadata(
                    None,
                    ttype=neondata.ThumbnailType.NEON,
                    model_score=score,
                    model_version=self.model_version,
                    frameno=frame_no,
                    filtered=attribute,
                    rank=rank)
                self.thumbnails.append((meta, PILImageUtils.from_cv(image)))
                rank += 1 

        # Get the baseline frames of the video
        self._get_center_frame(video_file)
        self._get_random_frame(video_file)

        statemon.state.increment('processed_video')

    def _get_center_frame(self, video_file, nframes=None):
        '''approximation of brightcove logic 
         #Note: Its unclear the exact nature of brighcove thumbnailing,
         the images are close but this is not the exact frame
        '''
        
        try:
            mov = cv2.VideoCapture(video_file)
            if nframes is None:
                nframes = mov.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)

            cv_image = self._get_specific_frame(mov, int(nframes / 2))
            meta = ThumbnailMetadata(None,
                                     ttype=neondata.ThumbnailType.CENTERFRAME,
                                     frameno=int(nframes / 2),
                                     rank=0)
            #TODO(Sunil): Get valence score once flann global data
            #corruption is fixed.
            self.thumbnails.append((meta, PILImageUtils.from_cv(cv_image)))
        except Exception, e:
            _log.error("Unexpected error extracting center frame from %s:"
                       " %s" % (self.video_url, e))
            raise

    def _get_random_frame(self, video_file, nframes=None):
        '''Gets a random frame from the video.
        '''
        try:
            mov = cv2.VideoCapture(video_file)
            if nframes is None:
                nframes = mov.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)

            frameno = random.randint(0, nframes-1)

            cv_image = self._get_specific_frame(mov, frameno)
            meta = ThumbnailMetadata(None,
                                     ttype=neondata.ThumbnailType.RANDOM,
                                     frameno=frameno,
                                     rank=0)
            #TODO(Sunil): Get valence score once flann global data
            #corruption is fixed.
            self.thumbnails.append((meta, PILImageUtils.from_cv(cv_image)))
        except Exception, e:
            _log.error("Unexpected error extracting random frame from %s:"
                           " %s" % (self.video_url, e))
            raise

    def _get_specific_frame(self, mov, frameno):
        ''' Grab a specific frame from the video.

        mov - The cv2 VideoCapture object
        frameno - The frame number to read
        '''
        _log.debug('Extracting frame %i from video %s' %
                   (frameno, self.video_url))
        try:            
            seek_sucess, image = utils.pycvutils.seek_video(mov, frameno)
            if seek_sucess:
                #Now grab the frame
                read_sucess, image = mov.read()
                if read_sucess:
                    return utils.pycvutils.to_pil(image)            
        except Exception, e:
            _log.exception("Unexpected error extracting frame %i from %s: %s" 
                           % (frameno, self.video_url, e))
            statemon.state.increment('extract_frame_error')
            raise

        _log.error('Error reading frame %i of video %s'
                    % (frameno, self.video_url))
        statemon.state.increment('extract_frame_error')
        raise VideoReadError('Error reading frame %i of video' % frameno)

    def finalize_response(self):
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

        reprocess = False 
        if api_request.state == neondata.RequestState.REPROCESS:
            reprocess = True

        # Get the CDN Metadata
        cdn_metadata = neondata.CDNHostingMetadataList.get(
            self.video_metadata.integration_id)
        if cdn_metadata is None:
            _log.warn_n('No cdn metadata for integration %s. '
                        'Defaulting to Neon CDN'
                        % self.video_metadata.integration_id, 10)
            cdn_metadata = [neondata.NeonCDNHostingMetadata()]

        # Attach the thumbnails to the video. This will upload the
        # thumbnails to the appropriate CDNs.
        if len(self.thumbnails) < 1:
            statemon.state.increment('no_thumbs')
            _log.warn("no thumbnails extracted for video %s url %s"\
                    % (self.video_metadata.key, self.video_metadata.url))
        for thumb_meta, image in self.thumbnails:
            self.video_metadata.add_thumbnail(thumb_meta, image,
                                              cdn_metadata=cdn_metadata,
                                              save_objects=False)

        # Save the thumbnail and video data into the database
        # TODO(mdesnoyer): do this as a single transaction
        def _merge_thumbnails(t_objs):
            for new_thumb, garb in self.thumbnails:
                old_thumb = t_objs[new_thumb.key]
                if old_thumb is not None:
                    # There was already an entry for this thumb, so update
                    urlset = set(new_thumb.urls + old_thumb.urls)
                    old_thumb.urls = [x for x in urlset]
                    old_thumb.width = new_thumb.width
                    old_thumb.height = new_thumb.height
                    old_thumb.type = new_thumb.type
                    old_thumb.model_score = new_thumb.model_score
                    old_thumb.model_version = new_thumb.model_version
                    old_thumb.rank = new_thumb.rank
                    old_thumb.phash = new_thumb.phash
                    old_thumb.frameno = new_thumb.frameno
                    old_thumb.filtered = new_thumb.filtered
                else:
                    # Create the new entry
                    t_objs[new_thumb.key] = new_thumb
            to_add = dict([(x.key, x) for x in self.thumbnails])
        sucess = neondata.ThumbnailMetadata.modify_many(
            [x[0].key for x in self.thumbnails],
            _merge_thumbnails)
        if not sucess:
            _log.error("Error writing thumbnail data to database")
            statemon.state.append('save_tmdata_error')
            raise DBError("Error writing thumbnail data to database")
        
        def _merge_video_data(video_obj):
            # If we are reprocessing, then we don't keep the random,
            # centerframe or neon thumbnails
            if reprocess:
                thumbs = ThumbnailMetadata.get_many(video_obj.thumbnail_ids)
                keep_thumbs = [x.key for x in thumbs if x.type not in [
                    neondata.ThumbnailType.NEON,
                    neondata.ThumbnailType.CENTERFRAME,
                    neondata.ThumbnailType.RANDOM]]
            else:
                keep_thumbs = video_obj.thumbnail_ids
            tidset = set(keep_thumbs +
                         self.video_metadata.thumbnail_ids)
            video_obj.thumbnail_ids = [x for x in tidset]
            video_obj.url = self.video_metadata.url
            video_obj.duration = self.video_metadata.duration
            video_obj.video_valence = self.video_metadata.video_valence
            video_obj.model_version = self.video_metadata.model_version
            video_obj.job_id = self.video_metadata.job_id
            video_obj.integration_id = self.video_metadata.integration_id
            video_obj.frame_size = self.video_metadata.frame_size
            video_obj.serving_enabled = len(video_obj.thumbnail_ids) > 0
        sucess = neondata.VideoMetadata.modify(self.video_metadata.key,
                                               _modify_video_data,
                                               create_missing=True)
        if not sucess:
            _log.error("Error writing video data to database")
            statemon.state.append('save_vmdata_error')
            raise DBError("Error writing video data to database")
        


        # Build the callbacks

        # Generate the Serving URL
        # TODO(Sunil): Pass TAI as part of the request?
        serving_url = None
        # Currently Neon has 4 sub-domains
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
                neondata.VideoMetadata.modify(vmdata.key,
                                              _modify_vmdata_atomically)

            ## Notification enabled for brightcove only 
            if request_type == "brightcove":
                self.send_notifiction_response()
        else:
            _log.error("request type not supported")
    
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
                       " msg=no previous thumbnail for %s %s" %(api_key,
                                                                video_id))
       
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
        job_id  = self.job_params[properties.REQUEST_UUID_KEY]
        ba = neondata.BrightcovePlatform.get_account(api_key, i_id) 
        thumbs = [t.to_dict_for_video_response() for t in self.thumbnails]
        vr = neondata.VideoResponse(video_id,
                                    job_id,
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

class VideoClient(multiprocessing.Process):
   
    '''
    Video Client processor
    '''
    def __init__(self, model_file, cv_semaphore):
        self.model_file = model_file
        self.kill_received = multiprocessing.Event()
        self.dequeue_url = options.video_server + "/dequeue"
        self.state = "start"
        self.model_version = None
        self.model = None
        self.pid = os.getpid()
        self.cv_semaphore = cv_semaphore
        self.videos_processed = 0

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
                    _log.info("key=worker [%s] msg=processing request %s for "
                              "%s " % (self.pid, job_id, api_key))
                except Exception,e:
                    _log.error("key=worker [%s] msg=db error %s" %(
                        self.pid, e.message))
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
            statemon.state.increment('model_load_error')
            _log.error('Error loading the Model from %s' % self.model_file)
            raise IOError('Error loading model from %s' % self.model_file)

    def run(self):
        ''' run/start method '''
        _log.info("starting worker [%s] " % (self.pid))
        while (not self.kill_received.is_set() and 
               self.videos_processed < options.max_videos_per_proc):
            self.do_work()
            self.videos_processed += 1

        _log.info("stopping worker [%s] " % (self.pid))

    def do_work(self):   
        ''' do actual work here'''
        try:

            job = self.dequeue_job()
            if not job or job == "{}": #string match
                raise Queue.Empty

            # TODO(mdesnoyer): Only load the model once. Right now,
            # there's a memory problem so we load the model for every
            # job.
            self.load_model()
            jparams = json.loads(job)
            vprocessor = VideoProcessor(jparams, self.model,
                                        self.model_version,
                                        self.cv_semaphore)
            vprocessor.start()

        except Queue.Empty:
            _log.debug("Q,Empty")
            time.sleep(options.dequeue_period * random.random())
        
        except Exception,e:
            statemon.state.increment('unknown_exception')
            _log.exception("key=worker [%s] "
                    " msg=exception %s" %(self.pid, e.message))
            time.sleep(options.dequeue_period)

    def kill(self):
        self.kill_received.set()

__workers = []   
__master_pid = None
__shutting_down = False
def shutdown_master_process():
    if os.getpid() != __master_pid:
        return
    
    _log.info('Shutting down')
    __shutting_down = True

    # Cleanup the workers
    for worker in worker_procs:
        worker.kill()

    # Wait for the workers and force kill if they take too long
    for worker in worker_procs:
        worker.join(1800.0) # 30min timeout
        if worker.is_alive():
            print 'Worker is still going. Force kill it'
            # Send a SIGKILL
            utils.ps.send_signal_and_wait(signal.SIGKILL, [worker.pid])
    

def main():
    utils.neon.InitNeon()

    __master_pid = os.getpid()

    # Register a function that will shutdown the workers
    atexit.register(shutdown_master_process)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    max_workers = multiprocessing.cpu_count() + 2
    cv_semaphore = multiprocessing.BoundedSemaphore(
        max(multiprocessing.cpu_count() - 1, 1))

    # Manage the workers 
    while not __shutting_down:
        # Remove all the workers that are stopped
        workers = [x for x in workers if x.is_alive()]

        statemon.state.running_workers = len(workers)

        # Create new workers until we get to n_workers
        while len(workers) < max_workers:
            vc = VideoClient(options.model_file, cv_semaphore)
            workers.append(vc)
            vc.start()

        
        # Check if memory has been exceeded & exit
        cur_mem_usage = psutil.virtual_memory()[2] # in %
        if cur_mem_usage > 85:
            __shutting_down = True

        time.sleep(10)
    

if __name__ == "__main__":
    main()
