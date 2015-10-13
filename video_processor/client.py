#!/usr/bin/env python
'''
Video Processing client, no longer a multiprocessing client

VideoClient class has a run loop which uses httpdownload object to
download the video file after dequeueing job from video-server

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

import atexit
import boto.exception
from boto.s3.connection import S3Connection
from cmsdb import neondata
import cv2
import ffvideo
import hashlib
import json
import model
import model.errors
import multiprocessing
import numpy as np 
from PIL import Image
import psutil
import Queue
import random
import re
import signal
import socket
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
import urlparse
from utils.imageutils import PILImageUtils
import utils.neon
import utils.pycvutils
import utils.http
import utils.sqsmanager
from utils import statemon

import logging
_log = logging.getLogger(__name__)

#Monitoring
statemon.define('processed_video', int)
statemon.define('processing_error', int)
statemon.define('dequeue_error', int)
statemon.define('save_tmdata_error', int)
statemon.define('save_vmdata_error', int)
statemon.define('modify_request_error', int)
statemon.define('no_thumbs', int)
statemon.define('model_load_error', int)
statemon.define('unknown_exception', int)
statemon.define('video_download_error', int)
statemon.define('default_thumb_error', int)
statemon.define('ffvideo_metadata_error', int)
statemon.define('video_duration_30m', int)
statemon.define('video_duration_60m', int)
statemon.define('video_read_error', int)
statemon.define('extract_frame_error', int)
statemon.define('running_workers', int)
statemon.define('workers_processing', int)
statemon.define('workers_downloading', int)
statemon.define('workers_cv_processing', int)
statemon.define('other_worker_completed', int)
statemon.define('s3url_download_error', int)
statemon.define('centerframe_extraction_error', int)
statemon.define('randomframe_extraction_error', int)

# ======== Parameters  =======================#
from utils.options import define, options
define('model_file', default=None, help='File that contains the model')
define('video_server', default="localhost:8081", type=str,
       help="host:port of the video processing server")
define('serving_url_format',
        default="http://i%s.neon-images.com/v1/client/%s/neonvid_%s", type=str)
define('max_videos_per_proc', default=100,
       help='Maximum number of videos a process will handle before respawning')
define('dequeue_period', default=10.0,
       help='Number of seconds between dequeues on a worker')
define('notification_api_key', default='icAxBCbwo--owZaFED8hWA',
       help='Api key for the notifications')
define('server_auth', default='secret_token',
       help='Secret token for talking with the video processing server')
define('extra_workers', default=0,
       help='Number of extra workers to allow downloads to happen in the background')
define('video_temp_dir', default=None,
       help='Temporary directory to download videos to')
define('max_bandwidth_per_core', default=15500000.0,
       help='Max bandwidth in MB/s')
define('min_load_to_throttle', default=0.50,
       help=('Fraction of cores currently working to cause the download to '
             'be throttled'))

class VideoError(Exception): pass 
class BadVideoError(VideoError): pass
class DefaultThumbError(VideoError): pass  
class VideoDownloadError(VideoError, IOError): pass  
class DBError(IOError): pass

# For when another worker completed the video
class OtherWorkerCompleted(Exception): pass 
    

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

    def __init__(self, params, model, model_version, cv_semaphore,
                 reprocess=False):
        '''
        @input
        params: dict of request
        model: model obj
        '''

        self.timeout = 300.0 #long running tasks ## -- is this necessary ???
        self.job_params = params
        self.reprocess = reprocess
        self.video_url = self.job_params['video_url']
        vsuffix = self.video_url.split('/')[-1]  #get the video file extension
        vsuffix = vsuffix.strip("!@#$%^&*[]^()+~")
        self.tempfile = tempfile.NamedTemporaryFile(
            suffix='_%s' % vsuffix, delete=True, dir=options.video_temp_dir)
        self.headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})

        self.base_filename = self.job_params['api_key'] + "/" + \
                self.job_params['job_id']

        self.n_thumbs = int(self.job_params.get('topn', None) or
                            self.job_params.get('api_param', None) or 5)
        self.n_thumbs = max(self.n_thumbs, 1)

        self.cv_semaphore = cv_semaphore

        integration_id = self.job_params['integration_id'] \
          if self.job_params.has_key('integration_id') else '0'
        self.video_metadata = neondata.VideoMetadata(
            neondata.InternalVideoID.generate(
                self.job_params['api_key'],
                self.job_params['video_id']),
            model_version=model_version,
            request_id=self.job_params['job_id'],
            video_url=self.job_params['video_url'],
            i_id=integration_id)
    
        #Video vars
        self.model = model
        self.model_version = model_version
        self.thumbnails = [] # List of (ThumbnailMetadata, pil_image)

    def start(self):
        '''
        Actual work done here
        '''
        try:
            statemon.state.increment('workers_downloading')
            try:
                self.download_video_file()
            finally:
                statemon.state.decrement('workers_downloading')
                

            #Process the video
            n_thumbs = max(self.n_thumbs, 5)

            with self.cv_semaphore:
                statemon.state.increment('workers_cv_processing')
                try:
                    self.process_video(self.tempfile.name, n_thumbs=n_thumbs)
                finally:
                    statemon.state.decrement('workers_cv_processing')

            #finalize response, if success send client and notification
            #response
            self.finalize_response()

        except OtherWorkerCompleted as e:
            statemon.state.increment('other_worker_completed')
            _log.info('Job %s for account %s was already completed' %
                      (self.job_params['job_id'], self.job_params['api_key']))
            return

        except Exception as e:
            new_state = neondata.RequestState.CUSTOMER_ERROR
            if not isinstance(e, VideoError):
                new_state = neondata.RequestState.INT_ERROR
                _log.error("Unexpected error [%s]: %s" % (os.getpid(), e))
                
            # Flag that the job failed
            statemon.state.increment('processing_error')

            cb = neondata.VideoCallbackResponse(self.job_params['job_id'],
                                                self.job_params['video_id'],
                                                err=e.message)
            
            def _write_failure(request):
                request.state = new_state
                request.response = cb.to_dict()
                request.fail_count += 1
            api_request = neondata.NeonApiRequest.modify(
                self.job_params['job_id'],
                self.job_params['api_key'],
                _write_failure)
            
            # Send the callback to let the client know there was an
            # error that they might be able to fix
            if isinstance(e, VideoError):
                api_request.send_callback()
       
        finally:
            #Delete the temp video file which was downloaded
            self.tempfile.close()

    def download_video_file(self):
        '''
        Download the video file 
        '''
        CHUNK_SIZE = 4*1024*1024 # 4MB
        s3re = re.compile('((s3://)|(https?://[a-zA-Z0-9\-_]+\.amazonaws\.com/))([a-zA-Z0-9\-_\.]+)/(.+)')

        # Find out if we should throttle
        do_throttle = False
        frac_processing = (float(statemon.state.workers_processing) / 
                           max(statemon.state.running_workers, 1))
        if frac_processing > options.min_load_to_throttle:
            do_throttle=True
            chunk_time = float(CHUNK_SIZE) / options.max_bandwidth_per_core

        _log.info('Starting download of video %s. Throttled: %s' % 
                  (self.video_url, do_throttle))

        try:
            s3match = s3re.search(self.video_url)
            if s3match:
                # Get the video from s3 directly
                try:
                    bucket_name = s3match.group(4)
                    key_name = s3match.group(5)
                    s3conn = S3Connection()
                    bucket = s3conn.get_bucket(bucket_name)
                    key = bucket.get_key(key_name)
                    key.get_contents_to_file(self.tempfile)
                    self.tempfile.flush()
                    return
                except boto.exception.S3ResponseError as e:
                    _log.warn('Error getting video url %s via boto. '
                              'Falling back on http: %s' % (self.video_url, e))
                    statemon.state.increment('s3url_download_error')
            
            # Use urllib2
            url_parse = urlparse.urlparse(self.video_url)
            url_parse = list(url_parse)
            url_parse[2] = urllib.quote(url_parse[2])
            req = urllib2.Request(urlparse.urlunparse(url_parse),
                                  headers=self.headers)
            response = urllib2.urlopen(req, timeout=self.timeout)
            last_time = time.time()
            data = response.read(CHUNK_SIZE)
            while data != '':
                if do_throttle:
                    time_spent = time.time() - last_time
                    print 'sleep time: %f, %f' % (chunk_time, chunk_time-time_spent)
                    time.sleep(max(0, chunk_time-time_spent))
                self.tempfile.write(data)
                self.tempfile.flush()
                last_time = time.time()
                data = response.read(CHUNK_SIZE)

            self.tempfile.flush()

            _log.info('Finished downloading video %s' % self.video_url)
        except urllib2.URLError as e:
            msg = "Error downloading video from %s: %s" % (self.video_url, e)
            _log.error(msg)
            statemon.state.increment('video_download_error')
            raise VideoDownloadError(msg)

        except boto.exception.BotoClientError as e:
            msg = ("Client error downloading video %s from S3: %s" % 
                   (self.video_url, e))
            _log.error(msg)
            statemon.state.increment('video_download_error')
            raise VideoDownloadError(msg)

        except boto.exception.BotoServerError as e:
            msg = ("Server error downloading video %s from S3: %s" %
                   (self.video_url, e))
            _log.error(msg)
            statemon.state.increment('video_download_error')
            raise VideoDownloadError(msg)

        except socket.error as e:
            msg = "Error downloading video from %s: %s" % (self.video_url, e)
            _log.error(msg)
            statemon.state.increment('video_download_error')
            raise VideoDownloadError(msg)

        except IOError as e:
            msg = "Error saving video to disk: %s" % e
            _log.error(msg)
            statemon.state.increment('video_download_error')
            raise VideoDownloadError(msg)

    def process_video(self, video_file, n_thumbs=1):
        ''' process all the frames from the partial video downloaded '''
        # The video might have finished by somebody else so double
        # check that we still want to process it.
        api_request = neondata.NeonApiRequest.get(self.job_params['job_id'],
                                                  self.job_params['api_key'])
        if api_request and api_request.state in [
                neondata.RequestState.FINISHED,
                neondata.RequestState.SERVING,
                neondata.RequestState.ACTIVE,
                neondata.RequestState.SERVING_AND_ACTIVE]:
            raise OtherWorkerCompleted()
        
        _log.info('Starting to search video %s' % self.video_url)
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
            raise model.errors.VideoReadError(str(e))

        #Try to open the video file using openCV
        try:
            mov = cv2.VideoCapture(video_file)
        except Exception, e:
            _log.error("Error opening video file %s: %s"  % 
                       (self.video_url, e))
            statemon.state.increment('video_read_error')
            raise model.errors.VideoReadError(str(e))

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
            results = \
              self.model.choose_thumbnails(
                  mov,
                  n=n_thumbs,
                  video_name=self.video_url)
        except model.errors.VideoReadError:
            _log.error("Error using OpenCV to read video. %s" % self.video_url)
            statemon.state.increment('video_read_error')
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
        _log.info('Sucessfully finished searching video %s' % self.video_url)

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
            meta = neondata.ThumbnailMetadata(
                None,
                ttype=neondata.ThumbnailType.CENTERFRAME,
                frameno=int(nframes / 2),
                rank=0)
            #TODO(Sunil): Get valence score once flann global data
            #corruption is fixed.
            self.thumbnails.append((meta, PILImageUtils.from_cv(cv_image)))
        except Exception, e:
            _log.error("Unexpected error extracting center frame from %s:"
                       " %s" % (self.video_url, e))
            statemon.state.increment('centerframe_extraction_error')
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
            meta = neondata.ThumbnailMetadata(
                None,
                ttype=neondata.ThumbnailType.RANDOM,
                frameno=frameno,
                rank=0)
            #TODO(Sunil): Get valence score once flann global data
            #corruption is fixed.
            self.thumbnails.append((meta, PILImageUtils.from_cv(cv_image)))
        except Exception, e:
            _log.error("Unexpected error extracting random frame from %s:"
                           " %s" % (self.video_url, e))
            statemon.state.increment('randomframe_extraction_error')
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
                    return image      
        except Exception, e:
            _log.exception("Unexpected error extracting frame %i from %s: %s" 
                           % (frameno, self.video_url, e))
            statemon.state.increment('extract_frame_error')
            raise

        _log.error('Error reading frame %i of video %s'
                    % (frameno, self.video_url))
        statemon.state.increment('extract_frame_error')
        raise model.errors.VideoReadError('Error reading frame %i of video'
                                          % frameno)

    def finalize_response(self):
        '''
        Finalize the respon after video has been processed.

        This updates the database and does any callbacks necessary.
        '''
        
        api_key = self.job_params['api_key']  
        job_id  = self.job_params['job_id']
        video_id = self.job_params['video_id']
        
        # get api request object
        somebody_else_finished = [False]
        def _flag_for_finalize(req):
            
            if req.state in [neondata.RequestState.PROCESSING,
                             neondata.RequestState.SUBMIT,
                             neondata.RequestState.REQUEUED,
                             neondata.RequestState.REPROCESS,
                             neondata.RequestState.FAILED,
                             neondata.RequestState.INT_ERROR,
                             neondata.RequestState.CUSTOMER_ERROR]:
                req.state = neondata.RequestState.FINALIZING
            else:
                somebody_else_finished[0] = True
        try:
            api_request = neondata.NeonApiRequest.modify(job_id, api_key,
                                                         _flag_for_finalize)
            if api_request is None:
                raise DBError('Api Request finalizing failed.It was not there')
        except Exception, e:
            _log.error("Error writing request state to database: %s" % e)
            statemon.state.increment('modify_request_error')
            raise DBError("Error modifying api request")
        
        if somebody_else_finished[0]:
            raise OtherWorkerCompleted()

        # Get the CDN Metadata
        cdn_metadata = neondata.CDNHostingMetadataList.get(
            neondata.CDNHostingMetadataList.create_key(
                api_key,
                self.video_metadata.integration_id))
        if cdn_metadata is None:
            _log.warn_n('No cdn metadata for account %s integration %s. '
                        'Defaulting to Neon CDN'
                        % (api_key, self.video_metadata.integration_id), 10)
            cdn_metadata = [neondata.NeonCDNHostingMetadata()]

        # Attach the thumbnails to the video. This will upload the
        # thumbnails to the appropriate CDNs.
        if len(filter(lambda x: x[0].type == neondata.ThumbnailType.NEON,
                      self.thumbnails)) < 1:
            # TODO (Sunil): Video to be marked as failed or int err ? 
            statemon.state.increment('no_thumbs')
            _log.warn("No thumbnails extracted for video %s url %s"\
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
                # There was already an entry for this thumb, so update
                urlset = set(new_thumb.urls + old_thumb.urls)
                old_thumb.urls = [x for x in urlset]
                old_thumb.video_id = new_thumb.video_id
                old_thumb.width = new_thumb.width
                old_thumb.height = new_thumb.height
                old_thumb.type = new_thumb.type
                old_thumb.model_score = new_thumb.model_score
                old_thumb.model_version = new_thumb.model_version
                old_thumb.rank = new_thumb.rank
                old_thumb.phash = new_thumb.phash
                old_thumb.frameno = new_thumb.frameno
                old_thumb.filtered = new_thumb.filtered
        try:
            new_thumb_dict = neondata.ThumbnailMetadata.modify_many(
                [x[0].key for x in self.thumbnails],
                _merge_thumbnails,
                create_missing=True)
            if len(self.thumbnails) > 0 and len(new_thumb_dict) == 0:
                raise DBError("Couldn't change some thumbs")
        except Exception, e:
            _log.error("Error writing thumbnail data to database: %s" % e)
            statemon.state.increment('save_tmdata_error')
            raise DBError("Error writing thumbnail data to database")
        
        def _merge_video_data(video_obj):
            # Don't keep the random centerframe or neon thumbnails
            thumbs = neondata.ThumbnailMetadata.get_many(
                video_obj.thumbnail_ids)
            keep_thumbs = [x.key for x in thumbs if x.type not in [
                neondata.ThumbnailType.NEON,
                neondata.ThumbnailType.CENTERFRAME,
                neondata.ThumbnailType.RANDOM]]
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
        try:
            new_video_metadata = neondata.VideoMetadata.modify(
                self.video_metadata.key,
                _merge_video_data,
                create_missing=True)
            if not new_video_metadata:
                raise DBError('This should not ever happen')
        except Exception, e:
            _log.error("Error writing video data to database: %s" % e)
            statemon.state.increment('save_vmdata_error')
            raise DBError("Error writing video data to database")
        self.video_metadata = new_video_metadata
        try:
            # A second attempt to save the default thumb
            api_request.save_default_thumbnail(cdn_metadata)
            
            # Enable the video to be served if we have any thumbnails available
            def _set_serving_enabled(video_obj):
                video_obj.serving_enabled = len(video_obj.thumbnail_ids) > 0
            new_video_metadata = neondata.VideoMetadata.modify(
                self.video_metadata.key,
                _set_serving_enabled)
        

            # Everything is fine at this point, so lets mark it finished
            api_request.state = neondata.RequestState.FINISHED

        except neondata.ThumbDownloadError, e:
            _log.warn("Default thumbnail download failed for vid %s" %
                      video_id)
            statemon.state.increment('default_thumb_error')
            err_msg = "Failed to download default thumbnail: %s" % e
            raise DefaultThumbError(err_msg)

  

        # Build the callback response
        cb_response = self.build_callback_response()

        # Update the database that the request is done with the processing 
        def _flag_request_done_in_db(request):
            request.state = api_request.state
            request.publish_date = time.time() * 1000.0
            request.response = cb_response
            request.callback_state = neondata.CallbackState.NOT_SENT
        try:
            sucess = neondata.NeonApiRequest.modify(api_request.job_id,
                                                    api_request.api_key,
                                                    _flag_request_done_in_db)
            if not sucess:
                raise DBError('Api Request finished failed. It was not there')
        except Exception, e:
            _log.error("Error writing request state to database: %s" % e)
            statemon.state.increment('modify_request_error')
            raise DBError("Error finishing api request")

        # Send the notifications
        self.send_notifiction_response(api_request)

        _log.info('Sucessfully finalized video %s. Is has video id %s' % 
                  (self.video_url, self.video_metadata.key))
        

    def build_callback_response(self):
        '''
        build the dict that defines the callback response

        '''

        frames = [x[0].frameno for x in self.thumbnails 
            if x[0].type == neondata.ThumbnailType.NEON]
        fnos = frames[:self.n_thumbs]
        thumbs = [x[0].urls[0] for x in self.thumbnails 
            if x[0].type == neondata.ThumbnailType.NEON]
        thumbs = thumbs[:self.n_thumbs]

        cresp = neondata.VideoCallbackResponse(self.video_metadata.job_id,
                neondata.InternalVideoID.to_external(self.video_metadata.key),
                fnos,
                thumbs,
                self.video_metadata.get_serving_url(save=False))
        return cresp.to_dict()

    def send_notifiction_response(self, api_request):
        '''
        Send Notification to endpoint
        '''

        api_key = self.job_params['api_key'] 
        video_id = self.job_params['video_id']
        title = self.job_params['video_title']
        i_id = self.video_metadata.integration_id
        job_id  = self.job_params['job_id']
        account = neondata.NeonUserAccount.get(api_key)
        if account is None:
            _log.error('Could not get the account for api key %s' %
                       api_key)
            return
        thumbs = [t[0].to_dict_for_video_response() for t in self.thumbnails]
        vr = neondata.VideoResponse(video_id,
                                    job_id,
                                    "processed",
                                    api_request.integration_type,
                                    i_id,
                                    title,
                                    self.video_metadata.duration, 
                                    api_request.publish_date,
                                    0, #current_tid
                                    thumbs)

        notification_url = \
          'http://www.neon-lab.com/api/accounts/%s/events' % account.account_id
        request_dict = {}
        request_dict["api_key"] = options.notification_api_key
        request_dict["video"] = vr.to_json()
        request_dict["event"] = "processing_complete"
        request = tornado.httpclient.HTTPRequest(
            url=notification_url, 
            method="POST",
            body=urllib.urlencode(request_dict), 
            request_timeout=60.0, 
            connect_timeout=10.0)
        response = utils.http.send_request(request)
        if response.error:
            _log.error("Notification response not sent to %r " % request)

class VideoClient(multiprocessing.Process):
   
    '''
    Video Client processor
    '''
    def __init__(self, model_file, cv_semaphore):
        super(VideoClient, self).__init__()
        self.model_file = model_file
        self.kill_received = multiprocessing.Event()
        self.state = "start"
        self.model_version = None
        self.model = None
        self.cv_semaphore = cv_semaphore
        self.videos_processed = 0

    def dequeue_job(self):
        ''' Blocking http call to global queue to dequeue work
            Change state to PROCESSING after dequeue
        '''
        _log.debug("Dequeuing job [%s] " % (self.pid))
        headers = {'X-Neon-Auth' : options.server_auth} 
        result = None
        dequeue_url = 'http://%s/dequeue' % options.video_server
        req = tornado.httpclient.HTTPRequest(
                                            url=dequeue_url,
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
                    api_key = job_params['api_key']
                    job_id  = job_params['job_id']
                    job_params['reprocess'] = False
                    def _change_job_state(request):
                        if request.state in [neondata.RequestState.SUBMIT,
                                             neondata.RequestState.REPROCESS,
                                             neondata.RequestState.REQUEUED]:
                            if request.state in [
                                    neondata.RequestState.REPROCESS]:
                                _log.info('Reprocessing job %s for account %s'
                                          % (job_id, api_key))
                                job_params['reprocess'] = True
                            request.state = \
                                neondata.RequestState.PROCESSING
                            request.model_version = self.model_version
                                
                    api_request = neondata.NeonApiRequest.modify(
                        job_id, api_key, _change_job_state)
                    if api_request is None:
                        _log.error('Could not get job %s for %s' %
                                   (job_id, api_key))
                        statemon.state.increment('dequeue_error')
                        return False
                    if api_request.state != neondata.RequestState.PROCESSING:
                        _log.info('Job %s for account %s ignored' %
                                  (job_id, api_key))
                        return False
                    _log.info("key=worker [%s] msg=processing request %s for "
                              "%s." % (self.pid, job_id, api_key))
                    return job_params
                except Exception,e:
                    _log.error("key=worker [%s] msg=db error %s" %(
                        self.pid, e.message))
                    return False
            return result
        else:
            _log.error("Dequeue Error")
            statemon.state.increment('dequeue_error')
        return False

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
        
        # Register a function to die cleanly on a sigterm
        atexit.register(self.stop)
        
        while (not self.kill_received.is_set() and 
               self.videos_processed < options.max_videos_per_proc):
            self.do_work()

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
            vprocessor = VideoProcessor(job, self.model,
                                        self.model_version,
                                        self.cv_semaphore,
                                        job['reprocess'])
            statemon.state.increment('workers_processing')
            try:
                vprocessor.start()
            finally:
                statemon.state.decrement('workers_processing')
            self.videos_processed += 1

        except Queue.Empty:
            _log.debug("Q,Empty")
            time.sleep(options.dequeue_period * random.random())
        
        except Exception,e:
            statemon.state.increment('unknown_exception')
            _log.exception("key=worker [%s] "
                    " msg=exception %s" %(self.pid, e.message))
            time.sleep(options.dequeue_period)

    def stop(self):
        self.kill_received.set()

_workers = []   
_master_pid = None
_shutting_down = False
@atexit.register
def shutdown_master_process():
    if os.getpid() != _master_pid:
        return
    
    _log.info('Shutting down')
    _shutting_down = True

    # Cleanup the workers
    for worker in _workers:
        worker.stop()

    # Wait for the workers and force kill if they take too long
    for worker in _workers:
        worker.join(1800.0) # 30min timeout
        if worker.is_alive():
            print 'Worker is still going. Force kill it'
            # Send a SIGKILL
            utils.ps.send_signal_and_wait(signal.SIGKILL, [worker.pid])
    

if __name__ == "__main__":
    utils.neon.InitNeon()

    _master_pid = os.getpid()

    # Register a function that will shutdown the workers
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    cv_slots = max(multiprocessing.cpu_count() - 1, 1)
    cv_semaphore = multiprocessing.BoundedSemaphore(cv_slots)

    # Manage the workers 
    try:
        while not _shutting_down:
            # Remove all the workers that are stopped
            _workers = [x for x in _workers if x.is_alive()]

            statemon.state.running_workers = len(_workers)

            # Create new workers until we get to n_workers
            while len(_workers) < (cv_slots + options.extra_workers):
                vc = VideoClient(options.model_file, cv_semaphore)
                _workers.append(vc)
                vc.start()

        
            # Check if memory has been exceeded & exit
            cur_mem_usage = psutil.virtual_memory()[2] # in %
            if cur_mem_usage > 85:
                _shutting_down = True

            time.sleep(10)
    except Exception as e:
        _log.exception('Unexpected error managing the workers: %s' % e)
