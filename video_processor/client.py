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
import concurrent.futures
import cv2
import dateutil.parser
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
import shutil 
import signal
import socket
import tempfile
import tornado.web
import tornado.gen
import tornado.escape
import tornado.httpclient
import tornado.httputil
import time
import urllib
import urllib2
import urlparse
from cvutils.imageutils import PILImageUtils
import utils.neon
from utils import pycvutils
import utils.http
from utils import statemon
from video_processor import video_processing_queue
import youtube_dl

import logging
_log = logging.getLogger(__name__)

#Monitoring
statemon.define('processed_video', int)
statemon.define('processing_error', int)
statemon.define('too_many_failures', int)
statemon.define('dequeue_error', int)
statemon.define('invalid_jobs', int)
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
statemon.define('youtube_video_not_found', int) 

# ======== Parameters  =======================#
from utils.options import define, options
define('model_file', default=None, help='File that contains the model')
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
       help='Max bandwidth in bytes/s')
define('min_load_to_throttle', default=0.50,
       help=('Fraction of cores currently working to cause the download to '
             'be throttled'))
define('max_fail_count', default=3, 
       help='Number of failures allowed before a job is discarded')
define('max_attempt_count', default=5, 
       help='Number of attempts allowed before a job is discarded')

class VideoError(Exception): pass 
class BadVideoError(VideoError): pass
class DefaultThumbError(VideoError): pass  
class VideoDownloadError(VideoError, IOError): pass  
class DBError(IOError): pass

# For when another worker completed the video
class OtherWorkerCompleted(Exception): pass 

# TimeoutError Exception
class TimeoutError(Exception): pass
class DequeueError(Exception): pass
class UninterestingJob(Exception): pass

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
    CHUNK_SIZE = 4*1024*1024 # 4MB

    def __init__(self, params, model, model_version, cv_semaphore,
                 job_queue, job_message, reprocess=False):
        '''
        @input
        params: dict of request
        model: model obj
        '''

        self.job_params = params
        self.reprocess = reprocess
        self.job_queue = job_queue
        self.job_message = job_message
        self.video_url = self.job_params['video_url']
        #get the video file extension
        parsed = urlparse.urlparse(self.video_url)
        vsuffix = os.path.splitext(parsed.path)[1]
        self.tempfile = tempfile.NamedTemporaryFile(
            suffix=vsuffix, delete=True, dir=options.video_temp_dir)
        self.headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})

        self.base_filename = self.job_params['api_key'] + "/" + \
                self.job_params['job_id']

        self.n_thumbs = int(self.job_params.get('topn', None) or
                            self.job_params.get('api_param', None) or 5)
        self.n_thumbs = max(self.n_thumbs, 1)

        # The default thumb url extracted from the video url
        self.extracted_default_thumbnail = None

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

        self.executor = concurrent.futures.ThreadPoolExecutor(10)

    def __del__(self):
        # Clean up the executor
        self.executor.shutdown(False)

    @staticmethod
    def percent_encode_url_path(url):
        '''
        Takes a url and re-encodes (i.e., decodes encodes) its path with
        percent-sign encoding. TODO consider cases: unicode, double-decode
        '''

        parse = urlparse.urlparse(url)
        parse = list(parse)
        parse[2] = urllib.quote(urllib.unquote(parse[2]))
        return urlparse.urlunparse(parse)

    @tornado.gen.coroutine
    def start(self):
        '''
        Actual work done here
        '''
        try:
            statemon.state.increment('workers_downloading')
            try:
                yield self.download_video_file()
            finally:
                statemon.state.decrement('workers_downloading')

            #Process the video
            n_thumbs = max(self.n_thumbs, 5)

            with self.cv_semaphore:
                statemon.state.increment('workers_cv_processing')
                try:
                    yield self.process_video(self.tempfile.name,
                                             n_thumbs=n_thumbs)
                finally:
                    statemon.state.decrement('workers_cv_processing')

            #finalize response, if success send client and notification
            #response
            yield self.finalize_response()

            # Delete the job from the queue
            _log.info('Deleting on %s' % self.job_queue)
            yield self.job_queue.delete_message(self.job_message)

        except OtherWorkerCompleted as e:
            statemon.state.increment('other_worker_completed')
            _log.info('Job %s for account %s was already completed' %
                      (self.job_params['job_id'], self.job_params['api_key']))
            return

        except Exception as e:
            new_state = neondata.RequestState.CUSTOMER_ERROR
            if not isinstance(e, VideoError):
                new_state = neondata.RequestState.INT_ERROR
                _log.exception("Unexpected error [%s]: %s" % (os.getpid(), e))

            cb = neondata.VideoCallbackResponse(self.job_params['job_id'],
                                                self.job_params['video_id'],
                                                err=e.message)
            
            def _write_failure(request):
                request.fail_count += 1
                request.response = cb.to_dict()
                if request.fail_count < options.max_fail_count:
                    # This job is going to be requeued
                    request.state = neondata.RequestState.REQUEUED
                else:
                    request.state = new_state
            api_request = yield neondata.NeonApiRequest.modify(
                self.job_params['job_id'],
                self.job_params['api_key'],
                _write_failure,
                async=True)

            if api_request.state == neondata.RequestState.REQUEUED:
                # Let another node pick up the job to try again
                try:
                    yield self.job_queue.hide_message(self.job_message, 
                                                      options.dequeue_period
                                                      / 2.0)
                except boto.exception.SQSError as e:
                    _log.warn('Error hiding message: %s' % e)
            
            else:
                # It's the final error
                statemon.state.increment('processing_error')
            
                # Send the callback to let the client know there was an
                # error that they might be able to fix
                if isinstance(e, VideoError):
                    yield api_request.send_callback(async=True)

                # Delete the job
                _log.warn('Job %s for account %s has failed' %
                          (api_request.job_id, api_request.api_key))
                yield self.job_queue.delete_message(self.job_message)
       
        finally:
            #Delete the temp video file which was downloaded
            self.tempfile.close()

    @tornado.concurrent.run_on_executor
    def _urllib_read(self, response, do_throttle, chunk_time):
        last_time = time.time()
        data = response.read(VideoProcessor.CHUNK_SIZE)
        while data != '':
            if do_throttle:
                time_spent = time.time() - last_time
                time.sleep(max(0, chunk_time-time_spent))
            self.tempfile.write(data)
            self.tempfile.flush()
            last_time = time.time()
            data = response.read(VideoProcessor.CHUNK_SIZE)

        self.tempfile.flush()

    @tornado.gen.coroutine
    def download_video_file(self):
        '''
        Download the video file 
        '''
        s3re = re.compile('((s3://)|(https?://[a-zA-Z0-9\-_]+\.amazonaws\.com/))([a-zA-Z0-9\-_\.]+)/(.+)')

        # Get the duration of the video if it was sent in
        video_duration = self.job_queue.get_duration(
            self.job_message)

        # Find out if we should throttle
        do_throttle = False
        frac_processing = (float(statemon.state.workers_processing) / 
                           max(statemon.state.running_workers, 1))
        if frac_processing > options.min_load_to_throttle:
            do_throttle = True

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
                    bucket = yield self.executor.submit(
                        s3conn.get_bucket, bucket_name)
                    key = yield self.executor.submit(
                        bucket.get_key, key_name)
                    self.video_metadata.duration = video_duration
                    yield self._set_job_timeout(self.video_metadata.duration,
                                                key.size)
                    yield self.executor.submit(
                        key.get_contents_to_file, self.tempfile)
                    yield self.executor.submit(self.tempfile.flush)
                    return
                except boto.exception.S3ResponseError as e:
                    _log.warn('Error getting video url %s via boto. '
                              'Falling back on http: %s' % (self.video_url, e))
                    statemon.state.increment('s3url_download_error')

            # Now try using youtube-dl to download the video. This can
            # potentially handle a ton of different video sources.
            def _handle_progress(x):
                if x['status'] == 'finished':
                    shutil.move(x['filename'], self.tempfile.name)
                    
            dl_params = {}
            dl_params['ratelimit'] = (options.max_bandwidth_per_core 
                                      if do_throttle else None)
            dl_params['restrictfilenames'] = True
            dl_params['progress_hooks'] = [_handle_progress]
            dl_params['outtmpl'] = unicode(str(
                os.path.join(options.video_temp_dir or '/tmp',
                             '%s_%%(id)s.%%(ext)s' %
                             self.job_params['api_key'])))

            # Specify for formats that we want in order of preference
            dl_params['format'] = (
                'best[ext=mp4][height<=720][protocol^=?http]/'
                'best[ext=mp4][protocol^=?http]/'
                'best[height<=720][protocol^=?http]/'
                'best[protocol^=?http]/'
                'best/'
                'bestvideo')
            dl_params['logger'] = _log
            
            with youtube_dl.YoutubeDL(dl_params) as ydl:
                # Dig down to the real url
                cur_url = self.video_url
                found_video = False
                while not found_video:
                    video_info = yield self.executor.submit(ydl.extract_info,
                        cur_url, download=False)
                    result_type = video_info.get('_type', 'video')
                    if result_type == 'url':
                        # Need to step to the next url
                        cur_url = video_info['url']
                        continue
                    elif result_type == 'video':
                        found_video = True
                    else:
                        # They gave us a playlist or other type of url
                        msg = ('Unhandled video type %s' %
                               (result_type))
                        raise youtube_dl.utils.DownloadError(msg)

                # Update information about the video before we download it
                self.extracted_default_thumbnail = video_info.get('thumbnail')
                def _update_title(x):
                    if x.video_title is None:
                        x.video_title = video_info.get('title', None)
                yield neondata.NeonApiRequest.modify(
                    self.job_params['job_id'], self.job_params['api_key'] ,
                    _update_title,
                    async=True)

                # Update some of our metadata if there's better info
                # from the video
                self.video_metadata.duration = video_info.get(
                    'duration', video_duration)
                if video_info.get('upload_date', None) is not None:
                    self.video_metadata.publish_date = \
                      dateutil.parser.parse(video_info['upload_date']).isoformat()

                # Update the timeout
                yield self._set_job_timeout(
                    self.video_metadata.duration,
                    video_info.get('filesize', 
                                   video_info.get('filesize_approx')))
                    
                # Do the real download
                video_info = yield self.executor.submit(ydl.extract_info,
                                                        cur_url, download=True)
                                                              

            _log.info('Finished downloading video %s' % self.video_url)

        except (youtube_dl.utils.DownloadError,
                youtube_dl.utils.ExtractorError, 
                youtube_dl.utils.UnavailableVideoError,
                socket.error) as e:
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

        except IOError as e:
            msg = "Error saving video to disk: %s" % e
            _log.error(msg)
            statemon.state.increment('video_download_error')
            raise VideoDownloadError(msg)

    @tornado.gen.coroutine
    def process_video(self, video_file, n_thumbs=1):
        ''' process all the frames from the partial video downloaded '''
        # The video might have finished by somebody else so double
        # check that we still want to process it.
        api_request = yield neondata.NeonApiRequest.get(
            self.job_params['job_id'],
            self.job_params['api_key'],
            async=True)
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
            raise BadVideoError(str(e))

        #Try to open the video file using openCV
        try:
            mov = cv2.VideoCapture(video_file)
        except Exception, e:
            _log.error("Error opening video file %s: %s"  % 
                       (self.video_url, e))
            statemon.state.increment('video_read_error')
            raise BadVideoError(str(e))

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

        # Fetch the ProcessingStrategy
        account_id = self.job_params['api_key']
        
        try:
            processing_strategy = yield neondata.ProcessingStrategy.get(
                account_id, async=True)
        except Exception, e:
            _log.error(("Could not fetch processing strategy for account_id "
                        "%s: %s")%(str(account_id), e))
            raise DBError("Could not fetch processing strategy")
        self.model.update_processing_strategy(processing_strategy)

        try:
            results = \
              self.model.choose_thumbnails(
                  mov,
                  n=n_thumbs,
                  video_name=self.video_url)
        except model.errors.VideoReadError:
            msg = "Error using OpenCV to read video. %s" % self.video_url
            _log.error(msg)
            statemon.state.increment('video_read_error')
            raise BadVideoError(msg)

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
                nframes = mov.get(cv2.CAP_PROP_FRAME_COUNT)

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
                nframes = mov.get(cv2.CAP_PROP_FRAME_COUNT)

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
            seek_sucess, image = pycvutils.seek_video(mov, frameno)
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
        raise BadVideoError('Error reading frame %i of video' % frameno)

    @tornado.gen.coroutine
    def finalize_response(self):
        '''
        Finalize the response after video has been processed.

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
                             neondata.RequestState.CUSTOMER_ERROR,
                             neondata.RequestState.FINALIZING]:
                req.state = neondata.RequestState.FINALIZING
            else:
                somebody_else_finished[0] = True
        try:
            api_request = yield neondata.NeonApiRequest.modify(
                job_id,
                api_key,
                _flag_for_finalize,
                async=True)
            if api_request is None:
                raise DBError('Api Request finalizing failed. '
                              'It was not there')
        except Exception, e:
            _log.error("Error writing request state to database: %s" % e)
            statemon.state.increment('modify_request_error')
            raise DBError("Error modifying api request")
        
        if somebody_else_finished[0]:
            raise OtherWorkerCompleted()

        # Get the CDN Metadata
        cdn_metadata = yield neondata.CDNHostingMetadataList.get(
            neondata.CDNHostingMetadataList.create_key(
                api_key,
                self.video_metadata.integration_id),
            async=True)
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
            yield self.video_metadata.add_thumbnail(
                thumb_meta, image,
                cdn_metadata=cdn_metadata,
                save_objects=False,
                async=True)
            
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
            new_thumb_dict = yield neondata.ThumbnailMetadata.modify_many(
                [x[0].key for x in self.thumbnails],
                _merge_thumbnails,
                create_missing=True,\
                async=True)
            if len(self.thumbnails) > 0 and len(new_thumb_dict) == 0:
                raise DBError("Couldn't change some thumbs")
        except Exception, e:
            _log.error("Error writing thumbnail data to database: %s" % e)
            statemon.state.increment('save_tmdata_error')
            raise DBError("Error writing thumbnail data to database")

        @tornado.gen.coroutine
        def _merge_video_data(video_obj):
            # Don't keep the random centerframe or neon thumbnails
            thumbs = yield neondata.ThumbnailMetadata.get_many(
                video_obj.thumbnail_ids, async=True)
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
            video_obj.publish_date = (video_obj.publish_date or 
                                      self.video_metadata.publish_date)
        try:
            new_video_metadata = yield neondata.VideoMetadata.modify(
                self.video_metadata.key,
                _merge_video_data,
                create_missing=True,
                async=True)
            if not new_video_metadata:
                raise DBError('This should not ever happen')
        except Exception, e:
            _log.error("Error writing video data to database: %s" % e)
            statemon.state.increment('save_vmdata_error')
            raise DBError("Error writing video data to database")
        self.video_metadata = new_video_metadata
        is_user_default_thumb = api_request.default_thumbnail is not None
        try:
            # A second attempt to save the default thumb
            api_request.default_thumbnail = (api_request.default_thumbnail or 
                                             self.extracted_default_thumbnail)
            yield api_request.save_default_thumbnail(cdn_metadata, async=True)

        except neondata.ThumbDownloadError, e:
            # If we extracted the default thumb from the url, then
            # don't error out if we cannot get thumb
            if is_user_default_thumb:
                _log.warn("Default thumbnail download failed for vid %s" %
                          video_id)
                statemon.state.increment('default_thumb_error')
                err_msg = "Failed to download default thumbnail: %s" % e
                raise DefaultThumbError(err_msg)

        # Enable the video to be served if we have any thumbnails available
        def _set_serving_enabled(video_obj):
            video_obj.serving_enabled = len(video_obj.thumbnail_ids) > 0
        new_video_metadata = yield neondata.VideoMetadata.modify(
            self.video_metadata.key,
            _set_serving_enabled,
            async=True)
        # Everything is fine at this point, so lets mark it finished
        api_request.state = neondata.RequestState.FINISHED

        # Build the callback response
        cb_response = self.build_callback_response()

        # Update the database that the request is done with the processing 
        def _flag_request_done_in_db(request):
            request.state = api_request.state
            request.publish_date = time.time() * 1000.0
            request.response = cb_response
            request.callback_state = neondata.CallbackState.NOT_SENT
        try:
            sucess = yield neondata.NeonApiRequest.modify(
                api_request.job_id,
                api_request.api_key,
                _flag_request_done_in_db,
                async=True)
            if not sucess:
                raise DBError('Api Request finished failed. It was not there')
        except Exception, e:
            _log.error("Error writing request state to database: %s" % e)
            statemon.state.increment('modify_request_error')
            raise DBError("Error finishing api request")

        # Send the notifications
        yield self.send_notifiction_response(api_request)

        _log.info('Sucessfully finalized video %s. Is has video id %s' % 
                  (self.video_url, self.video_metadata.key))
        
    def build_callback_response(self):
        '''
        build the dict that defines the callback response

        '''

        frames = [x[0].frameno for x in self.thumbnails
            if x[0].type == neondata.ThumbnailType.NEON]
        fnos = frames[:self.n_thumbs]
        thumbs = [x[0].key for x in self.thumbnails 
            if x[0].type == neondata.ThumbnailType.NEON]
        thumbs = thumbs[:self.n_thumbs]

        cresp = neondata.VideoCallbackResponse(
            self.video_metadata.job_id,
            neondata.InternalVideoID.to_external(self.video_metadata.key),
            fnos,
            thumbs,
            self.video_metadata.get_serving_url(save=False))
        return cresp.to_dict()

    @tornado.gen.coroutine
    def send_notifiction_response(self, api_request):
        '''
        Send Notification to endpoint
        '''

        api_key = self.job_params['api_key'] 
        video_id = self.job_params['video_id']
        title = self.job_params['video_title']
        i_id = self.video_metadata.integration_id
        job_id  = self.job_params['job_id']
        account = yield neondata.NeonUserAccount.get(api_key, async=True)
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
        response = yield utils.http.send_request(request, async=True)
        if response.error:
            _log.error("Notification response not sent to %r " % request)

    @tornado.gen.coroutine
    def _set_job_timeout(self, duration=None, size=None,
                         time_factor=3.0):
        '''Set the job timeout so that this worker gets the job for this time.

        Inputs:
        duration - Duration of the video in seconds
        size - Size of the video file in bytes
        time_factor - How long the job should run as a multiple of the video 
                      length.
        '''
        if not duration:
            if not size:
                # Do not set a timeout because we have no idea
                return

            # Approximate the length of the video
            duration = size * 8.0 / 1024 / 800

        try:
            yield self.job_queue.hide_message(self.job_message,
                                              int(duration * time_factor))
        except boto.exception.SQSError as e:
            _log.warn('Error extending job time: %s' % e)

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
        self.job_queue = video_processing_queue.VideoProcessingQueue()

    @tornado.gen.coroutine
    def dequeue_job(self):
        ''' Asynchronous call to dequeue work
            Change state to PROCESSING after dequeue
        '''
        _log.debug("Dequeuing job [%s] " % (self.pid)) 
        result = None
        job_params = None
        self.cur_message = yield self.job_queue.read_message()
        if self.cur_message:
            result = self.cur_message.get_body()
            _log.info(result)
            if result is not None and result != "{}":
                try:
                    job_params = tornado.escape.json_decode(result)
                except ValueError as e:
                    _log.warning('Job body %s was not JSON' % result)
                    statemon.state.increment('invalid_jobs')
                    raise UninterestingJob()
                #Change Job State
                api_key = job_params['api_key']
                job_id  = job_params['job_id']
                job_params['reprocess'] = False
                def _change_job_state(request):
                    request.try_count +=1
                    if request.state in [neondata.RequestState.SUBMIT,
                                         neondata.RequestState.REPROCESS,
                                         neondata.RequestState.REQUEUED,
                                         neondata.RequestState.FINALIZING]:
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
                    raise DequeueError('Api Request does not exist.')
                if api_request.state in [neondata.RequestState.FINISHED,
                                         neondata.RequestState.SERVING]:
                    _log.info('Dequeued a job that somebody else finished')
                    raise UninterestingJob('Somebody else finished')
                if api_request.state != neondata.RequestState.PROCESSING:
                    _log.error('Job %s for account %s could not set to '
                               'PROCESSING' %
                               (job_id, api_key))
                    statemon.state.increment('dequeue_error')
                    raise DequeueError('Could not set processing')
                if (api_request.fail_count >= options.max_fail_count or 
                    api_request.try_count >= options.max_attempt_count):
                    msg = ('Job %s for account %s has failed too many '
                           'times' % (job_id, api_key))
                    _log.error(msg)
                    statemon.state.increment('too_many_failures')
                    
                    def _write_failure(req):
                        cb = neondata.VideoCallbackResponse(job_id,
                                                            req.video_id,
                                                            err=msg)
                        req.response = cb.to_dict()
                        req.state = neondata.RequestState.INT_ERROR
                    yield neondata.NeonApiRequest.modify(job_id, api_key,
                                                         _write_failure,
                                                         async=True)
                    raise UninterestingJob('Job failed')
                
                _log.info("key=worker [%s] msg=processing request %s for "
                          "%s." % (self.pid, job_id, api_key))
            if job_params is not None:
                _log.debug("Dequeue Successful")
                raise tornado.gen.Return(job_params)

            _log.warning('Job body %s was uninteresting' % result)
            statemon.state.increment('invalid_jobs')
            raise UninterestingJob('Job did not have parameters')
        else:
            raise Queue.Empty()

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
        # The worker should ignore the SIGTERM because it will be
        # killed by the master thread via self.kill_received
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        
        _log.info("starting worker [%s] " % (self.pid))
        
        while (not self.kill_received.is_set() and 
               self.videos_processed < options.max_videos_per_proc):
            self.do_work()
 
        _log.info("stopping worker [%s] " % (self.pid))

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def do_work(self):   
        ''' do actual work here'''
        try:
            job = yield self.dequeue_job()

            # TODO(mdesnoyer): Only load the model once. Right now,
            # there's a memory problem so we load the model for every
            # job.
            self.load_model()
            vprocessor = VideoProcessor(job, self.model,
                                        self.model_version,
                                        self.cv_semaphore,
                                        self.job_queue,
                                        self.cur_message,
                                        job['reprocess'])
            statemon.state.increment('workers_processing')

            try:
                yield vprocessor.start()
            finally:
                statemon.state.decrement('workers_processing')
            self.videos_processed += 1

        except Queue.Empty:
            _log.debug("Q,Empty")
            time.sleep(options.dequeue_period * random.random())

        except DequeueError:
            # Already logged
            time.sleep(options.dequeue_period * random.random())

        except UninterestingJob:
            if self.cur_message:
                yield self.job_queue.delete_message(self.cur_message)
        
        except Exception as e:
            statemon.state.increment('unknown_exception')
            _log.exception("Unexpected exception [%s]: %s"
                           % (self.pid, e))
            time.sleep(options.dequeue_period * random.random())

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
        worker.join(600.0) # 10min timeout to finish the job
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
