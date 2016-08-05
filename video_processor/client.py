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
import cmsapiv2.client
from cmsdb import neondata
import concurrent.futures
import cv2
import dateutil.parser
import ffvideo
import hashlib
import integrations
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
import utils.autoscale
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
statemon.define('integration_error', int)
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
statemon.define('failed_to_send_result_email', int)
statemon.define('too_long_of_video', int)
statemon.define('unable_to_send_email', int)

# ======== Parameters  =======================#
from utils.options import define, options
define('model_file', default=None, help='File that contains the model')
define('model_server_port', default=9000, type=int,
       help="the port currently being used by model servers")
define('model_autoscale_groups', default='AquilaOnDemand', type=str,
       help="Comma separated list of autoscaling group names")
define('request_concurrency', default=22, type=int,
       help=("the maximum number of concurrent scoring requests to"
             " make at a time. Should be less than or equal to the"
             " server batch size."))
define('max_videos_per_proc', default=100,
       help='Maximum number of videos a process will handle before respawning')
define('dequeue_period', default=10.0,
       help='Number of seconds between dequeues on a worker')
define('notification_api_key', default='icAxBCbwo--owZaFED8hWA',
       help='Api key for the notifications')
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

define("cmsapi_user", default=None, help='User to make api requests with')
define("cmsapi_pass", default=None, help='Password for the cmsapi user')
define("frontend_base_url", default='https://app.neon-lab.com', help='The base url for the frontend')

class VideoError(Exception): pass 
class BadVideoError(VideoError): pass
class DefaultThumbError(VideoError): pass  
class VideoDownloadError(VideoError, IOError): pass  
class PredictionError(VideoError): pass
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
                            self.job_params.get('api_param', None) or 6)
        self.n_thumbs = max(self.n_thumbs, 1)
        self.m_thumbs = int(self.job_params.get('botm', None) or 6)
        self.m_thumbs = max(self.m_thumbs, 1)

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
        self.bad_thumbnails = []

        self.thumb_model_version = None

        self.executor = concurrent.futures.ThreadPoolExecutor(10)

    def __del__(self):
        # Clean up the executor
        self.executor.shutdown(False)

    @tornado.gen.coroutine
    def update_video_metadata_video_info(self):
        '''Updates information about the video on the video metadata object
        in the database.
        '''
        def _update_video_info(x):
            x.duration = self.video_metadata.duration
            x.publish_date = self.video_metadata.publish_date
            x.frame_size = x.frame_size or self.video_metadata.frame_size

        try:
            yield neondata.VideoMetadata.modify(self.video_metadata.key,
                                                _update_video_info,
                                                async=True)
        except Exception as e:
            _log.error("Error updating video data to database: %s" % e)
            statemon.state.increment('save_vmdata_error')
            raise DBError("Error updating video data to database")

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
            n_thumbs = max(self.n_thumbs, 6)
            m_thumbs = max(self.m_thumbs, 6)

            with self.cv_semaphore:
                statemon.state.increment('workers_cv_processing')
                try:
                    yield self.process_video(self.tempfile.name,
                                             n_thumbs=n_thumbs,
                                             m_thumbs=m_thumbs)
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

            if api_request is None:
                _log.warn('Job %s for account %s was deleted, so ignore' %
                          (self.job_params['job_id'],
                           self.job_params['api_key']))
                yield self.job_queue.delete_message(self.job_message)

            elif api_request.state == neondata.RequestState.REQUEUED:
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
                 
                # modify accountlimits to have one less video post
                def _modify_limits(al): 
                    al.video_posts -= 1 
                yield neondata.AccountLimits.modify( 
                    api_request.api_key, 
                    _modify_limits, 
                    async=True) 
       
        finally:
            #Delete the temp video file which was downloaded
            self.tempfile.close()

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
                    yield self.update_video_metadata_video_info()
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
            dl_params['noplaylist'] = True
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
                    # Distinguish between /playlist and /watch?list= urls:
                    # skip the former and download the latter.
                    elif result_type == 'playlist' and 'entries' not in video_info:
                        # This effectively strips list and index parameters from the playlist url.
                        cur_url = video_info['webpage_url']
                        continue
                    elif result_type == 'video':
                        # If type playlist, we get the first or current video.
                        found_video = True
                    else:
                        # They gave us another type of url
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
                yield self.update_video_metadata_video_info()

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
            # If this video came from an integration, then try to
            # refresh the url and re-download.
            if (self.video_metadata.integration_id is not None and 
                self.video_metadata.integration_id != '0'):
                new_url = None
                try:
                    db_integration = yield neondata.AbstractIntegration.get(
                        self.video_metadata.integration_id,
                        async=True)
                    integration = integrations.create_ovp_integration(
                        self.job_params['api_key'], db_integration)
                    video_info = yield integration.lookup_videos(
                        [self.job_params['video_id']])
                    if len(video_info) == 1:
                        new_url = integration.get_video_url(video_info[0])
                except Exception as integ_exception:
                    _log.warn('Unable to build OVP integration %s: %s' %
                              (self.video_metadata.integration_id,
                               integ_exception))
                    statemon.state.increment('integration_error')
                if new_url is not None and new_url != self.video_url:
                    _log.info('Video %s has moved to %s. '
                              'Trying to download at its new location' % 
                              (self.video_metadata.key, new_url))
                    self.video_url = new_url
                    self.video_metadata.url = new_url
                    yield self.download_video_file()
                    return

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

        except DBError:
            raise

        except IOError as e:
            msg = "Error saving video to disk: %s" % e
            _log.error(msg)
            statemon.state.increment('video_download_error')
            raise VideoDownloadError(msg)

    @tornado.gen.coroutine
    def process_video(self, video_file, n_thumbs=1, m_thumbs=0):
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
        # grab the accoutlimits for this account (if they exist) 
        account_limits = yield neondata.AccountLimits.get( 
            self.job_params['api_key'],
            log_missing=False, 
            async=True)

        if account_limits: 
            max_duration = account_limits.max_video_size
            if duration > max_duration: 
                statemon.state.increment('too_long_of_video')
                # lets modify the apirequest to set fail_count
                # to max fails, so that it doesn't retry this 
                def _modify_request(r): 
                    r.fail_count = options.max_fail_count
                def _seconds_to_hms_string(secs): 
                    m, s = divmod(secs, 60) 
                    h, m = divmod(m, 60)
                    return "%d:%d:%d" % (h,m,s)

                yield neondata.NeonApiRequest.modify(
                    self.job_params['job_id'],
                    self.job_params['api_key'],
                    _modify_request, 
                    async=True)
                
                raise BadVideoError('Video length %s is too long for this'\
                                    'account. The maximum video length that'\
                                    'can be processed for this account is %s.' % (
                                    _seconds_to_hms_string(duration), 
                                    _seconds_to_hms_string(max_duration))) 
 
        if duration <= 1e-3:
            _log.error("Video %s has no length" % (self.video_url))
            statemon.state.increment('video_read_error')
            raise BadVideoError("Video has no length")

        #Log long videos
        if duration > 1800:
            statemon.state.increment('video_duration_30m')
        if duration > 3600:
            statemon.state.increment('video_duration_60m')

        yield self.update_video_metadata_video_info()

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
            top_results, bottom_results = \
              self.model.choose_thumbnails(
                  mov,
                  n=n_thumbs,
                  m=m_thumbs,
                  video_name=self.video_url)
            top_results = sorted(top_results, key=lambda x: x.score, reverse=True)
            bottom_results = sorted(bottom_results, key=lambda x: x.score)
        except model.errors.VideoReadError:
            msg = "Error using OpenCV to read video. %s" % self.video_url
            _log.error(msg)
            statemon.state.increment('video_read_error')
            raise BadVideoError(msg)
        except model.errors.PredictionError as e:
            raise PredictionError(e.message)

        rank=0
        for result in top_results:
            meta = neondata.ThumbnailMetadata(
                None,
                internal_vid=self.video_metadata.key,
                ttype=neondata.ThumbnailType.NEON,
                model_score=result.score,
                model_version=result.model_version,
                features=result.features,
                frameno=result.frameno,
                rank=rank,
                filtered=result.filtered_reason)
            self.thumb_model_version = result.model_version
            self.thumbnails.append((meta, PILImageUtils.from_cv(result.image)))
            rank += 1

        for result in bottom_results:
            meta = neondata.ThumbnailMetadata(
                None,
                internal_vid=self.video_metadata.key,
                ttype=neondata.ThumbnailType.BAD_NEON,
                model_score=result.score,
                model_version=result.model_version,
                features=result.features,
                frameno=result.frameno,
                filtered=result.filtered_reason)
            self.bad_thumbnails.append((meta, PILImageUtils.from_cv(result.image)))

        # Get the baseline frames of the video
        yield self._get_center_frame(video_file)
        yield self._get_random_frame(video_file)

        statemon.state.increment('processed_video')
        _log.info('Sucessfully finished searching video %s' % self.video_url)

    @tornado.gen.coroutine
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
                internal_vid=self.video_metadata.key,
                ttype=neondata.ThumbnailType.CENTERFRAME,
                frameno=int(nframes / 2),
                rank=0)
            self.thumbnails.append((meta, PILImageUtils.from_cv(cv_image)))
        except Exception, e:
            _log.error("Unexpected error extracting center frame from %s:"
                       " %s" % (self.video_url, e))
            statemon.state.increment('centerframe_extraction_error')
            raise

    @tornado.gen.coroutine
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
                internal_vid=self.video_metadata.key,
                ttype=neondata.ThumbnailType.RANDOM,
                frameno=frameno,
                rank=0)
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
                _log.warn('Job %s was deleted while processing it. Ignoring' %
                          job_id)
                return
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

        # Get any known thumbs for the video
        known_thumbs = []
        known_video = yield neondata.VideoMetadata.get(self.video_metadata.key,
                                                       async=True)
        if known_video:
            known_tids = reduce(
                lambda x,y: x | y,
                [set(x.thumbnail_ids) for x in known_video.job_results],
                set())
            known_tids |= set(known_video.thumbnail_ids + known_video.bad_thumbnail_ids)

            known_thumbs = yield neondata.ThumbnailMetadata.get_many(
                known_tids, async=True)

        # Attach the thumbnails to the video. This will upload the
        # thumbnails to the appropriate CDNs.
        video_result = neondata.VideoJobThumbnailList(
            age=self.job_params.get('age'),
            gender=self.job_params.get('gender'),
            model_version=self.model_version)
        if len(filter(lambda x: x[0].type == neondata.ThumbnailType.NEON,
                      self.thumbnails)) < 1:
            # TODO (Sunil): Video to be marked as failed or int err ? 
            statemon.state.increment('no_thumbs')
            _log.warn("No thumbnails extracted for video %s url %s"\
                    % (self.video_metadata.key, self.video_metadata.url))

        for thumb_meta, image in self.thumbnails:
            # If we have a thumbnail of this type already and it's
            # scored with the same model, we do not need to keep the
            # new one around
            same_thumbs = [x for x in known_thumbs if 
                           x.type == thumb_meta.type and 
                           x.model_version == self.thumb_model_version]
            if (thumb_meta.type != neondata.ThumbnailType.NEON and 
                len(same_thumbs) > 0):
                same_thumbs = sorted(same_thumbs, key=lambda x: x.rank)
                video_result.thumbnail_ids.append(same_thumbs[0].key)
            else:
                # Fill out the data on this thumb and add it to the results
                yield thumb_meta.add_image_data(image, self.video_metadata,
                                                cdn_metadata,
                                                async=True)
                yield thumb_meta.score_image(self.model.predictor,
                                             image=PILImageUtils.to_cv(image))
                video_result.thumbnail_ids.append(thumb_meta.key)

        for thumb_meta, image in self.bad_thumbnails:
            yield thumb_meta.add_image_data(image, self.video_metadata,
                                            cdn_metadata,
                                            async=True)
            try:
                yield thumb_meta.score_image(self.model.predictor,
                                             image=PILImageUtils.to_cv(image))
            except model.errors.PredictionError as e:
                _log.warn('Error scoring image: %s' % e)
                # It's ok if it's not scored, so continue
            video_result.bad_thumbnail_ids.append(thumb_meta.key)

        # Save the thumbnail and video data into the database
        # TODO(mdesnoyer): do this as a single transaction
        def _merge_thumbnails(t_objs):
            for new_thumb, _ in self.thumbnails + self.bad_thumbnails:
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
                old_thumb.features = new_thumb.features
                old_thumb.rank = new_thumb.rank
                old_thumb.phash = new_thumb.phash
                old_thumb.frameno = new_thumb.frameno
                old_thumb.filtered = new_thumb.filtered
                old_thumb.features = new_thumb.features
        try:
            new_thumb_dict = yield neondata.ThumbnailMetadata.modify_many(
                [x[0].key for x in self.thumbnails + self.bad_thumbnails],
                _merge_thumbnails,
                create_missing=True,\
                async=True)
            if len(self.thumbnails) + len(self.bad_thumbnails) > 0 and len(new_thumb_dict) == 0:
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
                         video_result.thumbnail_ids)
            video_obj.thumbnail_ids = [x for x in tidset]

            # If there isn't a job result from before, but something
            # is there, then create the job result object from the
            # previous run
            if self.reprocess:
                prev_thumbs = [x.key for x in thumbs if x.type in [
                    neondata.ThumbnailType.NEON,
                    neondata.ThumbnailType.CENTERFRAME,
                    neondata.ThumbnailType.RANDOM]]
                if len(prev_thumbs) > 0 and len(video_obj.job_results) == 0:
                    video_obj.job_results.append(
                        neondata.VideoJobThumbnailList(
                            thumbnail_ids = prev_thumbs,
                            bad_thumbnail_ids = video_obj.bad_thumbnail_ids,
                            model_version=video_obj.model_version))
                    video_obj.non_job_thumb_ids = keep_thumbs
            
            # Update the job results
            found_result = False
            for result in video_obj.job_results:
                if (result.age == video_result.age and 
                    result.gender == video_result.gender):
                    # Replace the last run with these parameters
                    result.thumbnail_ids = video_result.thumbnail_ids
                    result.bad_thumbnail_ids = video_result.bad_thumbnail_ids
                    result.model_version = video_result.model_version
                    found_result = True
            if not found_result:
                video_obj.job_results.append(video_result)
            
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
            thumb = yield api_request.save_default_thumbnail(cdn_metadata,
                                                             async=True)

            # Score the default thumb
            if thumb is not None:
                # TODO(mdesnoyer): This will potentially download the
                # image twice. It would be better to avoid that, but
                # the code flow makes it annoying. Given this is a
                # long process, not a big deal, but we might want to
                # fix that
                yield thumb.score_image(self.model.predictor,
                                        save_object=True)

        except (neondata.ThumbDownloadError,
                model.errors.PredictionError) as e:
            # If we extracted the default thumb from the url, then
            # don't error out if we cannot get thumb
            if is_user_default_thumb:
                _log.warn("Default thumbnail download failed for vid %s" %
                          video_id)
                statemon.state.increment('default_thumb_error')
                err_msg = "Failed to download default thumbnail: %s" % e
                raise DefaultThumbError(err_msg)

        # Set the association of the video tag and each thumbnail.
        if(new_video_metadata.tag_id):
            _tag_thumb_ids = (video_result.thumbnail_ids +
                video_result.bad_thumbnail_ids +
                new_video_metadata.non_job_thumb_ids)
            ct = yield neondata.TagThumbnail.save_many(
                tag_id=new_video_metadata.tag_id,
                thumbnail_id=_tag_thumb_ids,
                async=True)

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
        new_request = None
        try:
            new_request = yield neondata.NeonApiRequest.modify(
                api_request.job_id,
                api_request.api_key,
                _flag_request_done_in_db,
                async=True)
            if not new_request:
                raise DBError('Api Request finished failed. It was not there')
        except Exception, e:
            _log.error("Error writing request state to database: %s" % e)
            statemon.state.increment('modify_request_error')
            raise DBError("Error finishing api request")

        try:
            if new_request:
                yield new_request.send_callback(async=True)
        except Exception as e:
            # Logging already done and we do not want this to stop the
            # flow if there's an error
            pass

        # Send the notifications
        yield self.send_notification_email(api_request, new_video_metadata)

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
    def send_notification_email(self, api_request, video): 
        """ 
            sends email to the email that is on the 
            api_request 

            returns True on success False on failure
          
            does not raise  
        """ 
        rv = True
        try: 
            # check for user that created the request 
            to_email = api_request.callback_email
            if not to_email: 
                raise tornado.gen.Return(True)  
                     
            user = yield neondata.User.get(
                to_email,
                log_missing=False,  
                async=True)

            # if we have a user, check if they are subscribed
            if user: 
                if not user.send_emails: 
                    raise tornado.gen.Return(True)  
            # create a new apiv2 client
            client = cmsapiv2.client.Client(
                options.cmsapi_user,
                options.cmsapi_pass)
            
            template_args = yield self._get_email_template_args(video)
               
            # build up the body of the request
            body_params = { 
                'template_slug' : 'video-results',
                'template_args' : template_args,  
                'subject' : 'Your Neon Images Are Here!', 
                'from_name' : 'Neon Video Results', 
                'to_email_address' : to_email 
            }     
            # make the call to post email
            relative_url = '/api/v2/%s/email' % api_request.api_key
            http_req = tornado.httpclient.HTTPRequest(
                relative_url, 
                method='POST', 
                headers = {"Content-Type" : "application/json"},
                body=json.dumps(body_params))

            response = yield client.send_request(http_req)
            if response.error: 
                statemon.state.increment('failed_to_send_result_email')
                _log.error('Failed to send email to %s due to %s' % 
                    (to_email, response.error))
                rv = False
             
        except AttributeError: 
            pass
        except tornado.gen.Return:
            raise
        except Exception as e:
            rv = False  
            statemon.state.increment('unable_to_send_email')
            _log.exception('Unexpected error %s when sending email' % e)  
        finally: 
            raise tornado.gen.Return(rv) 

    @tornado.gen.coroutine    
    def _get_email_template_args(self, video):
        tas = {}  
        thumbs = yield neondata.ThumbnailMetadata.get_many(
            video.thumbnail_ids,
            async=True)

        dt = filter(lambda t: t.type == neondata.ThumbnailType.DEFAULT,
            thumbs)
        rt = filter(lambda t: t.type != neondata.ThumbnailType.DEFAULT, 
            thumbs)

        if len(dt) == 0 or len(rt) < 4:
            raise Exception('Not enough thumbnails to process.')
         
        tas['collection_url'] = \
           "{base_url}/share/video/{vid}/account/{aid}/token/{token}/".format(
               base_url=options.frontend_base_url, 
               vid=neondata.InternalVideoID.to_external(video.key), 
               aid=video.get_account_id(), 
               token=video.share_token) 
        
        th_info = sorted(
            [(t.urls[0], t.get_estimated_lift(dt[0])) for t in rt], 
            key=lambda x: x[1], 
            reverse=True)

        tas['top_thumbnail'] = th_info[0][0]
        tas['lift'] = "{0:.0f}%".format(float(th_info[0][1] * 100)) 
  
        tas['thumbnail_one'] = th_info[1][0]
        tas['thumbnail_two'] = th_info[2][0] 
        tas['thumbnail_three'] = th_info[3][0]

        raise tornado.gen.Return(tas)

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

    def load_model(self, job):
        ''' load model '''
        _log.info('Generating predictor instance')
        aquila_conn = utils.autoscale.MultipleAutoScaleGroups(
            options.model_autoscale_groups.split(','))
        predictor = model.predictor.DeepnetPredictor(
            port=options.model_server_port,
            concurrency=options.request_concurrency,
            aquila_connection=aquila_conn,
            gender=job.get('gender'),
            age=job.get('age'))
        predictor.connect()
        self.model_version = os.path.basename(self.model_file)
        # note, the model file is no longer a complete model, but is instead
        # an input dictionary for local search.
        self.model = model.generate_model(self.model_file, predictor)
        if not self.model:
            statemon.state.increment('model_load_error')
            _log.error('Error loading the Model from %s' % self.model_file)
            raise IOError('Error loading model from %s' % self.model_file)
        # TODO (someone): How tf are we gonna handle exiting? Remember that
        #                 gRPC hangs due to a bug at exit.

    def unload_model(self):
        if self.model is not None:
            if self.model.predictor is not None:
                self.model.predictor.shutdown()
            self.model = None

    def run(self):
        ''' run/start method '''
        # The worker should ignore the SIGTERM because it will be
        # killed by the master thread via self.kill_received
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        
        _log.info("starting worker [%s] " % (self.pid))
        
        while (not self.kill_received.is_set() and 
               self.videos_processed < options.max_videos_per_proc):
            self.do_work()
        if self.model is not None:
            del self.model
 
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
            self.load_model(job)
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
                self.unload_model()
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
