'''A module of tools to help download videos

Copyright: 2016 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import boto.exception
from boto.s3.connection import S3Connection
import concurrent.futures
import logging
import re
import socket
import shutil
import subprocess
import tempfile
import tornado.gen
import urlparse
from utils.options import define, options
import youtube_dl

_log = logging.getLogger(__name__)

class FFmpegRotatorPP(youtube_dl.postprocessor.FFmpegPostProcessor):

    @staticmethod
    def get_ffprobe_path():
        return '/usr/local/bin/ffprobe'

    def __init__(self, ydl, output_path):
        self.output_path = output_path
	super(FFmpegRotatorPP, self).__init__(ydl)

    def run(self, information):
        path = information['filepath']
        # ffmpeg without any option will auto-rotate.

        proc = subprocess.Popen(
            [FFmpegRotatorPP.get_ffprobe_path(), path],
            stderr=subprocess.PIPE)
        _, probe = proc.communicate()

        if probe and 'rotate' in probe.lower():
            try:
                self.run_ffmpeg(path, self.output_path, [])
            except Exception as e:
                _log.warn('Failed in video autorotate %s' % e)
                if os.path.exists(path):
                    shutil.move(path, self.output_path)
        else:
            shutil.move(path, self.output_path)


        information['filepath'] = self.output_path
        return [path], information

define('max_bandwidth_per_core', default=15500000.0,
       help='Max bandwidth in bytes/s')
define('temp_dir', default=None, 
       help='Directory where videos will be downloaded to')

class VideoDownloadError(IOError): pass

class VideoDownloader(object):

    @staticmethod
    def get_ffmpeg_path():
        return '/usr/local/bin/ffmpeg'

    def __init__(self, url, throttle=False):
        '''Intitalize the downloader for one download.

        Inputs:
        url - Url to download
        throttle - Should this download be throttled?
        '''
        self.url = url
        
        self.video_info = {} # Dictionary of info in youtube_dl format

        # Temporary file where the video is stored on disk
        self.tempfile = tempfile.NamedTemporaryFile(
            suffix='.mp4', delete=True, dir=options.temp_dir)

        # S3 Specific fields
        s3re = re.compile('((s3://)|(https?://[a-zA-Z0-9\-_]+\.amazonaws'
                          '\.com/))([a-zA-Z0-9\-_\.]+)/(.+)')
        self.s3match = s3re.search(self.url)
        self.s3key = None

        # YouTube Dl object
        dl_params = {}
        dl_params['noplaylist'] = True
        dl_params['ratelimit'] = (options.max_bandwidth_per_core 
                                  if throttle else None)
        dl_params['restrictfilenames'] = True
        dl_params['outtmpl'] = unicode(str(
            os.path.join(options.temp_dir or '/tmp',
                         '%(id)s.%(ext)s')))
        # Specify for formats that we want in order of preference
        dl_params['format'] = (
            'best[ext=mp4][height<=720][protocol^=?http]/'
            'best[ext=mp4][protocol^=?http]/'
            'best[height<=720][protocol^=?http]/'
            'best[protocol^=?http]/'
            'best/'
            'bestvideo')
        dl_params['logger'] = _log
        dl_params['ffmpeg_location'] = VideoDownloader.get_ffmpeg_path()
        self.ydl = youtube_dl.YoutubeDL(dl_params)

        # Set post processor to apply metadata rotation.
        self._add_rotate_post_processor()

        self.executor = concurrent.futures.ThreadPoolExecutor(5)

    def __del__(self):
        self.close()

    def close(self):
        self.tempfile.close()
        self.executor.shutdown(False)

    def get_local_filename(self):
        return self.tempfile.name

    @tornado.gen.coroutine
    def get_video_info(self):
        '''Retrieves information about the video without downloading it.

        Returns a dictionary of video information in the same form as
        YouTubeDL. See extractor/common.py for more details.
        '''
        if self.video_info:
            raise tornado.gen.Return(self.video_info)
        try:
            if self.s3match:
                try:
                    s3key = yield self._get_s3_key()
                    self.video_info['url'] = self.url
                    self.video_info['filesize'] = s3key.size
                    raise tornado.gen.Return(self.video_info)
                except boto.exception.S3ResponseError as e:
                    _log.warn('Error getting video url %s via boto. '
                              'Falling back on http: %s' % (self.url, e))
            found_video = False
            while not found_video:
                self.video_info = yield self.executor.submit(
                    self.ydl.extract_info,
                    self.url, download=False)
                result_type = self.video_info.get('_type', 'video')
                if result_type == 'url':
                    # Need to step to the next url
                    self.url = self.video_info['url']
                    continue
                # Distinguish between /playlist and /watch?list= urls:
                # skip the former and download the latter.
                elif (result_type == 'playlist' and 
                      'entries' not in self.video_info):
                    # This effectively strips list and index
                    # parameters from the playlist url.
                    self.url = self.video_info['webpage_url']
                    continue
                elif result_type == 'video':
                    # If type playlist, we get the first or current video.
                    found_video = True
                else:
                    # They gave us another type of url
                    msg = ('Unhandled video type %s' %
                           (result_type))
                    raise youtube_dl.utils.DownloadError(msg)

            raise tornado.gen.Return(self.video_info)
        except (youtube_dl.utils.DownloadError,
                youtube_dl.utils.ExtractorError, 
                youtube_dl.utils.UnavailableVideoError,
                socket.error) as e:
            msg = "Error getting video info from %s: %s" % (self.url, e)
            _log.error(msg)
            raise VideoDownloadError(msg)
        
        except boto.exception.BotoClientError as e:
            msg = ("Client error getting video info %s from S3: %s" % 
                   (self.url, e))
            _log.error(msg)
            raise VideoDownloadError(msg)

        except boto.exception.BotoServerError as e:
            msg = ("Server error getting video info %s from S3: %s" %
                   (self.url, e))
            _log.error(msg)
            raise VideoDownloadError(msg)

        except IOError as e:
            msg = "Error saving video to disk: %s" % e
            _log.error(msg)
            raise VideoDownloadError(msg)
            

    @tornado.gen.coroutine
    def download_video_file(self):
        '''Downloads the video file to disk.'''
        video_info = yield self.get_video_info()
        
        _log.info('Downloading %s' % self.url)

        try:
            if self.s3key is not None:
                try:
                    yield self.executor.submit(
                        self.s3key.get_contents_to_file, self.tempfile)
                    yield self.executor.submit(self.tempfile.flush)
                    return
                except boto.exception.S3ResponseError as e:
                    _log.warn('Error getting video url %s via boto. '
                              'Falling back on http: %s' % (self.url, e))

            # Use Youtube DL. This can handle a ton of different video sources
            self.video_info = yield self.executor.submit(
                self.ydl.extract_info,
                self.url, download=True)

        except (youtube_dl.utils.DownloadError,
                youtube_dl.utils.ExtractorError, 
                youtube_dl.utils.UnavailableVideoError,
                socket.error) as e:
            msg = "Error downloading video from %s: %s" % (self.url, e)
            _log.error(msg)
            raise VideoDownloadError(msg)
        
        except boto.exception.BotoClientError as e:
            msg = ("Client error downloading video %s from S3: %s" % 
                   (self.url, e))
            _log.error(msg)
            raise VideoDownloadError(msg)

        except boto.exception.BotoServerError as e:
            msg = ("Server error downloading video %s from S3: %s" %
                   (self.url, e))
            _log.error(msg)
            raise VideoDownloadError(msg)

    @tornado.gen.coroutine
    def _get_s3_key(self):
        '''Gets the S3 key object for the video.'''
        if self.s3key is None:
            bucket_name = self.s3match.group(4)
            key_name = self.s3match.group(5)
            s3conn = S3Connection()
            bucket = yield self.executor.submit(s3conn.get_bucket, bucket_name)
            self.s3key = yield self.executor.submit(bucket.get_key, key_name)
        raise tornado.gen.Return(self.s3key)

    def _add_rotate_post_processor(self):
        '''Add the post processor to apply/strip metadata rotation'''
        rotate_processor = FFmpegRotatorPP(self.ydl, self.tempfile.name)
        self.ydl.add_post_processor(rotate_processor)
