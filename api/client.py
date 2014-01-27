#!/usr/bin/python
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

import datetime
import gzip
import hashlib
import model
import properties
import Queue
import random
import shutil
import signal
import tempfile
import tornado.web
import tornado.gen
import tornado.escape
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import time
import numpy as np
from PIL import Image
import ffvideo 

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from StringIO import StringIO

import tarfile

import brightcove_api
import youtube_api

from supportServices.neondata import NeonApiRequest, BrightcoveApiRequest
from supportServices.neondata import NeonPlatform, YoutubePlatform, \
        BrightcovePlatform, VideoMetadata, RequestState, InternalVideoID
from supportServices.neondata import ThumbnailID, ThumbnailType, \
        ThumbnailURLMapper, ThumbnailMetaData, ThumbnailIDMapper

import gc
import pprint

import logging
_log = logging.getLogger(__name__)

from pympler import summary
from pympler import muppy
from pympler.classtracker import ClassTracker
import pickle

import utils.neon
from utils.http import RequestPool

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
        for worker in workers:
            worker.kill_received = True
    except Exception, e:
        sys.exit(0)

###########################################################################
# Process Video File
###########################################################################

class ProcessVideo(object):
    """ class provides methods to process a given video """
    def __init__(self, request_map, request, model, model_version, debug, cur_pid):
        self.request_map = request_map
        self.request = request
        self.model = model
        self.model_version = model_version
        self.frames = []
        self.data_map = {} # frameNo -> (score, image_rgb)
        self.attr_map = {}
        self.timecodes = {}
        self.center_frame = None
        self.frame_size_width = 256
        self.sec_to_extract = 1 
        self.base_filename = request_map[properties.API_KEY] + "/" + \
                request_map[properties.REQUEST_UUID_KEY]  #Used as direcrtory name
        self.sec_to_extract_offset = 1.0 

        if request_map.has_key(properties.THUMBNAIL_RATE):
            self.sec_to_extract_offset = random.choice([0.20, 0.25, 0.5]) 

        self.sec_to_extract_offset = 1
        self.valence_scores = [[], []] #x,y 

        #Video Meta data
        self.video_metadata = {}
        self.video_metadata['codec_name'] = None
        self.video_metadata['duration'] = None
        self.video_metadata['framerate'] = None
        self.video_metadata['bitrate'] = None
        self.video_metadata['frame_size'] = None
        
        self.video_size = 0  # Calulated from bitrate and duration

        #S3 Stuff
        self.s3conn = S3Connection(properties.S3_ACCESS_KEY,
                                   properties.S3_SECRET_KEY)
        self.s3bucket_name = properties.S3_BUCKET_NAME
        self.s3bucket = Bucket(name=self.s3bucket_name,
                               connection=self.s3conn)
        self.format = "JPEG" #"PNG"

        #AB Test Data
        self.abtest_thumbnails = {}

        #thumbnail list of maps
        self.thumbnails = [] # ThumbnailMetaData
        
        self.debug = debug
        self.pid = cur_pid

    def process_all(self, video_file, n_thumbs=1):
        ''' process all the frames from the partial video downloaded '''
        start_process = time.time()
        try:
            mov = ffvideo.VideoStream(video_file)
            if self.video_metadata['codec_name'] is None:
                self.video_metadata['codec_name'] = mov.codec_name
                self.video_metadata['duration'] = mov.duration
                self.video_metadata['framerate'] = mov.framerate
                self.video_metadata['bitrate'] = mov.bitrate
                self.video_metadata['frame_size'] = mov.frame_size
                self.video_size = mov.duration * mov.bitrate / 8 # in bytes
        except Exception, e:
            _log.error("key=process_video worker[%s] " 
                        " msg=%s "  %(self.pid, e.message))
            return

        duration = mov.duration

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

        if self.debug:
            _log.info("key=process_all current time=%s " %(self.sec_to_extract))

        for image, score, frame_no, timecode, attribute in results:
            if attribute is not None and attribute == '':
                self.valence_scores[0].append(timecode)
                self.valence_scores[1].append(score)
                self.timecodes[frame_no] = timecode
                self.data_map[frame_no] = (score, image[:, :, ::-1])
                self.attr_map[frame_no] = attribute
        
        #del reference to stream object
        del mov
        
        end_process = time.time()
        if self.debug:
            _log.info("key=streaming_callback msg=debug " 
                    "time_processing=%s" %(end_process - start_process))

    def finalize(self, video_file):
        ''' method that is run before the video is deleted after downloading 
        use this to run cleanup code or misc methods '''
        return

    ############# THUMBNAIL METHODS ##################

    def get_center_frame(self, video_file):
        '''approximation of brightcove logic 
         #Note: Its unclear the exact nature of brighcove thumbnailing,
         the images are close but this is not the exact frame
        '''
        try:
            mov = ffvideo.VideoStream(video_file)
            mid = int(mov.duration / 2)
            mid_frame = mov.get_frame_at_sec(mid)
            return mid_frame.image()
        except Exception,e:
            _log.debug("key=get_center_frame msg=%s"%e)

    def get_timecodes(self, frames):
        ''' frame -> timecode '''
        result = []
        for f in frames:
            result.append(self.timecodes[f])
        return result

    def get_topn_thumbnails(self, n):
        ''' topn '''
        res = self.top_thumbnails_per_interval(nthumbnails = n)
        return res

    def get_thumbnail_at_rate(self, rate):
        ''' thumbnails per second ''' 
        # rate = 1 ; 1 thumbnail per sec

        def get_intervals(interval, r):
            for i in range(r):
                if i % interval == 0:
                    yield i

        data = self.data_map.items()
        interval = rate
        res = []
        
        #Generate intervals to extract best thumbnail from
        intervals = list(get_intervals(interval, 
                                       int( len(self.data_map) * 
                                            self.sec_to_extract_offset)))

        # Sort according to frame numbers
        frames = sorted(data, key=lambda tup: tup[0], reverse=False) 
        frms = [ x[0] for x in frames ]
        prev_intv = 0
        for i,intv in zip(range(len(intervals)), intervals):
            intv = int(intv / self.sec_to_extract_offset)
            if i > 0:
                data_slice = frames[prev_intv :intv]
                result = sorted(data_slice, 
                                key=lambda tup: tup[1], 
                                reverse=True)
                res.append(result[0])
            prev_intv = intv
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

    def save_data_to_s3(self):
        ''' tar.gz a few thumbs to s3 ''' 
        s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
        s3bucket = s3conn.get_bucket(self.s3bucket_name)

        try:
            # Save images to S3 
            tmp_tar_file = tempfile.NamedTemporaryFile(delete=True)
            tar_file = tarfile.TarFile(tmp_tar_file.name,"w")
            
            samples = 50
            nframes = len(self.data_map)
            sample_size = nframes if nframes < samples else samples 
            frame_nos = random.sample(self.data_map,sample_size)

            for frame_no in frame_nos:
                score = self.data_map[frame_no][0]
                image = Image.fromarray(self.data_map[frame_no][1])
                # appends frame_no +  attribute folder name + prediction score
                fname = 'thumbnail_' + str(frame_no) + "_" + self.attr_map[frame_no]  + "_" + str(score) + "." + self.format
                filestream = StringIO()
                image.save(filestream, self.format)
                filestream.seek(0)
                info = tarfile.TarInfo(name=fname)
                info.size = len(filestream.buf)
                tar_file.addfile(tarinfo=info, fileobj=filestream)
            tar_file.close()

            gzip_file = tempfile.NamedTemporaryFile(
                delete = properties.DELETE_TEMP_TAR)
            gz = gzip.GzipFile(filename=gzip_file.name, mode='wb')
            tmp_tar_file.seek(0)
            gz.write(tmp_tar_file.read())
            gz.close()
            
            if not options.local:
                #Save gzip file to S3
                keyname = self.base_filename + "/thumbnails.tar.gz"
                k = s3bucket.new_key(keyname)
                k.set_contents_from_filename(gzip_file.name)
            else:
                _log.info("thumbnails saved to " + gzip_file.name)
                if not os.path.exists('results'):
                    os.mkdir('results')
                fname = "thumbnails-" + self.request_map[properties.REQUEST_UUID_KEY] + '.tar.gz'
                shutil.copy(gzip_file.name,'results/' + fname)
                

            tmp_tar_file.close()
            gzip_file.close()

        except S3ResponseError,e:
            _log.error("key=save_to_s3 msg=s3 response error " + e.__str__() )
        except Exception,e:
            _log.error("key=save_to_s3 msg=general exception " + e.__str__() )
            if self.debug:
                raise
  
    def host_images_s3(self, frames):
        ''' Host images on s3 which is available publicly '''
        s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
        #s3bucket = Bucket(name = s3bucket_name,connection = s3conn)
        s3bucket = s3conn.get_bucket(s3bucket_name)
        
        fname_prefix = 'neon'
        fmt = 'jpeg'
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        s3_urls = []

        #upload the images to s3
        for i in range(len(frames)):
            filestream = StringIO()
            image = Image.fromarray(self.data_map[frames[i]][1])
            score = self.data_map[frames[i]][0]
            image.save(filestream, fmt, quality=100) 
            filestream.seek(0)
            imgdata = filestream.read()
            keyname = self.base_filename + "/" + fname_prefix + str(i) + "." + fmt
            k = s3bucket.new_key(keyname)
            k.set_contents_from_string(imgdata,{"Content-Type":"image/jpeg"})
            s3bucket.set_acl('public-read',keyname)
            s3fname = s3_url_prefix + "/" + self.base_filename + "/" + fname_prefix + str(i) + ".jpeg"
            s3_urls.append(s3fname)
            
            urls = []
            api_key = self.request_map[properties.API_KEY] 
            video_id = self.request_map[properties.VIDEO_ID]
            tid = ThumbnailID.generate(imgdata,
                                       InternalVideoID.generate(api_key,
                                                                video_id))
            urls.append(s3fname)
            created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            enabled = None 
            width   = image.size[0]
            height  = image.size[1] 
            ttype   = ThumbnailType.NEON 
            rank    = i +1 

            #populate thumbnails
            tdata = ThumbnailMetaData(tid, urls, created, width, height,
                        ttype, score, self.model_version, rank=rank)
            thumb = tdata.to_dict()
            self.thumbnails.append(thumb)
        return s3_urls

    def save_thumbnail_to_s3_and_metadata(self, image, score, s3bucket,
               keyname, s3fname, ttype, rank=0):
        
        fmt = 'jpeg'
        filestream = StringIO()
        image.save(filestream, fmt, quality=100) 
        filestream.seek(0)
        imgdata = filestream.read()
        k = s3bucket.new_key(keyname)
        k.set_contents_from_string(imgdata,{"Content-Type":"image/jpeg"})
        s3bucket.set_acl('public-read',keyname)
        
        urls = []
        api_key = self.request_map[properties.API_KEY] 
        video_id = self.request_map[properties.VIDEO_ID]
        tid = ThumbnailID.generate(imgdata,
                                   InternalVideoID.generate(api_key,
                                                            video_id))
        urls.append(s3fname)
        created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        width   = image.size[0]
        height  = image.size[1] 

        #populate thumbnails
        tdata = ThumbnailMetaData(tid, urls, created, width, height, ttype,
                score, self.model_version, rank=rank)
        thumb = tdata.to_dict()
        self.thumbnails.append(thumb)

    ############# Request Finalizers ##############
    
    def valence_score(self, image):
        ''' valence of pil.image '''
        im_array = np.array(image)
        im = im_array[:, :, ::-1]
        score,attr = self.model.score(im)
        return str(score)

    def save_video_metadata(self):
        '''
        Method to save video metadata in to the videoDB
        contains list of thumbnail ids 
        '''
        
        api_key = self.request_map[properties.API_KEY] 
        vid = self.request_map[properties.VIDEO_ID]
        i_vid = InternalVideoID.generate(api_key,vid)
        i_id = self.request_map[properties.INTEGRATION_ID] if self.request_map.has_key(properties.INTEGRATION_ID) else 0 
        job_id = self.request_map[properties.REQUEST_UUID_KEY]
        duration = self.video_metadata["duration"]
        video_valence = "%.4f" %float(np.mean(self.valence_scores[1])) 
        url = self.request_map[properties.VIDEO_DOWNLOAD_URL]
        model_version = self.model_version 
        frame_size = self.video_metadata['frame_size']

        tids = []

        #add thumbnail ids
        for thumb in self.thumbnails:
            tids.append(thumb["thumbnail_id"])

        vmdata = VideoMetadata(i_vid, tids, job_id, url, duration,
                    video_valence, model_version, i_id, frame_size)
        ret = vmdata.save()
        if not ret:
            _log.error("key=save_video_metatada msg=failed to save")


    def save_thumbnail_metadata(self, platform, i_id):
        '''Save the Thumbnail URL and ID to Mapper DB '''

        api_key = self.request_map[properties.API_KEY] 
        vid = self.request_map[properties.VIDEO_ID]
        job_id = self.request_map[properties.REQUEST_UUID_KEY]
        i_vid = InternalVideoID.generate(api_key,vid)

        thumbnail_mapper_list = []
        thumbnail_url_mapper_list = []
        if len(self.thumbnails) > 0:
            for thumb in self.thumbnails:
                tid = thumb["thumbnail_id"]
                for t_url in thumb["urls"]:
                    uitem = ThumbnailURLMapper(t_url, tid)
                    thumbnail_url_mapper_list.append(uitem)
                    item = ThumbnailIDMapper(tid, i_vid, thumb)
                    thumbnail_mapper_list.append(item)

            retid = ThumbnailIDMapper.save_all(thumbnail_mapper_list)
            returl = ThumbnailURLMapper.save_all(thumbnail_url_mapper_list)
            
            return (retid and returl)

    def finalize_neon_request(self, result=None):
        ''' Update the request state for Neon API Request '''
        api_key = self.request_map[properties.API_KEY] 
        job_id  = self.request_map[properties.REQUEST_UUID_KEY]
        video_id = self.request_map[properties.VIDEO_ID]
        api_request = NeonApiRequest.get(api_key,job_id)
        if not api_request:
            #TODO: more stuff ? 
            _log.error("key=finalize_neon_request msg=api request is null")
            return
      
        #change the status to requeued, and don't store a response 
        if result is None:
            api_request.state = RequestState.REQUEUED 
        elif len(self.thumbnails) == 0:
            api_request.state = RequestState.INT_ERROR
        else:
            
            try:
                #try decoding result, if error then video failed
                api_request.response = tornado.escape.json_decode(result)
                api_request.state = RequestState.FINISHED 
                api_request.publish_date = time.time() *1000.0 #ms
                
                #Save the mid frame (Random) thumbnail
                image = self.center_frame
                if image:
                    score  = self.valence_score(image) 
                    s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
                    s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
                    s3bucket = s3conn.get_bucket(s3bucket_name)
                    s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
                    keyname = self.base_filename + "/centerframe.jpeg" 
                    s3fname = s3_url_prefix + "/" + keyname
                    ttype = ThumbnailType.CENTERFRAME
                    self.save_thumbnail_to_s3_and_metadata(image, score, s3bucket,
                              keyname, s3fname, ttype, rank=0)
                else:
                    _log.error("key=finalize_neon_request msg=center frame is NULL")
      
            except:
                api_request.response = result 
                api_request.state = RequestState.FAILED
            
        ret = api_request.save()
        if ret:
            self.save_video_metadata()
            self.save_thumbnail_metadata("neon", 0)
        else:
            _log.error("key=finalize_neon_request msg=failed to save request")
        return


    def finalize_brightcove_request(self, result, error=False):
        '''
        Brightcove handler
        - host neon thumbs and also save bcove previous thumbnail in s3
        - Get Account settings and replace default thumbnail if enabled 
        - update request object with the thumbnails
        '''
        api_key = self.request_map[properties.API_KEY]  
        i_id = self.request_map[properties.INTEGRATION_ID]
        job_id  = self.request_map[properties.REQUEST_UUID_KEY]
        video_id = self.request_map[properties.VIDEO_ID]
        bc_request = BrightcoveApiRequest.get(api_key,job_id)
        bc_request.response = tornado.escape.json_decode(result)
        bc_request.publish_date = time.time() *1000.0 #ms

        if error:
            bc_request.save()
            return
        
        if len(self.thumbnails) == 0 :
            bc_request.state = RequestState.INT_ERROR
            bc_request.save()
        
        #Save previous thumbnail to s3
        if  bc_request.previous_thumbnail:
            p_url = bc_request.previous_thumbnail.split('?')[0]

            http_client = tornado.httpclient.HTTPClient()
            req = tornado.httpclient.HTTPRequest(url = p_url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)

            response = http_client.fetch(req)
            imgdata = response.body
            image = Image.open(StringIO(imgdata))
            score   = self.valence_score(image) 
            s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
            s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
            s3bucket = s3conn.get_bucket(s3bucket_name)
            s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
            keyname =  self.base_filename + "/brightcove.jpeg" 
            s3fname = s3_url_prefix + "/" + keyname
            ttype = ThumbnailType.BRIGHTCOVE
            self.save_thumbnail_to_s3_and_metadata(image, score, s3bucket,
                    keyname, s3fname, ttype, rank=0)
            bc_request.previous_thumbnail = s3fname
        
        else:
            _log.debug("key=finalize_brightcove_request "
                       " msg=no thumbnail for %s %s" %(api_key,video_id))
        
        #2 Push thumbnail in to brightcove account
        autosync = bc_request.autosync
        ba = BrightcovePlatform.get_account(api_key,i_id)
        if not ba:
            _log.error("key=finalize_brightcove_request msg=Brightcove " 
                    " account doesnt exists a_id=%s i_id=%s"%(api_key,i_id))
        else: 
            autosync = ba.auto_update 
        
        if autosync:
            rtoken  = self.request_map[properties.BCOVE_READ_TOKEN]
            wtoken  = self.request_map[properties.BCOVE_WRITE_TOKEN]
            pid = self.request_map[properties.PUBLISHER_ID]
            fno = bc_request.response["data"][0]
            img = Image.fromarray(self.data_map[fno][1])
            #img_url = self.thumbnails[0]["urls"][0]
            tid = self.thumbnails[0]["thumbnail_id"] 
            bcove   = brightcove_api.BrightcoveApi(
                neon_api_key=api_key,
                publisher_id=pid,
                read_token=rtoken,
                write_token=wtoken)
            
            frame_size = self.video_metadata['frame_size']
            ret = bcove.update_thumbnail_and_videostill(video_id, img, tid, frame_size)

            if ret[0]:
                #update enabled time & reference ID
                #NOTE: By default Neon rank 1 is always uploaded
                self.thumbnails[0]["chosen"] = True 
                self.thumbnails[0]["refid"] = tid

        #3 Update Request State
        #bc_request.thumbnails = self.thumbnails
        bc_request.state = RequestState.FINISHED 
        ret = bc_request.save()

        #NOTE: The newly uploaded thumbnail's url isn't available immidiately, 
        # A background check thumbnail job is run to get its url

        #4 Save the Thumbnail URL and ID to Mapper DB
        self.save_thumbnail_metadata("brightcove", i_id)

        if ret:
            self.save_video_metadata()
        else:
            _log.error("key=finalize_brightcove_request msg=failed to save request")


    def finalize_youtube_request(self,result,error=False):
        '''
        Final steps for youtube request
        '''
        '''
        api_key = self.request_map[properties.API_KEY]  
        job_id  = self.request_map[properties.REQUEST_UUID_KEY]
        video_id = self.request_map[properties.VIDEO_ID]
        yt_request = YoutubeApiRequest.get(api_key,job_id)
        yt_request.response = tornado.escape.json_decode(result)
       
        #save error result
        if error:
            yt_request.save()
            return
        
        if len(self.thumbnails) == 0 :
            yt_request.state = RequestState.INT_ERROR
            yt_request.save()
        
        #Save previous thumbnail to s3
        p_url = yt_request.previous_thumbnail
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = p_url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)

        response = http_client.fetch(req)
        imgdata = response.body 
        image = Image.open(StringIO(imgdata))
        s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
        s3bucket = Bucket(name = s3bucket_name,connection = s3conn)
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        k = Key(s3bucket)
        k.key = self.base_filename + "/youtube.jpeg" 
        k.set_contents_from_string(imgdata,{"Content-Type":"image/jpeg"})
        s3bucket.set_acl('public-read',k.key)
        s3fname = s3_url_prefix + "/" + k.key 
        yt_request.previous_thumbnail = s3fname
        
        #populate thumbnail
        urls = []
        tid = ThumbnailID.generate(imgdata,
                                   InternalVideoID.generate(api_key, video_id))
        urls.append(p_url)
        urls.append(s3fname)
        created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        enabled = True 
        width   = 480
        height  = 360
        ttype   = "youtube"
        rank    = 0 
        score   = self.valence_score(image)
        tdata = ThumbnailMetaData(tid,urls,created,width,height,ttype,score,self.model_version,enabled=enabled,rank=rank)
        thumb = tdata.to_dict()
        self.thumbnails.append(thumb)

        #TODO: Standalone youtube requests ?

        #2 Push thumbnail in to youtube account
        if yt_request.autosync:
            rtoken  = self.request_map["refresh_token"]
            atoken  = self.request_map["access_token"]
            expiry  = self.request_map["token_expiry"]
            fno = yt_request.response["data"][0]
            img = Image.fromarray(self.data_map[fno][1])
            yt  = youtube_api.YoutubeApi(rtoken)
            ret = yt.upload_youtube_thumbnail(video_id,img,atoken,expiry)
            if ret:
                self.thumbnails[0]["enabled"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        
        #3 Add thumbnails to the request object and save
        yt_request.thumbnails = self.thumbnails
        yt_request.state = RequestState.FINISHED
        ret = yt_request.save()

        if ret:
            self.save_video_metadata()
        else:
            _log.error("key=finalize_youtube_request msg=failed to save request")

        #self.save_thumbnail_metadata("youtube",i_id)
        '''
        pass

####################################################################################
# HTTP Downloader client
####################################################################################

class HttpDownload(object):
    ''' 
    HTTP Downloader class
    '''
    retry_codes = [403,500,502,503,504]

    def __init__(self, json_params, ioloop, model, model_version, 
            debug=False, cur_pid=None, sync=False):

        params = tornado.escape.json_decode(json_params)

        self.timeout = 300000.0 #long running tasks ## -- is this necessary ???
        self.ioloop = ioloop
        self.tempfile = tempfile.NamedTemporaryFile(delete=False)
        self.job_params = params
        url = params[properties.VIDEO_DOWNLOAD_URL]
        headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})

        req = tornado.httpclient.HTTPRequest(url=url, headers=headers,
                        use_gzip=False, request_timeout=self.timeout)
        self.size_so_far = 0
        self.pv = ProcessVideo(params, json_params, model, model_version, 
                                debug, cur_pid)
        self.error = None
        self.callback_data_size = 4096 * 1024 #4MB  --- TUNE 
        self.global_work_queue_url = properties.BASE_SERVER_URL + "/requeue"
        self.state = "start"
        self.total_size_so_far = 0
        self.content_length = 0

        #Timer for tracing
        self.pv.video_metadata["process_time"] = str(time.time())

        self.debug = debug
        self.debug_timestamps = {}
        self.debug_timestamps["streaming_callback"] = time.time()
        
        self.http_client_pool = RequestPool() #may be use for all outbound requests
        if not sync:
            tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
            req = tornado.httpclient.HTTPRequest(url=url, headers=headers,
                        streaming_callback=self.streaming_callback, 
                        use_gzip=False, request_timeout=self.timeout)
            http_client = tornado.httpclient.AsyncHTTPClient()
            http_client.fetch(req, self.async_callback)
        else:
            http_client = tornado.httpclient.HTTPClient()
            response = http_client.fetch(req)
            self.tempfile.write(response.body)
            self.async_callback(response)
        return 
        
    def streaming_callback(self, data):
        ''' callback after nbyes '''
        self.size_so_far += len(data)
        self.total_size_so_far += len(data)
        

        if not self.tempfile.closed:
            self.tempfile.write(data)
        else:
            _log.debug("key=streaming_callback msg=file already closed")
            self.error = INTERNAL_PROCESSING_ERROR
            #For clean shutdown incase of signals
            self.ioloop.stop()
            return

        if self.size_so_far > self.callback_data_size:
            if self.debug:
                end_time = time.time()
                _log.info("key=streaming_callback msg=debug time_bw_callback=%s, size_so_far=%s"
                    %(end_time - self.debug_timestamps["streaming_callback"],self.size_so_far) )
                self.debug_timestamps["streaming_callback"] = end_time

            self.size_so_far = 0

            # TODO(mdesnoyer): Remove this hack. Right now we capture
            # twice as many thubnails because later, we want to filter
            # based on the thumbs being too close in the video
            n_thumbs = 10 # Dummy
            if self.job_params.has_key(properties.TOP_THUMBNAILS):
                n_thumbs = 2*int(self.job_params[properties.TOP_THUMBNAILS])
            
            self.pv.process_all(self.tempfile.name, n_thumbs=n_thumbs)

    def async_callback(self, response):
        ''' called after the request ends '''
        # if video size < the chunk size
        try:
            #TODO Check for content type (html,json,xml) which may be
            #error messages

            #False link or transfer encoding is chunked
            if not response.headers.has_key('Content-Length'):
                self.ioloop.stop()
                self.error = "url not a video file"
                client_response = self.send_client_response(error=True)
                return
            
            #if the file downloaded was smaller than the callback data size
            if int(response.headers['Content-Length']) < self.callback_data_size:
                # TODO(mdesnoyer): Remove this hack. Right now we capture
                # twice as many thubnails because later, we want to filter
                # based on the thumbs being too close in the video
                n_thumbs = 10 # Dummy
                if self.job_params.has_key(properties.TOP_THUMBNAILS):
                    n_thumbs = 2*int(self.job_params[properties.TOP_THUMBNAILS])
                    
                self.pv.process_all(self.tempfile.name, n_thumbs=n_thumbs)

            #TODO If video partially downloaded & we have >n thumbnails,
            #then ignore reponse.error like timeout, connection closed
            #if one of the major error codes, then retry the video


        except Exception as e:
            _log.exception('key=async_callback Error processing the video: %s' % e)
            if self.debug:
                raise

        finally:
            if not self.tempfile.closed:
                self.tempfile.flush()
                self.tempfile.close()


        if response.error:
            if "HTTP 599: Operation timed out after" not in response.error.message:
                self.error = INTERNAL_PROCESSING_ERROR #response.error.message
                _log.error("key=async_callback_error  msg=%s request %s"
                        %(response.error.message,self.job_params[properties.VIDEO_DOWNLOAD_URL]))
            else:
                _log.error("key=async_request_timeout msg=" +response.error.message)
                ## Verify content length & total size to see if video
                ## has been downloaded == If request times out and we
                ## have 75% of data, then process the video and send
                ## data to client
                try:
                    self.content_length = response.headers['Content-Length']
                    if (self.total_size_so_far /float(self.content_length)) < 0.75:
                        self.error = INTERNAL_PROCESSING_ERROR
                except:
                    pass
        else:
            pass
            #print("Success: %s" % self.tempfile.name)
        self.ioloop.stop()
      
        #Process the final chunk, since all file size isn't always a
        #multiple of chunk size Certain video formats don't allow
        #partial rendering/ extraction of video

        #TODO:Remove n_thumbs hack
        n_thumbs = 10 # Dummy
        if self.job_params.has_key(properties.TOP_THUMBNAILS):
            n_thumbs = 2*int(self.job_params[properties.TOP_THUMBNAILS])
        self.pv.process_all(self.tempfile.name, n_thumbs=n_thumbs)
        ######
        
        #Save Center Frame
        self.pv.center_frame = self.pv.get_center_frame(self.tempfile.name)

        end_time = time.time()
        total_request_time =  end_time - float(self.pv.video_metadata[properties.VIDEO_PROCESS_TIME])
        self.pv.video_metadata[properties.VIDEO_PROCESS_TIME] = str(total_request_time)
        self.pv.video_metadata[properties.JOB_SUBMIT_TIME] = self.job_params[properties.JOB_SUBMIT_TIME]
        self.pv.video_metadata[properties.JOB_END_TIME] = str(end_time)

        #Delete the temp video file which was downloaded
        if os.path.exists(self.tempfile.name):
            os.unlink(self.tempfile.name)

        ######### Final Phase - send client response to callback url, save images & request data to s3 ########
        if self.error == INTERNAL_PROCESSING_ERROR:
            client_response = self.send_client_response(error=True)
        else:
            ## On Success 
            #Send client response
            client_response = self.send_client_response()
            self.pv.save_data_to_s3()
      
        #delete process video object
        del self.pv
        del self.tempfile

    def requeue_job(self):
        """ Requeue the api request on failure """ 
        if self.job_params.has_key("requeue_count"):
            rc = self.job_params["requeue_count"]
            if rc > 3:
                  _log.error("key=requeue_job msg=exceeded max requeue")
                  return False

            self.job_params["requeue_count"] = rc + 1
        else:
            self.job_params["requeue_count"] = 1

        body = tornado.escape.json_encode(self.job_params)
        requeue_request = tornado.httpclient.HTTPRequest(url = self.global_work_queue_url, 
                method = "POST",body =body, request_timeout = 60.0, connect_timeout = 10.0)
        http_client = tornado.httpclient.HTTPClient()
        response = http_client.fetch(requeue_request)
        if response.error:
            return False
        return True

    def send_client_response(self, error=False):
        ''' send callback response to client who submitted request '''
        s3_urls = None   
        
        #There was an error with processing the video
        #TODO: Have Error method to take care of failure
        if error:
            #If Internal error, requeue and dont send response to client yet
            #Send response to client that job failed due to the last reason
            #And Log the response we send to the client
            res = self.requeue_job()
            if res == False:
                #error_msg = self.response.error.message
                #self.error = error_msg
                error_msg = self.error
                cr = ClientCallbackResponse(self.job_params,None,error_msg)
                cr.send_response() 
                self.pv.finalize_neon_request(cr.response)
            return

        # API Specific client response
        request_type = self.job_params['request_type']
        api_method = self.job_params['api_method'] 
        api_param =  self.job_params['api_param']
        MAX_T = 6
        
        ''' Neon API section 
        '''
        if  api_method == properties.TOP_THUMBNAILS:
            n = topn = int(api_param)
            '''
            Always save 5 thumbnails for any request and host them on s3 
            '''
            if topn < MAX_T:
                n = MAX_T

            res = self.pv.get_topn_thumbnails(n)
            ranked_frames = [x[0] for x in res]
            data = ranked_frames[:topn]
            timecodes = self.pv.get_timecodes(data)
            
            #host top MAX_T images on s3
            s3_urls = self.pv.host_images_s3(ranked_frames[:MAX_T])
            cr = ClientCallbackResponse(self.job_params,data,self.error,urls=s3_urls[:topn])
            cr.send_response()  
           
            ## Neon section
            if request_type == "neon":
                
                #Save response that was created for the callback to client 
                self.pv.finalize_neon_request(cr.response)
                return

            ## Brightcove secion 
            elif request_type == "brightcove":
                #Update Brightcove
                self.pv.finalize_brightcove_request(cr.response,error)
            
            elif request_type == "youtube":
                pass
            else:
                if self.debug:
                    raise Exception("Request Type not Supported")
                _log.exception("type=Client Response msg=Request Type not Supported")

            #TO BE Implemented 
            #elif self.job_params.has_key(properties.THUMBNAIL_RATE):
            #rate = float(self.job_params[properties.THUMBNAIL_RATE])
            #rate = max(rate,1) #Rate should at least be 1
            #res = self.pv.get_thumbnail_at_rate(rate)
            #data = [x[0] for x in res]   
            #save ranked thumbnails
            #self.pv.save_result_data_to_s3(data)
       
        else:
            raise
            #TO BE Implemented


class ClientCallbackResponse(object):
    """ Http response to the callback url -- This is the final response to the client """
    def __init__(self,job_params,response_data,error=None,timecodes=None,urls=None):
        self.data = response_data
        self.timecodes = timecodes 
        self.job_params = job_params
        self.error = error
        self.client_url = self.job_params[properties.CALLBACK_URL]
        self.http_client = tornado.httpclient.HTTPClient()
        self.retries = 3
        self.response = None
        self.thumbnails = urls
        #Add standard headers
        return

    def build_request(self):
        '''
        build callback response
        '''
        response_body = {}
        response_body["job_id"] = self.job_params[properties.REQUEST_UUID_KEY] 
        response_body["video_id"] = self.job_params[properties.VIDEO_ID]
        response_body["data"] = self.data
        response_body["timecodes"] = self.timecodes
        response_body["thumbnails"] = self.thumbnails
        response_body["timestamp"] = str(time.time())

        if self.error is None:
            response_body["error"] = ""
        else:
            response_body["error"] = self.error

        #CREATE POST REQUEST
        body = tornado.escape.json_encode(response_body)
        #set this for future use
        self.response = body
        h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
        self.client_request = tornado.httpclient.HTTPRequest(url = self.client_url, method = "POST",
                headers = h,body = body, request_timeout = 60.0, connect_timeout = 10.0)
        return self.response

    def send_response(self):
        ''' send respone to callback url '''
        self.build_request()

        for i in range(self.retries):
            try:
                response = self.http_client.fetch(self.client_request)
                if response.error:
                    _log.error("type=client_response" 
                            " msg=failed to send response to client")
                    continue
                else:
                    _log.info("key=ClientCallbackResponse msg=sent client response")
                    break
            except:
                continue

class VideoClient(object):
   
    '''
    Video Client processor
    '''
    def __init__(self, model_file, debug=False, sync=False):
        self.model_file = model_file
        self.SLEEP_INTERVAL = 10
        self.kill_received = False
        self.dequeue_url = properties.BASE_SERVER_URL + "/dequeue"
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
        retries = 2
        
        http_client = tornado.httpclient.HTTPClient()
        headers = {'X-Neon-Auth' : properties.NEON_AUTH} 
        result = None
        for i in range(retries):
            try:
                response = http_client.fetch(self.dequeue_url,headers=headers)
                result = response.body
                break
            except tornado.httpclient.HTTPError, e:
                _log.error("Dequeue Error %s" %e)
                time.sleep(2)
                continue
             
        if result is not None and result != "{}":
            try:
                job_params = tornado.escape.json_decode(result)
                #Change Job State
                api_key = job_params[properties.API_KEY]
                job_id  = job_params[properties.REQUEST_UUID_KEY]
                api_request = NeonApiRequest.get(api_key,job_id)
                if api_request.state == RequestState.SUBMIT:
                    api_request.state = RequestState.PROCESSING
                    api_request.model_version = self.model_version
                    api_request.save()
                _log.info("key=worker [%s] msg=processing request %s "
                          %(self.pid,job_params[properties.REQUEST_UUID_KEY]))
            except Exception,e:
                _log.error("key=worker [%s] msg=db error %s" %(self.pid,e.message))
        return result
    
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
        self.load_model()
        while not self.kill_received:
            self.do_work()

    def do_work(self):   
        ''' do actual work here'''
        try:
            job = self.dequeue_job()
            if not job or job == "{}": #string match
                raise Queue.Empty
            
            ## ===== ASYNC Code Starts ===== ##
            ioloop = tornado.ioloop.IOLoop.instance()
            dl = HttpDownload(job, ioloop, self.model,self.model_version,
                              self.debug, self.pid, self.sync)
        
            #profile
            if options.profile:
                mem_tracker1 = summary.summarize(muppy.get_objects())
                ctracker = ClassTracker()
                ctracker.track_object(dl)
                ctracker.track_class(HttpDownload)
                ctracker.create_snapshot()
    
            ioloop.start()
            
            #delete http download object
            del dl
            gc.collect()
            
            if self.debug:
                un_objs = gc.collect()
                _log.debug('Unreachable objects:%r' %un_objs)
                pprint.pprint(gc.garbage)
    
            if options.profile:
                mem_tracker2 = summary.summarize(muppy.get_objects())
                mem_diff =  summary.get_diff(mem_tracker1,mem_tracker2)
                pr_ts = job_id #int(time.time())
                pickle.dump(mem_diff, open("muppy_profile."+str(pr_ts),"wb"))
                ctracker.create_snapshot()
                ctracker.stats.dump_stats('ctrackerprofile.'+str(pr_ts))

        except Queue.Empty:
            _log.debug("Q,Empty")
            time.sleep(self.SLEEP_INTERVAL * random.random())
        
        except Exception,e:
            _log.exception("key=worker [%s] "
                    " msg=exception %s" %(self.pid, e.message))
            if self.debug:
                raise
            time.sleep(self.SLEEP_INTERVAL)

def main():
    utils.neon.InitNeon()
    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    
    if options.local:
        _log.info("Running locally")
        properties.BASE_SERVER_URL = properties.LOCALHOST_URL

    #start video client
    global workers; workers = []
    vc = VideoClient(options.model_file,
                     options.debug, options.sync)
    workers.append(vc)
    vc.run()

if __name__ == "__main__":
    main()
