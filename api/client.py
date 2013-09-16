#!/usr/bin/python

##import redis; r = redis.StrictRedis(host='localhost', port=6379, db=0)


USAGE='%prog [options] <workers> <local properties>'

#============ Future Items ================
#TODO: IOLoop logging on blocking ops
#TODO: Send a signal to blocked threads ?
#TODO: IOLoop.handle_callback_exception
#TODO: Tracing calls and timers on the code
#TODO: ** Make it a state machine
#TODO: Benchmark streaming download - processing and prediction
#TODO: build a throttle mode for controlling number of clients based on mem/ cpu
#==============       =======================
import os
import os.path
import sys
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import model.model
import tempfile
import tornado.web
import tornado.gen
import tornado.escape
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import random
import multiprocessing
import Queue
import time
import hashlib
import numpy
import signal
import shutil
import datetime
import matplotlib
matplotlib.use('Agg') #To use without the $DISPLAY var on aws
import matplotlib.pyplot as plt

from PIL import Image
from optparse import OptionParser

import leargist
import svmlight
import ffvideo 
import errorlog
from BadImageFilter import BadImageFilter

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from StringIO import StringIO

import tarfile
import gzip
import copy

import brightcove_api
import youtube_api

sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../supportServices')))
from neondata import *

import gc
import pprint

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

from pympler import summary
from pympler import muppy
from pympler import tracker
from pympler.classtracker import ClassTracker
import pickle

# ======== API String constants  =======================#

MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 2
INTERNAL_PROCESSING_ERROR = "internal error"

#=============== Global Handlers =======================#
def sig_handler(sig, frame):
    log.debug('Caught signal: ' + str(sig) )

    try:
        for worker in workers:
            worker.kill_received = TrueQQQA
    except:
        sys.exit(0)

def format_status_json(state,timestamp,data=None):

    status = {}
    result = {}

    status['state'] = state
    status['timestamp'] = timestamp
    result['status'] = status
    result['result'] = ''

    if data is not None:
        result['result'] = data

    json = tornado.escape.json_encode(result)
    return json

#=============== Global Handlers =======================#

############################################################################
# STATE ENUM
############################################################################
class State(object):
    start,get_video_metadata,dequeue_master,process_video,rank_thumbnails,api_callback,insert_image_library,mark_inbox,complete,error  = range(10)

###########################################################################
# Process Video File
###########################################################################

class ProcessVideo(object):
    """ class provides methods to process a given video """
    def __init__(self, request_map, request, model, debug, cur_pid):
        self.request_map = request_map
        self.request = request
        self.model = model
        self.frames = []
        self.data_map = {} # frameNo -> (score, image_rgb)
        self.attr_map = {}
        self.timecodes = {}
        self.frame_size_width = 256
        self.sec_to_extract = 1 
        self.base_filename = request_map[properties.API_KEY] + "/" + request_map[properties.REQUEST_UUID_KEY]  #Used as direcrtory name
        self.sec_to_extract_offset = 1.0 #random.choice([0.9,1.0,1.1])

        if request_map.has_key(properties.THUMBNAIL_RATE):
            self.sec_to_extract_offset = random.choice([0.20,0.25,0.5]) #properties.MAX_SAMPLING_RATE

        self.sec_to_extract_offset = 1
        self.valence_scores = [[],[]] #x,y        

        #Video Meta data
        self.video_metadata = {}
        self.video_metadata['codec_name'] =  None
        self.video_metadata['duration'] = None
        self.video_metadata['framerate'] = None
        self.video_metadata['bitrate'] = None
        self.video_metadata['frame_size'] = None
        
        self.video_size = 0  # Calulated from bitrate and duration

        #S3 Stuff
        self.s3conn = S3Connection(properties.S3_ACCESS_KEY,
                                   properties.S3_SECRET_KEY)
        self.s3bucket_name = properties.S3_BUCKET_NAME
        self.s3bucket = Bucket(name = self.s3bucket_name,
                               connection = self.s3conn)
        self.format = "JPEG" #"PNG"

        #AB Test Data
        self.abtest_thumbnails= {}

        # Settings for the bad image filter
        self.bad_image_filter = BadImageFilter(30, 0.95)
        
        #thumbnail list of maps
        self.thumbnails = [] # thumbnail_id, url, created, enabled, width, height, type 
        
        self.debug = debug
        self.pid   = cur_pid

    ''' process all the frames from the partial video downloaded '''
    def process_all(self, video_file, n_thumbs=1):
        start_process = time.time()
        try:
            mov = ffvideo.VideoStream(video_file)
            if self.video_metadata['codec_name'] is None:
                self.video_metadata['codec_name'] =  mov.codec_name
                self.video_metadata['duration'] = mov.duration
                self.video_metadata['framerate'] = mov.framerate
                self.video_metadata['bitrate'] = mov.bitrate
                self.video_metadata['frame_size'] = mov.frame_size
                self.video_size = mov.duration * mov.bitrate / 8 # in bytes
        except Exception, e:
            log.error("key=process_video worker[%s] msg=%s "  %(self.pid,e.message))
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
                                       start_time=self.sec_to_extract,debug=self.debug)
        
        if self.debug:
            log.info("key=process_all current time=%s " %(self.sec_to_extract))

        for image, score, frame_no, timecode, attribute in results:
            self.valence_scores[0].append(timecode)
            self.valence_scores[1].append(score)
            self.timecodes[frame_no] = timecode
            self.data_map[frame_no] = (score, image[:,:,::-1])
            self.attr_map[frame_no] = attribute
        
        #del reference to stream object
        del mov
        
        end_process = time.time()
        if self.debug:
            log.info("key=streaming_callback msg=debug time_processing=%s" %(end_process - start_process))

    def get_mid_thumbnail(self,video_file):
        try:
            mov = ffvideo.VideoStream(video_file)
            mid = mov.duration / 2.0
            frame = mov.get_frame_at_sec(mid)
            self.abtest_thumbnails["neonb"] = frame.image();
            
        except Exception, e:
            log.error("key=process_video subkey=get_mid_thumbnail msg=" + 
                      e.__str__())
            if self.debug:
                raise
        return

    def get_filtered_thumbnail(self):
        score = 0
        #filter black and blur frames
        # TODO pick quality controlled frame which is not max
       
        data_slice = self.data_map.items()
        secondary_sorted_list = sorted(data_slice,
                                       key=lambda tup: hashlib.md5(str(tup[0])).hexdigest(),
                                       reverse=True)
        result = sorted(secondary_sorted_list, 
                        key=lambda tup: tup[1][0],
                        reverse=True)
        
        #Pick the mid element, which most likely isn't a high scored thumbnail
        mid  = len(result) / 2
        fno = result[mid][0]
        selected = self.data_map[fno]

        #while score == 0: 
        #    fno = random.choice(self.data_map.keys())
        #    selected = self.data_map[fno]
        #    score = selected[0]
        
        self.abtest_thumbnails["neonc"] = selected[1]
        return

    def get_neon_thumbnail(self):
        res = self.get_topn_thumbnails(1)
        fno = res[0][0]
        self.abtest_thumbnails["neona"] = self.data_map[fno][1]  #image 


    ''' method that is run before the video is deleted after downloading '''
    ''' use this to run cleanup code or misc methods '''
    def finalize(self,video_file):
        ### AB test stuff
        if os.path.exists(video_file): 
            try:
                self.get_mid_thumbnail(video_file)
                self.get_filtered_thumbnail()
                self.get_neon_thumbnail()
            except Exception, e:
                log.error("key=finalize msg=error msg=" + e.__str__())
                if self.debug:
                    raise
        return

    ############# THUMBNAIL METHODS ##################

    def get_timecodes(self,frames):
        result = []
        for f in frames:
            result.append(self.timecodes[f])
        return result

    def get_topn_thumbnails(self,n):
        res = self.top_thumbnails_per_interval(nthumbnails = n)
        return res

    def get_thumbnail_at_rate(self,rate):
        #thumbnails per second 
        # rate = 1 ; 1 thumbnail per sec

        def get_intervals(interval,r):
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

        for i,intv in zip(range(len(intervals)),intervals):
            intv = int(intv / self.sec_to_extract_offset)
            if i > 0:
                data_slice = frames[prev_intv :intv]
                result = sorted(data_slice, 
                                key=lambda tup: tup[1], 
                                reverse=True)
                res.append(result[0])
            prev_intv = intv
        return res

    ''' Get top n thumbnails per interval - used by topn '''
    def top_thumbnails_per_interval(self,nthumbnails =1,interval =0):
        #return array of top n sorted 
        #top_indices = sorted(range(len(data)), key=lambda i: data[i])[ -1 * nthumbnails:]

        data_slice = self.data_map.items()
        if interval != 0:
            data_slice = self.data_map.items()  #TODO slice the interval

        #result = sorted(data_slice, key=lambda tup: tup[1], reverse=True)
        
        #Randomize if the scores are the same, generate hash to use as
        #the secondary key
        secondary_sorted_list = sorted(data_slice, 
                                       key=lambda tup: hashlib.md5(str(tup[0])).hexdigest(),
                                       reverse=True)
        result = sorted(secondary_sorted_list,
                        key=lambda tup: tup[1][0],
                        reverse=True)
        #log.debug("key=thumbnails msg=" + str(len(result)) + " -- " + str(nthumbnails) ) 
      
        if len(result) < nthumbnails: 
            nthumbnails = min(len(result),nthumbnails)
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
        
        try:
            # Save images to S3 
            k = Key(self.s3bucket)
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

            if properties.SAVE_DATA_TO_S3:
                #Save gzip file to S3
                k.key = self.base_filename + "/thumbnails.tar.gz"
                k.set_contents_from_filename(gzip_file.name)
            else:
                log.info("thumbnails saved to " + gzip_file.name)
                if not os.path.exists('results'):
                    os.mkdir('results')
                fname = "thumbnails-" + self.request_map[properties.REQUEST_UUID_KEY] + '.tar.gz'
                shutil.copy(gzip_file.name,'results/' + fname)
                

            tmp_tar_file.close()
            gzip_file.close()

            ''' Save valence plot and video meta data '''

            #compute avg video valence score
            mean_valence = numpy.mean(self.valence_scores[1])
            self.video_metadata["video_valence"] = "%.4f" %float(mean_valence)
            video_metadata = tornado.escape.json_encode(self.video_metadata)
                
            #plot valence graph    
            plt.ylim([0,10])
            plt.xlabel('time in secs')
            plt.ylabel('valence score')
            plt.plot(self.valence_scores[0],self.valence_scores[1])
            fig = plt.gcf()
            
            if properties.SAVE_DATA_TO_S3:
                #Save video metadata
                k = Key(self.s3bucket)
                k.key = self.base_filename + "/"+ 'video_metadata.txt'
                k.set_contents_from_string(video_metadata)
                
                #save valence graph
                imgdata = StringIO()
                fig.savefig(imgdata, format='png')
                fig.clear()
                k = Key(self.s3bucket)
                k.key = self.base_filename + "/"+ 'vgraph' +"." + self.format
                imgdata.seek(0)
                data = imgdata.read()
                k.set_contents_from_string(data)
            else:
                #save to filesystem
                fname = "results/" + self.request_map[properties.REQUEST_UUID_KEY] + "-"
                with open(fname + 'video_metadata.txt','w') as f:
                    f.write(video_metadata)
                fig.savefig(fname + "vgraph" + '.png')
                fig.clear()

        except S3ResponseError,e:
            log.error("key=save_to_s3 msg=s3 response error " + e.__str__() )
        except Exception,e:
            log.error("key=save_to_s3 msg=general exception " + e.__str__() )
            if self.debug:
                raise
  
    ''' save previous thumbnail in the account to s3 ''' 
    def save_previous_thumbnail_to_s3(self):
        try:
            if self.request_map.has_key(properties.PREV_THUMBNAIL):
                url = self.request_map[properties.PREV_THUMBNAIL]
                http_client = tornado.httpclient.HTTPClient()
                req = tornado.httpclient.HTTPRequest(url = url,
                                                     method = "GET",
                                                     request_timeout = 60.0,
                                                     connect_timeout = 10.0)
                response = http_client.fetch(req)
                data = response.body 
                k = Key(self.s3bucket)
                k.key = self.base_filename + "/"+ 'previous' + "." + self.format
                k.set_contents_from_string(data)
        
        except S3ResponseError,e:
            log.error("key=save_top_thumb_to_s3 msg=s3 response error " +
                      e.__str__() )
        except Exception,e:
            log.error("key=save_top_thumb_to_s3 msg=general exception " +
                      e.__str__() )
            if self.debug:
                raise
        return k.key

    ''' Save the top thumnail to s3'''
    def save_top_thumbnail_to_s3(self,frame):
        try:
            image = self.data_map[frame][1]
            imgdata = StringIO()
            image.save(imgdata, format='jpeg')
            k = Key(self.s3bucket)
            k.key = self.base_filename + "/"+ 'result' + "." + self.format
            imgdata.seek(0)
            data = imgdata.read()
            k.set_contents_from_string(data)
        
        except S3ResponseError,e:
            log.error("key=save_top_thumb_to_s3 msg=s3 response error " +
                      e.__str__() )
        except Exception,e:
            log.error("key=save_top_thumb_to_s3 msg=general exception " +
                      e.__str__() )
            if self.debug:
                raise

    ''' Save the top thumbnails to s3 as tar.gz file '''
    def save_result_data_to_s3(self,frames):
        try:
            # Save Ranked images to S3 
            tmp_tar_file = tempfile.NamedTemporaryFile(delete=True)
            tar_file = tarfile.TarFile(tmp_tar_file.name,"w")
            k = Key(self.s3bucket)
            for rank,frame_no in zip(range(len(frames)),frames):
                score = self.data_map[frame_no][0]
                image = Image.fromarray(self.data_map[frame_no][1])
                size = properties.THUMBNAIL_IMAGE_SIZE

                # Image size requested is different set it
                if self.request_map.has_key(properties.THUMBNAIL_SIZE):
                    size = self.request_map.has_key(properties.THUMBNAIL_SIZE)

                image.thumbnail(size,Image.ANTIALIAS)
                #fname = "result/" +'rank_' + str(rank+1) + "_score_" + str(score) + "." + self.format
                #fname = "result/" +'rank_' + str(rank+1) + "." + self.format
                fname = "result-"+ self.request_map[properties.REQUEST_UUID_KEY]  + "/" +'rank_' + str(rank+1) + "." + self.format
                filestream = StringIO()
                image.save(filestream, self.format)
                filestream.seek(0)
                info = tarfile.TarInfo(name=fname)
                info.size = len(filestream.buf)
                tar_file.addfile(tarinfo=info, fileobj=filestream)
            tar_file.close()

            gzip_file = tempfile.NamedTemporaryFile(delete=properties.DELETE_TEMP_TAR)
            gz = gzip.GzipFile(filename=gzip_file.name, mode='wb')
            tmp_tar_file.seek(0)
            gz.write(tmp_tar_file.read())
            gz.close()

            if properties.SAVE_DATA_TO_S3:
                #Save gzip to s3
                #fname = self.request_map[properties.REQUEST_UUID_KEY] + '.tar.gz'
                k.key = self.base_filename + "/result.tar.gz"
                k.set_contents_from_filename(gzip_file.name)
        
            else:
                log.info("result saved to " + gzip_file.name)
                if not os.path.exists('results'):
                    os.mkdir('results')
                fname = self.request_map[properties.REQUEST_UUID_KEY] + '.tar.gz'
                shutil.copy(gzip_file.name,'results/' + fname)
                
            tmp_tar_file.close()
            gzip_file.close()

        except S3ResponseError,e:
            log.error("key=save_result_to_s3 msg=s3 response error " + 
                      e.__str__() )
        except Exception,e:
            log.error("key=save_result_to_s3 msg=general exception " + 
                      e.__str__() )
            if self.debug:
                raise

    
    ''' Host images on s3 which is available publicly '''
    def host_images_s3(self, frames):
        s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
        s3bucket = Bucket(name = s3bucket_name,connection = s3conn)
        
        fname_prefix = 'neon'
        fmt = 'jpeg'
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        s3_urls = []

        #upload the images to s3
        for i in range(len(frames)):
            filestream = StringIO()
            image = Image.fromarray(self.data_map[frames[i]][1])
            #image.thumbnail(size,Image.ANTIALIAS)
            image.save(filestream, fmt, quality=100) 
            filestream.seek(0)
            imgdata = filestream.read()
            k = Key(s3bucket)
            k.key = self.base_filename + "/" + fname_prefix + str(i) + "." + fmt 
            k.set_contents_from_string(imgdata,{"Content-Type":"image/jpeg"})
            s3bucket.set_acl('public-read',k.key)
            s3fname = s3_url_prefix + "/" + self.base_filename + "/" + fname_prefix + str(i) + ".jpeg"
            s3_urls.append(s3fname)
            
            #populate thumbnail
            thumb = {} 
            thumb['thumbnail_id'] = i
            thumb['url'] = s3fname
            thumb['created'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            thumb['enabled'] = None 
            thumb['width']   = image.size[0]
            thumb['height']  = image.size[1] 
            thumb['type']    = "neon" + str(i)
            self.thumbnails.append(thumb)

        return s3_urls

    ''' Host AB Test images for experiment '''
    def host_abtest_images(self):
       
        s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
        s3bucket = Bucket(name = s3bucket_name,connection = s3conn)

        fields = ["neona","neonb","neonc"] #["neonthumbnail","neonmid","neonfiltered"]
        fmt = 'jpeg'
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"

        if self.request_map.has_key(properties.THUMBNAIL_SIZE):
            size = 480,268 #self.request_map.has_key(properties.THUMBNAIL_SIZE)

        #upload the images to s3
        for field in fields:
            filestream = StringIO()
            image = self.abtest_thumbnails[field]
            image.thumbnail(size,Image.ANTIALIAS)
            image.save(filestream, fmt, quality=100 ) #TODO: Jpeg with specified quality 
            filestream.seek(0)
            imgdata = filestream.read()
            k = Key(s3bucket)
            k.key = self.base_filename + "/" + field + "." + fmt 
            k.set_contents_from_string(imgdata)
            s3bucket.set_acl('public-read',k.key)

        #update brightcove account with uploaded image urls
        api_key = self.request_map[properties.API_KEY]  
        rtoken  = self.request_map[properties.BCOVE_READ_TOKEN]
        wtoken  = self.request_map[properties.BCOVE_WRITE_TOKEN]
        video_id = self.request_map[properties.VIDEO_ID]
        request_id = self.request_map[properties.REQUEST_UUID_KEY]
        bcove   = brightcove_api.BrightcoveApi(neon_api_key=api_key,read_token=rtoken,write_token=wtoken)
        
        neona = s3_url_prefix + "/" + self.base_filename + "/" + "neona.jpeg"
        neonb = s3_url_prefix + "/" + self.base_filename + "/" + "neonb.jpeg"
        neonc = s3_url_prefix + "/" + self.base_filename + "/" + "neonc.jpeg"
        bcove.update_abtest_custom_thumbnail_video(video_id,neona,neonb,neonc)

    ############# Request Finalizers ##############

    ''' Update the request state for Neon API Request '''
    def finalize_neon_request(self, result=None):
        
        api_key = self.request_map[properties.API_KEY] 
        job_id  = self.request_map[properties.REQUEST_UUID_KEY]
        json_request = NeonApiRequest.get_request(api_key,job_id)
        api_request = NeonApiRequest.create(json_request)
        
        #change the status to requeued, and don't store a response 
        if result is None:
            api_request.state = "requeued" 
        else:
            try:
                api_request.response = tornado.escape.json_decode(result)
                api_request.state = "finished"
            except:
                api_request.response = result 
                api_request.state = "falied"
            
            api_request.thumbnails = self.thumbnails
        
        api_request.save()
        return


    '''
    Brightcove handler

    - host neon thumbs and also save bcove previous thumbnail in s3
    - Get Account settings and replace default thumbnail if enabled 
    - update request object with the thumbnails
    '''
    def finalize_brightcove_request(self,result,error=False):
       
        api_key = self.request_map[properties.API_KEY]  
        job_id  = self.request_map[properties.REQUEST_UUID_KEY]
        json_request = BrightcoveApiRequest.get_request(api_key,job_id)
        bc_request  = BrightcoveApiRequest.create(json_request)
        bc_request.response = tornado.escape.json_decode(result)
        
        if error:
            bc_request.save()
            return

        
        #Save previous thumbnail to s3
        p_url = bc_request.previous_thumbnail
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = p_url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)

        response = http_client.fetch(req)
        imgdata = response.body 
        s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
        s3bucket = Bucket(name = s3bucket_name,connection = s3conn)
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        k = Key(s3bucket)
        k.key = self.base_filename + "/brightcove.jpeg" 
        k.set_contents_from_string(imgdata)
        s3bucket.set_acl('public-read',k.key)
        s3fname = s3_url_prefix + "/" + k.key 
        bc_request.previous_thumbnail = s3fname
        
        #populate thumbnail
        thumb = {} 
        thumb['thumbnail_id'] = 5
        thumb['url'] = s3fname
        thumb['created'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        thumb['enabled'] = None 
        thumb['width']   = 480
        thumb['height']  = 360
        thumb['type']    = "brightcove"
        self.thumbnails.append(thumb)

        #2 Push thumbnail in to brightcove account
        if bc_request.autosync:
            rtoken  = self.request_map[properties.BCOVE_READ_TOKEN]
            wtoken  = self.request_map[properties.BCOVE_WRITE_TOKEN]
            video_id = self.request_map[properties.VIDEO_ID]
            pid = self.request_map[properties.PUBLISHER_ID]
            fno = bc_request.response["data"][0]
            img = Image.fromarray(self.data_map[fno][1])
            #img_url = self.thumbnails[0]["url"]
            bcove   = brightcove_api.BrightcoveApi(neon_api_key=api_key,publisher_id=pid,read_token=rtoken,write_token=wtoken)
            ret = bcove.update_thumbnail_and_videostill(video_id,img)

            if ret:
                self.thumbnails[0]["enabled"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        #3 Add thumbnails to the request object and save
        bc_request.thumbnails = self.thumbnails
        bc_request.state = "finished"
        bc_request.save()


    def finalize_youtube_request(self,result,error=False):
        api_key = self.request_map[properties.API_KEY]  
        job_id  = self.request_map[properties.REQUEST_UUID_KEY]
        json_request = YoutubeApiRequest.get_request(api_key,job_id)
        yt_request  = YoutubeApiRequest.create(json_request)
        yt_request.response = tornado.escape.json_decode(result)
       
        #save error result
        if error:
            yt_request.save()
            return
        
        #Save previous thumbnail to s3
        p_url = yt_request.previous_thumbnail
        http_client = tornado.httpclient.HTTPClient()
        req = tornado.httpclient.HTTPRequest(url = p_url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)

        response = http_client.fetch(req)
        imgdata = response.body 
        s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_HOST_BUCKET_NAME
        s3bucket = Bucket(name = s3bucket_name,connection = s3conn)
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        k = Key(s3bucket)
        k.key = self.base_filename + "/youtube.jpeg" 
        k.set_contents_from_string(imgdata)
        s3bucket.set_acl('public-read',k.key)
        s3fname = s3_url_prefix + "/" + k.key 
        yt_request.previous_thumbnail = s3fname
        
        #populate thumbnail
        thumb = {} 
        thumb['thumbnail_id'] = 5
        thumb['url'] = s3fname
        thumb['created'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        thumb['enabled'] = None 
        thumb['width']   = 480
        thumb['height']  = 360
        thumb['type']    = "youtube"
        self.thumbnails.append(thumb)
        
        #2 Push thumbnail in to youtube account
        if bc_request.autosync:
            rtoken  = self.request_map["refresh_token"]
            atoken  = self.request_map["access_token"]
            expiry  = self.request_map["token_expiry"]
            video_id = self.request_map[properties.VIDEO_ID]
            fno = yt_request.response["data"][0]
            img = Image.fromarray(self.data_map[fno][1])
            yt  = youtube_api.YoutubeApi(rtoken)
            ret = yt.upload_youtube_thumbnail(video_id,img,atoken,expiry)
            if ret:
                self.thumbnails[0]["enabled"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        
        #3 Add thumbnails to the request object and save
        yt_request.thumbnails = self.thumbnails
        yt_request.state = "finished"
        yt_request.save()


#############################################################################################
# HTTP Downloader client
#############################################################################################

class HttpDownload(object):
    retry_codes = [403,500,502,503,504]

    def __init__(self, json_params, ioloop, model, debug=False, cur_pid=None):
        #TODO Make chunk size configurable
        #TODO GZIP vs non gzip video download? 

        ### Notes: 
        ### curl async client used 1 per ioloop here as we do compute work
        ### Ideally this is perfect for making multiple http requests in parallel

        tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        params = tornado.escape.json_decode(json_params)

        self.timeout = 300000.0 #long running tasks ##TODO - is this necessary ??? ###
        self.ioloop = ioloop
        self.tempfile = tempfile.NamedTemporaryFile(delete=False)
        self.job_params = params
        url = params[properties.VIDEO_DOWNLOAD_URL]
        headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})

        req = tornado.httpclient.HTTPRequest(url = url, headers = headers,
                        streaming_callback = self.streaming_callback, 
                        use_gzip =False, request_timeout = self.timeout)
        http_client = tornado.httpclient.AsyncHTTPClient()
        http_client.fetch(req, self.async_callback)
        self.size_so_far = 0
        self.pv = ProcessVideo(params, json_params, model, debug, cur_pid)
        self.error = None
        self.callback_data_size = 4096 * 1024 #4MB  --- TUNE 
        self.global_work_queue_url = properties.BASE_SERVER_URL + "/requeue"
        self.state = State.start
        self.total_size_so_far = 0
        self.content_length = 0

        #Timer for tracing
        self.pv.video_metadata["process_time"] = str(time.time())

        self.debug = debug
        self.debug_timestamps = {}
        self.debug_timestamps["streaming_callback"] = time.time()
        return
    
        
    def streaming_callback(self, data):
        self.size_so_far += len(data)
        self.total_size_so_far += len(data)
        

        if not self.tempfile.closed:
            self.tempfile.write(data)
        else:
            log.debug("key=streaming_callback msg=file already closed")
            self.error = INTERNAL_PROCESSING_ERROR
            #For clean shutdown incase of signals
            self.ioloop.stop()
            return

        if self.size_so_far > self.callback_data_size:
            if self.debug:
                end_time = time.time()
                log.info("key=streaming_callback msg=debug time_bw_callback=%s, size_so_far=%s"
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

    # After the request ends
    def async_callback(self, response):
        # if video size < the chunk size
        try:
            #TODO Check for content type (html,json,xml) which may be error messages

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
            log.exception('key=async_callback Error processing the video: %s' % e)
            if self.debug:
                raise

        finally:
            if not self.tempfile.closed:
                self.tempfile.flush()
                self.tempfile.close()


        if response.error:
            if "HTTP 599: Operation timed out after" not in response.error.message:
                self.error = INTERNAL_PROCESSING_ERROR #response.error.message
                log.error("key=async_callback_error  msg=" + response.error.message + " request=" + self.job_params[properties.VIDEO_DOWNLOAD_URL])
            else:
                log.error("key=async_request_timeout msg=" +response.error.message)
                ## Verify content length & total size to see if video has been downloaded 
                ## == If request times out and we have 75% of data, then process the video and send data to client 
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
      
        #Process the final chunk, since all file size isn't always a multiple of chunk size
        #Certain video formats don't allow partial rendering/ extraction of video

        #TODO:Remove n_thumbs hack
        n_thumbs = 10 # Dummy
        if self.job_params.has_key(properties.TOP_THUMBNAILS):
            n_thumbs = 2*int(self.job_params[properties.TOP_THUMBNAILS])
        self.pv.process_all(self.tempfile.name, n_thumbs=n_thumbs)
        ######

        end_time = time.time()
        total_request_time =  end_time - float(self.pv.video_metadata[properties.VIDEO_PROCESS_TIME])
        self.pv.video_metadata[properties.VIDEO_PROCESS_TIME] = str(total_request_time)
        self.pv.video_metadata[properties.JOB_SUBMIT_TIME] = self.job_params[properties.JOB_SUBMIT_TIME]
        self.pv.video_metadata[properties.JOB_END_TIME] = str(end_time)

        #cleanup or misc methods to be run before the video is deleted
        #self.pv.finalize(self.tempfile.name)

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
                  log.error("key=requeue_job msg=exceeded max requeue")
                  return False

            self.job_params["requeue_count"] = rc + 1
        else:
            self.job_params["requeue_count"] = 1

        body = tornado.escape.json_encode(self.job_params)
        requeue_request = tornado.httpclient.HTTPRequest(url = self.global_work_queue_url, 
                method = "POST",body =body, request_timeout = 60.0, connect_timeout = 10.0)
        http_client = tornado.httpclient.HTTPClient()
        retries = 1

        for i in range(retries):
            try:
                response = http_client.fetch(requeue_request)
                break
            except tornado.httpclient.HTTPError, e:
                log.error("key=requeue  msg=requeue error " + e.__str__())
                continue

        return True

    def send_client_response(self,error=False):
        s3_urls = None   
        
        #There was an error with processing the video
        if error:
            #If Internal error, requeue and dont send response to client yet
            #Send response to client that job failed due to the last reason
            #And Log the response we send to the client
            res = self.requeue_job()
            if res == False:
                #error_msg = self.response.error.message
                #self.error = error_msg
                error_msg = self.error
                cr = ClientResponse(self.job_params,None,error_msg)
                cr.send_response() 
                self.pv.finalize_neon_request(cr.response)
            return

        # API Specific client response
      
        request_type = self.job_params['request_type']
        api_method = self.job_params['api_method'] 
        api_param =  self.job_params['api_param']
        MAX_T = 5

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
            
            #host top 5 images on s3
            s3_urls = self.pv.host_images_s3(ranked_frames[:MAX_T])
            cr = ClientResponse(self.job_params,data,self.error,urls=s3_urls[:topn])
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
                if debug:
                    raise Exception("Request Type not Supported")
                log.exception("type=Client Response msg=Request Type not Supported")

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


class ClientResponse(object):
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

    def format_get(self, url, data=None):
        if data is not None:
            if isinstance(data, dict):
                data = urlencode(data)
            if '?' in url:
                url += '&amp;%s' % data
            else:
                url += '?%s' % data
        return url

    def format_post(self, data):
        if data is not None:
            if isinstance(data, dict):
                data = urlencode(data)
        return data

    def build_request(self):
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
        self.build_request()

        for i in range(self.retries):
            try:
                response = self.http_client.fetch(self.client_request)
                #Verify HTTP 200 OK
                break
            except tornado.httpclient.HTTPError, e:
                log.error("type=client_response msg=response error")
                continue

##############################################
## Multi Processing worker class
#############################################

class Worker(multiprocessing.Process):

    """ Generic worker framework to execute tasks adopted for specific needs

        State Transitions 
        1. Download job from master queue (Metadata from api call)
        2. Downloading video and extracting frames
        3. onDownload complete, run through model and rank thumbnails
        4a. Use api callback to submit the result
        4b. Insert into image library with Metadata
        4c. Mark job in api inbox as complete

    """

    def __init__(self, model_file, model_version_file, debug=False):
        # base class initialization
        multiprocessing.Process.__init__(self)
        self.model_file = model_file
        self.model_version_file = model_version_file
        self.SLEEP_INTERVAL = 10
        self.kill_received = False
        self.dequeue_url = properties.BASE_SERVER_URL + "/dequeue"
        self.state = State.start
        self.model_version = -1
        self.code_version = self.read_version_from_file(code_version_file)
        self.model = None
        self.debug = debug
        self.check_model()

    def read_version_from_file(self,fname):
        with open(fname,'r') as f:
            return int(f.readline())

    """ Blocking http call to global queue to dequeue work """
    def dequeue_job(self):
        retries = 2

        http_client = tornado.httpclient.HTTPClient()
        result = None
        for i in range(retries):
            try:
                response = http_client.fetch(self.dequeue_url)
                result = response.body
                break
            except tornado.httpclient.HTTPError, e:
                log.error("Dequeue Error " + e.__str__())
                continue

        return result

    def check_code_release_version(self):
        code_version = self.read_version_from_file(code_version_file)
        
        # check if new code version > current
        # Also check code_version ==0, i.e graceful shutdown
        if code_version > self.code_version or code_version ==0:
            self.kill_received = True

    def check_model(self):
        with open(self.model_version_file,'r') as f:
            try:
                version = int(f.readline())
            except:
                log.error('Model version file not present: %s' %
                          self.model_version_file)
                return

        # Change the model
        if self.model_version < version:
            self.model_version = version
            log.info('Loading model from %s' % self.model_file)
            self.model = model.model.load_model(self.model_file)

    def run(self):
        log.info("starting worker [%s] %s " %(self.pid,str(i)))
        while not self.kill_received:
          # get a task
          try:
                job = self.dequeue_job()
                if job == "{}": #string match
                      raise Queue.Empty
                

                ## ===== ASYNC Code Starts ===== ##
                ioloop = tornado.ioloop.IOLoop.instance()
                dl = HttpDownload(job, ioloop, self.model, self.debug, self.pid)
                #log.info("ioloop %r" %ioloop)  
                try:
                    #Change Job State
                    api_key = dl.job_params[properties.API_KEY] 
                    job_id = dl.job_params[properties.REQUEST_UUID_KEY]
                    json_request = NeonApiRequest.get_request(api_key,job_id)
                    api_request = NeonApiRequest.create(json_request)
                    if api_request.state == "submit":
                        api_request.state = "processing" 
                        api_request.save()
                    ts = str(time.time())
                    #log.info("key=worker [%s] msg=request %s" % (self.pid,NeonApiRequest.get_request(api_key,job_id)) ) 
                    log.info("key=worker [%s] msg=processing request %s %s" %(self.pid,dl.job_params[properties.REQUEST_UUID_KEY],str(time.time())))

                except Exception,e:
                    log.error("key=worker [%s] msg=db error %s" %(self.pid,e.message))

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
                    print 'Unreachable objects:', un_objs
                    print "Remaining Garbage:"
                    pprint.pprint(gc.garbage)

                if options.profile:    
                    mem_tracker2 = summary.summarize(muppy.get_objects())
                    mem_diff =  summary.get_diff(mem_tracker1,mem_tracker2)
                    pr_ts = job_id #int(time.time())
                    pickle.dump(mem_diff, open("muppy_profile."+str(pr_ts),"wb"))
                    ctracker.create_snapshot()
                    ctracker.stats.dump_stats('ctrackerprofile.'+str(pr_ts))

          except Queue.Empty:
                #log.info("Q,Empty")
                time.sleep(self.SLEEP_INTERVAL * random.random())  

          except Exception,e:
                log.error("key=worker [%s] msg=exception %s" %(self.pid,e.message))
                if self.debug:
                      raise
                time.sleep(self.SLEEP_INTERVAL)
        
          #check for new model release
          self.check_model()

          #check for new code release
          self.check_code_release_version()

if __name__ == "__main__":
    parser = OptionParser(usage=USAGE)

    parser.add_option('--local', default=False, action='store_true',
                      help='If set, use the localproperties file for config')
    parser.add_option('--n_workers', default=1, type='int',
                      help='Number of workers to spawn')
    parser.add_option('--model_file', default=None,
                      help='File that contains the model')
    parser.add_option('--debug', default=False, action='store_true',
                      help='If true, runs in debug mode')
    parser.add_option('--profile', default=False, action='store_true',
                      help='If true, runs in debug mode')

    options, args = parser.parse_args()
    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    num_processes= options.n_workers
    if options.debug:
        num_processes = 1
    
    #Logger
    global log
    log = errorlog.FileLogger("client")
    
    if options.local:
        log.info("Running locally")
        import localproperties as properties
    else:
        import properties

    
    #code version file
    cdir = os.path.dirname(__file__)   
    code_version_file = os.path.join(cdir,"code.version")
    
    #Load the path to the model
    model_version_file = os.path.join(os.path.dirname(__file__),
                                      '..',
                                      'model', 
                                      "model.version")

    workers = []
    
    #spawn workers
    for i in range(num_processes):
        worker = Worker(options.model_file, model_version_file,options.debug)
        workers.append(worker)
        if options.debug or num_processes ==1:
            worker.run()
        else:
            worker.start()
    
    #join workers
    if not options.debug:
        if num_processes ==1:
            exit(0)
        for w in workers:
            w.join()
