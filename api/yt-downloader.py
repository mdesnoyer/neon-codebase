#!/usr/bin/python

### Extract frames from youtube

import os
import sys
import tempfile
import tornado.web
import tornado.gen
import tornado.httpclient
import tornado.ioloop
from PIL import Image
import leargist
import numpy
import svmlight
import ffvideo
import random
import multiprocessing
import Queue
import time
import BinarySVM
import youtube

##################################################
# Video Meta Class
##################################################
 
class VideoMeta(object):
    """  meta information on video"""
    def __init__(self):
        self.thumbnails =[]
        self.video_name = ""
        self.video_url = ""
        self.temp_file_name = ""
    
    def add_thumbnail(self,image):
        #print "add_thumbnail ",type(image)
        self.thumbnails.append(list(image))


###################################################
# Process Video File
###################################################

class ProcessVideo(object):
    """ class provides methods to process a given video """
    def __init__(self,fname,yt_url=None):
      self.frames =[]
      self.frame_size_width = 256
      self.sec_to_extract = 3
      self.filename = fname
      self.sec_to_extract_offset = 5
      self.yt_url = yt_url

    def process_sequentially(self,videoFile):
          try:
              mov = ffvideo.VideoStream(videoFile,frame_size=(self.frame_size_width, None))
          except Exception, e:
              print "movie file not found" ,e
              return
          duration = mov.duration
          filename = videoFile.split(".")
          try:
            #print "extracting frame ", self.sec_to_extract
            #frame = mov.get_frame_at_sec(i)
            self.sec_to_extract = random.randint(1,duration)
            count = 1
            while count <= 3:
                frame = mov.get_frame_at_sec(self.sec_to_extract)
                image = frame.image()
                self.sec_to_extract -=1 
                #score = model.predictions(image)
                image = frame.image()
                size = 256, 144
                image = image.resize(size)
                if self.yt_url is None:
                  directory = "thumbnails/"
                else:
                  directory = "thumbnails/" + self.yt_url + "/" 
                if not os.path.exists(directory):
                  os.makedirs(directory)
                image.save( directory + self.filename + "_" + str(count) + ".jpg")
                #image.save( directory + self.filename + "_" + str(frame.frameno) + "_" + str(score) + ".jpg")
                count += 1
            #Reset
            self.sec_to_extract += self.sec_to_extract_offset
          except Exception,e:
            #print "[process] exception", e
            #print "[frame] " + str(self.sec_to_extract)  
            #No more key frames to process
            return
          return len(self.frames)

    def process_all(self,videoFile):
          try:
              mov = ffvideo.VideoStream(videoFile)

          except Exception, e:
              print "movie file not found" ,e
              return

          duration = mov.duration
          filename = videoFile.split(".")
          count = 0
          while count <3:
            try:
              frame = mov.get_frame_at_sec(self.sec_to_extract)
              image = frame.image()
              size = 256, 144
              image = image.resize(size)
              self.sec_to_extract += self.sec_to_extract_offset
              directory = "thumbnails/"   
              if not os.path.exists(directory):
                os.makedirs(directory)
              image.save( directory + self.yt_url + "_" + str(count) + ".jpg")
              print "saving",  directory + self.yt_url + "_" + str(count) + ".jpg"
              count += 1
            except Exception,e:
              print "[DEBUG] [process] exception", e
              return
          return len(self.frames)

###############################
# HTTP Downloader client
###############################
class HttpDownload(object):

    def __init__(self, url, ioloop,video_meta_queue,fname,yt_url):
      #TODO Set time out for download connect_timeout=None, request_timeout=None
      #TODO Make chunk size configurable
      #TODO GZIP vs non gzip video download? 

      #curl async client used 1 per ioloop here as we do compute work
      #Ideally this is perfect for making multiple http requests in parallel
      tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

      self.timeout = 3000.0 #TODO higher? / long running tasks
      self.ioloop = ioloop
      self.tempfile = tempfile.NamedTemporaryFile(delete=True)
      self.req = tornado.httpclient.HTTPRequest(url = url,streaming_callback = self.streaming_callback, use_gzip =False, request_timeout = self.timeout)
      self.http_client = tornado.httpclient.AsyncHTTPClient()
      self.http_client.fetch(self.req, self.async_callback)
      self.size_so_far = 0
      self.pv = ProcessVideo(fname,yt_url)
      self.video_meta_queue = video_meta_queue
      self.error = None
      self.retry = 2
      self.callback_data_size = 4096 *1024
      return

    
    def streaming_callback(self, data):
      self.size_so_far += len(data)
      if self.size_so_far > 4096000: #400kb
        #print "streamer -",self.size_so_far
        self.size_so_far = 0
        self.pv.process_all(self.tempfile.name)

      if not self.tempfile.closed:
        self.tempfile.write(data)
      else:
        print "[DEBUG] File already closed"
        #For clean shutdown incase of signals
        self.ioloop.stop()

    def async_callback(self, response):
      # if video size < the chunk size
      try:
        if int(response.headers['Content-Length']) < self.callback_data_size:
            self.pv.process_all(self.tempfile.name)
      except:
            pass
      
      if not self.tempfile.closed:
        self.tempfile.flush()
        self.tempfile.close()

      #TODO: If video partially downloaded & we have >n thumbnails, then ignore reponse.error like timeout, connection closed
      #if one of the major error codes, then retry the video

      if response.error:
        self.error = response.error
        print "[ASYNC CALLBACK ERROR] ", response.error
        if os.path.exists(self.tempfile.name):
          os.unlink(self.tempfile.name)
        #return self.http_client.fetch(self.req, self.async_callback,x= x+1)

      else:
        print("Success: %s" % self.tempfile.name)
        #insert into the result queue to be processed
        """
        try:
          video_meta_queue.put(self.pv.video_meta)

        except Exception,e:
          print "Result Q Put error", e 
        """
        ##    
      self.ioloop.stop()

##############################################
# ENUM
##############################################
class State(object):
    start,dequeue_master,process_video,rank_thumbnails,api_callback,insert_image_library,mark_inbox,complete,error  = range(9)

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

    def __init__(self, work_queue, video_meta_queue):

      # base class initialization
      multiprocessing.Process.__init__(self)
      self.SLEEP_INTERVAL = 5

      # job management stuff
      self.work_queue = work_queue
      self.video_meta_queue = video_meta_queue
      self.kill_received = False
      self.state = State.start
      #self.pv = ProcessVideo()


    def run(self):
      while not self.kill_received:

        # get a task
        #job = self.work_queue.get_nowait()
        try:
            job = self.work_queue.get_nowait()
            ioloop = tornado.ioloop.IOLoop.instance()
            #url =
            yt = youtube.YouTube()
          
            for attempts in range(5):
              try:
               yt.url = job
              except:
                print "YT url not set - " + job
                time.sleep(10)
                continue 

            video = yt.get('mp4','1080p')

            if video is None:
              print "720p"
              video = yt.get('mp4','720p')
            if video is None:
              print "480p"
              video = yt.get('flv','480p')
            if video is None:
              print "360p"
              video = yt.get('flv','360p')  

            if video is None:
                print "VIDEO is none"
                raise Exception('xcontinue')

            fname = yt.filename[:5] #prefix used to save the thumbnails 
            dl = HttpDownload(video.url,ioloop,self.video_meta_queue,fname,job.split('=')[1])
            ioloop.start()

        except Queue.Empty:
            time.sleep(self.SLEEP_INTERVAL)
            continue #break


        except Exception,e:
          print "Other exception  ",e
          self.work_queue.put(job)
          time.sleep(self.SLEEP_INTERVAL)
          continue #break


class ImageWorker(multiprocessing.Process):

    def __init__(self, vmq):

      self.SLEEP_INTERVAL = 1
      # base class initialization
      multiprocessing.Process.__init__(self)

      # job management stuff
      self.video_meta_queue = vmq
      self.kill_received = False

    def run(self):
      while not self.kill_received:

        try:
          job = self.video_meta_queue.get_nowait()
          print "Deque result", job

        except Queue.Empty:
          print "Result Q empty"

        time.sleep(self.SLEEP_INTERVAL)
          

if __name__ == "__main__":

    jobs = []

    #Load jobs from a file
    try:
      if sys.argv[1] == None or sys.argv[2] == None:
        pass
    except:
      print "Usage ./client <workers> <youtube_url_file>"

    num_processes = int(sys.argv[1]) 

    f = open(sys.argv[2])
    url_list = f.readlines()
    jobs = [url.rstrip("\n") for url in url_list]

    #Load the model - Make this static or a singleton instance
    num_jobs = len(jobs)

    # run
    # load up work queue
    work_queue = multiprocessing.Queue()

    #TODO Create job object with tries counter
    for job in jobs:
      work_queue.put(job)

    # create a queue to pass to workers to store the results
    video_meta_queue = multiprocessing.Queue()

    # spawn workers
    for i in range(num_processes):
      worker = Worker(work_queue, video_meta_queue)
      worker.start()

    #w = ImageWorker(video_meta_queue)
    #w.start()

    # collect the results off the queue
    #for i in range(num_jobs):
    #  print(video_meta_queue.get())  
