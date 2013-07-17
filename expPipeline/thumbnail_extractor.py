#!/usr/bin/python
import os
import os.path
import sys
from PIL import Image
import numpy
import ffvideo
import random
import tempfile
import time
import ConfigParser
import shortuuid
import multiprocessing
import Queue
import errorlog
import youtube
from subprocess import call
from optparse import OptionParser
import leargist

import tornado.web
import tornado.gen
import tornado.escape
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import StringIO
import signal

import youtube_dl

def sig_handler(sig, frame):
    log.debug('Caught signal: ' + str(sig) )
    sys.exit(0)


class ProcessVideo(youtube_dl.PostProcessor):
    """ class provides methods to process a given video """
    def __init__(self,url,img_directory,nthumbnails,thumbnail_location,category,image_db):
        super(ProcessVideo, self).__init__()
        self.video_url = url
        self.sec_to_extract = 1 
        self.sec_to_extract_offset = 1
        self.thumbnail_size = 720 # either 16:9 or 4:3  
        self.thumbnails_dir = img_directory
        self.nthumbnails = int(nthumbnails)
        self.thumbnail_ids = []
        self.location = thumbnail_location
        self.video_category = category
        self.image_db = image_db
        self.aspect_ratio = 0

    def get_locations(self,duration):
        locations = []
        start_duration = 0
        end_duration = 0
        
        if self.location == "start":
            end_duration = duration / 3.0 
        elif self.location == "middle":
            start_duration = duration / 3.0
            end_duration = 2 * start_duration
        elif self.location == "end":
            start_duration = 2 * (duration / 3.0)
            end_duration = duration
        else:
            #default to random
            end_duration = duration

        dur = int (end_duration - start_duration)
        if dur > self.nthumbnails:
            locations = random.sample(range(dur),self.nthumbnails)
            return locations
        else:
            log.error("Duration smaller than nthumbnails dur = " + str(dur))
            return range(dur)

    ''' process all the frames from the partial video downloaded '''
    def run(self, information):
        try:
            mov = ffvideo.VideoStream(information['filepath'])
        except Exception, e:
            log.error("key=process_video msg=movie file not found")
            raise PostProcessingError()
        
        duration = mov.duration
        self.aspect_ratio = float(mov.frame_size[0]) / mov.frame_size[1]
        width = mov.frame_width
        self.thumbnail_size = width if width < self.thumbnail_size else self.thumbnail_size
        locations = self.get_locations(duration)

        # Extract thumbnail at specific locations
        for loc in locations:
                try:
                    frame = mov.get_frame_at_sec(loc)
                    image = frame.image()
                    # RESPECT ASPECT RATIO !! 
                    size = self.thumbnail_size,self.thumbnail_size # resize on width anyways
                    image.thumbnail(size,Image.ANTIALIAS)
                    #save the images now
                    uid = shortuuid.uuid()
                    tup = (uid,frame.frameno)
                    self.thumbnail_ids.append(tup)
                    imgFile = os.path.join(self.thumbnails_dir,
                                           str(uid) + '.jpg')
                    image.save(imgFile)

                    # calculate the gist features for the image
                    image.thumbnail((256,256), Image.ANTIALIAS)
                    descriptors = leargist.color_gist(image)
                    numpy.save(imgFile + ".npy",descriptors.tolist())

                except ffvideo.NoMoreData:
                    break

                except Exception,e:
                    log.exception("key=process_video msg=processing error msg=" + e.__str__())
                    raise PostProcessingError()

        self.save_to_db()
        
        return information

    ''' save thumbnail meta data to the DB '''
    def save_to_db(self):
        dbfile = self.image_db #'image.db'
        aspect_ratio  = '%.2f' %self.aspect_ratio

        #append to a flat file
        with open(dbfile,"a") as f:
            #schema
            #img_id #origin #frameno #semantic #aspect ratio 
            for id,fno in self.thumbnail_ids:
                data = str(id) + " " + self.video_url + " " + str(fno) + " " + aspect_ratio + " " + self.video_category + " null" #valence score =null
                f.write(data + "\n" )


class VideoDownload(object):
    ''' class that downloads the video asynchronously'''

    def __init__(self,url):
        self.timeout = 300000.0
        self.tempfile = tempfile.NamedTemporaryFile(delete=False)
        self.tempfile_path = self.tempfile.name
        self.total_size_so_far = 0
        self.url = url
        self.download_url = url

    def __del__(self):
        self.cleanup()

    def process(self):
        #Process Video and save thumbnails to image library
        directory = image_directory 
        log.info("Processing : " + self.url)
        pv = ProcessVideo(self.url,directory,nthumbnails,location,category,
                          image_db)
        pv.run({'filepath': self.tempfile_path})
        log.info("Done processing: " + self.url)

    def cleanup(self):
        #Delete the downloaded video file
        if os.path.exists(self.tempfile.name):
            os.unlink(self.tempfile.name)

    def log_failed_url(self):
        #TODO make sure the file has no duplicates
        with open(unprocessed_links,'a') as f:
            f.write(self.url + '\n')

        failed_count.value += 1

    def start(self):
        #Download the video
        log.info('Starting ' + self.url)

        try:
            self.youtube_dl()
            failed_count.value = 0
        except Exception, e:
            self.log_failed_url()

            log.error("key=async_callback_error  msg=" +
                          e.message) 
                
            #temp job mgmt stuff, insert into Q
            work_queue.put(self.url)
            work_queue_map[self.url].value += 1
        
        #If youtube url
        #if "youtube" in self.url:
        #    self.youtube_downloder()

        #If vimeo
        #elif "vimeo" in self.url:
        #    self.vimeo_downloader()

        #If other
        #else:
        #    self.generic_downloader()

    def youtube_dl(self):
        fd = youtube_dl.FileDownloader({'outtmpl':unicode(self.tempfile_path),
                                       'noprogress':True,
                                       })
        fd.add_post_processor(ProcessVideo(self.url,
                                           image_directory,
                                           nthumbnails,
                                           location,
                                           category,
                                           image_db))

        for extractor in youtube_dl.gen_extractors():
            fd.add_info_extractor(extractor)
        retcode = fd.download([self.url])

    def youtube_downloder(self):
        yt = youtube.YouTube()
        for attempts in range(3):
            try:
                yt.url = self.url
            except:
                time.sleep(3)
                continue
            break
       
        #Try downloading the highest resoultion
        video = yt.get('mp4','1080p')
        if video is None:
            video = yt.get('mp4','720p')
        if video is None:
            video = yt.get('mp4','520p')
        if video is None:
            video = yt.get('flv','480p')
        if video is None:
            video = yt.get('flv','360p')

        if video is not None:
            log.info("Downloading " + self.url)
            self.download_url = video.url
            return self.generic_downloader()

        else:
            self.log_failed_url()
            log.error("Youtube error")

    def vimeo_downloader(self):
        video_id = self.url.split('/')[-1]
        call(["./download_vimeo.sh",video_id])
        #video gets saved to /tmp/{video_id}
        self.tempfile_path = '/tmp/'+ video_id 
        
        # Now process the video and extract thumbnails.
        self.process()

    def generic_downloader(self):
        self.req = tornado.httpclient.HTTPRequest(url = self.download_url ,method = 'GET',
                 use_gzip =False, request_timeout = self.timeout)
        self.http_client = tornado.httpclient.HTTPClient()
        try:
            response = self.http_client.fetch(self.req)
            self.tempfile.write(response.body)
            self.error = None
            self.process()
            failed_count.value = 0
        except tornado.httpclient.HTTPError as e:
            if e.code in [400, 403]: # YouTube wants to display an ad
                log.info('Got a %i error code. So YouTube probably wanted to show an ad' % e.code)
            elif e.code == 599: # A closed connection
                log.error("key=async_request_timeout msg=" +
                          e.message)
                ## Verify content length & total size to see if video
                ## has been downloaded == If request times out and we
                ## have 75% of data, then process the video and send
                ## data to client
                try:
                    self.content_length = e.response.headers['Content-Length']
                    if (self.total_size_so_far /float(self.content_length)) > 0.75:
                        self.process()
                        return                    
                except:
                    pass
            else:
                log.error("key=async_callback_error  msg=" +
                          e.message) 
                #print response.headers['Location']
                #log the link that wasn't downloaded
                self.log_failed_url()
                
             #temp job mgmt stuff, insert into Q
            work_queue.put(self.url)
            work_queue_map[self.url].value += 1
            failed_count.value += 1

class Worker(multiprocessing.Process):

    def __init__(self):

        # base class initialization
        multiprocessing.Process.__init__(self)

        # job management stuff
        self.kill_received = False
        self.SLEEP_INTERVAL = 5

    def run(self):
        while not self.kill_received:
            try:
                job = work_queue.get_nowait()
                print job
                #compare n retries 
                retries = work_queue_map[job].value
                if retries > 3:
                    log.error("key=worker msg=Could not download %s" % job)
                    continue

                # See if we should wait a little bit because we're
                # hitting the server too hard. We try to use
                # exponential backoff here
                if failed_count.value >= 0:
                    sleep_time = (self.SLEEP_INTERVAL * 
                        (1 << failed_count.value) + 
                        random.random())
                    sleep_time = min(sleep_time, 600)
                    log.info('We failed, so sleeping for %fs' % sleep_time)
                    time.sleep(sleep_time)

                #Download Video
                vd = VideoDownload(job)
                vd.start()
            except Queue.Empty:
                exit(0)
            except Exception,e:
                log.exception("worker error" + e.__str__())
            
            self.kill_received = True

#Video downloader (Youtube, vimeo, general)
if __name__ == "__main__":

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    parser = OptionParser()

    parser.add_option("-n", "--n_process", type='int',
                      default=1,
                      help='Number of processes to spawn')
    parser.add_option("-c","--config",default='',
                      help='Config file to use that specifies what to download')
    
    options,args = parser.parse_args()
  

    log = errorlog.FileLogger("video")

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(options.config)
    sections = config_parser.sections()
    
    #Global parameters
    global category
    global location
    global image_directory
    global image_db

    url_fname = config_parser.get('video links','filename')
    category = config_parser.get('video links','category')
    nthumbnails = config_parser.get('params','thumbnails') 
    location = config_parser.get('params','location')
    image_directory = config_parser.get('params','image_directory')
    image_db = config_parser.get('params','image_db')

    if not os.path.exists(image_directory):
        os.makedirs(image_directory)

    #List of links that haven't been processed
    global unprocessed_links
    unprocessed_links = 'failed_links.txt'
    
    url_list = [] 
    with open(url_fname) as f:
        url_list = [url.rstrip('\n') for url in f.readlines()]

    global work_queue
    global work_queue_map
    global failed_count
    
    failed_count = multiprocessing.Value('i', 0)
    work_queue_map ={}
    work_queue = multiprocessing.Queue()
    for url in url_list:
        work_queue.put(url)
        work_queue_map[url] = multiprocessing.Value('i', 1)

    workers = []
    delay = 5 #secs

    #Run Loop
    while True:
        nproc_to_fork = options.n_process - len(multiprocessing.active_children())
        #spawn workers
        for i in range(nproc_to_fork):
            worker = Worker()
            workers.append(worker)
            worker.start()
            #worker.run()

        time.sleep(delay)        
