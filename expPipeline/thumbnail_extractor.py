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
import shortuuid
import multiprocessing
import Queue
import subprocess
from optparse import OptionParser
import logging
import logging.handlers

import StringIO
import signal
import traceback

import youtube_dl

_log = logging.getLogger(__name__)

def sig_handler(sig, frame):
    _log.info('Shutting Down')
    _log.info('Traceback:\n%s' % ''.join(traceback.format_stack(frame)))
    for proc in multiprocessing.active_children():
        proc.terminate()
        proc.join(10)
    for proc in multiprocessing.active_children():
        os.kill(proc.pid, signal.SIGKILL)
    sys.exit(0)

class TimeoutError(IOError): pass


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
        elif dur == 0:
            _log.warn("Duration is less than a second. Taking middle frame.")
            return [duration / 2.0]
        else:
            _log.warn("Duration smaller than nthumbnails dur = " + str(dur))
            return range(dur)

    ''' process all the frames from the partial video downloaded '''
    def run(self, information):
        try:
            mov = ffvideo.VideoStream(information['filepath'])
        except Exception, e:
            _log.error("key=process_video msg=movie file not found")
            raise youtube_dl.PostProcessingError()
        
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

                except ffvideo.NoMoreData:
                    break

                except Exception,e:
                    _log.exception("key=process_video msg=processing error msg=" + e.__str__())
                    raise youtube_dl.PostProcessingError()

        self.save_to_db()
        
        return information

    ''' save thumbnail meta data to the DB '''
    def save_to_db(self):
        dbfile = self.image_db 
        aspect_ratio  = '%.2f' %self.aspect_ratio

        #append to a flat file
        with open(dbfile,"a") as f:
            #schema
            #img_id #origin #frameno #semantic #aspect ratio 
            for id,fno in self.thumbnail_ids:
                data = str(id) + " " + self.video_url + " " + str(fno) + " " + aspect_ratio + " " + self.video_category + " null null" #valence score =null
                f.write(data + "\n" )


class VideoDownload(object):
    ''' class that downloads the video asynchronously'''

    def __init__(self, url, image_dir, nthumbs, location, image_db):
        self.tempfile = tempfile.NamedTemporaryFile(delete=False)
        self.tempfile_path = self.tempfile.name
        self.url = url
        self.image_dir = image_dir
        self.nthumbs = nthumbs
        self.location = location
        self.image_db = image_db

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        #Delete the downloaded video file
        if os.path.exists(self.tempfile.name):
            os.unlink(self.tempfile.name)

    def run(self):
        '''Download and process the video.

        Returns True if suceeded
        '''
        #Download the video
        _log.info('Starting ' + self.url)

        try:
            return self.youtube_dl() == 0
        except Exception, e:
            _log.exception("key=youtube_dl  msg=%s" % e) 
        return False
                
            

    def youtube_dl(self):
        fd = youtube_dl.FileDownloader({'outtmpl':unicode(self.tempfile_path),
                                       'noprogress':True,
                                       })
        fd.add_post_processor(ProcessVideo(self.url,
                                           self.image_dir,
                                           self.nthumbs,
                                           self.location,
                                           'null', # category
                                           self.image_db))

        for extractor in youtube_dl.gen_extractors():
            fd.add_info_extractor(extractor)
        return fd.download([self.url])

class Worker(multiprocessing.Process):
    _DOWNLOAD_TIMEOUT = 1800 # seconds

    def __init__(self, work_queue, failed_count, image_db,
                 image_dir, failed_links, nthumbs, thumb_location):

        # base class initialization
        multiprocessing.Process.__init__(self)

        # job management stuff
        self.SLEEP_INTERVAL = 5

        self.work_queue = work_queue
        self.failed_count = failed_count
        self.image_db = image_db
        self.image_dir = image_dir
        self.failed_links = failed_links
        self.nthumbs = nthumbs
        self.thumb_location = thumb_location

    def run(self):
        try:            
            url = self.work_queue.get()

            # Setup an alarm so that we can timeout
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGALRM,
                          lambda sig, frame: self.clean_timeout(url))
            signal.alarm(Worker._DOWNLOAD_TIMEOUT)

            # See if we should wait a little bit because we're
            # hitting the server too hard. We try to use
            # exponential backoff here
            if self.failed_count.value > 0:
                sleep_time = (self.SLEEP_INTERVAL * 
                              (1 << failed_count.value) + 
                              random.random())
                sleep_time = min(sleep_time, 600)
                _log.info('We failed, so sleeping for %fs' % sleep_time)
                time.sleep(sleep_time)

            #Download Video
            vd = VideoDownload(url,
                               self.image_dir,
                               self.nthumbs,
                               self.thumb_location,
                               self.image_db)
            if not vd.run():
                self.failed_count.value += 1
                if self.failed_links is not None:
                    with open(self.failed_links,'a') as f:
                        f.write(url + '\n')
            else:
                self.failed_count.value = 0
                
        except Queue.Empty:
            exit(0)
        except Exception,e:
            _log.exception("worker error" + e.__str__())

    def clean_timeout(self, url):
        self.work_queue.put(url)
        _log.error('Timeout when downloading %s. Requeing' % url)
        raise TimeoutError()
        

class LinkLoader(multiprocessing.Process):
    '''Process that identifies when new links to download are requested and quese them.

    '''
    def __init__(self, link_file, image_db, failed_links, q,
                 sleep_interval=60):
        # base class initialization
        multiprocessing.Process.__init__(self)
        
        self.link_file = link_file
        self.q = q
        self.known_links = set([])
        self.sleep_interval = sleep_interval
        self.generate_proc = None

        # Load the known links from those that failed and the image db
        if os.path.exists(image_db):
            with open(image_db) as f:
                for line in f:
                    fields = line.split()
                    self.known_links.add(fields[1])

        if failed_links is not None and os.path.exists(failed_links):
            with open(failed_links) as f:
                for line in f:
                    self.known_links.add(line.strip())

    def run(self):
        while True:
            new_links = 0

            link_stream = sys.stdin
            if self.link_file is not None:
                link_stream = open(self.link_file)
            try:
                for line in link_stream:
                    url = line.strip()
                    if url not in self.known_links:
                        self.known_links.add(url)
                        self.q.put(url)
                        new_links +=1

                if new_links > 0:
                    _log.info('Added %i new links to the queue for downloading'
                              % new_links)

            finally:
                if self.link_file is not None:
                    link_stream.close()

            if self.generate_proc is not None:
                self.generate_proc.poll()
                retcode = self.generate_proc.returncode
                if retcode is not None:
                    if retcode > 0:
                        _log.error('Generate process exited with error '
                                   'code: %i' % retcode)
                    self.generate_proc = None

            if self.q.empty() and self.generate_proc is None:
                _log.info('The queue is empty. '
                          'Trying to generate new stimuli sets.')
                # Generate a new stimuli set (and will add more
                # candidates to the queue file if setup properly.
                self.generate_proc = subprocess.Popen(
                    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 'settings',
                                 'generate_new_stimuli_sets.sh'))
                                 
                                 

            time.sleep(self.sleep_interval)

#Video downloader (Youtube, vimeo, general)
if __name__ == "__main__":

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    parser = OptionParser()

    parser.add_option('--nthumbs', type='int', default=6,
                      help='Number of thumbnails to extract per video')
    parser.add_option('--image_db', default=None,
                      help='Image database file. Will be appended to')
    parser.add_option('--image_dir', default=None,
                      help='Directory to output the images to')
    parser.add_option('-i', '--input', default=None,
                      help=('File that contains the list of links to '
                            'download. If None, stdin is used.'))
    parser.add_option('--location', default='random',
                      help=('Where to extract the thumbnails from. '
                            'start, middle, end or random'))
    parser.add_option('--nprocess', type='int', default=1,
                      help='Number of download processes to spawn')
    parser.add_option('--failed_links', default=None,
                      help='File to list the failed links')
    parser.add_option('--log', default=None,
                      help='Log file. If none, dumps to stdout')
    parser.add_option('--debug', action='store_true', default=False,
                      help='If true, allows you to debug the worker')
    
    options,args = parser.parse_args()

    # Set the logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if options.log:
        handler = logging.handlers.RotatingFileHandler(options.log,
                                                       maxBytes=(64*1024*1024),
                                                       backupCount=5)
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(handler)
    
    if not os.path.exists(options.image_dir):
        os.makedirs(options.image_dir)

    # Define the shared data
    failed_count = multiprocessing.Value('i', 0)
    work_queue = multiprocessing.Queue()

    # Start adding entries to the queue
    queue_loader = LinkLoader(options.input, options.image_db,
                              options.failed_links, work_queue)
    queue_loader.start()

    delay = 5 #secs

    # Run Loop
    try:
        while True:                   
            nproc_to_fork = (options.nprocess + 1 -
                             len(multiprocessing.active_children()))
            #spawn workers
            for i in range(nproc_to_fork):
                worker = Worker(work_queue,
                                failed_count,
                                options.image_db,
                                options.image_dir,
                                options.failed_links,
                                options.nthumbs,
                                options.location)
                if options.debug:
                    worker.run()
                else:
                    worker.start()

            time.sleep(delay)
    finally:
        for proc in multiprocessing.active_children():
            proc.terminate()
            proc.join(10)
        for proc in multiprocessing.active_children():
            os.kill(proc.pid, signal.SIGKILL)
        
