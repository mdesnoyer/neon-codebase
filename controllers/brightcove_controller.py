#!/usr/bin/env python
'''
AB Controller for Brightcove

Listens for updates from mastermind

On recieveing an update, schedules a task to be executed at time 't'
Tasks can include -- push thumbnail X in to brightcove account A 

'''
import os,os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

from api.brightcove_api import BrightcoveApi
import datetime
from heapq import heappush, heappop
import itertools
from multiprocessing.pool import ThreadPool
import random
from supportServices.neondata import VideoMetadata, ThumbnailMetadata, \
     InMemoryCache, InternalVideoID, ThumbnailID, BrightcovePlatform
from supportServices.url2thumbnail import URL2ThumbnailIndex
import time
import threading
import tornado
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen
import tornado.httpclient
import urllib
from utils.imageutils import PILImageUtils
import utils.neon
from utils.options import define, options
define("port", default=8888, help="run on the given port", type=int)
define("delay", default=10, help="initial delay", type=int)
define("service_url", default="http://localhost:8083", 
        help="service url", type=str)
define("mastermind_url", default="http://localhost:8086/get_directives", 
        help="mastermind url", type=str)
define("max_thumb_check_threads", default=10,
       help=("Maximum number of threads used to check the brightcove "
             "thumbnail state"))
define("thumbnail_sampling_period", default=304,
       help="Period, in seconds for checking brightcove for new thumbs")

import logging
_log = logging.getLogger(__name__)

#Monitoring vars
from utils import statemon
statemon.define('pqsize', int)
statemon.define('thumbchangetask', int)
statemon.define('thumbchangetask_fail', int)
statemon.define('thumbchecktask_fail', int)
# The number of videos being ab tested
statemon.define('nvideos_abtesting', int)
# The number of videos being monitored by this controller
statemon.define('nvideos', int)

random.seed(25110)

###################################################################################
## Priority Q Impl
###################################################################################

class PriorityQ(object):

    '''
    Priority Q Implementation using heapq library
    '''
    def __init__(self):
        self.pq = []                         # list of entries arranged in a heap
        self.entry_finder = {}               # mapping of tasks to entries
        self.REMOVED = '<removed-task>'      # placeholder for a removed task
        self.counter = itertools.count()     # unique sequence count

    def add_task(self,task, priority=0):
        'Add a new task, update if already present'
        if task in self.entry_finder:
            self.remove_task(task)
        count = next(self.counter)
        entry = [priority, count, task]
        self.entry_finder[task] = entry
        heappush(self.pq, entry)
        statemon.state.pqsize = len(self.pq)

    def remove_task(self,task):
        'Mark an existing task as REMOVED.  Raise KeyError if not found.'
        entry = self.entry_finder.pop(task)
        entry[-1] = self.REMOVED
        statemon.state.pqsize = len(self.pq)

    def pop_task(self):
        'Remove and return the highest priority task. Raise KeyError if empty'
        while self.pq:
            priority, count, task = heappop(self.pq)
            if task is not self.REMOVED:
                del self.entry_finder[task]
                statemon.state.pqsize = len(self.pq)
                return task, priority
        raise KeyError('pop from an empty priority queue')

    def peek_task(self):
        ''' helper peak function '''
        if len(self.pq) >0:
            return self.pq[0]
        else:
            return (None, None, None) 

###################################################################################
# TASK Abstraction and Manager 
###################################################################################

class AbstractTask(object):
    def __init__(self, vid):
        '''Initialize the task with the internal video id'''
        self.video_id = vid

    def execute(self):
        pass

class ThumbnailChangeTask(AbstractTask):
    
    ''' Task to change the thumbnail for a given video 
        nosave flag indicates that thumbnail state should not
        be changed to chosen=true

        #special case to save the thumbnail state is when mastermind
        #sends command to revert to a particular thumbnail, the directive
        #will have only a single thumbnail with fraction=1.0
    '''
    
    def __init__(self, account_id, video_id, new_tid, nosave=True):
        super(ThumbnailChangeTask, self).__init__(video_id)
        self.tid = new_tid
        self.service_url = options.service_url + \
                "/api/v1/brightcovecontroller/%s/updatethumbnail/%s" \
                %(account_id, video_id)
        self.nosave = nosave

    @tornado.gen.engine
    def execute(self):
        '''execute, set thumbnail '''
        http_client = tornado.httpclient.AsyncHTTPClient()
        body = urllib.urlencode(
                {'thumbnail_id': self.tid, 'nosavedb': self.nosave})
        req = tornado.httpclient.HTTPRequest(method='POST',
                        url=self.service_url, body=body,
                        request_timeout=10.0)
        result = yield tornado.gen.Task(http_client.fetch, req)
        if result.error:
            _log.error("key=ThumbnailChangeTask msg=thumbnail change failed" 
                    " video %s tid %s"%(self.video_id, self.tid))
            statemon.state.increment('thumbchangetask_fail')
        else:
            _log.info("key=ThumbnailChangeTask msg=thumbnail for video %s is %s"
                    %(self.video_id, self.tid))
            statemon.state.increment('thumbchangetask')

class TimesliceEndTask(AbstractTask):

    ''' Task that executes at the end of time slice'''
    def __init__(self, video_id, controller):
        super(TimesliceEndTask, self).__init__(video_id)
        self.controller = controller
        
    def execute(self):
        '''
        based on current state of video take a decision
        Get thumb distribution 
        '''
        tdist = self.controller.taskmgr.get_thumbnail_distribution(
            self.video_id)

        statemon.state.decrement('nvideos_abtesting')
        self.controller.thumbnail_change_scheduler(self.video_id, tdist)

class ThumbnailCheckTask(AbstractTask):
    '''
    Check the current thumbnail in brightcove and verify if the thumbnail has 
    been saved in the DB.
    
    If thumbnail is new, save the corresponding THUMB URL => TID
    mapping and create a new entry for the thumbnail if it doesn't
    exist already.
    
    '''   
    def __init__(self, video_id, url2thumb):
        super(ThumbnailCheckTask, self).__init__(video_id)
        self.url2thumb = url2thumb
        
    def execute(self):
        # Get the BrightcovePlatform associated with this video id
        video = VideoMetadata.get(self.video_id)
        if video is None:
            _log.error("key=ThumbnailCheckTask "
                       "msg=Could not find video id: %s" % self.video_id)
            statemon.state.increment('thumbchecktask_fail')
            return
        platform = BrightcovePlatform.get_account(video.get_account_id(),
                                                  video.integration_id)
        if platform is None:
            _log.error("key=ThumbnailCheckTask "
                       "msg=Could not find brightcove platform for video: %s" 
                       % self.video_id)
            statemon.state.increment('thumbchecktask_fail')
            return
        
        # Get the current thumbnail url showing on brightcove for this video
        thumb_url, still_url = platform.get_api().get_current_thumbnail_url(
            InternalVideoID.to_external(self.video_id))

        if thumb_url is None:
            _log.error("key=ThumbnailCheckTask "
                       "msg=Could not find thumbnail url for video: %s" %
                       self.video_id)
            statemon.state.increment('thumbchecktask_fail')
            return

        # Record the new thumbnail if it's new
        thumb_info = self.url2thumb.get_thumbnail_info(
            thumb_url, 
            internal_video_id=self.video_id)

        if thumb_info is None:
            # Somebody uploaded a new thumbnail to Brightcove so we
            # are going to record it.
            image = PILImageUtils.download_image(thumb_url)
            tid = ThumbnailID.generate(image, self.video_id)
            created = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            thumb = ThumbnailMetadata(tid, self.video_id, [thumb_url],
                                      created, image.size[0], image.size[1],
                                      'brightcove', rank=0)
            thumb.update_phash(image)
            thumb.save()

            #TODO(mdesnoyer): We have a potential race condition here
            #when updating the ranks in the brightcove thumbnails for
            #this video. It's not very risky because this task is only
            #ever running once (unless it takes longer than the
            #period), but it should be locked properly.
            video = VideoMetadata.get(self.video_id)
            video.thumbnail_ids.append(tid)
            video.save()
            for thumb_id in video.thumbnail_ids:
                if thumb_id == tid:
                    continue
                def inc_rank(t): 
                    if t.type == 'brightcove':
                        t.rank += 1
                ThumbnailMetadata.modify(thumb_id, inc_rank)

            self.url2thumb.add_thumbnail_to_index(thumb)


class VideoTaskInfo(object):
    '''
    Info about currently enqueud tasks associated with a video
    Also stores thumbnail distribution for a video
    '''
    def __init__(self, vid, tdist):
        self.video_id = vid
        self.dist = tdist
        self.tasks = []

    def remove_task(self, task):
        ''' remove task '''
        try:
            self.tasks.remove(task)
        except Exception,e:
            _log.exception('key=VideoTaskInfo msg=remove task exception %s' %e)

    def add_task(self, task):
        ''' Add task '''
        self.tasks.append(task)

    def get_thumbnail_distribution(self):
        ''' get thumbnail distribution '''
        return self.dist

class TaskManager(object):

    def __init__(self, poll_interval=1000):
        self.taskQ = PriorityQ()
        self.video_map = {} #vid => VideoTaskInfo

        # Start the task manager
        tornado.ioloop.PeriodicCallback(self.check_scheduler,
                                        poll_interval).start()

    def add_task(self, task, priority):
        ''' Add task to the taskQ '''
        self.taskQ.add_task(task, priority)
        vid = task.video_id
        if self.video_map.has_key(vid):
            vminfo = self.video_map[vid]
            vminfo.add_task(task)

    def pop_task(self):
        ''' pop '''
        try:
            task, priority = self.taskQ.pop_task()
        except Exception, e:
            _log.error("key=TaskManager msg=trying to pop from empty Q")
            return

        vid = task.video_id
        if self.video_map.has_key(vid):
            vminfo = self.video_map[vid]
            vminfo.remove_task(task)
        return task, priority

    def clear_taskinfo_for_video(self,vid):
        ''' #If already present, then clear the old tasks'''

        vtinfo = self.video_map[vid]
        for task in vtinfo.tasks:
            self.taskQ.remove_task(task)

        vtinfo.tasks = []

    def add_video_info(self, vid, tdist):
        ''' Add video info '''
        vminfo = VideoTaskInfo(vid, tdist) 
        if self.video_map.has_key(vid):
            self.clear_taskinfo_for_video(vid)

        self.video_map[vid] = vminfo
        
    def get_thumbnail_distribution(self, vid):
        ''' get thumbnail distribution  ''' 
        vtinfo = self.video_map[vid]
        return vtinfo.get_thumbnail_distribution()

    def task_worker(self, task):
        '''
        Schedule and execute task (one thread per task)
        '''
        #_log.info("[exec] %s %s" %(task, threading.current_thread() ))
        try:
            task.execute()
        except Exception as e:
            _log.exception('key=TaskManager msg=Uncaught exception: %s' %
                           e)
   
    @tornado.gen.engine
    def check_scheduler(self):
        ''' Check scheduler thread '''
        statemon.state.nvideos = len(self.video_map)
        priority, count, task = self.taskQ.peek_task()
        cur_time = time.time()
        if priority and priority <= cur_time:
            task, p = self.pop_task() 
            t = threading.Thread(target=self.task_worker, args=(task,))
            t.setDaemon(True)
            t.start()

###################################################################################
# Brightcove AB Controller Logic 
###################################################################################
class BrightcoveABController(object):
    ''' Brightcove AB controller '''

    def __init__(self, timeslice=4260, cushion_time=600):
        self.neon_service_url = options.service_url
        
        self.timeslice = timeslice 
        self.cushion_time = cushion_time

        self.taskmgr = TaskManager()
        self.url2thumb = URL2ThumbnailIndex()
        self.monitored_videos = set()
        self.thumb_check_pool = ThreadPool(options.max_thumb_check_threads)
        
        self.tornado_app = tornado.web.Application([
            (r"/(.*)", GetData, dict(controller=self)),
            ])

        # Check brightcove for the thumbnail state at least every 5 minutes
        tornado.ioloop.PeriodicCallback(
            self.check_thumbnails,
            options.thumbnail_sampling_period * 1000).start()

        self._initialized = False

    def load_initial_state(self):
        _log.info('Initializing the BrightCove controller')
        
        self.url2thumb.build_index_from_neondata()

        # Load the state from Mastermind
        response = utils.http.send_request(tornado.httpclient.HTTPRequest(
            url=options.mastermind_url))
        if response.error:
            _log.error('key=load_initial_state '
                       'msg=Failed to load data from mastermind')
            raise response.error
        directives = result.body.split('\n')
        for directive in directives:
            self.apply_directive(directive, options.delay)

        self._initialized = True

    def apply_directive(self, json_directive, max_update_delay=0):
        '''Apply a directive to a controller.

        Inputs:
        json_directive - JSON specifying the directive as 
                         {'d': (video_id, [(thumb_id, fraction)])}
        max_update_delay - Maximum delay to apply the directive
        '''
        #if not self._initialized:
        #    _log.critical('The controller has not been initialized and you '
        #                  'are trying to load a directive.')
        #    raise RuntimeError('Controller must be initialized first')

        parsed = tornado.escape.json_decode(json_directive)
        video_id, distribution = parsed['d']

        self.monitored_videos.add(video_id)

        # New data from mastermind, so add any new thumbnails to the
        # url2thumb index.
        for thumb in ThumbnailMetadata.get_many([x[0] for x in distribution]):
            self.url2thumb.add_thumbnail_to_index(thumb)

        self.taskmgr.add_video_info(video_id, distribution)
        self.thumbnail_change_scheduler(video_id, distribution,
                                        max_update_delay)

    def check_thumbnails(self):
        '''Launch jobs to check all of the urls on Brightcove.'''
        for video_id in self.monitored_videos:
            task = ThumbnailCheckTask(video_id, self.url2thumb)
            self.thumb_check_pool.apply_async(task.execute)

    def start(self):
        '''Starts running the brightcove controller on the current thread.

        Blocks until the controller is shut down.
        '''
        
        server = tornado.httpserver.HTTPServer(self.tornado_app)
        server.listen(options.port)
        tornado.ioloop.IOLoop.current().start()

    def thumbnail_change_scheduler(self, video_id, distribution,
                                   max_update_delay=0):
        ''' Change thumbnail scheduler '''

        account_id = video_id.split('_')[0] 

        #The time in seconds each thumbnail to be run
        time_dist = self.convert_from_percentages(distribution)
        
        active_thumbs = 0
        for thumb, thumb_time in time_dist:
            if thumb_time > 0.0:
                active_thumbs += 1

        #No thumbs are active, skip
        if active_thumbs == 0:
            return
        
        #If Active thumbnail ==1 and is brightcove
        #and no thumb is chosen, then skip. 
        if active_thumbs ==1:
            vm = VideoMetadata.get(video_id)
            tids = vm.thumbnail_ids 
            thumbnails = ThumbnailMetadata.get_many([tids])
            chosen = False
            for thumb in thumbnails:
                if thumb and thumb.chosen == True:
                    chosen = True
            
            if not chosen:
                return

        #import pdb; pdb.set_trace()
        #Make a decision based on the current state of the video data
        delay = random.randint(0, max_update_delay)
        cur_time = time.time()
        time_to_exec_task = cur_time + delay
        timeslice_start = time_to_exec_task 
        thumbA = time_dist.pop(0)
        
        #Minority thumbnail sums 
        majority_thumb_timeslice = thumbA[1]
        minority_thumb_timeslice = self.timeslice -\
                                        majority_thumb_timeslice 
        minority_thumb_boundary = self.timeslice -\
                                        2*self.cushion_time

        #Time when the thumbnail to be A/B tested starts (the lower % thumbnail)
        abtest_start_time = random.randint(self.cushion_time,
                                self.timeslice -
                                    int(minority_thumb_timeslice +1) #could be fraction, hence +1s 
                                    - self.cushion_time)
        # TODO(sunil): Fix
        # this. It is broken. You get negative numbers
        # sometimes. e.g. if A=0.9, B=0.1, cushion is 600, timeslice
        # is 4260, abtest_start_time is 3093

        # Task sched visualization
        #-----------------------------------------------------
        #| check |  sched A  | sched B | sched A |  end task |
        #--------0-------------------------------------------E-

        #Thumbnail Check Task -- May need to run more than once?
        self.taskmgr.add_task(ThumbnailCheckTask(video_id, self.url2thumb),
                              cur_time)

        #NOTE: Check what happens when you push same refID thumb to bcove
        #ans: It keeps the same thumb, discards the image being uploaded
    
        #TODO: Randomize the minority thumbnail scheduling

        #schedule A - The Majority run thumbnail at the start of time slice 
        taskA = ThumbnailChangeTask(account_id, video_id, 
                                    thumbA[0], active_thumbs==1) 
        self.taskmgr.add_task(taskA, timeslice_start) 
        _log.info("Sched A %s %s" % ((time_to_exec_task - cur_time - delay), 
                                     time_dist))

        #if Active thumbnail count <=1, skip
        if active_thumbs >1:
            #schedule the B's - the lower % thumbnails
            time_to_exec_task += abtest_start_time
            for tup in time_dist:
                #Avoid scheduling if time allocated is 0
                if tup[1] <= 0.0:
                    continue

                # Schedule a check of the brightcove url before we change it
                self.taskmgr.add_task(
                    ThumbnailCheckTask(video_id, self.url2thumb),
                    time_to_exec_task - 10)

                task = ThumbnailChangeTask(account_id, video_id, tup[0]) 
                self.taskmgr.add_task(task, time_to_exec_task)
                #print "---" , cur_time,delay,abtest_start_time
                _log.info ("Sched B %d" %(time_to_exec_task - cur_time - delay))
                
                # Add the time the thumbnail should run for 
                time_to_exec_task += tup[1] 
            
            _log.info ("Sched A %d" %(time_to_exec_task - cur_time - delay))
            #schedule A - The Majority run thumbnail for the rest of timeslice 
            taskA = ThumbnailChangeTask(account_id, video_id, thumbA[0]) 
            self.taskmgr.add_task(taskA, time_to_exec_task) 
            time_to_exec_task += (self.timeslice 
                                    - sum([tup[1] for tup in time_dist])
                                    - abtest_start_time)

            _log.info("end task %d" %(time_to_exec_task - cur_time - delay)) 
            task_time_slice = TimesliceEndTask(video_id, self) 
        
            #schedule End of Timeslice for a particular video
            self.taskmgr.add_task(task_time_slice, time_to_exec_task)
            
            statemon.state.increment('nvideos_abtesting')
        else:
            #Dont need to end the timeslice since the majority thumbnail is 
            #designated to run until mastermind sends a changed directive
            _log.info("key=thumbnail_change_scheduler"
                    " msg=less than 1 active thumbnail for video %s" %video_id)
            pass

    def convert_from_percentages(self, pd):
        ''' Convert from fraction(%) to time 

        Returns: [(thumb_id, # of seconds to run)] sorted by the number of seconds
        '''
        
        total_pcnt = sum([float(tup[1]) for tup in pd])
        time_dist = []

        for tup in pd:
            pcnt = float(tup[1])
            if total_pcnt > 1:
                tslice = (pcnt/total_pcnt) * self.timeslice  
            else:
                tslice = pcnt * self.timeslice

            pair = (tup[0], tslice)
            time_dist.append(pair)
        
        sorted_time_dist = sorted(time_dist, key=lambda tup: tup[1], reverse=True)
        return sorted_time_dist 

###################################################################################
# Create Tornado server application
###################################################################################

class GetData(tornado.web.RequestHandler):

    def initialize(self, controller=None):
        self.controller = controller
    
    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        
        '''
        Handler that recieves data from mastermind
        '''
        self.controller.apply_directive(self.request.body)
        self.set_status(201)
        statemon.state.decrement('nvideos_abtesting')
        self.finish()

    print "directive " , directive 
   
###################################################################################
# MAIN
###################################################################################

def main():
    controller = BrightcoveABController()
    controller.load_initial_state()
    controller.start()
    
# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

