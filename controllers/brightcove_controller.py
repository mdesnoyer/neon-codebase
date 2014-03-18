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

import itertools
import random
import time
import threading
import tornado
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen
import tornado.httpclient
import urllib
import utils.neon
from heapq import heappush, heappop
from supportServices.neondata import VideoMetadata, ThumbnailMetadata, InMemoryCache
from utils.options import define, options
define("port", default=8888, help="run on the given port", type=int)
define("delay", default=10, help="initial delay", type=int)
define("service_url", default="http://localhost:8083", 
        help="service url", type=str)
define("mastermind_url", default="http://localhost:8086/get_directives", 
        help="mastermind url", type=str)

import logging
_log = logging.getLogger(__name__)
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

    def remove_task(self,task):
        'Mark an existing task as REMOVED.  Raise KeyError if not found.'
        entry = self.entry_finder.pop(task)
        entry[-1] = self.REMOVED

    def pop_task(self):
        'Remove and return the highest priority task. Raise KeyError if empty'
        while self.pq:
            priority, count, task = heappop(self.pq)
            if task is not self.REMOVED:
                del self.entry_finder[task]
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
    
    def __init__(self,account_id, video_id, new_tid, nosave=True):
        self.video_id = video_id
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
        else:
            _log.info("key=ThumbnailChangeTask msg=thumbnail for video %s is %s"
                    %(self.video_id, self.tid))

class TimesliceEndTask(AbstractTask):

    ''' Task that executes at the end of time slice'''

    def __init__(self, vid):
        self.video_id = vid 

    def execute(self):
        '''
        based on current state of video take a decision
        Get thumb distribution 
        '''
        controller = BrightcoveABController()
        tdist = taskmgr.get_thumbnail_distribution(self.video_id)
        controller.thumbnail_change_scheduler(self.video_id, tdist)

class ThumbnailCheckTask(AbstractTask):
    '''
    Check the current thumbnail in brightcove and verify if the thumbnail has 
    been saved in the DB.
    If thumbnail is new, save the corresponding THUMB URL => TID mapping
    '''
    def __init__(self, account_id, video_id):

        self.video_id = video_id
        self.service_url = options.service_url + \
                "/api/v1/brightcovecontroller/%s/checkthumbnail/%s"\
                %(account_id, video_id)
    
    @tornado.gen.engine
    def execute(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(method='POST', url=self.service_url,
                        request_timeout=10.0, body="")
        result = yield tornado.gen.Task(http_client.fetch, req)
        if result.error:
            _log.error("key=ThumbnailCheckTask msg=service error for video %s"
                        %self.video_id)
        else:
            _log.info("key=ThumbnailCheckTask msg=run thumbnail check")


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

    def __init__(self, taskQ):
        self.taskQ = taskQ
        self.video_map = {} #vid => VideoTaskInfo

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
        vminfo = VideoTaskInfo(vid,tdist) 
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
        task.execute()
   
    @tornado.gen.engine
    def check_scheduler(self):
        ''' Check scheduler thread '''
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

    def __init__(self, delay=0, timeslice=4260, cushion_time=600):
        self.neon_service_url = options.service_url 
        self.max_update_delay = delay
        
        self.timeslice = timeslice 
        self.cushion_time = cushion_time 

    def thumbnail_change_scheduler(self, video_id, distribution):
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

        #Make a decision based on the current state of the video data
        delay = random.randint(0, self.max_update_delay)
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
                                    self.cushion_time) 

        #abtest start time correction, i.e if 
        if (abtest_start_time + minority_thumb_timeslice) > minority_thumb_boundary:
                abtest_start_time =\
                        (self.timeslice  
                            - self.cushion_time 
                            - majority_thumb_timeslice) 

        # Task sched visualization
        #-----------------------------------------------------
        #| check |  sched A  | sched B | sched A |  end task |
        #--------0-------------------------------------------E-

        #Thumbnail Check Task -- May need to run more than once? 
        ctask = ThumbnailCheckTask(account_id, video_id)
        taskmgr.add_task(ctask, cur_time)

        #NOTE: Check what happens when you push same refID thumb to bcove
        #ans: It keeps the same thumb, discards the image being uploaded
    
        #TODO: Randomize the minority thumbnail scheduling

        #schedule A - The Majority run thumbnail at the start of time slice 
        taskA = ThumbnailChangeTask(account_id, video_id, 
                            thumbA[0], active_thumbs==1) 
        taskmgr.add_task(taskA, timeslice_start) 
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

                task = ThumbnailChangeTask(account_id, video_id, tup[0]) 
                taskmgr.add_task(task, time_to_exec_task)
                #print "---" , cur_time,delay,abtest_start_time
                _log.info ("Sched B %d" %(time_to_exec_task - cur_time - delay))
                
                # Add the time the thumbnail should run for 
                time_to_exec_task += tup[1] 
            
            _log.info ("Sched A %d" %(time_to_exec_task - cur_time - delay))
            #schedule A - The Majority run thumbnail for the rest of timeslice 
            taskA = ThumbnailChangeTask(account_id, video_id, thumbA[0]) 
            taskmgr.add_task(taskA, time_to_exec_task) 
            time_to_exec_task += (self.timeslice 
                                    - sum([tup[1] for tup in time_dist])
                                    - abtest_start_time)

            _log.info("end task %d" %(time_to_exec_task - cur_time - delay)) 
            task_time_slice = TimesliceEndTask(video_id) 
        
            #schedule End of Timeslice for a particular video
            taskmgr.add_task(task_time_slice, time_to_exec_task)
        else:
            #Dont need to end the timeslice since the majority thumbnail is 
            #designated to run until mastermind sends a changed directive
            _log.info("key=thumbnail_change_scheduler"
                    " msg=less than 1 active thumbnail for video %s" %video_id)
            pass

    def convert_from_percentages(self, pd):
        ''' Convert from fraction(%) to time '''
        
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
    
    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self,*args,**kwargs):
        
        data = self.request.body
        setup_controller_for_video(data)
        self.set_status(201)
        self.finish()

###################################################################################
# Initialize AB Controller  
###################################################################################

def setup_controller_for_video(jsondata, delay=0):
    '''
    Data from Mastermind is sent as {'d': (video_id, [(thumb_id, fraction)])}
    
    Setup the controller given json data as
    (video_id, [(thumb_id, fraction)] ...)
    '''
    controller = BrightcoveABController(delay=delay)
    directive = tornado.escape.json_decode(jsondata)
    vid_tuple = directive["d"]
    vid = vid_tuple[0]
    tid_dists = vid_tuple[1]
    taskmgr.add_video_info(vid, tid_dists) #store vid,tdist info 
    controller.thumbnail_change_scheduler(vid, tid_dists)
    return True

def initialize_brightcove_controller():
    '''
    Populate data from mastermind 
    Fetch the video id => [(Tid,%)] mappings and populate the data
    
    '''
    #Send Push request to Mastermind
    #Set alerts on Mastermind
    http_client = tornado.httpclient.HTTPClient()
    req = tornado.httpclient.HTTPRequest(method='GET',
                            url=options.mastermind_url,
                            request_timeout=10.0)
    try:
        result = http_client.fetch(req)
        if not result.error:
            directives = result.body.split('\n')
            for directive in directives:
                setup_controller_for_video(directive, options.delay)
    except Exception, e:
        _log.error("key=Initialize Controller msg=failed to query mastermind %s" %e)
   
###################################################################################
# MAIN
###################################################################################

application = tornado.web.Application([
    (r"/(.*)", GetData),
])

def main():
    SCHED_CHECK_INTERVAL = 1000 #1s
    taskQ = PriorityQ()
    global taskmgr
    taskmgr = TaskManager(taskQ)
    initialize_brightcove_controller()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.PeriodicCallback(taskmgr.check_scheduler,
            SCHED_CHECK_INTERVAL).start()
    tornado.ioloop.IOLoop.instance().start()
    
# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

