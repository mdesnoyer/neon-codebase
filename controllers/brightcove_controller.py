'''
AB Controller for Brightcove

Listens for updates from mastermind

On recieveing an update, schedules a task to be executed at time 't'
Tasks can include -- push thumbnail X in to brightcove account A 

'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen
import tornado.httpclient
import time
from socket import *
from heapq import *
import random
import itertools
import os
import sys
import threading
import urllib
import utils.neon
from supportServices.neondata import *

from utils.options import define, options
define("port", default=8888, help="run on the given port", type=int)
define("service_url", default="http://localhost:8083/", 
        help="service url", type=str)

import logging
_log = logging.getLogger(__name__)

####################################################################################################
## Priority Q Impl
####################################################################################################

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
        'Remove and return the lowest priority task. Raise KeyError if empty.'
        while self.pq:
            priority, count, task = heappop(self.pq)
            if task is not self.REMOVED:
                del self.entry_finder[task]
                return task
        raise KeyError('pop from an empty priority queue')

    def peek_task(self):
        if len(self.pq) >0:
            return self.pq[0]
        else:
            return (None,None,None) 

###################################################################################
# TASK Abstraction and Manager 
###################################################################################

class AbstractTask(object):
    def __init__(self,vid):
        self.video_id = vid

    def execute(self):
        pass

class ThumbnailChangeTask(AbstractTask):
    
    ''' Task to change the thumbnail for a given video '''
    
    def __init__(self,account_id,video_id,new_tid):
        self.video_id = video_id
        self.tid = new_tid
        self.service_url = options.service_url + "/" + account_id + "/updatethumbnail/" + str(video_id) 
    
    def execute(self):
        self.set_thumbnail()

    @tornado.gen.engine
    def set_thumbnail(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        body = urllib.urlencode({'thumbnail_id':self.tid})
        req = tornado.httpclient.HTTPRequest(method = 'POST',url = self.service_url,body=body,
                        request_timeout = 10.0)
        result = yield tornado.gen.Task(http_client.fetch,req)
        if result.error:
            _log.error("key=ThumbnailChangeTask msg=thumbnail change failed")
        else:
            _log.debug("key=ThumbnailChangeTask msg=thumbnail for video %s is %s"
                        %(self.video_id,self.tid))

class TimesliceEndTask(AbstractTask):

    ''' Task that executes at the end of time slice'''

    def __init__(self,vid):
        self.video_id = vid 

    def execute(self):
        #based on current state of video take a decision
        #Get thumb distribution 
        controller = BrightcoveABController()
        tdist = taskmgr.get_thumbnail_distribution(self.video_id)
        controller.thumbnail_change_scheduler(self.video_id,tdist)

class ThumbnailCheckTask(AbstractTask):
    '''
    Check the current thumbnail in brightcove and verify if the thumbnail has been 
    saved in the DB.
    If thumbnail is new, save the corresponding THUMB URL => TID mapping
    '''
    def __init__(self,account_id,video_id):

        self.video_id = video_id
        self.service_url = options.service_url + "/" + account_id + "/checkthumbnail/" + str(video_id) 
    
    def execute(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(method = 'POST',url = self.service_url,
                        request_timeout = 10.0)
        result = yield tornado.gen.Task(http_client.fetch,req)
        if result.error:
            _log.error("key=ThumbnailCheckTask msg=service error for video %" %self.video_id)

class VideoTaskInfo(object):
    '''
    Info about currently enqueud tasks associated with a video
    Also stores thumbnail distribution for a video
    '''
    def __init__(self,vid,tdist):
        self.video_id = vid
        self.dist = tdist
        self.tasks = []

    def remove_task(self,task):
        try:
            self.tasks.remove(task)
        except Exception,e:
            _log.exception('key=VideoTaskInfo msg=remove task exception %s' %e)

    def add_task(self,task):
        self.tasks.append(task)

    def get_thumbnail_distribution(self):
        return self.dist

class TaskManager(object):

    def __init__(self,taskQ):
        self.taskQ = taskQ
        self.video_map = {} #vid => VideoTaskInfo

    def add_task(self,task,priority):
        self.taskQ.add_task(task,priority)
        vid = task.video_id
        if self.video_map.has_key(vid):
            vminfo = self.video_map[vid]
            vminfo.add_task(task)

    def pop_task(self):
        try:
            task = self.taskQ.pop_task()
        except:
            _log.error("key=TaskManager msg=trying to pop from empty Q")
            return

        vid = task.video_id
        if self.video_map.has_key(vid):
            vminfo = self.video_map[vid]
            vminfo.remove_task(task)
        return task

    #If already present, then clear the old tasks
    def clear_taskinfo_for_video(self,vid):
        vtinfo = self.video_map[vid]
        for task in vtinfo.tasks:
            self.taskQ.remove_task(task)

        vtinfo.tasks = []

    def add_video_info(self,vid,tdist):
        vminfo = VideoTaskInfo(vid,tdist) 
        if self.video_map.has_key(vid):
            self.clear_taskinfo_for_video(vid)

        self.video_map[vid] = vminfo
        
    def get_thumbnail_distribution(self,vid):
        vtinfo = self.video_map[vid]
        return vtinfo.get_thumbnail_distribution()

    '''
    Schedule and execute task (one thread per task)
    '''
    def task_worker(self,task):
        #_log.info("[exec] %s %s" %(task, threading.current_thread() )) 
        task.execute()
   
    @tornado.gen.engine
    def check_scheduler(self):
        priority, count, task = self.taskQ.peek_task()
        cur_time = time.time()
        if priority and priority <= cur_time:
            task = self.pop_task() 
            t = threading.Thread(target=self.task_worker,args=(task,))
            t.setDaemon(True)
            t.start()

###################################################################################
# Brightcove AB Controller Logic 
###################################################################################
class BrightcoveABController(object):

    timeslice = 71 *60    #timeslice for experiment
    cushion_time = 10 *60 #cushion time for data extrapolation

    def __init__(self,delay=0):
        self.neon_service_url = options.service_url 
        self.max_update_delay = delay

    def thumbnail_change_scheduler(self,video_id,distribution):
        
        account_id = video_id.split('_')[0] 

        #Make a decision based on the current state of the video data
        delay = random.randint(0, self.max_update_delay)
        abtest_start_time = random.randint(BrightcoveABController.cushion_time,
                BrightcoveABController.timeslice - BrightcoveABController.cushion_time) 
       
        time_dist = self.convert_from_percentages(distribution)
        thumbA = time_dist.pop(0)
        cur_time = time.time()
        time_to_exec_task = cur_time + delay
        
        #Thumbnail Check Task -- May need to run more than once? 
        ctask = ThumbnailCheckTask(account_id,video_id)
        taskmgr.add_task(ctask,cur_time)

        #TODO: Check what happens when you push same refID thumb to bcove
    
        #TODO: Randomize the minority thumbnail scheduling

        #schedule A - The Majority run thumbnail at the start of time slice 
        taskA = ThumbnailChangeTask(account_id,video_id,thumbA[0]) 
        taskmgr.add_task(taskA,time_to_exec_task) 

        #schedule the B's - the lower % thumbnails
        time_to_exec_task += abtest_start_time
        for tup in time_dist:
            task = ThumbnailChangeTask(account_id,video_id,tup[0]) 
            taskmgr.add_task(task,time_to_exec_task)
            time_to_exec_task += tup[1] # Add the time the thumbnail should run for 
        
        #schedule A - The Majority run thumbnail 
        taskA = ThumbnailChangeTask(account_id,video_id,thumbA[0]) 
        taskmgr.add_task(taskA,cur_time + delay) 
        time_to_exec_task += (BrightcoveABController.timeslice 
                                    - sum([tup[1] for tup in time_dist])
                                    - abtest_start_time)

        task_time_slice = TimesliceEndTask(video_id) 
        #schedule End of Timeslice for a particular video
        taskmgr.add_task(task_time_slice,time_to_exec_task)
        

    def convert_from_percentages(self,pd):
        total_pcnt = sum([tup[1] for tup in pd])
        time_dist = []

        for tup in pd:
            pcnt = float(tup[1])
            if total_pcnt > 1:
               tslice = (pcnt/total_pcnt) * BrightcoveABController.timeslice  
            else:
                tslice = pcnt * BrightcoveABController.timeslice

            pair = (tup[0],tslice)
            time_dist.append(pair)
        
        sorted_time_dist = sorted(time_dist, key=lambda tup: tup[1],reverse=True)
        return sorted_time_dist 


###################################################################################
# Create Tornado server application
###################################################################################

class GetData(tornado.web.RequestHandler):
    
    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self,*args,**kwargs):
        
        #input data = { video_id => [(tid,%)] }
        #print "[getdata] ", threading.current_thread()
        controller = BrightcoveABController()
        
        #For the videoid, get its metadata and insert in to Q
        data = tornado.escape.json_decode(self.request.body)
        for vid,tid_dists in vids.iteritems():
            controller.thumbnail_swap_scheduler(vid,tid_dists)
            taskmgr.add_video_info(vid,tid_dists) #store vid,tdist info 
            
        #priority = time.time() 
        #taskQ.add_task(task,priority) 
        self.finish()

application = tornado.web.Application([
    (r"/",GetData),
])

###################################################################################
# Initialize AB Controller  
###################################################################################

'''
Populate data from mastermind 

Fetch the video id => [(Tid,%)] mappings and populate the data
'''
def initialize_controller():
    pass
    
    #Send Push request to Mastermind
    #Set alerts on Mastermind

###################################################################################
# MAIN
###################################################################################

def main():
    utils.neon.InitNeon()
    SCHED_CHECK_INTERVAL = 1000 #1s
    taskQ = PriorityQ()
    taskmgr = TaskManager(taskQ)
    initialize_controller()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.PeriodicCallback(taskmgr.check_scheduler,SCHED_CHECK_INTERVAL).start()
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
   main()
