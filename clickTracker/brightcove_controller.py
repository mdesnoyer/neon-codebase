'''
AB Controller for Brightcove

Listens for updates from mastermind

On recieveing an update, schedules a task to be executed at time 't'
Tasks can include -- push thumbnail X in to brightcove account A 

'''
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
sys.path.insert(0,os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../api')))
import threading
import urllib
import logging
import logging.handlers
logging.basicConfig(filename= __file__.split('.')[0] + '.log', filemode='a', level=logging.DEBUG)
log = logging.getLogger(__name__)

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
    def execute(self):
        pass

class ThumbnailChangeTask(AbstractTask):
    
    ''' Task to change the thumbnail for a given video '''
    
    def __init__(self,video_id,new_tid):
        self.video_id = video_id
        self.tid = new_tid
        self.service_url = self.neon_service_url + "/" + account_id + "/updatethumbnail/" + str(video_id) 
    
    def execute(self):
        self.set_thumbnail()

    @tornado.gen.engine
    def set_thumbnail(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        body = urllib.urlencode({'thumbnail_id':self.tid})
        req = tornado.httpclient.HTTPRequest(method = 'POST',url = self.service_url,body=body,
                        request_timeout = 10.0)
        http_client.fetch(req,self.cb)
        result = yield tornado.gen.Task(http_client.fetch,url)
        if result.error:
            log.error("key=ThumbnailChangeTask msg=thumbnail change failed")
        else:
            log.debug("key=ThumbnailChangeTask msg=thumbnail for video %s is %s"
                        %(self.video_id,self.tid))

class TimesliceEndTask(AbstractTask):

    ''' Task that executes at the end of time slice'''

    def __init__(self,vid):
        self.video_id = vid 

    def execute(self):
        pass
        #based on current state of video take a decision
        #Get thumb distribution 
        

class ThumbnailCheckTask(AbstractTask):
    '''
    Check the current thumbnail in brightcove and verify if the thumbnail has been 
    saved in the DB.
    If thumbnail is new, save the corresponding THUMB URL => TID mapping
    '''
    def __init__(self,video_id):
        self.video_id = video_id
        self.service_url = self.neon_service_url + "/" + account_id + "/checkthumbnail/" + str(video_id) 
    
    def execute(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(method = 'POST',url = self.service_url,body=body,
                        request_timeout = 10.0)
        http_client.fetch(req,self.cb)
        result = yield tornado.gen.Task(http_client.fetch,url)
        if result.error:
            log.error("key=ThumbnailCheckTask msg=service error for video %" %self.video_id)

class TaskManager(object):
    '''
    Schedule and execute task (one thread per task)
    '''
    def task_worker(self,task):
        print "[exec] " ,task, threading.current_thread()  
        task.execute()
    
    def check_scheduler(self):
        priority, count, task = taskQ.peek_task()
        cur_time = time.time()
        if priority and priority <= cur_time:
            task = taskQ.pop_task() 
            t = threading.Thread(target=self.task_worker,args=(task,))
            t.setDaemon(True)
            t.start()

###################################################################################
# Brightcove AB Controller Logic 
###################################################################################
class BrightcoveABController(object):

    service_url =  "http://localhost:8083"
    timeslice = 71    #timeslice for experiment
    cushion_time = 10 #cushion time for data extrapolation

    def __init__(self,delay=0):
        #self.neon_service_url = "http://services.neon-lab.com"
        self.neon_service_url = "http://localhost:8083"
        self.max_update_delay = delay

    def cb(self,result):
        print "[cb]" ,time.time(), len(result.body), threading.current_thread()

    def thumbnail_change_scheduler(self,video_metadata,distribution):
        
        video_id = video_metadata.get_id()
        
        #Make a decision based on the current state of the video data
        delay = random.randint(0, self.max_update_delay)
        abtest_start_time = random.randint(BrightcoveABController.cushion_time,
                BrightcoveABController.timeslice - BrightcoveABController.cushion_time) 
        
        time_dist = self.convert_from_percentages(distribution)
        thumbA = time_dist.pop(0)
        cur_time = time.time()
        time_to_exec_task = cur_time + delay
        
        #Thumbnail Check Task -- May need to run more than once? 
        ctask = ThumbnailCheckTask(video_id)
        taskmgr.add_task(ctask,cur_time)

        #TODO: Check what happens when you push same refID thumb to bcove
    
        #TODO: Randomize the minority thumbnail scheduling

        #schedule A - The Majority run thumbnail 
        taskA = ThumbnailChangeTask(video_id,thumbA[0]) 
        taskmgr.add_task(taskA,time_to_exec_task) 

        #schedule the B's - the lower % thumbnails
        time_to_exec_task += abtest_start_time
        for tup in time_dist:
            task = ThumbnailChangeTask(video_id,tup[0]) 
            taskmgr.add_task(task,time_to_exec_task)
            time_to_exec_task += tup[1] # Add the time the thumbnail should run for 
        
        #schedule A - The Majority run thumbnail 
        taskmgr.add_task(taskA,time_to_exec_task) 
        time_to_exec_task += (BrightcoveABController.timeslice 
                                    - sum([tup[0] for tup in time_dist])
                                    - abtest_start_time)

        task_time_slice = TimesliceEndTask(video_id) 
        #schedule End of Timeslice for a particular video
        taskmgr.add_task(task_time_slice,time_to_exec_task)

    def convert_from_percentages(self,pd):
        total_pcnt = sum([tup[0] for tup in pd])
        time_dist = []

        for tup in pd:
            pcnt = float(tup[1])
            if total_pcnt != 100:
               tslice = (pcnt/total_pcnt) * BrightcoveABController.timeslice  
            else:
                tslice = (pcnt/100) * BrightcoveABController.timeslice

            pair = (tup[0],tslice)
            time_dist.append(pair)
        
        sorted_time_dist = sorted(time_dist, key=lambda tup: tup[1],reverse=True)
        return sorted_time_dist 


###################################################################################
# Create Tornado server application
###################################################################################

from tornado.options import define, options
define("port", default=8888, help="run on the given port", type=int)
define("service_url", default="http://services.neon-lab.com", 
        help="service url", type=basestring)

class GetData(tornado.web.RequestHandler):
    
    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self,*args,**kwargs):
        
        #input data = { video_id => [(tid,%)] }
        print "[getdata] ", threading.current_thread()
        controller = BrightcoveABController()
        
        #For the videoid, get its metadata and insert in to Q
        data = tornado.escape.json_decode(self.request.body)
        for vid,tid_dists in vids.iteritems():
            vmdata = yield tornado.gen.Task(VideoMetadata.get,vid)
            if vmdata is not None:
                controller.thumbnail_swap_scheduler(vmdata,tid_dists)
            
        priority = time.time() 
        taskQ.add_task(task,priority) 
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
    vids = {"int_vid1": [('i1',20)('i2',80)],"int_vid2": [('ii1',30)('ii2',70)] }
    controller = BrightcoveABController(delay=10) #stagger initial videos by introducing delay
    return

    for vid,tid_dists in vids.iteritems():
        tids = [tup[0] for tup in tid_dists] 
        vmdata = VideoMetadata.get(vid)
        #Insert in to video map (data cache)
        video_map[vid] = tid_dists    
        controller.thumbnail_change_scheduler(vmdata,tid_dists)
    
###################################################################################
# MAIN
###################################################################################

def main():
    SCHED_CHECK_INTERVAL = 1000 #1s

    global taskQ
    taskQ = PriorityQ()
    global video_map
    video_map = {}
    taskmgr = TaskManager()
    #initialize_controller()
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.PeriodicCallback(taskmgr.check_scheduler,SCHED_CHECK_INTERVAL).start()
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
   main()
