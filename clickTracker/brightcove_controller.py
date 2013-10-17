'''
AB Controller for Brightcove

Listens for updates from mastermind

On recieveing an update, schedules a task to be executed at time 't'
Tasks can include -- push thumbnail X in to brightcove account A 

#TODO: Try making the Task executor subscribe for tasks, have a pool of processes for exec.
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
import logging
import logging.handlers
logging.basicConfig(filename= __file__.split('.')[0] + '.log', filemode='a', level=logging.DEBUG)
log = logging.getLogger(__name__)

############################################################
## Priority Q Impl
############################################################

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

###########################################
# Thread Workers
###########################################

class TaskManager(object):
    '''
    Schedule and execute task (one thread per task)
    '''
    def thread_worker(self,task):
        print "[exec] " ,task, threading.current_thread()  
        bc = BrightcoveABController()
        bc.set_thumbnail(1,2)

    def check_scheduler(self):
        priority, count, task = taskQ.peek_task()
        cur_time = time.time()
        if priority and priority <= cur_time:
            task = taskQ.pop_task() 
            t = threading.Thread(target=self.thread_worker,args=(task,))
            t.setDaemon(True)
            t.start()

class BrightcoveThumbnailChecker(threading.Thread):
    '''
    Check the current thumbnail in brightcove and verify if the thumbnail has been 
    saved in the DB.
    If thumbnail is new, save the corresponding THUMB URL => TID mapping
    '''
    def __init__(self):
        self.videos_seen = {} #video_id => thumbnails urls mapped

    def brightcove_data_callback(self,response):
        pass

    def thumbnail_db_callback(self,response):
        pass

    def run(self):
        #Query Brightcove for all the videos
        pass


###########################################
# Brightcove AB Controller Logic 
###########################################
class BrightcoveABController(object):

    service_url =  "http://localhost:8083"
    timeslice = 71    #timeslice for experiment
    cushion_time = 10 #cushion time for data extrapolation
    max_update_delay = 10

    def __init__(self):
        #self.neon_service_url = "http://services.neon-lab.com"
        self.neon_service_url = "http://localhost:8083"

    def cb(self,result):
        print "[cb]" ,time.time(), len(result.body), threading.current_thread()

    @tornado.gen.engine
    def set_thumbnail(self,video_id,tid):
        http_client = tornado.httpclient.AsyncHTTPClient()
        #url = self.neon_service_url + "/" + account_id + "/" + updatethumbnail + "/" + str(video_id)
        #"http://54.221.234.42:9082"
        url = "ogle.com"
        print "[Make call] ", time.time(), threading.current_thread()
        #http_client.fetch(url,self.cb)
        result = yield tornado.gen.Task(http_client.fetch,url)
        print "[yield]",time.time(), len(result.body), threading.current_thread()

    def thumbnail_change_scheduler(self,video_metadata,distribution):
        #Make a decision based on the current state of the video data
        delay = random.randint(0,BrightcoveABController.max_update_delay)
        abtest_start_time = random.randint(BrightcoveABController.cushion_time,
                BrightcoveABController.timeslice - BrightcoveABController.cushion_time) 
        
        #schedule A
        #taskmgr.add_task()
        #schedule B
        #schedule End of Timeslice for a particular video

    def end_of_timeslice(self):
        pass
        #indicate which video it was

###########################################
# Create Tornado server application
###########################################

from tornado.options import define, options
define("port", default=8888, help="run on the given port", type=int)
define("service_url", default="http://services.neon-lab.com", help="service url", type=basestring)

class GetData(tornado.web.RequestHandler):
    
    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self,*args,**kwargs):
        
        #input data = { video_id => [(tid,%)] }
        print "[getdata] ", threading.current_thread()
        controller = BrightcoveABController()
        
        #For the videoid, get its metadata and insert in to Q
        data = tornado.escape.json_decode(self.request.body)
        for vid,tids_tuple in vids.iteritems():
            tids = [tup[0] for tup in tids_tuple] 
            tidmappings = yield tornado.gen.Task(ThumbnailIDMapper.get_ids,tids)
            if tidmappings is not None:
                vmdata = yield tornado.gen.Task(VideoMetadata.get,vid)
                if vmdata is Not None:
                    controller.thumbnail_swap_scheduler(vmdata,tidmappings,tids_tuple)
            
        priority = time.time() 
        taskQ.add_task(task,priority) 
        self.finish()

application = tornado.web.Application([
    (r"/",GetData),
])

###########################################
# Initialize AB Controller  
###########################################

'''
Populate data from mastermind 

Fetch the video id => [(Tid,%)] mappings and populate the data
'''
def initialize_controller():
    vids = {"int_vid1": [('i1',20)('i2',80)],"int_vid2": [('ii1',30)('ii2',70)] }
    controller = BrightcoveABController()
    return

    for vid,tids_tuple in vids.iteritems():
        tids = [tup[0] for tup in tids_tuple] 
        tidmappings = ThumbnailIDMapper.get_ids(tids)
        vmdata = VideoMetadata.get(vid)
        controller.thumbnail_swap_scheduler(vmdata,tidmappings,tids_tuple)

###########################################
# MAIN
###########################################

def main():
    global taskQ
    taskQ = PriorityQ()
    global video_map
    video_map = {}
    taskmgr = TaskManager()
    #initialize_controller()
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.PeriodicCallback(taskmgr.check_scheduler,1000).start()
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
    main()
