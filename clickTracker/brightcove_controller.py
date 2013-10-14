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

#Threads
import threading
class TaskExecutor(threading.Thread):
    def __init__(self,task_queue):
        threading.Thread.__init__(self)
        self.taskQ = task_queue
        self.daemon = True
        self.start()
    
    def run(self):
        while True:
            time.sleep(0.5)
            priority, count, task = self.taskQ.peek_task()
            cur_time = time.time()
            if priority and priority <= cur_time:
                task = self.taskQ.pop_task() 
                self.executor(task)

    def executor(self,task):
        print "[exec] " ,task, threading.current_thread()  
        bc = BrightcoveABController()
        bc.set_thumbnail(1,2)

    def my_cb(self):
        print "my_cb " , threading.current_thread() 

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
        #internal_aid = tidmappings[0].account_id 
        #internal_vid = tidmappings[0].internal_video_id
        #video_data = VideoMetadata.get_metadata(internal_aid,internal_vid) 
        vmdata = VideoMetadata.get(vid)
        controller.thumbnail_swap_scheduler(vmdata,thumb_probabilities)
        #run the ab controller logic to schedule tasks

###########################################
# Brightcove AB Controller Logic 
###########################################
class BrightcoveABController(object):
    
    def __init__(self):
        #self.neon_service_url = "http://services.neon-lab.com"
        self.neon_service_url = "http://localhost:8083"

    def cb(self,result):
        print "[cb]" ,time.time(), len(result.body), threading.current_thread()

    def set_thumbnail(self,video_id,tid):
        http_client = tornado.httpclient.AsyncHTTPClient()
        #url = "http://54.221.234.42:9082"
        url = "http://google.com"
        print "[Make call] ", time.time(), threading.current_thread()
        http_client.fetch(url,self.cb)

    def thumbnail_swap_scheduler(self,video_metadata,thumb_control_data):
        #Make a decision based on the current state of the video data
        pass

###########################################
# Create Tornado server application
###########################################

from tornado.options import define, options
define("port", default=8888, help="run on the given port", type=int)
define("service_url", default="http://services.neon-lab.com", help="service url", type=basestring)

def do_work(): print "DO WORK"
class GetData(tornado.web.RequestHandler):
    
    @tornado.web.asynchronous
    def post(self,*args,**kwargs):
        
        #input data = { video_id => [(tid,%)] }
        print "[getdata] ", threading.current_thread()
        task = self.request.body
        priority = time.time() + 2
        taskQ.add_task(task,priority) 
        #tornado.ioloop.IOLoop.instance().add_callback(tornado.ioloop.IOLoop.instance().add_timeout,(time.time()+5, do_work))
        self.finish()

application = tornado.web.Application([
    (r"/",GetData),
])

def main():
    global taskQ
    taskQ = PriorityQ()
    global video_map
    video_map = {}
    #TaskExecutor(taskQ)
    #initialize_controller()
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    #tornado.ioloop.PeriodicCallback(taskmgr.check_scheduler,1000).start()
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
    main()
