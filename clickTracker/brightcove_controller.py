import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen
import time
from socket import *
from heapq import *
import random
import itertools

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

################################################################
# CO ROUTINES
################################################################

# The Process logic, to schedule a task
def task_scheduler():
    try:
        while True:
            task = (yield)
            #Compute priority of the task
            priority = time.time() + 2
            taskQ.add_task(task,priority) 
    except GeneratorExit:
        print("Exit Task Scheduler")

# Check scheduler
def check_scheduler():
    
    print "[check scheduler]"
    global texecutor
    priority, count, task = taskQ.peek_task()
    cur_time = time.time()
    if priority and priority <= cur_time:
       task = taskQ.pop_task() 
       texecutor.send(task)
    
# Task Executor
def task_executor():
    try:
        while True:
            task = (yield)
            print "[Task exec] --> ", task, time.time()
    except GeneratorExit:
        print("Exit Task Executor")

### Globals

taskQ = PriorityQ()

tscheduler = task_scheduler()
tscheduler.next()

texecutor = task_executor()
texecutor.next()

###########################################
# Create Tornado server application
###########################################

from tornado.options import define, options
define("port", default=8888, help="run on the given port", type=int)

class GetData(tornado.web.RequestHandler):
    
    @tornado.web.asynchronous
    def post(self,*args,**kwargs):
        recv_data = self.request.body
        tscheduler.send(recv_data)
        self.finish()

application = tornado.web.Application([
    (r"/",GetData),
])

def main():
    tornado.options.parse_command_line()
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.PeriodicCallback(check_scheduler,1000).start()
    tornado.ioloop.IOLoop.instance().start()

# ============= MAIN ======================== #
if __name__ == "__main__":
    main()
    
# Listener for data from mastermind
#address = ('localhost', 6005)
#server_socket = socket(AF_INET, SOCK_DGRAM)
#server_socket.bind(address)

#while True:
#    print "Start Listening"
#    recv_data, addr = server_socket.recvfrom(4096)
#    tscheduler.send(recv_data)
#    check_scheduler(texecutor)
