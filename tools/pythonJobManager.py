'''
Creates a job allocation class, that allows arbitrary jobs to be submitted
by multiple requesters, processed by multiple consumers, and then allocated
back to the originating requesters. Essentially, this is an MPMC queue.
'''

from multiprocessing import Manager, Process, Queue

class _SubManagerRequester(object):
    def __init__(self, jm):
        self.jm = jm
        self.id = id(self)
        self.putQ = jm.input_queue
        self.getQ = jm._register_requester(self)
        self.working = True
        self.pending = 0

    def put(self, item):
        '''
        Puts an item in the request queue
        '''
        self.putQ.put((self.id, item))
        self.pending += 1

    def get(self):
        '''
        Returns an item from the completed queue
        '''
        if self.pending > 0:
            while True:
                try:
                    rv = self.getQ.get(block=True, timeout=2)
                    self.pending -= 1
                    break
                except:
                    self.jm._sync()
                    #print 'Failure to get [requester]!','working is:',self.working
                    if not self.working:
                        return None
            return rv
        return None

    def done(self):
        '''
        The requester is done producing.
        '''
        print 'Requester %i has requested to finish.'%(self.id)
        self.jm._kill_me(self.id)
        #print 'working is now set to ', self.working

class _SubManagerWorker(object):
    '''
    NOTE: dequeued jobs MUST be put() in the 
    same order they were get()!!!
    '''
    def __init__(self, jm):
        self.jm = jm
        self.id = id(self)
        self.id_list = []
        jm._register_worker(self)
        self.getQ = jm.input_queue
        self.working = True

    def put(self, item):
        if not len(self.id_list):
            raise KeyError('No ID for this job!')
        cid = self.id_list.pop(0)
        self.jm._put(cid, item)

    def get(self):
        while True:
            try:
                # try to get an item
                cid, item = self.getQ.get(block=True, timeout=2)
                if self.working:
                    self.id_list.append(cid)
                    #print 'Worker got',item
                    return item
            except:
                self.jm._sync()
                #print 'Failure to get! [worker]','working is:',self.working
            if not self.working:
                return None

    def done(self):
        '''
        The worker is done working.
        '''
        print 'Worker %i has requested to finish.'%(self.id)
        self.jm._kill_me(self.id)
        #print 'working is now set to ', self.working

class JobManager(object):
    def __init__(self):
        self.manager = Manager()
        self.input_queue = self.manager.Queue()
        self.output_queue = self.manager.Queue()
        self.output = dict() #self.manager.dict()
        self.workers = dict() #self.manager.dict()
        self.requesters = dict() #self.manager.dict()
        self.agents = self.manager.dict()
    
    def _put(self, cid, item):
        self.output[cid].put(item)

    def _register_requester(self, requester):
        self.agents[id(requester)] = True
        if not self.requesters.has_key(id(requester)):
            self.requesters[id(requester)] = requester
            self.output[id(requester)] = self.manager.Queue()
            print 'Registered requester %i'%(id(requester))
        return self.output[id(requester)]

    def _register_worker(self, worker):
        self.agents[id(worker)] = True
        if not self.workers.has_key(id(worker)):
            self.workers[id(worker)] = worker
            print 'Registered worker %i'%(id(worker))

    def _kill_me(self, cid):
        print 'Attempting to kill',cid
        self._sync()
        self._murder(cid)

    def _murder(self, cid):
        '''
        Does the bulk of work for _kill_me, but 
        does not invoke _sync() for efficiency,
        since modifications done by _murder and
        all downstream functions automatically 
        modify this instance.
        '''
        is_first_change = self.agents[cid]
        self.agents[cid] = False
        if self.requesters.has_key(cid):
            if is_first_change:
                print 'Requester %i terminating, %i remain'%(cid, len(self.requesters))
            _ = self.output.pop(cid)
            req = self.requesters.pop(cid)
            req.working = False
            if (not len(self.requesters)) and (len(self.workers)):
                if is_first_change:
                    print 'No more requesters, terminating workers!'
                self._stop_workers()
        elif self.workers.has_key(cid):
            if is_first_change:
                print 'Worker %i terminating, %i remain'%(cid, len(self.workers))
            work = self.workers.pop(cid)
            work.working = False
            #self.input_queue.put((None, None))
            if (not len(self.workers)) and (len(self.requesters)):
                if is_first_change:
                    print 'No more workers, terminating active requesters!'
                self._stop_requesters()
        else:
            print 'Worker or Requester %i either never existed or is already dead.'%(cid)

    def _sync(self):
        '''
        Syncronizes the knowledge across copies of jm
        '''
        for req in self.requesters.keys():
            if not self.agents[req]:
                _ = self._murder(req)
        for work in self.workers.keys():
            if not self.agents[work]:
                _ = self._murder(work)

    def _stop_requesters(self):
        for req in self.requesters.keys():
            self._murder(req)

    def _stop_workers(self):
        for work in self.workers.keys():
            self._murder(work)

    def __call__(self, is_worker=False):
        '''
        Registers an invoking object, and returns a
        an object that behaves like a queue.
        '''
        if not is_worker:
            return _SubManagerRequester(self)
        else:
            return _SubManagerWorker(self)