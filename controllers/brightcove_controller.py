#!/usr/bin/env python
'''
AB Controller for Brightcove

Listens for updates from mastermind

On recieveing an update, schedules a task to be executed at time 't'
Tasks can include -- push thumbnail X in to brightcove account A

'''
import os,os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from api.brightcove_api import BrightcoveApi
import base64
from boto.s3.connection import S3Connection
from cmsdb.neondata import VideoMetadata, ThumbnailMetadata, \
      AbstractPlatform, InternalVideoID, ThumbnailID, BrightcoveIntegration, \
      NeonApiRequest
import datetime
from heapq import heappush, heappop
import itertools
import json
import logging
from multiprocessing.pool import ThreadPool
from oauthlib.oauth2 import BackendApplicationClient
from pprint import pprint
import random
import re
from requests_oauthlib import OAuth2Session
import time
import threading
import tornado
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen
import tornado.httpclient
import urllib
from cvutils.imageutils import PILImageUtils
import utils.neon

from utils.options import define, options
define("delay", default=10, help="initial delay", type=int)
define("service_url", default="http://localhost:8083", 
        help="service url", type=str)
define('directive_address',
       default='s3://neon-image-serving-directives/mastermind', 
       help='Address to get the directive file from')
define("thumbnail_sampling_period", default=304,
       help="Period, in seconds for checking brightcove for new thumbs")

_log = logging.getLogger(__name__)

#Monitoring vars
from utils import statemon
statemon.define('pqsize', int)
statemon.define('thumbchangetask', int)
statemon.define('thumbchangetask_fail', int)
statemon.define('thumbchecktask_fail', int)
statemon.define('loaddirective_fail', int)
# The number of videos being ab tested
statemon.define('nvideos_abtesting', int)
# The number of videos being monitored by this controller
statemon.define('nvideos', int)


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
                "api/v1/brightcovecontroller/%s/updatethumbnail/%s" \
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
        
        # Get the BrightcoveIntegration associated with this video id
        video = VideoMetadata.get(self.video_id)
        if video is None:
            _log.error("key=ThumbnailCheckTask "
                       "msg=Could not find video id: %s" % self.video_id)
            statemon.state.increment('thumbchecktask_fail')
            return
        platform = BrightcoveIntegration.get(video.integration_id)
        if platform is None:
            _log.error("key=ThumbnailCheckTask "
                       "msg=Could not find brightcove platform account for video: %s" 
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

        # Check if a neontn exists, then its a Neon thumb. Else save this 
        # thumbnail as user upload
        # skip this for now
        # TODO: (Sunil) : complete the check thumbnail task 

        return


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
        except Exception, e:
            pass

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
    
    def check_video_exists(self, vid):
        try:
            self.video_map[vid]
            return True
        except KeyError:
            pass
        return False
    
    def check_distribution_change(self, vid, distribution):
        try:
            dist = self.get_thumbnail_distribution(vid)
            tids1 = [d[0] for d in dist]
            tids2 = [d[0] for d in distribution]
            return set(tids1) != set(tids2)
        except KeyError:
            pass

        return True

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

    random.seed(25110)
    
    # a thumbnail shall not be scheduled to last less than 10 minutes
    # in the overall video rotation period 
    MIN_THUMBNAIL_DURATION = 10 * 60 

    def __init__(self, timeslice=4260, cushion_time=600):
        self.neon_service_url = options.service_url
        
        self.timeslice = timeslice 
        self.cushion_time = cushion_time

        self.taskmgr = TaskManager()
        self.monitored_videos = set()
        
        # Check for new directives every minute
        tornado.ioloop.PeriodicCallback(
            self.load_directives,
            60000).start()

        self.directives = {} # Map of video_id -> directive
        self.directive_etag = None

        self.account_settings = {} # api_key => AbstractPlatform obj
        
        self._initialized = False

    def load_initial_state(self):
        _log.info('Initializing the BrightCove controller')
        
        self.load_accounts()
        self.load_directives(options.delay)
        
    def load_accounts(self):
        '''
        Load account level settings for each platform account
        (abtesting enabled, testing type etc..)
        '''
        for platform in AbstractPlatform.get_all():
            self.account_settings[platform.neon_api_key] = platform 

    def load_directives(self, max_update_delay=0):
        s3regex = re.compile('s3://([0-9a-zA-Z_\-]+)/(.+)')
        match = s3regex.match(options.directive_address)
        if not match:
            _log.error('Invalid directive address: %s' %
                       options.directive_address)
            statemon.state.increment('loaddirective_fail')
            return 
        
        # Get the directives file from S3
        s3conn = S3Connection()
        try:
            bucket = s3conn.get_bucket(match.group(1))
            key = bucket.get_key(match.group(2))
            if key is None:
                raise boto.exception.StorageResponseError(
                    404, 'Key %s does not exist' % match.group(2))
        except Exception as e:
            _log.error('Error getting directive file %s from S3: %s' % (
                options.directive_address, e))
            statemon.state.increment('loaddirective_fail')
            return

        if key.etag == self.directive_etag:
            # The file hasn't changed so we are done
            return
        self.directive_etag = key.etag

        _log.info('New directive file found. Processing.')
        
        lines = key.get_contents_as_string().split('\n')
        for line in lines[1:]:
            if line == "end":
                continue

            record = json.loads(line)
            if record['type'] == 'dir':
                # Create the directive from the record
                directive = (
                    record['vid'],
                    [(x['tid'], x['pct']) for x in record['fractions']])

                # There is a new directive so apply it
                self.apply_directive(directive, max_update_delay)
                statemon.state.decrement('nvideos_abtesting')
        
    def _check_testing_with_bcontroller(self, api_key):
        '''
        Check account is enabled for AB Testing and 
        the controller type is bc controller
        '''
        try:
            accnt = self.account_settings[api_key]
            if accnt.abtest and accnt.serving_controller == "bc_controller":
                return True

        except KeyError, e:
            _log.error("Account not found %s" % api_key)
            return False

        return False 

    def apply_directive(self, directive, max_update_delay=0):
        '''Apply a directive to a controller.
        
        Allow current directive cycle to continue even if %s change on it.
        Apply new directive only if the thumbnail ids being tested with change

        Inputs:
        directive - The directive as (video_id, [(thumb_id, fraction)])
        max_update_delay - Maximum delay to apply the directive
        '''

        video_id, distribution = directive
        #_log.info('New directive for video %s: %s' % (video_id,
        #                                              distribution))

        internal_account_id = video_id.split('_')[0]
        
        # NOTE: (In the spirit of limited release, only test for PPG now)
        if internal_account_id not in ["6d3d519b15600c372a1f6735711d956e"]:
            return
    
        # Check if the account is enabled for ABTesting and controller type
        # is BC controller
        if self._check_testing_with_bcontroller(internal_account_id):
            
            # Check that video state is "active" or "serving_active" 
            # if yes, then we should A/B Test 
            request = VideoMetadata.get_video_request(video_id)
            if request.state in ["active", "serving_active"]:
                self.monitored_videos.add(video_id)
                self.directives[video_id] = directive
                if self.taskmgr.check_video_exists(video_id):
                    # only schedule if distribution has changed 
                    if self.taskmgr.check_distribution_change(video_id, 
                                distribution):
                        self.thumbnail_change_scheduler(video_id, 
                                            distribution,
                                            max_update_delay)
                else:
                    self.thumbnail_change_scheduler(video_id, 
                                            distribution,
                                            max_update_delay)
            
                # Add lastest distribution to the map
                self.taskmgr.add_video_info(video_id, distribution)


    def start(self):
        '''Starts running the brightcove controller on the current thread.

        Blocks until the controller is shut down.
        '''
        tornado.ioloop.IOLoop.current().start()

    def thumbnail_change_scheduler(self, video_id, distribution,
                                   max_update_delay=0):
        ''' Change thumbnail scheduler '''

        account_id = video_id.split('_')[0] 

        # The time in seconds each thumbnail to be run
        time_dist = self.convert_from_percentages(distribution)

        # no thumbnails to run
        if time_dist is None:
            return

        # randomize the thumbnails order
        random.shuffle(time_dist)
        cur_time = time.time()

        # create a task for each thumbnail
        prev_offset = random.randint(0, 30) # stagger videos
        for t in time_dist:
            # thumbnail tuple is in format (thumb id, time slice)  
            task = ThumbnailChangeTask(account_id, video_id, t[0])
            # schedule it in the future, now + time slice
            tslice = cur_time + prev_offset
            prev_offset = t[1]
            self.taskmgr.add_task(task, int(tslice))
            _log.info("Added ThumbnailChangeTask for vid %s tid %s time %s"
                        % (video_id, t[0], tslice))
        
        # TODO (Sunil) : Add a CheckThumbnail task, skipping this for now
        
        # Schedule end of timeslice for a particular video
        task_time_slice = TimesliceEndTask(video_id, self) 
        time_to_exec_task = cur_time + sum([tup[1] for tup in time_dist])
        self.taskmgr.add_task(task_time_slice, time_to_exec_task)
        _log.info("Added TimesliceEndTask for vid %s time %s"
                        % (video_id, time_to_exec_task))

    @classmethod
    def enforce_minimum_time_slice(cls, ts):
        ''' 
        Returns an adjusted tslice 
        '''
        
        return max(ts, BrightcoveABController.MIN_THUMBNAIL_DURATION)

    def convert_from_percentages(self, pd):
        ''' Convert from fraction(%) to time 

        Thumbnails with zero or negative values percentege are skipped.
        If thumbnails total percentage do not add to 1.0, they will be scaled
        accordingly so the minimum rotation loop is fully filled proprotionally.


        Returns: [(thumb_id, # of seconds to run)] sorted by the number of seconds
        '''
        
        #  the result, a list of thumbnails with their associated calculated time slice 
        time_dist = []

        # add all thumbs percentage, should normally be a total of 1.0
        # but it is possible that it might less or more. Note that humbnails with
        # zero pct are in effect disregarded.   
        total_pcnt = 0.0

        for tup in pd:
            pct = float(tup[1])

            if pct >= 0.0:
                total_pcnt += pct
            else:
                _log.info('Dismissing fraction with negative percentage value: %s' %(pct))

        # no active thumbnails in this case,  return
        if total_pcnt <= 0.0:
            return None

        # if total_pcnt is not be exactly 1.0 we will use a scale factor
        scale_factor = 1.0
        if total_pcnt != 1.0:
            scale_factor = 1.0 / total_pcnt
            _log.info('Scaling used since total of all percentages isnt exactly 1.0:  %s' %(total_pcnt))

        # add each active thumbnail to the result sequence
        for tup in pd:

            pcnt = float(tup[1]) * scale_factor
           
            # thumbnails with zero percent are disregarded
            if pcnt == 0.0:
                continue

            # how long should this thumbnail have in the rotation 
            tslice = pcnt * self.timeslice

            # time slice must be at least minimum length
            tslice = BrightcoveABController.enforce_minimum_time_slice(tslice)

            # add thumbnail to result
            pair = (tup[0], tslice)
            time_dist.append(pair)
     
        return time_dist

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

