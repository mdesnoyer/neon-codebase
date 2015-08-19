#!/usr/bin/env python
'''
Neon video server

Keeps the thumbnail api requests made in to the Neon system via CMS API
The video clients consume the requests from this Q
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.exception
from boto.s3.connection import S3Connection
from cmsdb import neondata
from collections import deque
import datetime
import hashlib
import json
import logging
import multiprocessing
import os
import Queue
import random
import re
import redis
import time
import tornado.httpserver
import tornado.gen
import tornado.ioloop
import tornado.web
import tornado.escape
import threading
import urlparse
import urllib
import utils.botoutils
import utils.http
import utils.neon
import utils.ps
from utils import statemon

#Tornado options
from utils.options import define, options
define("port", default=8081, help="run on the given port", type=int)
define("test_key", default="3qswu22oabmnl8d8hqcuku14", help="test api key",
        type=str)
define("max_retries", default=3,
       help="Maximum number of video processing retries")
define("priority_check", default=600, type=str)

_log = logging.getLogger(__name__)
DIRNAME = os.path.dirname(__file__)

# Monitoring variables
statemon.define('server_queue', int)
statemon.define('duplicate_requests', int)
statemon.define('add_video_error', int)
statemon.define('dequeue_requests', int)
statemon.define('queue_size_bytes', int) #size of the queue in bytes of video
statemon.define('default_thumb_error', int)
#invalid url while retrieving headers
statemon.define('get_content_length_error', int) 
statemon.define('job_timedout', int)
statemon.define('requeue_exception', int)
statemon.define('too_many_job_retries', int)
statemon.define('bad_request', int)
statemon.define('neon_requests', int)
statemon.define('brightcove_requests', int)
statemon.define('ooyala_requests', int)

# Constants
TOP_THUMBNAILS = "topn"
CALLBACK_URL = "callback_url"
VIDEO_ID = "video_id"
VIDEO_DOWNLOAD_URL = "video_url"
VIDEO_TITLE = "video_title"
BCOVE_READ_TOKEN = "read_token"
BCOVE_WRITE_TOKEN = "write_token"
API_KEY = "api_key"
JOB_SUBMIT_TIME = "submit_time"
MAX_THUMBNAILS = 25
NEON_AUTH = "secret_key"
PUBLISHER_ID = "publisher_id"
INTEGRATION_ID = "integration_id"

class JobException(Exception): pass
class JobFailed(JobException): pass


class DBCache(object):
    customer_priorities = {} 
    last_priority_check = time.time()

    @classmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_customer_priority(cls, api_key):
        priority = 1 # by default return priority=1
        try:
            # invalidate the cache (a very naive implementation) 
            if cls.last_priority_check + options.priority_check < time.time():
                cls.last_priority_check = time.time()
                cls.customer_priorities = {}

            priority = cls.customer_priorities[api_key]
        except KeyError, e:
            nu = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                    api_key)
            if nu:
                priority = nu.processing_priority
                cls.customer_priorities[api_key] = priority
            else:
                _log.error("Failed to fetch NeonUserAccount %s" % api_key)

        raise tornado.gen.Return(priority)

class RequestData(object):
    '''
    Instance of this classs is stored in the Q
    '''

    def __init__(self, key, api_request, duration=None):
        '''
        @api_request: NeonApiRequest Object
        '''
        self.key = key
        self.api_request = api_request
        if duration is not None:
            # approximate the video size
            self.video_size = duration * 8.0 * 1024 * 800
        else:
            self.video_size = None # in bytes
        self.duration = duration # in seconds

    def get_key(self):
        return self.key
    
    def get_video_url(self):
        return self.api_request.video_url

    def get_video_size(self):
        return self.video_size

    def set_video_size(self, val):
        self.video_size = val

class SimpleThreadSafeDictQ(object):
    '''
    Threadsafe Q implementation using a double ended queue and
    a dictionary to look up the object in the queue directly
    '''
    
    def __init__(self, QItemType=RequestData):
        self.QItemType = QItemType
        self.qdict = {}
        self.q = deque()
        self._lock = threading.RLock()

    def _get_lock(self):
        return self._lock

    def is_empty(self):
        return self.size() == 0

    def size(self):
        return len(self.q)

    def put(self, key, item):
        '''
        @item: Instance of what ever the QItemType is defined as 
        
        Returns : KEY that can be used to access the Q element directly
                : None if the Queue already has the request element   
        '''
        if not isinstance(item, self.QItemType):
            raise Exception("Expects an obj of QItemType, check init method")

        # Don't insert the element in the Q if its already present 
        try:
            self.qdict[key]
            return False
        except KeyError, e:
            self.q.append((key,item))
            self.qdict[key] = item
            return True 

    def peek(self, key):
        '''
        '''
        with self._lock:
            try:
                return self.qdict[key]
            except KeyError, e:
                return None

    def get(self):
        with self._lock:
            try:
                key, item = self.q.popleft()    
                self.qdict.pop(key)
                return item

            except KeyError, e:
                return
            
            except IndexError, e:
                return

class FairWeightedRequestQueue(object):
    '''
    FairWeighted requeust Q

    '''
    def __init__(self, nqueues=3, weights='pow2'):
        # Queues that hold objects of type RequestData
        self.pqs = [SimpleThreadSafeDictQ(RequestData) for x in range(nqueues)]
        self.max_priority = 0.0
        self.cumulative_priorities = []
        if weights == 'pow2':
            for i in range(nqueues):
                self.max_priority += 1.0/2**(i) 
                self.cumulative_priorities.append(self.max_priority)
        else:
            raise Exception("unsupported weight scheme")
    
    def _get_priority_qindex(self):
        p = random.uniform(0, self.max_priority)
        for index in range(len(self.pqs)):
            if p < self.cumulative_priorities[index]:
                return index

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def put(self, api_request, duration=None):
        # Based on customer priority put in the appropriate Q
        p = yield DBCache.get_customer_priority(api_request.api_key, async=True)
        
        # if priority > # of queues, then consider all priorities > len(Qs) as
        # the lowest priority
        pindex = min(p, len(self.pqs) -1)
        key = api_request.key
        item = RequestData(key, api_request, duration=duration)
        ret = self.pqs[pindex].put(key, item)

        # TODO(Sunil): Remove the complexity of looking up the
        # metadata in the queue.
        try:
            yield self._add_metadata(pindex, key, async=True)
        except tornado.web.HTTPError as e:
            pass

        raise tornado.gen.Return(ret)

    def get(self):
        '''Returns a RequestData object from the queue or None if it is empty.'''
        # pick a random number in the interval (0, max_priority)
        pindex = self._get_priority_qindex()
        item = self.pqs[pindex].get()
        if item:
            return item
        else:
            # check other Qs
            for i in range(len(self.pqs)):
                if i != pindex:
                    item = self.pqs[i].get()
                    if item:
                        return item
        # Empty Q
        return None

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _add_metadata(self, pqid, key):
        '''
        Add metadata to the Q item
        '''
        pq = self.pqs[pqid]
        item = pq.peek(key)
        # if the item still exists in the Q
        if item and item.duration is None:
            video_url = item.get_video_url()

            # Get content length of the video

            try:
                nbytes = yield self._get_content_length(video_url)
                if nbytes is not None:
                    statemon.state.increment('queue_size_bytes', nbytes)
                    _log.info("Request %s had video file of size %s",
                              item.key, nbytes)
                    # add video size 
                    item.set_video_size(nbytes)
            except Exception as e:
                # on error do nothing for now
                _log.exception('An exception getting the video length: %s' % e)

    @tornado.gen.coroutine
    def _get_content_length(self, video_url):
        s3re = re.compile('((s3://)|(https?://[a-zA-Z0-9\-_]+\.amazonaws\.com/))([a-zA-Z0-9\-_\.]+)/(.+)')

        s3match = s3re.search(video_url)
        if s3match:
            # Get the video size from s3
            try:
                bucket_name = s3match.group(4)
                key_name = s3match.group(5)
                s3conn = S3Connection()
                bucket = yield utils.botoutils.run_async(s3conn.get_bucket,
                                                         bucket_name)
                key = yield utils.botoutils.run_async(bucket.get_key,
                                                      key_name)
                raise tornado.gen.Return(key.size)
            except boto.exception.S3ResponseError as e:
                _log.warn('Error getting video url %s via boto. '
                          'Falling back on http: %s' % (video_url, e))
                

        url_parse = urlparse.urlparse(video_url)
        url_parse = list(url_parse)
        url_parse[2] = urllib.quote(url_parse[2])
        req = tornado.httpclient.HTTPRequest(
            method='HEAD',
            url=urlparse.urlunparse(url_parse),
            request_timeout=5.0) 
            
        result = yield tornado.gen.Task(utils.http.send_request, req)

        if not result.error:
            headers = result.headers
            raise tornado.gen.Return(int(headers.get('Content-Length', 0)))
        else:
            statemon.state.increment('get_content_length_error')
        raise tornado.gen.Return(None)

    def qsize(self):
        sz = 0
        for i in range(len(self.pqs)):
            sz += self.pqs[i].size()
        return sz

    def _handle_default_thumbnail(self):
        pass

def _verify_neon_auth(value):
    # TODO: Implement the authentication token logic
    # Probably not required since all dequeue requests are from within the VPC
    return True


class JobManager(object):
    '''Class that manages jobs to run and/or are currently running.'''
    def __init__(self, job_check_interval=10.0, base_time=30.0):
        '''Create the job manager.

        Inputs:
        job_check_interval - Interval in seconds to check the running job status
        base_time - Base time in seconds for events to be requeued
        '''
        self.q = FairWeightedRequestQueue()
        self.base_time = base_time
        self.io_loop = tornado.ioloop.IOLoop.current()
        self.job_check_timer = tornado.ioloop.PeriodicCallback(
            self.check_running_jobs, job_check_interval * 1000)
        self.job_check_timer.start()

        # List of ((job_id, api_key), timeout_date) tuples for jobs that may
        # be running.
        self._lock = threading.RLock()
        self.running_jobs = [] 

    def get_qsize(self):
        return self.q.qsize()

    def is_healthy(self):
        '''Returns true if the job management is healthy.'''
        #TODO(Sunil): Write this function so that it actually checks
        #the state or take the check out of the health checker
        return True

    def get_job(self):
        '''Return a RequestData job, or None if there isn't one.'''
        job = self.q.get()
        statemon.state.server_queue = self.q.qsize()
        if job:
            # Set a timeout to be 2x the video length assuming an SD
            # video of 800 kbps or 15 min if we don't know
            # the length of the video.
            approx_video_length = self.base_time*15.0
            if job.duration:
                approx_video_length = job.duration
            elif job.video_size:
                approx_video_length = job.video_size * 8.0 / 1024 / 800
            deadline = datetime.datetime.now() + datetime.timedelta(
                seconds=approx_video_length*2.0)
            with self._lock:
                self.running_jobs.append(
                    ((job.api_request.job_id, job.api_request.api_key),
                     deadline))
        return job

    @tornado.gen.coroutine
    def check_running_jobs(self):
        '''Checks the running jobs to see if they should be requeued.

        If they failed or have timed out, requeue them.
        '''
        _log.debug('Checking on %i running jobs' % len(self.running_jobs))
        with self._lock:
            jobs = self.running_jobs
            self.running_jobs = []

        # Now walk through all the jobs and deal with them as necessary
        requests = yield tornado.gen.Task(
            neondata.NeonApiRequest.get_many,
            [x[0] for x in jobs])
        for job, request in zip(jobs, requests):
            job_id, api_key = job[0]
            deadline = job[1]

            delay = None

            if request.state in [neondata.RequestState.FINISHED,
                                 neondata.RequestState.SERVING,
                                 neondata.RequestState.ACTIVE]:
                # The job finished sucessfully
                continue
            elif request.state in [neondata.RequestState.FAILED,
                                   neondata.RequestState.INT_ERROR,
                                   neondata.RequestState.CUSTOMER_ERROR]:
                # Requeue the job with a large exponential backoff
                delay = self.base_time * (1 << (request.fail_count + 2))
                _log.warn('Job %s for account %s failed and will be retried'
                          % (job_id, api_key))

            elif (datetime.datetime.now() > deadline and
                  request.state in [neondata.RequestState.REQUEUED,
                                    neondata.RequestState.REPROCESS,
                                    neondata.RequestState.PROCESSING,
                                    neondata.RequestState.SUBMIT,
                                    neondata.RequestState.FINALIZING]):
                _log.error('Job %s from account %s timed out' %
                           (job_id, api_key))
                statemon.state.increment('job_timedout')
                
                # Requeue with a small exponential backoff and flag
                # the error in the database.
                delay = self.base_time *  (1 << request.fail_count)
                def _inc_fail_count(db_request):
                    db_request.fail_count += 1
                    db_request.state = neondata.RequestState.INT_ERROR
                request = yield tornado.gen.Task(
                    neondata.NeonApiRequest.modify, job_id, api_key,
                    _inc_fail_count)
            else:
              # We're still watching the job
              with self._lock:
                  self.running_jobs.append(job)
                    
            if delay is not None:
                self.io_loop.add_callback(lambda x: self.io_loop.call_later(
                    x, self._no_exceptions_requeue_job, job_id, api_key),
                    float(delay))

    @tornado.gen.coroutine
    def _no_exceptions_requeue_job(self, job_id, api_key):
        try:
            yield self.requeue_job(job_id, api_key)
        except JobFailed:
            pass
        except Exception as e:
            _log.exception('Unexpected exception when requeueing job %s' % e)
            statemon.state.increment('requeue_exception')

    @tornado.gen.coroutine
    def requeue_job(self, job_id, api_key):
        '''Requeue a job.

        Returns False if the job is already in the queue
        '''

        def _flag_requeue(request):
            if request.state in [neondata.RequestState.FINISHED,
                                 neondata.RequestState.SERVING,
                                 neondata.RequestState.ACTIVE,
                                 neondata.RequestState.SERVING_AND_ACTIVE,
                                 neondata.RequestState.REPROCESS]:
                request.state = neondata.RequestState.REPROCESS
                request.fail_count = 0
            elif request.fail_count >= options.max_retries:
                return
            else:
                request.state = neondata.RequestState.REQUEUED

        api_request = yield tornado.gen.Task(
            neondata.NeonApiRequest.modify,
            job_id,
            api_key,
            _flag_requeue)

        if api_request.fail_count >= options.max_retries:
            msg = ('Failed processing job %s for account %s too '
                   'many times' % (job_id, api_key))
            _log.error(msg)
            statemon.state.increment('too_many_job_retries')
            raise JobFailed(msg)
        retval = yield self.add_job(api_request)
        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def reprocess_job(self, job_id, api_key):
        '''Reprocess a job whether or not it had failed before.

        Returns False if the job is already in the queue.
        '''
        def _flag_reprocess(request):
            request.state = neondata.RequestState.REPROCESS
            request.fail_count = 0

        api_request = yield tornado.gen.Task(
            neondata.NeonApiRequest.modify,
            job_id,
            api_key,
            _flag_reprocess)

        retval = yield self.add_job(api_request)
        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def add_job(self, api_request):
        '''Add the NeonApiRequest to the queue.
        
        Returns False if the job is already in the queue.
        '''
        # Try to get the duration of the video
        duration = None
        video = yield tornado.gen.Task(
            neondata.VideoMetadata.get,
            neondata.InternalVideoID.generate(api_request.api_key,
                                              api_request.video_id))
        if video is not None:
            duration = video.duration
                    
        retval = yield self.q.put(api_request, duration, async=True)
        statemon.state.server_queue = self.q.qsize()
        _log.info('Add job %s for account %s to the queue' % 
                  (api_request.job_id, api_request.api_key))
        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def requeue_all_pending_jobs(self):
        '''Look into the database and requeue all pending jobs.

        This should only be called at the beginning of the program.
        '''
        _log.info('Requeuing pending jobs from db')
        requests = yield tornado.gen.Task(neondata.NeonApiRequest.get_all)
        for request in requests:
            if (request.fail_count < options.max_retries and
                request.state in [neondata.RequestState.SUBMIT,
                                  neondata.RequestState.PROCESSING,
                                  neondata.RequestState.FINALIZING,
                                  neondata.RequestState.REQUEUED,
                                  neondata.RequestState.REPROCESS,
                                  neondata.RequestState.FAILED,
                                  neondata.RequestState.INT_ERROR,
                                  neondata.RequestState.CUSTOMER_ERROR]):
                yield self.requeue_job(request.job_id, request.api_key)

class StatsHandler(tornado.web.RequestHandler):
    """ Q Stats """ 
    def initialize(self, job_manager):
        super(StatsHandler, self).initialize()
        self.job_manager = job_manager
    
    def get(self, *args, **kwargs):
        qsize = self.job_manager.get_qsize()
        self.write('{"size": %s}' % qsize)
        self.finish()

class DequeueHandler(tornado.web.RequestHandler):
    """ DEQUEUE JOB Handler """
    def initialize(self, job_manager):
        super(DequeueHandler, self).initialize()
        self.job_manager = job_manager
    
    def get(self, *args, **kwargs):
        if self.request.headers.has_key('X-Neon-Auth'):
            if not _verify_neon_auth(self.request.headers.get('X-Neon-Auth')):
                raise tornado.web.HTTPError(400)
        else:
            statemon.state.increment('bad_request')
            raise tornado.web.HTTPError(400)
        
        statemon.state.increment('dequeue_requests')
        job = self.job_manager.get_job()
        if job:
            #send http response
            h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})
            self.write(json.dumps(job.api_request.__dict__))
        else: 
            #Send Queue empty message as a string {}
            self.write("{}")

        self.finish()

class RequeueHandler(tornado.web.RequestHandler):
    """ REQUEUE JOB Handler"""
    def initialize(self, job_manager, reprocess=False):
        '''Initialize the handler.

        Inputs:
        job_manager - JobManager object
        reprocess - If true, requests will be flagged for reprocessing 
                    irrespective of their state and/or number of errors.
        '''
        super(RequeueHandler, self).initialize()
        self.job_manager = job_manager
        self.reprocess = reprocess

    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        
        try:
            jdata = self.request.body
            data = json.loads(jdata)
            try:
                key = data["_data"]["key"]
                api_request = neondata.NeonApiRequest._create(key, data)
            except KeyError, e:
                _log.error("Inavlid format for request json data")
                statemon.state.increment('bad_request')
                self.set_status(400)
                self.finish()
                return
            
            _log.info("key=requeue_handler msg=requeing job %s" % key)

            if self.reprocess:
                ret = yield self.job_manager.reprocess_job(api_request.job_id,
                                                           api_request.api_key)
            else:
                ret = yield self.job_manager.requeue_job(api_request.job_id,
                                                         api_request.api_key)
            if not ret:
                _log.warn("requed request %s is already in the Q" % key)
                self.set_status(409)
                self.write('{"error": "requeust already in the Q"}')

        except JobFailed, e:
            self.set_status(409)
            self.write('{"error": "Request has failed too many times. '
                       'If you want to force it to be tried again, call '
                       '/reprocess"}')
                
        except Exception, e:
            _log.error("key=requeue_handler msg=error %s" %e)
            raise tornado.web.HTTPError(500)

        self.finish()

## ===================== API ===========================================#
# External Handlers
## ===================== API ===========================================#

class GetThumbnailsHandler(tornado.web.RequestHandler):
    ''' Thumbnail API handler '''

    def initialize(self, job_manager):
        super(GetThumbnailsHandler, self).initialize()
        self.job_manager = job_manager

    def send_json_response(self, data, status=200):
       
        if status == 400:
            statemon.state.increment('bad_request')
        self.set_header("Content-Type", "application/json")
        self.set_status(status)
        self.write(data)
        self.finish()

    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        # insert job in to user account
        
        try:
            params = tornado.escape.json_decode(self.request.body)
            uri = self.request.uri
            self.parsed_params = {}
            api_request = None 
            http_callback = params.get(CALLBACK_URL, None)
            default_thumbnail = params.get('default_thumbnail', None)
            external_thumbnail_id = params.get('external_thumbnail_id', None)
            publish_date = params.get('publish_date', None)
            i_id = params.get(INTEGRATION_ID, '0')
            # Verify essential parameters
            try:
                api_key = params[API_KEY]
                vid = params[VIDEO_ID]
                if re.match('%s$' % 
                            neondata.InternalVideoID.VALID_EXTERNAL_REGEX,
                            vid) is None:
                    self.send_json_response(
                        '{"error":"video id contains invalid characters"}', 400)
                    return

                title = params[VIDEO_TITLE]
                url = params[VIDEO_DOWNLOAD_URL]
                if not default_thumbnail:
                    _log.debug("no default image for the video %s" % vid)
                

            except KeyError, e:
                self.send_json_response('{"error":"params not set"}', 400)
                return

            # compare with supported api methods
            if params.has_key(TOP_THUMBNAILS):
                api_method = "topn"
                api_param = min(int(params[TOP_THUMBNAILS]),
                                MAX_THUMBNAILS)
            else:
                self.send_json_response('{"error":"api method not supported"}',
                                        400)
                return
           
            # Generate JOB ID  
            # Use Params that can change to generate UUID, support same
            # video to be processed with diff params
            intermediate = api_key + str(vid) + api_method + str(api_param)
            job_id = hashlib.md5(intermediate).hexdigest()
          
            # Identify Request Type
            # TODO: Remove the special treatment for brightcove and ooyala
            if "brightcove" in self.request.uri:
                pub_id  = params[PUBLISHER_ID] #publisher id
                rtoken = params[BCOVE_READ_TOKEN]
                wtoken = params[BCOVE_WRITE_TOKEN]
                autosync = params["autosync"]
                request_type = "brightcove"
                api_request = neondata.BrightcoveApiRequest(
                    job_id, api_key, vid, title, url,
                    rtoken, wtoken, pub_id, http_callback, i_id,
                    default_thumbnail=default_thumbnail)
                api_request.autosync = autosync
                statemon.state.increment('brightcove_requests')

            elif "ooyala" in self.request.uri:
                request_type = "ooyala"
                oo_api_key = params["oo_api_key"]
                oo_secret_key = params["oo_secret_key"]
                autosync = params["autosync"]
                api_request = neondata.OoyalaApiRequest(
                    job_id,
                    api_key, 
                    i_id,
                    vid,
                    title,
                    url,
                    oo_api_key,
                    oo_secret_key, 
                    http_callback,
                    default_thumbnail=default_thumbnail)
                api_request.autosync = autosync
                statemon.state.increment('ooyala_requests')

            else:
                request_type = "neon"
                api_request = neondata.NeonApiRequest(
                    job_id,
                    api_key,
                    vid,
                    title,
                    url,
                    request_type,
                    http_callback,
                    default_thumbnail,
                    integration_id=i_id,
                    external_thumbnail_id=external_thumbnail_id,
                    publish_date=publish_date)
                statemon.state.increment('neon_requests')
            
            # API Method
            api_request.set_api_method(api_method, api_param)
            api_request.submit_time = str(time.time())
            api_request.state = neondata.RequestState.SUBMIT

            # Validate Request & Insert in to Queue (serialized/json)
            job_result = yield tornado.gen.Task(
                neondata.NeonApiRequest.get,
                api_request.job_id, api_request.api_key)

            if job_result is not None:
                response_data = '{"error":"duplicate job", "job_id": "%s" }'\
                                 % job_result.job_id 
                self.write(response_data)
                self.set_status(409)
                self.finish()
                statemon.state.increment('duplicate_requests')
                return
            
            # Response for the submission of request
            response_data = "{\"job_id\":\"" + job_id + "\"}"
            
            result = yield tornado.gen.Task(api_request.save)

            if not result:
                _log.error("key=thumbnail_handler  msg=request save failed: ")
                self.set_status(502)
            else:
                # Add the job to the queue
                yield self.job_manager.add_job(api_request)

                # NOTE: Only if this is a Neon request, save it to the
                # DB. Other platform requests get added on request
                # creation cron
                # TODO: Change this so that any source will have the video
                # added here.
                if request_type == 'neon':
                    nplatform = yield tornado.gen.Task(
                        neondata.NeonPlatform.get, api_key, '0')
                    if nplatform:
                        def _add_video(np): 
                            np.add_video(vid, job_id)
                        res = yield tornado.gen.Task(neondata.NeonPlatform.modify,
                                api_key, '0', _add_video)
                        if not res:
                            _log.error("key=update_neon_account" 
                                           " msg=video id %s not added to"
                                           " account" % vid)
                            self.send_json_response('{}', 500)
                            statemon.state.increment('add_video_error')
                    else:
                        _log.error("account not found or api key error")
                        self.send_json_response('{}', 400)
                        return

                # Save the video metadata and the default thumbnail
                def _merge_video_data(video_obj):
                    video_obj.job_id = api_request.job_id
                    video_obj.url = url
                    video_obj.integration_id = api_request.integration_id
                    video_obj.serving_enabled = \
                      len(video_obj.thumbnail_ids) > 0
                    video_obj.publish_date = publish_date or \
                      video_obj.publish_date
                internal_video_id = \
                  neondata.InternalVideoID.generate(api_key, vid)
                yield tornado.gen.Task(
                    neondata.VideoMetadata.modify,
                    internal_video_id,
                    _merge_video_data,
                    create_missing=True)
                yield api_request.save_default_thumbnail(async=True)
                def _set_serving_enabled(video_obj):
                    video_obj.serving_enabled = \
                      len(video_obj.thumbnail_ids) > 0
                yield tornado.gen.Task(neondata.VideoMetadata.modify,
                                       internal_video_id,
                                       _set_serving_enabled)
                
                self.set_status(201)
                self.write(response_data)
                self.finish()

        except ValueError, e:
            self.send_json_response('{"error":"%s"}' % e, 400)
            return
        
        except neondata.ThumbDownloadError, e:
            _log.warn("Default thumbnail download failed for vid %s" % vid)
            statemon.state.increment('default_thumb_error')
            # Should the state be updated here too?
            def _update_request_message(req):
                req.set_message("failed to download default thumbnail %s" % e)
            
            result = yield tornado.gen.Task(
                          neondata.NeonApiRequest.modify, api_request.job_id, 
                          api_request.api_key,
                          _update_request_message)
            # Even if the request state is not updated, its ok. since we'll try
            # to handle the upload on the video client again. Hence best effort 
            self.set_status(201)
            self.write(response_data)
            self.finish()
            return
        
        except IOError, e:
            _log.error("IOError for video %s msg=%s" % (vid, e))
            self.send_json_response('{"error":"%s"}' % e, 400)
            return

        except Exception, e:
            _log.exception("key=thumbnail_handler msg= %s" % e)
            self.send_json_response('{"error":"%s"}' % e, 500)
            statemon.state.increment('add_video_error')
            return

###########################################
# TEST Handlers 
###########################################

class TestCallback(tornado.web.RequestHandler):
    """ Test callback Handler to print the api output """
    def post(self, *args, **kwargs):
        
        try:
            _log.info("key=testcallback msg=output: " + self.request.body)
        except Exception, e:
            raise tornado.web.HTTPError(500)  
            _log.error("key=testcallback msg=error recieving message")
        
        self.finish()

class HealthCheckHandler(tornado.web.RequestHandler):
    ''' Health check for the Server'''
    def initialize(self, job_manager):
        super(HealthCheckHandler, self).initialize()
        self.job_manager = job_manager
    
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        test_account_key = options.test_key
        
        # Check the job manager
        if not self.job_manager.is_healthy():
            self.write("Job manager is unhealthy")
            self.set_status(503)
            self.finish()
            return
        
        # Ping the DB to see if its running
        try:
            ret = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                test_account_key)
            if ret:
                self.set_status(200)
                self.finish()
                return
            # if not ret, return 503. yes we can talk to the db but since we
            # couldn't fetch the test accout,  the DB could be in inconsistent
            # state
        except redis.ConnectionError, e:
            self.write("Error connecting to the video database")
        
        self.set_status(503)
        self.finish()

###########################################
# Create Tornado server application
###########################################

class Server(object):
    def __init__(self):
        self.job_manager = JobManager()
        self.application = tornado.web.Application([
            (r'/api/v1/submitvideo/(.*)', GetThumbnailsHandler,
             dict(job_manager=self.job_manager)),
            (r"/stats", StatsHandler,  dict(job_manager=self.job_manager)),
            (r"/dequeue", DequeueHandler, dict(job_manager=self.job_manager)),
            (r"/requeue", RequeueHandler, dict(job_manager=self.job_manager,
                                                 reprocess=False)),
            (r"/reprocess", RequeueHandler, dict(job_manager=self.job_manager,
                                                 reprocess=True)),
            (r"/testcallback", TestCallback),
            (r"/healthcheck", HealthCheckHandler,
             dict(job_manager=self.job_manager))
            ])

    def run(self):
        server = tornado.httpserver.HTTPServer(self.application)
        utils.ps.register_tornado_shutdown(server)
        server.listen(options.port)

        io_loop = tornado.ioloop.IOLoop.current()
        io_loop.add_callback(self.job_manager.requeue_all_pending_jobs)
        io_loop.start()
        

def main():
    utils.neon.InitNeon()
    server = Server()
    server.run()

# ============= MAIN ======================== #
if __name__ == "__main__":
    logging.getLogger('tornado.access').propagate = False
    main()
