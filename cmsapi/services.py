#!/usr/bin/env python
'''
This script launches the services server which hosts Services 
that neon web account uses.
- Neon Account managment
- Submit video processing request via Neon API, Brightcove, Youtube
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
import boto.exception
import boto.sqs
#from boto.s3.connection import S3Connection
import datetime
import json
import hashlib
import PIL.Image as Image
import logging
import os
import random
import signal
import time
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
import traceback
import utils.neon
import utils.logs
import utils.http
import video_processor.video_processing_queue

from StringIO import StringIO
from cmsdb import neondata
from utils.inputsanitizer import InputSanitizer
from utils import statemon
import utils.sync
from utils.options import define, options

define("thumbnailBucket", default="host-thumbnails", type=str,
        help="S3 bucket to Host thumbnails ")
define("port", default=8083, help="run on the given port", type=int)
define("local", default=0, help="call local service", type=int)
define("video_server", default="50.19.216.114", help="thumbnails.neon api", type=str)
define("max_videoid_size", default=128, help="max vid size", type=int)
# max tid size = vid_size + 40(md5 hexdigest)
define("max_tid_size", default=168, help="max tid size", type=int)
define('video_queue_region', default='us-east-1', help='region of the SQS queue to connect to')

import logging
_log = logging.getLogger(__name__)

def sig_handler(sig, frame):
    ''' signal handler'''
    _log.debug('Caught signal: %s'%sig)
    tornado.ioloop.IOLoop.instance().stop()

def CachePrimer():
    '''
    #TODO: On Bootstrap and periodic intervals, 
    #Load important blobs that don't change with TTL From storage in to cache
    '''
    pass

################################################################################
# Monitoring variables
################################################################################
statemon.define('total_requests', int) #all requests
_total_requests_ref = statemon.state.get_ref('total_requests')

# HHTP 500s totals and fine-grained issues counters
statemon.define('bad_gateway', int) # all HTTP 502s
_bad_gateway_ref = statemon.state.get_ref('bad_gateway')
statemon.define('internal_err', int) # all HTTP 500s
_internal_err_ref = statemon.state.get_ref('internal_err')
statemon.define('unexpected_exception', int)
statemon.define('custom_thumbnail_not_added', int)
statemon.define('brightcove_api_failure', int)
statemon.define('account_not_created', int)
statemon.define('account_not_updated', int)
statemon.define('ooyala_api_failure', int)
statemon.define('thumb_metadata_not_saved', int)
statemon.define('thumb_metadata_not_modified', int)
statemon.define('db_error', int)
statemon.define('thumb_updated', int)
statemon.define('custom_thumb_upload', int)
statemon.define('abtest_state_update', int)

# HTTP 400s total and fine-grained issues counters
statemon.define('bad_request', int) #all HTTP 400s
_bad_request_ref = statemon.state.get_ref('bad_request')
statemon.define('invalid_api_key', int)
statemon.define('invalid_method', int)
statemon.define('invalid_state_request', int)
statemon.define('invalid_thumbnail_id', int)
statemon.define('invalid_job_id', int)
statemon.define('invalid_video_id', int)
statemon.define('account_id_missing', int)
statemon.define('account_not_found', int)
statemon.define('job_not_found', int)
statemon.define('video_not_found', int)
_video_not_found_ref = statemon.state.get_ref('video_not_found')
statemon.define('api_params_missing', int)
statemon.define('invalid_video_link', int)
statemon.define('deprecated', int)
statemon.define('invalid_image_link', int)
statemon.define('video_id_missing', int)
statemon.define('job_creation_fail', int)
statemon.define('content_type_missing', int)
statemon.define('integration_id_missing', int)
statemon.define('thumbnail_args_missing', int)
statemon.define('invalid_json', int)
statemon.define('malformed_request', int)
statemon.define('not_supported', int)
statemon.define("failed_to_write_message", int)

#Place holder images for processing
placeholder_images = [
                'http://cdn.neon-lab.com/webaccount/neon_processing_1.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_2.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_3.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_4.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_5.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_6.png',
                'http://cdn.neon-lab.com/webaccount/neon_processing_7.png',
                ]
################################################################################
# Helper classes  
################################################################################

class GetVideoStatusResponse(object):
    ''' VideoStatus response on *_integration calls '''
    def __init__(self, items, count, page_no=0, page_size=100,
            processing_count=0, recommended_count=0, published_count=0,
            serving_count=0):
        self.items = items
        self.total_count = count 
        self.page_no = page_no
        self.page_size = page_size
        self.processing_count = processing_count
        self.recommended_count = recommended_count
        self.published_count = published_count
        self.serving_count = serving_count

    def to_json(self):
        ''' to json''' 
        for item in self.items:
            if item:
                for thumb in item['thumbnails']:
                    score = thumb['model_score']
                    if score == float('-inf') or score == '-inf' or score is None:
                        thumb['model_score'] =  -1 * sys.maxint

        return json.dumps(self, default=lambda o: o.__dict__)

################################################################################
# Account Handler
################################################################################

class CMSAPIHandler(tornado.web.RequestHandler):
    ''' /api/v1/accounts handler '''
    
    def prepare(self):
        ''' Called before every request is processed '''
       
        # If POST or PUT, then decode the json arguments
        if not self.request.method == "GET":
            ctype = self.request.headers.get("Content-Type")
            if ctype is None:
                data = '{"error": "missing content type header use json or\
                urlencoded"}'
                statemon.state.increment('content_type_missing')
                self.send_json_response(data, 400)
                return

            ## Convert any json input in to argument dictionary
            if self.request.body and "application/json" in ctype:
                try:
                    json_data = json.loads(self.request.body)
                    for k, v in json_data.items():
                        # Tornado expects values in the argument dict to be lists.
                        # in tornado.web.RequestHandler._get_argument the last
                        # argument is returned.

                        # The value has to be unicode, so convert it
                        if not isinstance(v, basestring):
                            v = json.dumps(v)
                        json_data[k] = [v]
                    #clear the request body arguments from dict
                    #self.request.arguments.pop(self.request.body)
                    self.request.arguments.update(json_data)
                except ValueError, e:
                    statemon.state.increment('invalid_json')
                    self.send_json_response('{"error": "invalid json request"}', 400)

        self.api_key = self.request.headers.get('X-Neon-API-Key') 
        if self.api_key == None:
            if self.request.uri.split('/')[-1] == "accounts" \
                    and self.request.method == 'POST':
                #only account creation call can lack this header
                return
            else:
                _log.exception("key=initialize msg=api header missing")
                data = '{"error": "missing or invalid api key" }'
                statemon.state.increment('invalid_api_key')
                self.send_json_response(data, 400)
                return

    @tornado.gen.coroutine
    def async_sleep(self, secs):
        ''' async sleep'''
        yield tornado.gen.Task(tornado.ioloop.IOLoop.current().add_timeout, 
                time.time() + secs)

    @tornado.gen.coroutine
    def delayed_callback(self, secs, callback):
        ''' delay a callback by x secs'''
        yield tornado.gen.Task(tornado.ioloop.IOLoop.current().add_timeout, 
                time.time() + secs)
        callback(secs)

    #### Support Functions #####

    @tornado.gen.coroutine
    def verify_account(self, a_id):
        ''' verify account '''

        api_key = yield tornado.gen.Task(neondata.NeonApiKey.get_api_key, a_id)
        if api_key == self.api_key:
            raise tornado.gen.Return(True)
        else:
            data = '{"error":"invalid api_key or account id"}'
            _log.warning(("key=verify_account "
                          "msg=api key doesn't match for account %s") % a_id)
            statemon.state.increment('invalid_api_key')
            self.send_json_response(data, 400)
            raise tornado.gen.Return(False)
        
    ######## HTTP Methods #########

    def send_json_response(self, data, status=200):
        '''Send response to service client '''
       
        statemon.state.increment(ref=_total_requests_ref, safe=False)
        if status == 400 or status == 409:
            statemon.state.increment(ref=_bad_request_ref, safe=False)
            _log.warn("Bad Request %r" % self.request)
        elif status == 502:
            statemon.state.increment(ref=_bad_gateway_ref, safe=False)
            _log.warn("Gateway Error. Request %r" % self.request)
        elif status >= 500:
            statemon.state.increment(ref=_internal_err_ref, safe=False)
            _log.warn("Internal Error. Request %r" % self.request)

        self.set_header("Content-Type", "application/json")
        self.set_status(status)
        self.write(data)
        self.finish()
    
    def method_not_supported(self):
        ''' unsupported method response'''
        data = '{"error":"api method not supported or REST URI is incorrect"}'
        _log.warn('Received invalid method %s', self.request.uri)
        statemon.state.increment('invalid_method')
        self.send_json_response(data, 400)

    @tornado.gen.coroutine
    def get_platform_account(self, i_type, i_id):
        #Get account/integration
        
        platform_account = None

        # Loose comparison
        if "brightcove" in i_type:
            platform_account = yield tornado.gen.Task(
                neondata.BrightcovePlatform.get,
                self.api_key, i_id)
        elif "ooyala" in i_type:
            platform_account = yield tornado.gen.Task(
                neondata.OoyalaPlatform.get,
                self.api_key, i_id)
        elif "neon" in i_type: 
            platform_account = yield tornado.gen.Task(
                neondata.NeonPlatform.get,
                self.api_key, '0')

        raise tornado.gen.Return(platform_account)


    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        ''' 
        GET /accounts/:account_id/status
        GET /accounts/:account_id/[brightcove_integrations|youtube_integrations] \
                /:integration_id/videos
        '''
        
        uri_parts = self.request.uri.split('/')

        try:
            #NOTE: compare string in parts[-1] since get args aren't cleaned up
            if "accounts" in self.request.uri:
                #Get account id
                try:
                    a_id = uri_parts[4]
                    itype = uri_parts[5]
                    i_id = uri_parts[6]
                    method = ''
                    if len(uri_parts) >= 8:
                        method = uri_parts[7]
                except Exception, e:
                    _log.error("key=get request msg=  %s" %e)
                    self.send_json_response(
                        '{"error":"malformed request, check API doc"}', 400)
                    statemon.state.increment('malformed_request')
                    return
                
                #Verify Account
                is_verified = yield self.verify_account(a_id)
                if not is_verified:
                    return

                if method == '':
                    yield self.get_account_info(itype, i_id)
                    return

                elif method == "status":
                    #self.get_account_status(itype,i_id)
                    self.send_json_response('{"error":"not yet impl"}', 200)
                    return

                elif method == "tracker_account_id":
                    yield self.get_tracker_account_id()

                elif method == "abteststate":
                    video_id = uri_parts[-1].split('?')[0]
                    yield self.get_abtest_state(video_id)

                elif method == "videos" or "videos" in method:
                    video_state = None
                    #NOTE: Video ids here are external video ids
                    ids = self.get_argument('video_ids', None)
                    video_ids = None if ids is None else ids.split(',')
                   
                    #NOTE: Clean up the parsing 
                    if len(uri_parts) == 9:
                        video_state = uri_parts[-1].split('?')[0]
                        if video_state not in ["processing", "recommended",
                                "published", "failed", "serving"]:
                                
                                # Check if there was a "/" 
                                if len(video_state) < 2: 
                                    vide_state = None
                                else:
                                    # Check if param after videos is a videoId 
                                    video_ids = [uri_parts[-1]]
                                    video_state = None

                    if itype  == "neon_integrations":
                        yield self.get_video_status("neon", i_id, video_ids,
                                                    video_state)
                
                    elif itype  == "brightcove_integrations":
                        yield self.get_video_status("brightcove", i_id,
                                                    video_ids, video_state)
                    
                    elif itype == "youtube_integrations":
                        statemon.state.increment('not_supported')
                        self.send_json_response(
                            '{"error": "not supported yet"}', 400)
                       
                elif method == "videoids":
                    yield self.get_all_video_ids(itype, i_id)
                
                else:
                    _log.warning(('key=account_handler '
                                  'msg=Invalid method in request %s method %s') 
                                  % (self.request.uri, method))
                    statemon.state.increment('invalid_method')
                    self.send_json_response(
                            '{"error": "api not supported yet"}', 400)

            elif "jobs" in self.request.uri:
                try:
                    job_id = uri_parts[4].split("?")[0]
                    yield self.get_job_status(job_id)
                    return
                except:
                    statemon.state.increment('invalid_job_id')
                    self.send_json_response(
                            '{"error": "invalid api call"}', 400)
                    return

            else:
                _log.warning(('key=account_handler '
                              'msg=Account missing in request %s')
                              % self.request.uri)
                statemon.state.increment('account_id_missing')
                self.send_json_response(
                            '{"error": "api not supported yet"}', 400)
        
        except Exception, e:
            # Catch all block to send a generic message on internal failure
            # and friendly logging in to the error log 
            _log.exception("Internal Error: %s" % e)
            self.send_json_response('{"error":"Neon CMS API internal failure"}',
                    500)
            return


    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        ''' Post methods '''

        uri_parts = self.request.uri.split('/')
        a_id = None
        method = None
        itype = None
        i_id = None
        try:
            a_id = uri_parts[4]
            itype = uri_parts[5]
            i_id = uri_parts[6]
            method = uri_parts[7]
        except Exception, e:
            pass
     
        try: 
            #POST /accounts ##Crete neon user account
            if a_id is None and itype is None:
                #len(ur_parts) == 4
                try:
                    a_id = self.get_argument("account_id") 
                    yield self.create_account_and_neon_integration(a_id)
                except:
                    data = '{"error":"account id not specified"}'
                    statemon.state.increment('account_id_missing')
                    self.send_json_response(data, 400)                
                return

            #Account creation
            if method is None:
                #POST /accounts/:account_id/brightcove_integrations
                if "brightcove_integrations" in self.request.uri:
                    yield self.create_brightcove_integration()
                

            #Video Request creation   
            elif method == 'create_video_request':
                if i_id is None:
                    data = '{"error":"integration id not specified"}'
                    statemon.state.increment('integration_id_missing')
                    self.send_json_response(data, 400)
                    return

                if "brightcove_integrations" == itype:
                    yield self.create_neon_thumbnail_api_request(i_id)
                elif "neon_integrations" == itype:
                    yield self.create_neon_video_request_from_ui(i_id)
                else:
                    self.method_not_supported()

            # Create thumbnail API
            elif method == "create_thumbnail_api_request":
                if i_id is None:
                    data = '{"error":"integration id not specified"}'
                    statemon.state.increment('integration_id_missing')
                    self.send_json_response(data, 400)
                    return
                yield self.create_neon_thumbnail_api_request(i_id)
            elif method == "reprocess_video_request":
                #TODO(Sunil): Implement this endpoint
                self.method_not_supported()
            else:
                self.method_not_supported()
            
        except Exception, e:

            # Catch all block to send a generic message on internal failure
            # and friendly logging in to the error log 
            _log.exception("Internal Error: %s" % e)
            self.send_json_response('{"error":"Neon CMS API internal failure"}',
                    500)


    @tornado.gen.coroutine
    def put(self, *args, **kwargs):
        '''
        /accounts/:account_id/[brightcove_integrations|youtube_integrations] \
                /:integration_id/{method}
        '''
       
        uri_parts = self.request.uri.split('/')
        method = None
        itype  = None
        try:
            a_id = uri_parts[4]
            itype = uri_parts[5]
            i_id = uri_parts[6]
            method = uri_parts[7]
        except Exception, e:
            pass

        try:

            #Update Accounts
            if method is None or method == "update":
                if "brightcove_integrations" == itype:
                    yield self.update_brightcove_integration(i_id)
                elif itype is None:
                    #Update basic neon account
                    self.method_not_supported()
                else:
                    self.method_not_supported()

            #Update the thumbnail property
            elif method == "thumbnails":
                tid = uri_parts[-1].split('?')[0]
                yield self.update_thumbnail_property(tid)
            
            #Update the thumbnail
            elif method == "videos":
                if len(uri_parts) == 9:
                    vid = uri_parts[-1]
                    if vid == "null":
                        _log.warn('vid is null')
                        statemon.state.increment('invalid_video_id')
                        self.send_json_response('{"error": "video id null" }',
                                                400)
                        return

                    i_vid = neondata.InternalVideoID.generate(self.api_key,
                                                              vid)
                    try:
                        # Get video property to be updated
                        # (change current_thumbnail, upload custom thumb,
                        # abtest)
                        new_tid = self.get_argument('current_thumbnail', None)
                        abtest = self.get_argument('abtest', None)
                        
                        if abtest is not None:
                            state = InputSanitizer.to_bool(abtest)
                            yield self.update_video_abtest_state(i_vid, state)
                            return

                        elif new_tid is None and abtest is None:
                            # custom thumbnail upload
                            thumbs = json.loads(self.get_argument('thumbnails'))
                            thumb_urls = [x['urls'][0] for x in thumbs 
                                          if x['type'] == 'custom_upload'] 
                            yield self.upload_video_custom_thumbnails(
                                i_id, i_vid,
                                thumb_urls)
                            return
                    except IOError, e:
                        data = '{"error": "internal error adding custom thumb"}'
                        statemon.state.increment('custom_thumbnail_not_added')
                        self.send_json_response(data, 500)
                        return
                    except tornado.web.MissingArgumentError, e:
                        data = '{"error": "missing thumbnail_id or thumbnails argument"}'
                        _log.warn('Missing argument %s' % e) 
                        statemon.state.increment('thumbnail_args_missing')
                        self.send_json_response(data, 400)
                        return
                    except Exception, e:
                        _log.exception('Unexpected exception: %s' % e)
                        data = '{"error": "internal error"}'
                        statemon.state.increment('unexpected_exception')
                        self.send_json_response(data, 500)
                        return
                
                    self.method_not_supported()
                    return
                    
                else:
                    self.method_not_supported()
                    return
            else:
                _log.error("Method not supported")
                statemon.state.increment('not_supported')
                self.set_status(400)
                self.finish()
        except tornado.web.MissingArgumentError, e:
            raise
        
        except Exception, e:

            # Catch all block to send a generic message on internal failure
            # and friendly logging in to the error log 
            _log.exception("Internal Error: %s" % e)
            self.send_json_response('{"error":"Neon CMS API internal failure"}',
                    500)
    
    ############## User defined methods ###########

    @tornado.gen.coroutine
    def get_account_info(self, i_type, i_id):
        platform_account = yield tornado.gen.Task(self.get_platform_account, 
                            i_type, i_id)
       
        if platform_account:
            # TODO: Filter output in the future.
            self.send_json_response(platform_account.to_json(), 200)
        else:
            data = '{"error":"account not found"}'
            statemon.state.increment('account_not_found')
            self.send_json_response(data, 400)

    @tornado.gen.coroutine
    def get_tracker_account_id(self):
        '''
        Return tracker account id associated with the neon user account
        '''
        nu = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                                    self.api_key)
        if nu:
            data = ('{"tracker_account_id":"%s","staging_tracker_account_id":"%s"}'
                    %(nu.tracker_account_id, nu.staging_tracker_account_id))
            self.send_json_response(data, 200)
        else:
            data = '{"error":"account not found"}'
            statemon.state.increment('account_not_found')
            self.send_json_response(data, 400)

    def get_neon_videos(self):
        ''' Get Videos which were called from the Neon API '''
        self.send_json_response('{"msg":"not yet implemented"}', 200)

    @tornado.gen.coroutine
    def get_job_status(self, job_id):
        '''
        Return the status of the job
        '''
        
        req = yield tornado.gen.Task(neondata.NeonApiRequest.get,
                                     job_id, self.api_key)
        if req:
            
            self.send_json_response(json.dumps(req,
                                    default=lambda o: o.__dict__))
            return
        statemon.state.increment('job_not_found')
        self.send_json_response('{"error":"job not found"}', 400)


    ## Submit a video request to Neon Video Server
    @tornado.gen.coroutine
    def submit_neon_video_request(self, api_key, video_id, video_url, 
                                  video_title, topn, callback_url, 
                                  default_thumbnail, integration_id=None,
                                  external_thumbnail_id=None,
                                  publish_date=None):

        '''
        Create the call in to the Video Server
        '''

        request_body = {}
        request_body["topn"] = topn 
        request_body["api_key"] = api_key 
        request_body["video_id"] = video_id 
        request_body["video_title"] = \
                video_url.split('//')[-1] if video_title is None else video_title 
        request_body["video_url"] = video_url
        request_body["default_thumbnail"] = default_thumbnail 
        request_body["external_thumbnail_id"] = external_thumbnail_id
        client_url = 'http://%s:8081/api/v1/submitvideo/topn'\
                        % options.video_server 
        if options.local == 1:
            client_url = 'http://localhost:8081/api/v1/submitvideo/topn'
        request_body["callback_url"] = callback_url 
        request_body["integration_id"] = integration_id or '0'
        request_body["publish_date"] = publish_date
        body = tornado.escape.json_encode(request_body)
        
        sqs_queue = video_processor.video_processing_queue.VideoProcessingQueue()

        yield sqs_queue.connect_to_server(options.video_queue_region)
 
        #TODO: I'm not sure how to get the priority here
        message = yield sqs_queue.write_message(0, body)
        
        if not message:
            _log.error("Failed to write message with video id %s", (video_id))
            data = '{"error adding job to the queue"}'
            self.send_json_response(data, 502)
            statemon.state.increment('failed_to_write_message')
            return

        #Success
        if message:
            _log.info(message)
            self.send_json_response(message, 201)
    
    @tornado.gen.coroutine
    def create_neon_thumbnail_api_request(self, integration_id):
        '''
        Endpoint for API calls to submit a video request
        '''
        video_id = InputSanitizer.sanitize_string(
            self.get_argument('video_id', None))
        if len(video_id) > options.max_videoid_size:
            statemon.state.increment('invalid_video_id')
            self.send_json_response(
                '{"error":"video id greater than 128 chars"}', 400)
            return    

        video_url = self.get_argument('video_url', "")
        video_url = video_url.replace("www.dropbox.com", 
                                "dl.dropboxusercontent.com")
        video_title = InputSanitizer.sanitize_string(
            self.get_argument('video_title', None))
        topn = self.get_argument('topn', 5)
        callback_url = InputSanitizer.sanitize_string(
            self.get_argument('callback_url', None))
        default_thumbnail = self.get_argument('default_thumbnail', None)
        external_thumb_id = None
        if default_thumbnail is not None:
            external_thumb_id = InputSanitizer.sanitize_string(
                self.get_argument('external_thumbnail_id', None))
        try:
            custom_data = InputSanitizer.to_dict(
                self.get_argument('custom_data', {}))
        except ValueError:
            self.send_json_response(
                '{"error":"custom data must be a dictionary"}', 400)
            return
        duration = InputSanitizer.sanitize_float(
            self.get_argument('duration', None))

        publish_date = InputSanitizer.sanitize_date(
            self.get_argument('publish_date', None))
        if publish_date is not None:
            publish_date = publish_date.isoformat()
        
        if video_id is None or video_url == "": 
            _log.error("key=create_neon_thumbnail_api_request "
                    "msg=malformed request or missing arguments")
            statemon.state.increment('malformed_request')
            self.send_json_response('{"error":"missing video_url"}', 400)
            return

        # Create the video metadata object
        internal_video_id = neondata.InternalVideoID.generate(self.api_key,
                                                              video_id)
        video = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                       internal_video_id)
        if video is not None:
            data = {'error' : 'request already processed',
                    'video_id' : video_id,
                    'job_id' : video.job_id}
            self.send_json_response(json.dumps(data), 409)
            return
        video = neondata.VideoMetadata(internal_video_id,
                                       video_url=video_url,
                                       i_id=integration_id,
                                       serving_enabled=False,
                                       custom_data=custom_data,
                                       duration=duration,
                                       publish_date=publish_date)
        yield tornado.gen.Task(video.save)
        
        #Create Neon API Request
        yield self.submit_neon_video_request(
            self.api_key,
            video_id,
            video_url,
            video_title,
            topn,
            callback_url,
            default_thumbnail,
            integration_id,
            external_thumb_id,
            publish_date)

    @tornado.gen.coroutine
    def create_neon_video_request_from_ui(self, i_id):
        ''' neon platform request via the Neon/ Demo account in the UI 
            this call is required for customers that put a demo link in to the
            system and don't have a video id per se
        '''

        title = None
        try:
            video_url = self.get_argument('video_url')
            title = self.get_argument('title')
            #video_url = video_url.split('?')[0]
            video_url = video_url.replace("www.dropbox.com", 
                                "dl.dropboxusercontent.com")
        except:
            _log.error("key=create_neon_video_request_from_ui "
                    "msg=malformed request or missing arguments")
            statemon.state.increment('malformed_request')
            self.send_json_response('{"error":"missing video_url"}', 400)
            return
        
        invalid_url_links = ["youtube.com", "youtu.be"]
        for invalid_url_link in invalid_url_links:
            if invalid_url_link in video_url:
                data = '{"error":"link given is invalid or not a video file"}'
                statemon.state.increment('invalid_video_link')
                self.send_json_response(data, 400)
                return

        #Validate link
        invalid_content_types = ['text/html', 'text/plain', 'application/json',
                    'application/x-www-form-urlencoded', 
                    'text/html; charset=utf-8', 'text/html;charset=utf-8']
        http_client = tornado.httpclient.AsyncHTTPClient()
        headers = tornado.httputil.HTTPHeaders({'User-Agent': 'Mozilla/5.0 \
            (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 \
            Firefox/3.5.7 GTB6 (.NET CLR 3.5.30729)'})
       
        req = tornado.httpclient.HTTPRequest(url=video_url, headers=headers,
                        use_gzip=False, request_timeout=1.5)
        vresponse = yield tornado.gen.Task(http_client.fetch, req)
       
        #If timeout, Ignore for now, may be a valid slow link.  
        if vresponse.code != 599:
            ctype = vresponse.headers.get('Content-Type')
            if vresponse.error or ctype is None or ctype.lower() in invalid_content_types:
                data = '{"error":"link given is invalid or not a video file"}'
                statemon.state.increment('invalid_video_link')
                self.send_json_response(data, 400)
                return

        video_id = hashlib.md5(video_url).hexdigest()
        request_body = {}
        request_body["topn"] = 6 
        request_body["api_key"] = self.api_key 
        request_body["video_id"] = video_id 
        request_body["video_title"] = \
                video_url.split('//')[-1] if title is None else title 
        request_body["video_url"]   = video_url
        request_body["callback_url"] = None 
        client_url = 'http://%s:8081/api/v1/submitvideo/topn'\
                        % options.video_server 
        if options.local == 1:
            client_url = 'http://localhost:8081/api/v1/submitvideo/topn'
        body = tornado.escape.json_encode(request_body)
        hdr = tornado.httputil.HTTPHeaders({"Content-Type": "application/json"})
        req = tornado.httpclient.HTTPRequest(url=client_url,
                                             method="POST",
                                             headers=hdr,
                                             body=body,
                                             request_timeout=30.0,
                                             connect_timeout=10.0)
        
        result = yield tornado.gen.Task(utils.http.send_request, req)
        
        if result.code == 409:
            data = '{"error":"url already processed","video_id":"%s"}'%video_id
            self.send_json_response(data, 409)
            return
        
        if result.error:
            _log.error("key=create_neon_video_request_from_ui "
                    "msg=thumbnail api error %s" %result.error)
            data = '{"error":"neon thumbnail api error"}'
            self.send_json_response(data, 502)
            return

        # NOTE: job id gets inserted into Neon platform account on video server

        job_id = json.loads(result.body)["job_id"] # get job id from response
        t_urls = [] 
        thumbs = []
        im_index = int(hashlib.md5(video_id).hexdigest(), 16) \
                                        % len(placeholder_images)
        placeholder_url = placeholder_images[im_index] 
        t_urls.append(placeholder_url)
        ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tm = neondata.ThumbnailMetadata(0, video_id, t_urls, ctime, 0, 0,
                                        neondata.ThumbnailType.CENTERFRAME,
                                        0, 0)
        thumbs.append(tm.to_dict_for_video_response())
        vr = neondata.VideoResponse(video_id,
                            job_id,
                            neondata.RequestState.PROCESSING,
                            "neon",
                            "0",
                            title,
                            None, #duration
                            time.time() * 1000,
                            0, 
                            thumbs)
        self.send_json_response(vr.to_json(), 200)

    ##### Generic get_video_status #####

    @tornado.gen.coroutine
    def get_video_status(self, i_type, i_id, vids, video_state=None):
        '''
         i_type : Integration type neon/brightcove/ooyala
         i_id   : Integration ID
         vids   : Platform video ids
         video_state: State of the videos to be requested 

         Get platform video to populate in the web account
         Get account details from db, including videos that have been
         processed so far.
         Multiget all video requests, using jobid 
         Check cached videos to reduce the multiget ( lazy load)
         Aggregrate results and format for the client
        '''
        
        #counters 
        c_published = 0
        c_processing = 0
        c_recommended = 0
        c_failed = 0
        c_serving = 0

        #videos by state
        p_videos = []
        r_videos = []
        a_videos = []
        serving_videos = []
        f_videos = [] #failed videos 
        
        # flag that indicates if an empty video [] should be returned
        insert_non_existent_videos = False
        if vids is not None:
            insert_non_existent_videos = True 

        page_no = 0 
        page_size = 300
        try:
            page_no = int(self.get_argument('page_no'))
            page_size = min(int(self.get_argument('page_size')), 300)
        except:
            pass

        result = {}
        incomplete_states = [
            neondata.RequestState.SUBMIT, neondata.RequestState.PROCESSING,
            neondata.RequestState.REQUEUED, neondata.RequestState.REPROCESS]
        
        failed_states = [neondata.RequestState.INT_ERROR, 
                         neondata.RequestState.FAILED]
        
        # single video state, if video not found return an error
        # GET /api/v1/accounts/{account_id}/
        # {integration_type}/{integration_id}/videos/{video_id}
        if vids is not None and len(vids) == 1:
            i_vid = neondata.InternalVideoID.generate(self.api_key, vids[0])
            v = yield tornado.gen.Task(neondata.VideoMetadata.get, i_vid,
                                       log_missing=False)
            if not v:
                # Video is not in the system yet.
                statemon.state.increment(ref=_video_not_found_ref, safe=False)
                self.send_json_response('{"total_count": 1, "items":[{}],\
                        "error":"video not found"}', 400)
                return

        #1 Get job ids for the videos from account, get the request status
        platform_account = yield tornado.gen.Task(self.get_platform_account, 
                            i_type, i_id)

        if not platform_account:
            _log.error("key=get_video_status_%s msg=account not found" % i_type)
            statemon.state.increment('account_not_found')
            self.send_json_response('{"error":"%s account not found"}' % i_type, 400)
            return

        #return all videos in the account
        if vids is None:
            vids = platform_account.get_videos()
        
        # No videos in the account
        if not vids:
            vstatus_response = GetVideoStatusResponse(
                        [], 0, page_no, page_size,
                        c_processing, c_recommended, c_published, c_serving)
            data = vstatus_response.to_json() 
            self.send_json_response(data, 200)
            return
       
        total_count = len(vids)
        job_request_keys = [] 
        for vid in vids:
            try:
                job_id = platform_account.videos[vid]
                if job_id is not None:
                    job_request_keys.append((platform_account.videos[vid],
                                             self.api_key))
            except KeyError, e:
                #job id not found
                job_request_keys.append(("dummy","dummy"))
 
        #2 Get Job status
        # jobs that have completed, used to reduce # of keys to fetch 
        completed_videos = [] 
        # Get all requests and populate video response object in advance
        requests = yield tornado.gen.Task(neondata.NeonApiRequest.get_many,
                                          job_request_keys,
                                          log_missing=False) 
        ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for request, vid in zip(requests, vids):
            if not request:
                result[vid] = None #indicate job not found
                continue

            thumbs = None
            status = neondata.RequestState.PROCESSING 
            if request.state in incomplete_states:
                t_urls = []
                thumbs = []
                t_type = i_type
                if i_type == "neon":
                    im_index = int(hashlib.md5(vid).hexdigest(), 16) \
                                        % len(placeholder_images)
                    placeholder_url = placeholder_images[im_index] 
                    t_urls.append(placeholder_url)
                else:
                    # Newer API request objects don't have prev thumb
                    # hence add a fallback
                    try:
                        t_urls.append(request.previous_thumbnail)
                    except AttributeError, e:
                        t_urls.append(request.default_thumbnail)

                tm = neondata.ThumbnailMetadata(
                    0, #Create TID 0 as a temp id for previous thumbnail
                    neondata.InternalVideoID.generate(self.api_key, vid),
                    t_urls, ctime, 0, 0, t_type, 0, 0)
                thumbs.append(tm.to_dict_for_video_response())
                p_videos.append(vid)
            elif request.state in failed_states:
                status = "failed" 
                thumbs = None
                f_videos.append(vid)
            else:
                # Jobs have finished
                # append to completed_videos 
                # for backward compatibility with all videos api call 
                completed_videos.append(vid)
                status = "finished"
                thumbs = None
                if request.state == neondata.RequestState.FINISHED:
                    r_videos.append(vid) #finshed processing
                elif request.state in [neondata.RequestState.ACTIVE, 
                        neondata.RequestState.SERVING_AND_ACTIVE]:
                    a_videos.append(vid) #published /active
                elif request.state == neondata.RequestState.SERVING:
                    serving_videos.append(vid)
                    status = neondata.RequestState.SERVING

            pub_date = request.__dict__.get('publish_date', None)
            if pub_date is not None:
                try:
                    pub_date = pub_date / 1000.0
                except ValueError:
                    pub_date = dateutil.parser.parse(pub_date)
                except Exception:
                    pub_date = None
            pub_date = int(pub_date) if pub_date else None #type
            vr = neondata.VideoResponse(vid,
                              request.job_id,
                              status,
                              request.request_type,
                              i_id,
                              request.video_title,
                              None, #duration
                              pub_date,
                              0, #current tid,add fake tid
                              thumbs)

            result[vid] = vr
         
        #2b Filter videos based on state as requested
        if video_state:
            if video_state == "published": #active
                vids = completed_videos = a_videos
            elif video_state == "recommended":
                vids = completed_videos = r_videos
            elif video_state == "processing":
                vids = p_videos
                completed_videos = []
            elif video_state == "serving":
                vids = completed_videos = serving_videos
            elif video_state == "failed":
                vids = f_videos
                completed_videos = []
            else:
                _log.error("key=get_video_status_%s" 
                        " msg=invalid state requested" %i_type)
                statemon.state.increment('invalid_state_request')
                self.send_json_response('{"error":"invalid state request"}', 400)
                return

        # backward compatibilty for recommended videos, hence add 
        # all the videos to the recommended list
        if video_state == "recommended":
            completed_videos.extend(serving_videos)

        #3. Populate Completed videos
        keys = [neondata.InternalVideoID.generate(
            self.api_key, vid) for vid in completed_videos] #get internal vids

        if len(keys) > 0:
            video_results = yield [
                tornado.gen.Task(neondata.VideoMetadata.get_many, keys,
                                 log_missing=False),
                tornado.gen.Task(neondata.VideoStatus.get_many, keys,
                                 log_missing=False)]
            tids = []
            for vresult, vstatus in zip(*video_results):
                if vresult:
                    # extend list of tids
                    tids.extend(vresult.thumbnail_ids)
            
                    # Update A/B Test state for the videos
                    vid = neondata.InternalVideoID.to_external(vresult.key)
                    result[vid].abtest = vresult.testing_enabled

                    # Add a Serving URL 
                    result[vid].serving_url = vresult.serving_url

                    # Populate the state of the video
                    if vstatus is not None:
                        result[vid].winner_thumbnail = vstatus.winner_tid
                
            # Get all the thumbnail data for videos that are done
            thumbnail_results = yield [
                tornado.gen.Task(neondata.ThumbnailMetadata.get_many, tids),
                tornado.gen.Task(neondata.ThumbnailStatus.get_many, tids)]
            for thumb, thumb_status in zip(*thumbnail_results):
                # if thumbnail and thumb is not centerframe or random
                if thumb and thumb.type not in [neondata.ThumbnailType.CENTERFRAME, 
                        neondata.ThumbnailType.RANDOM]:
                    vid = neondata.InternalVideoID.to_external(thumb.video_id)
                    if not result.has_key(vid):
                        _log.debug("key=get_video_status_%s"
                                " msg=video deleted %s"%(i_type, vid))
                    else:
                        tdata = thumb.to_dict_for_video_response()
                        tdata.update(
                            ((k, v) for k,v in 
                             thumb_status.__dict__.iteritems()
                             if k != 'key'))
                        result[vid].thumbnails.append(tdata)
            
        #4. Set the default thumbnail for each of the video
        tid_key = "thumbnail_id"
        for res in result:
            vres = result[res]
            if vres:
                platform_thumb_id = None
                for thumb in vres.thumbnails:
                    if thumb['chosen'] == True:
                        vres.current_thumbnail = thumb[tid_key]
                        if "neon" in thumb['type']:
                            vres.status = "active"

                    if thumb['type'] == i_type:
                        platform_thumb_id = thumb[tid_key]

                if vres.status == "finished" and vres.current_thumbnail == 0:
                    vres.current_thumbnail = platform_thumb_id

        #6. Set ab test state
        #convert to dict and count total counts for each state
        vresult = []
        for res in result:
            vres = result[res]
            if vres and vres.video_id in vids: #filter videos by state 
                vresult.append(vres.to_dict())
            else:
                if insert_non_existent_videos:
                    vresult.append({})
        c_processing = len(p_videos)
        c_recommended = len(r_videos)
        c_published = len(a_videos)
        c_failed = len(f_videos)
        c_serving = len(serving_videos)

        s_vresult = vresult
        # no sorting required since csv was requested
        if not insert_non_existent_videos:
            if i_type == "brightcove":
                #Sort brightcove videos by video_id, since publish_date 
                #is not currently set on ingest of videos
                s_vresult = sorted(
                    vresult, 
                    key=lambda k: k.get('submit_time', k['video_id']),
                    reverse=True)
            else:
                s_vresult = sorted(vresult, 
                                   key=lambda k: k['publish_date'],
                                   reverse=True)
           
        #2c Pagination, case: There are more vids than page_size
        if len(s_vresult) > page_size:
            s_index = page_no * page_size
            e_index = (page_no +1) * page_size
            s_vresult = s_vresult[s_index:e_index]

        vstatus_response = GetVideoStatusResponse(
                        s_vresult, total_count, page_no, page_size,
                        c_processing, c_recommended, c_published, c_serving)
        data = vstatus_response.to_json() 
        self.send_json_response(data, 200)

    @tornado.gen.coroutine
    def create_account_and_neon_integration(self, a_id):
        '''
        Create Neon user account and add neon integration
        '''
        user = neondata.NeonUserAccount(a_id)
        api_key = user.neon_api_key
        nuser_data = yield tornado.gen.Task(
            neondata.NeonUserAccount.get, a_id)
        if not nuser_data:
            def _create_neon_platform(x):
                x.account_id = a_id
            nplatform = yield tornado.gen.Task(
                neondata.NeonPlatform.modify, api_key, '0',
                _create_neon_platform, create_missing=True)
            
            user.add_platform(nplatform)
            res = yield tornado.gen.Task(user.save)
            if res:
                tai_mapper = neondata.TrackerAccountIDMapper(
                    user.tracker_account_id, api_key,
                    neondata.TrackerAccountIDMapper.PRODUCTION)
                tai_staging_mapper = neondata.TrackerAccountIDMapper(
                    user.staging_tracker_account_id, api_key,
                    neondata.TrackerAccountIDMapper.STAGING)
                staging_resp = yield tornado.gen.Task(tai_staging_mapper.save)
                resp = yield tornado.gen.Task(tai_mapper.save)
                if not (staging_resp and resp):
                    _log.error("key=create_neon_user "
                               " msg=failed to save tai %s" %
                               user.tracker_account_id)

                # Set the default experimental strategy
                strategy = neondata.ExperimentStrategy(api_key)
                res = yield tornado.gen.Task(strategy.save)
                if not res:
                    _log.error('Bad database response when adding '
                               'the default strategy for Neon account %s'
                               % api_key)
                    
                data = ('{ "neon_api_key": "%s", "tracker_account_id":"%s",'
                            '"staging_tracker_account_id": "%s" }'
                            %(user.neon_api_key, user.tracker_account_id,
                            user.staging_tracker_account_id)) 
                self.send_json_response(data, 200)
            else:
                data = '{"error": "account not created"}'
                statemon.state.increment('account_not_created')
                self.send_json_response(data, 500)

        else:
            data = '{"error": "integration/ account already exists"}'
            self.send_json_response(data, 409)

    @tornado.gen.coroutine
    def create_brightcove_integration(self):
        ''' Create Brightcove Account for the Neon user
        Add the integration in to the neon user account
        Extract params from post request --> create acccount in DB 
        --> verify tokens in brightcove -->
        send top 5 videos requests or appropriate error to client
        '''

        try:
            a_id = self.request.uri.split('/')[-2]
            i_id = InputSanitizer.to_string(self.get_argument("integration_id"))
            p_id = InputSanitizer.to_string(self.get_argument("publisher_id"))
            rtoken = InputSanitizer.to_string(self.get_argument("read_token"))
            wtoken = InputSanitizer.to_string(self.get_argument("write_token"))
            autosync = InputSanitizer.to_bool(self.get_argument("auto_update"))

        except Exception,e:
            _log.error("key=create brightcove account msg= %s" %e)
            data = '{"error": "API Params missing"}'
            statemon.state.increment('api_params_missing')
            self.send_json_response(data, 400)
            return 

        na = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                                    self.api_key)
        #Create and Add Platform Integration
        if na:
            
            #Check if integration exists
            if len(na.integrations) >0 and na.integrations.has_key(i_id):
                data = '{"error": "integration already exists"}'
                self.send_json_response(data, 409)
            else:
                def _initialize_bc_plat(x):
                    x.account_id = a_id
                    x.publisher_id = p_id
                    x.read_token = rtoken
                    x.write_token = wtoken
                    x.auto_update = autosync
                    x.last_process_date = time.time()
                    
                bc = yield tornado.gen.Task(
                    neondata.BrightcovePlatform.modify,
                    self.api_key, i_id,
                    _initialize_bc_plat,
                    create_missing=True)

                na = yield tornado.gen.Task(
                    neondata.NeonUserAccount.modify,
                    self.api_key,
                    lambda x: x.add_platform(bc))
                
                #Saved platform
                if na:
                    # Verify that the token works by making a call to
                    # Brightcove
                    bc_api = bc.get_api()
                    try:
                        bc_response = yield bc_api.search_videos(page_size=10,
                                                                 async=True)
                    except api.brightcove_api.BrightcoveApiError as e:
                        _log.error("Error accessing the Brightcove api. "
                                   "There is probably something wrong with "
                                   "the token for account %s, integration %s"
                                   % (self.api_key, bc.integration_id))
                        data = ('{"error": "Read token given is incorrect'  
                                ' or brightcove api failed"}')
                        self.send_json_response(data, 502)
                        return
                    
                    self.send_json_response('{}', 200)
                else:
                    data = '{"error": "platform was not added,\
                                account creation issue"}'
                    statemon.state.increment('account_not_created')
                    self.send_json_response(data, 500)
                    return
        else:
            _log.error("key=create brightcove account " 
                        "msg= account not found %s" %self.api_key)

    @tornado.gen.coroutine
    def update_brightcove_integration(self, i_id):
        ''' Update Brightcove account details '''
        try:
            rtoken = InputSanitizer.to_string(self.get_argument("read_token"))
            wtoken = InputSanitizer.to_string(self.get_argument("write_token"))
            autosync = InputSanitizer.to_bool(self.get_argument("auto_update"))
        except Exception,e:
            _log.error("key=create brightcove account msg= %s" %e)
            data = '{"error": "API Params missing"}'
            statemon.state.increment('api_params_missing')
            self.send_json_response(data, 400)
            return

        uri_parts = self.request.uri.split('/')

        def _update_fields(x):
            if x.auto_update == False and autosync == True:
                statemon.state.increment('deprecated')
                self.send_json_response(
                    '{"error": "autopublish feature has been deprecated"}',
                    400)
                return
            x.auto_update = autosync
            x.read_token = rtoken
            x.write_token = wtoken
        bc = yield tornado.gen.Task(neondata.BrightcovePlatform.modify,
                                    self.api_key, i_id, _update_fields)
        if not bc:
            _log.error("key=update_brightcove_integration " 
                    "msg=no such account %s integration id %s"\
                    % (self.api_key, i_id))
            data = '{"error": "account doesnt exists"}'
            statemon.state.increment('account_not_found')
            self.send_json_response(data, 400)

    # Get all the video ids
    @tornado.gen.coroutine
    def get_all_video_ids(self, itype, i_id):
        '''
        Get all the video ids from an account
        '''
        platform_account = yield tornado.gen.Task(self.get_platform_account, 
                            itype, i_id)
        if not platform_account:
            _log.error("%s account not found" % itype)
            statemon.state.increment('account_not_found')
            self.send_json_response('{"error":"%s account not found"}' % itype,
                    400)
            return

        vids = platform_account.get_videos()
        if not vids:
            vids = []
        data = json.dumps({ "videoids" : vids})
        self.send_json_response(data, 200)

    @tornado.gen.coroutine
    def upload_video_custom_thumbnails(self, i_id, i_vid, thumb_urls):
        '''
        Add custom thumbnails to the video

        Inputs:
        @i_id: Integration id
        @i_vid: Internal video id
        @thumb_urls: List of image urls that will be ingested
        '''

        p_vid = neondata.InternalVideoID.to_external(i_vid)
        
        # Get the video object
        vmdata = yield tornado.gen.Task(neondata.VideoMetadata.get, i_vid)
        if not vmdata:
            _log.error("Could not find video: %s" % i_vid)
            statemon.state.increment(ref=_video_not_found_ref, safe=False)
            self.send_json_response("Video not found: %s" % p_vid, 400)

        # Figure out the rank of the custom thumbs we know about so far
        existing_thumbs = yield tornado.gen.Task(
            neondata.ThumbnailMetadata.get_many,
            vmdata.thumbnail_ids)
        min_rank = 1
        for thumb in existing_thumbs:
            if (thumb.type == neondata.ThumbnailType.CUSTOMUPLOAD and 
                thumb.rank < min_rank):
                min_rank = thumb.rank
        cur_rank = min_rank - 1

        # Get the CDN metadata
        cdn_metadata = yield tornado.gen.Task(
            neondata.CDNHostingMetadataList.get,
            neondata.CDNHostingMetadataList.create_key(vmdata.get_account_id(),
                                                       i_id))

        # Upload the thumbnails to the hosting services
        thumb_futures = []
        new_thumbs = []
        for url in thumb_urls:
            new_thumb = neondata.ThumbnailMetadata(
                None,
                internal_vid = i_vid,
                ttype=neondata.ThumbnailType.CUSTOMUPLOAD,
                rank=cur_rank)
            new_thumbs.append(new_thumb)
            
            thumb_futures.append(vmdata.download_and_add_thumbnail(
                new_thumb,
                url,
                cdn_metadata=cdn_metadata,
                async=True))
                                                                
            cur_rank -= cur_rank

        try:
            yield thumb_futures
        except Exception as e:
            data = '{"error": "Invalid image link or failed to download image"}'
            _log.exception('Error downloading the image %s: %s' %
                           (thumb_urls, e))
            statemon.state.increment('invalid_image_link')
            self.send_json_response(data, 400)
            return
        new_tids = [x.key for x in new_thumbs]

        # Now save the information in the database
        # TODO(sunil): Do this as a transaction
        result = yield tornado.gen.Task(neondata.ThumbnailMetadata.save_all,
                                        new_thumbs)
        if result:
            vm_save = yield tornado.gen.Task(
                neondata.VideoMetadata.modify,
                i_vid,
                lambda x: x.thumbnail_ids.extend(new_tids))
            if vm_save:
                _log.info("custom thumbnails added to video=%s tids=%s"\
                          %(i_vid, new_tids))
                data = ''
                self.send_json_response(data, 202)
                statemon.state.increment('custom_thumb_upload')
                return
            else:
                _log.error('Error modifying the video metadata for vid %s' %
                           i_vid)
                data = '{"error": "internal error"}'
                statemon.state.increment('thumb_metadata_not_modified')
                self.send_json_response(data, 500)
                return
        else:
            _log.error('Error saving new thumbnail metadata to vid %s' %
                       i_vid)
            data = '{"error": "internal error"}'
            statemon.state.increment('thumb_metadata_not_saved')
            self.send_json_response(data, 500)
            return

    @tornado.gen.coroutine
    def update_video_abtest_state(self, i_vid, state):
        '''
        For a given video update the ABTest state
        '''

        vmdata = yield tornado.gen.Task(neondata.VideoMetadata.get, i_vid)
        if not vmdata:
            statemon.state.increment(ref=_video_not_found_ref, safe=False)
            self.send_json_response('{"error": "vid not found"}', 400)
            return
        
        if not isinstance(state, bool):
            statemon.state.increment('malformed_request')
            self.send_json_response(
                '{"error": "invalid data type or not boolean"}', 400)
            return

        def _update_video(vm):
            vm.testing_enabled = state
        result = yield tornado.gen.Task(neondata.VideoMetadata.modify, i_vid,
                _update_video)
        
        if not result:
            statemon.state.increment('db_error')
            self.send_json_response('{"error": "internal db error"}', 500)
            return

        statemon.state.increment('abtest_state_update')
        self.send_json_response('', 202)

    ### AB Test State #####
    
    @tornado.gen.coroutine
    def get_abtest_state(self, vid):

        '''
        Return the A/B Test state of the video
        Possible status values are:
        (running, complete, disabled, override, unkown) 

        if state == complete, return all the serving URLs
       
        json response:
        {  
            "state": "running",  
            "data" : []
        }
        
        '''

        i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
        video_status = yield tornado.gen.Task(neondata.VideoStatus.get, i_vid)
        if not video_status:
            statemon.state.increment(ref=_video_not_found_ref, safe=False)
            self.send_json_response('{"error": "vid not found"}', 400)
            return

        state = video_status.experiment_state
        
        response = {}
        response['state'] = state
        response['data'] = []

        # If complete, then send all the URLs for a given tid
        if state == neondata.ExperimentState.COMPLETE:
            rdata = []
            # Find the winner tid

            # If serving fraction = 1.0 its the winner
            # If override == true, then pick highest
            # else filter all > exp_frac ; then max frac

            winner_tid = video_status.winner_tid
            if winner_tid:
                s_urls = yield tornado.gen.Task(
                    neondata.ThumbnailServingURLs.get,
                    winner_tid)
                for size_tup, url in s_urls:
                    #Add urls to data section
                    s_url = {}
                    s_url['url'] = url 
                    s_url['width'] = size_tup[0]
                    s_url['height'] = size_tup[1]
                    rdata.append(s_url)
                response['data'] = rdata

                video_meta = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                                    i_vid)
                if not video_meta:
                    statemon.state.increment(ref=_video_not_found_ref,
                                             safe=False)
                    self.send_json_response('{"error": "vid not found"}', 400)
                    return
                
                # Get original sized thumbnail or max resolution
                try:
                    o_url = s_urls.get_serving_url(video_meta.frame_size[0],
                        video_meta.frame_size[1])
                except Exception, e:
                    # On any kind of exception
                    # TODO: get nearest to original frame_size
                    # For IGN this is sufficient, enhance this when needed

                    s_tup = max(s_urls, key=lambda item:item[0])[0]
                    o_url = s_urls.get_serving_url(s_tup[0], s_tup[1]) 

                response['original_thumbnail'] = o_url
                
                if o_url is None:
                    _log.error("orignal_thumbnail is None for video id %s" %
                            i_vid)
            else:
                response['state'] = "unknown" #should we define error state? 
                _log.error("winner tid not found for video id %s" % i_vid)

        data = json.dumps(response)
        self.send_json_response(data, 200)

    @tornado.gen.coroutine
    def update_thumbnail_property(self, tid):
       
        invalid_msg = "invalid thumbnail id or thumbnail id not found" 
        if tid is None:
            statemon.state.increment('invalid_thumbnail_id')
            self.send_json_response(invalid_msg, 400)

        prop = self.get_argument('property')
        if prop not in ['enabled']:
            raise tornado.web.MissingArgumentError('property')
        val = InputSanitizer.to_bool(self.get_argument('value'))

        def _mod_property(thumb_obj):
            thumb_obj.__dict__[prop] = val
                  

        tmdata = yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                        tid, _mod_property)
        if tmdata is None:
            statemon.state.increment('malformed_request')
            self.send_json_response(invalid_msg, 400)

        statemon.state.increment('thumb_updated')
        self.send_json_response('', 202)

class HealthCheckHandler(tornado.web.RequestHandler):
    '''Handler for health check ''' 

    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        '''Handle a test tracking request.'''
        if "video_server" in  self.request.uri:
            # Make a call to video server health check
            client_url = 'http://%s:8081/healthcheck'\
                            % options.video_server 
            req = tornado.httpclient.HTTPRequest(url=client_url,
                                                 method="GET",
                                                 request_timeout=5.0)
            result = yield tornado.gen.Task(utils.http.send_request, req)
            if result.error:
                self.set_status(502)
                self.write('{"error": "videoserver healthcheck fails"}') 
            else:
                self.set_status(200)
            self.finish()
            return

        self.write("<html> Server OK </html>")
        self.finish()

################################################################
### MAIN
################################################################

application = tornado.web.Application([
        (r"/healthcheck(.*)", HealthCheckHandler),
        (r'/api/v1/accounts(.*)', CMSAPIHandler),
        (r'/api/v1/jobs(.*)', CMSAPIHandler)],
        gzip=True)

def main():
    
    global server
    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    
    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
