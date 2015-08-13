#!/usr/bin/env python
'''
Monitor the time it take for a video to go through the Neon pipeline
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
from cmsdb import neondata
import json
import redis
import signal
import time
import urllib2
import utils.http
import utils.neon
import utils.ps
from utils import statemon

from utils.options import define, options
define("cmsapi_host", default="services.neon-lab.com", help="cmsapi server", type=str)
define("isp_host", default="i1.neon-images.com", help="host where the isp is")
define("account", default="159", help="account id", type=str)
define("api_key", default="3yd7b8vmrj67b99f7a8o1n30", help="api key", type=str)
define("sleep", default=1800, help="sleep time", type=int)
define("attempts_threshold", default=50, help="attempts", type=int)
define("test_video", default="https://neon-test.s3.amazonaws.com/output.mp4",
       help='Video to test with')

# counters
statemon.define('total_time_to_isp', int)
statemon.define('time_to_serving', int)
statemon.define('mastermind_to_isp', int)
statemon.define('job_not_serving', int)
statemon.define('exception_thrown', int)
statemon.define('not_available_in_isp', int)
statemon.define('request_not_in_db', int)

import logging
_log = logging.getLogger(__name__)

class RunningTooLongError(Exception): pass


class MyHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    '''
    A redirect handler for urllib2 requests 
    opener = urllib2.build_opener(MyHTTPRedirectHandler, cookieprocessor)
    in a single threaded process, you can use get_last_redirect_response()
    to get the intermediate 302 response
    '''
    
    redirect_response = None
    
    def http_error_302(self, req, fp, code, msg, headers):
        MyHTTPRedirectHandler.redirect_response = headers
        return urllib2.HTTPRedirectHandler.http_error_302(self, 
                                    req, fp, code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302

    @classmethod
    def get_last_redirect_response(cls):
        return cls.redirect_response

def create_neon_api_request(account_id, api_key):
    '''
    create random video processing request to Neon
    '''
    
    video_api_formater = "http://%s/api/v1/accounts/%s/neon_integrations/0/create_thumbnail_api_request"
    headers = {"X-Neon-API-Key" : api_key, "Content-Type" : "application/json"}
    request_url = video_api_formater % (options.cmsapi_host, account_id)
    v = int(time.time())
    video_id = "test%d" % v
    video_title = "monitoring"
    video_url = "%s?x=%s" % (options.test_video, video_id)

    data =     { 
        "video_id": video_id,
        "video_url": video_url, 
        "video_title": video_title,
        "callback_url": None
    }

    try:
        req = urllib2.Request(request_url, headers=headers)
        res = urllib2.urlopen(req, json.dumps(data))
        api_resp = json.loads(res.read())
        return (video_id, api_resp["job_id"])
    except urllib2.URLError, e: 
        print "video error" , e
    except:
        # any exception
        pass

def image_available_in_isp(pub, vid):
    url = "http://%s/v1/client/%s/neonvid_%s" % (options.isp_host, pub, vid)
 
    try:
        cookieprocessor = urllib2.HTTPCookieProcessor()
        opener = urllib2.build_opener(MyHTTPRedirectHandler, cookieprocessor)
        req = urllib2.Request(url)
        res = opener.open(req)

        # check for the headers and the final image
        redirect_response = MyHTTPRedirectHandler.get_last_redirect_response()
        headers = redirect_response.headers
        
        im_url = None
        for header in headers:
            if "Location" in header:
                im_url = header.split("Location: ")[-1].rstrip("\r\n")
                if "NO_VIDEO" in im_url:
                    return False
                return True
    except urllib2.URLError, e: 
        pass
    except:
        pass

    return False

def monitor_neon_pipeline():
    
    # reset all the counters
    #statemon.state.time_to_serving = 0 
    #statemon.state.mastermind_to_isp = 0 
    #statemon.state.total_time_to_isp = 0
    
    start_request = time.time()

    # Create a video request for test account
    try:
        video_id, job_id = create_neon_api_request(options.account, options.api_key)
        _log.info('created video request vid %s job %s' % (video_id, job_id))

        # Poll the API for job request completion
        job_serving = False
        attempts = 0 
        while not job_serving:
            attempts += 1
            request = neondata.NeonApiRequest.get(job_id, options.api_key)
            if request:
                _log.info_n("current request state is %s" % request.state,
                            10)
                if request.state == "serving":
                    job_serving = True
                    continue
            else:
                _log.warn("request data not found in db")
                statemon.state.increment('request_not_in_db')
                # Should we attempt to cleanup in this case ?

            time.sleep(30)
            if attempts > options.attempts_threshold:
                statemon.state.increment('job_not_serving')
                raise RunningTooLongError

        # time to video serving
        video_serving = time.time() - start_request
        statemon.state.time_to_serving = int(video_serving)
        _log.info("video is in serving state, took %s" % video_serving)

        # TODO(Sunil): track the image creation times as well
        # IF Serving.....
        # Poll the DB for Thumbnail serving generation
        #vm = neondata.VideoMetadata.get(i_vid)
        #if vm:
        #    thumbs = neondata.ThumbnailMetadata.get_many(vm.thumbnail_ids)
        #    if thumbs[0]:
        #        thumbs[0].created

        # Query ISP to get the IMG
        isp_start = time.time()
        isp_ready = False
        attempts = 0
        acct = neondata.NeonUserAccount.get(options.api_key)
        while not isp_ready:
            attempts += 1
            isp_ready = image_available_in_isp(acct.tracker_account_id, video_id)
            if not isp_ready:
                time.sleep(15)
            if attempts > 20: 
                statemon.state.increment('not_available_in_isp')
                raise RunningTooLongError
        
        isp_serving = time.time() - isp_start 
        statemon.state.mastermind_to_isp = int(isp_serving)
        _log.info("video is in ISP, took %s s from mastermind to ISP" % isp_serving)
        
        # Now you can delete the video from the database; Write an Internal API
        # delete video; request; thumbnails; serving thumbs
        
        total_time = time.time() - start_request
        statemon.state.total_time_to_isp = int(total_time)
        _log.info("total pipeline time %s" % total_time)

    finally:
        # cleanup
        np = neondata.NeonPlatform.get(options.api_key, '0')
        np.delete_all_video_related_data(video_id, really_delete_keys=True)

def main():
    utils.neon.InitNeon()

    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    
    while True:
        try:
            monitor_neon_pipeline()
        except Exception as e:
            _log.exception('Exception when monitoring')
            statemon.state.increment('exception_thrown')
        time.sleep(options.sleep)

if __name__ == "__main__":
    main()
