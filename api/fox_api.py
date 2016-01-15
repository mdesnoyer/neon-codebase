'''
Fox API Interface class
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
import datetime
import json
import tornado.gen
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import tornado.escape
import utils.http

class FoxApi(object):
    BASE_URL = 'http://feed.theplatform.com/f/fng-fbc' 
    def __init__(self, feed_pid_ref):
        self.feed_pid_ref = feed_pid_ref

    @tornado.gen.coroutine
    def search(self, 
               from_date=datetime.datetime(1970,1,1), 
               sort_by='updated', 
               retries=5):
        '''search the fox feed 
           
        ''' 
        from_date_str = from_date.strftime('%Y-%m-%dT%H:%M:%SZ~') 
        url = '%s/%s?sort=%s&byUpdated=%s' \
              % (FoxApi.BASE_URL, self.feed_pid_ref, sort_by, from_date_str) 
        request = tornado.httpclient.HTTPRequest(url, 
                                                 method='GET',
                                                 request_timeout = 60.0)

        response = yield utils.http.send_request(request,
                                                 ntries=5,
                                                 do_logging=True,
                                                 async=True)
        if response:
            raise tornado.gen.Return(json.loads(response.body))
        else: 
            raise Exception('unable to fetch fox feed') 
