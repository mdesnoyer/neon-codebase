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
import urllib
import utils.http
import utils.neon 

class MicrosoftApi(object):
    BASE_URL = 'http://query.prod.cms.msn.com' 
    def __init__(self, feed_id):
        self.feed_id = feed_id

    @tornado.gen.coroutine
    def search(self, 
               from_date=datetime.datetime(1970,1,1), 
               sort_by='_lastEditedDateTime', 
               retries=5):
        '''three step process for microsoft  
           
           get the video_list 
           get each of the videos from the list 
           get the default image

           returns a list of videos with the image url 
              added   
        '''
        from_date_str = from_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        filter_params = "'$type'eq'list'and'_id'eq'%s'and" \
            "'_lastEditedDateTime'gt'%s'" % (self.feed_id, from_date_str)
        url = "%s/cms/api/amp/search?$filter=%s&details=full" \
              % (MicrosoftApi.BASE_URL, urllib.quote(filter_params))
        ms_json = yield self._get_response_body_from_url(url)

        try:  
            video_info = [] 
            video_list = ms_json[0]['list']
            for v in video_list:
                url = "%s/%s" % (MicrosoftApi.BASE_URL, v['reference']['href']) 
                res_json = yield self._get_response_body_from_url(url)
                thumbnail = res_json['thumbnail']
                url = "%s/%s" % (MicrosoftApi.BASE_URL, thumbnail['href'])
                img_json = yield self._get_response_body_from_url(url)
                res_json['thumbnail']['address'] = img_json['_name'] 
                res_json['thumbnail']['id'] = img_json['_id'] 
                video_info.append(res_json) 
        except Exception as e: 
            raise

        raise tornado.gen.Return(video_info) 

    @tornado.gen.coroutine
    def _get_response_body_from_url(self, url, want_json=True): 
        request = tornado.httpclient.HTTPRequest(url, 
                                                 method='GET',
                                                 request_timeout = 20.0)
        response = yield utils.http.send_request(request,
                                                 ntries=5,
                                                 do_logging=True,
                                                 async=True)
        if want_json: 
            raise tornado.gen.Return(json.loads(response.body))
        else: 
            raise tornado.gen.Return(response.body)
        


@tornado.gen.coroutine 
def main(): 
    ma = MicrosoftApi('AAc7iy')
    yield ma.search()   

if __name__ == "__main__":
    tornado.ioloop.IOLoop.current().run_sync(lambda: main())
