'''
CNN API Interface class
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
import datetime
from dateutil.parser import parse
import tornado.gen
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import tornado.escape
import utils.http

class CNNApi(object):

    BASE_URL = 'https://services.cnn.com' 
    def __init__(self, api_key):
        self.api_key = api_key

    @tornado.gen.coroutine
    def search(self, 
               start_date, 
               start_row=0, 
               rows=50, 
               search_type='video',
               sort_by='firstPublishDate,asc', 
               retries=5):
        '''search the cnn feed 
           
           this api returns a json object full of videos 
           by default we sort by firstPublishDate
           the last object in the set, holds the publish_date 
              that should be sent in as start_date on the next 
              request. 

           end_date will be based off of start_date and just 
              be 24 hours in the future 

           rows - how many videos we will get - defaults to 50 
           search_type - what we are searching for - defaults to video 
           sort_by - how to sort the data - defaults to firstPublishDate,asc

           returns json object of the search (response.body)  
               or raises an exception if no response is found   
        ''' 
        parsed_start = parse(start_date)
        parsed_start += datetime.timedelta(hours=24) 
        end_date = parsed_start.strftime('%Y-%m-%dT%H:%M:%SZ')  
         
        url = '%s/newsgraph/search/type:%s/firstPublishDate:%s~%s/rows:%d/start:%d/sort:%s?api_key=%s' \
              % (CNNApi.BASE_URL, search_type, start_date, end_date, rows, start_row, sort_by, self.api_key) 
        request = tornado.httpclient.HTTPRequest(url, 
                                                 method='GET',
                                                 request_timeout = 60.0)
        response = None  
        for r in range(retries):
            response = yield utils.http.send_request(request,
                                                     ntries=1,
                                                     do_logging=True,
                                                     async=True)
            if response.error is None:
                response = response.body 
                break 
            delay = (1 << r) * base_delay * random.random()
            yield tornado.gen.sleep(delay)

        if response:
            raise tornado.gen.Return(response)
        else: 
            raise Exception('unable to fetch cnn feed') 
