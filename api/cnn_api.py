'''
CNN API Interface class
'''

import os
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


class CNNApi(object):

    BASE_URL = 'https://services.cnn.com'

    def __init__(self, api_key):
        self.api_key = api_key

    @tornado.gen.coroutine
    def search(self,
               start_date=datetime.datetime(1970, 1, 1),
               start_row=0,
               rows=50,
               search_type='video',
               sort_by='firstPublishDate,asc',
               retries=5):
        '''search the cnn feed

           by default we sort by firstPublishDate
           the last object in the set, holds the publish_date
              that should be sent in as start_date on the next
              request.

           rows - how many videos we will get - defaults to 50
           search_type - what we are searching for - defaults to video
           sort_by - how to sort the data - defaults to firstPublishDate,asc

           returns dictionary of the search (response.body)
               or raises an exception if no response is found
        '''
        end_date = start_date + datetime.timedelta(hours=24)
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        url = '%s/newsgraph/search/type:%s/firstPublishDate:%s~%s/rows:%d/start:%d/sort:%s?api_key=%s' \
              % (CNNApi.BASE_URL, search_type, start_date_str, end_date_str, rows, start_row, sort_by, self.api_key)
        request = tornado.httpclient.HTTPRequest(url,
                                                 method='GET',
                                                 request_timeout=60.0)

        response = yield utils.http.send_request(request,
                                                 ntries=5,
                                                 do_logging=True,
                                                 async=True)
        if response:
            raise tornado.gen.Return(json.loads(response.body))
        else:
            raise Exception('unable to fetch cnn feed')
