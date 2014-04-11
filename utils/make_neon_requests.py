''' Submit jobs to the server '''

import tornado.httpclient
import tornado.ioloop
import tornado.httputil
import shortuuid
from optparse import OptionParser

API_KEY = '2630b61d2db8c85e9491efa7a1dd48d0'
parser = OptionParser()
parser.add_option('--input', default="video.links", type=str) 
parser.add_option('--api_key', default="2630b61d2db8c85e9491efa7a1dd48d0", type=str) 

def request(api_key, id, url):
    #CREATE POST REQUEST
    request_body = {}
    request_body["api_key"] = api_key
    request_body["video_id"] = str(id)
    request_body["video_title"] = str(id) 
    request_body["video_url"] = url
    request_body["topn"] = 6 
    
    request_body["callback_url"] = "http://localhost:8081/testcallback"
    client_url = "http://localhost:8081/api/v1/submitvideo/topn"

    body = tornado.escape.json_encode(request_body)
    h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})

    http_client = tornado.httpclient.HTTPClient()
    req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers = h,body = body, request_timeout = 60.0, connect_timeout = 10.0)
    response = http_client.fetch(req)

if __name__ == "__main__":
    options, args = parser.parse_args()

    with open(options.input,'r') as f:
        urls = f.readlines()

    for url in urls:
        url = url.rstrip('\n')
        vid = shortuuid.uuid() 
        request(options.api_key, vid, url)

