''' Submit jobs to the server '''

import tornado.httpclient
import tornado.ioloop
import tornado.httputil
import sys

try:
	file = sys.argv[1]
	local = int(sys.argv[2])
except:
	print "./script <url file> <local>"
	sys.exit(1)

def request(id,url):
    #CREATE POST REQUEST
    request_body = {}
    request_body["api_key"] = 'a63728c09cda459c3caaa158f4adff49'
    request_body["video_id"] = str(id)
    request_body["video_title"] = str(id) 
    request_body["video_url"] = url
    
    if local == 1:
        request_body["callback_url"] = "http://localhost:8081/testcallback"
        client_url = "http://localhost:8081/api/v1/submitvideo/topn"
    else:
        request_body["callback_url"] = "http://thumbnails.neon-lab.com/testcallback"
        client_url = "http://thumbnails.neon-lab.com/api/v1/submitvideo/topn"

    request_body["topn"] = 1

    body = tornado.escape.json_encode(request_body)
    h = tornado.httputil.HTTPHeaders({"content-type": "application/json"})


    http_client = tornado.httpclient.HTTPClient()
    req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers = h,body = body, request_timeout = 60.0, connect_timeout = 10.0)
    response = http_client.fetch(req)

with open(file,'r') as f:
    urls = f.readlines()

id = 0
for url in urls:
    id +=1
    url = url.rstrip('\n')
    request(id,url)

'''
for url in urls:
    url = url.rstrip('\n')
    val = url.split('/')[-1]
    id = val.split('.mp4')[0]
    request(id,url)
'''
