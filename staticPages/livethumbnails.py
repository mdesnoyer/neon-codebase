#/usr/bin/env python
import redis as blockingRedis
import urllib2
import json
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from supportServices.neondata import *
from api import brightcove_api

html_start = '<html>\<head><style>div.img{  margin: 5px;  padding: 5px;  border: 1px solid #0000ff;\
height: auto;  width: auto;  float: left;  text-align: center;}   div.img img{  display: inline;  margin: 5px;  border: 1px solid #ffffff;}div.img a:hover img {border: 1px solid #0000ff;}div.desc{  text-align: center;  font-weight: normal;  width: 120px;  margin: 5px;}</style></head><body>'

html_end = '</body></html>'
img_w = 480 ; img_h = 360;
thumb_w = 129 ; thumb_h = 90;
img_div_tmpl = '<div class="img"><img src="%s" alt="vid-%s" width="%s" height="%s"></div>' 

img_divs = ''
thumb_divs = ''
try:
    # Get all Brightcove accounts
    host = '127.0.0.1'
    port = 6379
    rclient = blockingRedis.StrictRedis(host,port)
    accounts = rclient.keys('brightcoveplatform*')
    for accnt in accounts:
        jdata = rclient.get(accnt) 
        bc = BrightcovePlatform.create(jdata)
        jfeed = brightcove_api.BrightcoveApi(bc.neon_api_key,
                bc.publisher_id,bc.read_token,
                bc.write_token
                ).get_publisher_feed()
        feed = json.loads(jfeed.body)
        items = feed['items']
        try:
            for item in items:
                vid = item['id']
                still = item['videoStillURL']
                thumb = item['thumbnailURL']
                if "thumbnail" in thumb:
                    img_divs += img_div_tmpl %(still,vid,img_w,img_h)
                    img_divs +='\n'
                    thumb_divs += img_div_tmpl %(thumb,vid,thumb_w,thumb_h)
                    thumb_divs +='\n'
        except:
            pass

except Exception,e:
    print e

#create live thumbs page
page = html_start + '\n' + img_divs + '\n'  + html_end
page = html_start + '\n' + thumb_divs + '\n'  + html_end
with open("livethumbnails.html",'w') as f:
    print >>f, page
