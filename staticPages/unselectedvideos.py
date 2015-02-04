#/usr/bin/env python
import redis as blockingRedis
import urllib2
import json
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb.neondata import *
from api import brightcove_api

html_start = '<html><head><style>div.img{  margin: 5px;  padding: 5px;  border: 1px solid #0000ff;'\
'height: auto;  width: auto;  float: left;  text-align: center;}   div.img img{  display: inline;  margin: 5px;  border: 1px solid #ffffff;}div.img a:hover img {border: 1px solid #0000ff;}div.desc{  text-align: center;  font-weight: normal;  width: 120px;  margin: 5px;}'\
'div.bimg{   border: 3px solid #FF0000;height: auto;  width: auto;  float: left;  text-align: center;}   div.bimg img{  display: inline;  margin: 5px;  border: 3px solid #ffffff;}div.img a:hover img {border: 10px solid #ffffff;} div.desc{  text-align: center;  font-weight: normal;  width: 120px;  margin: 5px;} </style></head><body>'
html_end = '</body></html>'
img_w = 480 ; img_h = 360;
thumb_w = 129 ; thumb_h = 90;
img_div_tmpl = '<div class="%s"><img src="%s" alt="%s" title="%s" width="%s" height="%s"></div>' 

img_divs = ''
thumb_divs = ''

skip_accounts = ["brightcoveplatform_dd2e93646e6509e275cf1e7712ca5a94_production_test1_24",
"brightcoveplatform_8bda0ee38d1036b46d07aec4040af69c_production_test1_26",
"brightcoveplatform_5cbcbb8e8ea526235f2eea41b572cf36_production_test1_25",
"brightcoveplatform_5329143981226ef6593f3762b636bd44_production_test1_23",
"brightcoveplatform_d927f1b798758dcd1d012263608f4ae8_production_test1_13"
]
try:
    # Get all Brightcove accounts
    host = '127.0.0.1'
    port = 6379
    rclient = blockingRedis.StrictRedis(host,port)
    accounts = rclient.keys('brightcoveplatform*')
    for accnt in accounts:
        if accnt in skip_accounts:
            continue
        jdata = rclient.get(accnt) 
        bc = BrightcovePlatform.create(jdata)
        jfeed = brightcove_api.BrightcoveApi(bc.neon_api_key,
                bc.publisher_id,bc.read_token,
                bc.write_token
                ).get_publisher_feed()
      
        jfeed2 = brightcove_api.BrightcoveApi(bc.neon_api_key,
                bc.publisher_id,bc.read_token,
                bc.write_token
                ).get_publisher_feed(page_no=1)
      
        a_id = bc.account_id
        i_id = bc.integration_id
        url = "http://localhost:8083/api/v1/accounts/%s/brightcove_integrations/%s/videos?" %(a_id,i_id)
        headers = {'X-Neon-API-Key' : bc.neon_api_key }
        req = urllib2.Request(url,None,headers)
        response = urllib2.urlopen(req)
        neon_response = json.loads(response.read())
        active_videos = []
        
        feed = json.loads(jfeed.body)
        fitems = feed['items']
        feed2 = json.loads(jfeed2.body)
        fitems.extend(feed2['items'])
        live_thumbs={}
        for fitem in fitems:
            vid = str(fitem['id'])
            live_thumbs[vid] = fitem['thumbnailURL']

        for nitem in neon_response:
            if nitem["status"] != "active":
                vid = nitem["video_id"]
                title = nitem["title"]
                thumb_divs += title + "<br/><br/>"
                for thumbnail in nitem["thumbnails"]:
                    if thumbnail["rank"] in [4,5]:
                        continue
                    thumb = thumbnail["urls"][0]
                    if thumbnail["type"]  == "brightcove":
                        thumb_divs += img_div_tmpl %("bimg",thumb,title,title,thumb_w,thumb_h)
                    else:
                        thumb_divs += img_div_tmpl %("img",thumb,title,title,thumb_w,thumb_h)
                    thumb_divs +='\n'
                    
                if vid in live_thumbs.keys():
                    print vid
                    thumb_divs += img_div_tmpl %("bimg",live_thumbs[vid],title,title,thumb_w,thumb_h)

                thumb_divs +="<br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/> \n"

except Exception,e:
    print e

t_page = html_start + '\n' + thumb_divs + '\n'  + html_end
with open("/var/www/static/unselectedvideos.html",'w') as f:
    print >>f, t_page.encode('utf-8').strip()
