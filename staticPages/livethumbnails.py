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
'height: auto;  width: auto;  float: left;  text-align: center;}   div.img img{  display: inline;  margin: 5px;  border: 1px solid #ffffff;}div.img a:hover img {border: 1px solid #0000ff;}div.desc{  text-align: center;  font-weight: normal;  width: 120px;  margin: 5px;}</style></head><body>'

html_end = '</body></html>'
img_w = 480 ; img_h = 360;
thumb_w = 129 ; thumb_h = 90;
img_div_tmpl = '<div class="img"><img src="%s" alt="%s" title="%s"></div>' 

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
      
        a_id = bc.account_id
        i_id = bc.integration_id
        url = "http://localhost:8083/api/v1/accounts/%s/brightcove_integrations/%s/videos?" %(a_id,i_id)
        headers = {'X-Neon-API-Key' : bc.neon_api_key }
        req = urllib2.Request(url,None,headers)
        response = urllib2.urlopen(req)
        neon_response = json.loads(response.read())
        active_videos = []
        
        for nitem in neon_response:
            if nitem["status"] == "active":
                active_videos.append(nitem["video_id"])

        feed = json.loads(jfeed.body)
        items = feed['items']
        try:
            for item in items:
                vid = str(item['id'])
                title = item['name']
                still = item['videoStillURL']
                thumb = item['thumbnailURL']
                if vid in active_videos:
                    img_divs += img_div_tmpl %(still,title,title)
                    img_divs +='\n'
                    thumb_divs += img_div_tmpl %(thumb,title,title)
                    thumb_divs +='\n'
        except:
            pass

except Exception,e:
    print e

#create live thumbs page
s_page = html_start + '\n' + img_divs + '\n'  + html_end
t_page = html_start + '\n' + thumb_divs + '\n'  + html_end
with open("/var/www/static/livethumbnails.html",'w') as f:
    print >>f, t_page.encode('utf-8').strip()

with open("/var/www/static/livevideostills.html",'w') as f:
    print >>f, s_page.encode('utf-8').strip()
