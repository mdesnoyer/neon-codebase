#!/usr/bin/env python

'''
Create email summary by show of new videos added
'''
import os
import urllib2
import json
import logging
import sys
import StringIO
import smtplib
import calendar
import json
from datetime import date, datetime, time, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

NEON_CMS_URL = "http://services.neon-lab.com"

logging.basicConfig(filename='daily_mail_discovery.log',level=logging.DEBUG, format='%(asctime)s %(message)s')
_log = logging.getLogger(__name__)

### Check if its already running
pid = str(os.getpid())
pidfile = "/tmp/discovery_mail.pid"
if os.path.isfile(pidfile):
    with open(pidfile, 'r') as f:
        pid = f.readline().rstrip('\n')
        if os.path.exists('/proc/%s' %pid):
            print "%s already exists, exiting" % pidfile
            sys.exit()
        else:
            os.unlink(pidfile)
                


_log.info('\n\n-----Checking Neon for new Discovery videos-----\n')

def send_email(neon_vids):
    print "sending email"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = "New Videos from Yesterday"
    msg['From'] = "abtest@neon-lab.com" 
    msg['To'] = "lea@neon-lab.com"

    text = ""
    html = """
    <html>
        <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
        <head></head>
        <body>
            <h1 style='font-family: Arial, Helvetica, sans-serif';>Discovery videos added in the last day:</h1>
    """
    print neon_vids

    for vid in neon_vids:
        html = html + ("<h3 style='font-family: Arial, Helvetica, sans-serif';>%s</h2><table><tr>" % vid["title"].encode('utf-8'))
        for thumb in vid["thumbnails"]:
            html = html + "<td><img src='" + thumb["urls"][0].encode('utf-8')  + "' width=150 /><br /><span style='font-family: Arial, Helvetica, sans-serif';>" + thumb["type"].encode('utf-8') + "</span></td>"

        html = html + "</tr></table><br /><br />"
    html = html + """
        </body>
        </html>
    """

    print html

    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    msg.attach(part1)
    msg.attach(part2)

    #output = StringIO.StringIO()
    #output.write(data)
    #output.seek(0)
    #msg = MIMEText(output.read())
    #frm = "create_requests@neon-lab.com" 
    #to = "lea@neon-lab.com" 
    #msg['Subject'] = 'New Videos Email'
    #msg['From'] = "abtest@neon-lab.com" 
    #msg['To'] = "lea@neon-lab.com" 

    #s = smtplib.SMTP('localhost')
    #s.sendmail(me, you, msg.as_string())
    #s.quit()


def get_neon_account_videos(account_id, api_key, integration_id, video_ids):
    '''
    Get video metadata objects from Neon Account
    ''' 
    videos = []
    video_api_formater = "%s/api/v1/accounts/%s/brightcove_integrations/%s/videos?video_ids=%s"
    headers = {"X-Neon-API-Key" : API_KEY }
    request_url = video_api_formater % (NEON_CMS_URL, account_id, integration_id, video_ids)

    print request_url

    req = urllib2.Request(request_url, headers=headers)
    res = urllib2.urlopen(req)
    return json.loads(res.read())

def get_recent_bc_videos(api_call):
    '''
    Get video objects from Discovery Video Cloud account
    ''' 
    videos = []

    req = urllib2.Request(api_call)
    res = urllib2.urlopen(req)
    data = json.loads(res.read())
    return data

def is_last_24(publishedDate):
    midnight = datetime.combine(date.today(), time.min)
    yes_mid = midnight - timedelta(days=1)
    pub_date = datetime.fromtimestamp(int(publishedDate)/1000)
    return pub_date >= yes_mid

#Discovery Acct
account_id = "314"
integration_id = "71"
API_KEY = "gvs3vytvg20ozp78rolqmdfa"


#videos = get_neon_account_videos(account_id, API_KEY, integration_id)

'''
first get the list of "published in the last day" videos from Video Cloud
'''

api_call = "http://api.brightcove.com/services/library?command=search_videos&any=network%3Adsc&page_size=200&video_fields=id%2Cname%2CpublishedDate%2Ctags%2CcustomFields&media_delivery=default&sort_by=PUBLISH_DATE%3ADESC&page_number=0&token=09PjqJcVX6Or31v9VnXM3fZn_Z8X_SRmZyTqtUjdmiM."

data = get_recent_bc_videos(api_call)

videos = []

for item in data["items"]:
    if is_last_24(item["publishedDate"]):
        videos.append(item)

'''
Now go and get all these videos from Neon
'''
video_ids = ""

for vid in videos:
    video_ids = video_ids + ("%s," % vid["customFields"]["newmediapaid"])

video_ids = video_ids[:-1]

neon_vids = get_neon_account_videos(account_id, API_KEY, integration_id, video_ids)

'''
Time to format our email
'''
send_email(neon_vids["items"])


# Remove PID file                    
#os.unlink(pidfile)
#if len(video_requests) > 0:
#    send_email("".join(video_requests))
