#!/usr/bin/env python
'''
'''
from urllib2 import Request, urlopen
import os
import time
import urllib2
import json
import logging
import sys
import requests
import StringIO

token = "[access token from cms auth endpoint]"

NEON_CMS_URL = "http://services.neon-lab.com/api/v2/[account id]/thumbnails/?token=%s&thumbnail_id=%s&enabled=0"

with open('badthumbs.txt') as f:
    thumbs = [x.strip('\n') for x in f.readlines()]
    for thumb in thumbs:
 print thumb
 request_url = NEON_CMS_URL % (token, thumb)
 print request_url
 req = urllib2.Request(request_url)
 req.get_method = lambda: 'PUT'
 res = urllib2.urlopen(req)
 data = json.loads(res.read())
 print data
