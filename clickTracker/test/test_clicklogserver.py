'''
Test functionality of the click log server.

- Spin up the server ( n-1 click loggers, 1 s3 uploader)
- Generate 'X' requests 
- Drain 'X' requests 
- If successful drain and upload to s3, exit with success message
    
: False injections, disconnect s3 

#TODO: Nagios like script to monitor the following issues

- Server not at capacity, 1 or more process died
- S3 uploader has issues with s3 connection
- Memory on the box is running out, Q drain issue 

'''

import subprocess
import sys
import urllib2
import random
import boto
from boto.s3.connection import S3Connection
from moto import mock_s3
from logDatatoS3 import *

@mock_s3
def log_to_s3():
    conn = S3Connection('test','test')
    bucket = conn.create_bucket('neon-tracker-logs')
    drainer = S3DataHandler(nlines,port,fetch_count)
    drainer.run()


nlines = 1000
port = 9080
fetch_count = 100
l_url = 'http://localhost:'+str(port)+'/track?a=load&id=288edb2d31c34507&imgs=%5B%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F34%2F2294876105001_2727914703001_thumbnail-2296855887001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F354%2F2294876105001_2727881607001_thumbnail-2369368872001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F2294876105001_2660525568001_thumbnail-2296855886001.jpg%22%2C%22http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fe1%2Fpd%2F2294876105001%2F2294876105001_2617231423001_thumbnail-2323153341001.jpg%22%5D&cvid=2296855887001&ts=1381257030328&page=http%3A%2F%2Flocalhost%2Fbcove%2Ffplayerabtest.html&aid=accountIDNotSet&ttype=flashonlyplayer&noCacheIE=1381257030328'

c_url = 'http://localhost:'+str(port)+'/track?a=click&id=14b150ad6a59e93c&img=http%3A%2F%2Fbrightcove.vo.llnwd.net%2Fd21%2Funsecured%2Fmedia%2F2294876105001%2F201310%2F34%2F2294876105001_2727914703001_thumbnail-2296855887001.jpg&ts=1381264478544&page=http%3A%2F%2Flocalhost%2Fbcove%2Ffplayerabtest.html&aid=accountIDNotSet&ttype=flashonlyplayer'

p = subprocess.Popen("nohup python ../clickLogServer.py --port=" + str(port) + " &", shell=True, stdout=subprocess.PIPE)

#put data ( 99% click, 1% load)
for i in range(nlines):
    
    rd = random.randint(1,100)
    if rd <2:
        r = urllib2.urlopen(c_url) 
    else:
        r = urllib2.urlopen(l_url) 
    r.read()

log_to_s3()
