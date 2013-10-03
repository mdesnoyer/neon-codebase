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
import logDatatoS3
import urllib2

nlines = 1000
port = 9080
fetch_count = 100
s_url = 'http://localhost:'+str(port) + '/track?x=1243&y=203984&z=3125ra'
p = subprocess.Popen("nohup python clickLogServer.py --port=" + port + " &", shell=True, stdout=subprocess.PIPE)

#put data
for i in range(nlines):
    r = urllib2.urlopen(s_url) 
    r.read()

drainer = logDatatoS3.S3DataHandler(nlines,port,fetch_count)
drainer.do_work()

