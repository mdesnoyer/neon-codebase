#!/usr/bin/env python
'''
Send Neon ISP counters to carbon to be alerted on
'''


import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

import json
import platform
import socket
import threading
import time
import urllib
import urllib2
import utils.neon
import utils.http
from utils.options import options, define

define("carbon_server", default="54.225.235.97", help="Montioring server", type=str)
define("carbon_port", default=8090, help="Monitoring port", type=int)
define("interval", default=60, help="time between stats", type=int)
define("isp_port", default=80, help="ISP port", type=int)

def send_data(name, value):
    '''
    Format metric name/val pair and send the data to the carbon server

    This is a best effort send
    '''
    node = platform.node().replace('.', '-')
    timestamp = int(time.time())
    data = 'system.%s.%s.%s %s %d\n' % (node, "isp", name, value, timestamp)
    sock = socket.socket()
    try:
        sock.connect((options.carbon_server, options.carbon_port))
        sock.sendall(data)
        sock.close()
    except Exception, e:
        pass
        
def query_neon_isp():
    '''
    Query Neon isp and get the json data
    '''
    try:
        url = "http://localhost:%d/stats" % options.isp_port
        req = urllib2.Request(url)
        r = urllib2.urlopen(req)
        return r.read()

    except urllib2.HTTPError, e:
        print e
        pass
    except urllib2.URLError, e:
        pass

def main():
    utils.neon.InitNeon()
    while True:
        jvals = query_neon_isp()
        if jvals is None:
            continue
            
        vals = json.loads(jvals)
        for name, val in vals.iteritems():
            send_data(name.lower(), val)
        time.sleep(options.interval)

if __name__ == "__main__":
    main()
