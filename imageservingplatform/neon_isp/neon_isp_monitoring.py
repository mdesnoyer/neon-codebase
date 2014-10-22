#!/usr/bin/env python
'''
Send Neon ISP counters to carbon to be alerted on
'''


import os
import os.path
import json
import platform
import socket
import threading
import time
import urllib
import urllib2
from optparse import OptionParser


def send_data(name, value, options):
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
        
def query_neon_isp(port):
    '''
    Query Neon isp and get the json data
    '''
    try:
        url = "http://localhost:%s/stats" % port
        req = urllib2.Request(url)
        r = urllib2.urlopen(req)
        return r.read()

    except urllib2.HTTPError, e:
        print e
        pass
    except urllib2.URLError, e:
        pass

def main(options):
    while True:
        jvals = query_neon_isp(options.isp_port)
        if jvals is None:
            continue
            
        vals = json.loads(jvals)
        for name, val in vals.iteritems():
            send_data(name.lower(), val, options)
        time.sleep(options.sleep_interval)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-s", "--carbon_server", dest="carbon_server", default="54.225.235.97")
    parser.add_option("-d", "--carbon_port", dest="carbon_port", default=8090)
    parser.add_option("-i", "--sleep_interval", dest="sleep_interval", default=60)
    parser.add_option("-p", "--port", dest="isp_port", default=80)
    (options, args) = parser.parse_args()
    main(options)

