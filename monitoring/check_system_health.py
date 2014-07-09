#!/usr/bin/env python

'''
Check system vitals and send metrics to carbon agent /monitoring server

Run this script to send data to carbon server
'''

import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

import time
import os
import platform 
import psutil
import resource
import signal
import socket
import subprocess
import utils.neon
from utils.options import options, define

define('carbon_server', default='54.225.235.97', help='carbon ip address')
define('carbon_port', default=8090, type=int, help='carbon port')

def get_proc_memory():
    rusage_denom = 1024.
    if sys.platform == 'darwin':
        # ... it seems that in OSX the output is different units ...
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem

def get_disk_usage():
    #psutil.disk_partitions()
    v_root = psutil.disk_usage('/')
    v_mnt = psutil.disk_usage('/mnt')
    return v_root[3], v_mnt[3]

def get_network_usage():
    ''' return sent, recv'''
    vals = psutil.net_io_counters(pernic=True)
    if platform.system() == "Linux":
        vals = vals['eth0']
        return (vals[0], vals[1])
    elif platform.system() == "Darwin":
        vals = vals['en0']
        return (vals[0], vals[1])

def get_system_memory():
    #perfect used memory
    return psutil.virtual_memory()[2]

def get_cpu_usage():
    #normalize by num
    return psutil.cpu_times()[0] /psutil.NUM_CPUS

def get_loadavg():
    # For more details, "man proc" and "man uptime"  
    if platform.system() == "Linux":
        return open('/proc/loadavg').read().strip().split()[:3]
    else:   
        command = "uptime"
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        os.waitpid(process.pid, 0)
        output = process.stdout.read().replace(',', ' ').strip().split()
        length = len(output)
        return output[length - 3:length]

def send_data(name, value):
    '''
    Format metric name/val pair and send the data to the carbon server
    '''
    
    node = platform.node().replace('.', '-')
    timestamp = int(time.time())
    message = 'system.%s.%s %s %d\n' % (node, name, value, timestamp)
    sock = socket.socket()
    try:
        sock.connect((options.carbon_server, options.carbon_port))
        sock.sendall(message)
        sock.close()
    except Exception, e:
        pass

def main():
    delay = 60
    utils.neon.InitNeon()
    
    def sighandler(sig, frame):
        sys.exit(0)

    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    
    while True:
        
        try:
            send_data("cpu", get_cpu_usage())
            send_data("memory_used", get_system_memory())
            d_root, d_mnt = get_disk_usage()
            send_data("disk_used_root", d_root) 
            send_data("disk_used_mnt", d_mnt)
            b_sent, b_recv = get_network_usage()
            send_data("network_bytes_sent", b_sent)
            send_data("network_bytes_recv", b_recv)
        except:
            pass

        time.sleep(delay)

if __name__ == '__main__':
    main()
