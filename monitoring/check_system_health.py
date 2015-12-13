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
import utils.monitor
import utils.neon
from utils.options import options, define

def get_proc_memory():
    '''
    Returns memory in MB 
    '''
    rusage_denom = 1024.
    if sys.platform == 'darwin':
        # ... it seems that in OSX the output is different units ...
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem

def get_disk_usage():
    '''
    Return / & /mnt disk usage in %
    '''
    #psutil.disk_partitions()
    v_root = psutil.disk_usage('/')
    v_mnt = psutil.disk_usage('/mnt')
    return v_root[3], v_mnt[3]

def get_network_usage():
    ''' return sent, recv (in bytes)'''
    vals = psutil.net_io_counters(pernic=True)
    if platform.system() == "Linux":
        vals = vals['eth0']
        return (vals[0], vals[1])
    elif platform.system() == "Darwin":
        vals = vals['en0']
        return (vals[0], vals[1])

def get_system_memory():
    '''
    returns % of meomory currently used in the system  
    '''
    #perfect used memory
    return psutil.virtual_memory()[2]

def get_cpu_usage():
    '''
    return cpu usage in %
    '''
    # sum(user, system), normalize by num
    #return (psutil.cpu_times()[0] + psutil.cpu_times()[2]) /psutil.NUM_CPUS
    return psutil.cpu_percent(interval=0.1)

def get_loadavg():
    '''
    load average from uptime  
    '''

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

def main():
    delay = 60
    utils.neon.InitNeon()
    
    def sighandler(sig, frame):
        sys.exit(0)

    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    
    while True:
        
        try:
            utils.monitor.send_data("cpu", get_cpu_usage())
            utils.monitor.send_data("memory_used", get_system_memory())
            d_root, d_mnt = get_disk_usage()
            utils.monitor.send_data("disk_used_root", d_root) 
            utils.monitor.send_data("disk_used_mnt", d_mnt)
            b_sent, b_recv = get_network_usage()
            utils.monitor.send_data("network_bytes_sent", b_sent)
            utils.monitor.send_data("network_bytes_recv", b_recv)
        except:
            pass

        time.sleep(delay)

if __name__ == '__main__':
    main()
