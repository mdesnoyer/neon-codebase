# Launch clients
#!/usr/bin/env python
import subprocess
import sys
import time
import signal
from optparse import OptionParser


def sig_handler(sig, frame):
    kill = True
    print "kill launcher"
    print client_pids
    sys.exit(1)

def read_version_from_file(fname):
    with open(fname,'r') as f:
        return int(f.readline())

def launch_clients():

    for i in range(nclients):
        if local:
            print "start"
            p = subprocess.Popen("nohup python client.py --model_file=" + model  + " --local &", shell=True, stdout=subprocess.PIPE)
        else:
            p = subprocess.Popen("nohup python client.py --model_file=" + model  + " &", shell=True, stdout=subprocess.PIPE)

if __name__ == "__main__":

    #signal handlers
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    #globals
    global kill
    global client_pids

    kill = False
    parser = OptionParser()

    parser.add_option('--local', default=False, action='store_true',
                      help='If set, use the localproperties file for config')
    parser.add_option('--n_workers', default=1, type='int',
                      help='Number of workers to spawn')
    parser.add_option('--model_file', default=None,
                      help='File that contains the model')
    parser.add_option('--debug', default=False, action='store_true',
                      help='If true, runs in debug mode')
    options, args = parser.parse_args()
    
    nclients = options.n_workers
    model = options.model_file
    local = options.local
    
    #if len(options) <2:
    #    print "missing args"
    #    sys.exit(0)

    sleep_interval = 5
    code_version_file = "code.version"

    #Log code release version
    code_release_version = read_version_from_file(code_version_file)

    #launch clients
    launch_clients()

    #If client exits after new code release version, restart clients
    while not kill:
        ps = subprocess.Popen("ps aux|grep client.py|awk '{print $2}'", shell=True, stdout=subprocess.PIPE)
        data = ps.stdout.read()
        data = data.split('\n')[:-1] #split the string and ignore empty '' at end
        client_pids = data
        count = len(data)
        nclients = count -2 # -2 since grep shows up twice (/bin/sh -c grep & grep)

        new_code_version = read_version_from_file(code_version_file)
        # if no client processes running and code release version has changed, then relaunch clients
        if nclients <= 0 and code_release_version < new_code_version:
            #update code release version
            print "New version of the code available and swapped"
            code_release_version = read_version_from_file(code_version_file)
            launch_clients()

        # If graceful shutdown requested, then do not launch the clients again
        elif nclients <=0 and new_code_version ==0:
            print "Graceful shutdown Requested" 
            kill = True
        
        time.sleep(sleep_interval)

    #kill all the client processes
    sys.exit(0)
