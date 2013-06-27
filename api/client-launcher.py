# Launch clients
#!/usr/bin/env python
import subprocess
import sys
import time
import signal

def sig_handler(sig, frame):
    kill = True
    print "kill launcher"
    print client_pids
    sys.exit(1)

def read_version_from_file(fname):
    with open(fname,'r') as f:
        return int(f.readline())

def launch_clients():
    p = subprocess.Popen("nohup python client.py " + num_clients + " " + local + " &", shell=True, stdout=subprocess.PIPE)

if __name__ == "__main__":

    #signal handlers
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    #globals
    global kill
    global client_pids

    kill = False
    try:
        num_clients = sys.argv[1]
        local = sys.argv[2]
    except:
        print "./script <nclients> <local properties 0/1>"
        sys.exit(0)

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
