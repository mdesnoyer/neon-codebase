# Launch clients
#!/usr/bin/env python
import subprocess
import sys
import time
import signal
import os
from optparse import OptionParser

FNAME = 'clickLogServer.py' 
PORT  = 9080

def sig_handler(sig, frame):
    kill = True
    print "kill launcher"
    print client_pids
    sys.exit(1)

def launch_clients(port):
    p = subprocess.Popen("nohup python " + FNAME + " --port=" + port + " &", shell=True, stdout=subprocess.PIPE)

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option('--nclients', default=False,type='int',help='number of process to start')
    parser.add_option('--start_port', default=PORT,type='int' ,help='port number to start')
    parser.add_option('--stop', default=False,action='store_true',help='stop processes')
    options, args = parser.parse_args()
    
    num_clients = options.nclients
    start_port  = options.start_port

    #signal handlers
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)


    if not options.stop:
        for i in range(num_clients): 
            #launch clients
            launch_clients( str(start_port + i) )
    else:
        #stop clients
        ps = subprocess.Popen("ps aux|grep "+ FNAME + " |awk '{print $2}'", shell=True, stdout=subprocess.PIPE)
        data = ps.stdout.read()
        client_pid = data.split('\n')[:-1]
        for pid in client_pid:
            try:
                os.kill(int(pid),signal.SIGKILL)
            except:
                pass
