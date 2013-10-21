# Launch clients
#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import subprocess
import time
import signal
import os
import utils.neon
from utils.options import define, options

define('nclients', default=1, type=int, help='number of process to start')
define('start_port', default=9080, type=int, help='port number to start')
define('stop', default=0, type=int, help='stop processes')

FNAME = 'clickLogServer.py' 

def sig_handler(sig, frame):
    kill = True
    print "kill launcher"
    print client_pids
    sys.exit(1)

def launch_clients(port):
    p = subprocess.Popen("nohup python " + FNAME + " --port=" + port + " &", shell=True, stdout=subprocess.PIPE)

if __name__ == "__main__":
    
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
