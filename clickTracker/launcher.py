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

def launch_clients(port):
    p = subprocess.Popen("nohup python eventServer.py --port=" + port + " &", shell=True, stdout=subprocess.PIPE)

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option('--nclients', default=False,type='int',help='number of process to start')
    parser.add_option('--start_port', default=8080,type='int' ,help='port number to start')
    options, args = parser.parse_args()
    
    num_clients = options.nclients
    start_port  = options.start_port

    #signal handlers
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    for i in range(num_clients): 
        #launch clients
        launch_clients( str(start_port + i) )
