#!/usr/bin/env python

import platform
import socket
import threading
import time
import statemon
from options import options, define

define("carbon_server", default="127.0.0.1", help="Montioring server", type=str)
define("carbon_port", default=8090, help="Monitoring port", type=int)
define("sleep_interval", default=60, help="time between stats", type=int)

def send_data(name, value):
    '''
    Format metric name/val pair and send the data to the carbon server

    This is a best effort send
    '''
        
    node = platform.node().replace('.', '-')
    timestamp = int(time.time())
    data = 'system.%s.%s %s %d\n' % (node, name, value, timestamp)
    sock = socket.socket()
    try:
        sock.connect((options.carbon_server, options.carbon_port))
        sock.sendall(data)
        sock.close()
    except Exception, e:
        pass
        #print "excp", e

class MonitoringAgent(threading.Thread):
    '''
    Thread that monitors the statemon variables
    '''

    def __init__(self):
        super(MonitoringAgent, self).__init__()
        self.daemon = True

    def run(self):
        ''' Thread run loop
            Grab the statemon state variable and send its values
        '''
        while True:
            self._run()
            time.sleep(options.sleep_interval)
    
    def _run(self):        
            m_vars = statemon.state.get_all_variables()
            #Nothing to monitor
            if len(m_vars) <= 0:
                return

            for variable, m_value in m_vars.iteritems():
                send_data(variable, m_value.value) 
                statemon.state.reset(variable)
