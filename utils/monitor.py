#!/usr/bin/env python

import platform
import socket
import threading
import time
import statemon
from options import options, define

define("carbon_server", default="127.0.0.1", help="Montioring server", type=int)
define("carbon_port", default=8090, help="Monitoring port", type=int)

SLEEP_INTERVAL = 10

class MonitoringAgent(threading.Thread):
    '''
    Monitoring Agent
    '''

    def __init__(self):
        super(MonitoringAgent, self).__init__()
        self.daemon = True

    def send_data(self, name, value):
        '''
        Format metric name/val pair and send the data to the carbon server
        '''
        
        node = platform.node().replace('.', '-')
        timestamp = int(time.time())
        data = 'system.%s.%s %s %d\n' % (node, name, value, timestamp)
        self._send_msg(data)

    def _send_msg(self, message):
        ''' Send the message to the socket [best effort] '''
        sock = socket.socket()
        try:
            sock.connect((options.carbon_server, options.carbon_port))
            sock.sendall(message)
            sock.close()
        except Exception, e:
            pass
            #print "excp", e

    def run(self):
        ''' Thread run loop
            Grab the statemon state variable and send its values
        '''
        while True:
            self._run()
            time.sleep(SLEEP_INTERVAL)
    
    def _run(self):        
            m_vars = statemon.state.get_all_variables()
            #Nothing to monitor
            if len(m_vars) <= 0:
                return

            for variable, m_value in m_vars.iteritems():
                self.send_data(variable, m_value.value) 
