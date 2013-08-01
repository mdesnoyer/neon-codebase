#/usr/bin/env python

import logging
import logging.handlers
import SocketServer
import sys

def createLogger(name=None,
                 stream=None,
                 logfile=None,
                 socket_info=None,
                 fmt='%(asctime)s %(levelname)s %(message)s',
                 level=logging.INFO):
    '''Adds handlers to the named logger and returns it.

    Inputs:
    name - Name of the logger. If None, does the root
    stream - If set, a handler is created for a stream. eg. sys.stdout
    logfile - If set, a handler is created that logs to a file
    socket_info - If (host, port), then a socket handler is added
    fmt - The format of the log
    level - The level for the root logger

    Returns:
    the logger
    '''
    if name is None:
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(fmt)

    # For a stream output
    if stream is not None:
        handler = logging.StreamHandler(stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # For a file output
    if logfile is not None:
        handler = logging.FileHandler(logfile)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # For a socket output
    if socket_info is not None:
        handler = logging.handlers.SocketHandler(socket_info[0],
                                                 socket_info[1])
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def FileLogger(name, logfile='error.log'):
    return createLogger(name, logfile=logfile)

def SocketLogger(name, host='localhost', port=8020):
    return createLogger(name, socket_info=(host, port))

def StreamLogger(name, stream=sys.stdout):
    return createLogger(name, stream=stream)

class LogServer(SocketServer.BaseRequestHandler):

    logfile = "/var/log/neonserver.log"

    def handler(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        #print "{} wrote:".format(self.client_address[0])
        print self.data

	def start(self):
	    HOST, PORT = "localhost", 8020
	    server = SocketServer.TCPServer((HOST, PORT), self.handler)

	    # Activate the server; this will keep running until you
	    # interrupt the program with Ctrl-C
	    server.serve_forever()
