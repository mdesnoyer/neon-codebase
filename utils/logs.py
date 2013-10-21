'''Simple wrapper around the logging to make things easier.

In the __main__ process run:

AddConfiguredLogger()

In a module, create a local logger like:

_log = logging.getLogger(__name__)

and then call it to do logging e.g.

_log.error('Sad days. It is an error')

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

import logging
import logging.handlers
from .options import define, options
import SocketServer
import sys

define('file', default=None, type=str,
       help='File to output the default logs')
define('level', default='info', type=str,
       help=('Default logging level. '
       '"debug", "info", "warn", "error" or "critical"'))
define('format', default='%(asctime)s %(levelname)s:%(name)s %(message)s',
       help='Default log format')
define('do_stderr', default=1, type=int,
       help=('1 if we will generate a stderr output, 0 otherwise. '
             'The log level will be ERROR'))
define('do_stdout', default=1, type=int,
       help=('1 if we will generate a stdout output, 0 otherwise. '
             'The log level will be defined by the --level option'))

def AddConfiguredLogger():
    '''Adds a root logger defined by the config parameters.'''
    stdout_stream = None
    if options.do_stdout:
        stdout_stream = sys.stdout
        
    logger = CreateLogger(stream=stdout_stream,
                          logfile=options.file,
                          fmt=options.format,
                          level=str2level(options.level))

    # Add the extra stderr logger
    if options.do_stderr:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(options.format))
        handler.setLevel(logging.ERROR)
        logger.addHandler(handler)

def CreateLogger(name=None,
                 stream=None,
                 logfile=None,
                 socket_info=None,
                 fmt='%(asctime)s %(levelname)s:%(name)s %(message)s',
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
    return CreateLogger(name, logfile=logfile)

def SocketLogger(name, host='localhost', port=8020):
    return CreateLogger(name, socket_info=(host, port))

def StreamLogger(name, stream=sys.stdout):
    return CreateLogger(name, stream=stream)

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

def str2level(s):
    '''Converts a string to a logging level.'''
    d = {'debug': logging.DEBUG,
         'info' : logging.INFO,
         'warn' : logging.WARNING,
         'warning' : logging.WARNING,
         'error' : logging.ERROR,
         'critical' : logging.CRITICAL}

    return d[s.lower().strip()]
