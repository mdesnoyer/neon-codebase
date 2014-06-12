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

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import copy
import datetime
import json
import logging
import logging.handlers
import platform
import SocketServer
import sys
import tornado.httpclient
import urllib
import urllib2
import utils.http

from utils.options import define, options
### Options to define the root logger when AddConfiguredLogger is called ###
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
define('flume_url', default=None, type=str,
       help=('Location of a flume endpoint to send to. '
             'e.g. http://localhost:6366'))
define('loggly_tag', default=None, type=str,
       help=('If set, sends the logs to loggly with the given tag.'))


### Typical configuration options that will be applied to multiple loggers ###
define('loggly_base_url',
       default='https://logs-01.loggly.com/inputs/520b9697-b7f3-4970-a059-710c28a8188a',
       help='Base url for the loggly endpoint')

def AddConfiguredLogger():
    '''Adds a root logger defined by the config parameters.'''
    stdout_stream = None
    if options.do_stdout:
        stdout_stream = sys.stdout
        
    logger = CreateLogger(stream=stdout_stream,
                          logfile=options.file,
                          loggly_tag=options.loggly_tag,
                          flume_url=options.flume_url,
                          fmt=options.format,
                          level=str2level(options.level))

    # Add the extra stderr logger
    if options.do_stderr:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(options.format))
        handler.setLevel(logging.ERROR)
        logger.addHandler(handler)

    logging.captureWarnings(True)

def CreateLogger(name=None,
                 stream=None,
                 logfile=None,
                 socket_info=None,
                 loggly_tag=None,
                 flume_url=None,
                 fmt='%(asctime)s %(levelname)s:%(name)s %(message)s',
                 level=logging.INFO):
    '''Adds handlers to the named logger and returns it.

    Inputs:
    name - Name of the logger. If None, does the root
    stream - If set, a handler is created for a stream. eg. sys.stdout
    logfile - If set, a handler is created that logs to a file
    socket_info - If (host, port), then a socket handler is added
    loggly_tag - Loggly tag to send records to
    flume_url - URL for the flume agent. We use the JSON HTTP source
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

    # For a loggly output
    if loggly_tag is not None:
         handler = LogglyHandler(loggly_tag)
         handler.setFormatter(formatter)
         logger.addHandler(handler)

    # For a flume output
    if flume_url is not None:
        handler = FlumeHandler(flume_url)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def FileLogger(name, logfile='error.log'):
    return CreateLogger(name, logfile=logfile)

def SocketLogger(name, host='localhost', port=8020):
    return CreateLogger(name, socket_info=(host, port))

def StreamLogger(name, stream=sys.stdout):
    return CreateLogger(name, stream=stream)

def LogglyLogger(name, tag='python'):
    return CreateLogger(name, loggly_tag=tag)

def FlumeLogger(name, host='localhost', port=6366):
    return CreateLogger(name, flume_url='http://%s:%s' % (host, port))

class TornadoHTTPHandler(logging.Handler):
    '''
    A class that sends a tornado based http request
    '''
    def __init__(self, url):
        super(TornadoHTTPHandler, self).__init__()
        self.url = url

    def get_verbose_dict(self, record):
        '''Returns a verbose dictionary of the record.'''
        retval = copy.deepcopy(record.__dict__)
        retval['msg'] = record.getMessage()
        del retval['args']
        return retval

    def generate_request(self, record):
        '''Create a tornado.httpclient.HTTPRequest from the record.

        Overwrite this in subclasses if necessary.
        '''
        data = urllib.urlencode(self.get_verbose_dict(record))
        return tornado.httpclient.HTTPRequest(
            self.url, method='POST', 
            headers={'Content-type' : 'application/x-www-form-urlencoded',
                     'Content-length' : len(data) },
            body=data)

    def emit(self, record):
        # Define the callback function so that we don't block here
        def handle_response(response):
            if response.error:
                try:
                    raise response.error
                except:
                    self.handleError(record)

        try:
            utils.http.send_request(self.generate_request(record),
                                    callback=handle_response)
        except:
            self.handleError(record)

class LogglyHandler(TornadoHTTPHandler):
    '''
    Class that can send the records to loggly.
    '''
    def __init__(self, tag):
        super(LogglyHandler, self).__init__(
            '%s/tag/%s' % (options.loggly_base_url, tag))

    def generate_request(self, record):
        data = self.get_verbose_dict(record)
        data['timestamp'] = datetime.datetime.utcnow().isoformat()
        data['host'] = platform.node()
        log_data = ("PLAINTEXT=" + 
                    urllib2.quote(json.dumps(data)))
        return tornado.httpclient.HTTPRequest(
            self.url, method='POST', 
            headers={'Content-type' : 'application/x-www-form-urlencoded',
                     'Content-length' : len(log_data) },
            body=log_data)

class FlumeHandler(TornadoHTTPHandler):
    '''
    Class that can send the records to flume.
    '''
    def __init__(self, url):
        super(FlumeHandler, self).__init__(url)

    def generate_request(self, record):
        flume_event = {
            'headers' : {
                'timestamp' : long(record.created * 1000),
                'level' : record.levelname
            },
            'body' : self.format(record)
        }
        data = json.dumps([flume_event])
        return tornado.httpclient.HTTPRequest(
            self.url, method='POST', 
            headers={'Content-type' : 'application/json' },
            body=data)

def str2level(s):
    '''Converts a string to a logging level.'''
    d = {'debug': logging.DEBUG,
         'info' : logging.INFO,
         'warn' : logging.WARNING,
         'warning' : logging.WARNING,
         'error' : logging.ERROR,
         'critical' : logging.CRITICAL}

    return d[s.lower().strip()]
