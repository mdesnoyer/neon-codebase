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

import atexit
import copy
from cloghandler import ConcurrentRotatingFileHandler
import datetime
import json
import logging
import logging.handlers
import platform
import SocketServer
import sys
import threading
import tornado.gen
import tornado.httpclient
import urllib
import urllib2
from utils import statemon
import utils.sync

from utils.options import define, options
### Options to define the root logger when AddConfiguredLogger is called ###
define('file', default=None, type=str,
       help='File to output the default logs')
define('level', default='info', type=str,
       help=('Default logging level. '
       '"debug", "info", "warn", "error" or "critical"'))
define('format', default=('%(asctime)s %(levelname)s:%(name)s[%(threadName)s]'
                          ' %(message)s'),
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
define('max_log_file_size', default=104857600L, #100MB
       help='Maximum log file size')
define('access_log_file', default=None, type=str,
       help='File to write the access logs to')

_log = logging.getLogger(__name__)


### Typical configuration options that will be applied to multiple loggers ###
define('loggly_base_url',
       default='https://logs-01.loggly.com/inputs/520b9697-b7f3-4970-a059-710c28a8188a',
       help='Base url for the loggly endpoint')

# State variables
statemon.define('http_log_errors', int)
_http_log_error_ref = statemon.state.get_ref('http_log_errors')

# grabbed from logging.py
#
# _srcfile is used when walking the stack to check when we've got the first
# caller stack frame.
#
if hasattr(sys, 'frozen'): #support for py2exe
    _srcfile = "logging%s__init__%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)

def currentframe():
    """Return the frame object for the caller's stack frame."""
    try:
        raise Exception
    except:
        return sys.exc_info()[2].tb_frame.f_back

if hasattr(sys, '_getframe'): currentframe = lambda: sys._getframe(3)
# done filching

_added_configured_logger = False
def AddConfiguredLogger():
    '''Adds a root logger defined by the config parameters.'''
    global _added_configured_logger
    if _added_configured_logger:
        _log.warning('Already added logging')
        return
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

    # Add a logger for tornado access logs
    if options.access_log_file is not None:
        CreateLogger('tornado.access',
                     logfile=options.access_log_file,
                     fmt=options.format,
                     level=logging.INFO)
    logging.getLogger('tornado.access').propagate = False

    logging.captureWarnings(True)
    _added_configured_logger = True

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
        # Rotating file handler
        handler = ConcurrentRotatingFileHandler(
            logfile,
            maxBytes=options.max_log_file_size,
            backupCount=3)
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
    def __init__(self, url, emit_error_sampling_period=60):
        super(TornadoHTTPHandler, self).__init__()
        self.url = url
        self.emit_error_sampling_period = emit_error_sampling_period
        self.last_emit_error = None

        # import utils.http here, so that the NeonLogger is set as
        # the default logger before utils.http loads its logger. If
        # imported earlier, then the log_n function is not available
        # to utils.http Yes, this is a circular dependency, but lets
        # live with this for now !
        import utils.http
        self.request_pool = utils.http.RequestPool(5)

        self.logging_thread = utils.sync.IOLoopThread(
            name='logs{%s}' % self.__class__)
        self.logging_thread.daemon = True
        self.logging_thread.start()

    def __del__(self):
        self.logging_thread.stop()

    def get_verbose_dict(self, record):
        '''Returns a verbose dictionary of the record.'''
        retval = copy.copy(record.__dict__)
        retval['message'] = record.getMessage()
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

    @tornado.gen.coroutine
    def _emit_in_thread(self, record):
        '''Emits a log event in the logging_thread.'''
        response = yield self.request_pool.send_request(
            self.generate_request(record),
            do_logging=False,
            ntries=1,
            async=True)
        if response.error:
            try:
                raise response.error
            except:
                curtime = datetime.datetime.utcnow()
                statemon.state.increment(ref=_http_log_error_ref,
                                         safe=False)
                if (self.last_emit_error is None or 
                    (curtime - self.last_emit_error).total_seconds() >
                    self.emit_error_sampling_period):
                    self.last_emit_error = curtime
                    self.handleError(record)
        else:
            raise tornado.gen.Return(response)

    def emit(self, record):
        try:
            # Send it off to the logging thread so that we don't block here
            self.logging_thread.io_loop.add_callback(self._emit_in_thread,
                                                     record)
        except:
            curtime = datetime.datetime.utcnow()
            if (self.last_emit_error is None or 
                (curtime - self.last_emit_error).total_seconds() > 
                self.emit_error_sampling_period):
                self.last_emit_error = curtime
                self.handleError(record)

class LogglyHandler(TornadoHTTPHandler):
    '''
    Class that can send the records to loggly.
    '''
    def __init__(self, tag):
        super(LogglyHandler, self).__init__(
            '%s/tag/%s' % (options.loggly_base_url, tag))

    def generate_request(self, record):
        vdict = self.get_verbose_dict(record)

        data = {
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'host': platform.node(),
            'message': vdict['message'],
            'levelname': vdict['levelname'],
            'name': vdict['name'],
            'filename': vdict['filename'],
            'lineno': vdict['lineno']
            }
            
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

class NeonLogger(logging.Logger):
    '''A python logger with some extra functionality.'''
    def __init__(self, *args, **kwargs):
        super(NeonLogger, self).__init__(*args, **kwargs)
        self.sample_counters = {} # (filename, lineno) -> counter

    def reset_sample_counters(self):
        self.sample_counters = {}

    def findCaller(self):
        '''Overwrite the function to find the caller because the one in the
        logging module only skips stack frames in its own file
        '''
        f = currentframe()
        #On some versions of IronPython, currentframe() returns None if
        #IronPython isn't run with -X:Frames.
        if f is not None:
            f = f.f_back
        rv = "(unknown file)", 0, "(unknown function)"
        while hasattr(f, "f_code"):
            code = f.f_code
            filename = os.path.normcase(code.co_filename)
            if filename in [_srcfile, logging._srcfile]:
                f = f.f_back
                continue
            rv = (code.co_filename, f.f_lineno, code.co_name)
            break
        return rv

    def debug_n(self, msg, n=10, *args, **kwargs):
        '''Log every nth message.'''
        self.log_n(logging.DEBUG, msg, n, *args, **kwargs)

    def info_n(self, msg, n=10, *args, **kwargs):
        '''Log every nth message.'''
        self.log_n(logging.INFO, msg, n, *args, **kwargs)

    def warning_n(self, msg, n=10, *args, **kwargs):
        '''Log every nth message.'''
        self.log_n(logging.WARNING, msg, n, *args, **kwargs)
    warn_n = warning_n

    def error_n(self, msg, n=10, *args, **kwargs):
        '''Log every nth message.'''
        self.log_n(logging.ERROR, msg, n, *args, **kwargs)

    def exception_n(self, msg, n=10, *args, **kwargs):
        '''Log every nth message.'''
        self.error_n(msg, n, exc_info=1, *args, **kwargs)

    def critical_n(self, msg, n=10, *args, **kwargs):
        '''Log every nth message.'''
        self.log_n(logging.CRITICAL, msg, n, *args, **kwargs)
    fatal_n = critical_n

    def log_n(self, level, msg, n=10, *args, **kwargs):
        '''Log every nth message.'''
        filename, lno, func = self.findCaller()
        key = (filename, lno)
        cur_val = self.sample_counters.get(key, 0)
        self.sample_counters[key] = cur_val + 1
        if cur_val % n == 0:
            self.log(level, msg, *args, **kwargs)
logging.setLoggerClass(NeonLogger)

def str2level(s):
    '''Converts a string to a logging level.'''
    d = {'debug': logging.DEBUG,
         'info' : logging.INFO,
         'warn' : logging.WARNING,
         'warning' : logging.WARNING,
         'error' : logging.ERROR,
         'critical' : logging.CRITICAL}

    return d[s.lower().strip()]
