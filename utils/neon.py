'''
Stuff to set up the Neon environment.

In your __main__ routine, run InitNeon() and everything will be
magically setup.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

import contextlib
import logging
import os
import rpdb2
import signal
import socket
import tornado.httpclient
import threading

from . import logs
import monitor
from . import options

def InitNeon(usage='%prog [options]'):
    '''Perform the initialization for the Neon environment.

    Returns the leftover arguments
    '''
    garb, args = options.parse_options(usage=usage)
    logs.AddConfiguredLogger()
    EnableRunningDebugging()

    socket.setdefaulttimeout(30)
    if os.path.exists('/etc/ssl/certs/ca-certificates.crt'):
        tornado.httpclient.AsyncHTTPClient.configure(None, defaults=dict(
            ca_certs="/etc/ssl/certs/ca-certificates.crt"))

    magent = monitor.MonitoringAgent()
    magent.start()
    return args

def InitNeonTest():
    '''Perform the initialization for the Neon unittest environment.

    In particular, this silences all the logs.
    '''
    garb, args = options.parse_options()

    # Remove all the loggers that some sub libraries may have setup
    for mod, logger in logging.Logger.manager.loggerDict.iteritems():
        logger.handlers = []

    # Make a new logger that dumps stuff to /dev/null in case some sub
    # library tries to re-enable logging.
    logs.CreateLogger(logfile='/dev/null')

    logging.captureWarnings(True)

    EnableRunningDebugging()

    return args

def WritePid(pidfile):
    '''Write the pid of the current process to the pidfile
    '''
    pid = str(os.getpid())
    file(pidfile, 'w').write(pid)

def EnableRunningDebugging():
    '''This adds a signal handler on SIGUSR2 that can be used to enable
    remote debugging on a running process.

    The remote debugging is done using winpdb and will listen somewhere on
    ports 51000 -> 51024.
    '''
    signal.signal(signal.SIGUSR2,
                  lambda sig, frame: rpdb2.start_embedded_debugger(
                      'neon',
                      fAllowRemote = True))

@contextlib.contextmanager
def set_env(**environ):
    '''
    Temporarily set the process environment variables.

    >>> with set_env(PLUGINS_DIR=u'test/plugins'):

    :type environ: dict[str, unicode]
    :param environ: Environment variables to set
    '''
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
