'''ps like utilities

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

from contextlib import contextmanager
import logging
import os
import platform
import multiprocessing
import signal
import subprocess
import threading
import time
import tornado.ioloop

_log = logging.getLogger(__name__)    

def get_child_pids():
    '''Returns a list of pids for child processes.'''
    if platform.system() == 'Linux':
        ps_command = subprocess.Popen(
            "ps -o pid --ppid %d --noheaders" % os.getpid(),
            shell=True,
            stdout=subprocess.PIPE)

    elif platform.system() == 'Darwin':
        ps_command = subprocess.Popen(
            'ps -o pid,ppid -ax | grep %s | cut -f 1 -d " " | tail -1' %
            os.getpid(),
            shell=True,
            stdout=subprocess.PIPE)
    else:
        raise NotImplementedException("This only works on Mac or Linux")

    children = []
    for line in ps_command.stdout:
        children.append(int(line))

    retcode = ps_command.wait()
    assert retcode == 0, "ps command returned %d" % retcode

    try:
        children.remove(ps_command.pid)
    except ValueError:
        pass

    return children

def pid_running(pid):
    '''Returns true if the pid is running in unix.'''
    try:
        return os.waitpid(pid, os.WNOHANG) == (0,0)    
    except OSError:
        return False

def send_signal_and_wait(sig, pids, timeout=20):
    '''Sends a given signal to all the pids and waits until they finish.

    sig - The signal to send
    pids - List of pids to send the signals to
    timeout - Timeout in seconds

    Returns - True if any process is still running
    '''
    for pid in pids:
        try:
            os.kill(pid, sig)
        except OSError as e:
            if e.errno <> 3:
                # Process is already dead
                raise

    still_running = True
    deadline = time.time() + timeout
    while time.time() < deadline and still_running:
        still_running = False
        for pid in pids:
            if pid_running(pid):
                still_running = True
        time.sleep(0.1)

    return still_running
        

def shutdown_children():
    '''Shuts down the children of the current process.

    The easiest way to use is to just register it to run at exit like:
    
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    '''
    
    print 'Shutting down children of pid %i' % os.getpid()
    child_pids = get_child_pids()
    still_running = send_signal_and_wait(signal.SIGTERM, child_pids,
                                         timeout=5)

    # Some stupid processes ignore SIGTERM, but accept SIGINT e.g. webrick
    # So send SIGINTs
    if still_running:
        still_running = send_signal_and_wait(signal.SIGINT, child_pids,
                                             timeout=20)

    # Ok, we're done waiting. Bring out the big guns
    if still_running:
        for pid in child_pids:
            if pid_running(pid):
                print 'Process %i not down. Killing' % pid
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
    print 'Done killing children'

def register_tornado_shutdown(server):
    '''Registers a clean shutdown proceedure for a tornado server.

    Hooks onto SIGINT and SIGTERM.

    Inputs:
    server - http server instance
    '''
    try:
        cur_ioloop = tornado.ioloop.IOLoop.current()
    except ValueError:
        cur_ioloop = tornado.ioloop.IOLoop.instance()
    
    def shutdown():
        server.stop()

        deadline = time.time() + 3

        def stop_loop():
            now = time.time()
            if now < deadline and (cur_ioloop._callbacks or
                                   cur_ioloop._timeouts):
                cur_ioloop.add_timeout(now + 1, stop_loop)
                
            else:
                cur_ioloop.stop()
                _log.info('Shutdown')
        stop_loop()
        
    def sighandler(sig, frame):
        _log.warn('Received signal %s. Shutting down pid %i' % (sig,
                                                                os.getpid()))
        cur_ioloop.add_callback_from_signal(shutdown)

    try:
        signal.signal(signal.SIGINT, sighandler)
        signal.signal(signal.SIGTERM, sighandler)
    except ValueError as e:
        _log.warning('Can only register signal in the main thread. Skipping. '
                     '%s' % e)
        
class ActivityWatcher:
    '''A multiprocess shared object to keep track of when a process is active.

    To use it, in your process:
    with activity_counter.activate():
      do stuff

    Then, another process can call the other functions to find out if
    that process is in the activate() block.
    
    '''
    def __init__(self):
        self.value = multiprocessing.Value('i', 0)
        self.condition = multiprocessing.Condition()
    
    @contextmanager
    def activate(self):
        '''A context manager to signal when an activity is happening.'''
        with self.value.get_lock():
            self.value.value += 1
            _log.debug('Entering block for pid %i. val: %i' % 
                       (os.getpid(), self.value.value))
            with self.condition:
                self.condition.notify_all()

        try:
            yield

        finally:

            with self.value.get_lock():
                self.value.value -= 1
                _log.debug('Exited block for pid %i. val: %i' % 
                           (os.getpid(), self.value.value))
                with self.condition:
                    self.condition.notify_all()


    def is_active(self):
        '''Returns true if an activity is going on.'''
        return self.value.value > 0

    def is_idle(self):
        '''Returns true if the process is idle.'''
        return not self.is_active()

    def wait_for_idle(self):
        '''Join until the process is idle.'''
        while self.is_active():
            with self.condition:
                self.condition.wait()

    def wait_for_active(self):
        '''Join until the process is active.'''
        while self.is_idle():
            with self.condition:
                self.condition.wait()
