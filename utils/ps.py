'''ps like utilities

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import logging
import os
import platform
import signal
import subprocess
import time

_log = logging.getLogger(__name__)

def reap_child(signal, frame):
    print 'reaping'

def get_child_pids():
    '''Returns a list of pids for child processes.'''
    if platform.system() == 'Linux':
        ps_command = subprocess.Popen(
            "ps -o pid --ppid %d --noheaders" % os.getpid(),
            shell=True,
            stdout=subprocess.PIPE)

    elif platform.system() == 'Darwin':
        ps_command = subprocess.Popen(
            "ps -o pid,ppid -ax | grep %s | cut -f 1 -d " " | tail -1" %
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

def shutdown_children():
    '''Shuts down the children of the current process.

    The easiest way to use is to just register it to run at exit like:
    
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    '''
    import os
    import time
    
    print 'Shutting down children of pid %i' % os.getpid()
    child_pids = get_child_pids()    
    for pid in child_pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError as e:
            if e.errno <> 3:
                raise

    still_running = True
    count = 0
    while still_running and count < 20:
        still_running = False
        for pid in child_pids:
            if pid_running(pid):
                still_running = True
        time.sleep(1)
        count += 1

    if still_running:
        for pid in child_pids:
            if pid_running(pid):
                print 'Process %i not down. Killing' % pid
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
    print 'Done killing children'
