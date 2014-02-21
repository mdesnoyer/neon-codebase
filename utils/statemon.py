'''
A module to be able to monitor the state of a process using variables.

A module using this functionality would define its own monitoring
variables like:

  from utils import statemon

  statemon.define("q_size", int)

  def handle_val(val):
    statemon.state.q_size = 6

The valid types of variables are: int, float

All of the varoab;es are namespaced by module name. So, if the above
was in the module mastermind.core, the variable would have a full name
of mastermind.core.q_size.

The variables are visible from other processes if the process was
started using multiprocessing. To view the variable value, simply:

  statemon.state.get('mastermind.core.q_size')

When incrementing or decrementing, you should use the increment() or
decrement() functions instead of += because it is thread safe.

TODO(mdesnoyer): Have the variables be visible from an outside monitoring tool

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs

'''
import inspect
import multiprocessing
import os.path

class Error(Exception):
    """Exception raised by errors in the statemon module."""
    pass

class State(object):
    '''A collection of state variables.'''
    def __init__(self):
        self._lock = multiprocessing.RLock()
        self._vars = {}
        self._enable_reset = False #default 

        # Find the root directory of the source tree
        cur_dir = os.path.abspath(os.path.dirname(__file__))
        while cur_dir <> '/':
            if os.path.exists(os.path.join(cur_dir, 'NEON_ROOT')):
                self._NEON_ROOT = cur_dir
                break
            cur_dir = os.path.abspath(os.path.join(cur_dir, '..'))

        if cur_dir == '/':
            raise Error('Could not find the NEON_ROOT file in the source tree.')

    def __getattr__(self, name):
        if name.startswith('_'):
            return self.__dict__[name]
        global_name = self._local2global(name)
        return self.get(global_name)

    def get(self, global_name):
        with self._lock:
            try:
                return self._vars[global_name].value
            except KeyError:
                raise AttributeError("Unrecognized option %r" % global_name)

    def __getitem__(self, name):
        with self._lock:
            global_name = self._local2global(name)
            return self._vars[global_name].value

    def __setattr__(self, name, value):
        if name.startswith('_'):
            self.__dict__[name] = value
            return 
        
        with self._lock:
            global_name = self._local2global(name)
            try:
                self._vars[global_name].value = value
            except KeyError:
                raise AttributeError("Unrecognized variable %r" % global_name)

    def _local2global(self, local, stack_depth=2):
        '''Converts the local name of the variable to a global one.

        e.g. if define("font", ...) is in utils.py, this returns "utils.font"

        Stack depth controls how far back the module is found.
        Normally this is 2.
        '''
        
        frame = inspect.currentframe()
        for i in range(stack_depth):
                frame = frame.f_back
        mod = inspect.getmodule(frame)

        apath = os.path.abspath(mod.__file__)
        relpath = os.path.relpath(apath, self._NEON_ROOT)
        relpath = os.path.splitext(relpath)[0]
        prefix = '.'.join(relpath.split('/'))

        return '%s.%s' % (prefix, local)

    def define(self, name, typ, stack_depth=2):
        '''Define a new monitoring variable

        Inputs:
        name - Name of the variable
        typ - Type of object to expect
        stack_depth - Stack depth to your module
        '''

        global_name = self._local2global(name, stack_depth=stack_depth)
        
        if global_name in self._vars:
            # It's redefined, so ignore
            return

        typech = None
        if typ == int:
            typech = 'i'
        elif typ == float:
            typech = 'f'
        else:
            raise Error('Invalid type: %s' % typ)

        self._vars[global_name] = multiprocessing.Value(typech)

    def increment(self, name, diff=1):
        '''Increments the state variable safely.'''
        global_name = self._local2global(name)

        with self._vars[global_name].get_lock():
            self._vars[global_name].value += diff

    def decrement(self, name, diff=1):
        '''Decrements the state variable safely.'''
        global_name = self._local2global(name)

        with self._vars[global_name].get_lock():
            self._vars[global_name].value -= diff
    
    def get_all_variables(self):
        ''' return dict of all variables being monitored '''
        return self._vars
   
    def enable_reset(self):
        ''' Enable reset of state variables '''
        self._enable_reset = True

    def reset(self, global_name):
        ''' reset the state variable '''
        if self._enable_reset:
            with self._vars[global_name].get_lock():
                self._vars[global_name].value = 0 

state = State()
'''Global state variable object'''

def define(name, typ):
    return state.define(name, typ, stack_depth=3)
            
        
