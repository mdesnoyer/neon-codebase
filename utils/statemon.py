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

    def define(self, name, typ, default=None, stack_depth=2):
        '''Define a new monitoring variable

        Inputs:
        name - Name of the variable
        typ - Type of object to expect
        default - Default value for the parameter. If not set, takes the 
                  type's default value
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
        if default is not None:
            self._vars[global_name].value = default

    def increment(self, name=None, diff=1, ref=None, safe=True,
                  stack_depth=1):
        '''Increments the state variable

        Inputs:
        name - Name of the variable (Either this or ref must be set)
        diff - Amount to increment
        ref - Reference to the variable so that the lookup is skipped. Use get_ref() to get it.
        safe - If True, the increment is done with a thread lock.
               If it's ok to miss some increments in order to speed up the
               increment, this can be set to false.
        stack_depth - Stack depth to your module
        '''
        if not ((name is None) ^ (ref is None)):
            raise TypeError('Exactly one of name or ref must be given.')

        if ref is None:
            ref = self.get_ref(name, stack_depth+1)

        if safe:
            with ref.get_lock():
                ref.value += diff
        else:
            ref.value += diff

    def decrement(self, name=None, diff=1, ref=None, safe=True,
                  stack_depth=1):
        '''Decrements the state variable.'''
        self.increment(name, -diff, ref, safe, stack_depth+1)

    def define_and_increment(self, name, diff=1, typ=int, safe=True,
                             stack_depth=1):
        '''Increments a variable, but if it doesn't exist, it is created.

        Inputs:
        name - Name of the variable
        typ - Type of variable it should be
        diff - Amount to increment
        safe - If True, the increment is done with a thread lock.
        stack_depth - The stack depth to your module
        '''
        global_name = self._local2global(name, stack_depth=stack_depth+1)

        if global_name not in self._vars:
            with self._lock:
                self.define(name, typ, stack_depth=stack_depth+2)

        ref = self._vars[global_name]
        self.increment(ref=ref, diff=diff, safe=safe)
    
    def get_all_variables(self):
        ''' return dict of all variables being monitored '''
        return self._vars

    def get_ref(self, name, stack_depth=1):
        ''' Returns a reference of the variable.

        This is useful because the lookup requires inspection, which
        is slow if it happens a lot. So, if the variable is in a tight
        loop, you can keep this reference around.
        '''
        global_name = self._local2global(name, stack_depth+1)

        return self._vars[global_name]

    def _reset_values(self):
        '''Reset all the values in the state object.

        This should only be used for unittesting. Doesn't pay
        attention to the default value from the define function at
        this time.
        '''
        
        with self._lock:
            for value in self._vars.itervalues():
                with value.get_lock():
                    try:
                        value.value = 0
                    except TypeError:
                        value.value = 0.0

state = State()
'''Global state variable object'''

def define(name, typ, default=None):
    return state.define(name, typ, default=default, stack_depth=3)
            
        
