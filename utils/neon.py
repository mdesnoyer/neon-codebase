'''
Stuff to set up the Neon environment.

In your __main__ routine, run InitNeon() and everything will be
magically setup.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

from . import logs
from . import options

def InitNeon():
    '''Perform the initialization for the Neon environment.'''
    options.parse_options()
    logs.AddConfiguredLogger()
