'''A module needed for testing the namespacing in statemon.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')))

from utils import statemon

statemon.define('mod_int', int)

def run_increments(finished, kill):
    '''A function that can be run in a new process or thread to increment'''
    statemon.mod_int = 0

    for i in range(5):
        statemon.state.increment('mod_int')

    finished.set()
    kill.wait()
    statemon.mod_int = 0

def define(state, *args, **kwargs):
    state.define(*args, **kwargs)

def define_and_increment(state, *args, **kwargs):
    state.define_and_increment(*args, **kwargs)

def get(state, name):
    return state.__getattr__(name)

def set(state, name, value):
    state.__setattr__(name, value)
