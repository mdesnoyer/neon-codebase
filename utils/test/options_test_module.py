'''A module needed for testing the namespacing in options.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

def define(parser, *args, **kwargs):
    parser.define(*args, **kwargs)

def get(parser, name):
    return parser.__getattr__(name)
