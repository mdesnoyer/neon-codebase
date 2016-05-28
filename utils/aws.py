'''
Utilities for interacting with aws generally.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016
'''

import boto.utils

_az = 'unknown'
def get_current_az():
    '''Returns the availability zone of this machine.'''
    if _az is 'unknown':
        meta = boto.utils.get_instance_metadata()
        try:
            _az = meta['placement']['availability-zone']
        except KeyError as e:
            _az = None
    return _az
