'''
Utilities for interacting with aws generally.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016
'''

import boto.utils
import utils.obj

class InstanceMetadata(object):
    __metaclass__ = utils.obj.Singleton

    def __init__(self):
        self.meta = None

    def _get_metadata(self):
        if self.meta is None:
            self.meta = boto.utils.get_instance_metadata()

    
    def get_current_az(self):
        '''Returns the availability zone of this machine.'''
        self._get_metadata()
        try:
            az = self.meta['placement']['availability-zone']
        except KeyError as e:
            az = None

        return az
