#!/usr/bin/env python
'''A script that replaces a disk cache with a memory one in a model definition.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import model
import model.features
from optparse import OptionParser

_log = logging.getLogger(__name__)

def remove_disk_cache(obj):
    '''Recusively removes disk caches from the object.'''
    for name, val in obj.__dict__.items():
        if isinstance(val, model.features.DiskCachedFeatures):
            obj.__dict__[name] = (
                model.features.MemCachedFeatures.create_shared_cache(
                val.feature_generator))
        else:
            try:
                remove_disk_cache(val)
            except AttributeError:
                pass

    return obj
    

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--output', '-o', default='neon.model',
                      help='File to output the model definition')
    parser.add_option('--input', '-i', default='neon.model',
                      help='File to input the model definition')
    
    options, args = parser.parse_args()

    model.save_model(
        remove_disk_cache(model.load_model(options.input)),
        options.output)
