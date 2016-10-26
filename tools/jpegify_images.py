#!/usr/bin/env python
'''
Script that converts images to JPEGs if they were not stored that way

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs Inc.
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
from PIL import Image
import utils.neon
from utils.options import options, define

_log = logging.getLogger(__name__)
define('input', default=None,
       help='Input file with image filenames, one per line')

def main():
    valid_filenames = []
    with open(options.input) as f:
        for line in f:
            fn = line.strip()
            if fn:
                try:
                    im = Image.open(fn)
                    im.load()
                    if im.format != 'JPEG':
                        _log.info('Rewriting %s because it was %s' % 
                                  (fn, im.format))
                        im.load()
                        im = im.convert('RGB')
                        im.save(fn, 'jpeg', quality=90)
                    valid_filenames.append(fn)
                except IOError as e:
                    _log.info('File %s is invalid' % fn)
                    pass

    with open(options.input, 'w') as f:
        f.write('\n'.join(valid_filenames))

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
