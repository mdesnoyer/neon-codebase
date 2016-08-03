#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
from cmsdb import neondata
import utils

if __name__ == "__main__":
    utils.neon.InitNeon()
    model_name = '20160713-aquilav2'

    f = open('feature_names2.csv', 'r')
    for line in f:
        splits = line.split(',')
        name = splits[1].rstrip()
        if len(name) > 1:
            def modify_feature(x):
                x.name = name

            fkey = neondata.Feature.create_key(model_name, splits[0])
            feature = neondata.Feature.modify(fkey, modify_feature)
