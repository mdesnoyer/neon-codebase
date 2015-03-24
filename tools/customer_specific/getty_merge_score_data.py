#!/usr/bin/env python
'''A script that merges score data into a pandas DataFrame

Usage: ./getty_merge_score_data.py [options] <output_file>

Input is a csv of <image_file>,<score> one for chosen thumbs and
another for rejected thumbs.

Output is a pickled pandas DataFrame where each row is a different image and it
is indexed by timestamp of the image. Then the columns are filename,
chosen, model_scores

Copyright: 2015 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas
import utils.neon
from utils.options import define, options

define('chosen_input', default=None, help='CSV file of chosen examples')
define('rejected_intput', default=None, help='CSV file of rejected examples')
define('merge_existing', default=0,
       help='If 1, then if the output file exists, add to that DataFrame')
define('dataset', default='', help='Name of the dataset')

_log = logging.getLogger(__name__)

if __name__ == '__main__':
    args = utils.neon.InitNeon('%prog [options] <output_file>')

    output_file = args[0]

    if os.path.exists(output_file) and options.merge_existing:
        data = pandas.read_pickle(output_file)
    else:
        data = pandas.DataFrame()

    
