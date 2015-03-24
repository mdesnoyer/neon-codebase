#!/usr/bin/env python
'''A script that creates precision recall curves for getty in the simplest way

Usage: ./plot_pr_curves.py [options] <input_file>

Input is a pickled pandas DataFrame where each row is a different image and it
is indexed by timestamp (or index) of the image. Then the columns are filename,
chosen, model_scores

Copyright: 2015 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas
import utils.neon
from utils.options import define, options

define('dataset', default='', help='Name of the dataset')

_log = logging.getLogger(__name__)

if __name__ == '__main__':
    args = utils.neon.InitNeon('%prog [options] <input_file>')

    data = pandas.read_pickle(args[0])

    plt.figure(1)
    for mod in data.columns:
        if mod in ['chosen', 'filename']:
            continue
        sorted_images = data.sort(mod, ascending=False)

        chosen_count = sorted_images['chosen'].cumsum().reset_index()
        
        precision = chosen_count / range(1, len(chosen_count)+1)
        recall = chosen_count / sorted_images['chosen'].sum()

        plt.plot(recall, precision, label=mod)

    plt.legend()
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title('PR Curve for %s' % options.dataset)
    plt.show()
