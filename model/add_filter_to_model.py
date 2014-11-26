#!/usr/bin/env python
'''Routine that creates a definition of the model and saves it to a file.'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import model
import model.features
import model.filters as filters
import model.predictor
from optparse import OptionParser

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--input', '-i',default='',help='File to load model definition')
    parser.add_option('--output', '-o', default='neon.model',
                      help='File to output the model definition')

    options, args = parser.parse_args()


    mod = model.load_model(options.input)

    mod.filt.filters.append(filters.DeltaStdDevFilter())

    #mod.filt = filters.CascadeFilter([filters.BlurryFilter(),
    #                              filters.UniformColorFilter(),
    #                              filters.TextFilter(),
    #                              filters.DeltaStdDevFilter()],
    #    max_height=480)

    model.save_model(mod, options.output)
