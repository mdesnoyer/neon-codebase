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

    parser.add_option('--output', '-o', default='neon.model',
                      help='File to output the model definition')
    
    options, args = parser.parse_args()

    feature_generator = model.features.DiskCachedFeatures(
        model.features.GistGenerator(),
        '/data/neon/cache')
    filt = filters.CascadeFilter([filters.BlurryFilter(),
                                  filters.UniformColorFilter(),
                                  filters.TextFilter(0.025),
                                  filters.CrossFadeFilter(max_height=480)],
                                  max_height=480)
    predictor = model.predictor.KFlannPredictor(feature_generator, k=3)

    mod = model.Model(predictor, filt)

    model.save_model(mod, options.output)
