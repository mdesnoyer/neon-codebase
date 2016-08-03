#!/usr/bin/env python
'''Routine that creates a definition of the model and saves it to a file.'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import model
import model.features
import model.filters as filters
import model.predictor
import model.video_searcher
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
                                  filters.CrossFadeFilter(max_height=480)],
                                  max_height=480)
    haarF = '/data/model_data/haar_cascades/haarcascade_frontalface_alt2.xml'
    svmF = '/data/model_data/svm_pca/SVMw20'
    pcaF = '/data/model_data/pca/pca'
    CEC = filters.ClosedEyesFilter(haarFile=haarF, svmPkl=svmF, pcaPkl=pcaF, maxFaces=15)
    filt.append(CEC)

    predictor = model.predictor.KFlannPredictor(feature_generator, k=9)

    video_searcher = model.video_searcher.BisectSearcher(
        predictor,
        filt,
        filter_dups=True,
        startend_buffer=0.1,
        max_startend_buffer=5.0,
        thumb_min_dist=0.1,
        max_thumb_min_dist=10.0,
        processing_time_ratio=1.2,
        gist_threshold = 0.01,
        colorname_threshold = 0.015)

    mod = model.Model(predictor, filt, video_searcher)

    model.save_model(mod, options.output)
