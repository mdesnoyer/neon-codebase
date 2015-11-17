#!/usr/bin/env python
'''Creates a model using the local video searcher, which modifies the
structure of the frame extraction slightly: the model no longer performs
filtering, but the filters are passed directly to the searcher.

    REQUIRES:
        - A landmark file datafile, for facial landmark identification. The
          current implementation requires 68 landmark points.
        - A classifier, which classifies eyes are either open or closed. This
          is a scikit-learn type model, stored in a compressed format via the
          joblib library.
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import model
import model.features as features
import model.filters as filters
import model.predictor
from model.parse_faces import MultiStageFaceParser 
from model.score_eyes import ScoreEyes
import dlib
from sklearn.externals import joblib
from local_video_searcher import (LocalSearcher, Combiner, MINIMIZE, MAXIMIZE,
                                    NORMALIZE, PEN_LOW_HALF, PEN_HIGH_HALF)

from optparse import OptionParser

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--landmark_file', '-l',
                      help='The file storing facial landmark data')
    parser.add_option('--eye_classifier', '-e', default='eye_linear_model',
                      help=('The stored classifier file. Must be stored '
                            'as a compressed file via the scikit-learn '
                            'joblib function; must have as an attribute '
                            'a scaler that can scale the data appropriately'))
    parser.add_option('--feat_score_weight', '-fs', default=0.0,
                      help=('The combined feature score weight, which is used'
                            ' to compute the final combined score.'))
    parser.add_option('--output', '-o', default='neon.model',
                      help='File to output the model definition')
    
    options, args = parser.parse_args()

    # load the face segmentation
    f_predictor = dlib.shape_predictor(options.landmark_file)
    face_finder = MultiStageFaceParser(f_predictor)

    # load the eye classifier
    classifier = joblib.load(parser.eye_classifier)
    eye_scorer = ScoreEyes(classifier)

    # instantiate the filters
    pix_filt = filters.ThreshFilt(thresh=200)
    scene_filt = filters.SceneChangeFilter()
    face_filt = filters.FaceFilter()
    eye_filt = filters.EyeFilter()
    filters = [scene_filt, pix_filt, face_filt, eye_filt]

    # instantiate the feature generators
    pix_gen = features.PixelVarGenerator()
    sad_gen = features.SADGenerator()
    text_gen = features.TextGenerator()
    face_gen = features.FaceGenerator(face_finder)
    eye_gen = features.ClosedEyeGenerator(face_finder, classifier)
    vibrance_gen = features.VibranceGenerator()

    feature_generators = [pix_gen, sad_gen, text_gen, face_gen, eye_gen,
                      vibrance_gen]
    feats_to_cache = ['pixvar', 'blur', 'sad', 'eyes', 'text', 'vibrance']

    # instantiate the combiner
    weight_valence = {'blur':MAXIMIZE, 'sad':MINIMIZE, 'eyes':MAXIMIZE,
                      'text':MINIMIZE, 'pixvar':NORMALIZE,
                      'vibrance':PEN_LOW_HALF}

    ## ADD WEIGHTS TO THIS IF YOU WANT TO ADJUST THE RELATIVE IMPORTANCE OF
    ## THE FEATURES. 
    ## Structure: {feat_name: weight} where feat_name is a string as is
    ## yielded by the local feature generators (get_feat_name) and the weight
    ## is a float.
    ## TODO: ADD THIS IN AS AN OPTION, ALTHOUGH THE FORMAT IS NOT CLEAR.
    weight_dict = ddict(lambda: 1.)
    combiner = Combiner(weight_valence=weight_valence,
                        weight_dict=weight_dict)


    # instantiate the predictor
    feature_generator = features.DiskCachedFeatures(
        model.features.GistGenerator(),
        '/data/neon/cache')
    predictor = model.predictor.KFlannPredictor(feature_generator, k=9)

    # instantiate filters -- there are no filters to be passed to the 
    # model in this case.
    filt = []

    # instantiate the video searcher
    video_searcher = LocalSearcher(predictor, face_finder, eye_scorer,
                                   feature_generators=feature_generators,
                                   combiner=combiner,
                                   filters=filters,
                                   feats_to_cache=feats_to_cache,
                                   feat_score_weight=options.feat_score_weight)

    mod = model.Model(predictor, filt, video_searcher)

    model.save_model(mod, options.output)
