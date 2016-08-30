#!/usr/bin/env python
'''Routine that creates a definition of the model and saves it to a file.'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import dlib
import logging
import model
import model.clip_finder
import model.features
import model.filters
import model.local_video_searcher
from model.local_video_searcher import (MINIMIZE, MAXIMIZE,
                                        NORMALIZE, PEN_LOW_HALF,
                                        PEN_HIGH_HALF,
                                        PEN_ZERO)
from model.parse_faces import MultiStageFaceParser 
import model.predictor
from model.score_eyes import ScoreEyes
from optparse import OptionParser
import pickle
import scenedetect.detectors

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--output', '-o', default='neon.model',
                      help='File to output the model definition')
    
    options, args = parser.parse_args()

    
    face_predictor = dlib.shape_predictor(os.path.join(
        os.path.dirname(__file__), '..', '..', 'model_data',
        'shape_predictor_68_face_landmarks.dat'))
    face_finder = MultiStageFaceParser(face_predictor)

    eye_classifier = pickle.load(open(os.path.join(
        os.path.dirname(__file__), '..', '..', 'model_data',
        'eye_classifier.pkl'), 'rb'))
    # The classifier was built using an older version of scikit learn
    # and it's incompatible
    if 'std_' in eye_classifier.scaler.__dict__:
        print('The eye classifier is from an old version of scikit learn')
        eye_classifier.scaler.scale_ = eye_classifier.scaler.__dict__['std_']

    pix_gen = model.features.PixelVarGenerator()
    sad_gen = model.features.SADGenerator()
    #text_gen = model.features.TextGeneratorSlow()
    face_gen = model.features.FaceGenerator(face_finder)
    eye_gen = model.features.ClosedEyeGenerator(face_finder, eye_classifier)
    vibrance_gen = model.features.VibranceGenerator()
    blur_gen = model.features.BlurGenerator()
    ent_gen = model.features.EntropyGenerator()
    face_blur_gen = model.features.FacialBlurGenerator(face_finder)
    sat_gen = model.features.SaturationGenerator()
    bright_gent = model.features.BrightnessGenerator()

    filters = [
        model.filters.SceneChangeFilter(),
        model.filters.ThreshFilt(80, 'pixvar'),
        model.filters.FaceFilter(),
        model.filters.EyeFilter()]

    feature_stuff = dict()
    feature_stuff['pixvar'] = {'generator':pix_gen, 'cache':True,
                               'valence': MAXIMIZE, 'weight': 1.0,
                               'penalty':0.2, 'dependencies':[]}
    feature_stuff['blur'] = {'generator':blur_gen, 'cache':True,
                               'valence': MAXIMIZE, 'weight': 1.0,
                               'penalty':0.2,
                               'dependencies':[['faces', lambda x: x < 1]]}
    feature_stuff['sad'] = {'generator':sad_gen, 'cache':True,
                               'valence': MINIMIZE, 'weight': 2.0,
                               'penalty':0.25, 'dependencies':[]}
    feature_stuff['faces'] = {'generator':face_gen, 'cache':True,
                               'valence': PEN_ZERO, 'weight': 1.0,
                               'penalty':0.15, 'dependencies':[]}
    feature_stuff['eyes'] = {'generator':eye_gen, 'cache':True,
                               'valence': MAXIMIZE, 'weight': 2.0,
                               'penalty':0.3,
                               'dependencies':[['faces', lambda x: x > 0]]}
    feature_stuff['vibrance'] = {'generator':vibrance_gen, 'cache':True,
                               'valence': MAXIMIZE, 'weight': 1.0,
                               'penalty':0.2, 'dependencies':[]}
    feature_stuff['brightness'] = {'generator':bright_gent, 'cache':True,
                               'valence': PEN_LOW_HALF, 'weight': 1.0,
                               'penalty':0.1, 'dependencies':[]}
    feature_stuff['saturation'] = {'generator':sat_gen, 'cache':True,
                               'valence': MAXIMIZE, 'weight': 1.0,
                               'penalty':0.1, 'dependencies':[]}
    feature_stuff['entropy'] = {'generator':ent_gen, 'cache':True,
                               'valence': MAXIMIZE, 'weight': 1.0,
                               'penalty':0.25, 'dependencies':[]}
    feature_stuff['face_blur'] = {'generator':face_blur_gen, 'cache':True,
                               'valence': MAXIMIZE, 'weight': 1.0,
                               'penalty':0.3,
                               'dependencies':[['faces', lambda x: x > 0]]}

    feats_to_use = ['pixvar', 'blur', 'sad', 'faces', 'eyes', 'brightness',
                    'vibrance', 'entropy', 'face_blur']

    feature_generators = [feature_stuff[x]['generator'] for x in feats_to_use]
    weight_valence = {x:feature_stuff[x]['valence'] for x in feats_to_use}
    feats_to_cache = {x:feature_stuff[x]['cache'] for x in feats_to_use}
    weight_dict = {x:feature_stuff[x]['weight'] for x in feats_to_use}
    penalties = {x:feature_stuff[x]['penalty'] for x in feats_to_use}
    dependencies = {x:feature_stuff[x]['dependencies'] for x in feats_to_use}

    combiner = model.local_video_searcher.MultiplicativeCombiner(
        penalties=penalties,
        weight_valence=weight_valence,
        dependencies=dependencies)
        
    video_searcher = model.local_video_searcher.LocalSearcher(
        None,
        feature_generators=feature_generators,
        combiner=combiner,
        filters=filters,
        feats_to_cache=feats_to_cache,
        testing=False,
        feat_score_weight=0.5,
        local_search_width=32,
        local_search_step=2,
        processing_time_ratio=2.0,
        adapt_improve=True,
        use_best_data=True,
        use_all_data=False,
        testing_dir='/tmp',
        n_thumbs=6,
        startend_clip=0.025)

    clip_finder = model.clip_finder.ClipFinder(
        None,
        scenedetect.detectors.ContentDetector(30.0),
        model.features.ObjectActionGenerator(),
        valence_weight=1.0,
        action_weight=0.25,
        custom_weight=0.5,
        processing_time_ratio=0.7,
        startend_clip=0.1,
        cross_scene_boundary=True,
        min_scene_piece=15)

    mod = model.Model(None, vid_searcher=video_searcher,
                      clip_finder=clip_finder)

    model.save_model(mod, options.output)
