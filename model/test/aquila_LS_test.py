# let's try instantiating the predictor
import sys
if '/repos/neon/model' not in sys.path:
    sys.path.insert(0, '/repos/neon/model')
from model.features import (BlurGenerator, SADGenerator, FaceGenerator,
                            ClosedEyeGenerator,
                            PixelVarGenerator, VibranceGenerator,
                            EntropyGenerator, FacialBlurGenerator,
                            BrightnessGenerator, SaturationGenerator)
from model import predictor
import cv2
import numpy as np
from cPickle import load
from model.parse_faces import MultiStageFaceParser
from model.score_eyes import ScoreEyes
import dlib
from sklearn.externals import joblib
from model.local_video_searcher import (LocalSearcher, MultiplicativeCombiner,
                                  AdditiveCombiner, MINIMIZE, MAXIMIZE,
                                  NORMALIZE, PEN_LOW_HALF, PEN_HIGH_HALF,
                                  PEN_ZERO)

from model.filters import (ThreshFilt, SceneChangeFilter, FaceFilter,
                           EyeFilter)
import logging
import ipdb

import random

from glob import glob
from cPickle import dumps
if '/usr/local/lib/python2.7/dist-packages/' not in sys.path:
    sys.path.insert(0, '/usr/local/lib/python2.7/dist-packages/')

from collections import defaultdict as ddict
import os

from time import time
import datetime

'''
=======================================================================
                            LOGGING
=======================================================================
'''

class _AnsiColorizer(object):
    """
    A colorizer is an object that loosely wraps around a stream, allowing
    callers to write text to the stream in a particular color.

    Colorizer classes must implement C{supported()} and C{write(text, color)}.
    """
    _colors = dict(black=30, red=31, green=32, yellow=33,
                   blue=34, magenta=35, cyan=36, white=37)

    def __init__(self, stream):
        self.stream = stream

    @classmethod
    def supported(cls, stream=sys.stdout):
        """
        A class method that returns True if the current platform supports
        coloring terminal output using this method. Returns False otherwise.
        """
        if not stream.isatty():
            return False  # auto color only on TTYs
        try:
            import curses
        except ImportError:
            return False
        else:
            try:
                try:
                    return curses.tigetnum("colors") > 2
                except curses.error:
                    curses.setupterm()
                    return curses.tigetnum("colors") > 2
            except:
                raise
                # guess false in case of error
                return False

    def write(self, text, color):
        """
        Write the given text to the stream in the given color.

        @param text: Text to be written to the stream.

        @param color: A string label for a color. e.g. 'red', 'white'.
        """
        color = self._colors[color]
        self.stream.write('\x1b[%s;1m%s\x1b[0m' % (color, text))


class ColorHandler(logging.StreamHandler):
    def __init__(self, stream=sys.stderr):
        super(ColorHandler, self).__init__(_AnsiColorizer(stream))

    def emit(self, record):
        msg_colors = {
            logging.DEBUG: "green",
            logging.INFO: "blue",
            logging.WARNING: "yellow",
            logging.ERROR: "red"
        }
        # import ipdb
        # ipdb.set_trace()
        color = msg_colors.get(record.levelno, "blue")
        msg = self.format(record)
        self.stream.write(msg + "\n", color)

#logging.getLogger().addHandler(ColorHandler())

ch = ColorHandler()
ch.setFormatter(logging.Formatter('[%(module)-10s][%(funcName)s] %(message)s'))
_log = logging.getLogger()
_log.setLevel(logging.DEBUG)
_log.handlers = []
_log.addHandler(ch)
'''
=======================================================================
                            ENDOF LOGGING
=======================================================================
'''
valence_translator = {MINIMIZE:'MINIMIZE', MAXIMIZE:'MAXIMIZE',
                      NORMALIZE:'NORMALIZE', PEN_LOW_HALF:'PEN_LOW_HALF',
                      PEN_HIGH_HALF:'PEN_HIGH_HALF', PEN_ZERO:'PEN_ZERO'}

_log.info('creating landmark detector')
f_predictor_path = '/home/ubuntu/shape_predictor_68_face_landmarks.dat'
f_predictor = dlib.shape_predictor(f_predictor_path)
_log.info('loading closed eye classifier')
classifier = joblib.load('/home/ubuntu/linear_model_scaler_compressed')
_log.info('creating face finder')
face_finder = MultiStageFaceParser(f_predictor)
_log.info('instantiating closed eye class')
eye_scorer = ScoreEyes(classifier)

# generate the filters
_log.info('generating filters')
pix_filt = ThreshFilt(thresh=80)
scene_filt = SceneChangeFilter()
face_filt = FaceFilter()
eye_filt = EyeFilter()

filters = [scene_filt, pix_filt, face_filt, eye_filt]

_log.info('generating feature generators')
pix_gen = PixelVarGenerator()
sad_gen = SADGenerator()
#text_gen = TextGeneratorSlow()
face_gen = FaceGenerator(face_finder)
eye_gen = ClosedEyeGenerator(face_finder, classifier)
vibrance_gen = VibranceGenerator()
blur_gen = BlurGenerator()
ent_gen = EntropyGenerator()
face_blur_gen = FacialBlurGenerator(face_finder)
sat_gen = SaturationGenerator()
bright_gent = BrightnessGenerator()

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
                           'penalty':0.5, 'dependencies':[]}
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


_log.info('Generating combiner')
feature_generators = [feature_stuff[x]['generator'] for x in feats_to_use]
weight_valence = {x:feature_stuff[x]['valence'] for x in feats_to_use}
feats_to_cache = {x:feature_stuff[x]['cache'] for x in feats_to_use}
weight_dict = {x:feature_stuff[x]['weight'] for x in feats_to_use}
penalties = {x:feature_stuff[x]['penalty'] for x in feats_to_use}
dependencies = {x:feature_stuff[x]['dependencies'] for x in feats_to_use}

combiner_m = MultiplicativeCombiner(penalties=penalties,
                                    weight_valence=weight_valence,
                                    dependencies=dependencies)

combiner_a = AdditiveCombiner(weight_valence=weight_valence,
                              weight_dict=weight_dict)

_log.info('creating predictor')
predictor = predictor.DeepnetPredictor(hostport='10.0.66.209:9000')
#f = open(os.path.join(dest_folder, 'config'), 'w')
def getLS(feature_generators, combiner, filters, feats_to_cache, testing,
            feat_score_weight, local_search_width, local_search_step,
            processing_time_ratio, adapt_improve, use_best_data,
            use_all_data, testing_dir, n_thumbs, startend_clip):
    return LocalSearcher(predictor,
                   feature_generators=feature_generators,
                   combiner=combiner_m,
                   filters=filters,
                   feats_to_cache=feats_to_cache,
                   testing=testing,
                   feat_score_weight=feat_score_weight,
                   local_search_width=local_search_width,
                   local_search_step=local_search_step,
                   processing_time_ratio=processing_time_ratio,
                   adapt_improve=adapt_improve,
                   use_best_data=use_best_data,
                   use_all_data=use_all_data,
                   testing_dir=testing_dir,
                   n_thumbs=n_thumbs,
                   startend_clip=startend_clip)

combiner = combiner_m
testing = False
feat_score_weight=3.0
local_search_width=32
local_search_step=2
processing_time_ratio=2.5
adapt_improve=True,
use_best_data=True
use_all_data=False
testing_dir=''
n_thumbs=40,
startend_clip=0.025
LS = getLS(feature_generators, combiner, filters, feats_to_cache, testing,
            feat_score_weight, local_search_width, local_search_step,
            processing_time_ratio, adapt_improve, use_best_data,
            use_all_data, testing_dir, n_thumbs, startend_clip)

video = '/home/ubuntu/vid/targ.mp4'
vid = cv2.VideoCapture(video)
_log.info('Starting Search')
LS.choose_thumbnails(vid, 5, video_name='test')