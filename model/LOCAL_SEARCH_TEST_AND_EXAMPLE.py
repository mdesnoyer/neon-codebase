# %load_ext autoreload
# %autoreload 2
'''
Tests the Local Video Searcher
'''
if '/repos/neon/model' not in sys.path:
    sys.path.insert(0, '/repos/neon/model')
from model.features import (BlurGenerator, SADGenerator, FaceGenerator,
                            ClosedEyeGenerator, TextGenerator,
                            PixelVarGenerator, VibranceGenerator)
from model._model import load_model, save_model
import cv2
import numpy as np
from cPickle import load
from model.parse_faces import MultiStageFaceParser
from model.score_eyes import ScoreEyes
import dlib
from sklearn.externals import joblib
from local_video_searcher import (LocalSearcher, Combiner, MINIMIZE, MAXIMIZE,
                                    NORMALIZE)

from model.filters import (ThreshFilt, SceneChangeFilter, FaceFilter,
                           EyeFilter)
import logging
import ipdb

import random

from glob import glob
from cPickle import dumps
if '/usr/local/lib/python2.7/dist-packages/' not in sys.path:
    sys.path.insert(0, '/usr/local/lib/python2.7/dist-packages/')
import dill
from collections import defaultdict as ddict

# set the random seed
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

# logging.info('test info')
# logging.debug('test debug')
# logging.warning('test warning')
# logging.error('test error')
# logging.basicConfig(level=logging.INFO,
#                     format='[%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
#                     handlers=[ColorHandler()])
'''
=======================================================================
                             END LOGGING
=======================================================================
'''

'''
=======================================================================
                            UTILITY FUNCTIONS
=======================================================================
'''

'''
=======================================================================
                          END UTILITY FUNCTIONS
=======================================================================
'''


# THERES SOMETHING WITH WITH THE PREDICTOR
# if not 'predictor' in locals():
#     predictor = load(open('/data/local_search/predictor'))
# video_file = '/data/rank_centrality/starwars.mp4'
# video = cv2.VideoCapture(video_file)


# obtain the predictor
_log.info('loading predictor')
if not 'predictor' in locals():
    predictor = load(open('/data/local_search/predictor'))
_log.info('creating landmark detector')
f_predictor_path = '/repos/dlib-18.17/shape_predictor_68_face_landmarks.dat'
f_predictor = dlib.shape_predictor(f_predictor_path)
_log.info('loading closed eye classifier')
classifier = joblib.load('/data/Faces/new_models/linear_model_scaler_compressed')
_log.info('creating face finder')
face_finder = MultiStageFaceParser(f_predictor)
_log.info('instantiating closed eye class')
eye_scorer = ScoreEyes(classifier)


# generate the filters
_log.info('generating filters')
pix_filt = ThreshFilt(thresh=200)
scene_filt = SceneChangeFilter()
face_filt = FaceFilter()
eye_filt = EyeFilter()

filters = [scene_filt, pix_filt, face_filt, eye_filt]

_log.info('generating feature generators')
pix_gen = PixelVarGenerator()
sad_gen = SADGenerator()
text_gen = TextGenerator()
face_gen = FaceGenerator(face_finder)
eye_gen = ClosedEyeGenerator(face_finder, classifier)
vibrance_gen = VibranceGenerator()

feature_generators = [pix_gen, sad_gen, text_gen, face_gen, eye_gen,
                      vibrance_gen]

_log.info('Generating combiner')
weight_valence = {'blur':MAXIMIZE, 'sad':MINIMIZE, 'eyes':MAXIMIZE,
                  'text':MINIMIZE, 'pixvar':NORMALIZE, 'vibrance':MAXIMIZE}
combiner = Combiner(weight_valence=weight_valence)


feats_to_cache = ['pixvar', 'blur', 'sad', 'eyes', 'text', 'vibrance']

_log.info('Instantiating local searcher')
LS = LocalSearcher(predictor, face_finder, eye_scorer,
                   feature_generators=feature_generators,
                   combiner=combiner,
                   filters=filters,
                   feats_to_cache=feats_to_cache,
                   testing=True
                   testing_dir='/data/local_search/testing')
# this shouldn't be warning but I want to test it out
# _log.info('Testing DILL')
# try:
#     x = dill.dumps(LS)
#     LS = dill.loads(x)
# except:
#     _log.warn('Dill pickling failed!')
_log.info('Reading in video')
video_file = '/data/rank_centrality/starwars.mp4'
videos = [video_file] + glob('/data/discovery_pres/videos/*')
from time import time
start = time()
for video_file in videos:
    #video_file=videos[1]
    random.seed(1337)
    np.random.seed(1337)
    video = cv2.VideoCapture(video_file)
    video_name = video_file.split('/')[-1].split('.')[0]
    #video_name = 'star wars'
    n = 5

    thumbs = LS.choose_thumbnails(video, n, video_name)
    # _log.info('Testing DILL')
    # try:
    #     x = dill.dumps(LS)
    #     LS = dill.loads(x)
    # except:
    #     _log.warn('Dill pickling failed!')
    # if 'Giant_Shark_Stakes_Her_Claim' in video_file:
    #     break
    #break
    for n,t in enumerate(thumbs):
        cv2.imwrite('/data/discovery_pres/extracted_thumbs/%s_%i.jpg'%(video_name, n), t[0])
    from cPickle import dump
    with open('/data/discovery_pres/extracted_thumb_data/%s'%video_name, 'w') as f:
        dump(thumbs, f)
    #break
print 'Total time: %.2f'%(time()-start)
