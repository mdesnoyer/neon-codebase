# let's try to discover where this memory error is happening
# %load_ext autoreload
# %autoreload 2

import dill
import logging
import ipdb
import random
import numpy as np
from glob import glob
import cv2
import sys
import os

sys.path.append('/usr/local/lib/python2.7/dist-packages/')
from guppy import hpy
h = hpy()
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
#_log.propagate = 0 # silence the kids
_log.setLevel(logging.WARNING)
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
                    SET THE INITIAL HEAP
=======================================================================
'''
_log.warning('PRE MODEL / PRE READ HEAP')
h.heap()
_log.warning('SETTING THE INITIAL HEAP')
h.setrelheap()

'''
=======================================================================
                    END SET THE INITIAL HEAP
=======================================================================
'''
'''
=======================================================================
                    SIMULATE LOADING / UNLOADING MODEL
=======================================================================
'''
_log.warning('loading model -- INITIAL')
model = dill.load(open('/data/model_data/p_20151118_localsearch_autoimp.model'))
model.video_searcher.processing_time_ratio = 1.0
model_dir = '/data/local_search/testing_models'
new_model_fn = os.path.join(model_dir, 'tmp.model')

def save_load_model(model):
    '''replicates the save / load behavior of the model'''
    _log.warning('saving temp model')
    with open(new_model_fn, 'w') as f:
        dill.dump(model, f)
    _log.warning('loading temp model')
    with open(new_model_fn, 'r') as f:
        model = dill.load(f)
    return model

#save_load_model(model)

'''
=======================================================================
                   END SIMULATE LOADING / UNLOADING MODEL
=======================================================================
'''

def test(video_file, model):
    _log.getChild('').propagate = False # silence the kids
    _log.info('Reading in video')
    video = cv2.VideoCapture(video_file)
    video_name = video_file.split('/')[-1].split('.')[0]
    _log.warning('Video file: %s'%(video_name))
    n = 5
    thumbs = model.choose_thumbnails(video, n, video_name)

video_file = '/data/rank_centrality/starwars.mp4'
videos = [video_file] + glob('/data/discovery_pres/videos/*')
cnt = 0
model = save_load_model(model)
while True:
    for video_file in videos:
        _log.warning('POST MODEL / PRE READ HEAP')
        h.heap()
        test(video_file, model)
        model = save_load_model(model)
        _log.warning('POST MODEL / POST READ HEAP')
        h.heap()
        # cnt+=1
        # with open('/data/local_search/heap_reports/%04i'%(cnt), 'w') as f:
        #     dill.dump(h.heap(), f)
        break
    break