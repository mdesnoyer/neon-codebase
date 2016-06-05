'''A classifier that predicts Valence scores for images and videos.

The Predictor and FeatureGenerator classes are abstract and should be
specialized by subclassing. Note that as the code version changes,
code may need to be added to deal with backwards compatibility when
pickling/unpickling. See the Python pickling docs about those issues,
especially using the __setstate__ function.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import dill as pickle
import cv2
from model import features
from model import filters
import logging
from model import predictor
import utils.obj
from utils import statemon
from model import video_searcher
from model import local_video_searcher

_log = logging.getLogger(__name__)

class Model(object):
    '''The whole model, which consists of a predictor and a filter.'''    
    def __init__(self, predictor, filt=None, vid_searcher=None):
        self.__version__ = 3
        self.predictor = predictor
        self.filt = filt
        if video_searcher is None:
            # while it's tempting to modify this to use the LocalSearcher,
            # this would require adding numerous arguments to the
            # instantiation of the Model class, and as such convention will
            # now become that BisectSearcher is the default video searcher,
            # and is what will be adopted in the event there is insufficient
            # information available.
            self.video_searcher = video_searcher.BisectSearcher(
                predictor, filt)
        else:
            self.video_searcher = vid_searcher


    def __setstate__(self, state):
        if 'video_searcher' not in state:
            state['video_searcher'] = video_searcher.BisectSearcher(
                state['predictor'], state['filt'])
        self.__dict__ = state


    def __str__(self):
        return utils.obj.full_object_str(self)

    def update_processing_strategy(self, processing_strategy):
        try:
            self.video_searcher.update_processing_strategy(
                                                processing_strategy)
        except Exception, e:
            _log.error(("Video Searcher does not support different "
                        "processing strategies."))

    def reset(self):
        self.predictor.reset()


    def score(self, image, do_filtering=True):
        '''Scores a single image. 

        Inputs:
        image - Image in numpy BGR format (aka OpenCV)
        do_filtering - Should the filter be applied?

        Returns: (score, attribute_string) of the image. If it was
        filtered, the score will be -inf and the attribute will
        describe how it was filtered.

        '''
        if (self.filt is None or not do_filtering or 
            self.filt.accept(image, None, None)):
            return (self.predictor.predict(image), '')
        return (float('-inf'), self.filt.short_description())

       
    def choose_thumbnails(self, video, n=1, video_name=''):
        '''Select the top n thumbnails from a video.

        Returns:
        ([(image,score,frame_no,timecode,attribute)]) sorted by score
        '''
        return self.video_searcher.choose_thumbnails(video, n, video_name)


    def restore_additional_data(self, filename):
        '''
        Given filename (which points to the pkl of the restored model), 
        restores additional data, specifically for filters. New filters
        (i.e., the closed-eye filter) require access to pickled numpy
        arrays and scipy objects which are too expensive to pickle
        using the vanilla implementation. The closed-eye filter has
        an implementation of restore_additional_data that can find what
        it requires so long as it knows where model_data is, which it
        can determine based on where the model pickle is.
        '''
        if self.filt is None:
            return
        else:
            self.filt.restore_additional_data(filename)


def save_model(model, filename):
    '''Save the model to a file.'''
    with open(filename, 'wb') as f:
        pickle.dump(model, f, 2)

def load_model(filename):
    '''Loads a model from a file.

    Inputs:
    filename - The model to load

    '''
    with open(filename, 'rb') as f:
        model = pickle.load(f)
    model.restore_additional_data(filename)
    
    return model 

def generate_model(ls_inp_filename, predictor):
    '''
    Given a filename pointing to an input dict for 
    local video searcher and a functioning instance
    of the predictor, generates a new model.

    ls_inp_filename: A filename that contains a pickled dictionary of the
        inputs required for local search (the combiner, face finder, etc)
    predictor: an instance of the predictor.
    '''
    with open(ls_inp_filename) as f:
        ls_in_dict = pickle.load(ls_inp_filename)
    loc_srch = local_video_searcher.LocalSearcher(predictor, **ls_in_dict)
    model = Model(predictor, vid_searcher=loc_srch)
    model.restore_additional_data(ls_inp_filename)
    return model
