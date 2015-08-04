'''A classifier that predicts Valence scores for images and videos.

The Predictor and FeatureGenerator classes are abstract and should be
specialized by subclassing. Note that as the code version changes,
code may need to be added to deal with backwards compatibility when
pickling/unpickling. See the Python pickling docs about those issues,
especially using the __setstate__ function.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import cPickle as pickle
import cv2
from . import features
from . import filters
import logging
from . import predictor
import utils.obj
from utils import statemon
from . import video_searcher

_log = logging.getLogger(__name__)

class Model(object):
    '''The whole model, which consists of a predictor and a filter.'''    
    def __init__(self, predictor, filt=None, vid_searcher=None):
        self.__version__ = 3
        self.predictor = predictor
        self.filt = filt
        if video_searcher is None:
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
        if self.filt is not None:
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

