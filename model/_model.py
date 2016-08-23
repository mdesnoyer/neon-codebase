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

class VideoThumbnail(object):
    '''Holds data about a video thumbnail.'''
    def __init__(self, image=None, score=None, frameno=None,
                 model_version=None, features=None, filtered_reason=None):
        self.image = image # CV Image object
        self.score = score # Model score of this image
        self.frameno = frameno # What frame number this image came from
        # The version of the model used to score this image
        self.model_version = model_version 
        # A numpy array of the feature vector for this image
        self.features = features
        # String describing why this thumbnail was filtered
        self.filtered_reason = filtered_reason

    def __str__(self):
        return utils.obj.full_object_str(self, ['features', 'image'])

class VideoClip(object):
    '''Holds data about a video clip.'''
    def __init__(self, start, end, score):
        self.start = start # The start frame number
        self.end = end # The end frame number
        self.score = score # The score of this clip

    def __str__(self):
        return utils.obj.full_object_str(self)
    

class Model(object):
    '''The whole model, which consists of a predictor and a filter.'''    
    def __init__(self, predictor, filt=None, vid_searcher=None,
                 clip_finder=None):
        self.__version__ = 4
        self.predictor = predictor
        self.filt = filt
        self.clip_finder = clip_finder
        if video_searcher is None:
            raise ValueError('A vid_searcher is required')
        else:
            self.video_searcher = vid_searcher
        


    def __setstate__(self, state):
        if 'video_searcher' not in state:
            self.video_searcher = local_video_searcher.LocalSearcher(
                state['predictor'])
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
        if self.clip_finder:
            self.clip_finder.update_processing_strategy(processing_strategy)

    def set_predictor(self, predictor):
        self.predictor = predictor
        self.clip_finder.predictor = predictor
        self.video_searcher.predictor = predictor

    def reset(self):
        self.predictor.reset()


    def score(self, image, do_filtering=True):
        '''Scores a single image. 

        Inputs:
        image - Image in numpy BGR format (aka OpenCV)
        do_filtering - Should the filter be applied?

        Returns: (score, attribute_string, feature_vector, model_version) 
                 of the image. If it was filtered, the score will be -inf and
                 the attribute will describe how it was filtered.
        '''
        if (self.filt is None or not do_filtering or 
            self.filt.accept(image, None, None)):
            score, features, model_version = self.predictor.predict(image)
            return (score, features, model_version, '')
        return (float('-inf'), None, None, self.filt.short_description())

       
    def choose_thumbnails(self, video, n=1, video_name='', m=0):
        '''Select the top n and/or bottom m thumbnails from a video.

        Returns:
        List of VideoThumbnail objects sorted by score descending
        '''
        return self.video_searcher.choose_thumbnails(video, n, video_name, m)

    def find_clips(self, mov, n=1, max_len=None, min_len=None):
        '''Finds clips from a video.

        If both max_len and min_len are None, then only complete scenes
        will be returned.

        Inputs:
        mov - A OpenCV VideoCapture object
        n - Number of clips to find
        max_len - Max length of each clip (seconds)
        min_len - Min length of each clip (seconds)

        Outputs:
        List of VideoClip objects sorted by score descending
        '''
        return self.clip_finder.find_clips(mov, n, max_len, min_len)

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
        mod = pickle.load(f)
    mod.restore_additional_data(filename)
    
    return mod

def generate_model(filename, predictor):
    '''
    Given a filename pointing to a pickled model, load the model
    and set the predictor 

    ls_inp_filename: Filename of the pickled model
    predictor: an instance of the predictor.
    '''
    mod = load_model(filename)
    mod.set_predictor(predictor)
    return mod
