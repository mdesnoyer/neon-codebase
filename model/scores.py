import os
import pandas as pd
import numpy as np

def lookup(model, score, gender=None, age=None):
    """Maps the valence score to a neon score, 
    returns None if there's a problem"""
    def get_dir(model):
        return os.path.join(os.path.dirname(__file__),
                                'demographics',
                                '%s-score.pkl' % model)
    if not model:
        # try the previous (i.e., pre-aquila) model
        model = '00000000-prevmodel-score.pkl'
    if not os.path.exists(get_dir(model)):
        # try aquila v1
        model = '00000000-aquilav1-score.pkl'
    try:
        scores = pandas.read_pickle(get_dir(model))
    except IOError as e:
        _log.warn('Could not read a valid model score file at %s: %s' % 
                  (get_dir(model), e))
        return None
    if gender is None:
        gender = 'None'
    if age is None:
        age = 'None'
    score_float = float(score)
    try:
        return sum(scores[gender, age] < score_float)
    except KeyEror as e:
        _log.error('Invalid demographics:', gender, age)
        return None