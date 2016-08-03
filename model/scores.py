import os
import pandas as pd
import numpy as np
import re
import logging

_log = logging.getLogger(__name__)

def lookup(model, score, gender=None, age=None):
    """Maps the valence score to a neon score, 
    returns None if there's a problem"""
    def get_file(model):
        return os.path.join(os.path.dirname(__file__),
                                'demographics',
                                '%s-score.pkl' % model)
    if not model:
        # try the previous (i.e., pre-aquila) model
        model = '20160000-prevmodel'
    elif re.match('20[0-9]{6}-[a-zA-Z0-9]+', model):
        # it's aquila v2
        pass
    elif 'aqv1' in model:
        model = '20160000-aquilav1'
    else:
        model = '20160000-prevmodel'
    try:
        score_map = pd.read_pickle(get_file(model))
    except IOError as e:
        _log.warn('Could not read a valid model score file at %s: %s' % 
                  (get_file(model), e))
        return None
    if gender is None:
        gender = 'None'
    if age is None:
        age = 'None'
    score_float = float(score)
    try:
        return sum(score_map[gender, age] < score_float)
    except KeyEror as e:
        _log.error_n('Invalid demographics: %s %s' % (gender, age))
        return None
