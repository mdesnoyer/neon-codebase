
'''
Utilities to calculate useful metrics

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014

'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import math
import numpy as np
import scipy.stats

def calc_thumb_stats(baseCounts, thumbCounts):
    '''Calculates statistics for a thumbnail relative to a baseline.

    Inputs:
    baseCounts - (base_impressions, base_conversions)
    thumbCounts - (thumb_impressions, thumb_conversions)

    Outputs:
    (CTR, Extra Conversions, Lift, P Value)
    '''
    if baseCounts[0] == 0 or thumbCounts[0] == 0:
        return (0.0, 0.0, 0.0, 0.0)
    
    ctr_base = float(baseCounts[1]) / baseCounts[0]
    ctr_thumb = float(thumbCounts[1]) / thumbCounts[0]

    if baseCounts[1] > 0 and ctr_base <= 1.0:
        se_base = math.sqrt(ctr_base * (1-ctr_base) / baseCounts[0])
    else:
        se_base = 0.0

    if thumbCounts[1] > 0 and ctr_thumb <= 1.0:
        se_thumb = math.sqrt(ctr_thumb * (1-ctr_thumb) / thumbCounts[0])
    else:
        se_thumb = 0.0

    if se_base == 0.0 and se_thumb == 0.0:
        return (ctr_thumb, 0, 0.0, 0.0)

    zscore = (ctr_base - ctr_thumb) / \
      math.sqrt(se_base*se_base + se_thumb*se_thumb)

    p_value = scipy.stats.norm(0, 1).cdf(zscore)
    if p_value < 0.5:
        p_value = 1 - p_value

    return (ctr_thumb, 
            thumbCounts[1] - ctr_base * thumbCounts[0],
            (ctr_thumb - ctr_base) / ctr_base if ctr_base > 0.0 else 0.0,
            p_value)

def calc_aggregate_ab_metrics(data):
    '''Calculates aggregate A/B metrics for multiple videos

    Using random effects model assumption on the relative risk (or
    ratio of CTRs) and meta analysis math from:
    http://www.meta-analysis.com/downloads/Intro_Models.pdf

    And Relative Risk approximations from:
    http://en.wikipedia.org/wiki/Relative_risk

    Inputs:
    data - Matrix of ab data where each row corresponds to a video and is of
           the form: <base impressions>,<base conversions>,
                     <acting impressions>,<acting conversions>

    output (All number are in fractions):
     (Mean lift, p_value, lower 95% confidence bound,
     upper 95% confidence bound, percent of error from random effects)
    '''
    filtered_data = [x for x in data if
                     x[0] > 0 and x[2] > 0 and x[1] > 0 and x[3] > 0]
    ctr_base = np.array([float(x[1])/float(x[0]) for x in filtered_data])
    n_base = np.array([x[0] for x in filtered_data])
    ctr_neon = np.array([float(x[3])/float(x[2]) for x in filtered_data])
    n_neon = np.array([x[2] for x in filtered_data])

    log_ratio = np.log(np.divide(ctr_neon, ctr_base))
    var_log_ratio = (np.divide(1-ctr_neon, np.multiply(n_neon, ctr_neon)) +
                     np.divide(1-ctr_base, np.multiply(n_base, ctr_base)))

    w = 1 / var_log_ratio
    w_sum = np.sum(w)

    q = (np.dot(w, np.square(log_ratio)) -
         (np.square(np.dot(w, log_ratio)) / w_sum))
    c = w_sum - np.sum(np.square(w)) / w_sum

    t_2 = (q - len(data) + 1) / c

    w_star = 1 / (var_log_ratio + t_2)

    mean_log_ratio_star = np.dot(w_star, log_ratio) / np.sum(w_star)
    var_log_ratio_star = 1 / np.sum(w_star)
    standard_error = np.sqrt(var_log_ratio_star)

    low = np.exp(mean_log_ratio_star - 1.96*standard_error)
    up = np.exp(mean_log_ratio_star + 1.96*standard_error)
    mn = np.exp(mean_log_ratio_star)

    p_value = scipy.stats.norm.sf(mean_log_ratio_star / standard_error) * 2

    return (float(mn - 1), float(p_value),
            float(low - 1), float(up - 1),
            float(1 - np.sqrt(1/w_sum) / standard_error))
