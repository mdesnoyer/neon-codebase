
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
import pandas
import scipy.stats

def calc_lift_at_first_significant_hour(impressions, conversions):
    '''Calculates the lift for each thumbnail relative to the others 
    when statistical significant is reached.

    Inputs:
    Impressions - A pandas DataFrame of impression counts where rows are hours
                  and columns are thumbnails
    Conversions - A pandas DataFrame of conversion counts where rows are hours
                  and columns are thumbnails
    

    If there is no statistically significant point, then the aggregate
    is used for calculating the stats.

    Returns:
    
    A pandas Panel with three frames: 'lift', 'p_value' and
    'revlift'. Each frame is a symmetric matrix where the
    cols and rows represent the thumbnail and the entries are row
    vs. col (baseline)

    '''

    # Calculate the cumulative, per thumbnail stats we need.
    cum_imp = impressions.cumsum().fillna(method='ffill')
    cum_conv = conversions.cumsum().fillna(method='ffill')
    cum_ctr = cum_conv / cum_imp
    cum_stderr = np.sqrt(cum_ctr * (1-cum_ctr) / cum_imp)

    # Now put together the pairwise comparisons
    thumb_list = impressions.columns
    stats = pandas.Panel(items=['lift', 'p_value', 'revlift'],
                         major_axis=thumb_list,
                         minor_axis=thumb_list)
    for base in thumb_list:
        for top in [x for x in thumb_list if x != base]:
            if cum_imp[base].iloc[-1] == 0 or cum_imp[top].iloc[-1] == 0:
                continue
            
            zscore = (cum_ctr[base] - cum_ctr[top]) / \
                np.sqrt(cum_stderr[base]*cum_stderr[base] +
                        cum_stderr[top]*cum_stderr[top])

            p_value = pandas.Series(
                scipy.stats.norm(0, 1).cdf(zscore),
                index=zscore.index)
            p_value = p_value.where(p_value > 0.5, 1 - p_value)

            # Find where the first hour of statististical significance is
            sig = p_value[(p_value > 0.95) & (cum_imp[base] > 500) & 
                          (cum_imp[top] > 500) & (cum_conv[base] > 5) &
                          (cum_conv[top] > 5)]
            if len(sig) == 0:
                # There isn't statistical significance anywhere so use
                # the aggregate stats.
                idx = p_value.index[-1]

                # TODO(mdesnoyer): Playing. only keep data that's significant
                continue
            else:
                idx = sig.index[0]
                
            stats['p_value'][base][top] = p_value[idx]
            stats['lift'][base][top] = ((
                cum_ctr[top][idx] - cum_ctr[base][idx]) /
                cum_ctr[base][idx])

            stats['revlift'][base][top] = ((
                cum_ctr[top][idx] - cum_ctr[base][idx]))

    return stats

def calc_extra_conversions(impressions, revlift):
    '''Calculate the extra conversions for each thumb relative to the others.

    Inputs:
    impressions - A pandas DataFrame of impression counts where rows are hours
                  and columns are thumbnails
    revlift - A DataFrame of lift where row and cols are thumbs. cols are baseline

    Returns:
    A DataFrame of extra conversions in the same shape as revlift
    '''
    impr_totals = impressions.sum()
            
    retval = revlift.multiply(impr_totals, axis='index')
    retval = retval.replace(np.inf, 0).replace(-np.inf, 0)
    return retval

def calc_aggregate_click_based_stats_from_dataframe(data):
    '''Calculate click based stats using a dataframe.

    Inputs:
    data - Data frame with columns of extra_conversions, impr, conv and is_base

    Returns:
    pandas series of stats we generate
    '''
    data = data.fillna(0)
    data = data[(data['extra_conversions'] != 0) | data['is_base']]

    # Only grab videos that have a baseline and one non-baseline
    data.groupby(level=1).filter(lambda x: np.any(x['is_base']) 
                                 and np.any(x['is_base'] == False))

    
    base_sums = data.groupby(['is_base']).sum()
    neon_sums = data.groupby(level=['type']).sum()

    #lift = base_sums['impr'][True] * base_sums['extra_conversions'][False] / \
    #  (base_sums['conv'][True] * base_sums['impr'][False])

    lift = base_sums['impr'][True] * neon_sums['extra_conversions']['neon'] / \
      (base_sums['conv'][True] * neon_sums['impr']['neon'])
    

    return pandas.Series({'lift': lift})
            

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

    This is the DerSimonian and Laird method.

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

    t_2 = max(0, (q - len(data) + 1) / c)

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

def calc_aggregate_click_based_metrics(data):
    '''Caclulates the aggregate A/B metrics assuming that the average is
    click centric.

    In other words, this gives you the expected ctr for the next click
    across all videos; videos aren't reweighted.

    Inputs:
    data - Matrix of ab data where each row corresponds to a video and is of
           the form: <base impressions>,<base conversions>,
                     <acting impressions>,<acting conversions>

    output (All number are in fractions):
     (Mean lift (Positive value is good), p_value, lower 95% confidence bound,
     upper 95% confidence bound)
    '''
    raw_counts = np.sum(data, axis=0)

    np_data = np.array(data)
    extra_clicks = np_data[:,3] - np.multiply(np_data[:,2],
                                              np.divide(np_data[:,1],
                                                        np_data[:,0]))

    lift = raw_counts[0] / (raw_counts[2] * raw_counts[1]) * \
      np.sum(extra_clicks)

    return (lift,
            None,
            None,
            None)
    

    # The following is the naive approach which suffers pretty heavily
    # from Simpson's paradox because the the traffic going to each
    # case could be significantly different.
    '''
    counts = np.sum(data, axis=0)

    p_base = float(counts[1]) / counts[0]
    p_act = float(counts[3]) / counts[2]
    p_diff = p_act - p_base

    se_base = math.sqrt(p_base*(1-p_base)/counts[0])
    se_act = math.sqrt(p_act*(1-p_act)/counts[2])
    se_tot = math.sqrt(se_base*se_base + se_act*se_act)

    low = p_diff - 1.96*se_tot
    up = p_diff + 1.96*se_tot

    z_score = p_diff / se_tot
    p_value = scipy.stats.norm.sf(z_score) * 2

    return (p_diff/p_base,
            p_value,
            low/p_base,
            up/p_base)
    '''
