
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

import dateutil.parser
import math
import numpy as np
import pandas
import scipy.stats

def calc_lift_at_first_significant_hour(impressions, conversions,
                                        video_status, thumb_statuses,
                                        use_cmsdb_ctrs=False):
    '''Calculates the lift for each thumbnail relative to the others 
    when statistical significant is reached.
    Inputs:
    Impressions - A pandas DataFrame of impression counts where rows are hours
                  and columns are thumbnails
    Conversions - A pandas DataFrame of conversion counts where rows are hours
                  and columns are thumbnails
    video_status - The video status object for this video
    thumb_status - List of thumbnail status objects for the thumbnails
    
    If there is no statistically significant point, then the aggregate
    is used for calculating the stats.
    Returns:
    
    A pandas Panel with three frames: 'lift', 'p_value' and
    'revlift'. Each frame is a symmetric matrix where the
    cols and rows represent the thumbnail and the entries are row
    vs. col (baseline)
    '''

    # See when the experiment ended
    exp_end = None
    if video_status.experiment_state == 'complete' and len(video_status.state_history) > 0:
        exp_end = dateutil.parser.parse(video_status.state_history[-1][0])
    thumb_ctrs = dict([(x.get_id(), x.ctr or 0.0) for x in thumb_statuses])

    # Calculate the cumulative, per thumbnail stats we need.
    cum_imp = impressions.cumsum().fillna(method='ffill')
    cum_conv = conversions.cumsum().fillna(method='ffill')
    cum_ctr = cum_conv / cum_imp
    cum_stderr = np.sqrt(cum_ctr * (1-cum_ctr) / cum_imp)

    # Now put together the pairwise comparisons
    thumb_list = impressions.columns
    stats = pandas.Panel(items=['lift', 'p_value', 'revlift',
                                'xtra_conv_at_sig'],
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

            if exp_end and max(cum_imp.index) > exp_end:
                # Use the time in the cmsdb to flag when the
                # experiment finished.
                exp_end = exp_end.replace(minute=0, second=0, microsecond=0)
                idx = cum_imp.iloc[cum_imp.index >= exp_end].index[0]
            else:
                # Find where the first hour of statististical significance is
                sig = p_value[(p_value > 0.95) & (cum_imp[base] > 500) & 
                              (cum_imp[top] > 500) & (cum_conv[base] > 5) &
                              (cum_conv[top] > 5)]
                #sig = p_value[(p_value > 0.95) & (cum_imp[base] > 500) & 
                #              (cum_imp[top] > 500)]
                if len(sig) == 0:
                    # There isn't statistical significance anywhere so use
                    # the aggregate stats.
                    idx = p_value.index[-1]

                    # TODO(mdesnoyer): Playing. only keep data that's
                    #significant continue
                else:
                    idx = sig.index[0]

            if use_cmsdb_ctrs and exp_end and max(cum_imp.index) > exp_end:
                # The experiment has ended, so use the ctrs in the
                # database (as mastermind saw them)
                stats['p_value'][base][top] = np.max(p_value)
                if thumb_ctrs[base] > 0.0:
                    stats['lift'][base][top] = (
                        (thumb_ctrs[top] - thumb_ctrs[base]) /
                        thumb_ctrs[base])
                if (thumb_ctrs[top] < 1e-8 or
                    not np.isfinite(cum_ctr[top][idx])):
                    stats['revlift'][base][top] = 0.0
                else:
                    stats['revlift'][base][top] = (
                        1 - (thumb_ctrs[base] / thumb_ctrs[top]))
                stats['xtra_conv_at_sig'][base][top] = (
                    cum_conv[top][idx] - cum_imp[top][idx] *
                    cum_ctr[base][idx])
            else:
                stats['p_value'][base][top] = p_value[idx]
                stats['lift'][base][top] = ((
                    cum_ctr[top][idx] - cum_ctr[base][idx]) /
                    cum_ctr[base][idx])

                if (cum_ctr[top][idx] < 1e-8 or 
                    not np.isfinite(cum_ctr[top][idx])):
                    stats['revlift'][base][top] = 0.0
                else:
                    stats['revlift'][base][top] = (
                        1 - (cum_ctr[base][idx] / cum_ctr[top][idx]))

                stats['xtra_conv_at_sig'][base][top] = (
                    cum_conv[top][idx] - cum_imp[top][idx] * 
                    cum_ctr[base][idx])

    return stats

def calc_extra_conversions(conversions, revlift):
    '''Calculate the extra conversions for each thumb relative to the others.
    Inputs:
    conversions - A pandas DataFrame of conversion counts where rows are hours
                  and columns are thumbnails
    revlift - A DataFrame of lift where row and cols are thumbs. cols are baseline
    Returns:
    A DataFrame of extra conversions in the same shape as revlift
    '''
    if len(conversions.axes) == 2:
        conv_totals = conversions.sum()
    else:
        conv_totals = conversions
            
    retval = revlift.multiply(conv_totals, axis='index')
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
    data['xtra_conv_with_clamp'] = np.maximum(data['extra_conversions'],
                                              data['xtra_conv_at_sig'])
    all_data = data[(data['extra_conversions'] != 0) | data['is_base']]


    # Get the data from videos where there was a statistically
    # significant lift
    sig_data = all_data.copy()
    sig_data = sig_data.groupby(level=1).filter(
        lambda x: np.any(x['p_value']>0.95))

    neon_winners = sig_data[(sig_data['extra_conversions'] > 0) & 
                            (sig_data['p_value'] > 0.95)]

    lots_of_clicks = all_data.groupby(level=1).filter(
        lambda x: np.sum(x['conv']) > 100)

    cap_runaways = all_data.copy()
    cap_runaways['extra_conversions'] = cap_runaways['extra_conversions'].clip(-50)
    #no_runaways = cap_runaways.groupby(level=1).filter(
    #    lambda x: np.sum(x['extra_conversions']) > -50 or
    #    np.sum(x['conv']) < 50)
    

    return pandas.Series(
        {'significant_video_count' : sig_data.groupby(level=1).ngroups,
         'total_video_count' : all_data.groupby(level=1).ngroups,
         'total_neon_winners' : neon_winners.groupby(level=1).ngroups,
         'significant lift': calc_lift_from_dataframe(sig_data),
         'all_lift' : calc_lift_from_dataframe(all_data),
         'lots_clicks_lift' : calc_lift_from_dataframe(lots_of_clicks),
         'shutdown_bad_thumbs' : calc_lift_from_dataframe(
             all_data, 'xtra_conv_with_clamp'),
         'cap_runaways' : calc_lift_from_dataframe(cap_runaways)})

def calc_lift_from_dataframe(data, xtra_conv_col='extra_conversions'):
    if len(data) == 0:
        return float('nan')
    base_sums = data.groupby(['is_base']).sum()
    neon_sums = data.groupby(level=['type']).sum()
    all_sums = data.sum()

    #lift = base_sums['impr'][True] * base_sums[xtra_conv_col][False] / \
    #  (base_sums['conv'][True] * base_sums['impr'][False])

    # Lift based on the aggregate CTR differences
    #lift = base_sums['impr'][True] * neon_sums[xtra_conv_col]['neon'] / \
    #  (base_sums['conv'][True] * neon_sums['impr']['neon'])
   
    # Lift based on the extra clicks compared to the total clicks
    lift = neon_sums[xtra_conv_col]['neon'] / (all_sums['conv'] - 
           neon_sums[xtra_conv_col]['neon'])

    return lift

def calc_thumb_stats(base_impressions, base_conversions,
                     thumb_impressions, thumb_conversions):
    '''Calculates statistics for a thumbnail relative to a baseline.

    Inputs:
    Objects, like Series that can be calculated with normal operations

    Outputs:
    DataSeries indexed by groups and columns of stats
    '''
    ctr_base = pandas.Series(base_conversions / base_impressions)
    ctr_thumb = pandas.Series(thumb_conversions / thumb_impressions)

    lift = pandas.Series((ctr_thumb - ctr_base) / ctr_base)
    lift[np.logical_or(ctr_base < 1e-8, not np.isfinite(ctr_thumb))] = 0.0

    stats = pandas.DataFrame({'lift': lift,
                              'ctr' : ctr_thumb})
    stats['revlift'] = 1 - (ctr_base / ctr_thumb)
    stats['revlift'][np.logical_or(ctr_thumb < 1e-8, 
                                   not np.isfinite(ctr_thumb))] = 0.0
    
    se_base = np.sqrt(ctr_base * (1-ctr_base) / base_impressions)
    se_thumb = np.sqrt(ctr_thumb * (1-ctr_thumb) / thumb_impressions)

    zscore = (ctr_base - ctr_thumb) / \
      np.sqrt(se_base*se_base + se_thumb*se_thumb)

    p_value = pandas.Series(
        scipy.stats.norm(0, 1).cdf(zscore),
        index=zscore.index)
    p_value = p_value.where(p_value > 0.5, 1 - p_value)
    stats['p_value'] = p_value

    stats['ctr'] = ctr_thumb
    stats['extra_conversions'] = (thumb_conversions * stats['revlift']).replace(np.inf, 0).replace(-np.inf, 0)

    return stats

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
    np_data = np.array(data)
    extra_clicks = np_data[:,3] - np.multiply(np_data[:,2],
                                              np.divide(np_data[:,1],
                                                        np_data[:,0]))
    np_data = np_data[np.isfinite(extra_clicks),:]
    extra_clicks = extra_clicks[np.isfinite(extra_clicks)]
    
    raw_counts = np.sum(np_data, axis=0)

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
