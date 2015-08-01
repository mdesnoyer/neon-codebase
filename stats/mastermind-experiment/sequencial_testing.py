#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as spstats
import itertools

def avg(data, bin_size):
    avg_num = []
    count = 0
    cum = 0
    for x in data:
        count = count + bin_size
        cum = cum + x
        avg_num.append(float(cum)/float(count))
    return avg_num

def get_vn(xn, yn, n):
    vn = 2 * (xn*(1-xn)+yn*(1-yn))/n
    return vn

def get_new_vn(vn, tao, alpha):
    new_vn = np.sqrt((2 * np.log(1/alpha) - np.log(vn/(vn+tao))) * ((vn * (vn + tao)) / tao))
    return new_vn

def get_theta_n(xn, yn):
    theta_n = yn - xn
    return theta_n

def get_bandit_fractions(impressions, conversions):
    # return the factions, and return the value remained.
    MC_SAMPLES = 2000
    mc_series = [spstats.beta.rvs(conversion + 1,
                                  impression,
                                  size=MC_SAMPLES)
                                  for conversion, impression in itertools.izip(conversions, impressions)]
    row_winners = np.argmax(mc_series, axis=0)
    winner_counts = np.array(np.bincount(row_winners), dtype=float)
    winner_fractions = winner_counts / MC_SAMPLES
    winner_index = np.argmax(winner_fractions)

    max_row_values = np.max(mc_series, axis=0)
    winner_row_values = mc_series[:][winner_index]
    lost_value = (max_row_values - winner_row_values) / winner_row_values
    sorted_lost_value = np.sort(lost_value)
    value_remaining = sorted_lost_value[0.95 * MC_SAMPLES]

    # Ends the experiment when there's at least a 95% probability that
    # the value remaining in the experiment is less than 1% of the champion's conversion rate.
    if value_remaining <= 0.01:
        return (winner_index, value_remaining, True)
    else:
        return (winner_index, value_remaining, False)

def get_bandit_fracs(strategy, baseline, editor, candidates):
    '''Gets the serving fractions for a multi-armed bandit strategy.

    This uses the Thompson Sampling heuristic solution. See
    https://support.google.com/analytics/answer/2844870?hl=en for
    more details.
    
    '''
    run_frac = {}
    valid_bandits = copy.copy(candidates)
    value_remaining = None
    winner_tid = None
    experiment_frac = strategy.exp_frac
            
    valid_bandits = list(valid_bandits)

    # Now determine the serving percentages for each valid bandit
    # based on a prior of its model score and its measured ctr.
    bandit_ids = [x.id for x in valid_bandits]
    conv = dict([(x.id, self._get_prior_conversions(x) +
                  x.get_conversions())
            for x in valid_bandits])
    imp = dict([(x.id, Mastermind.PRIOR_IMPRESSION_SIZE * 
                         (1 - Mastermind.PRIOR_CTR) + 
                         x.get_impressions() - conv[x.id])
                         for x in valid_bandits])

    # Run the monte carlo series
    MC_SAMPLES = 1000.
    mc_series = [spstats.beta.rvs(max(1, conv[x]),
                                  max(1, imp[x]),
                                  size=MC_SAMPLES)
                                  for x in bandit_ids]
    if non_exp_thumb is not None:
        conv = self._get_prior_conversions(non_exp_thumb) + \
          non_exp_thumb.get_conversions()
        mc_series.append(
            spstats.beta.rvs(max(1, conv),
                             max(1, Mastermind.PRIOR_IMPRESSION_SIZE * 
                                    (1 - Mastermind.PRIOR_CTR) + 
                                    non_exp_thumb.get_impressions()-conv),
                             size=MC_SAMPLES))

    win_frac = np.array(np.bincount(np.argmax(mc_series, axis=0)),
                        dtype=np.float) / MC_SAMPLES
    
    win_frac = np.append(win_frac, [0.0 for x in range(len(mc_series) - 
                                                       win_frac.shape[0])])
    winner_idx = np.argmax(win_frac)

    # Determine the number of real impressions for each entry in win_frac
    impressions = [x.get_impressions() for x in valid_bandits]
    if non_exp_thumb is not None:
        impressions.append(non_exp_thumb.get_impressions())

    # Determine the value remaining. This is equivalent to
    # determing that one of the other arms might beat the winner
    # by x%
    lost_value = ((np.max(mc_series, 0) - mc_series[:][winner_idx]) / 
                  mc_series[:][winner_idx])
    value_remaining = np.sort(lost_value)[0.95*MC_SAMPLES]

    # For all those thumbs that haven't been seen for 1000 imp,
    # make sure that they will get some traffic
    for i in range(len(valid_bandits)):
        if impressions[i] < 500:
            win_frac[i] = max(0.1, win_frac[i])
    win_frac = win_frac / np.sum(win_frac)

    if win_frac[winner_idx] >= 0.95:
        # There is a winner. See if there were enough imp to call it
        if (win_frac.shape[0] == 1 or 
            impressions[winner_idx] >= 500):
            # The experiment is done
            experiment_state = neondata.ExperimentState.COMPLETE
            try:
                winner = valid_bandits[winner_idx]
            except IndexError:
                winner = non_exp_thumb
            winner_tid = winner.id
            return (experiment_state,
                    self._get_experiment_done_fracs(
                        strategy, baseline, editor, winner),
                    value_remaining,
                    winner_tid)

        else:
            # Only allow the winner to have 90% of the imp
            # because there aren't enough impressions to make a good
            # decision.
            win_frac[winner_idx] = 0.90
            other_idx = [x for x in range(win_frac.shape[0])
                         if x != winner_idx]
            if np.sum(win_frac[other_idx]) < 1e-5:
                win_frac[other_idx] = 0.1 / len(other_idx)
            else:
                win_frac[other_idx] = \
                  0.1 / np.sum(win_frac[other_idx]) * win_frac[other_idx]

    # The serving fractions for the experiment are just the
    # fraction of time that each thumb won the Monte Carlo
    # simulation.
    if non_exp_thumb is not None:
        win_frac = np.around(win_frac[:-1], 2)
        win_frac = win_frac / np.sum(win_frac)
    for thumb_id, frac in zip(bandit_ids, win_frac):
        run_frac[thumb_id] = frac * experiment_frac

    return (experiment_state, run_frac, value_remaining, winner_tid)
        

def simulator():
    ctr_array = [0.04, 0.045]
    bin_size = 200
    experiment_size = 10000
    experiments = map(lambda x: np.random.binomial(bin_size, x, experiment_size), ctr_array)
    cumsum_experiements = map(lambda x: x.cumsum(), experiments)
    bin_experiments = np.ones(experiment_size) * bin_size
    cumsum_bin = bin_experiments.cumsum()
    mean_experiments = map(lambda x: x/cumsum_bin, cumsum_experiements)
    vn_experiments = get_vn(mean_experiments[0], mean_experiments[1], cumsum_bin)
    vn_new = get_new_vn(vn_experiments, 0.1, 0.1)
    theta_n = get_theta_n(mean_experiments[0], mean_experiments[1])
    #plt.plot(vn_new[1:1000])
    diff = theta_n - vn_new
    # plt.plot(theta_n[1000:10000])
    plt.figure(1)
    plt.plot(diff[1:200])
    plt.plot(np.zeros(200))
    plt.show()

    value_remainings = np.zeros(experiment_size)
    stop_markers = np.zeros(experiment_size)
    winner_indexes = np.zeros(experiment_size)
    for i in range(experiment_size):
        winner_index, value_remaining, is_stopped = get_bandit_fractions([cumsum_bin[i], cumsum_bin[i]], [cumsum_experiements[0][i], cumsum_experiements[1][i]])
        value_remainings[i] = value_remaining
        stop_markers[i] = is_stopped
        winner_indexes[i] = winner_index

    plt.figure(2)
    plt.plot(value_remainings[1:100])
    plt.plot(stop_markers[1:100])
    plt.plot(winner_indexes[1:100]/2.0)
    plt.show()

if __name__ == '__main__':
    simulator()
