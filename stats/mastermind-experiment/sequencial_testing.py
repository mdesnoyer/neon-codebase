#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as spstats
import itertools

class Sequencial(object):
    def __init__(self, tau = 100, alpha = 0.05):
        self.tau = tau
        self.alpha = alpha


    def get_vn(self, xn, yn, n_x, n_y):
        vn = xn*(1-xn)/float(n_x) + yn*(1-yn))/float(n_y)
        return vn

    def get_new_vn(self, vn):
        new_vn = np.sqrt((2 * np.log2(1 / self.alpha) - np.log2(vn / (vn + self.tau))) * ((vn * (vn + self.tau)) / self.tau))
        return new_vn

    def calculate_significance(self, conversions_1, impressions_1, conversions_2, impressions_2):
        mean_1 = float(conversions_1)/float(impressions_1)
        mean_2 = float(conversions_2)/float(impressions_2)

        if mean_2 >= mean_1:
            theta_n = mean_2 - mean_1
        else:
            theta_n = mean_1 - mean_2

        vn_experiments = self.get_vn(mean_1, mean_2, impressions_1, impressions_2)
        vn_new = self.get_new_vn(vn_experiments, 100, 0.05)

        threshold_traditional = theta_n - np.sqrt(vn_experiments)*1.65
        threshold_new = theta_n - vn_new

        # mean difference, 
        return (mean_2 - mean_1, threshold_traditional, threshold_new)

class MultiArmedBandits(object):
    def __init__(self, alpha = 0.05, value_threshold = 0.01):
        self.alpha = alpha
        self.value_threshold = value_threshold

    def get_bandit_fractions(self, impressions, conversions):
        # return the factions, and return the value remained.
        MC_SAMPLES = 2000.
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
        value_remaining = sorted_lost_value[(1 - self.alpha) * MC_SAMPLES]


        # Ends the experiment when there's at least a 95% probability that
        # the value remaining in the experiment is less than 1% of the champion's conversion rate.
        if value_remaining <= self.value_threshold:
            return (winner_index, value_remaining, winner_fractions, True)
        else:
            return (winner_index, value_remaining, winner_franctions, False)

class StatsOptimizingSimulator(object):
    def __init__(self, bin_size = 100):
        self.bin_size = bin_size
        #self.ctr_array = [0.04, 0.05]
        self.experiment_number = 500
        self.max_iteration = 10000

    def run_sequencial_experiment(self, conversion_simulator_function):
        sequencial_method = Sequencial()

        traditional_end_count = 0
        new_end_count = 0
        traditional_err_count = 0
        new_err_count = 0
        traditional_inconclusive_count = 0
        new_inconclusive_count = 0
        for i in range(self.experiment_number):
            impression_counter_1 = 0
            impression_counter_2 = 0
            conversion_counter_1 = 0
            conversion_counter_2 = 0
            is_traditional_reached = False
            is_new_reached = False
            iteration_counter = 0
            while (not is_traditional_reached or not is_new_reached) and iteration_counter < self.max_iteration:
                conversion_1, conversion_2, impression_1, impression_2 = conversion_simulator_function(bin_size, iteration_counter)
                conversion_counter_1 = conversion_counter_1 + conversion_1
                conversion_counter_2 = conversion_counter_2 + conversion_2
                impression_counter_1 = impression_counter_1 + impression_1
                impression_counter_2 = impression_counter_2 + impression_2
                impression_counter = impression_counter_1 + impression_counter_2
                mean_diff, threshold_traditional, threshold_new = \
                    sequencial_method.calculate_significance(conversion_counter_1, impression_counter_1, conversion_counter_2, impression_counter_2)
                if not is_traditional_reached and threshold_traditional > 0:
                    is_traditional_reached = True
                    traditional_end_count = traditional_end_count + impression_counter
                    if mean_diff < 0:
                        traditional_err_count = traditional_err_count + 1
                if not is_new_reached and threshold_new > 0:
                    is_new_reached = True
                    new_end_count = new_end_count + impression_counter
                    if mean_diff < 0:
                        new_err_count = new_err_count + 1
                iteration_counter = iteration_counter + 1

            # If the condition is not reached then the inconclusive count adds one.
            if not is_traditional_reached:
                traditional_inconclusive_count = traditional_inconclusive_count + 1
            if not is_new_reached:
                new_inconclusive_count = new_inconclusive_count + 1

        if traditional_end_count == 0:
            traditional_avg = 0
        else:
            traditional_avg = traditional_end_count / (self.experiment_number - traditional_inconclusive_count)

        if new_end_count == 0:
            new_avg = 0
        else:
            new_avg = new_end_count / (self.experiment_number - new_inconclusive_count)

        return (traditional_avg, new_avg, traditional_inconclusive_count, new_inconclusive_count, traditional_err_count, new_err_count)

    def run_bandit_experiment(self, conversion_simulator_function):
        bandit_method = MultiArmedBandits()

        bandit_end_count = 0
        bandit_err_count = 0
        bandit_inconclusive_count = 0
        franctions = [0.5, 0.5]
        for i in range(self.experiment_number):
            impression_counter = 0
            conversion_counter_1 = 0
            conversion_counter_2 = 0
            is_bandit_reached = False
            iteration_counter = 0
            while (not is_traditional_reached or not is_new_reached) and iteration_counter < self.max_iteration:
                impression_counter = impression_counter + bin_size
                conversion_1, conversion_2, impression_1, impression_2 = conversion_simulator_function(bin_size, iteration_counter, franctions)
                conversion_counter_1 = conversion_counter_1 + conversion_1
                conversion_counter_2 = conversion_counter_2 + conversion_2
                impression_counter_1 = impression_counter_1 + impression_1
                impression_counter_2 = impression_counter_2 + impression_2
                impression_counter = impression_counter_1 + impression_counter_2
                conversions = [conversion_counter_1, conversion_counter_2]
                impressions = [impression_counter_1, impression_counter_2]
                winner_index, value_remaining, fractions, is_stopped = \
                    bandit_method.get_bandit_fractions(impressions, conversions)
                if not is_bandit_reached and is_stopped:
                    is_bandit_reached = True
                    bandit_end_count = bandit_end_count + impression_counter
                    if winner_index != 1:
                        bandit_err_count = bandit_err_count + 1
                iteration_counter = iteration_counter + 1

            # If the condition is not reached then the inconclusive count adds one.
            if not is_bandit_reached:
                bandit_inconclusive_count = bandit_inconclusive_count + 1

        if bandit_end_count == 0:
            bandit_avg = 0
        else:
            bandit_avg = bandit_end_count / (self.experiment_number - bandit_inconclusive_count)

        return (bandit_avg, bandit_inconclusive_count, bandit_err_count)

def simulator_function_simple(bin_size, count):
    ctr_array = [0.04, 0.05]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_type1_err(bin_size, count):
    ctr_array = [0.05, 0.05]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_exp_ctr(bin_size, count):
    ctr_array = np.array([0.04, 0.05])
    ctr_array = ctr_array**(1.02 * count)
    if ctr_array[0] < 0.01:
        ctr_array = np.array([0.04, 0.05])
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_exp_ctr_type1_err(bin_size, count):
    ctr_array = [0.05, 0.05]
    ctr_array = ctr_array**(1.02 * count)
    if ctr_array[0] < 0.01:
        ctr_array = np.array([0.05, 0.05])
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_random_ctr(bin_size, count):
    ctr_array = np.array([0.04, 0.05])
    ctr_array = ctr_array**(1.02 * count)
    if ctr_array[0] < 0.01:
        ctr_array = np.array([0.04, 0.05])
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_bandit_simple(bin_size, count, fractions):
    ctr_array = [0.04, 0.05]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_bandit_simple_type1_err(bin_size, count, fractions):
    ctr_array = [0.05, 0.05]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_bandit_constant(bin_size, count, fractions):
    ctr_array = [0.04, 0.05]
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_bandit_constant_type1_err(bin_size, count, fractions):
    ctr_array = [0.05, 0.05]
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_bandit_exp(bin_size, count, fractions):
    ctr_array = np.array([0.04, 0.05])
    ctr_array = ctr_array**(1.02 * count)
    if ctr_array[0] < 0.01:
        ctr_array = np.array([0.04, 0.05])
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator_function_bandit_exp_type1_err(bin_size, count, fractions):
    ctr_array = np.array([0.05, 0.05])
    ctr_array = ctr_array**(1.02 * count)
    if ctr_array[0] < 0.01:
        ctr_array = np.array([0.05, 0.05])
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    return (conversion_1, conversion_2, impression_1, impression_2)

def simulator():
    ctr_array = [0.04, 0.05]
    bin_size = 200
    experiment_size = 10000
    experiments = map(lambda x: np.random.binomial(bin_size, x, experiment_size), ctr_array)
    cumsum_experiements = map(lambda x: x.cumsum(), experiments)
    bin_experiments = np.ones(experiment_size) * bin_size
    cumsum_bin = bin_experiments.cumsum()
    mean_experiments = map(lambda x: x/cumsum_bin, cumsum_experiements)
    vn_experiments = get_vn(mean_experiments[0], mean_experiments[1], cumsum_bin)
    vn_new = get_new_vn(vn_experiments, 100, 0.05)
    theta_n = get_theta_n(mean_experiments[0], mean_experiments[1])
    #plt.plot(vn_new[1:1000])
    diff = theta_n - vn_new
    diff_old = theta_n - np.sqrt(vn_experiments)*1.65
    # plt.plot(theta_n[1000:10000])
    s_size = 500
    plt.subplot(2,1,1)
    plt.plot(diff[1:s_size])
    plt.plot(np.zeros(s_size))
    plt.plot(diff_old[1:s_size])
    plt.show()

def avg(data, bin_size):
    avg_num = []
    count = 0
    cum = 0
    for x in data:
        count = count + bin_size
        cum = cum + x
        avg_num.append(float(cum)/float(count))
    return avg_num

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
    ctr_array = [0.04, 0.05]
    bin_size = 200
    experiment_size = 10000
    experiments = map(lambda x: np.random.binomial(bin_size, x, experiment_size), ctr_array)
    cumsum_experiements = map(lambda x: x.cumsum(), experiments)
    bin_experiments = np.ones(experiment_size) * bin_size
    cumsum_bin = bin_experiments.cumsum()
    mean_experiments = map(lambda x: x/cumsum_bin, cumsum_experiements)
    vn_experiments = get_vn(mean_experiments[0], mean_experiments[1], cumsum_bin)
    vn_new = get_new_vn(vn_experiments, 100, 0.05)
    theta_n = get_theta_n(mean_experiments[0], mean_experiments[1])
    #plt.plot(vn_new[1:1000])
    diff = theta_n - vn_new
    diff_old = theta_n - np.sqrt(vn_experiments)*1.65
    # plt.plot(theta_n[1000:10000])
    s_size = 500
    plt.subplot(2,1,1)
    plt.plot(diff[1:s_size])
    plt.plot(np.zeros(s_size))
    plt.plot(diff_old[1:s_size])
    plt.show()

    # value_remainings = np.zeros(experiment_size)
    # stop_markers = np.zeros(experiment_size)
    # winner_indexes = np.zeros(experiment_size)
    # for i in range(experiment_size):
    #     winner_index, value_remaining, is_stopped = get_bandit_fractions([cumsum_bin[i], cumsum_bin[i]], [cumsum_experiements[0][i], cumsum_experiements[1][i]])
    #     value_remainings[i] = value_remaining
    #     stop_markers[i] = is_stopped
    #     winner_indexes[i] = winner_index

    # plt.subplot(2,1,2)
    # arm_size = s_size
    # plt.plot(value_remainings[1:arm_size])
    # plt.plot(stop_markers[1:arm_size])
    # plt.plot(winner_indexes[1:arm_size]/2.0)
    # plt.ylim(-0.1,1.1)
    # plt.show()

if __name__ == '__main__':
    simulator()
