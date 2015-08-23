#!/usr/bin/env python
'''
Simulation on various A/B Testing methods.

Author: Wiley Wang (wang@neon-lab.com)
Copyright 2015 Neon Labs
'''
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as spstats
import itertools

class Param(object):
    BASE_PERCENT = 0.04
    LIFT = 1.50
    EXP_RATE = 0.02
    EXP_STOP = 0.1
    MIN_CONVERSION = 50
    BANDIT_FRAC_END = 0
    RANDOM_WALK_UPPER = 0.15
    RANDOM_WALK_LOWER = 0.01
    RANDOM_WALK_STEP = 0.005
    ALPHA = 0.5
    FRAC_ADJUST_RATE = 0.0
    COMBINED_BIN_SIZE = 5

class Sequencial(object):
    def __init__(self, tau = 0.0001, alpha = Param.ALPHA):
        self.tau = tau
        self.alpha = alpha


    def get_vn(self, xn, yn, n_x, n_y):
        vn = xn*(1-xn)/float(n_x) + yn*(1-yn)/float(n_y)
        return vn

    def get_new_vn(self, vn):
        if vn == 0:
            return 0.0
        else:
            new_vn = np.sqrt((2 * np.log(1 / self.alpha) - np.log(vn / (vn + self.tau))) * ((vn * (vn + self.tau)) / self.tau))
            return new_vn

    def calculate_significance(self, conversions_1, impressions_1, conversions_2, impressions_2):
        mean_1 = float(conversions_1)/float(impressions_1)
        mean_2 = float(conversions_2)/float(impressions_2)

        if mean_2 >= mean_1:
            theta_n = mean_2 - mean_1
        else:
            theta_n = mean_1 - mean_2

        vn_experiments = self.get_vn(mean_1, mean_2, impressions_1, impressions_2)
        vn_new = self.get_new_vn(vn_experiments)

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
        try:
            mc_series = [spstats.beta.rvs(conversion + 1,
                                          impression - conversion + 1,
                                          size=MC_SAMPLES)
                                          for conversion, impression in itertools.izip(conversions, impressions)]
        except ValueError:
            print "Value Error:"
            print conversions
            print impressions

        row_winners = np.argmax(mc_series, axis=0)
        winner_counts = np.array(np.bincount(row_winners), dtype=float)
        winner_fractions = winner_counts / MC_SAMPLES
        winner_index = np.argmax(winner_fractions)

        max_row_values = np.max(mc_series, axis=0)
        winner_row_values = mc_series[:][winner_index]
        lost_value = (max_row_values - winner_row_values) / winner_row_values
        sorted_lost_value = np.sort(lost_value)
        value_remaining = sorted_lost_value[(1 - self.alpha) * MC_SAMPLES]

        total_conversion_count = sum(conversions)
        if total_conversion_count < Param.BANDIT_FRAC_END:
            winner_fractions = np.ones(len(impressions))/len(impressions)

        # winner_fractions = np.ones(len(impressions))
        winner_fractions = winner_fractions ** Param.FRAC_ADJUST_RATE
        winner_fractions = winner_fractions / sum(winner_fractions)
        # Ends the experiment when there's at least a 95% probability that
        # the value remaining in the experiment is less than 1% of the champion's conversion rate.
        if value_remaining <= self.value_threshold:
            return (winner_index, value_remaining, winner_fractions, True)
        else:
            return (winner_index, value_remaining, winner_fractions, False)

class StatsOptimizingSimulator(object):
    def __init__(self, bin_size = 100, experiment_number = 500, is_display = False):
        self.bin_size = bin_size
        #self.ctr_array = [0.04, 0.05]
        self.experiment_number = experiment_number
        self.max_iteration = 10000
        self.is_display = is_display

    def calc_aggregate_ab_metrics(self, data):
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

        p_value = spstats.norm.sf(mean_log_ratio_star / standard_error) * 2

        return (float(mn - 1), float(p_value),
                float(low - 1), float(up - 1),
                float(1 - np.sqrt(1/w_sum) / standard_error))

    def bin_data(self, data):
        binned_size = int(np.ceil(len(data)/float(Param.COMBINED_BIN_SIZE)))
        binned_data = np.zeros(binned_size)
        for index, value in enumerate(data):
            new_index = index / Param.COMBINED_BIN_SIZE
            binned_data[new_index] = binned_data[new_index] + value
        return binned_data


    def run_sequencial_experiment(self, conversion_simulator_function):
        sequencial_method = Sequencial()

        traditional_end_count = 0
        new_end_count = 0
        traditional_err_count = 0
        new_err_count = 0
        traditional_err_end_count = 0
        new_err_end_count = 0
        traditional_inconclusive_count = 0
        new_inconclusive_count = 0
        total_missed_traditional_conversion_count = 0
        total_missed_new_conversion_count = 0
        threshold_traditional_array = []
        threshold_new_array = []
        optimal_traditional_conversion_count = 0
        optimal_new_conversion_count = 0
        for i in range(self.experiment_number):
            impression_counter_1 = 0
            impression_counter_2 = 0
            conversion_counter_1 = 0
            conversion_counter_2 = 0
            is_traditional_reached = False
            is_new_reached = False
            iteration_counter = 0

            total_optimal_conversion_count = 0
            total_conversion_count = 0
            missed_traditional_conversion_count = 0
            missed_new_conversion_count = 0
            while (not is_traditional_reached or not is_new_reached) and iteration_counter < self.max_iteration:
                conversion_1, conversion_2, impression_1, impression_2, optimal_conversions = conversion_simulator_function(self.bin_size, iteration_counter)
                conversion_counter_1 = conversion_counter_1 + conversion_1
                conversion_counter_2 = conversion_counter_2 + conversion_2
                impression_counter_1 = impression_counter_1 + impression_1
                impression_counter_2 = impression_counter_2 + impression_2
                impression_counter = impression_counter_1 + impression_counter_2

                total_optimal_conversion_count = total_optimal_conversion_count + optimal_conversions
                total_conversion_count = total_conversion_count + conversion_1[0] + conversion_2[0]

                mean_diff, threshold_traditional, threshold_new = \
                    sequencial_method.calculate_significance(conversion_counter_1, impression_counter_1, conversion_counter_2, impression_counter_2)
                threshold_traditional_array.append(threshold_traditional)
                threshold_new_array.append(threshold_new)

                if not is_traditional_reached and threshold_traditional > 0:
                    if total_conversion_count < Param.MIN_CONVERSION:
                        continue
                    is_traditional_reached = True
                    traditional_end_count = traditional_end_count + impression_counter
                    if mean_diff < 0:
                        traditional_err_count = traditional_err_count + 1
                        traditional_err_end_count = traditional_err_end_count + impression_counter
                    missed_traditional_conversion_count = total_optimal_conversion_count - total_conversion_count
                    optimal_traditional_conversion_count = optimal_traditional_conversion_count + total_optimal_conversion_count

                if not is_new_reached and threshold_new > 0:
                    is_new_reached = True
                    new_end_count = new_end_count + impression_counter
                    if mean_diff < 0:
                        new_err_count = new_err_count + 1
                        new_err_end_count = new_err_end_count + impression_counter
                    missed_new_conversion_count = total_optimal_conversion_count - total_conversion_count
                    optimal_new_conversion_count = optimal_new_conversion_count + total_optimal_conversion_count

                iteration_counter = iteration_counter + 1

            # If the condition is not reached then the inconclusive count adds one.
            if not is_traditional_reached:
                traditional_inconclusive_count = traditional_inconclusive_count + 1
            if not is_new_reached:
                new_inconclusive_count = new_inconclusive_count + 1

            total_missed_traditional_conversion_count = total_missed_traditional_conversion_count + missed_traditional_conversion_count
            total_missed_new_conversion_count = total_missed_new_conversion_count + missed_new_conversion_count

        if traditional_end_count == 0:
            traditional_avg = 0
            missed_traditional_conversion_avg = 0
        else:
            traditional_avg = traditional_end_count / (self.experiment_number - traditional_inconclusive_count)
            missed_traditional_conversion_avg = total_missed_traditional_conversion_count / (self.experiment_number - traditional_inconclusive_count)
            optimal_traditional_conversion_avg = optimal_traditional_conversion_count / (self.experiment_number - traditional_inconclusive_count)

        if new_end_count == 0:
            new_avg = 0
            missed_new_conversion_avg = 0
        else:
            new_avg = new_end_count / (self.experiment_number - new_inconclusive_count)
            missed_new_conversion_avg = total_missed_new_conversion_count / (self.experiment_number - new_inconclusive_count)
            optimal_new_conversion_avg = optimal_new_conversion_count / (self.experiment_number - new_inconclusive_count)

        if traditional_err_count == 0:
            traditional_err_end_avg = 0
        else:
            traditional_err_end_avg = traditional_err_end_count / traditional_err_count

        if new_err_count == 0:
            new_err_end_avg = 0
        else:
            new_err_end_avg = new_err_end_count / new_err_count

        if self.is_display:
            plt.plot(threshold_traditional_array)
            plt.plot(threshold_new_array)
            plt.plot(np.zeros(len(threshold_new_array)))
            plt.show()

        return (traditional_avg, new_avg, traditional_inconclusive_count,
            new_inconclusive_count, traditional_err_count, new_err_count,
            missed_traditional_conversion_avg, missed_new_conversion_avg,
            traditional_err_end_avg, new_err_end_avg,
            optimal_traditional_conversion_avg, optimal_new_conversion_avg
            )

    def run_bandit_experiment(self, conversion_simulator_function):
        bandit_method = MultiArmedBandits()

        bandit_end_count = 0
        bandit_err_count = 0
        bandit_err_end_count = 0
        bandit_inconclusive_count = 0
        total_missed_bandit_conversion_count = 0
        value_remaining_array = []
        fractions_array = []
        simple_lift_array = []
        relative_risk_array = []

        optimal_bandit_conversion_count = 0
        for i in range(self.experiment_number):
            print "experiment: ", i
            impression_counter_1 = 0
            impression_counter_2 = 0
            conversion_counter_1 = 0
            conversion_counter_2 = 0
            is_bandit_reached = False
            iteration_counter = 0
            fractions = [0.5, 0.5]

            conversion_array_1 = []
            conversion_array_2 = []
            impression_array_1 = []
            impression_array_2 = []

            total_optimal_conversion_count = 0
            total_conversion_count = 0
            missed_bandit_conversion_count = 0
            while (not is_bandit_reached) and iteration_counter < self.max_iteration:
                conversion_1, conversion_2, impression_1, impression_2, optimal_conversions = conversion_simulator_function(self.bin_size, iteration_counter, fractions)

                fractions_array.append(fractions)

                conversion_counter_1 = conversion_counter_1 + conversion_1
                conversion_counter_2 = conversion_counter_2 + conversion_2
                impression_counter_1 = impression_counter_1 + impression_1
                impression_counter_2 = impression_counter_2 + impression_2
                impression_counter = impression_counter_1 + impression_counter_2
                conversions = [conversion_counter_1, conversion_counter_2]
                impressions = [impression_counter_1, impression_counter_2]

                # Record all the changes.
                conversion_array_1.append(conversion_1)
                conversion_array_2.append(conversion_2)
                impression_array_1.append(impression_1)
                impression_array_2.append(impression_2)

                total_optimal_conversion_count = total_optimal_conversion_count + optimal_conversions
                total_conversion_count = total_conversion_count + conversion_1[0] + conversion_2[0]

                winner_index, value_remaining, fractions, is_stopped = \
                    bandit_method.get_bandit_fractions(impressions, conversions)
                if not is_bandit_reached and is_stopped:
                    if total_conversion_count < Param.MIN_CONVERSION:
                        continue
                    if conversion_counter_1 == 0 or conversion_counter_2 == 0: # There has to be some traffic
                        continue
                    is_bandit_reached = True
                    bandit_end_count = bandit_end_count + impression_counter
                    if winner_index != 1:
                        bandit_err_count = bandit_err_count + 1
                        bandit_err_end_count = bandit_err_end_count + impression_counter
                    missed_bandit_conversion_count = total_optimal_conversion_count - total_conversion_count
                    optimal_bandit_conversion_count = optimal_bandit_conversion_count + total_optimal_conversion_count
                iteration_counter = iteration_counter + 1

                value_remaining_array.append(value_remaining)

            # Bin the results into groups for calculation
            conversion_array_binned_1 = self.bin_data(conversion_array_1)
            conversion_array_binned_2 = self.bin_data(conversion_array_2)
            impression_array_binned_1 = self.bin_data(impression_array_1)
            impression_array_binned_2 = self.bin_data(impression_array_2)

            combined_data_array = np.array([impression_array_binned_1, conversion_array_binned_1, impression_array_binned_2, conversion_array_binned_2])
            combined_data_array = combined_data_array.T
            relative_risk = self.calc_aggregate_ab_metrics(combined_data_array)
            # if conversion_counter_1 == 0:
            #     print conversion_counter_2, impression_counter_2, conversion_counter_1, impression_counter_1
            simple_lift = np.log((conversion_counter_2/impression_counter_2) / (conversion_counter_1/impression_counter_1))

            # print "calc_aggregate_ab_metrics: ", relative_risk
            # print "simple: ", simple_lift

            simple_lift_array.append(simple_lift)
            relative_risk_array.append(relative_risk)

            # If the condition is not reached then the inconclusive count adds one.
            if not is_bandit_reached:
                bandit_inconclusive_count = bandit_inconclusive_count + 1

            total_missed_bandit_conversion_count = total_missed_bandit_conversion_count + missed_bandit_conversion_count

        if bandit_end_count == 0:
            bandit_avg = 0
            missed_bandit_conversion_avg = 0
            optimal_bandit_conversion_avg = 0
        else:
            bandit_avg = bandit_end_count / (self.experiment_number - bandit_inconclusive_count)
            missed_bandit_conversion_avg = total_missed_bandit_conversion_count / (self.experiment_number - bandit_inconclusive_count)
            optimal_bandit_conversion_avg = optimal_bandit_conversion_count / (self.experiment_number - bandit_inconclusive_count)

        if bandit_err_count == 0:
            bandit_err_end_avg = 0
        else:
            bandit_err_end_avg = bandit_err_end_count / bandit_err_count

        if self.is_display:
            plt.plot(value_remaining_array)
            fractions_array = np.array(fractions_array)
            plt.plot(fractions_array[:, 0])
            plt.plot(fractions_array[:, 1])
            plt.plot(np.zeros(len(value_remaining_array)))
            plt.show()

        mean_log_ratio_star = np.mean(simple_lift_array)
        print "mean simple: ", np.exp(np.mean(simple_lift_array))
        standard_error = np.std(simple_lift_array)
        bound_num = 1.0#1.96
        print "low simple bound: ", np.exp(mean_log_ratio_star - bound_num*standard_error)
        print "high simple bound: ", np.exp(mean_log_ratio_star + bound_num*standard_error)
        print "log std simple: ", np.exp(np.std(simple_lift_array))
        print "mean relative risk: ", np.mean(np.array(relative_risk_array), 0)
        print "std relative risk: ", np.std(np.array(relative_risk_array), 0)


        return (bandit_avg, bandit_inconclusive_count, bandit_err_count, missed_bandit_conversion_avg, bandit_err_end_avg, optimal_bandit_conversion_avg)

def random_walk(total_amount):
    ctr_start_pt = Param.BASE_PERCENT
    steps = np.random.normal(0, Param.RANDOM_WALK_STEP, total_amount)
    walk = ctr_start_pt
    walks = np.zeros(total_amount)
    for i in range(total_amount):
        walks[i] = walk
        walk = walk + steps[i]
        walk = Param.RANDOM_WALK_LOWER if walk < Param.RANDOM_WALK_LOWER else walk
        walk = Param.RANDOM_WALK_UPPER if walk > Param.RANDOM_WALK_UPPER else walk
    return walks

def simulator_function_simple(bin_size, count):
    ctr_array = [Param.BASE_PERCENT, Param.BASE_PERCENT * Param.LIFT]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_type1_err(bin_size, count):
    ctr_array = [Param.BASE_PERCENT, Param.BASE_PERCENT]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_exp_ctr(bin_size, count):
    ctr_array = np.array([Param.BASE_PERCENT, Param.BASE_PERCENT * Param.LIFT])
    ctr_array = ctr_array * np.exp(-count * Param.EXP_RATE)
    if ctr_array[0] < Param.EXP_STOP * Param.BASE_PERCENT:
        ctr_array = np.array([Param.EXP_STOP * Param.BASE_PERCENT, Param.EXP_STOP * Param.BASE_PERCENT * Param.LIFT])
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_exp_ctr_type1_err(bin_size, count):
    ctr_array = np.array([Param.BASE_PERCENT, Param.BASE_PERCENT])
    ctr_array = ctr_array * np.exp(-count * Param.EXP_RATE)
    if ctr_array[0] < Param.EXP_STOP * Param.BASE_PERCENT:
        ctr_array = np.array([Param.EXP_STOP * Param.BASE_PERCENT, Param.EXP_STOP * Param.BASE_PERCENT])
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_random_walk_preset(bin_size, count, walk_array):
    ctr_array = [walk_array[count], walk_array[count] * Param.LIFT]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_random_walk_preset_type1_err(bin_size, count, walk_array):
    ctr_array = [walk_array[count], walk_array[count]]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_simple(bin_size, count, fractions):
    ctr_array = [Param.BASE_PERCENT, Param.BASE_PERCENT * Param.LIFT]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_simple_type1_err(bin_size, count, fractions):
    ctr_array = [Param.BASE_PERCENT, Param.BASE_PERCENT]
    impression_1 = bin_size / 2
    impression_2 = bin_size / 2
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_constant(bin_size, count, fractions):
    ctr_array = [Param.BASE_PERCENT, Param.BASE_PERCENT * Param.LIFT]
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_constant_type1_err(bin_size, count, fractions):
    ctr_array = [Param.BASE_PERCENT, Param.BASE_PERCENT]
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_exp(bin_size, count, fractions):
    ctr_array = np.array([Param.BASE_PERCENT, Param.BASE_PERCENT * Param.LIFT])
    ctr_array = ctr_array * np.exp(-count * Param.EXP_RATE)
    if ctr_array[0] < Param.EXP_STOP * Param.BASE_PERCENT:
        ctr_array = np.array([Param.EXP_STOP * Param.BASE_PERCENT, Param.EXP_STOP * Param.BASE_PERCENT * Param.LIFT])
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_exp_type1_err(bin_size, count, fractions):
    ctr_array = np.array([Param.BASE_PERCENT, Param.BASE_PERCENT])
    ctr_array = ctr_array * np.exp(-count * Param.EXP_RATE)
    if ctr_array[0] < Param.EXP_STOP * Param.BASE_PERCENT:
        ctr_array = np.array([Param.EXP_STOP * Param.BASE_PERCENT, Param.EXP_STOP * Param.BASE_PERCENT])
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_random_walk_preset(bin_size, count, fractions, walk_array):
    ctr_array = [walk_array[count], walk_array[count] * Param.LIFT]
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

def simulator_function_bandit_random_walk_preset_type1_err(bin_size, count, fractions, walk_array):
    ctr_array = [walk_array[count], walk_array[count]]
    impression_1 = bin_size * fractions[0]
    impression_2 = bin_size - impression_1
    conversion_1 = np.random.binomial(impression_1, ctr_array[0], 1)
    conversion_2 = np.random.binomial(impression_2, ctr_array[1], 1)
    optimal_conversions = bin_size * ctr_array[1]
    return (conversion_1, conversion_2, impression_1, impression_2, optimal_conversions)

# For testing and verification
def get_samples_bandit_exp():
    result = map(lambda x : simulator_function_bandit_exp(200, x, [0.5, 0.5]), range(1, 1000))
    result = np.array(map(lambda x : [x[0][0], x[1][0], x[4]], result))
    return result

# For testing and verification
def get_samples_sequencial_exp():
    result = map(lambda x : simulator_function_exp_ctr(200, x), range(1, 1000))
    result = np.array(map(lambda x : [x[0][0], x[1][0], x[4]], result))
    return result

def get_randomwalk_sequencial():
    random_walk_array = random_walk(10000)
    result = map(lambda x : simulator_function_random_walk_preset(200, x, random_walk_array), range(1, 1000))
    result = np.array(map(lambda x : [x[0][0], x[1][0], x[4]], result))
    return result

def get_randomwalk_bandit():
    random_walk_array = random_walk(10000)
    result = map(lambda x : simulator_function_bandit_random_walk_preset(200, x, [0.5, 0.5], random_walk_array), range(1, 1000))
    result = np.array(map(lambda x : [x[0][0], x[1][0], x[4]], result))
    return result

def test_by_plot():
    # a = get_samples_bandit_exp()
    # a = get_samples_sequencial_exp()
    # a = get_randomwalk_sequencial()
    a = get_randomwalk_bandit()
    plt.plot(a)
    print sum(a)
    plt.show()

def min_conversion_to_error_experiment():
    stat_simulator = StatsOptimizingSimulator(bin_size = 200, experiment_number = 500, is_display = False)
    traditional_error_array = []
    new_error_array = []
    traditional_avg_array = []
    new_avg_array = []
    bandit_error_array = []
    bandit_avg_array = []
    for i in np.arange(0, 201, 10):
        Param.MIN_CONVERSION = i
        (traditional_avg, new_avg, traditional_inconclusive_count,\
         new_inconclusive_count, traditional_err_count, new_err_count,\
         missed_traditional_conversion_avg, missed_new_conversion_avg,\
         traditional_err_end_avg, new_err_end_avg,\
         optimal_traditional_conversion_avg, optimal_new_conversion_avg) = stat_simulator.run_sequencial_experiment(simulator_function_simple)
        (bandit_avg, bandit_inconclusive_count, bandit_err_count, missed_bandit_conversion_avg, bandit_err_end_avg, optimal_bandit_conversion_avg) = \
            stat_simulator.run_bandit_experiment(simulator_function_bandit_constant)
        traditional_error_array.append(traditional_err_count)
        new_error_array.append(new_err_count)
        traditional_avg_array.append(traditional_avg)
        new_avg_array.append(new_avg)
        bandit_error_array.append(bandit_err_count)
        bandit_avg_array.append(bandit_avg)
        # print "err=%d, avg=%d" % (bandit_err_count, bandit_avg)
    plt.plot(np.arange(0, 201, 10), traditional_error_array)
    plt.plot(np.arange(0, 201, 10), new_error_array)
    plt.plot(np.arange(0, 201, 10), bandit_error_array)
    plt.show()
    plt.plot(np.arange(0, 201, 10), traditional_avg_array)
    plt.plot(np.arange(0, 201, 10), new_avg_array)
    plt.plot(np.arange(0, 201, 10), bandit_avg_array)
    plt.show()

def min_conversion_to_error_exp_experiment():
    stat_simulator = StatsOptimizingSimulator(bin_size = 200, experiment_number = 500, is_display = False)
    traditional_error_array = []
    new_error_array = []
    traditional_avg_array = []
    new_avg_array = []
    bandit_error_array = []
    bandit_avg_array = []
    for i in np.arange(0, 201, 10):
        Param.MIN_CONVERSION = i
        (traditional_avg, new_avg, traditional_inconclusive_count,\
         new_inconclusive_count, traditional_err_count, new_err_count,\
         missed_traditional_conversion_avg, missed_new_conversion_avg,\
         traditional_err_end_avg, new_err_end_avg,\
         optimal_traditional_conversion_avg, optimal_new_conversion_avg) = stat_simulator.run_sequencial_experiment(simulator_function_exp_ctr)
        (bandit_avg, bandit_inconclusive_count, bandit_err_count, missed_bandit_conversion_avg, bandit_err_end_avg, optimal_bandit_conversion_avg) = \
            stat_simulator.run_bandit_experiment(simulator_function_bandit_exp)
        traditional_error_array.append(traditional_err_count)
        new_error_array.append(new_err_count)
        traditional_avg_array.append(traditional_avg)
        new_avg_array.append(new_avg)
        bandit_error_array.append(bandit_err_count)
        bandit_avg_array.append(bandit_avg)
        # print "err=%d, avg=%d" % (bandit_err_count, bandit_avg)
    plt.plot(np.arange(0, 201, 10), traditional_error_array)
    plt.plot(np.arange(0, 201, 10), new_error_array)
    plt.plot(np.arange(0, 201, 10), bandit_error_array)
    plt.show()
    plt.plot(np.arange(0, 201, 10), traditional_avg_array)
    plt.plot(np.arange(0, 201, 10), new_avg_array)
    plt.plot(np.arange(0, 201, 10), bandit_avg_array)
    plt.show()
    
def simulator():
    # min_conversion_to_error_experiment()
    # min_conversion_to_error_exp_experiment()
    # return
    random_walk_array = random_walk(10000)
    stat_simulator = StatsOptimizingSimulator(bin_size = 200, experiment_number = 100, is_display = False)
    # Start the testing.
    print "Sequencial Testing"
    print """(traditional_avg, new_avg, traditional_inconclusive_count,
              new_inconclusive_count, traditional_err_count, new_err_count,
              missed_traditional_conversion_avg, missed_new_conversion_avg,
              traditional_err_end_avg, new_err_end_avg,
              optimal_traditional_conversion_avg, optimal_new_conversion_avg)"""
    # print stat_simulator.run_sequencial_experiment(simulator_function_simple)
    # print stat_simulator.run_sequencial_experiment(simulator_function_type1_err)
    # print stat_simulator.run_sequencial_experiment(simulator_function_exp_ctr)
    # print stat_simulator.run_sequencial_experiment(simulator_function_exp_ctr_type1_err)
    # print stat_simulator.run_sequencial_experiment(lambda x, y: simulator_function_random_walk_preset(x, y, random_walk_array))
    # print stat_simulator.run_sequencial_experiment(lambda x, y: simulator_function_random_walk_preset_type1_err(x, y, random_walk_array))
    
    print "Bandit Testing"
    print """(bandit_avg, bandit_inconclusive_count, bandit_err_count,
              missed_bandit_conversion_avg, bandit_err_end_avg, optimal_bandit_conversion_avg)"""
    # print stat_simulator.run_bandit_experiment(simulator_function_bandit_simple)
    # print stat_simulator.run_bandit_experiment(simulator_function_bandit_simple_type1_err)
    # print stat_simulator.run_bandit_experiment(simulator_function_bandit_constant)
    print stat_simulator.run_bandit_experiment(simulator_function_bandit_constant_type1_err)
    # print stat_simulator.run_bandit_experiment(simulator_function_bandit_exp)
    # print stat_simulator.run_bandit_experiment(simulator_function_bandit_exp_type1_err)
    # print stat_simulator.run_bandit_experiment(lambda x, y, z: simulator_function_bandit_random_walk_preset(x, y, z, random_walk_array))
    # print stat_simulator.run_bandit_experiment(lambda x, y, z: simulator_function_bandit_random_walk_preset_type1_err(x, y, z, random_walk_array))

if __name__ == '__main__':
    simulator()
