#!/usr/bin/env python
'''Script that aggregates CTR stats across videos.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2013
'''

import logging
from optparse import OptionParser
import numpy as np
import scipy.stats

_log = logging.getLogger(__name__)

# Data, for now. Deal with inputs later.
# <video_name>,<base imp>,<base click>,<Neon imp>,<Neon click>
data = [
    ['Adil1',10883,256,11018,296],
    ['Adil2',14958,307,14667,751],
    ['Adil3',8429,110,8409,107],
    ['Always1',43811,831,43925,901],
    ['Duo1',50009,731,50256,963],
    ['Guitar1',62391,689,62836,982],
    ['Keith1',8556,183,8543,150],
    ['Keith2',12452,525,12670,303],
    ['Keith3',12337,203,12550,84],
    ['Keith4',9603,127,9541,176],
    ['Little1',19731,171,19522,139],
    ['Little2',34173,624,34139,757],
    ['Open1',30329,1000,29792,842],
    ['Ora1',28642,561,28654,895],
    ['Ora2',14840,108,14704,146],
    ['Pay1',46899,907,46187,750],
    ['Pose1',20406,933,20321,605],
    ['Pose2',6697,132,6756,119],
    ['Pose3',3608,13,3480,7]]

def main(options):
    ctr_base = np.array([float(x[2])/x[1] for x in data])
    n_base = np.array([x[1] for x in data])
    ctr_neon = np.array([float(x[4])/x[3] for x in data])
    n_neon = np.array([x[3] for x in data])

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

    _log.info('''With %i entries:
    Mean: %3.2f %%
    P_Value: %3.2f
    Lower 95%% confidence interval: %3.2f %%
    Upper 95%% confidence interval: %3.2f %%
    ''' % (len(data), (mn - 1)*100, p_value, (low-1)*100, (up-1)*100))

if __name__ == '__main__':
    parser = OptionParser()
    
    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(options)
