#!/usr/bin/env python
'''Script that aggregates CTR stats across videos.

Using random effects model assumption and meta analysis math from:
http://www.meta-analysis.com/downloads/Intro_Models.pdf

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2013
'''

import logging
from optparse import OptionParser
import math
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

def significance_calculator(lb, cb, la, ca):
    '''
    AB Test significance calculator
    '''

    ctr_a = ca/la
    ctr_b = cb/lb
    se_a = math.sqrt(ctr_a *(1-ctr_a)/la)
    se_b = math.sqrt(ctr_b *(1-ctr_b)/lb)
    x = (ctr_b - ctr_a)/ (math.sqrt( se_a**2 + se_b**2 ))
    p_value = scipy.stats.norm(0,1).cdf(x)
    if p_value >= 0.95 or p_value <= 0.05:
        return True
    return False

def main(options):
    if options.filename:
        global data
        data = []
        with open(options.filename, 'r') as f:
            for line in f.readlines():
                print line
                d = line.split(' ')
                d[-1] = d[-1].rstrip('\n')
                for i in range(len(d) -1):
                    i = i+1
                    d[i] = float(d[i])
                
                if options.significant:
                    if not significance_calculator(d[1], d[2], d[3], d[4]):
                        continue
                data.append(d)
    
    ctr_base = np.array([float(x[2])/float(x[1]) for x in data])
    n_base = np.array([x[1] for x in data])
    ctr_neon = np.array([float(x[4])/float(x[3]) for x in data])
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
    95%% confidence interval: (%3.2f %%, %3.2f %%)
    Percent of error from random effects: %3.2f %%
    ''' % (len(data),
           (mn - 1)*100,
           p_value,
           (low-1)*100,
           (up-1)*100,
           (1 - np.sqrt(1/w_sum) / standard_error)*100))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                        help="write report to FILE", metavar="FILE")
    parser.add_option("-s", "--significant", dest="significant",
                        help="AB test significance", default=None)

    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(options)
