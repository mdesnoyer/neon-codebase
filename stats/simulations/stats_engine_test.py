import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as spstats
import itertools

class OptStatsEngine(object):
    def __init__(self, tau = 0.0001, alpha = 0.5):
        self.tau = tau
        self.alpha = alpha
        self.xn = 0.004
        self.yn = 0.005

    def get_vn(self, n):
        vn = self.xn*(1-self.xn)/float(n/2) + self.yn*(1-self.yn)/float(n/2)
        return vn

    def get_new_vn(self, vn):
        new_vn = np.sqrt((2 * np.log(1 / self.alpha) - np.log(vn / (vn + self.tau))) * ((vn * (vn + self.tau)) / self.tau))
        return new_vn

    def calculate_significance(self, n):
        theta_n = self.yn - self.xn
        vn = self.get_vn(n)
        vn_new = self.get_new_vn(vn)
        threshold_new = theta_n - vn_new
        return threshold_new

    def stats_plot(self):
        # To plot the function.
        experiment_size = 50000
        step = 10
        significances = np.zeros(experiment_size)
        count = 0
        is_found = False
        for n in np.arange(step, step*experiment_size, step):
            significances[count] = self.calculate_significance(n)
            if not is_found and significances[count] > 0:
                print("At %d, it ends." % n)
                is_found = True
            count = count + 1
        plt.plot(significances)
        plt.plot(np.zeros(experiment_size))
        plt.show()

if __name__ == '__main__':
    stats_engine = OptStatsEngine()
    stats_engine.stats_plot()
