#!/usr/bin/env python
'''Tools for validating the model.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path

if __name__ == '__main__':
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                    '..', '..')))

import copy
import cv2
import logging
import math
import matplotlib.pyplot as plt
import model
import numpy as np
from optparse import OptionParser
import PIL
import random
import re
import scipy
import scipy.interpolate
import scipy.stats
import sys
from model.train_model import parse_image_scores

_log = logging.getLogger(__name__)

class Metric(object):
    def __init__(self):
        self.results = []

    def append_result(self, true_scores, predicted_scores):
        '''Appends the metric between true_scores and predicted_scores.

        true_scores - Vector of true scores
        predicted_scores - Vector of predicted scores
        '''
        self.results.append(self.calculate(true_scores, predicted_scores))

    def calculate(self, true_scores, predicted_scores):
        '''Calculates the metric.

        true_scores - Vector of true scores
        predicted_scores - Vector of predicted scores
        
        '''
        raise NotImplementedError()

class ScalarMetric(Metric):
    def __init__(self):
        super(ScalarMetric, self).__init__()

    def describe(self):
        return '%s: %f (+-%f)' % (self.__class__.__name__,
                                  np.mean(self.results),
                                  np.std(self.results))

class RMSError(ScalarMetric):
    '''The root mean squared error.'''
    def __init__(self):
        super(RMSError, self).__init__()
    
    def calculate(self, true_scores, predicted_scores):
        return np.sqrt(np.mean(np.square(true_scores - predicted_scores)))

class PearsonRankCoefficient(ScalarMetric):
    '''The pearson rank coefficient.

    From -1 to 1 where 0 is uncorrelated. Assumes linear relationship.

    '''
    def __init__(self):
        super(PearsonRankCoefficient, self).__init__()

    def calculate(self, true_scores, predicted_scores):
        return scipy.stats.pearsonr(predicted_scores, true_scores)[0]

class SpearmanRankCoefficient(ScalarMetric):
    '''The spearman rank coefficient.

    From -1 to 1 where 0 is uncorrelated. Measures ordering.

    '''
    def __init__(self):
        super(SpearmanRankCoefficient, self).__init__()

    def calculate(self, true_scores, predicted_scores):
        return scipy.stats.spearmanr(predicted_scores, true_scores)[0]

class KendallTau(ScalarMetric):
    '''The kendall tau metric.

    From -1 to 1 where 0 is uncorrelated. Measures how consistent the
    ordering is.

    '''
    def __init__(self):
        super(KendallTau, self).__init__()

    def calculate(self, true_scores, predicted_scores):
        return scipy.stats.kendalltau(predicted_scores, true_scores)[0]
    
class PlottingMetric(Metric):
    def __init__(self):
        super(PlottingMetric, self).__init__()

    def plot(self):
        '''Plots the data.

        The default approach assumes that self.results is a of the
        form [([x], [y])]. We then plot the mean y with std regions
        around it being transparent.

        '''
        plt.figure()
        xVals, meanY, stdY, stdErrorY = self._calculate_line_stats(
            self.results)

        # Do the plotting
        plt.fill_between(xVals, meanY - stdY, meanY + stdY, alpha=0.5)
        plt.plot(xVals, meanY, linewidth=2)

    def _calculate_line_stats(self, data):
        '''Calculates some statistics about a set of lines.

        Inputs:
        data - List of lines where the points are specified [([x], [y])]

        Outputs:
        Tuple of lists that can be used to plot.
        Tuple is (x, meanY, stdY, stdErrorY)
        
        '''
        # Go through all the datasets and get the x values we care to
        # sample about
        xVals = set()
        for x, y in data:
            xVals.update(x)
        xVals = sorted(xVals)

        # Get samples of all the y values at the x values we care
        # about, interpolating if necessary
        y_stack = []
        for x, y in data:
            f = scipy.interpolate.interp1d(
                x,
                y,
                kind='linear',
                bounds_error=False,
                fill_value=float('nan'))
            y_stack.append(f(xVals))

        # Calculate the stats of the y values
        meanY = scipy.stats.nanmean(y_stack, axis=0)
        stdY = scipy.stats.nanstd(y_stack, axis=0)
        stdY = np.nan_to_num(stdY)
        stdErrorY = stdY / np.sqrt(np.sum(np.isfinite(y_stack), axis=0))

        return (xVals, meanY, stdY, stdErrorY)

class EnergyPrecision(PlottingMetric):
    '''Metric that is equivalent to a precision recall curve but for regression.

    This is done by ranking entries by their predicted score and
    comparing that to the rank by the the true score. To calculate the
    "precision", we take the top N entries sorted by predicted score
    and sum their true scores and then divide that by the maximum sum
    of N examples in the true score. We can call the sum of the true
    scores, the "energy". The "recall" is then the fraction of energy
    in the top N entries divided by the total possible energy.

    '''
    
    def __init__(self):
        super(EnergyPrecision, self).__init__()

        self.example_scores = []

    def calculate(self, true_scores, predicted_scores):
        # Sort the entries by their predicted scores in descending order
        sortI = sorted(range(len(true_scores)),
                       reverse = True,
                       key = lambda i: (predicted_scores[i]))
        best_sort = np.sort(np.array(true_scores, dtype=np.float))[::-1]
        true_sort = [true_scores[x] for x in sortI]

        best_energy = np.cumsum(best_sort)
        energy = np.cumsum(true_sort)

        precision = energy / best_energy
        recall = energy / energy[-1]

        self.example_scores = true_scores

        return (recall, precision)

    def plot(self):
        super(EnergyPrecision, self).plot()

        # Using the example scores from one of the calculate() calls,
        # estimate what a random ordering would look like on the
        # graph.
        rand_results = []
        for i in range(20):
            shuffled_scores = copy.deepcopy(self.example_scores)
            random.shuffle(shuffled_scores)
            rand_results.append(
                self.calculate(self.example_scores, shuffled_scores))
        randX, randY, randStdY, randStdErrorY = self._calculate_line_stats(
            rand_results)
        plt.plot(randX, randY, linewidth=2, color='r')
        plt.legend(['Model', 'Random'], loc=4)

        plt.xlabel('Recall of Valence Mass')
        plt.ylabel('Precision of Valence Mass')
        plt.title('Valence Mass PR')

class ConfusionMatrix(PlottingMetric):
    def __init__(self, bins=7):
        super(ConfusionMatrix, self).__init__()
        self.bins = float(bins)

    def calculate(self, true_scores, predicted_scores):
        mat = np.zeros(shape=(self.bins, self.bins), dtype=np.float)

        max_score = max(true_scores)
        min_score = min(true_scores)

        for true_score, pred_score in zip(true_scores, predicted_scores):
            tbin = int((true_score - min_score) * self.bins / 
                       (max_score - min_score) - 0.01)
            pbin = int((pred_score - min_score) * self.bins /
                       (max_score - min_score) - 0.01)
            
            mat[tbin, pbin] += 1

        return mat / np.sum(mat, axis=None)
        
    def plot(self):
        mat = np.mean(self.results, axis=0)
        #mat = np.divide(mat, np.tile(np.sum(mat, axis=0), (mat.shape[0],1)))

        plt.figure()
        plt.cla()
        plt.imshow(mat, aspect='auto', interpolation='nearest',
                   origin='lower')
        plt.colorbar()
        

class NFoldCrossValidation:
    '''Cross validataion for checking a valence predictor.'''
    def __init__(self, n, metrics, predictor, seed=None):
        '''Create an NFold cross validation object.

        n - Number of ways to split the data up
        metrics - List of Metric objects that will capture metrics
        seed - Random seed to mix the data. If None, randomization is not done
        predictor - The ValencePredictor to run the cross validation on
        '''
        self.seed = seed
        self.metrics = metrics
        self.n = n
        self.predictor = copy.deepcopy(predictor)
        self.predictor.reset()

    def run(self, img_list, scores):
        '''Runs the cross validation

        Inputs:
        img_list - list of image files to read
        scores - list of scores, one for each file
        '''

        # If called for, randomize the data
        sample_idx = range(len(img_list))
        if self.seed is not None:
            random.seed(self.seed)
            random.shuffle(sample_idx)
        img_list = [img_list[i] for i in sample_idx]
        scores = [scores[i] for i in sample_idx]

        _log.info('Calculating the features for all the images.')
        data = []
        img_cnt = 0
        for img_file in img_list:
            cur_image = cv2.imread(img_file)
            if cur_image is None:
                _log.critical('Could not find image %s. Exiting.' % img_file)
                sys.exit(1)
            descriptor = self.predictor.feature_generator.generate(cur_image)
            data.append(descriptor)

            img_cnt +=1
            if img_cnt % 100 == 0:
                _log.info('Processed %i images' % img_cnt)

        # Go through the folds
        nsamples = len(scores)
        step = int(float(nsamples)) / self.n
        for i in xrange(self.n):
            _log.info('Processing fold %i' % i)
            test_idx = set(range((i*step), ((i+1)*step)))
            test_images = [img_list[x] for x in test_idx]
            test_scores = np.array([scores[x] for x in test_idx])
            train_data = [(data[j], scores[j]) for j in range(nsamples)
                          if j not in test_idx]
            
            
            cur_predictor = copy.deepcopy(self.predictor)
            for feature, score in train_data:
                cur_predictor.add_feature_vector(feature, score)

            _log.info('Training the predictor')
            cur_predictor.train()

            _log.info('Testing the predictor')
            predict_scores = np.array([
                cur_predictor.predict(cv2.imread(img_file))
                for img_file in test_images])

            for metric in self.metrics:
                metric.append_result(test_scores, predict_scores)

        _log.info('Finished cross validation. Results are:')
        for metric in self.metrics:
            if isinstance(metric, ScalarMetric):
                _log.info(metric.describe())
            elif isinstance(metric, PlottingMetric):
                metric.plot()

def plot_neighbours(predictor, img_files, labels, n=8):
    plt.figure(figsize=(16, 12), dpi=80)

    for row in range(len(img_files)):
        test_image = cv2.imread(img_files[row])
        frame = plt.subplot(len(img_files), n, row*n + 1)
        frame.axes.get_xaxis().set_ticks([])
        frame.axes.get_yaxis().set_visible(False)
        plt.imshow(test_image[:,:,::-1])
        plt.xlabel(labels[row])
        
        neighbours = predictor.get_neighbours(test_image, k=n-1)
        fig_num = 2
        for score, dist, n_file in neighbours:
            frame = plt.subplot(len(img_files), n, row*n + fig_num)
            frame.axes.get_xaxis().set_ticks([])
            frame.axes.get_yaxis().set_visible(False)
            fig_num += 1
            plt.imshow(cv2.imread(n_file)[:,:,::-1])
            plt.xlabel('s: %3.2f, d: %3.2f' % (score, dist))

    plt.tight_layout()
        

def show_extremes(img_files, scores, predictor):
    _log.info('Doing processing to find the most extreme examples')
    
    # Split into training and testing
    ntest = int(float(len(img_files))*0.2)
    test_set = zip(img_files[0:ntest], scores[0:ntest])
    train_set = zip(img_files[ntest:], scores[ntest:])

    # Train the predictor
    for img_file, score in train_set:
        predictor.add_image(cv2.imread(img_file), score, img_file)
    predictor.seed = 198652394
    predictor.train()

    # Get the amount of the error
    results = []
    for img_file, score in test_set:
        error = predictor.predict(cv2.imread(img_file)) - score
        results.append((error, img_file, score))
    results = sorted(results)

    nrows = 7

    # The most underrated
    plot_neighbours(predictor, [x[1] for x in results[0:nrows]],
                    [x[2] for x in results[0:nrows]])

    # The most overrated
    plot_neighbours(predictor, [x[1] for x in results[-nrows:]],
                    [x[2] for x in results[-nrows:]])

    # The most accurate
    results = sorted(results, key = lambda x: (abs(x[0]), -x[2]))
    plot_neighbours(predictor, [x[1] for x in results[0:nrows]],
                    [x[2] for x in results[0:nrows]])

    # The best prediction of the highly rated images
    results = sorted(results, key = lambda x: (-x[2], abs(x[0])))
    plot_neighbours(predictor, [x[1] for x in results[0:nrows]],
                    [x[2] for x in results[0:nrows]])

def show_labeled_images(img_files, labels, fig_title=None, grid_size=(5,5)):
    plt.figure(figsize=(grid_size[0]*4, grid_size[1]*3), dpi=50)
    if fig_title is not None:
        plt.title(fig_title)

    for i in range(grid_size[0]*grid_size[1]):
        if i >= len(img_files):
            break

        frame = plt.subplot(grid_size[0], grid_size[1], i)
        frame.axes.get_xaxis().set_ticks([])
        frame.axes.get_yaxis().set_visible(False)
        plt.imshow(cv2.imread(img_files[i])[:,:,::-1])
        plt.xlabel(labels[i])

    plt.tight_layout()
    

def show_filter_analysis(img_files, scores, filt):
    '''Does an analysis of how well the filter works for filtering really
    bad examples.

    '''
    scores = np.array(scores, dtype=np.float)
    sortI = sorted(range(len(scores)), key=lambda i: scores[i])
    sort_scores = np.sort(scores)
    sort_imgs = [img_files[i] for i in sortI]
    
    # Figure out which images pass the filter
    try:
        fmeas = np.array([filt.score(cv2.imread(x)) for x in sort_imgs])
    except NotImplementedError:
        fmeas = [0 for x in range(len(sort_imgs))]
    accepted = np.array([filt.accept(cv2.imread(x)) for x in sort_imgs])
    filtered = np.logical_not(accepted)

    # Print the precision and recall of filtering those examples below
    # a percentile
    for percentile in [0.02, 0.11, 0.3, 0.5]:
        low_thresh = sort_scores[int(len(scores) * percentile)]
        below = scores <= low_thresh
        low_p = (float(np.sum(np.logical_and(filtered, below))) 
                 / np.sum(filtered))
        low_r = (float(np.sum(np.logical_and(filtered, below))) 
                 / np.sum(below))
        _log.info('Examples below %3.2f that should be filtered. P=%f R=%f' %
                  (low_thresh, low_p, low_r))

        high_thresh = sort_scores[int(len(scores) * (1- percentile))]
        above = scores >= high_thresh
        high_p = (float(np.sum(np.logical_and(accepted, above)))
                  / np.sum(accepted))
        high_r = (float(np.sum(np.logical_and(accepted, above))) 
                  / np.sum(above))
        _log.info('Examples above %3.2f that should be accepted. P=%f R=%f' %
                  (high_thresh, high_p, high_r))

    # Display examples that were filtered and had high valence
    filteredI = np.nonzero(filtered)[0]
    show_labeled_images([sort_imgs[i] for i in filteredI[::-1]],
                        ['%s v:%3.2f s:%3.2f' % (
                            os.path.basename(sort_imgs[i]), sort_scores[i],
                                             fmeas[i]) 
                         for i in filteredI[::-1]],
                        'Filtered with High Valence')

    # Display examples that were filtered and had low valence
    show_labeled_images([sort_imgs[i] for i in filteredI],
                        ['%s v:%3.2f s:%3.2f' % (
                            os.path.basename(sort_imgs[i]), sort_scores[i],
                            fmeas[i]) 
                         for i in filteredI],
                        'Filtered with Low Valence')

    # Display examples that were not filtered and had low valence
    acceptedI = np.nonzero(accepted)[0]
    show_labeled_images([sort_imgs[i] for i in acceptedI],
                        ['%s v:%3.2f s:%3.2f' % (
                            os.path.basename(sort_imgs[i]), sort_scores[i],
                            fmeas[i]) 
                         for i in acceptedI],
                        'Not filtered with Low Valence')
    

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--model', default=None,
                      help='File containing the model')
    parser.add_option('--scores', default=None,
                      help='File containing "<filename>,<score>" on each line')
    parser.add_option('--image_source', default=None,
                      help='File containing "<img_id> <url>" on each line')
    parser.add_option('--img_id_regex', default='([a-zA-Z0-9_-]+)\.jpg',
                      help='Regex for extracting the image id from a filename')
    parser.add_option('--image_dir', default=None,
                      help='Directory with the images')
    parser.add_option('--n_fold', type='int', default=5,
                      help='Number of folds to do in the cross validation')
    parser.add_option('--seed', type='int', default=16984,
                      help='Random seed')
    parser.add_option('--do_xvalidation', action='store_true', default=False, 
                      help='Force cross validation to occur')
    parser.add_option('--show_extremes', action='store_true', default=False,
                      help='For display of the extereme examples')
    parser.add_option('--show_filter_analysis', action='store_true',
                      default=False,
                      help='Show the analysis of how the filter is doing')
    
    options, args = parser.parse_args()
    random.seed(options.seed)

    logging.basicConfig(level=logging.INFO)

    img_id_regex = re.compile(options.img_id_regex)

    img_files, scores = parse_image_scores(options.scores,
                                           options.image_source,
                                           img_id_regex,
                                           options.image_dir)

    mod = model.load_model(options.model)

    if options.do_xvalidation:
        validator = NFoldCrossValidation(
            options.n_fold,
            [RMSError(),
             PearsonRankCoefficient(),
             SpearmanRankCoefficient(),
             KendallTau(),
             EnergyPrecision(),
             ConfusionMatrix()],
             mod.predictor)

        validator.run(img_files, scores)

    if options.show_extremes:
        show_extremes(img_files, scores, copy.deepcopy(mod.predictor))

    # if options.show_filter_analysis:
    #     show_filter_analysis(img_files, scores, model.filters.TextFilter(0.02))

    plt.show()
