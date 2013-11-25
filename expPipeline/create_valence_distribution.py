#!/usr/bin/env python
''' Script that bins the valence scores.

'''
import csv
import logging
import sys
import numpy as np
from optparse import OptionParser


def main(options):

    score_map = {}
        
    with open(options.input) as f:
        for fields in csv.reader(f):
            score_map[fields[0]] = float(fields[1])

    fnames, scores = zip(*score_map.items())

    # Limit to +- 3 stddev so that outliers don't throw off the bins
    mn = np.mean(scores)
    stdev = np.std(scores)
    lowRange = max(min(scores), mn - 3*stdev)
    hiRange = min(max(scores), mn + 3*stdev)

    hist, bins = np.histogram(scores, options.nbins, range=(lowRange, hiRange))
    bins[0] = min(scores)
    bins[-1] = max(scores)
    new_scores = np.digitize(scores, bins)

    with open(options.output,'w') as f:
        writer = csv.writer(f)
        for fname, score in zip(fnames, new_scores):
            writer.writerow([fname, score])

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--input', '-i', default=None,
                      help='File with input scores in raw valences from mturk task')
    parser.add_option('--output', '-o', default=None,
                      help='File with binned output scores')
    parser.add_option('--nbins', type='int', default=7,
                      help='Number of bins')
    
    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(options)


