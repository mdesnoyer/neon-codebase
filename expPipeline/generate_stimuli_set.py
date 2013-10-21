#!/usr/bin/env python
'''
Script that generates new stimuli sets from images that haven't been labeled yet. 

The stimuli set is created by identifying N regions in image space and 
placing all candidate images into those regions. Then, one image is taken 
from each region to create a set. The image chosen in each region is 
prioritized by the average distance to the nearest K examples that have been 
labeled (larger distance is better). These images are then added to those
that will be labeled and the priority queues are recalculated. We stop when
one of the queues is empty.

Copyright: 2013 Neon Labs
Author: Sunil Mallya (mallaya@neon-lab.com)
        Mark Desnoyer (desnoyer@neon-lab.com)
'''
USAGE = '%prog [options]'

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
import model
import model.features

import cv2
import heapq
import logging
import re
import os
import numpy as np
from optparse import OptionParser
import cPickle as pickle
import pyflann
from PIL import Image
import scipy.spatial.distance
import utils.logs
import youtube_video_id_scraper as yt_scraper

_log = logging.getLogger(__name__)

def load_gist_generator(cache_dir=None):
    generator = model.features.GistGenerator()
    if cache_dir is not None:
        generator = model.features.DiskCachedFeatures(generator, cache_dir)

    return generator

def parse_image_db(imdb_file, aspect_ratio, image_dir):
    '''Parses the image database file.

    Inputs:
    imgdb_file: file that contains the image database
    aspect_ratio: string specifying the aspect ratio to accept
    image_dir: directory that contains the images

    Outputs: [(image_file, video_url)], [known_ids]

    '''
    _log.info('Loading the image database file: %s' % imdb_file)
    retval = []
    known_ids = []
    with open(imdb_file) as f:
        for line in f:
            fields = line.split()
            if len(fields) < 2:
                continue
            known_ids.append(videoid_from_url(fields[1]))
            if float(fields[3]) == aspect_ratio:
                cur_file = '%s.jpg' % fields[0]
                if not os.path.exists(os.path.join(image_dir, cur_file)):
                    _log.warn(
                        'Image is in the database but cannot be found: %s' %
                        cur_file)
                else:
                    retval.append(('%s.jpg' % fields[0], fields[1]))

    return (retval, set(known_ids))

def find_labeled_files(stimuli_dir, image_dir):
    '''Find all the images that are already in a stimuli set.'''
    labeled = set()
    for root, dirs, files in os.walk(stimuli_dir, followlinks=True):
        for name in files:
            if os.path.exists(os.path.join(image_dir, name)):
                labeled.add(name)
            else:
                _log.debug('Image is in a stimuli set, but cannot be found: %s'
                           % name)

    return labeled

def load_codebook(codebook):
    '''Returns (example_urls, centers, whitening_vector) from the codebook.'''
    if codebook is None:
        return (None, None, 1.0)
    with open(codebook, 'rb') as f:
        return pickle.load(f)

def merge_example_urls(codebook_urls, examples_file):
    if examples_file is not None:
        _log.info('Loading example urls from %s' % examples_file)
        with open(examples_file, 'rb') as f:
            new_examples = pickle.load(f)

        if len(codebook_urls) <> len(new_examples):
            _log.error('Wrong number of clusters')
            return codebook_urls

        for cluster_id in range(len(new_examples)):
            codebook_urls[cluster_id].extend(new_examples[cluster_id])
    return codebook_urls

def generate_features(image_files, image_dir, white_vector, generator):
    '''Creates a matrix of features.

    The matrix is one image per row and the features a divided by the
    whitening vector.

    '''
    features = []
    _log.info('Loading features from %i images' % len(image_files))
    for cur_file in image_files:
        vec = generator.generate(cv2.imread(os.path.join(image_dir, cur_file)))
        features.append(vec / white_vector)

    return np.array(features)


def create_cluster_queues(codebook):
    '''Create a list of heaps. One for each cluster.'''
    if codebook is None:
        return [[]]
    return [[] for x in range(codebook.shape[0])]

def calc_mean_dist(knn_index, example, k=5):
    '''Calculates the mean distance^2 to the k nearest neighbours of the example.'''
    garb, dists = knn_index.nn_index(example, 5, checks=3)
    return np.mean(dists)

def assign_examples_to_clusters(examples, clusters, priority_func,
                                codebook=None):
    '''Places examples in their cluster with a priority.

    The clusters are a list of heaps where the entries are (p_val, example_idx)

    If codebook is None, only use a single heap with (p_val, example_idx)

    The id of the cluster corresponding to black frames is also returned
    (if found) so that the cluster doesn't limit stimuli sets from being
    generated.

    returns: (clusters, black_cluster)
    '''
    _log.info('Assigning %i examples to %i clusters' % (len(examples),
                                                        len(clusters)))
    black_cluster = None

    if codebook is None:
        chosen_clusters = [0 for x in range(examples.shape[0])]
    else:
        dists = scipy.spatial.distance.cdist(examples, codebook)
        chosen_clusters = np.argmin(dists, axis=1)
    for i in range(examples.shape[0]):
        if np.sqrt(np.sum(np.square(examples[i]))) < 1e-6:
            # This is the black cluster (or uniform color cluster)
            # because the gist features will all be zero.
            black_cluster = chosen_clusters[i]
            continue
        p_val = priority_func(examples[i])
        if p_val > -0.5:
            # Image is the same as one in the index, so skip
            continue
        heapq.heappush(clusters[chosen_clusters[i]],
                       (p_val, i))

    cluster_sizes = [len(x) for x in clusters]
    _log.info('The smallest cluster has %i examples. The largest has %i' %
              (min(cluster_sizes), max(cluster_sizes)))
    return clusters, black_cluster

def recalculate_priorities(examples, clusters, priority_func):
    '''Recalculates the priorities so that the top entry is on each cluster'''
    for cluster in clusters:
        last_update = None
        while len(cluster) > 0 and cluster[0][1] <> last_update:
            last_update = cluster[0][1]
            new_pval = priority_func(examples[last_update])
            heapq.heapreplace(cluster, (new_pval, last_update))
            
    return clusters   

def build_knn_index(examples):
    _log.info('Building the flann index of labeled examples')
    flann = pyflann.FLANN()
    sample_fraction = 0.20
    if examples.shape[0] > 10000:
        sample_fraction = 0.05
    flann.build_index(examples, algorithm = 'autotuned',
                      target_precision=0.95,
                      build_weight=0.01,
                      memory_weight=0.7,
                      sample_fraction=sample_fraction,
                      log_level='info',
                      random_seed=184369)

    return flann

def is_duplicate(feature, feature_set, d_thresh=0.5):
    '''Is the feature a duplicate of one in the feature_set?'''
    if len(feature_set) == 0:
        return False
    dists = scipy.spatial.distance.cdist([feature], feature_set)
    return np.min(dists) < d_thresh

def choose_examples(queue, examples, n, priority_func):
    '''Choose n examples at the top of the priority queue, ignoring dups.'''
    chosen = []
    chosen_idx = []
    while len(chosen) < n and len(queue) > 0:
        recalculate_priorities(examples, [queue], priority_func)
        p_dist, idx = heapq.heappop(queue)
        if not is_duplicate(examples[idx], chosen):
            chosen.append(examples[idx])
            chosen_idx.append(idx)

    return chosen_idx

def videoid_from_url(url, regex='v=([0-9a-zA-Z_-]+)'):
    '''Finds the video id in the url.

    The regex must produce a group which is the video id.
    '''
    reg = re.compile(regex)
    parse = reg.search(url)
    if parse:
        return parse.groups()[0]
    else:
        return None

def find_new_urls(seed_ids, output_file, n_videos=100, known_ids=[]):
    '''Outputs a list of new videos to download.

    Videos are those that are similar to the seed. We will walk around
    the youtube graph to find them.

    Inputs:
    seed_ids - List of youtube video ids that we will start looking from
    output_file - File that will have new urls appended to it
    n_videos - Number of videos to grab
    known_ids - set of known video ids that we skip

    Returns:
    list of ids added to the file
    '''
    new_ids = yt_scraper.get_new_videos(seed_ids, known_ids,
                                        n_videos=n_videos)
    
    
    dir_path = os.path.dirname(output_file)
    if dir_path == '':
        dir_path = '.'
    if not os.path.exists(dir_path):
        os.makedirs(os.path.dirname(output_file))
    with open(output_file, 'a') as f:
        _log.info('Appending %i new urls to %s' % (len(new_ids), output_file))
        f.write('\n'.join(['http://www.youtube.com/watch?v=%s' % x for
                      x in new_ids]))
        f.write('\n')

    return new_ids

def main(options):
    generator = load_gist_generator(options.cache_dir)
    example_urls, codebook, white_vector = load_codebook(options.codebook)
    example_urls = merge_example_urls(example_urls, options.example_urls)

    db_parse, known_videoids = parse_image_db(options.image_db,
                                              options.aspect_ratio,
                                              options.image_dir)

    labeled_files = find_labeled_files(options.stimuli_dir,
                                       options.image_dir)
    unlabeled_files = [x[0] for x in db_parse if x[0] not in labeled_files]
    labeled_files = [x for x in labeled_files]
    _log.info('Found %i labeled images and %i unlabeled images' % 
              (len(labeled_files), len(unlabeled_files)))

    labeled = generate_features(labeled_files, options.image_dir,
                                white_vector, generator)
    knn_index = build_knn_index(labeled)

    unlabeled = generate_features(unlabeled_files, options.image_dir,
                                  white_vector, generator)
    cluster_qs = create_cluster_queues(codebook)
    cluster_qs, black_cluster = assign_examples_to_clusters(
        unlabeled,
        cluster_qs,
        lambda x: -calc_mean_dist(knn_index, x),
        codebook)

    cur_stimuli_index = options.start_index
    found_empty_cluster = False
    while (not found_empty_cluster and 
           cur_stimuli_index < (options.start_index + options.n_sets)):
        _log.info('Building stimuli set %i' % cur_stimuli_index)

        stimuli_files = []
        chosen_examples = []
        if codebook is None:
            chosen_idx = choose_examples(
                cluster_qs[0],
                unlabeled,
                options.n_img,
                lambda x: -calc_mean_dist(knn_index, x))
            for idx in chosen_idx:
                stimuli_files.append(unlabeled_files[idx])
                chosen_examples.append(unlabeled[idx])
                
        else:
            for clusterq, cluster_idx in zip(cluster_qs,
                                             range(len(cluster_qs))):
                if len(clusterq) == 0:
                    if cluster_idx == black_cluster:
                        stimuli_files.append(None)
                        continue
                    
                    _log.warning('There are no more examples in a cluster,'
                    'so we are done')
                    found_empty_cluster = True
                    if options.new_urls is not None:
                        known_videoids = known_videoids.union(
                            find_new_urls(
                                filter(lambda x: x is not None,
                                       [videoid_from_url(
                                           x, 'vi/([0-9a-zA-Z_-]+)/.+\.jpg')
                                           for x in example_urls[cluster_idx]]),
                                options.new_urls,
                                100,
                                known_videoids))
                    continue

                p_dist, idx = heapq.heappop(clusterq)
                stimuli_files.append(unlabeled_files[idx])
                chosen_examples.append(unlabeled[idx])

        if not found_empty_cluster and (
                len(stimuli_files) == (options.n_img)):
            dest_dir = options.output % cur_stimuli_index
            _log.info('Writing stimuli set to %s' % dest_dir)
            if os.path.exists(dest_dir):
                _log.error('Stimuli set %s already exists' % dest_dir)
                continue
            os.makedirs(dest_dir)
            for image_file in stimuli_files:
                if image_file is None:
                    # Insert a black image into the stimuli set
                    cur_image = Image.new("RGB", (256,144))
                    cur_image.save(os.path.join(dest_dir, 'black.jpg'))
                else:
                    cur_image = Image.open(os.path.join(options.image_dir,
                                                        image_file))
                    cur_image.thumbnail((256,256), Image.ANTIALIAS)
                    cur_image.save(os.path.join(dest_dir, image_file))


            _log.info('Adding the chosen examples to the knn index.')
            labeled = np.vstack((labeled, chosen_examples))
            knn_index = build_knn_index(labeled)

            cluster_qs = recalculate_priorities(
                unlabeled, cluster_qs,
                lambda x: -calc_mean_dist(knn_index, x))

        cur_stimuli_index += 1

    

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)
    
    parser.add_option('-i', '--image_db', default=None,
                      help='Image database file')
    parser.add_option('--stimuli_dir', default=None,
                      help='Directory containing previous stimuli sets')
    parser.add_option('-o','--output', default='stimuli_set%i',
                      help='Format of the directory name to output a stimuli set')
    parser.add_option('--new_urls', default=None,
                      help=('Output file that will where new urls to try '
                            'downloading to fill out the clusters will be '
                            'appended. One url per line.'))
    
    parser.add_option('--image_dir', default=None,
                      help='Image directory')
    parser.add_option('--codebook', default=None,
                      help=('File containing the codebook definition. '
                            'Created using the divide_visual_space.py script.'))
    parser.add_option('--example_urls', default=None,
                      help=('File of example urls for each cluster '
                            'generated using generate_example_urls.py'))

    parser.add_option('--cache_dir', default=None,
                      help='Directory for cached feature files.')
    parser.add_option('-s', '--start_index',type='int', default=None,
                      help='start index of the stimuli set')
    parser.add_option('-a', '--aspect_ratio',type='float', default=1.78,
                      help='aspect ratio to select')
    parser.add_option('-n', '--n_sets', type='int', default=5,
                      help='Number of stimuli sets to create')
    parser.add_option('--n_img', type='int', default=108,
                      help='Number of images per set')
    parser.add_option('--log', default=None,
                      help='Log file. If none, dumps to stdout')

    options, args = parser.parse_args()

    if options.log is None:
        utils.logs.StreamLogger(None)
    else:
        utils.logs.FileLogger(None, options.log)

    main(options)
