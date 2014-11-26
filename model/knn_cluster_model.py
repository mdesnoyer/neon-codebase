'''The baseline model when Mark joined Neon

The model that does 1-vs-all SVM classification for clustering and
then nearest neighbour in the cluster in order figure out the valence
score.

This is a port of the existing code into the ValencePredictor
framework so that we can do cross fold validation testing.

'''
import copy
import logging
import model
import predictor
import random
from scipy.cluster import vq
import scipy.spatial.distance as sp_dist
import numpy as np
import svmlight
import tempfile
import os

_log = logging.getLogger(__name__)

class Predictor(predictor.Predictor):
    '''Create the predictor that uses 1 vs all SVM for clustering.

    Inputs:
    feature_generator - A model.FeatureGenerator object
    nclusters - Number of clusters to use.
    '''
    def __init__(self, feature_generator, nclusters, seed=None):
        super(Predictor, self).__init__(feature_generator)
        self.nclusters = nclusters
        self.seed = seed
        self.reset()

    def reset(self):
        self.is_trained = False

        # Before training, self.scores and self.data are lists of
        # scores and data respectively. After training, it is a
        # sequence of lists, one for each cluster.
        self.scores = [] # List of scores
        self.data = [] # List of training feature vectors
        self.metadata = [] # List of metadata, same size as data
        self.cluster_models = [] # List of SVM models for the clusters

    def __str__(self):
        return model._full_object_str(self,
                                      exclude=['scores', 'data', 'metadata',
                                               'cluster_models'])

    def __getstate__(self):
        # The cluster_models can't be pickled directly. svmlight's
        # write_model() function must be called which has to write to a
        # file. So, we write to a temporary file and then read that
        # file back and pickle the strings. sigh.
        state = self.__dict__.copy()
        models = []
        for i in range(len(state['cluster_models'])):
            models.append(state['cluster_models'][i])
            tfile,tfilename = tempfile.mkstemp()
            try:
                os.close(tfile)
                svmlight.write_model(state['cluster_models'][i], tfilename)
                with open(tfilename, 'rt') as f:
                    state['cluster_models'][i] = f.read()
            finally:
                os.unlink(tfilename)
        self.cluster_models = models
        return state

    def __setstate__(self, state):
        # Rebuild the svm models using svmlight's read_model function
        for i in range(len(state['cluster_models'])):
            tfile,tfilename = tempfile.mkstemp()
            try:
                os.close(tfile)
                with open(tfilename, 'w+t') as f:
                    f.write(state['cluster_models'][i])
                state['cluster_models'][i] = svmlight.read_model(tfilename)
            finally:
                os.unlink(tfilename)

        self.__dict__ = state

    def add_feature_vector(self, features, score, metadata=None):
        '''Adds a veature vector to train on.

        Inputs:
        features - a 1D numpy vector of the feature vector
        score - score of this example.
        metadata - to attach to this example
        '''
        if self.is_trained:
            raise model.AlreadyTrainedError()
        self.scores.append(score)
        self.data.append(features)
        self.metadata.append(metadata)

    def train(self):
        '''Train on any images that were previously added to the predictor.'''
        _log.info('Training on %i images' % len(self.scores))

        if self.seed is not None:
            random.seed(self.seed)

        clusters, cluster_ids = self._cluster_data(self.data, self.nclusters)

        svmdata = self._np2svmlight(self.data)
        for cluster_id in range(len(clusters)):
            _log.info("Training cluster %i using %i training images." % 
                      (cluster_id, len(cluster_ids)))
            train_data = []
            for i in xrange(len(cluster_ids)):
                if cluster_ids[i] == cluster_id:
                    label = 1
                else:
                    label = -1
                train_data.append((label, svmdata[i]))

            self.cluster_models.append(svmlight.learn(train_data,
                                                      type='classification',
                                                      verbosity=1,
                                                      kernel_param=1))

        # Change the scores and data to be grouped by cluster
        self.data = clusters
        score_clusters = []
        metadata_clusters = []
        for i in range(len(clusters)):
            score_clusters.append(np.compress(cluster_ids == i,
                                              self.scores, 0))
            metadata_clusters.append([self.metadata[j] 
                                      for j in range(len(cluster_ids)) 
                                      if cluster_ids[j] == i])
        self.scores = score_clusters
        self.metadata = metadata_clusters

        self.is_trained = True
            

    def predict(self, image):
        '''Predicts the valence score of an image.

        Inputs:
        image - numpy array of the image
        
        Returns: predicted valence score

        Raises: NotTrainedError if it has been called before train() has.
        '''
        if not self.is_trained:
            raise model.NotTrainedError()

        neighbours = self.get_neighbours(image, k=1)
        return neighbours[0][0]

    def get_neighbours(self, image, k=5):
        '''Retrieve N neighbours of an image.

        Inputs:
        image - numpy array of the image in opencv format
        n - number of neighbours to return

        Outputs:
        Returns a list of [(score, dist, metadata)]

        '''
        features = [self.feature_generator.transform(image)]
        svm_features = self._np2svmlight(features)

        # Decide which cluster the example is in
        cluster_preds = []
        for cluster_model in self.cluster_models:
            pred = svmlight.classify(cluster_model, [(0, svm_features[0])])
            cluster_preds.append(pred[0])
        max_score = max(cluster_preds)
        chosen_cluster = cluster_preds.index(max_score)

        # Find distances to all examples in the cluster
        dists = sp_dist.cdist(features, self.data[chosen_cluster], 'euclidean')

        # Grab the valence score from the closest example
        sortI = sorted(range(len(dists[0])), key = lambda i: dists[0][i])

        return [(self.scores[chosen_cluster][i],
                 dists[0][i],
                 self.metadata[chosen_cluster][i]) for i in sortI[0:k]]

    def _cluster_data(self, data, nclusters, iters=20, thresh=1e-5):
        '''Does kmeans clustering on the data.

        Returns (clusters, cluster_ids)
        '''
        _log.info('Clustering images into %i clusters.' % nclusters)
        wh_data = vq.whiten(data)
        code_book,dist = vq.kmeans(wh_data, nclusters, iters, thresh)
        code_ids, distortion = vq.vq(wh_data,code_book)
        clusters = []
        for i in range(len(code_book)):
            cluster = np.compress(code_ids == i,data,0)
            clusters.append(cluster)
        return clusters, code_ids

    def _np2svmlight(self, np_data):
        '''Converts an np array of feature vectors into the svmlight format.

        The svmlight format is:

        [(<feature>, <value>), ...] for each row in the numpy array
        '''
        svmdata = []
        for row in np_data:
            svmdata.append([(k+1, row[k]) for k in xrange(len(row))])
            
        return svmdata
