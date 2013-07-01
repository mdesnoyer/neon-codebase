from scipy import *
from pylab import *
from scipy.cluster import vq
import scipy.spatial.distance as DIST
import sys
import os 
import numpy
import shutil
from optparse import OptionParser

def cluster_data(data,cluster_cnt,iter=20,thresh=1e-5):
    """ Group data into a number of common clusters
        data -- 2D array of data points ( vectors).  Each point is a row in the array.
        cluster_cnt -- The number of clusters to use
    """
    wh_data = vq.whiten(data)
    code_book,dist = vq.kmeans(wh_data,cluster_cnt,iter,thresh)
    #code_books - centroids
    code_ids, distortion = vq.vq(wh_data,code_book)
    clusters = []
    for i in range(len(code_book)):
        cluster = compress(code_ids == i,data,0)
        clusters.append(cluster)
    return clusters,code_ids
 
if __name__ == "__main__":

    parser = OptionParser()
    
    parser.add_option('-i', '--input', default=None,
                      help='Input file, with valence scores. Format is <filname> <score>')
    parser.add_option('--in_dir', default=None,
                      help='Input directory')
    parser.add_option('-n', type='int', default=40,
                      help='Number of clusters')

    options, args = parser.parse_args()

    # Parse the valence score file to get the filenames to use
    descriptor_files = []
    for line in open(options.input):
        descriptor_files.append(line.strip().split()[0])

    nclusters = options.n
    arrays = []
    data_map = []

    f = open('clustering_output_' + str(nclusters) ,"w")

    i=0
    for inFile in descriptor_files:
        full_file = os.path.join(options.in_dir, '%s.npy' % inFile)
        smarray = numpy.load(full_file)
        arrays.append(smarray)
        data_map.append('%s.npy' % inFile)

    data = numpy.array(arrays)
    clusters,code_ids = cluster_data(data,nclusters)
    
    result = [ [] for _ in range(len(data_map))]
    result_fnames = [ [] for _ in range(len(data_map))]

    i = 0 
    for id in code_ids:
        result_fnames[id].append(data_map[i]) #.split('_')[0])
        result[id].append(arrays[i])
        i += 1

    # Make the directories and copy the files to those directory
    d = options.in_dir + "_" + str(nclusters)
    if not os.path.exists(d):
        os.mkdir(d)
    
    for i in range(len(clusters)):
        subd =  os.path.join(d,str(i)) 
        os.mkdir(subd) 
        for fname in result_fnames[i]:
            f.write(fname + " " + str(i)  + '\n')
            #copy the file to the subdirectory
            shutil.copy( os.path.join(options.in_dir, fname),
                         os.path.join(subd,fname)) 

    f.close()

    # Create Image tags for the clusters they are in - may be a text file ?
    # Run SVM Model Creation & Test accuracy  

