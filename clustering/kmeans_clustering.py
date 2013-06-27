from scipy import *
from pylab import *
from scipy.cluster import vq
import scipy.spatial.distance as DIST
import sys
import os 
import numpy
import shutil

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

    path = os.getcwd()
    dir = sys.argv[1]
    path = path + "/" + dir + "/" 
    dirList=os.listdir(path)
    dirList.sort()

    nclusters = int(sys.argv[2])
    arrays = []
    data_map = []

    f = open('clustering_output_' + str(nclusters) ,"w")

    i=0
    for infile in dirList:
        if infile.endswith('.npy'):
            smarray = numpy.load(path+infile)
            arrays.append(smarray)
            data_map.append(infile)

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
    d = dir + "_" + str(nclusters)
    os.mkdir(d)
    
    for i in range(len(clusters)):
        subd =  d + '/' + str(i) 
        os.mkdir(subd) 
        for fname in result_fnames[i]:
            f.write(fname + " " + str(i)  + '\n')
            #copy the file to the subdirectory
            shutil.copy( path + '/' + fname, subd + '/' + fname) 

    f.close()

    # Create Image tags for the clusters they are in - may be a text file ?
    # Run SVM Model Creation & Test accuracy  

