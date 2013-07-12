#!/usr/bin python
'''
Find untagged images of given aspect ratio and generate stimuli set (directories or text file with filenames)
'''

import os
import sys
import shutil
from scipy import *
from pylab import *
from scipy.cluster import vq
import scipy.spatial.distance as DIST
import numpy
import time
import scikits.ann as ann
from optparse import OptionParser
from Queue import PriorityQueue

USAGE = '%prog [options]'
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

''' Check if image is not already in a stimuli set, conforms to aspect ratio '''
def select_image(imdb,id,ar=1.78):
    try:
        tup = imdb[id] #(ar,stimset)
        if tup[0] == ar and tup[1] == 'null':
            return True
    except:
        pass

    return False

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)

    parser.add_option('-d', '--descriptor_dir', default=None,
                      help='Directory with image descriptors')
    parser.add_option('-m', '--model_dir', default=None,
                      help='Model root directory')
    parser.add_option('-i', '--image_dir', default=None,
                      help='Image directory')
    parser.add_option('-s', '--start_index',type='int', default=None,
                      help='start index of the stimuli set')
    parser.add_option('-c', '--csize',type='int', default=108,
                      help='cluster size')
    parser.add_option('-n', '--nset',type='int', default=1,
                      help='max number of stimuli set to generate')
    parser.add_option('-f', '--image_db', default=None,
                      help='Image database file')
    parser.add_option('-a', '--aspect_ratio',type='float', default=1.78,
                      help='aspect ratio to select')
    parser.add_option('-o','--output', default='txt',
                      help='dir|txt - stimuli set with images | text file with filenames')

    options, args = parser.parse_args()
    source = options.descriptor_dir
    start_index = options.start_index
    nclusters = options.csize
    nsets = options.nset
    image_db = options.image_db
    aspect_ratio = options.aspect_ratio
    output = options.output
    model_source = options.model_dir
    image_dir = options.image_dir

    map = {}
    all_files = []
    file_map  = {}
    data_map  = [] 
    descriptors = []
    kdistance_map = {}
    imdb = {}

    # Load image database in to a map
    with open(image_db,'r') as f:
        lines = f.readlines()
    
    for line in lines:
        parts = line.split(' ')
        id = parts[0]
        ar = float(parts[3])
        stim = parts[-1].rstrip('\n')
        imdb[id] = (ar,stim)

    # Create a map of each cluster and associate the distance or distribution with each element
    # Get only new images that arent' tagged yet !
    dirList = os.listdir(source)
    for infile in dirList:
        if infile.endswith('.npy'):
            id = infile.split('.')[0]
            if select_image(imdb,id,aspect_ratio):
                smarray = numpy.load( source + "/" + infile)
                descriptors.append(smarray)
                data_map.append(infile)

    data = numpy.array(descriptors)
    clusters,code_ids = cluster_data(data,nclusters)
    
    result = [ PriorityQueue() for _ in range(nclusters)] #populated list of filenames in each cluster
    result_fnames = [ [] for _ in range(nclusters)]

    # Create a KD Tree for ANN
    model_descriptors = []
   
    #load descriptors from the current model ( valence_descriptors/ folder)
    src = model_source + "/valence_descriptors"
    dirList = os.listdir(src)
    for dir in dirList:
        s_src = src + "/" + dir
        for infile in os.listdir(s_src):
            if infile.endswith('.npy'):
                smarray = numpy.load( s_src + "/" + infile)
                model_descriptors.append(smarray)

    # Populate the distance metric for each of the descriptors
    k = 3
    kdtree = ann.kdtree(numpy.array(model_descriptors))
    for desc,fname in zip(descriptors,data_map):
        idx,dist = kdtree.knn(desc,k)
        kdistance_map[fname] = dist[0]

    # Format cluster result
    i = 0 
    for id in code_ids:
        result_fnames[id].append(data_map[i])
        i += 1
    
    for i in range(nclusters): 
        #Create fname,distance tuple
        for fname in result_fnames[i]:
            dist = kdistance_map[fname] 
            mean = numpy.mean(dist)
            var = numpy.var(dist)
            result[i].put(fname, -1 * mean) #insert into pq, -ve of dist

    # cluster with least number of files
    min_cluster = 999
    for i in range(nclusters):
        l = len(result_fnames[i]) 
        if l < min_cluster:
            min_cluster = l  

    if min_cluster == 0:
        print "Not enough images in each cluster to create even a single stimuli set"
        sys.exit(0)

    # For min # of clusters, create stimuli sets
    output_file = 'stimset_output.' + str(int(time.time()))
    f = open(output_file,'w') 
    for i in range(min_cluster):
        if output == 'dir': 
            target = "stimuli_" + str(start_index + i)
            if not os.path.exists(target):
                os.mkdir(target)
         
        for j in range(nclusters):
            pq = result[j]
            #pq.get() blocks if size =0
            if pq.qsize() <= 0:
                break
            fname = pq.get()
            fname = fname.split('.')[0]  + '.jpg'
            f.write(fname + " s" + str(start_index + i)  + '\n')
            
            if output == 'dir':
                shutil.copy(image_dir + '/' + fname, target + '/' + fname) 
    
    f.close() 

    #TODO : Update the image DB with the stimuli set it was associated with 
