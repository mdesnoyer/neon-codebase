#!/usr/bin/env python

import sys
import os
import numpy
from scipy import *
import scipy.spatial.distance as DIST

try:
    source = sys.argv[1] #npy files
    euclidean_duplicate_thresold = float(sys.argv[2])
except:
    print "python script.py <source dir> <euc threshold>"
    sys.exit(0)

files = [f for f in os.listdir(source) if f.endswith('.npy') ] #get only npy files
descriptors = []
for file in files:
    d = numpy.load(source + "/" + file)
    descriptors.append(d)

mtrx = matrix(descriptors)
distmt = DIST.pdist(mtrx,'euclidean',p=2)

file_indices = range(len(descriptors))
MAXCOUNT = len(descriptors)
c = 0;
newMtrx = numpy.zeros(shape=(MAXCOUNT,MAXCOUNT))
duplicates = []

#Calculate the pairwise euclidean distance 
for i in range(MAXCOUNT):
    for j in range ( i+1,MAXCOUNT):
        if distmt[c] <= euclidean_duplicate_thresold:
            val = 0
            if i != j:
                #print i,j
                duplicates.append(i)
        else:
            val = 1
        newMtrx[i][j] = val 
        newMtrx[j][i] = val
        c = c+1;

#remove duplicates
st = set(duplicates)
duplicates = list(st) #uniques in list
for dup in duplicates:
    try:
        file_indices.remove(dup)
        os.remove(source + "/" + files[dup])
        #import shutil
        #shutil.move( source + "/" + files[dup], source + "/" + str(dup)) 
    except Exception,e:
        pass

#from pylab import *
#imshow(newMtrx,aspect='auto',interpolation='nearest', origin='lower' ) #Normalize())
#colorbar()
#show()
