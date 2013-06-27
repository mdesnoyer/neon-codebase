#!/usr/bin/env python

#K Nearest Neighbor 
import scikits.ann as ann
import numpy
import sys
import os

try:
    dir = sys.argv[1]
    test_file = sys.argv[2]
except:
    print "./script <directory with gist descriptors> <test_file>"
    sys.exit(0)

def irange(sequence):
        return zip(range(len(sequence)), sequence)

files = os.listdir(dir)
descriptors =[]

#remove the .DS_Store file
try:
    files.remove('.DS_Store')
except:
    pass

for i,file in irange(files):
    d = numpy.load(dir + "/" + file) 
    descriptors.append(d)

#create KD Tree
k=ann.kdtree(numpy.array(descriptors))

#test file
test_descriptor = numpy.load(test_file)
res = k.knn(test_descriptor,1)
idx = res[0][0]
print files[idx]

