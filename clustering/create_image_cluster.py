#!/bin/python
#Given a cluster set copy 

import os
import sys
import shutil

try:
    source = sys.argv[1]
    testset = sys.argv[2]
except:
    print "./test <cluster source> <image set>"
    exit(0)

target = "images_" + source

files = []
for d in os.listdir(source):
    for file in os.listdir( source + "/" + d ):
        tup = (file,d)
        files.append(tup)

if not os.path.exists(target):
    os.mkdir(target)
    
for file in files:
    fname = file[0].split('.npy')[0]
    dirname = file[1]
    subdir = target + "/" + dirname
    if not os.path.exists(subdir):
        os.mkdir(subdir)
    shutil.copy(testset + "/"+ fname, subdir + "/" + fname)
