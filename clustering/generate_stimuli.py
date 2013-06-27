#!/bin/python
#Given a cluster set copy 

import os
import sys
import shutil
import random

try:
    source = sys.argv[1]
    cluster_size = int(sys.argv[2])
    nsets = int(sys.argv[3])
except:
    print "./test <image cluster source> <cluster size> <nsets>"
    exit(0)

target_dir = "stimuli_"

for i in range(nsets):
    target = "stimuli_" + str(i)
    if not os.path.exists(target):
        os.mkdir(target)

map = {}
all_files = []
file_map = {}

for d in os.listdir(source):
    files = os.listdir( source + "/" + d )
    #populate file map
    for f in files:
        file_map[f] = source + "/" + d
    
    current_dir  = source + "/" + d + "/"
    nfiles = len(files)
    all_files.extend(files)
    if nfiles <= nsets:
        for i in range(nfiles):
            fname = files[i]
            if not map.has_key(fname):
                map[fname] = 1
                all_files.remove(fname)
            shutil.copy(current_dir + fname, target_dir + str(i) + "/" + fname)
    else:
        count =0
        while count < nsets:
            idx = random.randint(0,len(files)-1)
            fname = files[idx]
            if not map.has_key(fname):
                map[fname] = 1
                all_files.remove(fname)
                #copy file to the right stimuli set
                shutil.copy(current_dir + fname, target_dir + str(count) + "/" + fname)
                count += 1
            else:
                continue
    file_map[d] = files

#Randomly pick to complete the set for each stimuli 
#if len of each stimuli set not equal to cluster size, then get more; maintain list with unselected images
for i in range(nsets):
    target = "stimuli_" + str(i)
    files = os.listdir(target)
    files_needed = cluster_size - len(files)
    while files_needed >0:
        idx = random.randint(0,len(all_files)-1)
        fname = all_files[idx]
        if not map.has_key(fname):
            map[fname] = 1
            all_files.remove(fname)
            src = file_map[fname] + "/"
            shutil.copy(src + fname, target + "/" + fname)
            files_needed -=1


