#!/bin/python

import os
import sys
import glob
import leargist
import Image
import numpy

dir = sys.argv[1]
savedir = sys.argv[2]

cwd = os.getcwd()
path = os.getcwd()
size = 256, 256

for imgFile in glob.glob (os.path.join(dir + "/" , '*.jpg')):
    im = Image.open(imgFile)
    im.thumbnail(size,Image.ANTIALIAS)
    descriptors = leargist.color_gist(im)
    loadArray = descriptors.tolist()
    numpy.save( savedir + "/" + imgFile.split('/')[-1] + ".npy",loadArray);
