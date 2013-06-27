#!/bin/python

''' Plot gist descriptors with their corresponding image '''

import matplotlib.pyplot as plt
import leargist
import PIL.Image as Image
import sys
import os

try:
    dir = sys.argv[1]
except:
    print "./script <directory with images>"
    sys.exit(0)

def irange(sequence):
        return zip(range(len(sequence)), sequence)

ims = os.listdir(dir)

#remove the .DS_Store file
try:
    ims.remove('.DS_Store')
except:
    pass

c = 1
for i,imf in irange(ims):
    im = Image.open(dir + "/" + imf)
    descriptors = leargist.color_gist(im)
    plt.subplot(len(ims),2,c)
    c +=1
    plt.plot(descriptors)
    
    plt.subplot(len(ims),2,c)
    plt.imshow(im)
    c +=1

plt.show()
