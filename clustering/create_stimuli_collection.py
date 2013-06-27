# From the image database, create image collection of the same sizes
# Select images that aren't tagged with any valence scores

import os
import sys
import shutil

dir = sys.argv[1]
imdb = dir +'/image.db'

with open(imdb,'r') as f:
    lines = f.readlines()

collection = {} 

for line in lines:
    vals = line.split(' ')
    #valence tag is null
    if 'null' in vals[-1] :
        size = vals[3] 
        if not os.path.exists('s'+size):
            os.mkdir('s'+size)
        
        #copy file
        fname = vals[0] + ".jpg"
        source = dir + "/" + fname
        destination = 's'+size + "/" + fname 
        shutil.copy(source,destination)

# Run clustering alg to create stimuli sets
