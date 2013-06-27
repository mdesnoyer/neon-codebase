from PIL import Image
import svmlight
import sys
import os
import glob
import leargist
import numpy

dir = sys.argv[1]
path = os.getcwd()
image_files = glob.glob( os.path.join(path + "/" + dir + "/", '*.jpg'))

def save():
        for imgFile in image_files:
                im = Image.open(imgFile)
                size = 256,256
                im.thumbnail(size,Image.ANTIALIAS)
                descriptors = leargist.color_gist(im)
                loadArray = descriptors.tolist()
                numpy.save(imgFile + ".npy",loadArray);

save()

