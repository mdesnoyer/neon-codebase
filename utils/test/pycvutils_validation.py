#!/usr/bin/env python
'''Script to validate the image differences.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import glob
import imagehash
import matplotlib.pyplot as plt
import numpy as np
import PIL.Image
from utils import pycvutils
import random
import scipy as sp
from StringIO import StringIO
import utils.neon
from utils.options import define, options

define('image_dir', default='/data/neon/imdb/staging/images',
       help='Directory to get test images from')
define('n_images', default=400, type=int,
       help='Number of images to use in analysis')

def compress_image(orig_image):
    if not isinstance(orig_image, np.ndarray):
        pimage = orig_image
    else:
        pimage = pycvutils.to_pil(orig_image)
    buf = StringIO()
    pimage.save(buf, format="JPEG", quality=50)
    buf.seek(0)

    cimage = PIL.Image.open(buf)
    if not isinstance(orig_image, np.ndarray):
        return cimage
    else:
        return pycvutils.from_pil(cimage)

def compare_imagehashes():
    hashes = []
    same_image_scores = []
    diff_image_scores = []
    #for image_fn in random.sample(glob.glob('%s/*.jpg' % options.image_dir),
    #                                 options.n_images):
    for image_fn in glob.glob('%s/*.jpg' % options.image_dir):
        orig_image = PIL.Image.open(image_fn)
        orig_hash = imagehash.dhash(orig_image)
        
        cmp_image = compress_image(orig_image)
        cmp_hash = imagehash.dhash(cmp_image)

        same_image_scores.append(orig_hash - cmp_hash)

        for other_hash, other_size in hashes:
            if other_size != orig_image.size:
                diff_image_scores.append(float('inf'))
            else:
                diff_image_scores.append(orig_hash - other_hash)

        hashes.append((orig_hash, orig_image.size))
        if len(hashes) > options.n_images:
            break

    return same_image_scores, diff_image_scores
        

def compare_hsv_differences():
    images = []
    same_image_scores = []
    diff_image_scores = []
    for image_fn in random.sample(glob.glob('%s/*.jpg' % options.image_dir),
                                  options.n_images):
        orig_image = sp.misc.imread(image_fn)[:,:,::-1]

        cmp_image = compress_image(orig_image)

        score = pycvutils.hsv_image_difference(cmp_image, orig_image)
        same_image_scores.append(score)
                                                                
        for other_image in images:
            diff_image_scores.append(pycvutils.hsv_image_difference(
                other_image, orig_image))

        images.append(orig_image)

    return same_image_scores, diff_image_scores
    

def main():
    utils.neon.InitNeon()

    random.seed(23541234)

    same_image_scores, diff_image_scores = compare_imagehashes()

    plt.figure(1)
    plt.plot(same_image_scores, 'x')

    plt.figure(2)
    plt.plot(diff_image_scores, '+')

    min_diff = np.min(diff_image_scores)
    print ('Number of sames greater than %f: %i' % 
           (min_diff, np.sum(same_image_scores > min_diff)))
    max_same = np.max(same_image_scores)
    print ('Number of diffs less than %f: %i' % 
           (max_same, np.sum(diff_image_scores < max_same)))
                                                   

    plt.show()
        
if __name__ == '__main__':
    main()
