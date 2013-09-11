#!/usr/bin/env python
'''
Scrtipt that adds pillar boxed images to the 4x3 ones in the image database.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
USAGE = '%prog [options]'

import cv2
import csv
import errorlog
import logging
import numpy as np
from optparse import OptionParser
import os.path

_log = logging.getLogger(__name__)

def main(options):
    aspect = eval(options.aspect_ratio)
    suffix = '%ix%i' % aspect
    
    with open(options.image_db) as f:
        for line in f:
            fields = line.split()

            target_id = '%s_%s' % (fields[0], suffix)
            target_file = os.path.join(options.image_dir,
                                       target_id + '.jpg')
            if os.path.exists(target_file):
                continue

            orig = cv2.imread(os.path.join(options.image_dir,
                                           fields[0] + '.jpg'))

            newshape = (max(orig.shape[0],
                            orig.shape[1] * aspect[1] / aspect[0]),
                        max(orig.shape[1],
                            orig.shape[0] * aspect[0] / aspect[1]),
                        orig.shape[2])

            if newshape == orig.shape:
                continue
                
            new_img = np.zeros(newshape, dtype=np.uint8)
            new_img[((newshape[0]-orig.shape[0])/2):
                    ((newshape[0]+orig.shape[0])/2),
                    ((newshape[1]-orig.shape[1])/2):
                    ((newshape[1]+orig.shape[1])/2),:] = orig

            fields[3] = '%3.2f' % (float(aspect[0]) / aspect[1])
            fields[0] = target_id

            cv2.imwrite(target_file, new_img)
            with open(options.image_db, 'a') as db:
                db.write(' '.join(fields) + '\n')

            

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)
    
    parser.add_option('-i', '--image_db', default=None,
                      help='Image database file')
    parser.add_option('--image_dir', default=None,
                      help='Image directory')
    parser.add_option('--aspect_ratio', default="(16,9)",
                      help='Target aspect ratio of the image')

    options, args = parser.parse_args()

    errorlog.StreamLogger(None)

    main(options)
