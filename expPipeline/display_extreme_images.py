#!/usr/bin/env python
'''Script that displays the top or bottom images from the mturk results.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import csv
from optparse import OptionParser
import shutil
import os.path
import matplotlib.pyplot as plt
import PIL

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--scores', default=None,
                      help='File containing "<filename>,<score>" on each line')
    parser.add_option('--image_dir', default=None,
                      help='Directory with the images')
    parser.add_option('--page', '-p', type='int', default=0,
                      help='Page of images to look at')
    parser.add_option('--reverse', action='store_true', default=False,
                      help='If set, reverse the image ordering')
    parser.add_option('--output_dir', default=None,
                      help='If defined, all the images in this page will be copied to the given directory.')
    
    options, args = parser.parse_args()

    
    data = [(x[0], float(x[1])) for x in csv.reader(open(options.scores))]

    data = sorted(data, key = lambda x: x[1], reverse=options.reverse)

    IMAGES_PER_ROW = 5
    IMAGES_PER_PAGE = IMAGES_PER_ROW * IMAGES_PER_ROW
    data = data[(options.page * IMAGES_PER_PAGE):
                ((options.page+1) * IMAGES_PER_PAGE)]
    plt.figure(figsize=(15,15), dpi=70)

    for i in range(IMAGES_PER_PAGE):
        imageFn = os.path.join(options.image_dir, data[i][0])
        image = PIL.Image.open(imageFn)
        frame = plt.subplot(IMAGES_PER_ROW, IMAGES_PER_ROW, i+1)
        frame.axes.get_xaxis().set_ticks([])
        frame.axes.get_yaxis().set_visible(False)
        #plt.imshow(image)
        plt.imshow(image.transpose(PIL.Image.FLIP_TOP_BOTTOM))
        plt.xlabel('%5.4f' % data[i][1])

        # Copy the image to a new directory
        if options.output_dir:
            shutil.copy(imageFn, options.output_dir)
            with open(os.path.join(options.output_dir, 'scores.txt'), 'aw') as f:
                f.write('%s,%s\n' % data[i])

    plt.tight_layout()

    plt.show()
            
