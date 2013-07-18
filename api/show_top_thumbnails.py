#!/usr/bin/python
'''Script that shows the top thumbnail from a local video.
'''
USAGE = '%prog [options]'

from optparse import OptionParser
from BadImageFilter import BadImageFilter
import ffvideo
from PIL import Image
import sys
import os.path
import cv2
import numpy as np
import time

def load_new_model(options):
    sys.path.insert(0, options.model_dir)
    import model as mod2
    return mod2.load_model(os.path.join(options.model_dir,
                                       '071013_trained.model'))

def run_new_model(model, options):
    # Output the model rewrite results    
    mov = ffvideo.VideoStream(options.input)
    start_time = time.time()
    chosen_thumbs = mod.choose_thumbnails(mov, options.n)
    print 'Processing time %f' % (time.time() - start_time)
    for i in range(len(chosen_thumbs)):
        print 'new %i: %f' % (i, chosen_thumbs[i][1])
        cur_file = 'mod2_' + options.output % i
        cv2.imwrite(cur_file, chosen_thumbs[i][0])
    

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)

    parser.add_option('-o', '--output', default=None,
                      help='Python string with a %i specifying the output filename. If not specified, it is displayed')
    parser.add_option('-i', '--input', default=None,
                      help='Input video.')
    parser.add_option('-n', default=1, type='int',
                      help='Number of thumbnails to show')
    parser.add_option('--model_dir', default='.',
                      help='Directory containing the model')

    options, args = parser.parse_args()

    mod = load_new_model(options)
    run_new_model(mod, options)

    # Load the model
    sys.path.insert(0, options.model_dir)
    import BinarySVM
    model = BinarySVM.BinarySVM(model_dir=options.model_dir)
    model.load_model()
    model.load_valence_scores()

    bad_image_filter = BadImageFilter(30, 0.95)

    # Process the video
    try:
        mov = ffvideo.VideoStream(options.input)
    except Exception, e:
        print 'Cannot open %s' % options.input
        exit(1)

    duration = mov.duration

    #If a really long video, then increase the sampling rate
    sec_to_extract_offset = 0.5
    if duration > 1800:
        sec_to_extract_offset = 2 

    # >1 hr
    if duration > 3600:
        sec_to_extract_offset = 4

    #Sequentially extract key frame every sec_to_extract_offset
    start_time = time.time()
    sec_to_extract = 1
    valence_scores = [[],[]]
    results = []
    while sec_to_extract < duration :
        try:
            frame = mov.get_frame_at_sec(sec_to_extract)
            image = frame.image()
            timecode = sec_to_extract
            valence_scores[0].append(sec_to_extract)
            sec_to_extract += sec_to_extract_offset
            score = 0 

            ''' If image not dominated by blackish pixels'''
            size = 256,256
            image.thumbnail(size,Image.ANTIALIAS)
            width, height = image.size

            ''' Check if image is too uniform a color'''
            is_too_uniform = bad_image_filter.should_filter(image)

            if is_too_uniform:  
                score = 0
                attr = 'uniform_color'
            else:
                ''' Check if image is blur '''
                blur = model.image_blur_score(frame.image())
                score,attr = model.euc_distance_valence(image) #calc model score using euc distance
                if blur == True:
                    score = 0.5 * score 
                    attr = 'blur'

            valence_scores[1].append(score)
            image = frame.image()
            results.append((score,image))

        #No more key frames to process
        except ffvideo.NoMoreData:
            break

        except Exception,e:
            print "key=process_video msg=processing error msg=" + e.__str__()
            #No more key frames to process
            break

    results = sorted(results, key=lambda x: x[0], reverse=True)

    print 'Processing time %f' % (time.time() - start_time)
    for i in range(options.n):
        print 'orig %i: %f' % (i, results[i][0])
        cur_file = options.output % i
        results[i][1].save(cur_file)

