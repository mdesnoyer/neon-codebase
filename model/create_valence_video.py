#!/usr/bin/env python
'''A script that creates a video showing the valence score.

It's a visual aid!

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import os.path
if __name__ == '__main__':
    import sys
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if sys.path[0] <> base_path:
        sys.path.insert(0,base_path)

import colorsys
import cv2
import logging
import model
import numpy as np
from optparse import OptionParser

_log = logging.getLogger(__name__)

def RenderValence(image, valence, attr, valenceRange=(1.0,7.0)):
    if not np.isfinite(valence):
        valence = 1.0
    
    scoreWidth = 30

    fracFill = ((valence - valenceRange[0]) / 
                (valenceRange[1] - valenceRange[0]))
    image[:, 0:scoreWidth, :] = 0

    color = colorsys.hsv_to_rgb((1-fracFill),
                                0.9,
                                0.6)

    cv2.rectangle(image,
                  (0,
                   int((1-fracFill)*image.shape[0])),
                  (scoreWidth, image.shape[0]-1),
                  cv2.cv.RGB(int(color[0]*255),
                             int(color[1]*255),
                             int(color[2]*255)),
                             cv2.cv.CV_FILLED)

    cv2.putText(image, attr, (0, image.shape[0]-10),
                cv2.FONT_HERSHEY_PLAIN, 4.0, cv2.cv.RGB(200,200,50), 5)

    return image    

def main(options):     
    _log.info('Loading model')
    mod = model.load_model(options.model)

    mod.filt.filters[-1] = model.filters.CrossFadeFilter(18)

    _log.info('Opening %s for input' % options.input)
    inputVideo = cv2.VideoCapture(options.input)
    moreData, frame = inputVideo.read()

    if not moreData:
        _log.error('Error opening: %s' % options.input)
        exit(1)

    _log.info('Opening %s for output' % options.output)
    fps = inputVideo.get(cv2.cv.CV_CAP_PROP_FPS)
    codec = int(inputVideo.get(cv2.cv.CV_CAP_PROP_FOURCC))
    if codec == cv2.cv.CV_FOURCC('a','v','c','1'):
        # The ffmpeg output for x264 is broken, so switch formats
        codec = cv2.cv.CV_FOURCC('d','i','v','x')
    outputVideo = cv2.VideoWriter(options.output,
                                  codec,
                                  30.0,
                                  (frame.shape[1], frame.shape[0]))
                                  

    frameNo = 0
    valence = 0
    sampleStep = int(fps / options.fps)
    while moreData:
        if frameNo % 500 == 0:
            _log.info('Processed %i frames' % frameNo)

        if frameNo % sampleStep == 0:
            valence, attr = mod.score(frame, frameNo, inputVideo)

        outputVideo.write(RenderValence(frame, valence, attr))
            
        moreData, frame = inputVideo.read()
        frameNo += 1
        
if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--model', default=None,
                      help='File containing the model')
    parser.add_option('--input', '-i', default=None,
                      help='The video to process')
    parser.add_option('--fps', default=15.0, type='float',
                      help='Frame rate to sample valence score')
    parser.add_option('--output', '-o', default=None,
                      help='Video output. Should be an avi probably.')
    
    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(options)
