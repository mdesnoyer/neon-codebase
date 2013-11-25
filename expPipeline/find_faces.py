#!/usr/bin/env python
'''Script that finds all the images with faces in them.

Input is a list of image paths, one per line. Output is a stream of
images with faces and the detected face count like:

<image_name>,<face_count>

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import csv
import cv2
import numpy as np
from optparse import OptionParser
import os.path
import sys

FRONT_FACE_MODEL = 'haarcascade_frontalface_alt.xml'
PROFILE_FACE_MODEL = 'haarcascade_profileface.xml'

def main(options):
    frontDetector = cv2.CascadeClassifier(os.path.join(options.face_models,
                                                       FRONT_FACE_MODEL))
    sideDetector = cv2.CascadeClassifier(os.path.join(options.face_models,
                                                       PROFILE_FACE_MODEL))
    
    inputStream = sys.stdin
    if options.input is not None:
        inputStream = open(options.input)

    outputStream = sys.stdout
    if options.output is not None:
        outputStream = open(options.output, 'w')
    outputWriter = csv.writer(outputStream)

    try:
        for line in inputStream:
            imageFile = line.strip()
            image = cv2.imread(imageFile)
            if image is not None:
                frontFaces = frontDetector.detectMultiScale(image, 1.1, 5, 0,
                                                            (100,100))
                sideFaces = sideDetector.detectMultiScale(image, 1.1, 5, 0,
                                                          (100, 100))
                nFaces = len(frontFaces) + len(sideFaces)

                if nFaces > 0:
                    outputWriter.writerow([imageFile, nFaces])

    finally:
        inputStream.close()
        outputStream.close()

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--input', '-i', default=None,
                      help='File containing a list of images to check. Defaults to STDIN')
    parser.add_option('--output', '-o', default=None,
                      help='Output file of images with faces . Defaults to STDOUT')
    parser.add_option('--face_models',
                      default='/usr/local/share/OpenCV/haarcascades/',
                      help='Directory containing the face models')
    
    options, args = parser.parse_args()

    main(options)
