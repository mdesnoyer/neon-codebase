import os.path
import sys
import math
import itertools
import cv2
import numpy as np
import sys
import dlib
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
from utils.options import define, options
from scipy.fftpack import idct
from scipy.fftpack import dct

define("haarFileFront",
       default=os.path.join(os.path.dirname(__file__),
                           'data/haarcascade_frontalface_alt2.xml'),
       type=str,
       help="Front face detector haar cascade file.")

define("haarFileProfile",
       default=os.path.join(os.path.dirname(__file__),
                           'data/haarcascade_profileface.xml'),
       type=str,
       help="Profile face detector haar cascade file.")

define("textClassifier1",
       default=os.path.join(os.path.dirname(__file__),
                           'data/trained_classifierNM1.xml'),
       type=str,
       help="Trained text classifier for step 1.")

define("textClassifier2",
       default=os.path.join(os.path.dirname(__file__),
                           'data/trained_classifierNM2.xml'),
       type=str,
       help="Trained text classifier for step 2.")

class ImageSignatureSaliency(object):
    ''' Image signature saliency implementation.
    Based on paper: Image signature: Highlighting sparse salient regions.
    X Hou, J Harel, C Koch. 2012
    '''
    def __init__(self, src, map_size = 640):
        # self.lab_im = cv2.cvtColor(src, cv2.COLOR_BGR2LAB)
        # l_channel, a_channel, b_channel = cv2.split(self.lab_im)
        # Resize the image to the longest edge 64.
        if len(src.shape) == 2:
            src = cv2.cvtColor(src, cv2.COLOR_GRAY2BGR)
        self.src = src
        ratio = max(src.shape[0]/64.0, src.shape[1]/64.0)
        im = cv2.resize(src, (int(src.shape[1]/ratio), int(src.shape[0]/ratio)))
        sal_map = np.zeros(im.shape)
        for i in xrange(0, im.shape[2]):
            sal_map[0:, 0:, i] = np.power(idct(np.sign(dct(im[0:,0:, i]))), 2)
        out_map = np.sum(sal_map, 2)
        self.map_width = int(im.shape[1] * map_size / 64.0)
        self.map_height = int(im.shape[0] * map_size / 64.0)
        out_map_resized = cv2.resize(out_map, (self.map_width, self.map_height))

        self.smooth_map = cv2.GaussianBlur(out_map_resized,
                                      (map_size/20*2+1, map_size/20*2+1),
                                      map_size/15.0)
        self.smooth_map = np.uint8((self.smooth_map - np.min(self.smooth_map)) /
                              (np.max(self.smooth_map) - np.min(self.smooth_map)) * 255)

    def get_saliency_map(self):
        return cv2.resize(self.smooth_map,
                          (self.src.shape[1], self.src.shape[0]))

    def draw_resized_im(self):
        ''' This function is to show the bounderies of the saliency. '''
        resized_im = cv2.resize(self.src, (self.map_width, self.map_height))
        w = resized_im.shape[1]
        h = resized_im.shape[0]
        saliency_threshold = 50
        for x_begin in xrange(0, w-1):
            if max(self.smooth_map[0:, x_begin] > saliency_threshold):
                break
        for x_end in xrange(w-1, 0, -1):
            if max(self.smooth_map[0:, x_end] > saliency_threshold):
                break
        cv2.rectangle(resized_im, (x_begin, 0), (x_end, h), (0, 255, 255), 3, 8)
        return resized_im


class SmartCrop(object):
    _instance_ = None
    def __init__(self):
        self.haarFileFront = options.haarFileFront
        self.haarFileProfile = options.haarFileProfile
        self.textClassifier1 = options.textClassifier1
        self.textClassifier2 = options.textClassifier2
        self.front_face_cascade = cv2.CascadeClassifier()
        self.profile_face_cascade = cv2.CascadeClassifier()
        self.front_face_cascade.load(self.haarFileFront)
        self.profile_face_cascade.load(self.haarFileProfile)
        self.dlib_face_detector = dlib.get_frontal_face_detector()
        self.haarParams = {'minNeighbors': 8, 'minSize': (50, 50), 'scaleFactor': 1.1}

    @classmethod
    def get_cropper(cls):
        ''' Return a singlton instance. '''
        cls._instance_ = cls._instance_ or SmartCrop()
        return cls._instance_

    def generate_saliency_map(self, im):
        (height, width, elem) = im.shape
        half_im = cv2.resize(im, (width / 4, height / 4))
        half_sm = SaliencyMap(half_im)
        full_sm = cv2.resize(half_sm.map, (width, height))
        return full_sm

    def detect_front_faces(self, im):
        prep_im = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        faces = self.dlib_face_detector(prep_im)
        face_array = np.zeros((len(faces), 4), int)
        for i, face in enumerate(faces):
            face_array[i, 0:] = np.array([face.left(), face.top(),
                                          face.width(), face.height()])
        return face_array

    def detect_faces(self, im):
        front_faces = self.detect_front_faces(im)
        profile_faces = \
            self.profile_face_cascade.detectMultiScale(im, **self.haarParams)
        im_flip = cv2.flip(im, 1)
        flip_profile_faces = \
            self.profile_face_cascade.detectMultiScale(im_flip, **self.haarParams)

        if len(flip_profile_faces) != 0:
            flip_profile_faces[0:, 0] = im.shape[1] - (flip_profile_faces[0:, 0] +
                                                       flip_profile_faces[0:, 2])
            if len(profile_faces) == 0:
                profile_faces = flip_profile_faces
            else:
                profile_faces = np.append(profile_faces, flip_profile_faces, axis=0)


        if len(front_faces) == 0:
            return profile_faces
        elif len(profile_faces) == 0:
            return front_faces
        else:
            faces = np.append(front_faces, profile_faces, axis=0)
            return faces

    def text_crop(self, im, draw_im = None):
        # Downsize the image first. Make the longest edge to be 360 pixels.
        height = im.shape[0]
        width = im.shape[1]
        ratio = max(im.shape[0]/600.0, im.shape[1]/600.0)
        im_resized = cv2.resize(im, (int(im.shape[1]/ratio),
                                     int(im.shape[0]/ratio)))
        # Text detector is defined in erfilter.cpp in neon version of opencv3
        # textDetect(InputArray _src, const String &model_1, const String &model_2,
        #         int thresholdDelta,
        #         float minArea, float maxArea, float minProbability,
        #         bool nonMaxSuppression, float minProbabilityDiff,
        #         float minProbablity_2,
        #         vector<Rect> &groups_boxes, OutputArray _dst)

        boxes, mask = cv2.text.textDetect(im_resized,
            self.textClassifier1,
            self.textClassifier2,
            16,0.00015,0.003,0.8,True,0.5, 0.9)

        # draw on it if it is provided, for debugging purpose.
        # Assuming the draw_im is a copy of im.
        if len(boxes) == 0:
            return im
        boxes *= ratio
        boxes = boxes.astype(int)
        # Draw im is used purely for display purposes.
        if draw_im is not None:
            for box in boxes:
                tl = (box[0], box[1])
                br = (box[0] + box[2], box[1] + box[3])
                cv2.rectangle(draw_im, tl, br, ( 0, 255, 255 ), 3, 8)

        bottom = height * 0.7

        top_height_array = []
        for box in boxes:
            if box[1] < bottom:
                continue
            top_height_array.append(box[1])
        # leave 3 pixels for cushion.
        if not top_height_array:
            return im
        top_height = min(top_height_array) - 3
        new_width = top_height * width / height
        x = (width - new_width)/2
        cropped_im = im[0 : top_height, x:x+new_width]
        return cropped_im


    def crop_and_resize(self, src, w, h):
        ''' Find the cropped area maximizes the summation of the saliency
        value
        '''
        saliency_threshold = 50
        im = src.copy()

        (height, width, elem) = im.shape

        saliency = ImageSignatureSaliency(im)
        saliency_map = saliency.get_saliency_map()
        faces = self.detect_faces(im)

        # Saliency Map is calculated then trimmed to along the boundaries
        # to remove low saliency area.
        if float(h) / height > float(w) / width:
            new_width = int(height / float(h) * float(w))
            new_height = height
            # Crop horizontally
            for x_begin in xrange(0, width-1):
                if max(saliency_map[0:, x_begin] > saliency_threshold):
                    break
            for x_end in xrange(width-1, 0, -1):
                if max(saliency_map[0:, x_end] > saliency_threshold):
                    break
            # If there are high saliency points, then they will meet in the
            # center of the target. If there are no high saliency point,
            # both x_begin and x_end will reach to the ends, then the mean is
            # the middle, so it still works.
            center_horizontal = (x_begin + x_end) / 2
            center_vertical = height / 2
        else:
            new_width = width
            new_height = int(width / float(w) * float(h))
            # Crop vertically
            for y_begin in xrange(0, height-1):
                if max(saliency_map[y_begin, 0:] > saliency_threshold):
                    break
            for y_end in xrange(height-1, 0, -1):
                if max(saliency_map[y_end, 0:] > saliency_threshold):
                    break
            # If there are high saliency points, then they will meet in the
            # center of the target. If there are no high saliency point,
            # both x_begin and x_end will reach to the ends, then the mean is
            # the middle, so it still works.
            center_horizontal = width / 2
            center_vertical = (y_begin + y_end) / 2

        new_x = center_horizontal - new_width / 2
        new_x = 0 if new_x < 0 else new_x
        new_x = (width - new_width)  if new_x > (width - new_width) else new_x

        new_y = center_vertical - height / 2
        new_y = 0 if new_y < 0 else new_y
        new_y = (height - new_height) if new_y > (height - new_height) else new_y

        new_x_end = new_x + new_width - 1
        new_y_end = new_y + new_height - 1

        maximum_try = 3
        # Check if face is being cut off. Try for example 3 times
        for count in xrange(maximum_try):
            face_cut_left = 0
            face_cut_right = 0
            face_left_bound = 0
            face_right_bound = width - 1
            for face in faces:
                if face[0] < new_x and face[0] + face[2] - 1 >= new_x:
                    face_cut_left = face[3]
                    face_left_bound = face[0] - 10
                if face[0] < new_x_end and face[0] + face[2] - 1 >= new_x_end:
                    face_cut_right = face[3]
                    face_right_bound = face[0] + face[2] - 1 + 10
            if face_cut_left == 0 and face_cut_right == 0:
                break
            if face_cut_left > face_cut_right:
                new_x = face_left_bound
            else:
                new_x = face_right_bound - new_width + 1

        cropped_im = src[new_y:new_y+new_height, new_x:new_x+new_width]

        text_cropped_im = self.text_crop(cropped_im)

        resized_im = cv2.resize(text_cropped_im, (w, h))
        return resized_im
