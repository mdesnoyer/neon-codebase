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
import time
define("saliency_threshold", default=50, type=int,
       help="Saliency cut off threshold for boarder trimming.")

define("haar_profile",
       default=os.path.join(os.path.dirname(__file__),
                           'data/haarcascade_profileface.xml'),
       type=str,
       help="Profile face detector haar cascade file.")

define("text_classifier1",
       default=os.path.join(os.path.dirname(__file__),
                           'data/trained_classifierNM1.xml'),
       type=str,
       help="Trained text classifier for step 1.")

define("text_classifier2",
       default=os.path.join(os.path.dirname(__file__),
                           'data/trained_classifierNM2.xml'),
       type=str,
       help="Trained text classifier for step 2.")


def tic():
    #Homemade version of matlab tic and toc functions
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()
    return startTime_for_tictoc

def toc():
    if 'startTime_for_tictoc' in globals():
        print "Elapsed time is " + str(time.time() - startTime_for_tictoc) + " seconds."
    else:
        print "Toc: start time not set"
    return time.time()

class ImageSignatureSaliency(object):
    ''' Image signature saliency implementation.
    Based on paper: Image signature: Highlighting sparse salient regions.
    X Hou, J Harel, C Koch. 2012
    '''
    def __init__(self, src, map_size = 640):
        # self.lab_im = cv2.cvtColor(src, cv2.COLOR_BGR2LAB)
        # l_channel, a_channel, b_channel = cv2.split(self.lab_im)
        # Resize the image to the longest edge 64.
        self._saliency_threshold = options.saliency_threshold
        if len(src.shape) == 2:
            src = cv2.cvtColor(src, cv2.COLOR_GRAY2BGR)
        self.src = src
        ratio = max(src.shape[0]/64.0, src.shape[1]/64.0)
        im = cv2.resize(src, (int(src.shape[1]/ratio), int(src.shape[0]/ratio)))
        sal_map = np.zeros(im.shape)
        for i in xrange(0, im.shape[2]):
            sal_map[0:, 0:, i] = np.power(idct(np.sign(dct(
                                    im[0:,0:, i].astype(float)))), 2)
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
        for x_begin in xrange(0, w-1):
            if max(self.smooth_map[0:, x_begin] > self._saliency_threshold):
                break
        for x_end in xrange(w-1, 0, -1):
            if max(self.smooth_map[0:, x_end] > self._saliency_threshold):
                break
        cv2.rectangle(resized_im.astype(uint8), (x_begin, 0), (x_end, h),
                      (0, 255, 255), 3, 8)
        return resized_im


class SmartCrop(object):
    _instance_ = None
    def __init__(self, image):
        ''' This function should not be called by directly.
        Using the get_cropper to get the singlton instead.
        '''
        self.haar_profile = options.haar_profile
        self.profile_face_cascade = cv2.CascadeClassifier()
        self.profile_face_cascade.load(self.haar_profile)
        self.text_classifier1 = options.text_classifier1
        self.text_classifier2 = options.text_classifier2

        self.dlib_face_detector = dlib.get_frontal_face_detector()
        self.haar_params = {'minNeighbors': 8, 'minSize': (50, 50), 'scaleFactor': 1.1}
        self._saliency_map = None
        self._saliency_threshold = options.saliency_threshold
        self._faces = None
        self._text_boxes = None
        self.image = image
        self._bottom_percent = 0.7



    # @classmethod
    # def get_cropper(cls):
    #     ''' Return a singlton instance. '''
    #     cls._instance_ = cls._instance_ or SmartCrop()
    #     # Check if options have changed.
    #     if cls._instance_.haar_profile != options.haar_profile:
    #         cls._instance_.haar_profile = options.haar_profile
    #         cls._instance_.profile_face_cascade = cv2.CascadeClassifier()
    #         cls._instance_.profile_face_cascade.load(cls._instance_.haar_profile)

    #     cls._instance_.text_classifier1 = options.text_classifier1
    #     cls._instance_.text_classifier2 = options.text_classifier2
    #     return cls._instance_

    def get_saliency_map(self):
        if self._saliency_map is None:
            saliency = ImageSignatureSaliency(self.image)
            self._saliency_map = saliency.get_saliency_map()
        return self._saliency_map

    def detect_front_faces(self, im):
        prep_im = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        height = self.image.shape[0]
        width = self.image.shape[1]
        ratio = max(self.image.shape[0]/600.0, self.image.shape[1]/600.0)
        im_resized = cv2.resize(prep_im, (int(self.image.shape[1]/ratio),
                                          int(self.image.shape[0]/ratio)))
        faces = self.dlib_face_detector(im_resized)
        face_array = np.zeros((len(faces), 4), int)
        for i, face in enumerate(faces):
            face_array[i, 0:] = np.array([face.left(), face.top(),
                                          face.width(), face.height()])
        face_array *= ratio
        face_array = face_array.astype(int)

        return face_array

    def detect_faces(self):
        if self._faces is None:
            self._faces = self._detect_faces(self.image)
        return self._faces

    def _detect_faces(self, im):
        front_faces = self.detect_front_faces(im)
        # return front_faces
        tic()
        height = self.image.shape[0]
        width = self.image.shape[1]
        ratio = max(self.image.shape[0]/600.0, self.image.shape[1]/600.0)
        im_resized = cv2.resize(im, (int(self.image.shape[1]/ratio),
                                          int(self.image.shape[0]/ratio)))
        profile_faces = \
            self.profile_face_cascade.detectMultiScale(im_resized, **self.haar_params)
        if len(profile_faces) == 0:
            profile_faces = np.array([])
        profile_faces *= ratio
        profile_faces = profile_faces.astype(int)
        im_flip = cv2.flip(im_resized, 1)
        flip_profile_faces = \
            self.profile_face_cascade.detectMultiScale(im_flip, **self.haar_params)
        if (len(flip_profile_faces) == 0):
            flip_profile_faces = np.array([])
        flip_profile_faces *= ratio
        flip_profile_faces = flip_profile_faces.astype(int)
        print "time for profile_faces."
        toc()
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

    def get_text_boxes(self):
        if self._text_boxes is None:
            # Downsize the image first. Make the longest edge to be 360 pixels.
            height = self.image.shape[0]
            width = self.image.shape[1]
            ratio = max(self.image.shape[0]/600.0, self.image.shape[1]/600.0)
            im_resized = cv2.resize(self.image, (int(self.image.shape[1]/ratio),
                                                 int(self.image.shape[0]/ratio)))
            cut_top = int(self._bottom_percent * im_resized.shape[0])
            bottom_image = im_resized[cut_top:, 0:]

            # Text detector is defined in erfilter.cpp in neon version of opencv3
            # textDetect(InputArray _src, const String &model_1, const String &model_2,
            #         int thresholdDelta,
            #         float minArea, float maxArea, float minProbability,
            #         bool nonMaxSuppression, float minProbabilityDiff,
            #         float minProbablity_2,
            #         vector<Rect> &groups_boxes, OutputArray _dst)

            boxes, mask = cv2.text.textDetect(bottom_image,
                self.text_classifier1,
                self.text_classifier2,
                16, # thresholdDelta, steps for MSER
                0.00015, # min area, ratio to the total area
                0.003, # max area, ratio to the total area
                0.8, # min probablity for step 1
                True, # bool nonMaxSuppression
                0.5, # min probability differernce
                0.9 # min probability for step 2
                )
            if len(boxes) == 0:
                return np.array([])
            boxes[0:, 1] += cut_top
            boxes *= ratio
            boxes = boxes.astype(int)

            self._text_boxes = boxes

        return self._text_boxes

    def text_crop(self, x, y, width, height, draw_im=None):
        ''' Detect the text in the lower part of the image and remove it
        draw_im is used to mark the text box area, it can be the copy of im.
        When draw_im is used, yellow boxes are drawn to show the text area.
        x, y, w, h is the cut off region of the image. We will only concern
        the images in the rectangle.
        '''
        boxes = self.get_text_boxes()
        # draw on it if it is provided, for debugging purpose.
        # Assuming the draw_im is a copy of im.
        if len(boxes) == 0:
            return self.image[y:y+height, x:x+width]

        # Draw im is used purely for display purposes.
        if draw_im is not None:
            for box in boxes:
                tl = (box[0], box[1])
                br = (box[0] + box[2], box[1] + box[3])
                cv2.rectangle(draw_im, tl, br, ( 0, 255, 255 ), 3, 8)

        bottom = height * 0.7 + y

        upper_text_y_array = []
        for box in boxes:
            if box[1] < bottom:
                continue
            upper_text_y_array.append(box[1])
        # leave 3 pixels for cushion.
        if not upper_text_y_array:
            return self.image[y:y+height, x:x+width]
        upper_text_y = min(upper_text_y_array) - 3
        new_height = upper_text_y - y
        new_width = new_height * width / height
        new_x = x + (width - new_width)/2
        new_x_end = x + new_width
        cropped_im = self.image[y:upper_text_y, x:x+new_width]
        faces = self.detect_faces()
        face_cut_left = 0
        face_cut_right = 0
        for face in faces:
            if face[0] < new_x and face[0] + face[2] - 1 >= new_x:
                face_cut_left = face[3]
                face_left_bound = x
            if face[0] < new_x_end and face[0] + face[2] - 1 >= new_x_end:
                face_cut_right = face[3]
                face_right_bound = x + width - 1
        if face_cut_left > face_cut_right:
            new_x = face_left_bound
        if face_cut_right > face_cut_left:
            new_x = face_right_bound - new_width + 1

        return self.image[y:y+new_height, new_x:new_x+new_width]

    def face_adjust(faces, x):
        ''' Search nearby and horizontally to avoid face cutting.
        Face exclusion is also possible.
        '''
        pass

    def crop_and_resize(self, h, w):
        ''' Find the cropped area maximizes the summation of the saliency
        value
        '''
        # t_begin = tic()
        text_boxes = self.get_text_boxes()
        # print "get_text_boxes"
        # toc()
        # tic()
        if len(text_boxes) == 0:
            neutral_width = float(self.image.shape[1]) * h / self.image.shape[0]
            if abs(neutral_width - w) <= 5.0:
                return cv2.resize(self.image, (w, h))

        im = self.image.copy()

        (height, width, elem) = self.image.shape
        # print "before saliency"
        # toc()
        # tic()
        saliency_map = self.get_saliency_map()
        # print "after saliency"
        # toc()
        # tic()
        faces = self.detect_faces()
        # print "after face"
        # toc()
        # tic()

        # Saliency Map is calculated then trimmed to along the boundaries
        # to remove low saliency area.
        if float(h) / height > float(w) / width:
            new_width = int(height / float(h) * float(w))
            new_height = height
            # Crop horizontally
            for x_begin in xrange(0, width-1):
                if max(saliency_map[0:, x_begin] > self._saliency_threshold):
                    break
            for x_end in xrange(width-1, 0, -1):
                if max(saliency_map[0:, x_end] > self._saliency_threshold):
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
                if max(saliency_map[y_begin, 0:] > self._saliency_threshold):
                    break
            for y_end in xrange(height-1, 0, -1):
                if max(saliency_map[y_end, 0:] > self._saliency_threshold):
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

        # cropped_im = im[new_y:new_y+new_height, new_x:new_x+new_width]

        # print "before text"
        # toc()
        # tic()
        text_cropped_im = self.text_crop(new_x, new_y, new_width, new_height)

        # print "after text"
        # toc()
        # tic()
        resized_im = cv2.resize(text_cropped_im, (w, h))
        # "end"
        # t_end = toc()
        # print "total", (t_end-t_begin), "seconds"
        return resized_im
