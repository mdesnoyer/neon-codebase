import os.path
import sys
import math
import itertools
import cv2
import numpy as np
import sys
import dlib
import logging
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
from utils.options import define, options
from scipy.fftpack import idct
from scipy.fftpack import dct
import time

_log = logging.getLogger(__name__)

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
    '''Crop and resize images using face, text and saliency.
    '''
    _instance_ = None
    _dlib_face_detector = None
    def __init__(self, image,
                 with_saliency=True,
                 with_face_detection=True,
                 with_text_detection=True):
        self.image = image
        self.with_saliency = with_saliency
        self.with_face_detection = with_face_detection
        self.with_text_detection = with_text_detection
        if with_face_detection:
            self.haar_profile = options.haar_profile
            self.profile_face_cascade = cv2.CascadeClassifier()
            self.profile_face_cascade.load(self.haar_profile)
            if SmartCrop._dlib_face_detector is None:
                SmartCrop._dlib_face_detector = \
                  dlib.get_frontal_face_detector()
            self.dlib_face_detector = SmartCrop._dlib_face_detector
            
            self.haar_params = {'minNeighbors': 8,
                                'minSize': (100, 100),
                                'scaleFactor': 1.1}
            self._faces = None

        if with_text_detection:
            self.text_classifier1 = options.text_classifier1
            self.text_classifier2 = options.text_classifier2
            self._text_boxes = None
            self._bottom_percent = 0.7

        if with_saliency:
            self._saliency_map = None
            self._saliency_threshold = options.saliency_threshold


    def get_saliency_map(self):
        if self._saliency_map is None:
            saliency = ImageSignatureSaliency(self.image)
            self._saliency_map = saliency.get_saliency_map()
        return self._saliency_map

    def detect_front_faces(self, im):
        '''Using dlib to detect front faces.
        '''
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
        face_array = (face_array.astype(np.float64) * ratio).astype(np.int32)

        return face_array

    def detect_profile_faces(self, im):
        '''Using haar detector to detect profile faces.
        '''
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
        if len(flip_profile_faces) != 0:
            flip_profile_faces[0:, 0] = im.shape[1] - (flip_profile_faces[0:, 0] +
                                                       flip_profile_faces[0:, 2])
            if len(profile_faces) == 0:
                profile_faces = flip_profile_faces
            else:
                profile_faces = np.append(profile_faces, flip_profile_faces, axis=0)
        return profile_faces

    def detect_faces(self):
        '''Detect both frontal and profile faces and combine the results.
        self._faces is set to only calculate once.
        '''
        if self._faces is not None:
            return self._faces

        front_faces = self.detect_front_faces(self.image)
        profile_faces = self.detect_profile_faces(self.image)

        if len(front_faces) == 0:
            faces = profile_faces
        elif len(profile_faces) == 0:
            faces = front_faces
        else:
            faces = np.append(front_faces, profile_faces, axis=0)
        self._faces = faces
        return faces

    def get_text_boxes(self):
        '''Detect text boxes using opencv3 text detector.
        The text detector is created in C++ language using the opencv3.
        Please refer the neon opencv_contrib branch for details.
        '''
        if self._text_boxes is not None:
            return self._text_boxes

        # Downsize the image first. Make the longest edge to be 600 pixels.
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
            self._text_boxes = np.array([])
            return self._text_boxes
        boxes[0:, 1] += cut_top
        boxes = (boxes.astype(np.float64) * ratio).astype(np.int32)

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
        new_height = min(height, upper_text_y - y)
        new_width = new_height * width / height
        new_x = x + (width - new_width)/2
        new_x_end = x + new_width
        if self.with_face_detection:
            faces = self.detect_faces()
            new_x = self.face_adjust(faces, new_x, new_width)

        return self.image[y:y+new_height, new_x:new_x+new_width]

    def _insert_interval(self, interval_group, new_interval):
        '''Insert a new interval into the interval group.
        If it overlaps with existing intervals, combine them.
        The intervals can be viewed as the face cuts.
        '''
        new_interval_group = []
        for interval in interval_group:
            if new_interval[0] >= interval[0] and new_interval[0] <= interval[1] or \
               new_interval[1] >= interval[0] and new_interval[1] <= interval[1]:
                new_interval = (min(new_interval[0], interval[0]),
                                max(new_interval[1], interval[1]))
            else:
                new_interval_group.append(interval)
        new_interval_group.append(new_interval)
        return new_interval_group

    def _get_interval_sum(self, interval_group):
        '''The length summation of all the intervals.
        For example, if a vertical line cut multiple faces, this returns the
        total length of face cuts.
        '''
        lengths = [x[1] - x[0] + 1 for x in interval_group]
        return sum(lengths)

    def _face_cut_length(self, left_line, right_line, faces):
        '''Check the left and right bounding box to return the size of face cut.
        If there is not face cut, returns 0; otherwise, returns the max.
        '''
        left_cuts = []
        right_cuts = []
        for face in faces:
            # left bound of the box is cutting off the face
            if face[0] < left_line and face[0] + face[2] - 1 >= left_line:
                left_cuts = self._insert_interval(left_cuts,
                                                  (face[1], face[1] + face[3] -1))
            # right bound of the box is cutting off the face
            if face[0] < right_line and face[0] + face[2] - 1 >= right_line:
                right_cuts = self._insert_interval(right_cuts,
                                                  (face[1], face[1] + face[3] -1))

        left_length = self._get_interval_sum(left_cuts)
        right_length = self._get_interval_sum(right_cuts)
        return max(left_length, right_length)


    def face_adjust(self, faces, new_x, new_width):
        ''' Search nearby and horizontally to avoid face cutting.
        '''
        face_cusion = 10
        (height, width, elem) = self.image.shape
        new_x_end = new_x + new_width - 1
        # Search to the left
        # search to the right
        # Check if face is being cut off. Try for example 3 times
        min_face_cut = height
        min_cut_left = new_x
        # Search all the spaces to the left to see if we can have non cut face.
        for left_x in xrange(new_x, -1, -1):
            left_x_end = left_x + new_width - 1
            face_cut = self._face_cut_length(left_x, left_x_end, faces)
            if face_cut < min_face_cut:
                min_face_cut = face_cut
                min_cut_left = left_x
            if face_cut == 0:
                break

        min_face_cut = height
        min_cut_right = new_x
        # Search all the spaces to the right to see if we can have non cut face.
        for right_x in xrange(new_x, width - new_width + 2):
            right_x_end = right_x + new_width - 1
            face_cut = self._face_cut_length(right_x, right_x_end, faces)
            if face_cut < min_face_cut:
                min_face_cut = face_cut
                min_cut_right = right_x
                if face_cut == 0:
                    break

        if new_x - min_cut_left < min_cut_right - new_x:
            new_x = min_cut_left
        else:
            new_x = min_cut_right
        # Check if the new_x is still in bounding range. Just to be safe
        new_x = 0 if new_x < 0 else new_x
        new_x = (width - new_width)  if new_x > (width - new_width) else new_x
        return new_x

    def saliency_adjust(self, h, w):
        '''Crop the image using saliency map.
        The current method is to crop the image conservatively. 
        '''
        (height, width, elem) = self.image.shape
        # tic()
        saliency_map = self.get_saliency_map()
        # print "after saliency"
        # toc()
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

        return (new_x, new_y, new_width, new_height)


    def crop_and_resize(self, h, w):
        ''' Crop and resize to the new size, based on face, saliency and text.
        Saliency crop is biased to the center of the image.
        '''
        (height, width, elem) = self.image.shape
        if self.with_text_detection:
            text_boxes = self.get_text_boxes()
            if len(text_boxes) == 0:
                neutral_width = float(width) * h / height
                if abs(neutral_width - w) <= 5.0:
                    return cv2.resize(self.image, (w, h))

        # Get the default crop locations.
        if float(h) / height > float(w) / width:
            new_width = int(height / float(h) * float(w))
            new_height = height
        else:
            new_width = width
            new_height = int(width / float(w) * float(h))
        center_horizontal = width / 2
        center_vertical = height / 2
        new_x = center_horizontal - new_width / 2
        new_y = center_vertical - new_height / 2


        if self.with_saliency:
            (new_x, new_y, new_width, new_height) = self.saliency_adjust(h, w)

        if self.with_face_detection:
            # tic()
            faces = self.detect_faces()
            # print "after face"
            # toc()
            new_x = self.face_adjust(faces, new_x, new_width)


        if self.with_text_detection:
            cropped_im = self.text_crop(new_x, new_y, new_width, new_height)
        else:
            cropped_im = self.image[new_y:new_y+new_height,
                                    new_x:new_x+new_width]

        resized_im = cv2.resize(cropped_im, (w, h))
        return resized_im
