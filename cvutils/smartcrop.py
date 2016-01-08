import os.path
import sys
import math
import itertools
import cv2
import numpy as np
from scipy.stats import norm
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

        # Makde the saliency map to be more weighted towards to the center.
        bias_filter = self._create_center_bias(self.map_height, self.map_width)
        self.smooth_map *= bias_filter
        self.smooth_map = np.uint8((self.smooth_map - np.min(self.smooth_map)) /
                              (np.max(self.smooth_map) - np.min(self.smooth_map)) * 255)

    def get_saliency_map(self):
        return cv2.resize(self.smooth_map,
                          (self.src.shape[1], self.src.shape[0]))

    def _create_center_bias(self, height, width):
        '''For a given input im, create the rule of third filter match to size.
        Adding a gaussian shaped boost horizontally at the third left and right.
        '''
        target_filter = np.ones((height, width))
        # A Gaussian sample is created 4/3 width of the image width. The samples
        # are done symatrically. To get a rule of third bias, we either take the
        # array from the start or from the end. Then the center of the original
        # curve will be the new peaks located at the thrids of the target_filter.

        ppf_begin = 0.12
        ppf_end = 1 - ppf_begin
        x = np.linspace(norm.ppf(ppf_begin), norm.ppf(ppf_end), width)
        gaussian_x = np.array([norm.pdf(x)]) # make the sample 2d array
        gaussian_array_h = np.repeat(gaussian_x, height, axis=0)
        y = np.linspace(norm.ppf(ppf_begin), norm.ppf(ppf_end), height)
        gaussian_y = np.array([norm.pdf(y)]) # make the sample 2d array        
        gaussian_array_v = np.repeat(gaussian_y, width, axis=0)
        gaussian_array_v = np.transpose(gaussian_array_v)

        gaussian_array = gaussian_array_h * gaussian_array_v

        # Normalize the filter
        bias_filter = gaussian_array / gaussian_array.max()

        # cv2.imshow('filter', bias_filter)
        # cv2.waitKey(0)
        return bias_filter

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
                 with_text_detection=True,
                 no_text_crop_same_aspect_ratio=False):
        self.image = image
        self.with_saliency = with_saliency
        self.with_face_detection = with_face_detection
        self.with_text_detection = with_text_detection
        self.no_text_crop_same_aspect_ratio = no_text_crop_same_aspect_ratio
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

    def create_rule_of_third_filter(self, height, width):
        '''For a given input im, create the rule of third filter match to size.
        Adding a gaussian shaped boost horizontally at the third left and right.
        '''
        target_filter = np.ones((height, width))
        # A Gaussian sample is created 4/3 width of the image width. The samples
        # are done symatrically. To get a rule of third bias, we either take the
        # array from the start or from the end. Then the center of the original
        # curve will be the new peaks located at the thrids of the target_filter.

        line_size = width * 2 / 3
        x = np.linspace(norm.ppf(0.1), norm.ppf(0.9), line_size)
        gaussian_x = np.array([norm.pdf(x)]) # make the sample 2d array
        gaussian_array = np.repeat(gaussian_x, height, axis=0)
        target_filter[:, 0:width/2] = gaussian_array[:, 0:width/2]
        target_filter[:, width/2:] = gaussian_array[:, -(width-width/2):]

        v_target_filter = np.ones((width, height))
        v_line_size = height * 2 / 3
        y = np.linspace(norm.ppf(0.1), norm.ppf(0.9), v_line_size)
        gaussian_y = np.array([norm.pdf(y)]) # make the sample 2d array
        v_gaussian_array = np.repeat(gaussian_y, width, axis=0)
        v_target_filter[:, 0:height/2] = v_gaussian_array[:, 0:height/2]
        v_target_filter[:, height/2:] = v_gaussian_array[:, -(height-height/2):]
        v_target_filter = v_target_filter.transpose()

        target_filter *= v_target_filter

        # Normalize the filter
        # Uncomment to visualize the filter.
        # cv2.imshow('filter', target_filter/target_filter.max())
        # cv2.waitKey(0)
        target_filter = target_filter / sum(sum(target_filter))
        return target_filter

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
        face_array *= ratio
        face_array = face_array.astype(int)

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
        boxes *= ratio
        boxes = boxes.astype(int)

        self._text_boxes = boxes

        return self._text_boxes

    def face_saliency_boost(self):
        '''Using faces to boost corresponding area of saliency.
        '''
        height = self.image.shape[0]
        width = self.image.shape[1]
        boost_array = np.zeros((height, width))
        faces = self.detect_faces()
        for box in faces:
            cv2.rectangle(boost_array, (box[0], box[1]),
                          (box[0] + box[2] - 1, box[1] + box[3] - 1),
                          (255, 255, 255),
                          cv2.FILLED)
        boost_array = (boost_array > 0).astype(float)
        return boost_array

    def _get_face_zeros(self, faces, im_width, crop_width):
        '''
        Find locations that the cropping cuts off faces.
        '''
        x_loc_array = np.ones(im_width - crop_width + 1)
        for face in faces:
            face_x = face[0]
            face_x_end = face[0] + face[2] - 1
            crop_left_begin = face_x - crop_width + 1
            crop_left_end = face_x_end - crop_width + 1
            crop_right_begin = face_x + crop_width - 1
            crop_right_end = face_x_end + crop_width - 1
            if crop_left_end >= 0:
                x_loc_array[max(crop_left_begin, 0):crop_left_end+1] = 0
            if crop_right_begin < im_width:
                x_loc_array[face_x:min(im_width - crop_width + 1, face_x_end) + 1] = 0
        return x_loc_array

    def saliency_face_crop(self, saliency_map, h, w):
        height = saliency_map.shape[0]
        width = saliency_map.shape[1]
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
        new_y = center_vertical - height / 2

        if self.with_face_detection:
            faces = self.detect_faces()
            # Get the vector of starting cropping location
            face_zero_vector = self._get_face_zeros(faces, width, new_width)

        if self.with_saliency:
            fil = self.create_rule_of_third_filter(new_height, new_width)
            # import ipdb; ipdb.set_trace()
            filter_result = cv2.filter2D(saliency_map, cv2.CV_32F, fil)
            # search horizontally, since the result is the same dimension as
            # the saliency_map, we take the truncated end. It is 1D vector.
            filter_line = filter_result[new_y + new_height - 1,
                                        new_width - 1:]

            if self.with_face_detection:
                # Zero out locations that faces are cut
                filter_line *= face_zero_vector

            # Find the max.
            max_loc = np.argmax(filter_line)
            # If all filter_line is zero, for example, too many faces, can't
            # avoiding cut face, then we just center the cut. Otherwise, we
            # maximize the saliency overlap.
            if filter_line[max_loc] > 0:
                new_x = max_loc

        return (new_x, new_y, new_width, new_height)


    def crop(self, h, w):
        '''Using a sliding window to decide what's the optimal crop region.
        The sliding window avoids text at the bottom of the image, and avoids
        cropping faces, maximizes the saliency.
        '''
        # tic()
        height = self.image.shape[0]
        width = self.image.shape[1]

        saliency_map = np.zeros((height, width))
        if self.with_saliency:
            saliency_map = self.get_saliency_map()

        if self.with_face_detection:
            saliency_map += self.face_saliency_boost() * 0.2
        
        # Crop the image area to maximize saliency and avoid cropping faces.
        (new_x, new_y, new_width, new_height) = \
            self.saliency_face_crop(saliency_map, h, w)

        # Get the image with text cropped
        is_text_cut = False
        # "Main" is referring to the default face saliency cropping. If it
        # doesn't crop text, we keep it as is. If it contains text boxes, then
        # we crop the text area and rerun the saliency_face_crop.
        has_main_crop_text = False
        if self.with_text_detection:
            boxes = self.get_text_boxes()
            if len(boxes) > 0:
                bottom = height * 0.7
                upper_text_y_array = []
                for box in boxes:
                    if box[1] < bottom:
                        continue
                    upper_text_y_array.append(box[1])
                    # check to see the cropped image area overlaps with text.
                    if (box[0] > new_x and box[0] < new_x + new_width - 1) or \
                       (box[0] + box[2] - 1 > new_x and \
                        box[0] + box[2] - 1< new_x + new_width - 1):
                       has_main_crop_text = True

                # leave 3 pixels for cushion.
                if  upper_text_y_array:
                    is_text_cut = True
                    upper_text_y = min(upper_text_y_array) - 3
                    text_cropped_im = self.image[0:upper_text_y, :]

        if is_text_cut and has_main_crop_text:
            saliency_map_with_text_cut = saliency_map[0:upper_text_y, :]
            (new_x, new_y, new_width, new_height) = \
                self.saliency_face_crop(saliency_map_with_text_cut, h, w)
        # Using tic, toc to measure the performance.
        # toc()
        return (new_x, new_y, new_width, new_height)


    def crop_and_resize(self, h, w):
        ''' Crop and resize to the new size, based on face, saliency and text.
        Saliency crop is biased to the center of the image.
        '''

        # Check if the aspect ratio is the same as the original.
        (height, width, elem) = self.image.shape
        neutral_width = float(width) * h / height
        is_same_ratio = False
        if abs(neutral_width - w) <= 5.0:
            is_same_ratio = True

        text_boxes = []
        if self.no_text_crop_same_aspect_ratio and is_same_ratio:
            return  cv2.resize(cropped_im, (w, h))
        
        if self.with_text_detection:
            text_boxes = self.get_text_boxes()

        if len(text_boxes) == 0 and is_same_ratio:
            return cv2.resize(self.image, (w, h))

        (new_x, new_y, new_width, new_height) = self.crop(w, h)
        cropped_im = self.image[new_y:new_y+new_height,
                                new_x:new_x+new_width]

        resized_im = cv2.resize(cropped_im, (w, h))
        return resized_im
