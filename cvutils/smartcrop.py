import os.path
import sys
import math
import itertools
import cv2
import numpy as np
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
from model import TextDetectionPy
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

def normalize_range(src, begin=0, end=255):
    dst = np.zeros((len(src), len(src[0])))
    amin, amax = np.amin(src), np.amax(src)
    for y, x in itertools.product(xrange(len(src)), xrange(len(src[0]))):
        if amin != amax:
            dst[y][x] = (src[y][x] - amin) * (end - begin) / (amax - amin) + begin
        else:
            dst[y][x] = (end + begin) / 2
    return dst

def normalize(src):
    src = normalize_range(src, 0., 1.)
    amax = np.amax(src)
    maxs = []

    for y in xrange(1, len(src) - 1):
        for x in xrange(1, len(src[0]) - 1):
            val = src[y][x]
            if val == amax:
                continue
            if val > src[y - 1][x] and val > src[y + 1][x] and \
               val > src[y][x - 1] and val > src[y][x + 1]:
                maxs.append(val)

    if len(maxs) != 0:
        src *= math.pow(amax - (np.sum(maxs) / np.float64(len(maxs))), 2.)

    return src

class GaussianPyramid(object):
    def __init__(self, src):
        # maps calculated using Gaussian Pyramid for intensity
        # colors and orientations.
        self.maps = self._make_gaussian_pyramid(src)

    def _make_gaussian_pyramid(self, src):
        ''' gaussian pyramid | 0 ~ 8(1/256) . not use 0 and 1.
        Calcuate saliency map using Gaussian pyramid.
        Using intensity, colors and orientations.
        '''
        maps = {'intensity': [],
                'colors': {'b': [], 'g': [], 'r': [], 'y': []},
                'orientations': {'0': [], '45': [], '90': [], '135': []}}
        amax = np.amax(src)
        b, g, r = cv2.split(src)
        for x in xrange(1, 9):
            b, g, r = map(cv2.pyrDown, [b, g, r])
            if x < 2:
                continue
            buf_its = np.zeros(b.shape)
            buf_colors = map(lambda _: np.zeros(b.shape), range(4))  # b, g, r, y
            for y, x in itertools.product(xrange(len(b)), xrange(len(b[0]))):
                buf_its[y][x] = self._get_intensity(b[y][x], g[y][x], r[y][x])
                buf_colors[0][y][x], \
                buf_colors[1][y][x], \
                buf_colors[2][y][x], \
                buf_colors[3][y][x] = \
                    self._get_colors(b[y][x], 
                                      g[y][x],
                                      r[y][x],
                                      buf_its[y][x],
                                      amax)
            maps['intensity'].append(buf_its)
            for (color, index) in zip(sorted(maps['colors'].keys()), xrange(4)):
                maps['colors'][color].append(buf_colors[index])
            for (orientation, index) in zip(sorted(maps['orientations'].keys()),
                                            xrange(4)):
                maps['orientations'][orientation].append(
                    self._conv_gabor(buf_its, np.pi * index / 4))
        return maps

    def _get_intensity(self, b, g, r):
        return (np.float64(b) + np.float64(g) + np.float64(r)) / 3.

    def _get_colors(self, b, g, r, i, amax):
        ''' Get the color channels of blue, green, red and yellow hues.
        '''
        b, g, r = map(lambda x: np.float64(x) if (x > 0.1 * amax) else 0., [b, g, r])
        nb, ng, nr = map(lambda x, y, z: max(x - (y + z) / 2., 0.), [b, g, r], [r, r, g], [g, b, b])
        ny = max(((r + g) / 2. - math.fabs(r - g) / 2. - b), 0.)

        if i != 0.0:
            return map(lambda x: x / np.float64(i), [nb, ng, nr, ny])
        else:
            return nb, ng, nr, ny

    def _conv_gabor(self, src, theta):
        kernel = cv2.getGaborKernel((8, 8), 4, theta, 8, 1)
        return cv2.filter2D(src, cv2.CV_32F, kernel)


class FeatureMap(object):
    def __init__(self, srcs):
        self.maps = self._make_feature_map(srcs)

    def _make_feature_map(self, srcs):
        ''' scale index for center-surround calculation | (center, surround)
        index of 0 ~ 6 is meaned 2 ~ 8 in thesis (Ich)
        '''
        cs_index = ((0, 3), (0, 4), (1, 4), (1, 5), (2, 5), (2, 6))
        maps = {'intensity': [],
                'colors': {'bg': [], 'ry': []},
                'orientations': {'0': [], '45': [], '90': [], '135': []}}

        for c, s in cs_index:
            maps['intensity'].append(self._scale_diff(srcs['intensity'][c],
                                     srcs['intensity'][s]))
            for key in maps['orientations'].keys():
                maps['orientations'][key].append(
                self._scale_diff(srcs['orientations'][key][c], srcs['orientations'][key][s]))
            for key in maps['colors'].keys():
                maps['colors'][key].append(self._scale_color_diff(
                    (srcs['colors'][key[0]][c], srcs['colors'][key[0]][s]),
                    (srcs['colors'][key[1]][c], srcs['colors'][key[1]][s])
                ))
        return maps

    def _scale_diff(self, c, s):
        c_size = tuple(reversed(c.shape))
        return cv2.absdiff(c, cv2.resize(s, c_size, None, 0, 0, cv2.INTER_NEAREST))

    def _scale_color_diff(self, (c1, s1), (c2, s2)):
        c_size = tuple(reversed(c1.shape))
        return cv2.absdiff(c1 - c2, cv2.resize(s2 - s1, c_size, None, 0, 0,
                                               cv2.INTER_NEAREST))


class ConspicuityMap(object):
    def __init__(self, srcs):
        self.maps = self._make_conspicuity_map(srcs)

    def _make_conspicuity_map(self, srcs):
        ''' Combine feature map into a single conspicuity map.
        '''
        intensity = self._scale_add(map(normalize, srcs['intensity']))
        for key in srcs['colors'].keys():
            srcs['colors'][key] = map(normalize, srcs['colors'][key])
        color = self._scale_add([srcs['colors']['bg'][x] + srcs['colors']['ry'][x] \
                for x in xrange(len(srcs['colors']['bg']))])
        orientation = np.zeros(intensity.shape)
        for key in srcs['orientations'].keys():
            orientation += self._scale_add(map(normalize, srcs['orientations'][key]))
        return {'intensity': intensity,
                'color': color,
                'orientation': orientation}

    def _scale_add(self, srcs):
        buf = np.zeros(srcs[0].shape)
        for x in srcs:
            buf += cv2.resize(x, tuple(reversed(buf.shape)))
        return buf


class SaliencyMap(object):
    def __init__(self, src):
        self.gp = GaussianPyramid(src)
        self.fm = FeatureMap(self.gp.maps)
        self.cm = ConspicuityMap(self.fm.maps)
        self.map = cv2.resize(self._make_saliency_map(self.cm.maps),
                              tuple(reversed(src.shape[0:2])))

    def _make_saliency_map(self, srcs):
        srcs = map(normalize, [srcs[key] for key in srcs.keys()])
        return srcs[0] / 3. + srcs[1] / 3. + srcs[2] / 3.

class ImageSignatureSaliency(object):
    def __init__(self, src, map_size = 640):
        # self.lab_im = cv2.cvtColor(src, cv2.COLOR_BGR2LAB)
        # l_channel, a_channel, b_channel = cv2.split(self.lab_im)
        # Resize the image to the longest edge 64.
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

    def get_resized_im(self):
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
        self.front_face_cascade = cv2.CascadeClassifier()
        self.profile_face_cascade = cv2.CascadeClassifier()
        self.front_face_cascade.load(self.haarFileFront)
        self.profile_face_cascade.load(self.haarFileProfile)
        self.haarParams = {'minNeighbors': 8, 'minSize': (50, 50), 'scaleFactor': 1.1}

    @classmethod
    def get_cropper(cls):
        cls._instance_ = cls._instance_ or SmartCrop()
        return cls._instance_

    def generate_saliency_map(self, im):
        (height, width, elem) = im.shape
        half_im = cv2.resize(im, (width / 4, height / 4))
        half_sm = SaliencyMap(half_im)
        full_sm = cv2.resize(half_sm.map, (width, height))
        return full_sm


    def detect_face(self, im):
        front_faces = self.front_face_cascade.detectMultiScale(im, **self.haarParams)
        profile_faces = self.profile_face_cascade.detectMultiScale(im, **self.haarParams)
        if len(front_faces) == 0:
            return profile_faces
        elif len(profile_faces) == 0:
            return front_faces
        else:
            faces = np.append(front_faces, profile_faces, axis=0)
            return faces

    def text_crop(self, im, left, right):
        # Downsize the image first. Make the longest edge to be 360 pixels.
        ratio = max(im.shape[0]/480.0, im.shape[1]/480.0)
        im_resized = cv2.resize(im, (int(im.shape[1]/ratio),
                                     int(im.shape[0]/ratio)))
        boxes, mask = cv2.text.textDetect(im_resized,
            '/home/wiley/src/opencv_contrib/modules/text/samples/trained_classifierNM1.xml',
            '/home/wiley/src/opencv_contrib/modules/text/samples/trained_classifierNM2.xml',
            16,0.00015,0.003,0.8,True,0.5, 0.9)
        # (height, width, elem) = im.shape
        # text_image = TextDetectionPy.TextDetection(im)
        # Get the bottom 1/3 of the image.
        # Then find where the line separate the text part of the image, but
        # the line is not going to be bigger than the bottom part of the image.
        # text_contours, hierarchy = cv2.findContours(text_image,
                                                     # cv2.RETR_LIST,
                                                     # cv2.CHAIN_APPROX_TC89_L1)
        bottom = im.shape[1] * 3 / 4

        top_height_array = []
        for box in boxes:
            box = box * ratio
            if box[1] < bottom:
                continue
            l = box[0]
            r = box[0] + box[2] - 1
            if (l > left and l < right) or (r > left and r < right):
                top_height_array.append[box[1]]
            top_height_array.append(box[1])
        # leave 3 pixels for cushion.
        if not top_height_array:
            return im
        top_height = min(top_height_array) - 3
        cropped_im = im[0 : top_height, 0 : width]
        return cropped_im


    def crop(self, src, w, h):
        ''' Find the cropped area maximizes the summation of the saliency
        value
        '''
        saliency_threshold = 50
        im = self.text_crop(src, src.shape[1]-w/2, src.shape[1]+w/2)

        (height, width, elem) = im.shape

        saliency = ImageSignatureSaliency(im)
        saliency_map = saliency.get_saliency_map()
        faces = self.detect_face(im)

        # cv2.imshow('saliency', saliency_map)
        # cv2.waitKey(0)
        # cv2.destroyAllWindows()
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

        cropped_im = src[new_x:new_x+new_width, new_y:new_y+new_height]
        resized_im = cv2.resize(cropped_im, (w, h))
        return resized_im
