import os.path
import sys
import math
import itertools
import cv2
import numpy as np
from lib import TextDetectionPy

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
        self.maps = self._make_gaussian_pyramid(src)

    def _make_gaussian_pyramid(self, src):
        # gaussian pyramid | 0 ~ 8(1/256) . not use 0 and 1.
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
        # scale index for center-surround calculation | (center, surround)
        # index of 0 ~ 6 is meaned 2 ~ 8 in thesis (Ich)
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

haarF = '/data/model_data/haar_cascades/haarcascade_frontalface_alt2.xml'

class SmartCrop(object):
    def __init__(self, src):
        self.haarFile = haarF
        self.face_cascade = cv2.CascadeClassifier()
        self.face_cascade.load(self.haarFile)
        self.haarParams = {'minNeighbors': 8, 'minSize': (50, 50), 'scaleFactor': 1.1}

        self.original_im = src
        (self.src_height, self.src_width, elem) = src.shape

        self.generate_saliency_map()
        self.detect_face()

        # The idea is to make the face area to be highest saliency area.
        max_saliency = np.max(self.saliency_map)
        self.new_saliency_map = self.saliency_map
        for face in self.faces:
            self.new_saliency_map[face[1]:face[1]+face[3], face[0]:face[0]+face[2]] += \
                max_saliency

        self.integral_map = cv2.integral(self.new_saliency_map)


    def generate_saliency_map(self):
        half_im = cv2.resize(self.original_im, (self.src_width / 2, self.src_height / 2))
        half_sm = SaliencyMap(half_im)
        full_sm = cv2.resize(half_sm.map, (self.src_width, self.src_height))
        self.saliency_map = full_sm


    def detect_face(self):
        self.faces = self.face_cascade.detectMultiScale(self.original_im, **self.haarParams)
        return self.faces

    def text_crop(self):
        text_image = TextDetectionPy.TextDetection(self.original_im)
        # Get the bottom 1/3 of the image.
        # Then find where the line separate the text part of the image, but
        # the line is not going to be bigger than the bottom part of the image.
        text_contours, hierarchy = cv2.findContours(text_image,
                                                     cv2.RETR_LIST,
                                                     cv2.CHAIN_APPROX_TC89_L1)
        bottom_third = self.src_height * 2 / 3

        top_height_array = []
        for points in text_contours:
            if len(points) < 2:
                continue
            height_of_points = [x[0][1] for x in points]
            if min(height_of_points) < bottom_third:
                continue
            else:
                top_height_array.append(min(height_of_points))
        # leave 3 pixels for cushion.
        if not top_height_array:
            self.cropped_im = self.original_im
            self.cropped_height, self.cropped_width = self.src_height, self.src_width
            return self.cropped_im
        top_height = min(top_height_array) - 3
        self.cropped_im = self.original_im[0 : top_height, 0 : self.src_width]
        (self.cropped_height, self.cropped_width, elem) = self.cropped_im.shape
        return self.cropped_im


    def crop(self, w, h):
        ''' Find the cropped area maximizes the summation of the saliency
        value
        '''
        self.text_crop()
        # Now cropped_im is used.

        # First is to decide which direction of the rectange is to crop.
        if float(h) / self.cropped_height > float(w) / self.cropped_width:
            # Crop horizontally
            new_width = int(self.cropped_height / float(h) * float(w))
            saliency_array = []
            for i in xrange(0, self.cropped_width - new_width + 1):
                area_saliency = self.integral_map[self.cropped_height, i+new_width] - \
                    self.integral_map[self.cropped_height, i] - \
                    self.integral_map[0, i+new_width] + \
                    self.integral_map[0, i]
                saliency_array.append(area_saliency)
            saliency_array = np.array(saliency_array)
            max_index = saliency_array.argmax()
            self.cropped_im = self.cropped_im[0:self.cropped_height, \
                                              max_index:max_index + new_width]
        else:
            # Crop vertically
            new_height = int(self.cropped_width / float(w) * float(h))
            saliency_array = []
            for i in xrange(0, self.cropped_height - new_height + 1):
                area_saliency = self.integral_map[i+new_height, self.cropped_width] - \
                    self.integral_map[i, self.cropped_width] - \
                    self.integral_map[i+new_height, 0] + \
                    self.integral_map[i, 0]
                saliency_array.append(area_saliency)
            saliency_array = np.array(saliency_array)
            max_index = saliency_array.argmax()
            self.cropped_im = self.cropped_im[max_index:max_index + new_height, \
                                              0:self.cropped_width]
    
        self.resized_im = cv2.resize(self.cropped_im, (w, h))
        return self.resized_im
