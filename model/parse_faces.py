'''
Implements a class that handles parsing of images
and manages the extraction of facial components.

It needs the detector (which finds the face) and
the predictor (which locates facial components) to
be provided when it is initialized. 
'''
import numpy as np
import cv2
import os
import dlib
from utils import pycvutils

comp_dict = {}
comp_dict['face'] = range(17)
comp_dict['r eyebrow'] = range(17, 22)
comp_dict['l eyebrow'] = range(22, 27)
comp_dict['nose'] = range(27, 36)
comp_dict['r eye'] = range(36, 42)
comp_dict['l eye'] = range(42, 48)
comp_dict['mouth'] = range(48, 68)

class ParseStateError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        if self.value == 0:
            return "No image analyzed"
        if self.value == 1:
            return "Invalid face index requested"
        if self.value == 2:
            return "Invalid component requested"

class MultiStageFaceParser(object):
    '''
    Wraps FindAndParseFaces, but allows three-stage evaluation.
    This is necessary because the detection of faces has been
    split from the scoring of closed eyes. Since the segmenting
    faces requires faces to be detected, and scoring eyes requires
    the faces be segmented, this allows us to make multiple evaluations
    within-step and across-images without having to repeat either
    detection or segmentation steps, which saves us time.

    This is designed to work just like any other feature extractor--
    it accepts an image on each call--however, it keeps the MD5 of
    those images cached so that it can look for them again.

    One potentially problematic area is if the images are preprocessed
    differently in the face detection vs. the closed eye detection
    steps. This assumes they will stay the same! To this end, it has as
    an attribute its own preprocessing object.
    '''

    def __init__(self, predictor, max_height=520):
        self.detector = dlib.get_frontal_face_detector()
        self.fParse = FindAndParseFaces(predictor, self.detector)
        self.image_data = {}
        self.max_height = max_height

    def reset(self):
        self.image_data = {}

    def get_faces(self, image):
        ihash = hash(image.tostring())
        if self.image_data.has_key(ihash):
            return len(self.image_data[ihash][0])
        det = self.fParse._SEQfindFaces(image)
        self.image_data[ihash] = [det]
        return len(det)

    def get_seg(self, image):
        '''
        Performs segmentation, but does not return anything
        (at the moment). This must be queried in the order
        in which the images were originally submitted to the
        detector, but can otherwise tolerate dropped frames.
        '''
        # find the image index
        ihash = hash(image.tostring())
        if self.image_data.has_key(ihash):
            if len(self.image_data[ihash]) > 1:
                return self.image_data[ihash][1]
        else:
            self.get_faces(image)
        det = self.image_data[ihash][0]
        points = self.fParse._SEQsegFaces(image, det)
        self.image_data[ihash].append(points)

    def get_eyes(self, image):
        '''
        Obtains all the eyes for an image.
        '''
        self.get_seg(image)
        return self.fParse.get_all(['l eye', 'r eye'])

    def get_face_subimages(self, image):
        '''
        Returns all face subimages, as a list.
        '''
        ihash = hash(image.tostring())
        if not self.image_data.has_key(ihash):
            self.get_faces(image)
        faces = []
        dets = self.image_data[ihash][0]
        for det in dets:
            L, T, R, B = det.left(), det.top(), det.right(), det.bottom()
            # account for image overruns!
            T = max(0, T)
            B = min(image.shape[0], B)
            R = max(0, R)
            L = min(image.shape[1], L)
            faces.append(image[T:B,L:R])
        return faces
        
    def __getstate__(self):
        self.reset()
        self.fParse.reset()
        return self.__dict__.copy()

    def _get_prep(self):
        return pycvutils.ImagePrep(max_height=self.max_height)

class FindAndParseFaces(object):
    '''
    Detects faces, and segments them.
    '''
    def __init__(self, predictor, detector=None):
        if detector is None:
            self.detector = dlib.get_frontal_face_detector()
        else:
            self.detector = detector
        self.predictor = predictor
        self.reset()

    def _get_prep(self):
        return pycvutils.ImagePrep(convert_to_gray=True)

    def reset(self):
        self._faceDets = []
        self._facePoints = []
        self._image = []

    def _check_valid(self, face=None, comp=None):
        '''
        Generally checks to see if a request is valid
        '''
        if self._image is None:
            raise ParseStateError(0)
        if not len(self._faceDets):
            return False
        if face is not None:
            if type(face) != int:
                raise ValueError("Face index must be an integer")
            if face >= len(self._faceDets):
                raise ParseStateError(1) 
            if face < 0:
                raise ParseStateError(1)
        if comp is not None:
            if type(comp) != str:
                raise ValueError("Component key must be string")
            if not comp_dict.has_key(comp):
                raise ParseStateError(2)
        return True

    def _getSquareBB(self, points):
        '''
        Returns a square bounding box given the centroid
        and maximum x or y distance.

        points is a list of [(x, y), ...] pairs.
        '''
        points = np.array(points)
        x_dists = np.abs(points[:,0][:,None]-points[:,0][None,:])
        y_dists = np.abs(points[:,1][:,None]-points[:,1][None,:])
        mx = max(np.max(x_dists), np.max(y_dists))
        cntr_x = np.mean(points[:,0])
        cntr_y = np.mean(points[:,1])
        top = int(cntr_x - mx * 0.5)
        left = int(cntr_y - mx * 0.5)
        return (top, left, mx, mx)

    def _get_points(self, shape, comp):
        '''
        Gets the points that correspond to a given component
        '''
        xypts = []
        points = [shape.part(x) for x in range(shape.num_parts)]
        for pidx in comp_dict[comp]:
            p = points[pidx]
            xypts.append([p.x, p.y])
        return xypts

    def _extract(self, shape, comp):
        '''
        Returns a subimage containing the requested component
        '''
        points = self._get_points(shape, comp)
        top, left, height, width = self._getSquareBB(points)
        top = max(0, top)   # account for the top of the screen
        left = max(0, left)  # account for the left of the screen
        # the other directions (bottom, right) will be handled implicitly
        # by the way numpy deals with indices.
        return self._image[left:left+width, top:top+height]

    def ingest(self, image):
        prep = self._get_prep()
        image = prep(image)
        self._image = image
        self._faceDets = self.detector(image)
        self._facePoints = []
        for f in self._faceDets:
            self._facePoints.append(self.predictor(image, f))

    def _SEQfindFaces(self, image):
        '''
        Detects faces, returns detections to be used by
        MultiStageFaceParser.
        '''
        prep = self._get_prep()
        self._image = prep(image)
        return self.detector(image)

    def _SEQsegFaces(self, image, dets):
        '''
        Returns segmented face points given the image and
        the detections, to be used by MultiStageFaceParser
        '''
        self._faceDets = dets
        prep = self._get_prep()
        self._image = prep(image)
        self._facePoints = []
        for f in self._faceDets:
            self._facePoints.append(self.predictor(image, f))
        return self._facePoints

    def _SEQfinStep(self, image, dets, points):
        '''
        Restores the parser to the 'final' configuration,
        as if it had run the entire thing from end-to-end,
        allowing us to proceed with the eye scoring.
        '''
        self._faceDets = dets
        prep = self._get_prep()
        self._image = prep(image)
        self._facePoints = facePoints

    def get_comp(self, face, comp):
        '''
        Given a face index and a component key, returns the
        sub-image.
        '''
        if not self._check_valid(face, comp):
            return None
        shape = self._facePoints[face]
        return self._extract(shape, comp)

    def get_N_faces(self):
        return len(self._faceDets)

    def get_face(self, face):
        '''
        Returns the subimage that contains a face
        at a given facial idx
        '''
        if not self._check_valid(face):
            return None
        det = self._faceDets[face]
        L, T, R, B = det.left(), det.top(), det.right(), det.bottom()
        simg = self._image[T:B,L:R]
        return simg

    def get_comp_pts(self, face, comp):
        '''
        Given a face index and a component key, returns the
        points.
        '''
        if not self._check_valid(face, comp):
            return None
        shape = self._facePoints[face]
        return self._get_points(shape, comp)

    def iterate_all(self, comp):
        '''
        Given a list of components, returns a generator
        that sequentially yields all the sub images.

        If comp is not a list, it will be converted into
        one.
        '''
        if type(comp) != list:
            comp = [comp]
        for face in range(len(self._facePoints)):
            for c in comp:
                yield self.get_comp(face, c)

    def get_all(self, comp):
        '''
        Similar to iterate_all, only returns the values as
        a list.
        '''
        rcomps = []
        if type(comp) != list:
            comp = [comp]
        for face in range(len(self._facePoints)):
            for c in comp:
                rcomps.append(self.get_comp(face, c))
        return rcomps

    def __getstate__(self):
        self.reset()
        return self.__dict__.copy()
