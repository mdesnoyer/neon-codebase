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

class FindAndParseFaces(object):
    def __init__(self, predictor):
        self.detector = dlib.get_frontal_face_detector()
        self.predictor = predictor
        self._faceDets = []
        self._facePoints = []
        self._image = None

    @staticmethod
    def get_gray(image):
        # returns the grayscale version of an image
        if len(image.shape) == 3 and image.shape[2] > 1:
            image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        return image

    def _check_valid(self, face=None, comp=None):
        '''
        Generally checks to see if a request is valid
        '''
        if self._image == None:
            raise ParseStateError(0)
        if not len(self._faceDets):
            return False
        if face != None:
            if type(face) != int:
                raise ValueError("Face index must be an integer")
            if face >= len(self._faceDets):
                raise ParseStateError(1) 
            if face < 0:
                raise ParseStateError(1)
        if comp != None:
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
        return self._image[left:left+width, top:top+height]

    def ingest(self, image):
        image = FindAndParseFaces.get_gray(image)
        self._image = image
        self._faceDets = self.detector(image)
        self._facePoints = []
        for f in self._faceDets:
            self._facePoints.append(self.predictor(image, f)) 
        print '%i faces detected'%(len(self._faceDets))

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