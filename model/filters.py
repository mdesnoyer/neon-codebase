'''Filters for known bad frames.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import cv2
from glob import glob
import logging
import numpy as np
import os
import pickle
import scipy
import scipy.ndimage.filters
from sklearn import svm 
from sklearn.externals import joblib
import tempfile 
import utils.obj
from utils import pycvutils
import dlib
from score_eyes import ScoreEyes
from parse_faces import FindAndParseFaces
import cPickle

_log = logging.getLogger(__name__)

class Filter(object):
    '''Abstract class to filter out bad images.'''
    def __init__(self, max_height=None):
        self.__version__ = 2
        self.max_height = max_height
    
    def __str__(self):
        return utils.obj.full_object_str(self)
    

    def accept(self, image, frameno=None, video=None):
        '''Returns True if the image passes the filter.

        Input:
        image - The image to filter. In OpenCV BGR format
        franemno - Frame number in the video for this image.
                   None if it not a video
        video - The OpenCV video. None if it is not a video

        Returns: True of the image passed the filter, False otherwise
        '''
        return self.accept_score(self._resize_image(image), frameno, video)[0]

    def _resize_image(self, image):
        '''Resizes the image according to max_height.'''
        if ((self.max_height is not None) and 
            (image.shape[0] > self.max_height)):
            new_size = (int(2*round(float(image.shape[1]) * 
                                    self.max_height / image.shape[0] /2)),
                                    self.max_height)
            return cv2.resize(image, new_size)
        return image

    def _accept_impl(self, image, frameno=None, video=None):
        '''Implementation of accept that should be defined by inheritors.'''
        raise NotImplementedError()

    def short_description(self):
        '''Returns a short description of what the filter did on the last
        accept() call.

        '''
        raise NotImplementedError()

    def score(self, image):
        '''Returns the score of an image related to the filter.'''
        raise NotImplementedError()

    def accept_score(self, image, frameno=None, video=None):
        '''Returns (accept, score) simultaneously'''
        raise NotImplementedError()

    def __setstate__(self, state):
        if not 'max_height' in state:
            state['max_height'] = None
        self.__dict__ = state

    def restore_additional_data(self, filename):
        '''
        This function is to be defined in individual classes, and restores 
        additional data that is required by a given filter but is static and
        not easily filtered. 

        When implemented in actual filter classes, this function accepts the 
        filename of the pickled model that is being loaded and uses that 
        filename to locate the appropriate data. This is based on the 
        presumption that other (non-model) pickled files are stored in a 
        consistent way in the model_data directory. 
        '''
        pass 

class LocalFilter(object):
    '''
    Abstract local filter. In contrast with other filters, this one
    relies on the output of a feature generator. Therefore, it accepts
    a list or 1D numpy array of feature and returns an equal-sized list
    of booleans indicating whether or not the frame should be filtered.

    FALSE -> Filter frame.
    TRUE  -> Do not filter frame.
    '''
    def __init__(self):
        self.__version__ = 1
        self.feature = None

    def _ensure_np(self, feat_vec):
        '''Makes sure the input vec is a numpy-style array'''
        if not type(feat_vec).__module__ == np.__name__:
            feat_vec = np.array(feat_vec)
        return feat_vec

    def filter(self, feat_vec):
        '''
        Inputs:
            A 1D numpy array or list of features (floats or ints)
        Returns:
            a list of type boolean with size
            equal to that of the feat_vec.
        '''
        feat_vec = self._ensure_np(feat_vec)
        return self._filter_impl(feat_vec)

    def _filter_impl(self, feat_vec):
        raise NotImplementedError()

class VideoFilter(Filter):
    '''Abstract video filter'''
    def __init__(self, max_height=None):
        super(VideoFilter, self).__init__(max_height)

    def accept(self, image, frameno=None, video=None):
        if frameno is None or video is None:
            # This isn't a video, so the image passes automatically
            return True
        return self._accept_impl(self._resize_image(image), frameno, video)

class ThreshFilt(LocalFilter):
    '''
    Removes frames for whom the value of the feature is too low. Accepts
    a function as the thresh, so that it may be calculated dynamically. The
    function must accept no arguments and return only a single value.
    '''
    def __init__(self, thresh, feature='pixvar'):
        super(ThreshFilt, self).__init__()
        self._thresh = thresh
        self.feature = feature

    @property
    def thresh(self):
        try:
            return self._thresh()
        except TypeError:
            return self._thresh

    def _filter_impl(self, feat_vec):
        return feat_vec > self.thresh

    def short_description(self):
        return 'thresholdfilter'

class SceneChangeFilter(LocalFilter):
    '''
    Removes frames that are near scene changes. 
    '''
    def __init__(self, mean_mult=2., std_mult=1.5, 
                 min_thresh=None, max_thresh=None):
        '''
        Scene Change filtering. 
        Parameters:
            mean_mult : defines thresh1, see below
            std_mult  : defines thresh2, see below
            min_thresh : images with SAD < min_thresh are never filtered
            max_thresh : images with SAD > max_thresh are always filtered

        Note: min_thresh and max_thresh may be calculated dynamically, similar
        to ThreshFilt, if passed as a function that takes no parameter. 

        Note: There is an edge case in which filtering is performed on only
        one image. This can occur if the previous filters reject all-but-one
        image, in which case the SAD feature generator cannot calculate SAD,
        and hence the entire interval is thrown out (as it's likely to be bad
        anyway). 

        Constructs parameters based on mean and std.

        thresh1 = mean(SAD) * mean_mult
        thresh2 = std(SAD) * std_mult

        Images are filtered if:
        ((SAD_i > thresh1) AND (SAD_i > thresh2) AND SAD_i > min_thresh)
        OR 
        (SAD_i > max_thresh)

        If any input parameters are None, then they do not affect the
        calculation. 

        '''
        super(SceneChangeFilter, self).__init__()
        self.mean_mult = mean_mult
        self.std_mult = std_mult
        self._min_thresh = min_thresh
        self._max_thresh = max_thresh
        self.feature = 'sad'

    def mean_thresh(self, feat_vec):
        return np.mean(feat_vec) * self.mean_mult 

    def std_thresh(self, feat_vec):
        return np.std(feat_vec) * self.std_mult + np.mean(feat_vec)

    @property
    def min_thresh(self):
        try:
            return self._min_thresh()
        except TypeError:
            return self._min_thresh

    @property
    def max_thresh(self):
        try:
            return self._max_thresh()
        except TypeError:
            return self._max_thresh

    def _filter_impl(self, feat_vec):
        if len(feat_vec) < 2:
            # nothing can be determined. Throw the whole thing out.
            return np.array([False])
        crit = np.ones(feat_vec.shape, dtype=bool)
        if self.mean_mult is not None:
            crit = np.logical_and(crit, feat_vec < self.mean_thresh(feat_vec))
        if self.std_mult is not None:
            crit = np.logical_and(crit, feat_vec < self.std_thresh(feat_vec))
        if self.min_thresh is not None:
            crit = np.logical_or(crit, feat_vec < self.min_thresh)
        if self.max_thresh is not None:
            crit = np.logical_and(crit, feat_vec < self.max_thresh)
        return crit

    def short_description(self):
        return 'scenechange'

class FaceFilter(LocalFilter):
    '''
    Removes frames that have less faces than other frames.
    '''
    def __init__(self):
        super(FaceFilter, self).__init__()
        self.feature = 'faces'

    def _filter_impl(self, feat_vec):
        return feat_vec == np.max(feat_vec)

    def short_description(self):
        return 'faces'

class EyeFilter(LocalFilter):
    '''
    Removes frames that definitely have closed eyes (i.e.,
    the eye scores do not cross the separating hyperplane
    of the classifier)
    '''
    def __init__(self):
        super(EyeFilter, self).__init__()
        self.feature = 'eyes'

    def _filter_impl(self, feat_vec):
        return feat_vec >= 0

    def short_description(self):
        return 'eyes'

class CascadeFilter(Filter):
    '''A sequence of filters where if one cuts out the image, it fails.'''
    def __init__(self, filters, max_height=None):
        super(CascadeFilter, self).__init__(max_height)
        self.filters = filters
        self.last_failed = None # The filter that failed the last image

    def __str__(self):
        field_strs = ['last_failed: %s' % self.last_failed]
        field_strs.append('filters: %s' % [str(x) for x in self.filters])
        return '<%s> {%s}' % (self.__class__.__name__, ','.join(field_strs))

    def _accept_impl(self, image, frameno, video):
        self.last_failed = None
        for filt in self.filters:
            if not filt.accept(image, frameno, video):
                self.last_failed = filt
                return False

        return True

    def short_description(self):
        if self.last_failed is None:
            return ''
        return self.last_failed.short_description()

    def restore_additional_data(self, filename):
        for filt in self.filters:
            filt.restore_additional_data(filename)

class CachedCascadeFilter(CascadeFilter):
    '''Wraps CascadeFilter, but stores the image values such that
    they can be accessed later. If initialized with force, it will
    always run through all the filters.'''
    def __init__(self, filters, max_height=None, force=False):
        super(CachedCascadeFilter, self).__init__(max_height)
        self.last_scores = None
        
    def _accept_impl(self, image, frameno, video):
        self.last_failed = None
        self.last_scores = []
        for filt in self.filters:
            accepted, score = filt.accept_score(image, frameno, video)
            self.last_scores.append((accepted, score))
            if (not accepted) and (self.last_failed is None):
                self.last_failed = filt
                if not force:
                    return False

        return True

    def short_description(self):
        if self.last_failed is None:
            return ''
        return self.last_failed.short_description()

    def restore_additional_data(self, filename):
        for filt in self.filters:
            filt.restore_additional_data(filename)

class UniformColorFilter(Filter):
    '''Filters an image that is too uniform a color.'''

    def __init__(self, dthresh=30, frac_pixels=0.95, max_height=480):
        '''Create the filter with some parameters.

        dthresh - Distance (L1) in RGB space for a pixel to be considered the
                  same color
        frac_pixels - Fraction of pixels that must be the same color to reject
        '''
        super(UniformColorFilter, self).__init__(max_height)
        self.dthresh = dthresh
        self.frac_pixels = frac_pixels

    def _accept_impl(self, image, frameno, video):
        '''Returns True if we should filter the image.

        image - the image to check
        '''
        return self.score(image) < self.frac_pixels

    def score(self, image):
        n_image = np.array(image, dtype=np.float)

        npixels = float(n_image.shape[0]*n_image.shape[1])

        # Find the average color of the image
        avg_color = np.sum(np.sum(n_image, 1),0) / npixels

        # Calculate the L1 distance of each pixel to the average color
        dist = np.abs(n_image -
                      np.squeeze(np.tile(avg_color, (n_image.shape[0],
                                                     n_image.shape[1],
                                                     1))))
        if len(dist.shape) > 2:
            dist = np.sum(dist, 2)
        else:
            dist *= 3; # There are three colors so the distance needs
                       # to increase

        # Calculate the fraction of pixels more than dthresh from the average
        frac_same = np.count_nonzero(dist < self.dthresh) / npixels

        return frac_same

    def accept_score(self, image, frameno=None, video=None):
        score = self.score(image)
        accepted = score < self.frac_pixels
        return (accepted, score)

    def short_description(self):
        return 'ucolor'

class BlurryFilter(Filter):
    '''Filters on an image that is too blurry.'''

    def __init__(self, blur_threshold = 60, percentile=0.99, max_height=480):
        super(BlurryFilter, self).__init__(max_height)
        self.thresh = blur_threshold
        self.percentile = percentile

    def _accept_impl(self, image, frameno, video):
        return self.score(image) > self.thresh

    def score(self, image):
        scale_factor = 512.0 / image.shape[1]
        new_size = (512, int(round(image.shape[0] * scale_factor)))
        thumb = cv2.resize(cv2.cvtColor(image, cv2.COLOR_BGR2GRAY),
                           new_size)

        # Grab the middle 3/4 of the image to avoid most overlays
        border_size = (new_size[0]/8, new_size[1]/8)
        thumb = thumb[border_size[1] : (new_size[1] - border_size[1]),
                      border_size[0] : (new_size[0] - border_size[0])]

        # Calculat the laplacian
        laplace_im = cv2.Laplacian(thumb, cv2.CV_32F)

        # Do a robust max
        sort_im = np.sort(laplace_im, axis=None)
        return sort_im[int(self.percentile * len(sort_im))-1]

    def accept_score(self, image, frameno=None, video=None):
        score = self.score(image)
        accepted = score > self.thresh
        return (accepted, score)

    def short_description(self):
        return 'blur'

class InterlaceFilter(Filter):
    '''Filters on a large amount of horizontal edge energy from interlacing.'''

    def __init__(self, count_thresh=0.1, diff_thresh=9):
        super(InterlaceFilter, self).__init__()
        self.count_thresh = count_thresh
        self.diff_thresh = diff_thresh

    def _accept_impl(self, image, frameno, video):
        return self.score(image) < self.count_thresh

    def score(self, image):
        gimage = np.array(cv2.cvtColor(image, cv2.COLOR_BGR2GRAY),
                          dtype=np.float)
        if gimage.shape[0] % 2 == 1:
            gimage = gimage[1:,:]

        arows = gimage[0:-2:2,:]
        brows = gimage[1:-1:2,:]
        crows = gimage[2::2,:]

        ccomb = np.abs(crows-arows)
        bcomb = np.abs(brows-arows)
        is_comb = np.logical_and(ccomb < 10, bcomb > 15)

        afield = scipy.ndimage.filters.convolve1d(gimage[0::2,:],
                                                  [1./3, 1./3, 1./3],
                                                  axis=0)
        bfield = scipy.ndimage.filters.convolve1d(gimage[1::2,:], [1./2, 1./2],
                                                  axis=0)
        is_comb = np.abs(afield[1:] - bfield[:-1]) > self.diff_thresh
        

        return (float(np.count_nonzero(is_comb)) /
                (image.shape[0] * image.shape[1]))

    def accept_score(self, image, frameno=None, video=None):
        score = self.score(image)
        accepted = score < self.count_thresh
        return (accepted, score)

    def short_description(self):
        return 'interlace'

class DeltaStdDevFilter(VideoFilter):
    def __init__(self, upper_thresh = 0.76, lower_thresh=-0.74):
        super(DeltaStdDevFilter,self).__init__()
        self.upper = upper_thresh
        self.lower = lower_thresh
        self.d2_std = None
  
    def _accept_impl(self, image, frameno, video):
        return self._hop_accept_impl(image, frameno, video)

    def _hop_accept_impl(self, image, frameno, video):

        std_devs = [0.,np.std(image),0.]
       
        last_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)
 
        # get one after frameno
        sucess, cur_frame = pycvutils.seek_video(video, frameno+1)
        if not sucess:
            # We couldn't grab the frame, so let the image pass this filter
            return True
        moreData, frame = video.read() 
        std_devs[2] = np.std(frame) if moreData else 0.
        
        # get one before frameno
        sucess, cur_frame = pycvutils.seek_video(video, frameno-1,
                                                       cur_frame=cur_frame)
        if not sucess:
            # We couldn't grab the frame, so let the image pass this filter
            return True
        moreData,frame = video.read() 
        std_devs[0] = np.std(frame) if moreData else 0.

        sucess, cur_frame = pycvutils.seek_video(video, last_frame,
                                                       cur_frame=cur_frame)
   
        # compute stats
        self.d2_std = np.diff(std_devs,n=2)[0] # technically it should be divided by
					  # dt^2, but that will be a positive 
					  # constant so what odds, right?
        should_accept = self.d2_std >= self.lower and self.d2_std <= self.upper

        return should_accept  

    def accept_score(self, image, frameno, video):
        accepted = self._accept_impl(image, frameno, video)
        score = self.d2_std
        return (accepted, score)
    
    def score(self):
        return self.d2_std

    def short_description(self):
        return 'scene_chng'

class CrossFadeFilter(VideoFilter):
    '''Filter out the cross fade by seeing if the current frame is a linear
    combination of frames before and after this one.'''
    def __init__(self, residual_thresh=18.0, stddev_thresh=4.0,
                 dframe=2, max_height=480):
        '''
        Inputs:
        residual_thresh - Threshold for the average residual of a pixel. 
                          If it is above this number we accept the frame 
                          because it isn't a combination of the two frames 
                          surround it.
        stddev_thresh - Threshold for the difference in stddev. 
                        If the scene is changing quickly the delta stddev
                        will be higher.
        dframe - The number of frames before and after the image to look at
        max_height - The maximum height of the frame to resize to
        '''
        super(CrossFadeFilter, self).__init__(max_height=max_height)
        self.residual_thresh = residual_thresh
        self.stddev_thresh = stddev_thresh
        self.dframe = dframe

    def _accept_impl(self, image, frameno, video):
        residual, delta_stddev = self.score(image, frameno, video)
        return (delta_stddev < self.stddev_thresh or 
                residual > self.residual_thresh)
    
    def score(self, image, frameno, video):
        '''Calculate the score for the frame.

        This is the average residual across the frame after solving
        for the linear combination of I = aI(-) + bI(+)
        ''' 
        last_frame = video.get(cv2.cv.CV_CAP_PROP_POS_FRAMES)

        # Get the frame in the future of this one
        sucess, cur_frame = pycvutils.seek_video(video,
                                                       frameno + self.dframe)
        if not sucess:
            return float('inf'), float('inf')
        sucess, future_frame = video.read()
        if not sucess:
            return float('inf'), float('inf')
        future_frame = self._resize_image(future_frame)

        # Get the frame in the past
        sucess, cur_frame = pycvutils.seek_video(video,
                                                       frameno - self.dframe)
        if not sucess:
            # We couldn't grab the frame, so let the image pass this filter
            return float('inf'), float('inf')
        sucess, past_frame = video.read()
        if not sucess:
            return float('inf'), float('inf')
        past_frame = self._resize_image(past_frame)

        sucess, cur_frame = pycvutils.seek_video(video, last_frame)

        A = np.transpose(np.vstack((np.ravel(past_frame),
                                    np.ravel(future_frame))))
        x, residuals, rank, s = np.linalg.lstsq(A, np.ravel(image))
        if rank < 2:
            return (0.0, 0.0)

        std_devs = [np.std(past_frame), np.std(future_frame)]
        
        return (np.sqrt(residuals[0] / image.size),
                np.abs(std_devs[1] - std_devs[0]))

    def accept_score(self, image, frameno, video):
        residual, delta_stddev = self.score(image, frameno, video)
        accepted = ((delta_stddev < self.stddev_thresh) or 
                    (residual > self.residual_thresh))
        score = [residual, delta_stddev]
        return (accepted, score)

    def short_description(self):
        return 'cross_fade'

class ClosedEyeDetector(Filter):
    def __init__(self, predictor_path, classifier_path, scaler_path, max_height=640):
        super(ClosedEyeDetector, self).__init__(max_height=max_height)
        self.detector = dlib.get_frontal_face_detector()
        self.predictor = dlib.shape_predictor(predictor_path)
        with open(classifier_path) as f:
            self.classifier = cPickle.load(f)
        with open(scaler_path) as f:
            self.scaler = cPickle.load(f)
        self.faceParse = FindAndParseFaces(self.predictor)
        self.scoreEyes = ScoreEyes(self.classifier, self.scaler)
        self.n_people_last = None

    def score(self, image):
        '''
        Computes the scores for an image
        '''
        accepted, scores = self.accept_score(image)
        return scores

    def _accept_impl(self, image, frameno=None, video=None):
        accepted, scores = self.accept_score(image, frameno, video)
        return accepted

    def accept_score(self, image, frameno=None, video=None):
        self._resize_image(image)
        self.faceParse.ingest(image)
        self.n_people_last = self.faceParse.get_N_faces()
        eyes = self.faceParse.get_all(['l eye', 'r eye'])
        classif, scores = self.scoreEyes.classifyScore(eyes)
        if any(np.array(classif) == 0):
            accepted = False
        else:
            accepted = True
        return accepted, scores

    def restore_additional_data(self, filename):
        pass

    def short_description(self):
        return 'closed_eyes'
