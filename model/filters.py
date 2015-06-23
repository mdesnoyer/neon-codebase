'''Filters for known bad frames.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import cv2
import logging
import numpy as np
import scipy
import scipy.ndimage.filters
from . import TextDetectionPy
import utils.obj
import utils.pycvutils
from glob import glob 
from sklearn import svm 
import tempfile 
import pickle 
from sklean.externals import joblib

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
        return self._accept_impl(self._resize_image(image), frameno, video)

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

    def __setstate__(self, state):
        if not 'max_height' in state:
            state['max_height'] = None
        self.__dict__ = state

    def restore_additional_data(self, filename):
        '''
        This function is to be defined in individual classes, and restores 
        additional data that is required by a given filter but is static and
        not easily filtered.  
        '''
        pass 

class VideoFilter(Filter):
    '''Abstract video filter'''
    def __init__(self, max_height=None):
        super(VideoFilter, self).__init__(max_height)

    def accept(self, image, frameno=None, video=None):
        if frameno is None or video is None:
            # This isn't a video, so the image passes automatically
            return True
        return self._accept_impl(self._resize_image(image), frameno, video)

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

    def short_description(self):
        return 'ucolor'

class BlurryFilter(Filter):
    '''Filters on an image that is too blurry.'''

    def __init__(self, blur_threshold = 60, percentile=0.999, max_height=480):
        super(BlurryFilter, self).__init__(max_height)
        self.thresh = blur_threshold
        self.percentile = percentile

    def _accept_impl(self, image, frameno, video):
        return self.score(image) > self.thresh

    def score(self, image):
        scale_factor = 256.0 / image.shape[1]
        new_size = (256, int(round(image.shape[0] * scale_factor)))
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

    def short_description(self):
        return 'interlace'

class TextFilter(Filter):
    '''Filters on large amounts of text in the image.'''

    def __init__(self, frac_thresh=0.03, max_height=480):
        super(TextFilter, self).__init__(max_height)
        self.frac_thresh = frac_thresh

    def _accept_impl(self, image, frameno, video):
        return self.score(image) < self.frac_thresh

    def score(self, image):

        # Cut out the bottom 20% of the image because it often has tickers
        cut_image = image[0:int(image.shape[0]*.82), :, :]
    
        text_image = TextDetectionPy.TextDetection(cut_image)
        

        return (float(np.count_nonzero(text_image)) /
                (text_image.shape[0] * text_image.shape[1]))

    def short_description(self):
        return 'text'

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
        sucess, cur_frame = utils.pycvutils.seek_video(video, frameno+1)
        if not sucess:
            # We couldn't grab the frame, so let the image pass this filter
            return True
        moreData, frame = video.read() 
        std_devs[2] = np.std(frame) if moreData else 0.
        
        # get one before frameno
        sucess, cur_frame = utils.pycvutils.seek_video(video, frameno-1,
                                                       cur_frame=cur_frame)
        if not sucess:
            # We couldn't grab the frame, so let the image pass this filter
            return True
        moreData,frame = video.read() 
        std_devs[0] = np.std(frame) if moreData else 0.

        sucess, cur_frame = utils.pycvutils.seek_video(video, last_frame,
                                                       cur_frame=cur_frame)
   
        # compute stats
        self.d2_std = np.diff(std_devs,n=2)[0] # technically it should be divided by
					  # dt^2, but that will be a positive 
					  # constant so what odds, right?
        should_accept = self.d2_std >= self.lower and self.d2_std <= self.upper

        return should_accept  
    
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
        sucess, cur_frame = utils.pycvutils.seek_video(video,
                                                       frameno + self.dframe)
        if not sucess:
            return float('inf'), float('inf')
        sucess, future_frame = video.read()
        if not sucess:
            return float('inf'), float('inf')
        future_frame = self._resize_image(future_frame)

        # Get the frame in the past
        sucess, cur_frame = utils.pycvutils.seek_video(video,
                                                       frameno - self.dframe)
        if not sucess:
            # We couldn't grab the frame, so let the image pass this filter
            return float('inf'), float('inf')
        sucess, past_frame = video.read()
        if not sucess:
            return float('inf'), float('inf')
        past_frame = self._resize_image(past_frame)

        sucess, cur_frame = utils.pycvutils.seek_video(video, last_frame)

        A = np.transpose(np.vstack((np.ravel(past_frame),
                                    np.ravel(future_frame))))
        x, residuals, rank, s = np.linalg.lstsq(A, np.ravel(image))
        if rank < 2:
            return (0.0, 0.0)

        std_devs = [np.std(past_frame), np.std(future_frame)]
        
        return (np.sqrt(residuals[0] / image.size),
                np.abs(std_devs[1] - std_devs[0]))

    def short_description(self):
        return 'cross_fade'

class ClosedEyesFilter(Filter):
    '''
    Filters out images that have too many closed faces.
    -------------------------------------------------------
    Faces are extracted by vanilla Viola-Jones cascades. They are 
    decomposed into HOG features, and then fed into an SVM that has
    been trained to recognize closed-eye or partially closed-eye 
    faces. The filter requires a haar cascade file, and pickled SVM
    file(s), as generated by sklean's joblib. 
    '''
    def __init__(self, haarFile=None, haarParams=None,
                 hogWin=128,svmPkl=None, alpha=None, maxFaces=None,
                 scoreType=2, accCrit=3, f=0.66, areaW=1, areaWs=1,
                 distWs=1, fast = True):
        '''
        Create the filter with parameters.
        PARAMETERS:
        haarRoot        directory containing viola-jones files
        haarCascade     the Viola-Jones xml file to use
        haarParams      a dictionary of parameters for the haar filter
        hogWin          the hog window size
        svmPkl          path to the pickled SVM
        alpha           the minimum score needed to pass
        maxFaces        the maximum number of face to consider (None = all)
        scoreType       scoring method, either 1 or 2
        accCrit         acceptance criteria, either 1 or 2 or 3
        f               the critical ratio
        areaW           boolean, whether or not to take face area into account
        areaWs          area weight
        distWs          hyperplane distance weight
        fast            boolean, if true will not pickle entire thing but
                        rather store pointers to joblib pickled files
        '''
        if scoreType != 1 and scoreType != 2:
            # raise an input error
            pass
        if accCrit != 1 and accCrit != 2 and accCrit != 3:
            # raise an input error
            pass
        self.haarFile = haarFile 
        if haarFile == None:
            self.haarFile = '/data/model_data/haar_cascades/haarcascade_frontalface_alt2.xml'
        if haarParams == None:
            self.haarParams = {'minNeighbors': 8, 'minSize': (50, 50), 'scaleFactor': 1.1}
        else:
            self.haarParams = haarParams
        if svmPkl == None:
            self.svmPkl = '/data/model_data/svms/SVMw40.pkl'
        else:
            self.svmPkl = svmPkl
        self.svm = joblib.load(self.svmPkl)
        self.alpha, self.maxFaces, self.scoreType, self.accCrit, \
        self.f, self.areaW, self.areaWs, self.distWs, self.hogWin \
        = alpha, maxFaces, scoreType, accCrit, f, areaW, areaWs, \
        distWs, hogWin
        self.cellSize = np.array([8,8])
        self.blockStride = np.array([8,8])
        self.blockSize = np.array([16,16])
        self.fast = fast # if True, will load the svmPkl directly
        # upon resuming, otherwise it will pickle the SVM file itself.
        self.gradientBinSize = 9
        if self.accCrit == 3:
            if self.scoreType != 2:
                print 'Type-3 accuracy filtering requires score type 2'
                self.scoreType = 2
        self.face_cascade = cv2.CascadeClassifier()
        self.face_cascade.load(os.path.join(haarRoot, haarCascade))
        self.HOGparams = [(hogWin, hogWin), (16, 16), (8, 8), (8, 8), 9]
        self.hog = cv2.HOGDescriptor(*self.HOGparams)
        self.nFaces = 0
        self.openEyeFaces = 0

    def get_gray(self, image):
        # returns the grayscale version of an image
        if len(image.shape) == 3 and image.shape[2] > 1:
            image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        return image

    def _compute_hogs(self, img):
        cellSize = np.array([8,8])
        img = cv2.resize(img, (self.hogWin, self.hogWin)) # resize is width x height! wtf.
        descriptorValues = self.hog.compute(img)
        cells_in_x_dir, cells_in_y_dir = self.hogWin / self.cellSize # assuming [height, width]
        gradientStrengths = np.zeros([cells_in_y_dir, cells_in_x_dir, self.gradientBinSize])
        cellUpdateCounter = np.zeros([cells_in_y_dir, cells_in_x_dir])
        descriptorDataIdx = 0
        for blockx in range(cells_in_x_dir - 1):
            for blocky in range(cells_in_y_dir - 1):
                for cellNr in range(4):
                    cellx, celly = blockx, blocky
                    if cellNr == 1 or cellNr == 3: celly+=1
                    if cellNr == 2 or cellNr == 3: cellx+=1
                    for binN in range(self.gradientBinSize):
                        gradientStrengths[celly, cellx, binN] += descriptorValues[descriptorDataIdx]
                        descriptorDataIdx+=1
                    cellUpdateCounter[celly, cellx]+=1
        for binN in range(self.gradientBinSize):
            gradientStrengths[celly, cellx, binN]/=cellUpdateCounter[celly, cellx]
        return gradientStrengths

    def extract_face(self, image, coords):
        x, y, w, h = coords
        return image[y:(y+h), x:(x+w)]

    def score(self, image):
        image = self.get_gray(image)
        # detect faces
        faces = self.face_cascade.detectMultiScale(image, **self.haarParams)
        self.nFaces = len(faces)
        if not len(faces):
            # if no faces are seen, return a score of 0
            self.openEyeFaces = 0
            return 0
        if self.maxFaces != None:
            faces = faces[:self.maxFaces]
        # extract faces
        areas = np.array([np.prod(x[-2:]) for x in faces])
        faces = [self.extract_face(image, x) for x in faces]
        HOGs = [self._compute_hogs(x) for x in faces]
        HOGs = np.concatenate([x[...,np.newaxis] for x in HOGs], axis=3)
        X = np.array([x.reshape(-1) for x in HOGs.transpose(3,0,1,2)])
        labels = self.svm.predict(X)
        self.openEyeFaces = sum(labels)
        if self.scoreType == 1:
            dists = np.array(self.svm.decision_function(X))
            areas = (self.areaWs*areas)**self.areaW
            dists = dists*self.distWs
            return 1./np.sum(areas) * np.dot(areas, dists)
        else:
            return 1./np.sum(areas) * np.dot(areas, labels)

    def _accept_impl(self, image, frameno, video):
        score = self.score(image)
        if not self.nFaces:
            return True
        if self.accCrit == 1:
            return score > self.alpha
        elif self.accCrit == 2:
            return self.openEyeFaces >= self.f + (1-self.f)/self.nFaces
        elif self.accCrit == 3:
            return score >= self.f + (1-self.f)/self.nFaces

    def __getstate__(self):
        # the openCV functions cannot be pickled directly, so as in
        # predictor.py, we have to manually pickle/unpickle them.
        # the HOG feature detector does not itself store anything
        # terribly complicated and can be initialized fairly rapidly,
        # so we don't really need to worry about that, but the
        # haarCascade should be pickled as the required haar file.
        #
        # further, this filter makes use of scikit-learn SVMs, which,
        # while able to be pickled, are more efficiently stored using
        # joblib, which has been optimized for numpy arrays.
        state = self.__dict__.copy()
        #############################################
        # let's attempt to do it with joblib
        #svm_str = pickle.dumps(self.svm)
        if not self.fast:
            tfile,tfilename = tempfile.mkstemp()
            try:
                os.close(tfile)
                files = joblib.dump(self.svm, tfilename)
                svm_str = [open(x).read() for x in files]
            finally:
                _ = [os.unlink(x) for x in files]
            state['svm'] = [files, svm_str]
        else:
            state['svm'] = self.svmPkl


        #     pass
        #############################################
        haar_str = open(self.haarFile, 'r').read()
        state['hog'] = ''
        state['face_cascade'] = haar_str
        return state

    def __setstate__(self, state):
        #############################################
        # self.svm = pickle.loads(state['svm'])
        if not state['fast']:
            try:
                for n,joblibf in enumerate(state['svm'][1]):
                    # we have to make sure these stay open while we're using
                    # them
                    with open(state['svm'][0][n], 'w+b') as f:
                        f.write(joblibf)
                files_to_remove = state['svm'][0]
                state['svm'] = joblib.load(state['svm'][0][0])
            finally:
                for pklfn in files_to_remove:
                    if os.path.exists(pklfn):
                        os.unlink(pklfn)
        else:
            try:
                print 'In test mode: not attempting to load the pkl from filename'
                #state['svm'] = joblib.load(state['svmPkl'])
            except:
                pass 
                # the svm file could not be located, instead
                # the restore_additional_data method will have to be 
                # used. 
        #############################################
        state['hog'] = cv2.HOGDescriptor(*state['HOGparams'])
        tfile,tfilename = tempfile.mkstemp()
        face_cascade = cv2.CascadeClassifier()
        try:
            os.close(tfile)
            with open(tfilename, 'w+b') as f:
                f.write(state['face_cascade'])
            face_cascade.load(tfilename)
        finally:
            os.unlink(tfilename)
        state['face_cascade'] = face_cascade
        self.__dict__ = state

    def restore_additional_data(self, filename):
        # this is based on the presumption that the model_data
        # folder will store both the model pickles as well as
        # the required SVM files. Filename is the filename of 
        # the model that you are trying to load. If the 
        #   
        fn = os.path.join('/'.join(filename.split('/')[:-1]), 'svms', self.svmPkl)
        self.svm = joblib.load(fn)

    def short_description(self):
        return 'closed eyes'