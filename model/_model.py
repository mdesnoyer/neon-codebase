'''A classifier that predicts Valence scores for images.

To use this model, you first add labeled images to the model by
calling add_*() and then train the model using train().  The
Predictor and FeatureGenerator classes are abstract and should
be specialized by subclassing. Note that as the code version changes,
code may need to be added to deal with backwards compatibility when
pickling/unpickling. See the Python pickling docs about those issues.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import bisect
import cPickle as pickle
import cv2
from . import features
from . import filters
import ffvideo
import hashlib
import heapq
import logging
import math
import numpy as np
import operator
from . import predictor
import time
import utils.obj
from utils import pycvutils
from utils import statemon

_log = logging.getLogger(__name__)

statemon.define('all_frames_filtered', int)
statemon.define('cv_video_read_error', int)
statemon.define('video_processing_error', int)
statemon.define('low_number_of_frames_seen', int)

class VideoReadError(IOError): pass

class Model(object):
    '''The whole model, which consists of a predictor and a filter.'''    
    def __init__(self, predictor, filt=None):
        self.__version__ = 2
        self.predictor = predictor
        self.filt = filt
        self.gist = features.MemCachedFeatures.create_shared_cache(
            features.GistGenerator())

    def __str__(self):
        return utils.obj.full_object_str(self)

    def reset(self):
        self.predictor.reset()

    def score(self, image, frameno=None, video=None):
        '''Scores a single image. 

        Inputs:
        image - Image in numpy BGR format (aka OpenCV)
        franemno - Frame number in the video for this image (if a video)
        video - The OpenCV video (if a video)

        Returns: (score, attribute_string) of the image. If it was
        filtered, the score will be -inf and the attribute will
        describe how it was filtered.

        '''
        if self.filt is None or self.filt.accept(image, frameno, video):
            return (self.predictor.predict(image), '')
        return (float('-inf'), self.filt.short_description())

    def is_duplicate(self, a, b):
        '''Returns true if image a is a duplicate of image b.

        a and b are numpy images in BGR format.
        '''
        dist = np.linalg.norm(self.gist.generate(a) -
                              self.gist.generate(b))
        return dist <= 0.6

    def filter_duplicates(self, image_list, n=None, tup_idx=0):
        '''Filters an image list of duplicates and returns at most n entries.

        image_list - a list of tuples where entry tup_idx is a numpy image
                     in BGR format. Must be ordered by score.
        n - Maximum number of images to return

        returns: a filtered list of tuples with a numpy images in BGR format
                 as the first entry
        '''
        if n is None:
            n = len(image_list)

        retval = []
        for tup in image_list:
            # Check if this image is a duplicate of any selected image
            is_dup = False
            for chosen_tup in retval:
                if self.is_duplicate(chosen_tup[tup_idx], tup[tup_idx]):
                    is_dup = True
                    break

            if not is_dup:
                # This image is selected
                retval.append(tup)

            if len(retval) == n:
                break
        return retval
       
    def choose_thumbnails(self, video, n=1, start_time=0.0,
                          end_buffer_time=0.0,
                          thumb_min_dist=5.0,
                          processing_time_ratio=1.0,
                          video_name=''):
        # Clear the gist cache
        self.gist.reset()
        
        return self.choose_thumbnails_bisect(
            video,
            n=n,
            start_time=start_time,
            end_buffer_time=end_buffer_time,
            thumb_min_dist=thumb_min_dist,
            processing_time_ratio=processing_time_ratio,
            video_name=video_name)

    def choose_thumbnails_bisect(self, video, n=1, start_time=0.0,
                                 end_buffer_time=0.0,
                                 thumb_min_dist=5.0,
                                 processing_time_ratio=1.0,
                                 video_name=''):
        ''' Selects the top n thumnails from a video.  Uses a bisection search
        method.

        Inputs:
        video - Hook to a video. Expects cv2.VideoCapture
        n - Number of thumbnails to return.  Sorted by decreasing score.
        start_time - The time in seconds when to start looking. 
        end_buffer_time - The time in seconds to ignore at the end of the video
        thumb_min_dist - Returned thumbnails must be at least this number of
                         seconds apart.
        processing_time_ratio - Ratio of processing time allowed relative to 
                                the video length.
        video_name - Name of the video for logging purposes

        Returns: ([(image,score,frame_no,timecode,attribute)],end_time)
          sorted by score
	
        '''
        #TODO(mdesnoyer): Parameterize this function for runtime and
        #min leaf distance
        brackets = []
        scores = {}
        frames_processed = 0
        seek_loc = [None]

        # A min heap that holds tuples of the form (score, frame_no,
        # attribute, thumb)
        results = []
        max_heap_size = int(5 * n)

        def get_frame(video, f):
            more_data, cur_frame = pycvutils.seek_video(video, f,
                                                        cur_frame=seek_loc[0])
            seek_loc[0] = cur_frame
            if not more_data:
                if cur_frame is None:
                    raise VideoReadError("Could not read the video")
            more_data, frame = video.read() 
            return frame
        ### end get_frame

        def score_frame(video, f):
            if not f in scores.keys():
                frame = get_frame(video, f)
                if not frame == None:
                    score, attr = self.score(frame, f, video)

                    # If the frame was filtered, then the score is
                    # -inf. Set it to nan, so that it's not included in the
                    # mean
                    if attr:
                        scores[f] = None
                    else:
                        scores[f] = score

                    result = (score, f, attr, frame)
                    # Add the result to the heap if the score isn't too small
                    if len(results) < max_heap_size:
                        # There's room in the heap, so just add the element
                        heapq.heappush(results, result)
                    else:
                        # Push the result onto the heap, but pop the
                        # smallest element to keep the size the same
                        heapq.heappushpop(results, result)
                else:
                    scores[f] = None
                    _log.info_n('Could not extract frame %d in video %s' % 
                                (f, video_name),
                                10)
                ### end if
            return scores[f]
        ### end score_frame 

        # local helper function to score brackets
        def score_bracket(video, bracket, num_frames):
            score = [score_frame(video, b) for b in bracket]
            score = [x for x in score if x is not None]
            if len(score) == 0:
                # If both ends are filtered, then prioritize by the
                # size of the bracket
                return 1000-float(bracket[1]-bracket[0]) / num_frames
            return -np.mean(score)
        ### end def score_bracket

     
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        video_time = float(num_frames) / fps
        max_processing_time = processing_time_ratio * video_time
        start_processing_time = time.time()
        _log.info('Processing video %s' % video_name)
        _log.info('Number of frames: %d. FPS: %f' % (num_frames, fps))
        leaves = []
        # Only look at frames far enough away from previous find
        leaf_min_dist = fps * thumb_min_dist 
        end_offset = max(2, fps*end_buffer_time)

        # num_frames-2 to avoid emptry frame exceptions that 
        # seem to occur when its num_frames-1
        b0 = (int(fps*start_time), num_frames-end_offset) 
        try:
            # num_frames-2 to avoid emptry frame exceptions that
            # seem to occur when its num_frames-1
            score0 = score_bracket(video, b0, num_frames)
            heapq.heappush(brackets, (score0, b0))
            while (len(brackets) > 0 and
                   (time.time() - start_processing_time) < 
                    max_processing_time):
                old_frames = len(scores.keys())
                b = heapq.heappop(brackets)
                if (b[1][1] - b[1][0]) < 2:
                    # We reached a leaf, so stop
                    bisect.insort_left(leaves, b[1][0])
                    continue

                # If we are too close to a leaf, then drop this bracket
                leaf_idx = (bisect.bisect_left(leaves, b[1][0]),
                            bisect.bisect_left(leaves, b[1][1]))
                if (len(leaves) > 0 and 
                    max((min([abs(leaves[leaf_idx[i]-j] - b[1][i]) 
                              for j in [0,1] if ((leaf_idx[i]-j) >= 0 and 
                                                 (leaf_idx[i]-j) < len(leaves))
                            ]) 
                        for i in [0,1])) < leaf_min_dist):
                    _log.debug('Dropping %s' % str(b))
                    continue
                
                _log.debug('Looking at bracket (%i, %i)' % b[1])
                #_log.debug('Brackets %s' % brackets)
                # Split the bracket in half.
                b1 = (b[1][0], int(math.floor((b[1][0]+b[1][1])/2)))
                b2 = (int(math.ceil((b[1][0]+b[1][1])/2)), b[1][1])
                # Rescore each half of the bracket and put them in
                # the priority queue
                s1 = score_bracket(video, b1, num_frames)
                heapq.heappush(brackets, (s1, b1))

                s2 = score_bracket(video, b2, num_frames)
                heapq.heappush(brackets, (s2, b2)) 
            ### end while still_executing
        except VideoReadError:
            statemon.state.increment('cv_video_read_error')
            raise
        except Exception as e:
            _log.exception("Unexpected error when searching through video %s" %
                           video_name)
            statemon.state.increment('video_processing_error')
                
        ### end except Exception as e
        _log.info('Looked at %i frames (%3.2f%%)' % (
            len(scores), 100.0 * len(scores) / num_frames))
        if len(scores) < 10:
            _log.warn('Low number of frames (%i) seen from video %s' %
                      (len(scores), video_name))
            statemon.state.increment('low_number_of_frames_seen')

        # now we have the scored frames, we have to sort them 
        # in decreasing score order and return the stats 
        # the interface demands.
        results = sorted(results,
                         key=lambda x: (-x[0],
                                        hashlib.md5(str(x[1])).digest()))

        # Check to see if all the images were filtered. If they were,
        # then just sample the video evenly
        if len(results) == 0 or results[0][2] != '':
            _log.warn('All images filtered in video %s. '
                      'Grabbing even samples instead.' % video_name)
            statemon.state.increment('all_frames_filtered')
            return self.grab_frames_evenly(video, n), b0[1]/fps
        
        results = self.filter_duplicates(results, n=n, tup_idx=3)
        
        retval = []
        for score, frameno, attr, image in results:
            retval.append((image,
                           score,
                           frameno,
                           frameno/fps,
                           attr))
        ### end for ... in results

        return retval, b0[1]/fps
    ### end choose_thumbnails_bisect 

    def grab_frames_evenly(self, video, n=1, end_buffer=0.1):
        '''Extracts frames from the video evenly.

        Duplicates are not filtered.

        Inputs:
        video - Hook to a video. cv2.VideoCapture please
        n - Number of thumbnails to extract
        end_buffer - Fraction of the video to skip at the beginning and end

        Returns: ([ (image, score, frame_no, timecode, attribute) ])
        '''
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        step_size = int(num_frames*(1.0-2.0*end_buffer)/(n-1))
        
        results = []
        cur_frame = None
        for frameno in range(int(num_frames * end_buffer),
                             int(num_frames * (1-end_buffer))+1,
                             step_size):
            try:
                seek_sucess, cur_frame = pycvutils.seek_video(
                    video,
                    frameno,
                    cur_frame=cur_frame)
                if not seek_sucess:
                    if cur_frame is None:
                        raise VideoReadError("Could not read the video")
                    continue

                # Read the frame
                read_sucess, image = video.read()
                if not read_sucess:
                    continue
                cur_frame += 1

                # Get the score
                score, attr = self.score(image, cur_frame, video)

                # Append to the results
                results.append((
                    image, 
                    score,
                    frameno,
                    frameno / float(fps),
                    attr))
            except VideoReadError:
                statemon.state.increment('cv_video_read_error')
                raise
            except Exception as e:
                _log.exception("key= grab_frames_evenly"
                               "msg=processing error msg=" + e.__str__())
                statemon.state.increment('video_processing_error')
                break

        # Reorder the frames by score and then a hash of the frame number
        results = sorted(results,
                         key=lambda x: (-x[1],
                                        hashlib.md5(str(x[2])).digest()))
        return results
                    

    def choose_thumbnails_uniform(self, video, n=1, sample_step=1.0,
                                  start_time=0.0):
        '''Selects the top n thumbnails from a video.

        Inputs:
        video - Hook to a video. cv2.VideoCapture please
        n - Number of thumbnails to return. They will be sorted by score
        sample_step - Number of seconds to step between samples
        start_time - Time in seconds in the video when to start looking

        Returns: ([ (image, score, frame_no, timecode, attribute) ],
                  end_time)
        Note: The images are in BGR format

        '''
        # min heap that holds a tuples of (score, frame_no, timecode,
        # attribute, image)
        results = []
        max_heap_size = int(5 * n)
            
        duration = video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)
        
        # Assume 30.0 fps if the backend doesn't tell us what it really is
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0

        # Clear the gist cache
        self.gist.reset()

        frame_step = max(1, round(sample_step * fps))
        next_frame = max(0, start_time * fps)
        seek_sucess = True
        read_sucess = True
        first_move = True
        cur_frame = None
        
        while seek_sucess and read_sucess:
            try:
                seek_sucess, cur_frame = pycvutils.seek_video(
                    video,
                    next_frame,
                    do_log=first_move,
                    cur_frame=cur_frame)
                if not seek_sucess:
                    if cur_frame is None:
                        raise VideoReadError("Could not read the video")
                    break
                first_move = False
                
                # Read the frame
                read_sucess, image = video.read()
                if not read_sucess:
                    break
                timecode = cur_frame / float(fps)
                frameno = cur_frame

                
                score, attr = self.score(image, cur_frame, video)

                cur_frame = next_frame + 1
                next_frame += frame_step

                result = (score, frameno, timecode, attr, image)

                # Add the result to the heap if the score isn't too small
                if len(results) < max_heap_size:
                    # There's room in the heap, so just add the element
                    heapq.heappush(results, result)
                else:
                    # Push the result onto the heap, but pop the
                    # smallest element to keep the size the same
                    heapq.heappushpop(results, result)
                    
            except Exception as e:
                _log.exception("key=choose_thumbnails_uniform "
                               "msg=processing error msg=" + e.__str__())
                break

        # Sort by score and then the hash of the frame number
        results = sorted(results,
                         key=lambda x: (-x[0],
                                        hashlib.md5(str(x[1])).digest()))

        results = self.filter_duplicates(results, n=n, tup_idx=4)

        retval = []
        for score, frameno, timecode, attr, image in results:
            retval.append((image,
                           score,
                           frameno,
                           timecode, 
                           attr))
            
        return retval, cur_frame / float(fps)

    #TODO(mdesnoyer): Remove this function and get OpenCV to be able
    #to handle more video types.
    def ffvideo_choose_thumbnails(self, video, n=1, sample_step=1.0,
                                  start_time=0.0):
        '''Same as choose_thumbnails, but uses and ffvideo stream.'''
        
        # min heap that holds a tuples of (score, frame_no, timecode,
        # attribute, image)
        results = []
        max_heap_size = int(5 * n)

        duration = video.duration

        # Clear the gist cache
        self.gist.reset()

        cur_time = start_time
        while cur_time < duration:
            try:
                frame = video.get_frame_at_sec(cur_time)
                timecode = cur_time
                frameno = frame.frameno
                cur_time += sample_step

                if frame.mode == 'RGB':
                    # Convert to BGR
                    image = frame.ndarray()[:,:,::-1]
                else:
                    # It is greyscale
                    image = frame.ndarray()
                score, attr = self.score(image)

                result = (score, frameno, timecode, attr, image)
                # Add the result to the heap if the score isn't too small
                if len(results) < max_heap_size:
                    # There's room in the heap, so just add the element
                    heapq.heappush(results, result)
                else:
                    # Push the result onto the heap, but pop the
                    # smallest element to keep the size the same
                    heapq.heappushpop(results, result)
 
            # No more frames to process
            except ffvideo.NoMoreData:
                break

            except Exception as e:
                _log.exception("key=choose_thumbnails msg=processing error msg=" + e.__str__())
                break

        # Sort by score and then the hash of the frame number
        results = sorted(results,
                         key=lambda x: (-x[0],
                                        hashlib.md5(str(x[1])).digest()))

        results = self.filter_duplicates(results, n=n, tup_idx=4)

        retval = []
        for score, frameno, timecode, attr, image in results:
            retval.append((image,
                           score,
                           frameno,
                           timecode, 
                           attr))
            
        return retval, cur_frame / float(fps)

def save_model(model, filename):
    '''Save the model to a file.'''
    with open(filename, 'wb') as f:
        pickle.dump(model, f, 2)

def load_model(filename):
    '''Loads a model from a file.

    Inputs:
    filename - The model to load

    '''
    with open(filename, 'rb') as f:
        return pickle.load(f)
