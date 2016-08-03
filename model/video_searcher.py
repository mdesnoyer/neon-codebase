'''An algorithm to search through a video for the best thumbnails.

WARNING: As of 2016-07, nothing in this file should be used. The interface is broken.
TODO(Nick): Fix the interface, or remove this module as it has been superseeded by local_video_searcher.

Copyright: 2015 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import bisect
import cv2
import hashlib
import heapq
import ffvideo
import logging
import math
import model.errors
from model import features
from model import colorname
import numpy as np
import time
import utils.obj
from utils import pycvutils
from utils import statemon

_log = logging.getLogger(__name__)

statemon.define('all_frames_filtered', int)
statemon.define('cv_video_read_error', int)
statemon.define('video_processing_error', int)
statemon.define('low_number_of_frames_seen', int)

# TODO(Nick): Merge the local video searcher into this common
# interface. Also, make this return model.VideoThumbnail object lists.
class VideoSearcher(object):
    '''Abstract video searcher.'''
    def __init__(self, predictor,
                 filt=None,
                 filter_dups=True,
                 startend_buffer=0.1,
                 max_startend_buffer=5.0,
                 thumb_min_dist=0.1,
                 max_thumb_min_dist=10.0,
                 gist_threshold = 0.01,
                 colorname_threshold = 0.015):
        '''
        Inputs:
        predictor - Predictor to use for a single frame
        filt - Filter to use on the frames
        filter_dups - If true, duplicates are filtered
        startend_buffer - Fraction of the video to ignore at the start and 
                          end of the video
        max_start_end_buffer - Maximum startend_buffer in seconds
        thumb_min_dist - Minimum distance between thumbs to return in 
                         fraction of the video
        max_thumb_min_dist - Maximum thumb_min_dist in seconds.
        '''
        
        self.__version__ = 1
        self.predictor = predictor
        self.filt = filt
        self.gist = features.MemCachedFeatures.create_shared_cache(
            features.GistGenerator())
        self.colorname = features.MemCachedFeatures.create_shared_cache(
            features.ColorNameGenerator())
        self.startend_buffer=startend_buffer
        self.max_startend_buffer=max_startend_buffer
        self.thumb_min_dist=thumb_min_dist
        self.max_thumb_min_dist=max_thumb_min_dist
        self.filter_dups=filter_dups
        self.gist_threshold = gist_threshold
        self.colorname_threshold = colorname_threshold

    def __str__(self):
        return utils.obj.full_object_str(self)

    def choose_thumbnails(self, video, n=1, video_name='', m=0):
        '''Selects the top n thumbnails from a video.

        Inputs:
        video - Hook to a video. Expects cv2.VideoCapture
        n - Number of thumbnails to return.  Sorted by decreasing score.
        video_name - Name of the video for logging purposes
        m - Number of bad thumbnails to return.

        Returns:
        ([(image,score,frame_no,timecode,attribute)]) sorted by score

        Note: The images are in OpenCV aka BGR format.
        '''
        # Clear the gist cache
        self.gist.reset()

        new_n = 5*n if self.filter_dups else n
        
        thumbs = self.choose_thumbnails_impl(video, new_n, video_name)
        if self.filter_dups:
            return self.filter_duplicates(thumbs, n, 0, m)
        return thumbs, []

    def choose_thumbnails_impl(self, video, n=1, video_name='', m=0):
        '''Implementation of choose_thumbnails.

        Returns:
        ([(image,score,frame_no,timecode,attribute)]) sorted by score
        '''
        raise NotImplementedError();

    def is_duplicate(self, a, b):
        '''Returns true if image a is a duplicate of image b.

        a and b are numpy images in BGR format.
        '''
        gist_dis = colorname.JSD(self.gist.generate(a),
                                       self.gist.generate(b))
        colorname_dis = colorname.JSD(self.colorname.generate(a),
                                            self.colorname.generate(b))
        return ((gist_dis < self.gist_threshold and \
                    colorname_dis < 2 * self.colorname_threshold)
                or (colorname_dis < self.colorname_threshold and
                    gist_dis < 2 * self.gist_threshold))

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

    def score_single_frame(self, image, frameno=None, video=None):
        '''Scores a single image. 

        Inputs:
        image - Image in numpy BGR format (aka OpenCV)
        franemno - Frame number in the video for this image (if a video)
        video - The OpenCV video (if a video)
        do_filtering - Should the filter be applied?

        Returns: (score, attribute_string) of the image. If it was
        filtered, the score will be -inf and the attribute will
        describe how it was filtered.

        '''
        if (self.filt is None or self.filt.accept(image, frameno, video)):
            return (self.predictor.predict(image), '')
        return (float('-inf'), self.filt.short_description())

    def get_name(self):
        return 'AbstractSearcher'


class BisectSearcher(VideoSearcher):
    '''A video searcher that uses bisection search.'''
    def __init__(self, predictor,
                 filt=None,
                 filter_dups=True,
                 startend_buffer=0.1,
                 max_startend_buffer=5.0,
                 thumb_min_dist=0.1,
                 max_thumb_min_dist=10.0,
                 processing_time_ratio=1.0,
                 gist_threshold = 0.01,
                 colorname_threshold = 0.015):
        super(BisectSearcher, self).__init__(predictor,
                                             filt,
                                             filter_dups,
                                             startend_buffer,
                                             max_startend_buffer,
                                             thumb_min_dist,
                                             max_thumb_min_dist,
                                             gist_threshold,
                                             colorname_threshold)
        self.processing_time_ratio = processing_time_ratio

    def choose_thumbnails_impl(self, video, n=1, video_name='', m=0):
        brackets = []
        scores = {}
        frames_processed = 0
        seek_loc = [None]

        results = []

        def get_frame(video, f):
            more_data, cur_frame = pycvutils.seek_video(video, f,
                                                        cur_frame=seek_loc[0])
            seek_loc[0] = cur_frame
            if not more_data:
                if cur_frame is None:
                    raise model.errors.VideoReadError(
                        "Could not read the video")
            more_data, frame = video.read() 
            return frame
        ### end get_frame

        def score_frame(video, f):
            if not f in scores.keys():
                frame = get_frame(video, f)
                if frame is not None:
                    score, attr = self.score_single_frame(frame, f, video)

                    # If the frame was filtered, then the score is
                    # -inf. Set it to nan, so that it's not included in the
                    # mean
                    if attr:
                        scores[f] = None
                    else:
                        scores[f] = score

                    result = (score, f, attr, frame)
                    # Add the result to the heap if the score isn't too small
                    if len(results) < n:
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
        max_processing_time = self.processing_time_ratio * video_time
        start_processing_time = time.time()
        _log.info('Processing video %s' % video_name)
        _log.info('Number of frames: %d. FPS: %f' % (num_frames, fps))
        leaves = []
        # Only look at frames far enough away from previous find
        leaf_min_dist = int(min(num_frames * self.thumb_min_dist, 
                                fps * self.max_thumb_min_dist))
        startend_buffer = int(min(num_frames * self.startend_buffer,
                                  fps * self.max_startend_buffer))
        end_offset = max(2, startend_buffer)

        # num_frames-2 to avoid emptry frame exceptions that 
        # seem to occur when its num_frames-1
        b0 = (startend_buffer, num_frames-end_offset) 
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
        except model.errors.VideoReadError:
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
            return self.grab_frames_evenly(video, n)
        
        retval = []
        for score, frameno, attr, image in results:
            retval.append((image,
                           score,
                           frameno,
                           frameno/fps,
                           attr))
        ### end for ... in results

        return retval
    ### end choose_thumbnails_bisect 

    def grab_frames_evenly(self, video, n=1):
        '''Extracts frames from the video evenly.

        Duplicates are not filtered.

        Inputs:
        video - Hook to a video. cv2.VideoCapture please
        n - Number of thumbnails to extract

        Returns: ([ (image, score, frame_no, timecode, attribute) ])
        '''
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))
        end_buffer = int(min(self.max_startend_buffer * fps,
                             self.startend_buffer * num_frames))
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
                        raise model.errors.VideoReadError("Could not read the video")
                    continue

                # Read the frame
                read_sucess, image = video.read()
                if not read_sucess:
                    continue
                cur_frame += 1

                # Get the score
                score, attr = self.score_single_frame(image, cur_frame, video)

                # Append to the results
                results.append((
                    image, 
                    score,
                    frameno,
                    frameno / float(fps),
                    attr))
            except model.errors.VideoReadError:
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

    def get_name(self):
        return 'BisectSearcher'

class UniformSamplingSearcher(VideoSearcher):
    '''Searches through the video using uniform sampling.'''
    def __init__(self,
                 predictor,
                 filt=None,
                 filter_dups=True,
                 startend_buffer=0.1,
                 max_startend_buffer=5.0,
                 thumb_min_dist=0.1,
                 max_thumb_min_dist=10.0,
                 sample_step=1.0,
                 gist_threshold = 0.01,
                 colorname_threshold = 0.015):
        super(UniformSamplingSearcher, self).__init__(predictor,
                                                      filt,
                                                      filter_dups,
                                                      startend_buffer,
                                                      max_startend_buffer,
                                                      thumb_min_dist,
                                                      max_thumb_min_dist,
                                                      gist_threshold,
                                                      colorname_threshold)
        
        self.sample_step = sample_step # Step in seconds within the video

    def choose_thumbnails_impl(self, video, n=1, video_name='', m=0):
        results = []
            
        duration = video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT)
        
        # Assume 30.0 fps if the backend doesn't tell us what it really is
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS) or 30.0
        num_frames = int(video.get(cv2.cv.CV_CAP_PROP_FRAME_COUNT))

        frame_step = max(1, round(self.sample_step * fps))
        startend_buffer = int(min(num_frames * self.startend_buffer,
                                  fps * self.max_startend_buffer))
        thumb_min_dist = int(min(num_frames * self.thumb_min_dist, 
                                 fps * self.max_thumb_min_dist))
        
        next_frame = max(0, startend_buffer)
        seek_sucess = True
        read_sucess = True
        first_move = True
        cur_frame = None
        
        while seek_sucess and read_sucess and next_frame < startend_buffer:
            try:
                seek_sucess, cur_frame = pycvutils.seek_video(
                    video,
                    next_frame,
                    do_log=first_move,
                    cur_frame=cur_frame)
                if not seek_sucess:
                    if cur_frame is None:
                        raise model.errors.VideoReadError("Could not read the video")
                    break
                first_move = False
                
                # Read the frame
                read_sucess, image = video.read()
                if not read_sucess:
                    break
                timecode = cur_frame / float(fps)
                frameno = cur_frame

                
                score, attr = self.score_single_frame(image, cur_frame, video)

                cur_frame = next_frame + 1
                next_frame += frame_step

                result = (score, frameno, timecode, attr, image)

                # TODO: Drop this result if there is a higher score
                # within thumb_min_dist

                # Add the result to the heap if the score isn't too small
                if len(results) < n:
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

        retval = []
        for score, frameno, timecode, attr, image in results:
            retval.append((image,
                           score,
                           frameno,
                           timecode, 
                           attr))
            
        return retval

    def get_name(self):
        return 'UniformSamplingSearcher'
