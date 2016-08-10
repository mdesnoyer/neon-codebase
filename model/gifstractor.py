#!/usr/bin/env python
""" Extracts a gif from a video by using Aquila."""

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

# import scenedetect
import cv2
import imageio
import numpy as np
from Queue import Queue
from threading import Thread
from threading import Lock
from threading import Event
from collections import defaultdict as ddict
from collections import Counter
import time
import model.predictor as predictor
import utils.autoscale
import utils.neon
import subprocess
import socket
from scipy import stats


# filetypes for the extraction
filetypes = ['gif', 'mp4']

# destination
dest = '/tmp/'

# source video
# source = '/data/fergie/targ.mp4'
# source = '/tmp/targ.mkv'
# source = '/tmp/donald.mp4'
# source = '/tmp/targ2.mkv'
# source = '/tmp/gopro.mkv'
# source = '/tmp/poketarg.mp4'
#src_n = 'goats'
#source = '/tmp/%s.mp4' % src_n
src_n = 'surfies'
source = '/data/neon/gopro/raw_yt/Surfies Point (Phillip Island) Raw Gopro Footage-2iMMb-NTSD0.mp4'

target_weights = None
target_weights = 'pro' # or 'raw'
weight_file = '/data/neon/gopro/gopro_weights.pkl'

# target gif/clip time in seconds
target_length = 10

# min_shot_distance controls how close to the beginning or the end
# of the extracted clip scene changes my occur, and it's given in 
# seconds. So if it's 1, then no clip will be extracted that has
# a detected scene change within 1 second of either the start or
# the end.
min_shot_distance = 0.5

# whether or not the gifs should be in temporal order
inorder = True

# the maximum amount of time to process, in terms of the total length of the
# video
processing_time_ratio = 0.5

# queue size is the number of images to maintain in the array.
max_queue_size = 100 

# max_width is the largest width for an image output
max_width = 500

# the number of workers to spawn
nworkers = 4

# weighting of valence vs. action
weight_dict = {'valence': 1.0, 'action': 0.25}

# number of gifs to pick
n_to_pick = 4

# max_overlap is the maximum overlap fraction that the
# gifs returned may have, where a value of 1.0 means that
# the gifs may totally overlap.
max_overlap = 0.0


# filters
def action_filter(prop_dict):
    """ require that the regions has an average action rank
    in the top 80 percentile """
    return prop_dict['action'][0] > 0.2

def coverage_filter(prop_dict):
    """ require that at least 1% of the frames in the region
    have been scored """
    for key in prop_dict.keys():
        if prop_dict[key][1] < 0.01:
            return False
    return True

region_filters = [action_filter] #, coverage_filter]

##############################################################################
#                   UTILITY
##############################################################################
def elapsed(start):
    """ returns elapsed time """
    return time.time() - start

class DummyPredictor(object):
    def __init__(self):
        pass

    def connect(self):
        pass

    def predict(self, img):
        return [np.random.randn(), None, None]

    def shutdown(self):
        pass

class DummyProc(object):
    def __init__(self):
        pass

    def terminate(self):
        pass

##############################################################################
#                   SCENE DETECTION
##############################################################################
class SceneDetector(object):
    """Base SceneDetector class to implement a scene detection algorithm."""
    def __init__(self):
        pass

    def process_frame(self, frame_num, frame_img, frame_metrics, scene_list):
        """Computes/stores metrics and detects any scene changes.

        Prototype method, no actual detection.
        """
        return

    def post_process(self, scene_list):
        pass


class ContentDetector(SceneDetector):
    """Detects fast cuts using changes in colour and intensity between frames.

    Since the difference between frames is used, unlike the ThresholdDetector,
    only fast cuts are detected with this method.  To detect slow fades between
    content scenes still using HSV information, use the DissolveDetector.
    """

    def __init__(self, threshold = 30.0, min_scene_len = 15):
        super(ContentDetector, self).__init__()
        self.threshold = threshold
        self.min_scene_len = min_scene_len  # minimum length of any given scene, in frames
        self.last_frame = None
        self.last_scene_cut = None
        self.last_hsv = None

    def process_frame(self, frame_num, frame_img, frame_metrics, scene_list):
        # Similar to ThresholdDetector, but using the HSV colour space DIFFERENCE instead
        # of single-frame RGB/grayscale intensity (thus cannot detect slow fades with this method).

        # Value to return indiciating if a scene cut was found or not.
        cut_detected = False
    
        if self.last_frame is not None:
            # Change in average of HSV (hsv), (h)ue only, (s)aturation only, (l)uminance only.
            delta_hsv_avg, delta_h, delta_s, delta_v = 0.0, 0.0, 0.0, 0.0

            if frame_num in frame_metrics and 'delta_hsv_avg' in frame_metrics[frame_num]:
                delta_hsv_avg = frame_metrics[frame_num]['delta_hsv_avg']
                delta_h = frame_metrics[frame_num]['delta_hue']
                delta_s = frame_metrics[frame_num]['delta_sat']
                delta_v = frame_metrics[frame_num]['delta_lum']

            else:
                num_pixels = frame_img.shape[0] * frame_img.shape[1]
                curr_hsv = cv2.split(cv2.cvtColor(frame_img, cv2.COLOR_BGR2HSV))
                last_hsv = self.last_hsv
                if not last_hsv:
                    last_hsv = cv2.split(cv2.cvtColor(self.last_frame, cv2.COLOR_BGR2HSV))

                delta_hsv = [-1, -1, -1]
                for i in range(3):
                    num_pixels = curr_hsv[i].shape[0] * curr_hsv[i].shape[1]
                    curr_hsv[i] = curr_hsv[i].astype(np.int32)
                    last_hsv[i] = last_hsv[i].astype(np.int32)
                    delta_hsv[i] = np.sum(np.abs(curr_hsv[i] - last_hsv[i])) / float(num_pixels)
                delta_hsv.append(sum(delta_hsv) / 3.0)
                delta_h, delta_s, delta_v, delta_hsv_avg = delta_hsv

                frame_metrics[frame_num]['delta_hsv_avg'] = delta_hsv_avg
                frame_metrics[frame_num]['delta_hue'] = delta_h
                frame_metrics[frame_num]['delta_sat'] = delta_s
                frame_metrics[frame_num]['delta_lum'] = delta_v

                self.last_hsv = curr_hsv

            if delta_hsv_avg >= self.threshold:
                if self.last_scene_cut is None or (
                  (frame_num - self.last_scene_cut) >= self.min_scene_len):
                    scene_list.append(frame_num)
                    self.last_scene_cut = frame_num
                    cut_detected = True

            #self.last_frame.release()
            del self.last_frame
                
        self.last_frame = frame_img.copy()
        return cut_detected


def detect_scenes(cap, scene_list, detector_list, downscale_factor = 0, 
                  frame_skip = 0, quiet_mode = False,
                  perf_update_rate = -1, start_frame = 0, end_frame = 0,
                  duration_frames = 0):
    """Performs scene detection based on passed video and scene detectors.

    Args:
        cap:  An open cv2.VideoCapture object that is assumed to be at the
            first frame.  Frames are read until cap.read() returns False, and
            the cap object remains open (it can be closed with cap.release()).
        scene_list:  List to append frame numbers of any detected scene cuts.
        detector_list:  List of scene detection algorithms to run on the video.
        perf_update_rate:  Optional. Number of seconds between which to
            continually prints updates with the current processing speed.
        downscale_factor:  Optional. Indicates at what factor each frame will
            be downscaled/reduced before processing (improves performance).
            For example, if downscale_factor = 2, and the input is 1024 x 400,
            each frame will be reduced to 512 x 200 ( = 1024/2 x 400/2).
            Integer number >= 2, otherwise disabled (i.e. scale = 1).
        frame_skip:  Optional.  Number of frames to skip during each iteration,
            useful for higher FPS videos to improve performance.
            Unsigned integer number larger than zero, otherwise disabled.
        save_images:  Optional.  If True the first and last frame of each scene
            is saved as an image in the current working directory, with the
            same filename as the original video and scene/frame # appended.
        start_frame:  Optional.  Integer frame number to start processing at.
        end_frame:  Optional.  Integer frame number to stop processing at.
        duration_frames:  Optional.  Integer number of frames to process;
            overrides end_frame if the two values are conflicting.

    Returns:
        Unsigned, integer number of frames read from the passed cap object.
    """
    frames_read = 0
    frame_metrics = {}
    last_frame = None       # Holds previous frame if needed for save_images.

    perf_show = True
    perf_last_update_time = time.time()
    perf_last_framecount = 0
    perf_curr_rate = 0
    if perf_update_rate > 0:
        perf_update_rate = float(perf_update_rate)
    else:
        perf_show = False

    if not downscale_factor >= 2: downscale_factor = 0
    if not frame_skip > 0: frame_skip = 0

    # set the end frame if duration_frames is set (overrides end_frame if set)
    if duration_frames > 0:
        end_frame = start_frame + duration_frames

    # If start_frame is set, we drop the required number of frames first.
    # (seeking doesn't work very well, if at all, with OpenCV...)
    while (frames_read < start_frame):
        (rv, im) = cap.read()
        frames_read += 1

    video_fps = cap.get(cv2.CAP_PROP_FPS)

    while True:
        # If we passed the end point, we stop processing now.
        if end_frame > 0 and frames_read >= end_frame:
            break

        # If frameskip is set, we drop the required number of frames first.
        if frame_skip > 0:
            for i in range(frame_skip):
                (rv, im) = cap.read()
                if not rv:
                    break
                frames_read += 1

        (rv, im) = cap.read()
        if not rv:
            break
        if not frames_read in frame_metrics:
            frame_metrics[frames_read] = dict()
        im_scaled = im
        if downscale_factor > 0:
            im_scaled = im[::downscale_factor,::downscale_factor,:]
        cut_found = False
        for detector in detector_list:
            cut_found = detector.process_frame(frames_read, im_scaled,
                frame_metrics, scene_list) or cut_found
        frames_read += 1
        # periodically show processing speed/performance if requested
        if not quiet_mode and perf_show:
            curr_time = time.time()
            if (curr_time - perf_last_update_time) > perf_update_rate:
                delta_t = curr_time - perf_last_update_time
                delta_f = frames_read - perf_last_framecount
                if delta_t > 0: # and delta_f > 0: # delta_f will always be > 0
                    perf_curr_rate = delta_f / delta_t
                else:
                    perf_curr_rate = 0.0
                perf_last_update_time = curr_time
                perf_last_framecount = frames_read
                print("[PySceneDetect] Current Processing Speed: %3.1f FPS" % perf_curr_rate)
        # save images on scene cuts/breaks if requested (scaled if using -df)

        del last_frame
        last_frame = im.copy()

    if start_frame > 0: frames_read = frames_read - start_frame
    return frames_read, frame_metrics


##############################################################################
#                   ESTABLISHING AN AQUILA CONNECTION
##############################################################################
def establish_tunnel(local_port=9000,
                     db_ip='10.0.69.27',
                     db_port=9000,
                     ssh_key=None):
    """
    Establishes an ssh tunnel to the database or instance of
    your choice.

    :param local_port: The local port you want to forward.
    :param db_ip: The database / instance IP address.
    :param db_port: The port on the instance to forward to.
    :param ssh_key: The ssh key to use.
    :return: The subprocess representing the connection.
    """
    if ssh_key:
        cmd = ('ssh -i ~/.ssh/{ssh_key} -L {local}:localhost:{db_port} ubuntu@{'
               'db_addr}').format(
            ssh_key=ssh_key,
            local=local_port,
            db_port=db_port,
            db_addr=db_ip)
    else:
        cmd = ('ssh -L {local}:localhost:{db_port} ubuntu@{'
               'db_addr}').format(
            local=local_port,
            db_port=db_port,
            db_addr=db_ip)
    print 'Forwarding port %s to the database' % local_port
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            stdin=subprocess.PIPE)
    # Wait until we can connect to the db
    sock = None
    for i in range(12):
        try:
            sock = socket.create_connection(('localhost', local_port), 3600)
        except socket.error:
            time.sleep(5)
    if sock is None:
        raise Exception('Could not connect to the database')

    print 'Connection made to the database at %s on %i' % (db_ip, db_port)
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()

    # ensure that proc is terminated with proc.terminate() before you exit!
    return proc

class Conn:
    def get_ip(self, force_refresh=False):
        return '10.0.69.27'

class LConn:
    def get_ip(self, force_refresh=False):
        return 'localhost'

##############################################################################
#                   SCENE DETECTION
##############################################################################
def obtain_scenes(targ):
    """
    Reads a video, and finds the frame number for each scene boundary. A value
    in the scene list indicates the frame corresponding to the start of the
    next shot.
    """
    detector_list = [scenedetect.detectors.ContentDetector()]
    scene_list = []
    video_fps, frames_read = scenedetect.detect_scenes_file(
        path, scene_list, detector_list, perf_update_rate=5,
        save_images=False, downscale_factor=2)
    print 'FPS is %.2f, %i total frames read.' % (video_fps, frames_read)
    return scene_list, video_fps, frames_read


##############################################################################
#                   FRAME YIELDER
##############################################################################
def frame_yielder(scene_list):
    # randomly yields scene, frame tuples for sampling
    pend = 0
    scenes2frames = dict()
    for n, i in enumerate(scene_list):
        scenes2frames[n] = range(pend, i)
        pend = i
        np.random.shuffle(scenes2frames[n])
    while scenes2frames:
        scene = np.random.choice(scenes2frames.keys())
        frame = scenes2frames[scene].pop()
        if not scenes2frames[scene]:
            scenes2frames.pop(scene)
        yield (scene, frame)


##############################################################################
#                   FRAME EXTRACTOR
##############################################################################
class Extractor(object):
    def __init__(self, video):
        self.video = video
        self.cur_frame = None

    @property
    def width(self):
        return self.video.get(cv2.CAP_PROP_FRAME_WIDTH)

    @property
    def height(self):
        return self.video.get(cv2.CAP_PROP_FRAME_HEIGHT)

    @property
    def frameno(self):
        return self.video.get(cv2.CAP_PROP_POS_FRAMES)

    @property
    def fps(self):
        return self.video.get(cv2.CAP_PROP_FPS)

    @frameno.setter
    def frameno(self, frame):
        self.video.set(cv2.CAP_PROP_POS_FRAMES, frame)

    def get_frame(self, frame):
        """ returns the frame """
        self.frameno = frame
        attempts = 0
        while True:
            if attempts > 0 and attempts % 1 == 0:
                print 'WARNING! %i attempts to seek %f, frame is %f' % (
                    attempts, frame, self.frameno)
            self.frameno = frame
            succ, img = self.video.read()
            if succ and (self.frameno == (frame + 1)):
                return img[:,:,::-1]  # make sure it's the PIL style
            attempts += 1

    def get_frames(self, frames):
        """ returns all the frames, in a dictionary """
        frames = sorted(frames)
        data = dict()
        for frame in frames:
            data[frame] = self.get_frame(frame)
        return data

def frame_extractor(video, scene_list, queue, halt_event):
    """ populates a queue with frames. Each element of the queue is a
    tuple of the form (scene, frameno, image)"""
    fydr = frame_yielder(scene_list)
    extr = Extractor(video)
    chk = []
    enq_cnt = 0
    for n, item in enumerate(fydr):
        chk.append(item)
        if len(chk) == max_queue_size:
            imdict = extr.get_frames([x[1] for x in chk])
            for tup in chk:
                ntup = (tup[0], tup[1], imdict[tup[1]])
                while True:
                    if halt_event.is_set():
                        print 'Halt is set -- Extractor terminating!'
                        return
                    try:
                        queue.put_nowait(ntup)
                        enq_cnt += 1
                        if not enq_cnt % 100:
                            print 'Enqueued %i frames' % enq_cnt
                        break
                    except Exception, e:
                        print e
                        time.sleep(2)
                        pass
            chk = []
    imdict = extr.get_frames([x[1] for x in chk])
    for tup in chk:
        ntup = (tup[0], tup[1], imdict[tup[1]])
        while True:
            if halt_event.is_set():
                return
            try:
                queue.put(ntup, block=True, timeout=1)
            except:
                pass


##############################################################################
#                   SCORE / DELTAS MAINTAINER
##############################################################################
class RegionScore(object):
    """
    Defines a class that performs scoring of arbitrary regions
    on a set of arbitrary parameters. This is intended to be
    used with video, so regions are referred to as 'scenes'
    which are quantized into 'frames'
    """
    def __init__(self, weights=None, regions=None, filters=None):
        """
        weights: a dict, mapping property_name -> importance
                 where importance is a float value in the
                 domain [0, 1]
        regions: a list of region boundaries as integers. The 
                 first value is either zero of the start of the
                 second region.
        filters: a list of functions that accepts a property 
                 dictionary and returns True or False

        Both of these may be set after the fact.
        """
        self._weights = None
        self._regions = regions
        self._filters = []
        if weights:
            self.set_weights(weights)
        if regions:
            self.set_regions(regions)
        if filters:
            self.set_filters(filters)
        # self._props is a dict of dicts as:
        # self._props[property][frame] = raw_score
        self._props = ddict(lambda: dict())
        self.n_scored = Counter()
        self._is_finalized = False
        self._rprops = dict()
        self._mt_lock = Lock()

    def set_weights(self, weights):
        try:
            self.property_names = weights.keys()
        except AttributeError, e:
            raise AttributeError('weights must be a dict mapping property to importance')
        self._weights = weights

    def set_regions(self, regions):
        if regions[0] == 0:
            self._regions = regions[1:]
        else:
            self._regions = regions[:]

    def set_filters(self, filters):
        self.filters = filters

    def update(self, property_name, frame, raw_score):
        """ updates the scoring objects knowledge, can be multithreaded """
        with self._mt_lock:
            self._props[property_name][frame] = raw_score
            self._is_finalized = False
            self.n_scored[property_name] += 1

    def _finalize(self):
        """
        computes the ranking ... 
        should it be the mean of ranks or the rank of a mean?

        the mean of ranks is probably more sensitive.
        """
        if self._is_finalized:
            return
        for cur_prop in self.property_names:
            if not len(self._props[cur_prop]):
                continue
            keys, values = zip(*self._props[cur_prop].iteritems())
            ranks = stats.rankdata(values)
            self._rprops[cur_prop] = {k:(v/len(ranks)) for k, v in zip(keys, ranks)}
        self.is_finalized = True

    def get_results(self):
        """ 
        returns a dictionary keyed by region number and
        includes, in order,
        [region_num, start, end, n frames, combined_score, mean_rank_dict]

        where mean_rank_dict is a dictionary from properties to a
        tuple: (mean_rank, frac_frames_scored)
        """
        self._finalize()
        results = []
        for region_n in range(len(self._regions)):
            start, end, prop_dict = self._get_region_score(region_n)
            comb_score = self._combine(prop_dict)
            nframes = end - start
            results.append([region_n, start, end, nframes, comb_score, prop_dict])
        return results

    def _combine(self, prop_dict):
        """
        accepts a property dictionary of the form returned by 
        _get_region_score and combines them according to 
        self._weights
        """
        cscore = 0
        wsum = 0
        if not self._check_filters_passed(prop_dict):
            return 0.
        for k, w in self._weights.iteritems():
            mean_rank, nsamp = prop_dict.get(k, (0., 0.))
            if not nsamp:
                continue
            cscore += mean_rank * w
            wsum += w
        if wsum == 0:
            return 0
        return cscore / wsum

    def _check_filters_passed(self, prop_dict):
        for cfilter in self._filters:
            if not cfilter(prop_dict):
                return False
        return True

    def _get_region_score(self, region_n):
        """
        returns the start and end of a region as well as a 
        dictionary for region_n that maps property
        names to a tuple (mean_rank, n_frames)
        """
        if region_n == 0:
            start = 0
        else:
            start = self._regions[region_n - 1]
        end = self._regions[region_n]
        cur_prop_dict = dict()
        for cur_prop in self.property_names:
            cur_prop_sum = 0
            cur_prop_count = 0
            for i in range(start, end):
                if i in self._rprops[cur_prop]:
                    cur_prop_sum += self._rprops[cur_prop][i]
                    cur_prop_count += 1
            cur_prop_dict[cur_prop] = (float(cur_prop_sum) / max(1, cur_prop_count), 
                                       float(cur_prop_count) / (end - start))
        return start, end, cur_prop_dict


##############################################################################
#                   ASSEMBLE GIF/CLIP
##############################################################################
def _asset_assemble(frames, video, fps, outfn):
    """ creates a gif or a clip given a sequence of frames. 
    the output filename and the frames per second must be specified """
    if inorder:
        frames.sort()
    ex = Extractor(video)
    h, w = ex.height, ex.width
    if w > max_width:
        scalef = float(max_width) / w
        h, w = int(h * scalef), int(w * scalef)
    all_frames = []
    print 'Extracting %i frames' % len(frames)
    w = int(w)
    h = int(h)
    for frame in frames:
        all_frames.append(cv2.resize(ex.get_frame(frame), (w, h)))
    print 'Writing asset'
    try:
        imageio.mimwrite(outfn, all_frames, fps=fps)
    except:
        raise Error("Could not create asset %s" % outfn)

##############################################################################
#                   WORKER
##############################################################################
def _worker(score_obj, predictor, input_queue, halt_event, weights):
    """ reads in the images and then computes the scores, this will
    be the target of the asynchronous threads """
    print 'Worker starting'
    while True:
        try:
            while True:
                if halt_event.is_set():
                    print 'Halt is set -- Worker terminating'
                    return
                try:
                    item = input_queue.get_nowait()
                    break
                except Exception, e:
                    time.sleep(1)
            scene, frameno, frame = item
            score, features, version = predictor.predict(frame)
            if weights is not None:
                score = pandas.Series(features).dot(weights)
            score_obj.update('valence', frameno, score)
            if score_obj.n_scored['valence'] % 100 == 0:
                print '%i images scored so far' % (score_obj.n_scored['valence'])
        except:
            print 'WORKER ENCOUNTERED ERROR!'
            halt_event.set()


##############################################################################
#                   OPTIMAL REGION FINDER
##############################################################################
def get_gif_regions(results_obj, num_gifs):
    """
    Returns the start point of all the gifs that you want. `results_obj` is
    a results structure in the style returned by the scoring object. This
    is done by a series of convolutions.
    """
    scorev = np.zeros(int(vlen))
    shotev = np.zeros(int(vlen))
    for idx in scene_list + [vlen - 1]:
        shotev[idx] = 1
    for _, start, stop, _, rscore, _ in results_obj:
        scorev[start:stop] = rscore
    clen = int(fps * target_length)  # clip length
    shotdist = int(fps * min_shot_distance)
    # the vector of convolution for scores
    convr_sc = np.ones(clen) / float(clen)  
    # the vector of convolution for shot boundaries
    convr_sb = np.zeros(clen)
    convr_sb[:shotdist] += 1
    convr_sb[-shotdist:] += 1
    reg_score = np.convolve(scorev, convr_sc, mode='valid')
    allowable = np.convolve(shotev, convr_sb, mode='valid')
    # now, it's a matter of selecting the best region here:
    allow_and_score = reg_score * (allowable == 0)
    gif_starts = []
    for i in range(n_to_pick):
        start = int((allow_and_score == np.max(allow_and_score)).nonzero()[0][0])
        gif_starts.append(start)
        # now, they can start before
        #   int(start - clen * (1 - max_overlap))
        # or after
        #   int(end - clen * max_overlap)
        disallow_region_start = int(start - (clen * (1 - max_overlap)))
        disallow_region_end = int(start + clen - (clen * max_overlap))
        allow_and_score[disallow_region_start:disallow_region_end] = 0
    return gif_starts



##############################################################################
#                   SET-UP
##############################################################################
def main():
    # open the video
    vid = cv2.VideoCapture(source)
    fps = vid.get(cv2.CAP_PROP_FPS)
    vlen = vid.get(cv2.CAP_PROP_FRAME_COUNT)
    vtime = vlen / fps  # length of video in seconds
    vlen = int(vlen)
    # extract the scenes
    scene_list = []
    detectors = [ContentDetector()]
    downscale_factor = 2
    print 'extracting scenes'
    tot_read, deltas = detect_scenes(vid, scene_list, detectors, 
                                     downscale_factor=downscale_factor)
    print 'establishing tunnel to aquila'
    #proc = establish_tunnel(db_ip=Conn().get_ip())
    conn = utils.autoscale.MultipleAutoScaleGroups(['AquilaOnDemandTest',
                                                    'AquilaSpotTest'])
    predictor = predictor.DeepnetPredictor(aquila_connection=conn)
    # proc = DummyProc()
    # predictor = DummyPredictor()
    print 'establishing RPC connection to aquila'
    predictor.connect()

    weights = None
    if target_weights is not None:
        weights_mat = pandas.read_pickle(weight_file)
        weights = weights_mat[target_weights]
        

    # re-open the video, just in case
    try:
        print 'Opening video'
        vid = cv2.VideoCapture(source)
        inQ = Queue(maxsize=100)

        halter = Event()

        score_obj = RegionScore(weights=weight_dict, 
                                regions=scene_list + [vlen-1], 
                                filters=region_filters)


        fe = Thread(target=frame_extractor, 
                    args=(vid, scene_list, inQ, halter))
        workers = [fe]
        for w in range(nworkers):
            workers.append(Thread(target=_worker, 
                                  args=(score_obj, predictor, inQ, halter,
                                        weights)))

        start = time.time()
        analysis_time = vtime * processing_time_ratio

        for w in workers:
            w.daemon = True
            w.start()

        while elapsed(start) < analysis_time:
            ela = elapsed(start)
            print '%.1fsec elapsed - %.1fsec remain' % (ela, analysis_time - ela)
            time.sleep(10)

        halter.set()

        for w in workers:
            w.join()

        print 'Valence scoring complete'


        # TODO: Update deltas

    finally:
        predictor.shutdown()
        #proc.terminate()

    # UPDATE THE DELTAS
    for frame, data in deltas.iteritems():
        if 'delta_hsv_avg' in data.keys():
            score_obj.update('action', frame, data['delta_hsv_avg'])

    r = score_obj.get_results()

    gif_starts = get_gif_regions(r, n_to_pick)

    clen = int(fps * target_length)
    for i, start in enumerate(gif_starts):
        end = start + clen
        vid = cv2.VideoCapture(source)
        frames = range(start, end)
        outfn = '/tmp/asset_%s_%s_%i.gif' % (src_n, target_weights, i)
        _asset_assemble(frames, vid, fps, outfn)



if __name__ == '__main__':
    utils.neon.InitNeon()
    main()



