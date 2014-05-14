'''
The core business logic and state tracking for mastermind.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import math
import cPickle as pickle
import tempfile
import threading
from utils import strutils

_log = logging.getLogger(__name__)

# Enum for different distribution types
class DistributionType:
    NEON = 0
    BRIGHTCOVE = 1
    YOUTUBE = 2
    OOYALA = 3

    @classmethod
    def fromString(cls, string):
        d = {'neon' : cls.NEON,
             'brightcove' : cls.BRIGHTCOVE,
             'youtube' : cls.YOUTUBE,
             'ooyala' : cls.OOYALA
             }
        return d[string.lower()]

class VideoInfo(object):
    '''Container to store information needed about each video.'''
    def __init__(self, testing_enabled, thumbnails=[]):
        self.thumbnails = thumbnails # [ThumbnailInfo]
        self.testing_enabled = testing_enabled # Is A/B testing enabled?

    def __str__(self):
        return strutils.full_object_str(self)
        

class ThumbnailInfo(object):
    '''Container to store information about a thumbnail.'''
    def __init__(self, _id, origin, rank, enabled, chosen, score,
                 loads=0, clicks=0):
        self.id = _id # thumbnail id
        self.origin = origin # who created thumb. "neon", "brightcove", etc...
        self.rank = rank # For the same origin, a rank of importance. 0 is best
        self.enabled = enabled # Can this thumbnail be shown?
        self.chosen = chosen # Is this thumbnail chosen to show as the primary.
        self.score = score # The model's prediction for this thumbnail
        self.loads = loads # no. of loads
        self.clicks = clicks # no. of clicks

    def __str__(self):
        return strutils.full_object_str(self)

    def update_stats(self, other_info):
        '''Updates the statistics from another ThumbnailInfo.'''
        if self.id <> other_info.id:
            _log.critical("Two thumbnail ids don't match. %s vs %s" %
                          (self.id, other_info.id))
            return self

        self.loads = other_info.loads
        self.clicks = other_info.clicks
        return self

    @staticmethod
    def from_db_data(data):
        '''Returns a ThumbnailInfo object from a database object.

        Inputs:
        data - A neondata.ThumbnailMetaData object
        '''
        return ThumbnailInfo(data.key,
                             data.type,
                             data.rank,
                             data.enabled,
                             data.chosen,
                             data.model_score)

class Mastermind(object):
    '''Class that defines the core logic of how much to show each thumbnail.

    All of the update_* functions return an updated serving directive
    that is in the form (video_id, [(thumb_id, fraction)]) if it has changed
    since last time. Otherwise None is returned.

    This object is thread safe, so only one thread is allow in at a time
    as long as you keep to the public interface.
    '''
    
    def __init__(self):
        self.video_info = {} # video_id -> VideoInfo
        self.last_serving_directive = {} # video -> (video_id, [(thumb_id, fraction)])

        # To record deltas so that we can replay recent ones when a
        # full stats db update arrives.
        self.delta_log = tempfile.TemporaryFile()

        # For thread safety
        self.lock = threading.RLock()

    def get_directives(self, video_ids):
        '''Returns a generator for the serving directives for all the video ids

        ***Warning*** The generator returned from this function cannot
           be shared between multiple threads.

        Inputs:
        video_ids - List of video_ids to query, or returns all of them if None

        Returns:

        Generator that spits out (video_id, [(thumb_id, fraction)])
        tuples.  It's thread safe as long as you keep the generator
        that's returned in a single thread.
        
        '''
        if video_ids is None:
            with self.lock:
                video_ids = self.video_info.keys()

        for video_id in video_ids:
            try:
                with self.lock:
                    directive = self._calculate_current_serving_directive(
                        self.video_info[video_id], video_id)
                yield (video_id, directive)
            except KeyError:
                # Some other thread changed the data so we don't have
                # information about this video id anymore. Oh well.
                pass

    def update_video_info(self, video_id, testing_enabled, thumbnails):
        '''Updates information about the video.

        Inputs:
        video_id - The video id to update
        testing_enable - True if testing should be enabled for this video
        thumbnails - List of ThumbnailInfo objects. Stats are ignored.

        Output:
        (video_id, [(thumb_id, fraction)]) if there was an actionable change,
        None otherwise
        '''
        with self.lock:
            try:
                video_info = self.video_info[video_id]
                # Update the statistics for all the thumbnails based
                # on our known state.
                for new_thumb in thumbnails:
                    for old_thumb in video_info.thumbnails:
                        if new_thumb.id == old_thumb.id:
                            new_thumb.update_stats(old_thumb)

                video_info.thumbnails = thumbnails
                video_info.testing_enabled = testing_enabled
                
            except KeyError:
                # No information about this video yet, so add it to the index
                video_info = VideoInfo(testing_enabled, thumbnails)
                self.video_info[video_id] = video_info

            return self._calculate_new_serving_directive(video_id)
            

    def update_thumbnail_info(self, video_id, thumb_id, enabled, chosen):
        '''Updates the information for a single thumbnail.

        Inputs:
        video_id - The video id for the thumbnail
        thumb_id - Thumbnail id to update
        enabled - True if the thumbnail should be possible to show
        chosen - True if the thumbnail is chosen explicitly as the primary.

        Output:
        (video_id, [(thumb_id, fraction)]) if there was an actionable change,
        None otherwise
        '''
        with self.lock:
            thumb = self._find_thumb(self, video_id, thumb_id)
            if thumb is None:
                return None
            thumb.enabled = enabled
            thumb.chosen = chosen

            return self._calculate_new_serving_directive(video_id)
                

    def update_stats_info(self, time, data):
        '''Update the stats info from a ground truth in the database.

        Inputs:
        time - Timestamp in seconds since epoch of when this stats info is
               current.
        data - generator that creates a stream of 
               (video_id, thumb_id, loads, clicks)

        Output:
        A generator that produces (video_id, [(thumb_id,
        fraction)]) for all changes. Note, there may be some extra
        changes that are unnecessary.
        
        '''
        with self.lock:
            changes = {} # video_id -> directive
            for video_id, thumb_id, loads, clicks in data:
                # Load up all the data
                thumb = self._find_thumb(video_id, thumb_id)
                if thumb is None:
                    continue
                thumb.loads = float(loads)
                thumb.clicks = float(clicks)

                new_directive = self._calculate_new_serving_directive(video_id)
                if new_directive is not None:
                    changes[video_id] = new_directive

            # Now replay all the deltas since the last db update

            # First step is to swap out the delta buffer
            old_delta_logs = self.delta_log
            self.delta_log = tempfile.TemporaryFile()
            old_delta_logs.flush()
            old_delta_logs.seek(0)

            # Now read through the old deltas and apply any that are
            # after the database timestamp
            try:
                while True:
                    delta = pickle.load(old_delta_logs)
                    if delta[0] > time:
                        new_directive = self.incorporate_delta_stats(*delta)
                        if new_directive is not None:
                            changes[video_id] = new_directive
            except EOFError:
                # Done reading the stream of pickles
                pass
        
            return changes.values()

    def incorporate_delta_stats(self, time, video_id, thumb_id, dloads,
                                dclicks):
        '''Take a delta in the statistics and add it to the state

        Inputs:
        video_id - The video id to update
        thumb_id - The thumbnail id to update
        loads - The number of loads of the thumbnail to add
        clicks - The number of clicks to add
        time - Timestamp in seconds since epoch when this delta was generated

        Output:
        (video_id, [(thumb_id, fraction)]) if there was an actionable change,
        None otherwise
        '''
        if dclicks < 0 or dloads < 0:
            raise ValueError('Invalid delta stat (%i, %i)' % (dloads, dclicks))
        
        # Grab the lock
        with self.lock:
            # Log the delta for replaying later
            pickle.dump((time, video_id, thumb_id, dloads, dclicks),
                        self.delta_log, pickle.HIGHEST_PROTOCOL)

            # Add the delta to the current state
            thumb = self._find_thumb(video_id, thumb_id)
            if thumb is None:
                return None

            thumb.loads += dloads
            thumb.clicks += dclicks
            
            return self._calculate_new_serving_directive(video_id)

    def _calculate_new_serving_directive(self, video_id):
        '''Decide the amount of time each thumb should show for each video.

        Inputs:
        video_id - Id for the video

        Outputs:
        (video_id, [(thumb_id, fraction)]) or None if no change
        '''
        try:
            video_info = self.video_info[video_id]
        except KeyError:
            _log.critical(
                'Could not find video_id %s. This should never happen' 
                % video_id)
            return None
        
        new_directive = self._calculate_current_serving_directive(
            video_info, video_id)
        try:
            if self.last_serving_directive[video_id] == new_directive:
                return None
            else:
                #Establish that there is only 1 thumb chosen (1,0,0)
                thumb_id, fraction = max(new_directive, key=lambda tup:tup[1])
                if fraction == 1.0:
                    _log.info("Only showing thumbnail %s for video id %s"
                        %(thumb_id, video_id))
        except KeyError:
            pass
            
        self.last_serving_directive[video_id] = new_directive
                
        return (video_id, new_directive)

    def _calculate_current_serving_directive(self, video_info, video_id=''):
        '''Decide the amount of time each thumb should show for each video.

        This is the guts of the business logic.

        Inputs:
        video_info - A VideoInfo object

        Outputs:
        [(thumb_id, fraction)] or None if we had an error
        '''
        # Find the default and chosen thumbnails
        default = None
        chosen = None
        neon_chosen = None
        run_frac = {} # thumb_id -> fraction
        chosen_flagged = False
        for thumb in video_info.thumbnails:
            run_frac[thumb.id] = 0.0
            if not thumb.enabled:
                continue
            
            if thumb.origin <> 'neon':
                if default is not None:
                    _log.error('There were multiple default thumbnails for '
                               'video: %s' % video_id)
                default = thumb
            
            if thumb.chosen:
                if chosen_flagged:
                    _log.error('There were multiple thumbnails chosen for '
                               'video id: %s' % video_id)
                chosen = thumb
                chosen_flagged = True

            if thumb.origin == 'neon':
                if neon_chosen is None or thumb.rank < neon_chosen.rank:
                    neon_chosen = thumb

        if default is None and chosen is None and neon_chosen is None:
            _log.error('Could not find any thumbnails to show for video %s'
                       % video_info)
            return []

        if default is None:
            _log.error('Could not find default thumbnail for video: %s' %
                       video_info)
            if chosen is None:
                run_frac[neon_chosen.id] = 1.0
            else:
                run_frac[chosen.id] = 1.0
        elif chosen is None:
            run_frac[default.id] = 1.0
        elif default.id == chosen.id or not video_info.testing_enabled:
            run_frac[chosen.id] = 1.0
        else:
            # We are A/B testing
            if self._chosen_thumb_bad(chosen, default):
                # The chosen thumbnail is bad for some reason, so show
                # the default.
                run_frac[default.id] = 1.0
            else:
                # Run the A/B test.  The default thumb is running the
                # equivalent of 10 min an hour because we're starting
                # primarily with brightcove.
                run_frac[default.id] = 0.15
                run_frac[chosen.id] = 0.85

        return run_frac.items()

    def _chosen_thumb_bad(self, chosen, default):
        '''Returns True if the chosen thumbnail is bad.

        We do a statistical significance test on the loads and clicks
        to see if the chosen thumbnail is worse than the default.
        '''
        if chosen.loads < 500 or default.loads < 500:
            return False

        if chosen.clicks < 1 or defaulcks < 1:
            return False
        
        p_chosen = float(chosen.clicks) / chosen.loads
        p_default = float(default.clicks) / default.loads

        se2_chosen = (p_chosen * (1-p_chosen)) / chosen.loads
        se2_default = (p_default * (1-p_default)) / default.loads

        zscore = (p_default - p_chosen) / math.sqrt(se2_chosen + se2_default)

        # Threshold on a 90% one way confidence interval
        return zscore > 1.65
            

    def _find_thumb(self, video_id, thumb_id):
        '''Find the thumbnail info object.

        Logging of key errors is handled here.

        Inputs:
        video_id - The video id
        thumb_id - The thumbnail id

        Outputs:
        The ThumbnailInfo object (that can be written to) or None if
        it's not there.
        
        '''
        try:
            for thumb in self.video_info[video_id].thumbnails:
                if thumb.id == thumb_id:
                    return thumb
        except KeyError:
            _log.warn('Could not find information for video %s' % video_id)
            return None
