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
from supportServices import neondata
import threading
from utils import strutils

_log = logging.getLogger(__name__)

class MastermindError(Error): pass
class UpdateError(MastermindError): pass

class VideoInfo(object):
    '''Container to store information needed about each video.'''
    def __init__(self, accout_id, testing_enabled, thumbnails=[]):
        self.account_id = account_id
        self.thumbnails = thumbnails # [ThumbnailInfo]
        self.testing_enabled = testing_enabled # Is A/B testing enabled?

    def __str__(self):
        return strutils.full_object_str(self)
        

class ThumbnailInfo(object):
    '''Container to store information about a thumbnail.'''
    def __init__(self, metadata, loads=0, views=0, clicks=0, plays=0):
        self.metadata = metadata # neondata.ThumbnailMetadata
        self.id = self.metadata.key # Thumbnail id
        self.loads = loads # no. of loads
        self.views = views # no. of views
        self.clicks = clicks # no. of clicks
        self.plays = plays # no. of video plays
        

    def __str__(self):
        return strutils.full_object_str(self)

    def update_stats(self, other_info):
        '''Updates the statistics from another ThumbnailInfo.'''
        if self.id <> other_info.id:
            _log.critical("Two thumbnail ids don't match. %s vs %s" %
                          (self.id, other_info.id))
            return self

        self.loads = other_info.loads
        self.views = other_info.views
        self.clicks = other_info.clicks
        self.plays = other_info.plays
        return self

class Mastermind(object):
    '''Class that defines the core logic of how much to show each thumbnail.

    All of the update_* functions return an updated serving directive
    that is in the form ((account_id, video_id), [(thumb_id, fraction)]) if it has changed
    since last time. Otherwise None is returned.

    This object is thread safe, so only one thread is allow in at a time
    as long as you keep to the public interface.
    '''
    
    def __init__(self):
        self.video_info = {} # video_id -> VideoInfo
        
        # account_id -> neondata.ExperimentStrategy
        self.experiment_strategy = {} 
        
        # video_id -> ((account_id, video_id), [(thumb_id, fraction)])
        self.serving_directive = {} 
        
        # For thread safety
        self.lock = threading.RLock()

    def get_directives(self, video_ids):
        '''Returns a generator for the serving directives for all the video ids

        ***Warning*** The generator returned from this function cannot
           be shared between multiple threads.

        Inputs:
        video_ids - List of video_ids to query, or returns all of them if None

        Returns:

        Generator that spits out ((account_id, video_id), 
                                  [(thumb_id, fraction)])
        tuples.  It's thread safe as long as you keep the generator
        that's returned in a single thread.
        
        '''
        if video_ids is None:
            with self.lock:
                video_ids = self.video_info.keys()

        for video_id in video_ids:
            try:
                with self.lock:
                    directive = self.serving_directive[video_id]
                yield directive
            except KeyError:
                # Some other thread changed the data so we don't have
                # information about this video id anymore. Oh well.
                pass

    def update_video_info(self, video_metadata, thumbnails,
                          testing_enabled=True):
        '''Updates information about the video.

        Inputs:
        video_metadata - neondata.VideoMetadata object
        testing_enable - True if testing should be enabled for this video
        thumbnails - List of ThumbnailMetadata objects. Stats are ignored.
        '''
        video_id = video_metadata.get_id()
        testing_enabled = testing_enabled and video_metadata.testing_enabled
        thumbnail_infos = [ThumbnailInfo(x) for x in thumbnails]
        with self.lock:
            try:
                video_info = self.video_info[video_id]
                
                # Update the statistics for all the thumbnails based
                # on our known state.
                for new_thumb in thumbnail_infos:
                    for old_thumb in video_info.thumbnails:
                        if new_thumb.id == old_thumb.id:
                            new_thumb.update_stats(old_thumb)

                video_info.thumbnails = thumbnail_infos
                video_info.testing_enabled = testing_enabled
                if video_metadata.get_account_id() != video_info.account_id:
                    _log.error(('The account has changed for video if %s, '
                                'account id %s and it should not have') % 
                                (video_id, video_metadata.get_account_id()))
                    video_info.account_id = video_metadata.get_account_id()
                
            except KeyError:
                # No information about this video yet, so add it to the index
                video_info = VideoInfo(
                    video_metadata.get_account_id(),
                    testing_enabled,
                    thumbnail_infos)
                self.video_info[video_id] = video_info

            self._calculate_new_serving_directive(video_id)
                

    def update_stats_info(self, data):
        '''Update the stats info from a ground truth in the database.

        Inputs:
        data - generator that creates a stream of 
               (video_id, thumb_id, loads, views, clicks, plays)
        
        '''
        with self.lock:
            for video_id, thumb_id, loads, clicks in data:
                # Load up all the data
                thumb = self._find_thumb(video_id, thumb_id)
                if thumb is None:
                    continue
                thumb.loads = float(loads)
                thumb.views = float(views)
                thumb.clicks = float(clicks)
                thumb.plays = float(plays)

                self._calculate_new_serving_directive(video_id)

    def update_experiment_strategy(self, account_id, strategy):
        '''Updates the experiment strategy for a given account.

        Inputs:
        account_id - Account id that may be updated
        strategy - A neondata.ExperimentStrategy object
        '''
        with self.lock:
            try:
                old_strategy = self.experiment_strategy[account_id]
                if old_strategy == strategy:
                    # No change in strategy so we're done
                    return
            except KeyError:
                pass

            self.experiment_strategy[account_id] = strategy

            # Now update all the serving directives
            for video_info in self.video_info.values():
                if video_info.account_id == account_id:
                    self._calculate_new_serving_directive(video_id)

    def _calculate_new_serving_directive(self, video_id):
        '''Decide the amount of time each thumb should show for each video.

        Updates the self.serving_directive entries if it changes

        Inputs:
        video_id - Id for the video
        '''
        try:
            video_info = self.video_info[video_id]
        except KeyError:
            _log.critical(
                'Could not find video_id %s. This should never happen' 
                % video_id)
            return
        
        new_directive = self._calculate_current_serving_directive(
            video_info, video_id)
            
        self.serving_directive[video_id] = ((account_id, video_id),
                                            new_directive)

    def _calculate_current_serving_directive(self, video_info, video_id=''):
        '''Decide the amount of time each thumb should show for each video.

        This is the guts of the business logic.

        Inputs:
        video_info - A VideoInfo object

        Outputs:
        [(thumb_id, fraction)] or None if we had an error
        '''
        try:
            strategy = self.experiment_strategy[video_info.account_id]
        except KeyError:
            _log.error(('Could not find the experimental strategy for account'
                        ' %s' % video_info.account_id))
            return None
            
        # Find the different types of thumbnails. We are looking for:
        # - The baseline
        # - The editor's selected thumbnails
        # - All the Neon thumbnails
        baseline = None
        editor = None
        chosen = None
        default = None
        neon_thumbs = []
        run_frac = {} # thumb_id -> fraction
        for thumb in video_info.thumbnails:
            run_frac[thumb.id] = 0.0
            if not thumb.metadata.enabled:
                continue

            # A neon thumbnail
            if thumb.metadata.type == neondata.ThumbnailType.NEON:
                neon_thumbs.append(thumb)

            # A thumbnail that was explicitly chosen in the database
            if thumb.metadata.chosen:
                if chosen is not None:
                    _log.warn(('More than one chosen thumbnail for video %s '
                               '. Choosing the one of lowest rank' % video_id))
                    if thumb.metadata.rank < chosen.metadata.rank:
                        chosen = thumb
                else:
                    chosen = thumb

            # This is identified as the baseline thumbnail
            if thumb.metadata.type == strategy.baseline_type:
                if (baseline is None or 
                    thumb.metadata.rank < baseline.metadata.rank):
                    baseline = thumb

            # A default thumbnail that comes from a partner source or
            # is explicitly set in the API
            if thumb.metadata.type not in [neondata.ThumbnailType.NEON,
                                           neondata.ThumbnailType.CENTERFRAME,
                                           neondata.ThumbnailType.RANDOM,
                                           neondata.ThumbnailType.FILTERED]:
                if (default is None or 
                    default.metadata.type == neondata.ThumbnailType.DEFAULT 
                    or thumb.metadata.rank < default.metadata.rank)):
                    default = thumb

        if strategy.chosen_thumb_overrides and chosen is not None:
            run_frac[chosen.id] = 1.0
            return run_frame.items()
        editor = chosen or default
        if (editor is not None and 
            baseline is not None and 
            hamming_dist(editor.metadata.phash, default.metadata.phash) < 10):
            # The editor thumbnail looks exactly like the baseline one
            # so ignore the editor one.
            editor = None
                    
            
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

        if chosen.clicks < 1 or default.clicks < 1:
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
