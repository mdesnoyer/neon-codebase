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

from cmsdb import neondata
import concurrent.futures
import copy
import logging
import math
import multiprocessing.pool
import numpy as np
import pandas
import scipy as sp
import scipy.stats as spstats
import threading
import utils.dists
from utils.options import define, options
from utils import statemon
from utils import strutils

from collections import defaultdict as ddict

_log = logging.getLogger(__name__)

define('modify_pool_size', type=int, default=5,
       help='Number of processes that can modify the db simultaneously')

statemon.define('n_directives', int)
statemon.define('directive_changes', int)
statemon.define('pending_modifies', int)
statemon.define('no_experiment_strategy', int)
statemon.define('invalid_experiment_type', int) 
statemon.define('db_update_error', int) # error updating database
statemon.define('no_valid_thumbnails', int) # no valid thumbnails for a video
statemon.define('critical_error', int)

class MastermindError(Exception): pass
class UpdateError(MastermindError): pass

class ScoreType(object):
    '''
    Exports some variable names that map to integers
    to refer to the different scoring types.
    '''
    RANK_CENTRALITY = 2
    CLASSICAL = 1
    UNKNOWN = 0
    DEFAULT = RANK_CENTRALITY

class ModelMapper(object):
    '''
    Stores relevant information about the model used for a given
    video, particularly with respect to computing the score
    priors. This was made necessary by the move to using Rank
    Centrality and, more broadly, no longer binning scores into
    [1, 7]. 

    Note: This makes several simplifying assumptions. Namely, 
    if a model is not in `classical models,' then it is using
    Rank Centrality, and is assumed to have a mean of 1. Generally
    when Rank Centrality is estimated the sum of the vector is 1--
    since the vector corresponds to transition probabilities. But
    since we wanted to the mean to not be dependent on the number
    of tested items, the sum of the vector corresponds to the number
    of training items, and hence the mean is definitionally one.

    IF YOU PLAN ON MAKING MORE MODELS USING THE CLASSICAL SCORING 
    METHOD, YOU MUST ADD THEM TO CLASSICAL_MODELS.
    '''
    MODEL2TYPE = ddict(lambda: ScoreType.DEFAULT,
        {x:ScoreType.CLASSICAL for x in ['20130924_textdiff',
        '20130924_crossfade','p_20150722_withCEC_w20',
        '20130924_crossfade_withalg','p_20150722_withCEC_w40',
        '20150206_flickr_slow_memcache','20130924',
        'p_20150722_withCEC_w10','p_20150722_withCEC_wA',
        'p_20150722_withCEC_wNone']})
    MODEL2TYPE[None] = ScoreType.UNKNOWN # reserved for unknown models

    @classmethod
    def _add_model(cls, modelid, score_type=ScoreType.DEFAULT):
        if ((score_type != ScoreType.RANK_CENTRALITY) and
            (score_type != ScoreType.CLASSICAL) and
            (score_type != ScoreType.UNKNOWN)):
            _log.error('Invalid score type specification for model '
                       '%s defaulting to UNKNOWN'%(modelid))
            score_type = ScoreType.UNKNOWN
            # if it's already in there, do not attempt to change
            if ModelMapper.MODEL2TYPE.has_key(modelid):
                _log.error('Model %s with invalid score type %s'
                    ' is already in MODEL2TYPE, original score '
                    'type remains'%(str(modelid), str(score_type)))
                return
        if not ModelMapper.MODEL2TYPE.has_key(modelid):
            _log.info('Model %s is not in model dicts; adding it,'
                  ' as score type %s'%(str(modelid), str(score_type)))
        cls.MODEL2TYPE[modelid] = score_type

    @classmethod
    def get_model_type(cls, modelid):
        '''
        Returns the model type given either a model
        number or a model name.
        '''
        if not ModelMapper.MODEL2TYPE.has_key(modelid):
            ModelMapper._add_model(modelid)
        return ModelMapper.MODEL2TYPE[modelid]

class VideoInfo(object):
    '''Container to store information needed about each video.'''
    def __init__(self, account_id, testing_enabled, thumbnails=[],
                 score_type=ScoreType.UNKNOWN):
        self.account_id = str(account_id)
        self.thumbnails = thumbnails # [ThumbnailInfo]
        self.testing_enabled = testing_enabled # Is A/B testing enabled?
        self.score_type = score_type

    def __str__(self):
        return strutils.full_object_str(self)

    def __repr__(self):
        return str(self)

    def __cmp__(self, other):
        if other is None:
            return 1
        return cmp(self.__dict__, other.__dict__)
        

class ThumbnailInfo(object):
    '''Container to store information about a thumbnail.'''
    __slots__ = [
        'enabled',
        'type',
        'chosen',
        'rank',
        'phash',
        'model_score',
        'id',
        'base_imp',
        'incr_imp',
        'base_conv',
        'incr_conv',
        '__weakref__'
        ]
        
    def __init__(self, metadata, base_impressions=0, incremental_impressions=0,
                 base_conversions=0, incremental_conversions=0):
        # These entries are from the neondata.ThumbnailMetadata
        self.enabled = metadata.enabled
        self.type = str(metadata.type)
        self.chosen = metadata.chosen
        self.rank = None if metadata.rank is None else int(metadata.rank)
        self.phash = metadata.phash
        self.model_score = None if metadata.model_score is None \
          else float(metadata.model_score)

        # Last chunk of the thumbnail id
        self.id = str(metadata.key).split('_')
        if len(self.id) != 3:
            _log.error('The thumbnail id %s does not seem to be valid for '
                       'video %s' % (metadata.key, metadata.video_id))
            self.id = str(metadata.key)
        else:
            self.id = self.id[2]

        # These fields are from the statistics
        self.base_imp = base_impressions
        self.incr_imp = incremental_impressions
        self.base_conv = base_conversions
        self.incr_conv = incremental_conversions

    def __str__(self):
        return str({
            'enabled': self.enabled,
            'type': self.type,
            'chosen' : self.chosen,
            'rank' : self.rank,
            'phash': self.phash,
            'model_score' : self.model_score,
            'id': self.id,
            'base_imp': self.base_imp,
            'incr_imp': self.incr_imp,
            'base_conv': self.base_conv,
            'incr_conv': self.incr_conv})

    def __repr__(self):
        return str(self)

    def __cmp__(self, other):
        if other is None:
            return 1
        return cmp((self.enabled,
                    self.type,
                    self.chosen,
                    self.rank,
                    self.phash,
                    self.model_score,
                    self.id,
                    self.get_impressions(),
                    self.get_conversions()),
                   (other.enabled,
                    other.type,
                    other.chosen,
                    other.rank,
                    other.phash,
                    other.model_score,
                    other.id,
                    other.get_impressions(),
                    other.get_conversions()))

    def get_impressions(self):
        return self.base_imp + self.incr_imp

    def get_conversions(self):
        return self.base_conv + self.incr_conv

    def update_stats(self, other_info):
        '''Updates the statistics from another ThumbnailInfo.'''
        if self.id <> other_info.id:
            _log.error("Two thumbnail ids don't match. %s vs %s" %
                       (self.id, other_info.id))
            return self

        self.base_imp = other_info.base_imp
        self.incr_imp = other_info.incr_imp
        self.base_conv = other_info.base_conv
        self.incr_conv = other_info.incr_conv
        return self

class Mastermind(object):
    '''Class that defines the core logic of how much to show each thumbnail.

    All of the update_* functions return an updated serving directive
    that is in the form ((account_id, video_id), [(thumb_id, fraction)]) if it has changed
    since last time. Otherwise None is returned.

    This object is thread safe, so only one thread is allow in at a time
    as long as you keep to the public interface.
    '''
    PRIOR_IMPRESSION_SIZE = 10
    PRIOR_CTR = 0.1
    VALUE_THRESHOLD = 0.01
    
    def __init__(self):
        self.video_info = {} # video_id -> VideoInfo
        
        # account_id -> neondata.ExperimentStrategy
        self.experiment_strategy = {}
        
        # video_id -> ((account_id, video_id), [(thumb_id, fraction)])
        self.serving_directive = {}

        # video_id -> neondata.ExperimentState
        self.experiment_state = {}
        
        # For thread safety
        self.lock = multiprocessing.RLock()
        self.modify_waiter = multiprocessing.Condition()

        # Counter for the number of pending modify calls to the database
        self.pending_modifies = multiprocessing.Value('i', 0)
        statemon.state.pending_modifies = 0

        # A thread pool to modify entries in the database
        self.modify_pool = concurrent.futures.ThreadPoolExecutor(
            options.modify_pool_size)

    def _incr_pending_modify(self, count):
        '''Safely increment the number of pending modifies by count.'''
                
        with self.lock:
            self.pending_modifies.value += count

        with self.modify_waiter:
            self.modify_waiter.notify_all()
        statemon.state.pending_modifies = self.pending_modifies.value

    def wait_for_pending_modifies(self):
        with self.modify_waiter:
            while self.pending_modifies.value > 0:
                self.modify_waiter.wait()

    def get_directives(self, video_ids=None):
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
                keys = self.serving_directive.keys()
        else:
            keys = video_ids

        for key in keys:
            try:
                directive = self.serving_directive[key]
                video_id = directive[0][1]
                yield (directive[0],
                       [('_'.join([video_id, thumb_id]), frac)
                        for thumb_id, frac in directive[1]])
            except KeyError:
                # Some other thread probably changed the data so we
                # don't have information about this video id
                # anymore. Oh well.
                pass

    def _thumbnail_status_to_directive(self, thumbnail_status):
        '''Convert thubmnail_status to serving directive

        Inputs:
        thumbnails_status - ThumbnailStatus

        Returns:
        tuple (video_id, (thumbnail_id, directive)) video_id is 
        extracted from thumbnail_status. directive is the serving fraction
        '''

        # An example of ThumbnailStatus id: thumbnailstatus_acct1_vid1_v1t2
        # It contains four parts: thumbnails status, account id, video id
        # and thumbnail id.
        ids = str(thumbnail_status.get_id()).split('_')
        if len(ids) != 3:
            _log.error('The thumbnail_status id %s does not seem to be valid.' %
                thumbnail_status.get_id())
            return (None, None)
        else:
            video_id = ('_').join([ids[0], ids[1]])
            thumbnail_partial_id = ids[2]
            if (thumbnail_status is None or 
                thumbnail_status.serving_frac is None or 
                thumbnail_status.serving_frac == ''):
                return (video_id, None)
            return (video_id,
                    (thumbnail_partial_id,
                     float(thumbnail_status.serving_frac)))

    def update_experiment_state_directive(self, video_id, video_status,
                                          thumbnail_status_list):
        '''Add a video experiment state to the experiment_state

        Inputs:
        video_id - video_id to be updated
        video_status: neondata.VideoStatus object
        thumbnail_status_list: list of thumbnail status objects

        '''
        if video_id is not None and len(thumbnail_status_list) > 0:
            with self.lock:
                has_error = False
                self.experiment_state[video_id] = video_status.experiment_state
                directive_list = []
                frac_sum = 0.0
                for thumbnail_status in thumbnail_status_list:
                    if thumbnail_status is None:
                        continue
                    t_video_id, directive = \
                        self._thumbnail_status_to_directive(thumbnail_status)
                    if t_video_id is None or directive is None:
                        has_error = True;
                        break
                    if t_video_id != video_id:
                        _log.error('ThumbnailStatus video id %s does not match'
                                   ' the query video id %s' % (t_video_id,
                                   video_id))
                        has_error = True
                        break
                    frac_sum = frac_sum + directive[1]
                    directive_list.append(directive)
                if not has_error:
                    if abs(frac_sum - 1.0) >= 0.001:
                        has_error = True
                        _log.error('ThumbnailStatus of video id %s does not'
                                   ' sum to 1.0' % video_id)
                    # Validate the summation.

                if has_error:
                    self.experiment_state[video_id] = \
                        neondata.ExperimentState.UNKNOWN
                else:
                    # No Error so set the serving directive
                    account_id = video_id.split('_')[0]
                    self.serving_directive[video_id] = \
                        ((account_id, video_id), directive_list)



    def update_video_info(self, video_metadata, thumbnails,
                          testing_enabled=True):
        '''Updates information about the video.

        Inputs:
        video_metadata - neondata.VideoMetadata object
        testing_enable - True if testing should be enabled for this video
        thumbnails - List of ThumbnailMetadata objects. Stats are ignored.
        '''
        video_id = str(video_metadata.get_id())
        testing_enabled = testing_enabled and video_metadata.testing_enabled
        thumbnail_infos = [ThumbnailInfo(x) for x in thumbnails]
        new_video_info = VideoInfo(
            video_metadata.get_account_id(),
            testing_enabled,
            thumbnail_infos,
            score_type=ModelMapper.get_model_type(
                video_metadata.model_version))
        old_video_info = None
        
        with self.lock:
            try:
                old_video_info = self.video_info[video_id]
                
                # Update the statistics for all the thumbnails based
                # on our known state.
                # Also, we will find out if there are new thumbnails added,
                # this can happen if editor add new thumbnails.
                added_thumbnail_infos = []
                for new_thumb in thumbnail_infos:
                    is_exist = False
                    for old_thumb in old_video_info.thumbnails:
                        if new_thumb.id == old_thumb.id:
                            new_thumb.update_stats(old_thumb)
                            is_exist = True
                    if not is_exist:
                        added_thumbnail_infos.append(new_thumb)

                if video_metadata.get_account_id() != old_video_info.account_id:
                    _log.error(('The account has changed for video id %s, '
                                'account id %s and it should not have') % 
                                (video_id, video_metadata.get_account_id()))

                # If the video experiment ended, but there are new editor
                # thumbnails added, we will restart the experiment again.
                # TODO: validate if there is only one chosen?
                if self.experiment_state.get(video_id, None) == \
                    neondata.ExperimentState.COMPLETE:
                    for thumb in added_thumbnail_infos:
                        if thumb.type not in [
                                neondata.ThumbnailType.CENTERFRAME,
                                neondata.ThumbnailType.RANDOM,
                                neondata.ThumbnailType.FILTERED]:
                            self.experiment_state[video_id] = \
                                neondata.ExperimentState.RUNNING
                            break

            except KeyError:
                # No information about this video yet, so add it to the index
                pass
            
            self.video_info[video_id] = new_video_info

            if old_video_info != new_video_info:
                self._calculate_new_serving_directive(video_id)

    def remove_video_info(self, video_id):
        '''Removes the video from being managed.'''
        with self.lock:
            if video_id in self.video_info:
                del self.video_info[video_id]
            if video_id in self.experiment_state:
                del self.experiment_state[video_id]
            if video_id in self.serving_directive:
                del self.serving_directive[video_id]
                self._incr_pending_modify(1)
                self.modify_pool.submit(
                    _modify_video_info,
                    self, video_id, neondata.ExperimentState.DISABLED, None,
                    None)

    def is_serving_video(self, video_id):
        '''Returns true if the video is being managed.'''
        if neondata.InternalVideoID.is_no_video(video_id):
            return True
        if video_id is None:
            return False
        return video_id in self.serving_directive
                

    def update_stats_info(self, data):
        '''Update the stats info from a ground truth in the database.

        Any of the stats entries can be None, in which case they are
        not updated.

        Inputs:
        data - generator that creates a stream of tuples grouped by video id
               (video_id, thumb_id, base_impr, incr_impr, base_conv, incr_conv)
        
        '''
        last_video_id = None
        last_vid_change = False
        for video_id, thumb_id, base_imp, incr_imp, base_conv, incr_conv in \
          data:
            with self.lock:
                did_change = False
                # Load up all the data
                thumb = self._find_thumb(video_id, thumb_id)
                if thumb is None:
                    continue
                if base_imp is not None and thumb.base_imp != float(base_imp):
                    thumb.base_imp = float(base_imp)
                    did_change = True
                if incr_imp is not None and thumb.incr_imp != float(incr_imp):
                    thumb.incr_imp = float(incr_imp)
                    did_change = True
                if (base_conv is not None and 
                    thumb.base_conv != float(base_conv)):
                    thumb.base_conv = float(base_conv)
                    did_change = True
                if (incr_conv is not None and 
                    thumb.incr_conv != float(incr_conv)):
                    thumb.incr_conv = float(incr_conv)
                    did_change = True

                if video_id != last_video_id:
                    if last_vid_change and last_video_id is not None:
                        self._calculate_new_serving_directive(last_video_id)
                    
                    last_video_id = video_id
                    last_vid_change = did_change
                last_vid_change = last_vid_change or did_change
                
        if last_video_id is not None and last_vid_change:
            with self.lock:
                self._calculate_new_serving_directive(last_video_id)

    def update_experiment_strategy(self, account_id, strategy):
        '''Updates the experiment strategy for a given account.

        Inputs:
        account_id - Account id that may be updated
        strategy - A neondata.ExperimentStrategy object
        '''
        if strategy is None or account_id is None:
            _log.error('Invalid account id %s and strategy: %s' %
                       (account_id, strategy))
            return

        # Clean up data types in the experiment strategy
        try:
            strategy.exp_frac = float(strategy.exp_frac)
            strategy.holdback_frac = float(strategy.holdback_frac)
            strategy.frac_adjust_rate = float(strategy.frac_adjust_rate)
            strategy.min_conversion = int(strategy.min_conversion)
            strategy.max_neon_thumbs = (None if strategy.max_neon_thumbs 
                                        is None else 
                                        int(strategy.max_neon_thumbs))
        except ValueError as e:
            _log.error('Invalid entry in experiment strategy %s' %
                       strategy.get_id())
            return
        
        with self.lock:
            try:
                old_strategy = self.experiment_strategy[account_id]
                if old_strategy == strategy:
                    # No change in strategy so we're done
                    return
            except KeyError:
                pass

            self.experiment_strategy[account_id] = strategy

            _log.info_n(('The experiment strategy has changed for account %s. '
                         'Building new serving directives.') % account_id,
                100)

            # Now update all the serving directives
            for video_id, video_info in self.video_info.items():
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
            statemon.state.increment('critical_error') 
            return
        
        # if video has already finished the experiment, just keep the
        # previous directive.
        if self.experiment_state.get(video_id, None) == \
           neondata.ExperimentState.COMPLETE:
            return

        result = self._calculate_current_serving_directive(
            video_info, video_id)
        if result is None:
            # There was an error, so stop here
            return 

        experiment_state, new_directive, value_left, winner_tid = result
        self.experiment_state[video_id] = experiment_state

        self._modify_video_state(video_id, experiment_state, value_left,
                                 winner_tid, new_directive, video_info)
        
        self.serving_directive[video_id] = ((video_info.account_id,
                                             video_id),
                                             new_directive.items())
        statemon.state.n_directives = len(self.serving_directive)
        statemon.state.increment('directive_changes')

    def _calculate_current_serving_directive(self, video_info, video_id=''):
        '''Decide the amount of time each thumb should show for each video.

        This is the guts of the business logic.

        Inputs:
        video_info - A VideoInfo object

        Outputs:
        (experiment_state, {thumb_id => fraction}, value_left, winner_tid) or 
        None if we had an error
        '''
        if len(video_info.thumbnails) == 0:
            # There's no valid thumb yet to show. That's ok. We just ignore this video
            return None
        
        try:
            strategy = self.experiment_strategy[video_info.account_id]
        except KeyError:
            _log.error(('Could not find the experimental strategy for account'
                        ' %s' % video_info.account_id))
            statemon.state.increment('no_experiment_strategy')
            return None
            
        # Find the different types of thumbnails. We are looking for:
        # - The baseline
        # - The editor's selected thumbnails
        # - All the candidate thumbnails
        baseline = None
        editor = None
        chosen = None
        default = None
        experiment_state = neondata.ExperimentState.UNKNOWN
        winner = None
        value_left=None
        candidates = set()
        run_frac = {} # thumb_id -> fraction
        for thumb in video_info.thumbnails:
            run_frac[thumb.id] = 0.0
            if not thumb.enabled:
                continue

            # A neon thumbnail
            if thumb.type in [neondata.ThumbnailType.NEON,
                              neondata.ThumbnailType.CUSTOMUPLOAD]:
                candidates.add(thumb)

            # A thumbnail that was explicitly chosen in the database
            if thumb.chosen:
                if chosen is not None:
                    _log.warn(('More than one chosen thumbnail for video %s '
                               '. Choosing the one of lowest rank' % video_id))
                    if thumb.rank < chosen.rank:
                        chosen = thumb
                else:
                    chosen = thumb

            # This is identified as the baseline thumbnail
            if thumb.type == strategy.baseline_type:
                if (baseline is None or 
                    thumb.rank < baseline.rank):
                    baseline = thumb

            # A default thumbnail that comes from a partner source or
            # is explicitly set in the API
            if thumb.type not in [neondata.ThumbnailType.NEON,
                                  neondata.ThumbnailType.CENTERFRAME,
                                  neondata.ThumbnailType.RANDOM,
                                  neondata.ThumbnailType.FILTERED]:
                if (default is None or 
                    (thumb.type == neondata.ThumbnailType.DEFAULT 
                     and thumb.rank < default.rank)):
                    default = thumb

        if strategy.chosen_thumb_overrides and chosen is not None:
            run_frac[chosen.id] = 1.0
            experiment_state = neondata.ExperimentState.OVERRIDE
            return (experiment_state, run_frac, None, None)
        editor = chosen or default
        if editor:
            candidates.discard(editor)

        # If the editor thumbnail looks exactly like the baseline
        # one, ignore the editor one.
        if (editor is not None and 
            baseline is not None and 
            utils.dists.hamming_int(editor.phash,
                                    baseline.phash) < 10):
            editor = None

        if editor is None and baseline is None:
            if len(candidates) == 0:
                _log.error("No valid thumbnails for video %s" % video_id)
                statemon.state.increment('no_valid_thumbnails')
                return None
            
            _log.warn_n('Could not find a baseline for video id: %s' %
                        video_id)
            if not video_info.testing_enabled:
                _log.error_n(
                    'Testing was disabled and there was no baseline for'
                    ' video %s' % video_id, 5)
                statemon.state.increment('no_valid_thumbnails')
                return None

        # Limit the number of Neon thumbnails being shown
        if strategy.max_neon_thumbs is not None:
            neon_thumbs = [thumb for thumb in candidates if 
                           thumb.type == neondata.ThumbnailType.NEON]
            neon_thumbs = sorted(
                neon_thumbs,
                key=lambda x: (x.rank, -x.model_score if x.model_score else 
                               float('inf')))
            if len(neon_thumbs) > strategy.max_neon_thumbs:
                candidates = candidates.difference(
                    neon_thumbs[strategy.max_neon_thumbs:])

        # Done finding all the thumbnail types, so start doing the allocations 
        if not video_info.testing_enabled:
            if editor is None:
                run_frac[baseline.id] = 1.0
            else:
                run_frac[editor.id] = 1.0
            experiment_state = neondata.ExperimentState.DISABLED

        elif strategy.only_exp_if_chosen and chosen is None:
            # We aren't experimenting because no thumb was chosen
            if default:
                run_frac[default.id] = 1.0
            elif baseline:
                run_frac[baseline.id] = 1.0
            else:
                _log.warn("Could not find the default thumbnail to show for "
                          "video %s. Trying a Neon one instead." % video_id)
                ranked_candidates = sorted(
                    candidates, key=lambda x: (x.rank,
                                               x.type != 
                                               neondata.ThumbnailType.NEON))
                run_frac[ranked_candidates[0].id] = 1.0
            experiment_state = neondata.ExperimentState.DISABLED
        else:
            # First allocate the non-experiment portion
            non_exp_thumb = None
            experiment_frac = float(strategy.exp_frac)
            if experiment_frac >= 1.0:
                experiment_frac = 1.0
                # When the experimental fraction is 100%, put everything
                # into the candidates that makes sense.
                if editor is not None:
                    candidates.add(editor)
                    if baseline and strategy.always_show_baseline:
                        candidates.add(baseline)
                elif baseline is not None:
                    candidates.add(baseline)
            else:
                if editor is None:
                    if baseline:
                        run_frac[baseline.id] = 1.0 - experiment_frac
                        non_exp_thumb = baseline
                    else:
                        # There is nothing to run in the main fraction, so
                        # run the experiment over everything.
                        experiment_frac = 1.0
                else:
                    run_frac[editor.id] = 1.0 - experiment_frac
                    non_exp_thumb = editor
                    if baseline and strategy.always_show_baseline:
                        candidates.add(baseline)

            # Now run the chosen strategy
            if (strategy.experiment_type == 
                neondata.ExperimentStrategy.MULTIARMED_BANDIT):
                experiment_state, exp_frac, value_left, winner = \
                  self._get_bandit_fracs(strategy,
                                         candidates,
                                         video_info,
                                         non_exp_thumb)
            elif (strategy.experiment_type == 
                neondata.ExperimentStrategy.SEQUENTIAL):
                experiment_state, exp_frac, value_left, winner = \
                  self._get_sequential_fracs(strategy, candidates, video_info,
                                             non_exp_thumb, editor or baseline,
                                             video_id)
            else:
                _log.error('Invalid experiment type for video %s : %s' % 
                           (video_id, strategy.experiment_type))
                statemon.state.increment('invalid_experiment_type')
                return None

            if winner is None:
                # Normalize the serving percentages
                sum_fracs = sum(exp_frac.itervalues())
                for tid in exp_frac.iterkeys():
                    exp_frac[tid] *= experiment_frac / sum_fracs 
                run_frac.update(exp_frac)
            else:
                # The experiment is done
                run_frac = dict((thumb.id, 0.0) 
                                for thumb in video_info.thumbnails)
                run_frac.update(self._get_experiment_done_fracs(strategy,
                                                                baseline,
                                                                editor,
                                                                winner))
            
        return (experiment_state, run_frac, value_left,
                (winner.id if winner else None))

    def _get_bandit_fracs(self, strategy, candidates, video_info,
                          non_exp_thumb):
        '''Gets the serving fractions for a multi-armed bandit strategy.

        This uses the Thompson Sampling heuristic solution. See
        https://support.google.com/analytics/answer/2844870?hl=en for
        more details.

        Inputs:
        strategy - The Experiment strategy object to run
        baseline - The baseline ThumbnailInfo object
        editor - The editor ThumbnailInfo object
        candidates - A set of thumbnail candidates to experiment with
        video_info - A VideoInfo object for this video
        non_exp_thumb - A ThumbnailInfo object that is pegged to a specific
                        serving percentage and thus not being experimented
                        with.

        Returns tuple containing:
        experiment_state - The new state of the experiment
        run_frac - Dictionary of thumb_id -> serving percentage for the 
                   candidates.
        value_left - The value left in the experiment
        winner - Thumbnail that is the winner if there is one now
        
        '''
        run_frac = {}
        valid_bandits = list(copy.copy(candidates))
        experiment_state = neondata.ExperimentState.RUNNING
        value_remaining = None
        winner = None

        if non_exp_thumb is not None:
            valid_bandits.append(non_exp_thumb)

        # Now determine the conversions and impressions for each thumb
        # based on a prior of its model score and its measured ctr.
        bandit_ids = [x.id for x in valid_bandits]
        conv = dict([(x.id, self._get_prior_conversions(x, video_info) +
                      x.get_conversions())
                for x in valid_bandits])
        imp = dict([(x.id, Mastermind.PRIOR_IMPRESSION_SIZE * 
                             (1 - Mastermind.PRIOR_CTR) + 
                             x.get_impressions())
                             for x in valid_bandits])

        # Run the monte carlo series
        MC_SAMPLES = 1000.

        # Change: the formula in the paper is conversions+1,
        #         impressions-conversions+1
        mc_series = [spstats.beta.rvs(conv[x] + 1,
                                      max(imp[x] - conv[x], 0) + 1,
                                      size=MC_SAMPLES) for x in bandit_ids]

        win_frac = np.array(np.bincount(np.argmax(mc_series, axis=0),
                                        minlength=len(mc_series)),
                            dtype=np.float) / MC_SAMPLES
        winner_idx = np.argmax(win_frac)

        # Determine the number of real impressions for each entry in win_frac
        impressions = [x.get_impressions() for x in valid_bandits]

        # Calculate the summation of all conversions.
        total_conversions = sum(x.get_conversions() for x in valid_bandits)

        # Determine the value remaining. This is equivalent to
        # determing that one of the other arms might beat the winner
        # by x%
        lost_value = ((np.max(mc_series, 0) - mc_series[:][winner_idx]) / 
                      mc_series[:][winner_idx])
        value_remaining = np.sort(lost_value)[0.95*MC_SAMPLES]

        # For all those thumbs that haven't been seen for 1000 imp,
        # make sure that they will get some traffic
        for i in range(len(bandit_ids)):
            if impressions[i] < 500:
                win_frac[i] = max(0.1, win_frac[i])
        win_frac = win_frac / np.sum(win_frac)

        # Change the winning strategy to value_remaining is less than
        # 0.01 (by the paper) Means that 95% of chance the value
        # remaining is 1% of the picked winner.  This will make it
        # comes to conclusion quicker comparing to using: if
        # win_frac[winner_idx] >= 0.95:
        if value_remaining <= Mastermind.VALUE_THRESHOLD:
            # There is a winner. See if there were enough imp to call it
            if (win_frac.shape[0] == 1 or 
                (impressions[winner_idx] >= 500 and 
                 total_conversions >= int(strategy.min_conversion))):
                # The experiment is done
                experiment_state = neondata.ExperimentState.COMPLETE
                try:
                    winner = valid_bandits[winner_idx]
                except IndexError:
                    winner = non_exp_thumb

            else:
                # Only allow the winner to have 90% of the imp
                # because there aren't enough impressions to make a good
                # decision.
                win_frac[winner_idx] = 0.90
                other_idx = [x for x in range(win_frac.shape[0])
                             if x != winner_idx]
                if np.sum(win_frac[other_idx]) < 1e-5:
                    win_frac[other_idx] = 0.1 / len(other_idx)
                else:
                    win_frac[other_idx] = \
                      0.1 / np.sum(win_frac[other_idx]) * win_frac[other_idx]

        # Remove the non experiment thumb from the win fractions
        if non_exp_thumb is not None:
            win_frac = win_frac[:-1]
            bandit_ids = bandit_ids[:-1]

        # The serving fractions for the experiment are just the
        # fraction of time that each thumb won the Monte Carlo
        # simulation adjusted by frac_adjust rate.
        # if frac_adjust_rate == 0.0, then all the fractions are equal.
        # If frac_adjust_rate == 1.0, then run_frac stays the same.
        win_frac = win_frac ** float(strategy.frac_adjust_rate)
        win_frac = win_frac / np.sum(win_frac)
        win_frac = np.around(win_frac, 2)

        return (experiment_state,
                dict(zip(bandit_ids, win_frac)),
                value_remaining,
                winner)

    def _get_rc_equivalent_prior(self, thumb_info, video_info):
        '''Get the rank centrality equivalent prior.

        Rank centrality is a score from 0 to inf with a mean of
        1. After taking the exponent of that, you can calucate the
        probability of A being more liked than B. In particular,

        P(A>B) = A / (A + B)
        '''
        score = thumb_info.model_score
        if (score is None or score < 1e-4 or math.isinf(score) or 
            math.isnan(score)):
            if thumb_info.chosen:
                # An editor chose this so give it a 5% lift
                return 1.10
        elif video_info.score_type == ScoreType.CLASSICAL:
            z = (score-4.0)/1.5
            return min(max((0.5 + z*0.05) / (0.5 - z*0.05), 0.0), 1.0)
        elif video_info.score_type == ScoreType.RANK_CENTRALITY:
            return score
        return 1.0
        

    def _get_prior_conversions(self, thumb_info, video_info):
        '''Get the number of conversions we would expect based on the model 
        score.'''
        
        score = thumb_info.model_score
        if score is not None:
            score = float(score)
        if (score is None or score < 1e-4 or math.isinf(score) or 
            math.isnan(score)):
            if thumb_info.chosen:
                # An editor chose this thumb, so give it a 5% lift
                return max(1.0, (Mastermind.PRIOR_CTR * 
                                 Mastermind.PRIOR_IMPRESSION_SIZE * 1.05))
            else:
                return max(1.0, (Mastermind.PRIOR_CTR * 
                                 Mastermind.PRIOR_IMPRESSION_SIZE))
        if video_info.score_type == ScoreType.CLASSICAL:
            # then it was calculated using Borda Count
            # original doc:
            # Peg a score of 5.5 as a 10% lift over random and a score of
            # 4.0 as neutral
            return max(1.0, ((0.10*(score-4.0)/1.5 + 1) * 
                            Mastermind.PRIOR_CTR * 
                            Mastermind.PRIOR_IMPRESSION_SIZE))
        elif video_info.score_type == ScoreType.RANK_CENTRALITY:
            # then it was calculated using Rank Centrality
            # in which case the lift is given directly by RC
            # of course, the calculated score only accounts for
            # about 22% of the variance (computed by Spearman's Rho)
            # and so we should regress it back to the mean
            # lift is given by the ratio of the images scores, 
            # regressed to the mean by some prior. Furthermore, 
            # we assume the mean value, which is 1 due to how we 
            # compute rank centrality.
            prior = 0.3
            return max(1.0, prior * score + (1-prior) * 1.0) 
        else:
            # then it is none, assume a lift of 0%
            return max(1.0, (1. * Mastermind.PRIOR_CTR * 
                             Mastermind.PRIOR_IMPRESSION_SIZE))

    def _get_sequential_fracs(self, strategy, candidates, video_info,
                              non_exp_thumb, baseline, video_id):
        '''Gets the serving fractions for a sequential testing strategy.

        This strategy gives equal serving percentages to each
        thumbnail (biased by the prior). Then, if a thumb goes below
        statistical significance relative to the baseline, it is
        turned off.

        Returns tuple containing:
        experiment_state - The new state of the experiment
        run_frac - Dictionary of thumb_id -> serving percentage
        value_left - The value left in the experiment
        winner_tid - Thumbnail id of the winner if there is one now
        
        '''
        run_frac = {}
        experiment_state = neondata.ExperimentState.RUNNING
        value_remaining = None
        winner = None
        valid_thumbs = list(copy.copy(candidates))
        if non_exp_thumb is not None:
            valid_thumbs.append(non_exp_thumb)

        # Build up a pandas data frame indexed by thumbnail id
        data = pandas.DataFrame(
            [{'conv': (self._get_prior_conversions(x, video_info) +
                       x.get_conversions()),
              'impr': Mastermind.PRIOR_IMPRESSION_SIZE + x.get_impressions(),
              'tid': x.id,
              'model_score' : self._get_rc_equivalent_prior(x, video_info)
              } for x in valid_thumbs])
        data = data.set_index('tid')

        # Now calculate the statistical significance relative to the
        # winning thumb.
        data['ctr'] = data['conv'] / data['impr']
        data['std_var'] = data['ctr'] * (1-data['ctr']) /  data['impr']
        win_id = data['ctr'].argmax()
        data['z_winner'] = ((data['ctr'] - data['ctr'][win_id]) / 
                            np.sqrt(data['std_var'] + 
                                    data['std_var'][win_id]))

        # Is the winning thumb the winner, enough conversions and impressions?
        best_z = np.max(data['z_winner'][data.index != win_id])
        value_remaining = sp.stats.norm(0,1).cdf(best_z)
        enough_conversions = (np.sum(data['conv']) >= 
                              int(strategy.min_conversion))
        if (value_remaining < 0.025 and
            data['impr'][win_id] >= 500 and
            enough_conversions):
            experiment_state = neondata.ExperimentState.COMPLETE
            winner = [x for x in valid_thumbs if x.id == win_id][0]
        else:
            # Set the experiment fractions for each thumb based on the prior
            run_frac = data['model_score'] / np.sum(data['model_score'])
            run_frac = run_frac ** float(strategy.frac_adjust_rate)

            # Now remove thumbnails that are significantly worse than
            # the baseline
            if baseline is not None and enough_conversions:
                data['z_base'] = ((data['ctr'] - data['ctr'][baseline.id]) / 
                                  np.sqrt(data['std_var'] + 
                                          data['std_var'][baseline.id]))
                done_thumbs = ((data['z_base'] < -1.645) &
                               (data['impr'] >= 500))
                run_frac[done_thumbs] = 0.0

                # Now turn off any thumbs that were previously turned off
                try:
                    old_directive = self.serving_directive[video_id][1]
                    off_thumbs = [x[0] for x in old_directive if (
                        x[1] < 1e-7 and 
                        (baseline is None or x[0] != baseline.id) and
                        (non_exp_thumb is None or x[0] != non_exp_thumb.id))]
                    run_frac[off_thumbs] = 0.0
                except KeyError:
                    pass

            # Normalize the running fractions to sum to 1.0
            if non_exp_thumb is not None:
                run_frac = run_frac[run_frac.index != non_exp_thumb.id]
            run_frac = (run_frac / np.sum(run_frac)).to_dict()
            
        return (experiment_state,
                run_frac,
                value_remaining,
                winner)

    def _get_experiment_done_fracs(self, strategy, baseline, editor, winner):
        '''Returns the serving fractions for when the experiment is complete.

        Just returns a dictionary of the directive { id -> frac }
        '''
        holdback_frac = max(min(float(strategy.holdback_frac), 1.0), 0.0)
        exp_frac = max(min(float(strategy.exp_frac), 1.0), 0.0)
        
        majority = editor or baseline
        if majority and majority.id == winner.id:
            # The winner was the default so put in the baseline as a holdback
            if baseline and majority.id != baseline.id:
                return { winner.id : 1.0 - holdback_frac,
                         baseline.id : holdback_frac }
        elif strategy.override_when_done:
            # The experiment is done and we want to serve the winner
            # most of the time.
            majority = baseline or editor
            if majority and majority.id != winner.id:
                return { winner.id : 1.0 - holdback_frac,
                         majority.id : holdback_frac }
        else:
            # The experiment is done, but we do not show the winner
            # for most of the traffic (usually because it's still a
            # pilot). So instead, just show it for the full
            # experimental percentage.
            if majority:
                return { winner.id : exp_frac,
                         majority.id : 1.0 - exp_frac }
            
        return { winner.id : 1.0 }
            

    def _find_thumb(self, video_id, thumb_id):
        '''Find the thumbnail info object.

        Logging of key errors is handled here.

        Inputs:
        video_id - The video id
        thumb_id - The thumbnail id in its full form

        Outputs:
        The ThumbnailInfo object (that can be written to) or None if
        it's not there.
        
        '''
        thumb_suffix = thumb_id.split('_')
        if len(thumb_suffix) != 3:
            thumb_suffix = thumb_id
            _log.warn('Invalid thumbnail id %s for video id %s' %
                      (thumb_id, video_id))
        else:
            thumb_suffix = thumb_suffix[2]
            
        try:
            for thumb in self.video_info[video_id].thumbnails:
                if thumb.id == thumb_suffix:
                    return thumb
        except KeyError:
            _log.warn_n(
                'Could not find information for video %s' % video_id,
                50)
            return None

        _log.warn('Could not find information for thumbnail %s in video %s' % 
                  (thumb_id, video_id))
        return None

    def _modify_video_state(self, video_id, experiment_state, value_left,
                            winner_tid, new_directive, video_info):
        '''Modifies the database with the current state of the video.'''

        # Update the serving percentages in the database
        self._incr_pending_modify(2)
        self.modify_pool.submit(
            _modify_many_serving_fracs,
            self, video_id, new_directive, video_info)
        
        self.modify_pool.submit(
            _modify_video_info,
            self, video_id, experiment_state, value_left, winner_tid)


def _modify_many_serving_fracs(mastermind, video_id, new_directive,
                               video_info):
    try:
        def _update(status_dict):
            for thumb_info in video_info.thumbnails:
                try:
                    status = status_dict['_'.join([video_id, thumb_info.id])]
                    status.set_serving_frac(new_directive[thumb_info.id])
                    if thumb_info.get_impressions() > 0:
                        status.ctr = (float(thumb_info.get_conversions()) /
                                      float(thumb_info.get_impressions()))
                    status.imp = thumb_info.get_impressions()
                    status.conv = thumb_info.get_conversions()
                except KeyError:
                    continue

        neondata.ThumbnailStatus.modify_many(
            ['_'.join([video_id, x.id]) for x in video_info.thumbnails],
            _update)
    except Exception as e:
        _log.exception('Unhandled exception when updating thumbs %s' % e)
        statemon.state.increment('db_update_error')
        raise
    finally:
        mastermind._incr_pending_modify(-1)

def _modify_video_info(mastermind, video_id, experiment_state, value_left,
                       winner_tid):

    try:
        full_winner = winner_tid
        if full_winner is not None:
            full_winner = '_'.join([video_id, full_winner])
        old_state = [None]
        def _update(status):
           old_state[0] = status.experiment_state
           status.set_experiment_state(experiment_state)
           status.winner_tid = full_winner
           status.experiment_value_remaining = value_left
        neondata.VideoStatus.modify(video_id, _update, create_missing=True)

        # Send the callback for the request if there was a state change
        if old_state[0] != experiment_state:
            vmeta = neondata.VideoMetadata.get(video_id)
            if vmeta is not None:
                request = neondata.NeonApiRequest.get(vmeta.job_id,
                                                      vmeta.get_account_id())
                if request is not None:
                    request.send_callback()
    except Exception as e:
        _log.exception('Unhandled exception when updating video %s' % e)
        statemon.state.increment('db_update_error')
        raise
    finally:
        mastermind._incr_pending_modify(-1)
