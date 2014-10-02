#!/usr/bin/env python
'''
Tests for the core module

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
import mastermind.core
from mastermind.core import Mastermind, ThumbnailInfo, VideoInfo

import decimal
import fake_filesystem
import fake_tempfile
import logging
from mock import patch, MagicMock
import multiprocessing.pool
import numpy.random
from supportServices import neondata
from supportServices.neondata import ThumbnailMetadata, ExperimentStrategy, VideoMetadata
import test_utils.neontest
import test_utils.redis
import utils.neon
import unittest

_log = logging.getLogger(__name__)

def build_thumb(metadata=neondata.ThumbnailMetadata(None, None),
                impressions=0, conversions=0, phash=None):
    if phash is None:
        metadata.phash = numpy.random.randint(1<<30)
    else:
        metadata.phash = phash
    return ThumbnailInfo(metadata, impressions, conversions)

#TODO(mdesnoyer) what happens when a video is removed from the db?!?

class TestObjects(test_utils.neontest.TestCase):
    def test_video_info_equals(self):
        video_info_1 = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                                           phash=32)])

        video_info_2 = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                                           phash=32)])

        self.assertEqual(video_info_1, video_info_2)
        self.assertEqual(repr(video_info_1), repr(video_info_2))

    def test_update_stats_with_wrong_id(self):
        thumb1 = ThumbnailInfo(ThumbnailMetadata('t1', 'v1'), impressions=300)
        thumb2 = ThumbnailInfo(ThumbnailMetadata('t2', 'v1'), impressions=400)

        with self.assertLogExists(logging.ERROR,
                                  "Two thumbnail ids don't match"):
            self.assertEqual(thumb1.update_stats(thumb2), thumb1)
        self.assertEqual(thumb1.imp, 300)
    
class TestCurrentServingDirective(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestCurrentServingDirective, self).setUp()
        numpy.random.seed(1984934)

        # Mock out the redis connection so that it doesn't throw an error
        self.redis_patcher = patch(
            'supportServices.neondata.blockingRedis.StrictRedis')
        self.redis_patcher.start()
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)

        self.mastermind = Mastermind()
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1'))
        logging.getLogger('mastermind.core').reset_sample_counters()

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.redis_patcher.stop()

    def test_priors(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))
        
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1',
                                               ttype='neon',
                                               model_score='3.5')),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))]))[1]

        self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
                         ['n2', 'ctr', 'bc', 'n1'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

    def test_exp_frac_1(self):
        # Testing all the cases when the experiment fraction is 1.0
        # because in that case, we add the editor's selection and/or
        # the baseline to the videos being experimented with
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))

        # Start with just some Neon thumbs
        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon',
                                           model_score=u'3.5'))])
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Now add a baseline thumb
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertGreater(directive['ctr'], 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Now add a default thumb that's chosen by the editor. In this
        # case, the baseline is still shown because
        # always_show_baseline defaults to True.
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('o1', 'vid1', chosen=True,
                                          ttype='ooyala')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertGreater(directive['o1'], 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Finally, turn off the always show baseline
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0,
                                        always_show_baseline=False))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertGreater(directive['n2'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['o1'], 0.0)
        self.assertAlmostEqual(directive['ctr'], 0.0)
         

    def test_disabled_videos(self):
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8,
                                               enabled=False)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1',
                                               ttype='neon', model_score=3.5)),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe'))]))[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2':0.01, 'ctr':0.99 })

    def test_finding_baseline_thumb(self):
        video_info = VideoInfo('acct1', True, [])
        
        # When there is just a Neon thumb, we should show the Neon one
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n1', 'vid1', ttype='neon')))
        with self.assertLogExists(logging.WARNING, ('Could not find a '
                                                    'baseline')):
            directive = self.mastermind._calculate_current_serving_directive(
                video_info)[1]
        self.assertEqual(directive, {'n1': 1.0})

        # Now we add a baseline of highish rank
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr1', 'vid1', rank=3,
                                          ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.01, 'ctr1': 0.99 })

        # Finally add a lower rank baseline
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr2', 'vid1', rank=1,
                                          ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.01, 'ctr1': 0.0, 'ctr2': 0.99})

    def test_baseline_is_different_type(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', baseline_type='random'))

        # The center frame is not shown if it's not the baseline type
        video_info = VideoInfo('acct1', True, [])
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1', ttype='centerframe')))
        with self.assertLogExists(logging.ERROR,
                                  'No valid thumbnails for video'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    video_info))

        # Add a random thumb and it should be shown because it is the baseline
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('rnd', 'vid1', ttype='random')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'rnd': 1.0, 'ctr': 0.0})

    def test_baseline_is_also_default_type(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', baseline_type='brightcove'))

        video_info = VideoInfo('acct1', True, [])
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1', ttype='centerframe')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1', ttype='brightcove')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n1', 'vid1', ttype='neon')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.99, 'ctr': 0.0, 'n1': 0.01})

        # When the baseline wins, it should be shown for 100%
        video_info.thumbnails[1].conv = 5000
        video_info.thumbnails[1].imp = 5000
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 1.0, 'ctr': 0.0, 'n1': 0.0})

    def test_baseline_is_neon(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', baseline_type='neon',
                                        holdback_frac=0.02))

        video_info = VideoInfo('acct1', True, [])
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1', ttype='centerframe')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1', ttype='brightcove')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n1', 'vid1', ttype='neon')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.99, 'ctr': 0.0, 'n1': 0.01})

        # When the baseline wins, it should be shown for all 100%
        video_info.thumbnails[2].conv = 5000
        video_info.thumbnails[2].imp = 5000
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.0, 'ctr': 0.0, 'n1': 1.0})

        # If the default one wins, we show the baseline for the holdback
        video_info.thumbnails[2].conv = 0
        video_info.thumbnails[2].imp = 0
        video_info.thumbnails[1].conv = 5000
        video_info.thumbnails[1].imp = 5000
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.98, 'ctr': 0.0, 'n1': 0.02})

    def test_always_show_baseline(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', always_show_baseline=True,
                                        holdback_frac=0.02))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=1,
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1', rank=3,
                                           ttype='neon', model_score=3.1)),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='centerframe')),
             build_thumb(ThumbnailMetadata('bc1', 'vid1', chosen=True,
                                           ttype='brightcove'))])

        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(directive['bc1'], 0.99)
        for val in directive.values():
            self.assertGreater(val, 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)

        # Now don't show the baseline in the experiment
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', always_show_baseline=False,
                                        holdback_frac=0.02))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(directive['bc1'], 0.99)
        self.assertAlmostEqual(directive['ctr'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['n2'], 0.0)

    def test_multiple_chosen_thumbs(self):
        video_info = VideoInfo('acct1', True, [])

        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n1', 'vid1', ttype='neon',
                                          chosen=True, rank=3)))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 1.0 })

        # Choose an ooyala thumb, we will experiment on the Neon one still
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('o1', 'vid1', ttype='ooyala',
                                          chosen=True, rank=2)))
        with self.assertLogExists(logging.WARNING, 'More than one chosen'):
            directive = self.mastermind._calculate_current_serving_directive(
                video_info)[1]
        self.assertEqual(directive, {'n1': 0.01, 'o1': 0.99})

        # Choose a better neon thumb and the Ooyala one is no longer served
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n2', 'vid1', ttype='neon',
                                          chosen=True, rank=1)))
        with self.assertLogExists(logging.WARNING, 'More than one chosen'):
            directive = self.mastermind._calculate_current_serving_directive(
                video_info)[1]
        self.assertEqual(directive, {'n1': 0.01, 'o1': 0.0, 'n2': 0.99})
        

    def test_experiments_off(self):
        # First check the case where there is no baseline around
        video_info = VideoInfo(
                'acct1', False, [build_thumb(
                    ThumbnailMetadata('n1', 'vid1',ttype='neon',
                                      model_score=5.8))])
        with self.assertLogExists(logging.ERROR, ('Testing was disabled and '
                                                  'there was no baseline')):
            with self.assertLogExists(logging.WARNING, ('Could not find a '
                                                        'baseline')):
                self.assertIsNone(
                    self.mastermind._calculate_current_serving_directive(
                        video_info))

        # Now add a baseline and it should be shown all the time
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'ctr':1.0 })

        # Now add a default thumb and make it is shown all the time
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1',
                                          ttype='brightcove')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'ctr':0.0, 'bc':1.0 })

        # Finally, the editor selects the Neon thumb and it should be
        # shown all the time.
        video_info.thumbnails[0].chosen = True
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 1.0, 'ctr':0.0, 'bc':0.0 })

    def test_multiple_partner_thumbs(self):
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe')),
                 build_thumb(ThumbnailMetadata('bc1', 'vid1', rank=1,
                                               ttype='brightcove')),
                 build_thumb(ThumbnailMetadata('bc2', 'vid1', rank=2,
                                               ttype='brightcove'))]))[1]
        self.assertAlmostEqual(directive['bc1'], 0.99)
        self.assertAlmostEqual(directive['bc2'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['ctr'], 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)

    def test_editor_uploaded_baseline_thumb(self):
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe'),
                                               phash=64),
                 build_thumb(ThumbnailMetadata('bc1', 'vid1', rank=1,
                                               ttype='brightcove')),
                 build_thumb(ThumbnailMetadata('cust', 'vid1', chosen=True,
                                               ttype='customupload'),
                                               phash=67)]))[1]
        self.assertAlmostEqual(directive['ctr'], 0.99)
        self.assertAlmostEqual(directive['cust'], 0.0)
        self.assertAlmostEqual(directive['bc1'], 0.0)
        self.assertAlmostEqual(directive['n1'], 0.01)
        self.assertAlmostEqual(sum(directive.values()), 1.0)

    def test_chosen_neon_thumb(self):
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', chosen=True,
                                               ttype='neon', model_score=3.5)),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1',
                                               ttype='brightcove'))]))[1]
        
        self.assertAlmostEqual(directive['n2'], 0.99)
        self.assertAlmostEqual(directive['bc'], 0.0)
        self.assertGreater(directive['n1'], directive['ctr'])
        self.assertGreater(directive['ctr'], 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        
    def test_chosen_override(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', chosen_thumb_overrides=True))

        video_info = VideoInfo(
            'acct1', True,
            [ build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
              build_thumb(ThumbnailMetadata('n2', 'vid1',
                                            ttype='neon', model_score=3.5)),
              build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                            ttype='centerframe')),
              build_thumb(ThumbnailMetadata('bc', 'vid1',
                                            ttype='brightcove'))])

        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Choose the n2 thumbnail
        video_info.thumbnails[1].chosen=True
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 1.0, 'ctr': 0.0,
                                     'bc': 0.0})

        # Choose the brightcove thumbnail
        video_info.thumbnails[1].chosen=False
        video_info.thumbnails[3].chosen=True
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 0.0,
                                     'bc': 1.0})

    def test_only_experiment_if_chosen(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', only_exp_if_chosen=True))

        video_info = VideoInfo('acct1', True, [])

        with self.assertLogExists(logging.ERROR,
                                  'No valid thumbnails for video'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    video_info))

        # The lowest rank Neon thumb will be shown if there is no baseline
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n2', 'vid1', rank=2,
                                          ttype='neon', model_score=3.5)))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n1', 'vid1', rank=1,
                                          ttype='neon', model_score=5.8)))
        with self.assertLogExists(logging.WARNING,
                                  'Could not find the default thumbnail'):
            directive = self.mastermind._calculate_current_serving_directive(
                video_info)[1]
        self.assertEqual(directive, {'n1': 1.0, 'n2': 0.0})
                                          
        # The baseline thumb will be shown because nothing was chosen
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 1.0})

        # The default thumbnail will be shown 100% of the time because
        # nothing is chosen.
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1', ttype='brightcove')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 0.0,
                                     'bc': 1.0})

        # Choose the n2 thumbnail and now both the center frame and n1
        # will show in the experiment fraction
        video_info.thumbnails[0].chosen=True
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(directive['n2'], 0.99)
        self.assertAlmostEqual(directive['bc'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['ctr'], 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)

    def test_too_many_thumbs_disabled(self):
        video_info = VideoInfo('acct1', True, [])
        with self.assertLogExists(logging.ERROR,
                                  'No valid thumbnails for video'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    video_info))

        # A Neon video is the only one that isn't disabled, so show it
        # at all times and throw a warning.
        video_info.thumbnails.extend(
            [ build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
              build_thumb(ThumbnailMetadata('ctr', 'vid1', enabled=False,
                                            ttype='centerframe')),
              build_thumb(ThumbnailMetadata('bc', 'vid1', enabled=False,
                                            ttype='brightcove'))])
        with self.assertLogExists(logging.WARNING,
                                  'Could not find a baseline'):
            directive = self.mastermind._calculate_current_serving_directive(
                video_info)[1]
        self.assertEqual(directive, {'n1': 1.0, 'ctr': 0.0, 'bc': 0.0})


        # Disabled the Neon video
        video_info.thumbnails[0].enabled = False
        with self.assertLogExists(logging.ERROR,
                                  'No valid thumbnails for video'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    video_info))

    def test_sequential_strategy(self):
        # The sequential strategy is currently not implemented, so we
        # should log a fatal and then fallback on the multi-armed
        # bandit.
        self.mastermind.update_experiment_strategy(
            'acct1', 
            ExperimentStrategy('acct1',
                               experiment_type=ExperimentStrategy.SEQUENTIAL))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5)),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='centerframe')),
             build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                           ttype='brightcove'))])

        with self.assertLogExists(logging.ERROR, 'not implemented'):
            directive = self.mastermind._calculate_current_serving_directive(
                video_info)[1]

        self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
                         ['n2', 'ctr', 'n1', 'bc'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)
        self.assertEqual(directive['bc'], 0.99)

    def test_invalid_strategy(self):
        self.mastermind.update_experiment_strategy(
            'acct1', 
            ExperimentStrategy('acct1', experiment_type='unknown'))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8))])

        with self.assertLogExists(logging.ERROR, 'Invalid experiment type'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    video_info))

    def test_no_strategy_set(self):

        with self.assertLogExists(logging.ERROR, 
                                  'Could not find the experimental strategy'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    VideoInfo('acct2', True, [])))

    def test_winner_found_override_editor(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', holdback_frac=0.02))
        
        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5))])

        def _set_winner(thumb_name):
            for thumb in video_info.thumbnails:
                thumb.conv = 5000 if thumb.id == thumb_name else 0
                thumb.imp = 5000 if thumb.id == thumb_name else 0

        _set_winner('n2')

        # In the default case with no editor selection or baseline,
        # just show the winner 100% of the time
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 1.0})

        # Now add a baseline, which should be shown as the holdback
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.98, 'ctr': 0.02})

        # If the baseline wins, it should be shown for 100%
        _set_winner('ctr')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 1.0})

        # Now add a chosen thumb to the party. We use the baseline as
        # a holdback
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                          ttype='brightcove')))
        _set_winner('n2')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.98, 'ctr': 0.02,
                                     'bc': 0.0})

        # If the chosen one wins, still show the baseline as a holdback
        _set_winner('bc')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 0.02,
                                     'bc': 0.98})

        # If the baseline wins, still give it 100%
        _set_winner('ctr')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 1.0,
                                     'bc': 0.0})

    def test_winner_found_no_override_editor(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', holdback_frac=0.02,
                                        override_when_done=False))
        
        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5))])

        def _set_winner(thumb_name):
            for thumb in video_info.thumbnails:
                thumb.conv = 5000 if thumb.id == thumb_name else 0
                thumb.imp = 5000 if thumb.id == thumb_name else 0

        _set_winner('n2')
        # There is no editor choice, so show the winner for 100%
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 1.0})

        # Now add a baseline, which should be shown as the majority
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.01, 'ctr': 0.99})

        # If the baseline wins, it should be shown for 100%
        _set_winner('ctr')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 1.0})

        # Now add a chosen thumb to the party
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                          ttype='brightcove')))
        _set_winner('n2')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.01, 'ctr': 0.0,
                                     'bc': 0.99})

        # If the chosen one wins, still show the baseline as a holdback
        _set_winner('bc')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 0.02,
                                     'bc': 0.98})

        # But if the baseline beats the editor, give it the experiment traffic
        _set_winner('ctr')
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 0.01,
                                     'bc': 0.99})

    def test_stats_change_serving_frac_no_winner(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))
        video_info = VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1',
                                               ttype='neon', model_score=3.5)),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))])
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
                         ['n2', 'ctr', 'bc', 'n1'])

        # Add some stats where n2 starts to bubble up but doesn't win
        video_info.thumbnails[0].imp = 1000
        video_info.thumbnails[0].conv = 10
        video_info.thumbnails[1].imp = 1000
        video_info.thumbnails[1].conv = 20
        video_info.thumbnails[2].imp = 1000
        video_info.thumbnails[2].conv = 10
        video_info.thumbnails[3].imp = 1000
        video_info.thumbnails[3].conv = 10

        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
                         ['ctr', 'bc', 'n1', 'n2'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

    def test_not_enough_impressions_for_winner(self):
        # There needs to be 500 impressions of the winner in order to declare it
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))
        
        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                         impressions=50, conversions=20),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5)),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='centerframe')),
             build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                           ttype='brightcove'))])
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertAlmostEqual(max(directive.values()), directive['n1'])
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Now test with a large enough number that floating point
        # numbers go to zero
        video_info.thumbnails[0].imp=499
        video_info.thumbnails[0].conv=200
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)
        self.assertAlmostEqual(max(directive.values()), directive['n1'])

    def test_multiple_candidates_no_baseline(self):
        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5))
            ])

        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertGreater(directive['n2'], 0.0)
        self.assertGreater(directive['n1'], directive['n2'])

    def test_limit_num_neon_thumbs(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', max_neon_thumbs=2,
                                        exp_frac=1.0))

        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=1,
                                               ttype='neon', enabled=False,
                                               model_score='3.5')),
                 build_thumb(ThumbnailMetadata('n3', 'vid1', rank=2,
                                               ttype='neon',
                                               model_score='3.4')),
                 build_thumb(ThumbnailMetadata('n4', 'vid1', rank=3,
                                               ttype='neon',
                                               model_score='3.3')),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))]))[1]
        self.assertEqual(
            sorted(directive.keys(), key=lambda x: directive[x])[2:],
            ['n3', 'ctr', 'bc', 'n1'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertAlmostEqual(directive['n2'], 0.0)
        self.assertAlmostEqual(directive['n4'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['n3'], 0.0)
        self.assertGreater(directive['ctr'], 0.0)
        self.assertGreater(directive['bc'], 0.0)

class TestUpdatingFuncs(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestUpdatingFuncs, self).setUp()
        numpy.random.seed(1984934)

        # Mock out the redis connection so that it doesn't throw an error
        self.redis_patcher = patch(
            'supportServices.neondata.blockingRedis.StrictRedis')
        self.redis_mock = self.redis_patcher.start()
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)

        self.mastermind = Mastermind()
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1'))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1',
                               ttype='neon')], True)
        logging.getLogger('mastermind.core').reset_sample_counters()

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.redis_patcher.stop()
        logging.getLogger('mastermind.core').reset_sample_counters()

    def test_no_data_yet(self):
        self.assertItemsEqual(self.mastermind.get_directives(['acct2_vid1']),
                              [])

    def test_update_with_empty_experiment_strategy(self):
        with self.assertLogExists(logging.ERROR,
                                  'Invalid account id.* and strategy'):
            self.mastermind.update_experiment_strategy('acct1', None)

        self.assertIsNotNone(self.mastermind.experiment_strategy['acct1'])

    def test_update_experiment_strategy(self):        
        with self.assertLogExists(logging.INFO, 'strategy has changed'):
            self.mastermind.update_experiment_strategy(
                'acct2', ExperimentStrategy('acct2'))

        self.mastermind.update_video_info(
            VideoMetadata('acct2_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct2_vid1',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct2_vid1',
                               ttype='neon')], True)

        orig_directive = self.mastermind.get_directives(['acct2_vid1']).next()

        # A strategy that isn't different is ignored
        logging.getLogger('mastermind.core').reset_sample_counters()
        with self.assertLogNotExists(logging.INFO, 'strategy has changed'):
            self.mastermind.update_experiment_strategy(
                'acct2', ExperimentStrategy('acct2'))
        self.assertEqual(orig_directive,
                         self.mastermind.get_directives(['acct2_vid1']).next())

        logging.getLogger('mastermind.core').reset_sample_counters()
        with self.assertLogExists(logging.INFO, 'strategy has changed'):
            self.mastermind.update_experiment_strategy(
                'acct2', ExperimentStrategy('acct2', exp_frac=0.5))
        self.assertNotEqual(
            orig_directive,
            self.mastermind.get_directives(['acct2_vid1']).next())

    def test_add_first_video_info(self):
        self.mastermind.wait_for_pending_modifies()
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 0.99),
                                                 ('acct1_vid1_tid2', 0.01)])
        self.assertGreater(self.redis_mock.call_count, 0)
        self.redis_mock.reset_mock()

        # Repeating the info doesn't produce a change
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1',
                               ttype='neon')], True)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 0.99),
                                                 ('acct1_vid1_tid2', 0.01)])
        self.assertEqual(self.redis_mock.call_count, 0)

    def test_add_new_thumbs(self):
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1',
                               ttype='neon', rank=1),
             ThumbnailMetadata('acct1_vid1_tid3', 'acct1_vid1',
                               ttype='neon', rank=2)],
             True)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        directive = dict(directives[0][1])
        self.assertItemsEqual(directive.keys(),
                              ['acct1_vid1_tid1', 'acct1_vid1_tid2',
                               'acct1_vid1_tid3'])
        self.assertEqual(directive['acct1_vid1_tid1'], 0.99)
        self.assertGreater(directive['acct1_vid1_tid2'], 0.0)
        self.assertGreater(directive['acct1_vid1_tid3'], 0.0)

    def test_remove_thumbs(self):
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('tid1', 'acct1_vid1', ttype='centerframe')],
             True)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 1.0)])

    def test_disable_testing_as_param(self):
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1', ttype='neon')],
             testing_enabled=False)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 1.0),
                                                 ('acct1_vid1_tid2', 0.0)])

    def test_disable_testing_as_video_metadata(self):
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1', testing_enabled=False),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1', ttype='neon')],
             testing_enabled=True)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 1.0),
                                                 ('acct1_vid1_tid2', 0.0)])

    def test_update_video_with_bad_data(self):
        # Keep the old serving fractions
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='centerframe', enabled=False),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1', ttype='neon',
                               enabled=False)],
             testing_enabled=True)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 0.99),
                                                 ('acct1_vid1_tid2', 0.01)])
    
class TestStatUpdating(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestStatUpdating, self).setUp()
        numpy.random.seed(1984934)

        # Mock out the redis connection so that it doesn't throw an error
        self.redis_patcher = patch(
            'supportServices.neondata.blockingRedis.StrictRedis')
        self.redis_mock = self.redis_patcher.start()
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)

        self.mastermind = Mastermind()

        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1'))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_v1t1', 'acct1_vid1',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid1_v1t2', 'acct1_vid1', ttype='neon')])
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid2'),
            [ThumbnailMetadata('acct1_vid2_v2t1', 'acct1_vid2',
                               ttype='centerframe'),
             ThumbnailMetadata('acct1_vid2_v2t2', 'acct1_vid2',
                               ttype='neon', rank=0),
             ThumbnailMetadata('acct1_vid2_v2t3', 'acct1_vid2',
                               ttype='neon', rank=3)])

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.redis_patcher.stop()
        
    def test_initial_stats_update(self):
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 1000, 5),
            ('acct1_vid1', 'acct1_vid1_v1t2', 1000, 100),
            ('acct1_vid2', 'acct1_vid2_v2t1', 10, 5),
            ('acct1_vid2', 'acct1_vid2_v2t2', 1000, 100),
            ('acct1_vid2', 'acct1_vid2_v2t3', 1000, 100)])

        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_v1t1', 0.01),
                               ('acct1_vid1_v1t2', 0.99)])
        for val in [x[1] for x in directives[('acct1', 'acct1_vid2')]]:
            self.assertGreater(val, 0.0)

    def test_decimal_from_db(self):
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', decimal.Decimal(1000),
             decimal.Decimal(5)),
            ('acct1_vid1', 'acct1_vid1_v1t2', decimal.Decimal(1000),
             decimal.Decimal(100)),
            ('acct1_vid2', 'acct1_vid2_v2t1', decimal.Decimal(10),
             decimal.Decimal(5)),
            ('acct1_vid2', 'acct1_vid2_v2t2', decimal.Decimal(1000),
             decimal.Decimal(100)),
            ('acct1_vid2', 'acct1_vid2_v2t3', decimal.Decimal(1000),
             decimal.Decimal(100))])

        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_v1t1', 0.01),
                               ('acct1_vid1_v1t2', 0.99)])
        for val in [x[1] for x in directives[('acct1', 'acct1_vid2')]]:
            self.assertGreater(val, 0.0)

    def test_update_stats_for_unknown_video(self):
        with self.assertLogExists(logging.WARNING,
                                  'Could not find information for video'):
        
            self.mastermind.update_stats_info([
                ('acct1_unknown', 'v1t1', 1000, 5)
                ])

    def test_update_stats_for_unknown_thumb(self):
        with self.assertLogExists(logging.WARNING,
                                  'Could not find information for thumbnail'):
        
            self.mastermind.update_stats_info([
                ('acct1_vid1', 'v1t_where', 1000, 5)
                ])

class TestStatusUpdatesInDb(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestStatusUpdatesInDb, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)
        
        numpy.random.seed(1984934)
        self.mastermind = Mastermind()

        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0,
                                        holdback_frac=0.02))

        # Add some initial data to the database
        self.thumbnails = [
            ThumbnailMetadata('acct1_vid1_n1', 'acct1_vid1',
                              ttype='neon', model_score=5.8),
            ThumbnailMetadata('acct1_vid1_n2', 'acct1_vid1',
                              ttype='neon', model_score=3.5),
            ThumbnailMetadata('acct1_vid1_ctr', 'acct1_vid1',
                              ttype='centerframe'),
            ThumbnailMetadata('acct1_vid1_bc', 'acct1_vid1', chosen=True,
                              ttype='brightcove')]
        
        ThumbnailMetadata.save_all(self.thumbnails)
        self.video_metadata = VideoMetadata(
            'acct1_vid1', tids=[x.key for x in self.thumbnails])
        self.video_metadata.save()
        self.mastermind.update_video_info(self.video_metadata, self.thumbnails)
        self._wait_for_db_updates()

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.redis.stop()

    def _wait_for_db_updates(self):
        self.mastermind.wait_for_pending_modifies()

    def test_db_when_experiment_running(self):
        video = VideoMetadata.get('acct1_vid1')
        thumbs = ThumbnailMetadata.get_many(video.thumbnail_ids)
        directive = dict([(x.key, x.serving_frac) for x in thumbs])
        self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
                         ['acct1_vid1_n2', 'acct1_vid1_ctr', 'acct1_vid1_bc',
                          'acct1_vid1_n1'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)
            
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.RUNNING)
        self.assertGreater(video.experiment_value_remaining,
                           0.10)

    def test_db_experiment_disabled(self):
        self.mastermind.update_video_info(self.video_metadata, self.thumbnails,
                                          False)
        self._wait_for_db_updates()

        thumbs = ThumbnailMetadata.get_many(self.video_metadata.thumbnail_ids)
        video = VideoMetadata.get('acct1_vid1')
        directive = dict([(x.key, x.serving_frac) for x in thumbs])
        self.assertEqual(directive, {'acct1_vid1_bc':1.0,
                                     'acct1_vid1_n1':0.0,
                                     'acct1_vid1_n2':0.0,
                                     'acct1_vid1_ctr':0.0})
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.DISABLED)

    def test_db_remove_video(self):
        # Remove a video that is there
        self.assertTrue(self.mastermind.is_serving_video('acct1_vid1'))
        self.mastermind.remove_video_info('acct1_vid1')
        self._wait_for_db_updates()
        self.assertFalse(self.mastermind.is_serving_video('acct1_vid1'))
        self.assertItemsEqual(self.mastermind.get_directives(), [])

        # Remove a video that wasn't there
        self.assertFalse(self.mastermind.is_serving_video('acct1_vid123'))
        self.mastermind.remove_video_info('acct1_vid123')
        self.assertFalse(self.mastermind.is_serving_video('acct1_vid123'))
        
        # Check that the video's state is recorded
        video = VideoMetadata.get('acct1_vid1')
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.DISABLED)

    def test_db_experiment_finished(self):
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'n2', 5000, 200)])
        self._wait_for_db_updates()

        video = VideoMetadata.get('acct1_vid1')
        thumbs = ThumbnailMetadata.get_many(video.thumbnail_ids)
        directive = dict([(x.key, x.serving_frac) for x in thumbs])
        self.assertEqual(directive, {'acct1_vid1_bc':0.0,
                                     'acct1_vid1_n1':0.0,
                                     'acct1_vid1_n2':0.98,
                                     'acct1_vid1_ctr':0.02})
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.COMPLETE)
        self.assertLess(video.experiment_value_remaining,
                        0.05)

    def test_db_override_thumb(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0,
                                        holdback_frac=0.02,
                                        chosen_thumb_overrides=True))
        self._wait_for_db_updates()
        
        video = VideoMetadata.get('acct1_vid1')
        thumbs = ThumbnailMetadata.get_many(video.thumbnail_ids)
        directive = dict([(x.key, x.serving_frac) for x in thumbs])
        self.assertEqual(directive, {'acct1_vid1_bc':1.0, 'acct1_vid1_n1':0.0,
                                     'acct1_vid1_n2':0.0,
                                     'acct1_vid1_ctr':0.0})
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.OVERRIDE)    

if __name__ == '__main__':
    utils.neon.InitNeonTest()
    test_utils.neontest.main()
