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
from mock import patch
import numpy.random
from supportServices import neondata
from supportServices.neondata import ThumbnailMetadata, ExperimentStrategy
import utils.neon
import unittest

def build_thumb(metadata=neondata.ThumbnailMetadata(None, None),
                loads=0, views=0, clicks=0, plays=0, phash=None):
    if phash is None:
        metadata.phash = numpy.random.randint(1<<30)
    else:
        metadata.phash = phash
    return ThumbnailInfo(metadata, loads, views, clicks, plays)

#TODO(mdesnoyer) what happens when a video is removed from the db?!?
class TestBadThumbCheck(unittest.TestCase):
    def setUp(self):
        self.mastermind = Mastermind()
        
    def test_better_chosen(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=50000, clicks=1000),
            build_thumb(views=60000, clicks=200)))

    def test_insignificant_difference(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=50000, clicks=1000),
            build_thumb(views=50000, clicks=1050)))

    def test_not_enough_views(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=500, clicks=1),
            build_thumb(views=499, clicks=300)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=10000, clicks=1),
            build_thumb(views=499, clicks=300)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=499, clicks=1),
            build_thumb(views=10000, clicks=300)))

    def test_better_default(self):
        self.assertTrue(self.mastermind._chosen_thumb_bad(
            build_thumb(views=50000, clicks=1000),
            build_thumb(views=70000, clicks=1520)))

    def test_no_data(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=0, clicks=0),
            build_thumb(views=0, clicks=0)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=0, clicks=0),
            build_thumb(views=1, clicks=0)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(views=1, clicks=0),
            build_thumb(views=0, clicks=0)))


class TestCurrentServingDirective(unittest.TestCase):
    def setUp(self):
        numpy.random.seed(1984934)
        self.mastermind = Mastermind()

        # Mock out the redis connection so that it doesn't throw an error
        redis_patcher = patch(
            'supportServices.neondata.blockingRedis.StrictRedis')
        redis_patcher.start()
        self.addCleanup(redis_patcher.stop)

        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1'))

    def test_priors(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))
        
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1',
                                               ttype='neon', model_score=3.5)),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='centerframe')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))]))

        self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
                         ['n2', 'ctr', 'bc', 'n1'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

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
                                               ttype='centerframe'))]))
        self.assertEqual(directive, {'n1': 0.0, 'n2':0.01, 'ctr':0.99 })

    def test_experiments_off(self):
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', False,
                [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='brightcove'))]))
        self.assertEqual(directive, {'n1': 0.0, 'ctr':1.0 })

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
                                               ttype='brightcove'))]))
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
                                               phash=67)]))
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
                                               ttype='brightcove'))]))
        
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
            video_info)
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Choose the n2 thumbnail
        video_info.thumbnails[1].metadata.chosen=True
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)
        self.assertEqual(directive, {'n1': 0.0, 'n2': 1.0, 'ctr': 0.0,
                                     'bc': 0.0})

        # Choose the brightcove thumbnail
        video_info.thumbnails[1].metadata.chosen=False
        video_info.thumbnails[3].metadata.chosen=True
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 0.0,
                                     'bc': 1.0})

    def test_only_experiment_if_chosen(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', only_exp_if_chosen=True))

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

        # The default thumbnail will be shown 100% of the time because
        # nothing is chosen.
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)
        self.assertEqual(directive, {'n1': 0.0, 'n2': 0.0, 'ctr': 0.0,
                                     'bc': 1.0})

        # Choose the n2 thumbnail and now both the center frame and n1
        # will show in the experiment fraction
        video_info.thumbnails[1].metadata.chosen=True
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)
        self.assertAlmostEqual(directive['n2'], 0.99)
        self.assertAlmostEqual(directive['bc'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['ctr'], 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)


    
@unittest.skip('ignore')
class TestStatUpdating(unittest.TestCase):
    def setUp(self):
        self.mastermind = Mastermind()

        # Create a fake filesystem because the deltas will get logged to disk
        self.filesystem = fake_filesystem.FakeFilesystem()
        tempfile_patcher = patch(
            'mastermind.core.tempfile',
            return_value=fake_tempfile.FakeTempfileModule(self.filesystem))
        tempfile_patcher.start()
        self.addCleanup(tempfile_patcher.stop)

        # Initialize mastermind with some videos
        self.mastermind.update_video_info('vidA', True, [
            build_thumb(id='a', origin='neon', rank=0, chosen=True),
            build_thumb(id='aa', origin='neon', rank=1),
            build_thumb(id='az', origin='bc')])
        self.mastermind.update_video_info('vidB', True, [
            build_thumb(id='b', origin='neon', rank=0),
            build_thumb(id='ba', origin='neon', rank=1, chosen=True),
            build_thumb(id='bz', origin='bc')])

    def tearDown(self):
        pass

    def test_initial_stats_update(self):
        result = dict(self.mastermind.update_stats_info(100, [
            ('vidA', 'a', 1000, 5),
            ('vidA', 'az', 1000, 100),
            ('vidB', 'b', 10, 5),
            ('vidB', 'ba', 1000, 100),
            ('vidB', 'bz', 1000, 100)]))
        self.assertIn('vidA', result)
        self.assertNotIn('vidB', result)
        self.assertItemsEqual([('a', 0.0), ('aa', 0.0),
                               ('az', 1.0)], result['vidA'])

    def test_decimal_from_db(self):
        result = dict(self.mastermind.update_stats_info(100, [
            ('vidA', 'a', decimal.Decimal(1000), decimal.Decimal(5)),
            ('vidA', 'az', decimal.Decimal(1000), decimal.Decimal(100)),
            ('vidB', 'b', decimal.Decimal(10), decimal.Decimal(5)),
            ('vidB', 'ba', decimal.Decimal(1000), decimal.Decimal(100)),
            ('vidB', 'bz', decimal.Decimal(1000), decimal.Decimal(100))]))
        self.assertIn('vidA', result)
        self.assertNotIn('vidB', result)
        self.assertItemsEqual([('a', 0.0), ('aa', 0.0),
                               ('az', 1.0)], result['vidA'])

    def test_deltas_on_clean_state(self):
        self.assertIsNone(self.mastermind.incorporate_delta_stats(
            100, 'vidA', 'a', 1000, 5))
        result = self.mastermind.incorporate_delta_stats(
            101, 'vidA', 'az', 1000, 100)
        self.assertEqual(result[0], 'vidA')
        self.assertItemsEqual([('a', 0.0), ('aa', 0.0),
                               ('az', 1.0)], result[1])


    def test_negative_deltas(self):
        with self.assertRaises(ValueError):
            self.mastermind.incorporate_delta_stats(100, 'vidA', 'a', 1000,-5)
        with self.assertRaises(ValueError):
            self.mastermind.incorporate_delta_stats(100, 'vidA', 'a', -1000,5)

    def test_deltas_replayed(self):
        '''Test that deltas are replayed after a db update.'''
        self.mastermind.incorporate_delta_stats(100, 'vidA', 'a', 1000, 5)
        self.mastermind.incorporate_delta_stats(101, 'vidA', 'az', 1000, 100)
        result = self.mastermind.incorporate_delta_stats(200, 'vidA', 'a',
                                                         100, 100)
        self.assertEqual('vidA', result[0])
        self.assertItemsEqual([('a', 0.85),('aa', 0.0),('az', 0.15)],result[1])


        # Now get a database update at t=150. Even though there is no
        # net change, when the database updates, it changes, but then
        # changes back with the replayed delta, so that's ok to
        # trigger an extra change event.
        result = dict(self.mastermind.update_stats_info(150, [
            ('vidA', 'a', 1000, 5),
            ('vidA', 'az', 1000, 100)]))
        self.assertIn('vidA', result)
        self.assertItemsEqual([('a', 0.85), ('aa', 0.0),
                               ('az', 0.15)], result['vidA'])
        
@unittest.skip('ignore')
class TestVideoInfoUpdate(unittest.TestCase):
    def setUp(self):
        self.mastermind = Mastermind()

        # Create a fake filesystem because the deltas will get logged to disk
        self.filesystem = fake_filesystem.FakeFilesystem()
        tempfile_patcher = patch(
            'mastermind.core.tempfile',
            return_value=fake_tempfile.FakeTempfileModule(self.filesystem))
        tempfile_patcher.start()
        self.addCleanup(tempfile_patcher.stop)

        # Insert a single video by default
        self.mastermind.update_video_info(
            'vidA', True, [
                build_thumb(id='a', origin='neon', rank=0, chosen=True),
                build_thumb(id='aa', origin='neon', rank=1),
                build_thumb(id='az', origin='bc')])

    def tearDown(self):
        pass

    def test_add_first_video_info(self):
        result = self.mastermind.update_video_info(
            'vidB', True, [build_thumb(id='bz', origin='bc')])
        self.assertEqual(result[0], 'vidB')
        self.assertItemsEqual([('bz', 1.0)],  result[1])

        # Repeating the info doesn't produce a change
        self.assertIsNone(self.mastermind.update_video_info(
            'vidB', True, [build_thumb(id='bz', origin='bc')]))

    def test_add_new_thumbs(self):
        result = self.mastermind.update_video_info(
            'vidA', True, [
                build_thumb(id='a', origin='neon', rank=0, chosen=True),
                build_thumb(id='aa', origin='neon', rank=1),
                build_thumb(id='aaa', origin='neon', rank=2),
                build_thumb(id='az', origin='bc')])
        self.assertEqual(result[0], 'vidA')
        self.assertItemsEqual([('a', 0.85), ('aa', 0.0), ('aaa', 0.0),
                               ('az', 0.15)], result[1])

    def test_remove_thumbs(self):
        result = self.mastermind.update_video_info(
            'vidA', True, [build_thumb(id='az', origin='bc')])
        self.assertEqual(result[0], 'vidA')
        self.assertItemsEqual([('az', 1.0)], result[1])

    def test_change_test_settings(self):
        result = self.mastermind.update_video_info(
            'vidA', True, [
                build_thumb(id='a', origin='neon', rank=0),
                build_thumb(id='aa', origin='neon', rank=1),
                build_thumb(id='az', origin='bc', enabled=False)])
        self.assertEqual(result[0], 'vidA')
        self.assertItemsEqual([('a', 1.0), ('aa', 0.0), ('az', 0.0)],
                              result[1])

        # Turning off A/B testing is the same as disabling the default.
        self.assertIsNone(self.mastermind.update_video_info(
            'vidA', False, [
                build_thumb(id='a', origin='neon', rank=0, chosen=True),
                build_thumb(id='aa', origin='neon', rank=1),
                build_thumb(id='az', origin='bc')]))

        # Change to the second rank neon image
        result = self.mastermind.update_video_info(
            'vidA', False, [
                build_thumb(id='a', origin='neon', rank=0),
                build_thumb(id='aa', origin='neon', rank=1, chosen=True),
                build_thumb(id='az', origin='bc')])
        self.assertEqual(result[0], 'vidA')
        self.assertItemsEqual([('a', 0.0), ('aa', 1.0), ('az', 0.0)],
                              result[1])
        

if __name__ == '__main__':
    utils.neon.InitNeonTest()
    unittest.main()
