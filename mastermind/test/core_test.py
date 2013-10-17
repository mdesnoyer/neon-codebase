#!/usr/bin/env python
'''
Tests for the core module

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
from mastermind.core import *

import fake_filesystem
import fake_tempfile
import unittest

def build_thumb(id=None, origin=None, rank=None, enabled=True, chosen=False,
                score=0, loads=0, clicks=0):
    return ThumbnailInfo(id, origin, rank, enabled, chosen, score, loads,
                         clicks)


class TestBadThumbCheck(unittest.TestCase):
    def setUp(self):
        self.mastermind = Mastermind()
        
    def test_better_chosen(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=50000, clicks=1000),
            build_thumb(loads=60000, clicks=200)))

    def test_insignificant_difference(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=50000, clicks=1000),
            build_thumb(loads=50000, clicks=1050)))

    def test_not_enough_views(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=500, clicks=1),
            build_thumb(loads=499, clicks=300)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=10000, clicks=1),
            build_thumb(loads=499, clicks=300)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=499, clicks=1),
            build_thumb(loads=10000, clicks=300)))

    def test_better_default(self):
        self.assertTrue(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=50000, clicks=1000),
            build_thumb(loads=70000, clicks=1520)))

    def test_no_data(self):
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=0, clicks=0),
            build_thumb(loads=0, clicks=0)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=0, clicks=0),
            build_thumb(loads=1, clicks=0)))
        self.assertFalse(self.mastermind._chosen_thumb_bad(
            build_thumb(loads=1, clicks=0),
            build_thumb(loads=0, clicks=0)))

class TestCurrentServingDirective(unittest.TestCase):
    def setUp(self):
        self.mastermind = Mastermind()

    def test_no_default_or_chosen(self):
        self.assertIsNone(self.mastermind._calculate_current_serving_directive(
            VideoInfo(True, [])))
        self.assertIsNone(self.mastermind._calculate_current_serving_directive(
            VideoInfo(True,
                      [build_thumb(origin='neon', rank=2, enabled=False)])))

    def test_no_ab_testing(self):
        self.assertItemsEqual([('a', 0.0), ('b', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(False,
                          [build_thumb(id='a', origin='bc'),
                           build_thumb(id='b', origin='neon', rank=0)])))
        self.assertItemsEqual([('a', 1.0), ('b', 0.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(False,
                          [build_thumb(id='a', origin='bc', chosen=True),
                           build_thumb(id='b', origin='neon', rank=0)])))

    def test_no_default(self):
        self.assertItemsEqual([('a', 0.0), ('b', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=1),
                           build_thumb(id='b', origin='neon', rank=0)])))
        self.assertItemsEqual([('a', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0)])))

    def test_no_chosen(self):
        self.assertItemsEqual([('b', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='b', origin='bc', rank=0)])))
        
    def test_chosen_default(self):
        self.assertItemsEqual([('a', 0), ('b', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0),
                           build_thumb(id='b', origin='bc', chosen=True)])))

    def test_doing_ab_test(self):
        self.assertItemsEqual([('a', 0.85), ('aa', 0), ('b', 0.15)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0),
                           build_thumb(id='aa', origin='neon', rank=1),
                           build_thumb(id='b', origin='bc')])))

    def test_chosen_did_bad(self):
        self.assertItemsEqual([('a', 0), ('b', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0,
                                       loads=1500, clicks=3),
                           build_thumb(id='b', origin='bc',
                                       loads=1000, clicks=600)])))
        self.assertItemsEqual([('a', 0), ('aa', 0), ('b', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0,
                                       loads=10, clicks=0),
                           build_thumb(id='aa', origin='neon', rank=1,
                                       loads=1500, clicks=3, chosen=True),          
                           build_thumb(id='b', origin='bc',
                                       loads=1000, clicks=600)])))

    def test_disabled_videos(self):
        self.assertItemsEqual([('a', 0.0), ('aa', 0.85), ('b', 0.15)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0,
                                       enabled=False),
                           build_thumb(id='aa', origin='neon', rank=1),
                           build_thumb(id='b', origin='bc')])))
        
        self.assertItemsEqual([('a', 0.0), ('aa', 1.0), ('b', 0.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0,
                                       enabled=False),
                           build_thumb(id='aa', origin='neon', rank=1),
                           build_thumb(id='b', origin='bc', enabled=False)])))
        
        self.assertItemsEqual([('a', 1.0), ('aa', 0.0), ('b', 0.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0),
                           build_thumb(id='aa', origin='neon', rank=1),
                           build_thumb(id='b', origin='bc', enabled=False)])))
        
        self.assertItemsEqual([('a', 0.0), ('aa', 0.0), ('b', 1.0)],
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0,
                                       enabled=False),
                           build_thumb(id='aa', origin='neon', rank=1,
                                       enabled=False),
                           build_thumb(id='b', origin='bc')])))

        self.assertIsNone(
            self.mastermind._calculate_current_serving_directive(
                VideoInfo(True,
                          [build_thumb(id='a', origin='neon', rank=0,
                                       enabled=False),
                           build_thumb(id='aa', origin='neon', rank=1,
                                       enabled=False),
                           build_thumb(id='b', origin='bc', enabled=False)])))

class TestStatUpdating(unittest.TestCase):
    def setUp(self):
        self.mastermind = Mastermind()

        # Create a fake filesystem because the deltas will get logged to disk
        self.real_tempfile = sys.modules['tempfile']
        self.filesystem = fake_filesystem.FakeFilesystem()
        sys.modules['tempfile'] = fake_tempfile.FakeTempfileModule(
            self.filesystem)

        # Initialize mastermind with some videos
        self.mastermind.update_video_info('vidA', True, [
            build_thumb(id='a', origin='neon', rank=0),
            build_thumb(id='aa', origin='neon', rank=1),
            build_thumb(id='az', origin='bc')])
        self.mastermind.update_video_info('vidB', True, [
            build_thumb(id='b', origin='neon', rank=0),
            build_thumb(id='ba', origin='neon', rank=1, chosen=True),
            build_thumb(id='bz', origin='bc')])

    def tearDown(self):
        sys.modules['tempfile'] = self.real_tempfile

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
        

class TestVideoInfoUpdate(unittest.TestCase):
    def setUp(self):
        self.mastermind = Mastermind()

        # Create a fake filesystem because the deltas will get logged to disk
        self.real_tempfile = sys.modules['tempfile']
        self.filesystem = fake_filesystem.FakeFilesystem()
        sys.modules['tempfile'] = fake_tempfile.FakeTempfileModule(
            self.filesystem)

        # Insert a single video by default
        self.mastermind.update_video_info(
            'vidA', True, [
                build_thumb(id='a', origin='neon', rank=0),
                build_thumb(id='aa', origin='neon', rank=1),
                build_thumb(id='az', origin='bc')])

    def tearDown(self):
        sys.modules['tempfile'] = self.real_tempfile

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
                build_thumb(id='a', origin='neon', rank=0),
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
                build_thumb(id='a', origin='neon', rank=0),
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
    unittest.main()
