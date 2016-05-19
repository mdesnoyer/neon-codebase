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
from mastermind.core import Mastermind, ThumbnailInfo, VideoInfo, ModelMapper, ScoreType

from cmsdb import neondata
from cmsdb.neondata import ThumbnailMetadata, ExperimentStrategy, VideoMetadata
import decimal
import fake_filesystem
import fake_tempfile
import json
import logging
from mock import patch, MagicMock
import multiprocessing.pool
import numpy.random
import numpy as np
import pandas
import test_utils.neontest
import test_utils.redis
import tornado.httpclient
import utils.neon
import unittest

_log = logging.getLogger(__name__)

def build_thumb(metadata=neondata.ThumbnailMetadata(None, None),
                base_impressions=0, incremental_impressions=0,
                base_conversions=0, incremental_conversions=0, phash=None):
    if phash is None:
        metadata.phash = numpy.random.randint(1<<30)
    else:
        metadata.phash = phash
    return ThumbnailInfo(metadata, base_impressions, incremental_impressions,
                         base_conversions, incremental_conversions)

#TODO(mdesnoyer) what happens when a video is removed from the db?!?

class TestObjects(test_utils.neontest.TestCase):
    def test_video_info_equals(self):
        video_info_1 = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                                           phash=32,
                                           incremental_impressions=56,
                                           base_impressions=9849,
                                           incremental_conversions=98,
                                           base_conversions=4986)],
            score_type=ScoreType.CLASSICAL)

        video_info_2 = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                                           phash=32,
                                           incremental_impressions=56,
                                           base_impressions=9849,
                                           incremental_conversions=98,
                                           base_conversions=4986)],
            score_type=ScoreType.CLASSICAL)

        self.assertEqual(video_info_1, video_info_2)
        self.assertEqual(repr(video_info_1), repr(video_info_2))

    def test_update_stats_with_wrong_id(self):
        thumb1 = ThumbnailInfo(ThumbnailMetadata('t1', 'v1'),
                               base_impressions=300)
        thumb2 = ThumbnailInfo(ThumbnailMetadata('t2', 'v1'),
                               base_impressions=400)

        with self.assertLogExists(logging.ERROR,
                                  "Two thumbnail ids don't match"):
            self.assertEqual(thumb1.update_stats(thumb2), thumb1)
        self.assertEqual(thumb1.get_impressions(), 300)

class TestCurrentServingDirective(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestCurrentServingDirective, self).setUp()
        numpy.random.seed(1984934)

        # Mock out the redis connection so that it doesn't throw an error
        self.redis_patcher = patch(
            'cmsdb.neondata.blockingRedis.StrictRedis')
        self.redis_mock = self.redis_patcher.start()
        self.redis_mock().get.return_value = None
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)
        logging.getLogger('cmsdb.neondata').propagate = False

        # TODO(wiley): Once we actually listen to the priors but keep
        # serving fractions constant, set frac_adjust_rate to the
        # default setup
        self.mastermind = Mastermind()
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', 
            exp_frac=0.01, holdback_frac=0.01, frac_adjust_rate=1.0))
        logging.getLogger('mastermind.core').reset_sample_counters()

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.redis_patcher.stop()
        logging.getLogger('cmsdb.neondata').propagate = True

    def test_serving_directives_with_priors(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))

        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=1,
                                               ttype='neon',
                                               model_score='3.5')),
                 build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                               ttype='random')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))],
                 score_type=ScoreType.CLASSICAL))[1]

        # TODO (mdesnoyer): Change this test to have the initial model
        # score significantly change the prior serving
        # percentages. That is disabled now because have a highly
        # weighted prior that is wrong (10% vs. 1% say) causes the
        # experiment to run much longer that would be ideal.
        #self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
        self.assertItemsEqual(directive.keys(),
                              ['n2', 'ctr', 'bc', 'n1'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)
    
    def test_model_mapping(self):
        # tests that the mapping from models --> score type is 
        # correct for known models. 
        modelsToTest = ['20130924_textdiff',
        '20130924_crossfade','p_20150722_withCEC_w20',
        '20130924_crossfade_withalg','p_20150722_withCEC_w40',
        '20150206_flickr_slow_memcache','20130924',
        'p_20150722_withCEC_w10','p_20150722_withCEC_wA',
        'p_20150722_withCEC_wNone']
        for model in modelsToTest:
            self.assertEqual(ScoreType.CLASSICAL, 
                ModelMapper.get_model_type(model))

        # test that it correctly adds new models
        with self.assertLogExists(logging.INFO, 
            ('Model %s is not in model dicts; adding it,'
                  ' as score type %s'%(str('unknown_model_5hx'), 
                    str(ScoreType.DEFAULT)))):
            self.assertEqual(ScoreType.DEFAULT,
                ModelMapper.get_model_type('unknown_model_5hx'))
        self.assertTrue(
            ModelMapper.MODEL2TYPE.has_key('unknown_model_5hx'))
        
        # test that invalid score types are mapped to UNKNOWN
        with self.assertLogExists(logging.ERROR, 
            ('Invalid score type specification for model '
             '%s defaulting to UNKNOWN'%('unknown_model_z9i'))):
            ModelMapper._add_model('unknown_model_z9i', 21)
        self.assertEqual(ScoreType.UNKNOWN,
            ModelMapper.get_model_type(
                'unknown_model_z9i'))
        self.assertTrue(
            ModelMapper.MODEL2TYPE.has_key('unknown_model_z9i'))
        with self.assertLogExists(logging.ERROR, 
            ('Model %s with invalid score type %s'
             ' is already in MODEL2TYPE, original score '
            'type remains'%('unknown_model_z9i', 
            str(ScoreType.UNKNOWN)))):
            ModelMapper._add_model('unknown_model_z9i', 'score!')
        self.assertEqual(ScoreType.UNKNOWN,
            ModelMapper.get_model_type(
                'unknown_model_z9i'))
        
        # test that model score_types cannot be changed to
        # invalid values
        ModelMapper._add_model('unknown_model_5hx', 12)
        self.assertEqual(ScoreType.DEFAULT,
            ModelMapper.get_model_type('unknown_model_5hx'))
        
        # test that model score_types can be changed to 
        # valid values.
        ModelMapper._add_model('unknown_model_5hx', 
            ScoreType.UNKNOWN)
        self.assertEqual(ScoreType.UNKNOWN,
            ModelMapper.get_model_type('unknown_model_5hx'))

    def test_priors(self):
        # the computation of the prior has been modified significantly,
        # such that it's not computed based on whether or not the 
        # scoring type thumbnail is the classical (Borda Count) or the
        # new method (Rank Centrality). 
        # in order to test the priors, we have to label the thumbnails
        # with their respective models. This occurs when we call 
        # update_video_info, which isn't heretofor invoked. 
        modelsTested = [['20130924_crossfade', ScoreType.CLASSICAL], 
                        [None, ScoreType.UNKNOWN],
                        ['asdf', ScoreType.RANK_CENTRALITY]]
        thumbnails = [ThumbnailMetadata('n1', 'vid1', rank=0,
                                        ttype='neon', model_score=5.8),
                      ThumbnailMetadata('n2', 'vid1', rank=1,
                                        ttype='neon',
                                        model_score='3.5'),
                      ThumbnailMetadata('ctr', 'vid1',
                                        ttype='random',
                                        model_score=0.2),
                      ThumbnailMetadata('bc', 'vid1', chosen=False,
                                        ttype='brightcove',
                                        model_score=0.)]

        expected_scores = [[1.12, 1.0, 1.0, 1.0],
                           [1.0, 1.0, 1.0, 1.0],
                           [2.44, 1.75, 1.0, 1.0]]

        for n, (model_version, model_type_num) in enumerate(
                                                  modelsTested):
            self.mastermind.update_video_info(
                VideoMetadata('acct1_vid1', 
                              model_version=model_version),
                              thumbnails)
            # ensure that the model type, obtained by name, 
            # is correct. 
            modelType_byname = ModelMapper.get_model_type(model_version)
            self.assertTrue(modelType_byname == model_type_num)
            # acquire the video_info
            m_vid_info = self.mastermind.video_info['acct1_vid1']
            # iterate over each thumbnail, ensuring that it is 
            # correct
            for m,t in enumerate(m_vid_info.thumbnails):
                gpc = self.mastermind._get_prior_conversions(t, m_vid_info)
                self.assertAlmostEqual(gpc, expected_scores[n][m])

    def test_more_conversions_than_impressions(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0,
                                        frac_adjust_rate=1.0))

        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon', model_score=5.8),
                                               base_conversions=2000,
                                               base_impressions=200),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))],
                 score_type=ScoreType.CLASSICAL))[1]

        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['bc'], 0.0)

    def test_inf_model_score(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))

        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon', model_score=5.8)),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=1,
                                               ttype='neon',
                                               model_score=float('nan'))),
                 build_thumb(ThumbnailMetadata('n3', 'vid1', rank=2,
                                               ttype='neon',
                                               model_score='-inf')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))],
                 score_type=ScoreType.CLASSICAL))[1]

        self.assertEquals(len(directive), 4)
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

    def test_ign_breaker_three(self):
        self.mastermind.update_experiment_strategy(
            'testacct123', ExperimentStrategy('gvs3vytvg20ozp78rolqmdfa', exp_frac=1.2, baseline_type='brightcove'))
        self.mastermind.serving_directive = {
            'testacct123_4324552316001': (('gvs3vytvg20ozp78rolqmdfa', '4324552316001'),
                           [
                            ('tid1', 0),
                            ('tid2', 0),
                            ('tid3', 0),
                            ('tid4', 0),
                            ('tid5', 0),
                            ('tid6', 0),
                            ('tid7', 0)
                            ]) }
        self.mastermind.video_info['testacct123_4324552316001'] = VideoInfo(
                'testacct123', True,
                [build_thumb(ThumbnailMetadata(
                    'd6dfa36d8431e795b573263bed0a71e8', '4324552316001', 
                    ctr=None,rank=1,height=720,width=1280,
                    model_version='20130924_crossfade_withalg',
                    phash=4437922592898527388,
                    ttype='neon', model_score=5.654004413457463),
                    phash=14285162934004088064L,
                    incremental_impressions=0,
                    base_impressions=234234234,
                    incremental_conversions=234234234,
                    base_conversions=-1),
                 build_thumb(ThumbnailMetadata(
                     '1e10632ba402134d74bef2eb47ae51af',
                     '4324552316001', rank=3, height=720, width=1280,
                     ctr=None,
                     model_version='20130924_crossfade_withalg',
                     phash=11159919755030691327L,
                     ttype='neon',
                     model_score=5.009933999821487)),
                 build_thumb(ThumbnailMetadata(
                     'bb0dbd191853aa5ce998121e1b6d54d6',
                     '4324552316001', rank=0, height=720, width=1280,
                     ctr=None,
                     phash=4438202951250849932,
                     ttype='random',
                     model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     '050c67f1b38449ea254f1eb3024b999e',
                     '4324552316001', rank=2, height=720, width=1280,
                     ctr=None,
                     model_version='20130924_crossfade_withalg',
                     phash=1949432825026632815,
                     ttype='neon',
                     model_score=5.037118012144308)),
                 build_thumb(ThumbnailMetadata(
                     'ec480d6634dadbae513e9a4fc28e84eb',
                     '4324552316001', rank=0, height=360, width=740,
                     ctr=None,
                     model_version=None,
                     phash=10216194004065988127L,
                     ttype='brightcove',
                     model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     '4dd194c4c4a082bba85a4f3bd57dc854',
                     '4324552316001', rank=0, height=720, width=1280,
                     ctr=None,
                     model_version='20130924_crossfade_withalg',
                     phash=4438202951275983004,
                     ttype='neon',
                     model_score=5.666209793655988)),
                 build_thumb(ThumbnailMetadata(
                     '7833f53877497433fed22ee030a534a8',
                     '4324552316001', rank=0, height=720, width=1280,
                     ctr=None,
                     model_version=None,
                     phash=4437921476290916540,
                     filtered=None,
                     ttype='centerframe',
                     model_score=None))],
                 score_type=ScoreType.CLASSICAL)

        self.mastermind._calculate_new_serving_directive(
            'testacct123_4324552316001')
        self.assertEquals(len(
            self.mastermind.serving_directive['testacct123_4324552316001'][1]),
            7)

    def test_ign_breaker_one(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=0.2,
                                        baseline_type='brightcove'))
        self.mastermind.serving_directive = {
            'acct1_vid1': (('acct1', 'vid1'),
                           [
                            ('tid11', -3242343242342232423423423423442341299999999999999999999999999999999999999999999999999999999999999999999999999.124102984023984230952390582040982309482334534534534521),
                            ('tid12', 3037000499.9760499),
                            ('tid13', 1.0),
                            ('tid14', 0.324234234234),
                            ('tid15', -0.23423111123),
                            ('tid16', 0),
                            ('tid17', 0.0e4000000),
                            ('tid18', 15314e999999990000000000000),
                            ('tid19', 0)
                            ]) }
        self.mastermind.video_info['acct1_vid1'] = VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata(
                    'n1', 'vid1', rank=0, height=720, width=1280,
                    model_version='20130924_crossfade_withalg',
                    phash=14285162934004088064L,
                    urls=['http://blah.invalid.com'],
                    ttype='neon', model_score=5.406484635388814)),
                 build_thumb(ThumbnailMetadata(
                     'n2', 'vid1', rank=0, height=720, width=1280,
                     model_version=None,
                     phash='4576300592785859713',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='random', model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     'n3', 'vid1', rank=0, height=360, width=640,
                     model_version=None,
                     phash='4576300592785859713',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='brightcove', model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     'n4', 'vid1', rank=4, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859715',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.292761188458726)),
                 build_thumb(ThumbnailMetadata(
                     'n5', 'vid1', rank=3, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859716',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.330244313549762)),
                 build_thumb(ThumbnailMetadata(
                     'n6', 'vid1', rank=1, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859717',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.390517351344677)),
                 build_thumb(ThumbnailMetadata(
                     'n7', 'vid1', rank=0, height=360, width=640,
                     model_version=None,
                     phash='4576300592785859718',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='centerframe', model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     'n8', 'vid1', rank=2, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859719',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.3649998500954)),
                 build_thumb(ThumbnailMetadata(
                     'n9', 'vid1', rank=0, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859720',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=None))],
                 score_type=ScoreType.CLASSICAL)

        self.mastermind._calculate_new_serving_directive('acct1_vid1')
        self.assertEquals(len(self.mastermind.serving_directive['acct1_vid1'][1]), 9)

    def test_ign_breaker_two(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=0.2,
                                        baseline_type='brightcove'))
        self.serving_directive = {
            'acct1_vid1': (('acct1', 'vid1'),
                           [
                            ('tid11', ''),
                            ('tid12', 3037000499.9760499),
                            ('tid13', 1.0),
                            ('tid14', 0.324234234234),
                            ('tid15', -0.23423111123),
                            ('tid16', 0),
                            ('tid17', 0),
                            ('tid18', 0),
                            ('tid19', 0)
                            ]) }
        directive = self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata(
                    'n1', 'vid1', rank=0, height=720, width=1280,
                    model_version='20130924_crossfade_withalg',
                    phash='4576300592785859712',
                    urls=['http://blah.invalid.com'],
                    ttype='neon', model_score=5.406484635388814)),
                 build_thumb(ThumbnailMetadata(
                     'n2', 'vid1', rank=0, height=720, width=1280,
                     model_version=None,
                     phash='4576300592785859713',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='random', model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     'n3', 'vid1', rank=0, height=360, width=640,
                     model_version=None,
                     phash='4576300592785859713',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='brightcove', model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     'n4', 'vid1', rank=4, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859715',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.292761188458726)),
                 build_thumb(ThumbnailMetadata(
                     'n5', 'vid1', rank=3, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859716',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.330244313549762)),
                 build_thumb(ThumbnailMetadata(
                     'n6', 'vid1', rank=1, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859717',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.390517351344677)),
                 build_thumb(ThumbnailMetadata(
                     'n7', 'vid1', rank=0, height=360, width=640,
                     model_version=None,
                     phash='4576300592785859718',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='centerframe', model_score=None)),
                 build_thumb(ThumbnailMetadata(
                     'n8', 'vid1', rank=2, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859719',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=5.3649998500954)),
                 build_thumb(ThumbnailMetadata(
                     'n9', 'vid1', rank=0, height=720, width=1280,
                     model_version='20130924_crossfade_withalg',
                     phash='4576300592785859720',
                     urls=['http://blah.invalid2.jpg'],
                     ttype='neon', model_score=None))],
                 score_type=ScoreType.CLASSICAL))[1]

        self.assertEquals(len(directive), 9)
        self.assertAlmostEqual(sum(directive.values()), 1.0)

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
                                           model_score=u'3.5'))],
             score_type=ScoreType.CLASSICAL)
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Now add a baseline thumb
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='random')))
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
                                               ttype='random'))],
                 score_type=ScoreType.CLASSICAL))[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2':0.01, 'ctr':0.99 })

    def test_finding_baseline_thumb(self):
        video_info = VideoInfo('acct1', True, [], score_type=ScoreType.CLASSICAL)

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
                                          ttype='random')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.01, 'ctr1': 0.99 })

        # Finally add a lower rank baseline
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr2', 'vid1', rank=1,
                                          ttype='random')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.01, 'ctr1': 0.0, 'ctr2': 0.99})

    def test_baseline_is_different_type(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', baseline_type='centerframe'))

        # The random frame is not shown if it's not the baseline type
        video_info = VideoInfo('acct1', True, [], score_type=ScoreType.CLASSICAL)
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1', ttype='random')))
        with self.assertLogExists(logging.ERROR,
                                  'No valid thumbnails for video'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    video_info))

        # Add a center thumb and it should be shown because it is the baseline
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('rnd', 'vid1', ttype='centerframe')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'rnd': 1.0, 'ctr': 0.0})

    def test_baseline_is_also_default_type(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', 
            exp_frac=0.01, holdback_frac=0.01, 
            baseline_type='brightcove'))

        video_info = VideoInfo('acct1', True, [], score_type=ScoreType.CLASSICAL)
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1', ttype='random')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1', ttype='brightcove')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n1', 'vid1', ttype='neon')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.99, 'ctr': 0.0, 'n1': 0.01})

        # When the baseline wins, it should be shown for 100%
        video_info.thumbnails[1].base_conv = 5000
        video_info.thumbnails[1].base_imp = 5000
        video_info.thumbnails[2].base_imp = 1000
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 1.0, 'ctr': 0.0, 'n1': 0.0})

    def test_baseline_is_neon(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', baseline_type='neon',
                                        exp_frac=0.01, holdback_frac=0.02))

        video_info = VideoInfo('acct1', True, [],score_type=ScoreType.CLASSICAL)
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1', ttype='random')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('bc', 'vid1', ttype='brightcove')))
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('n1', 'vid1', ttype='neon')))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.99, 'ctr': 0.0, 'n1': 0.01})

        # When the baseline wins, it should be shown for all 100%
        video_info.thumbnails[1].base_imp = 1000
        video_info.thumbnails[2].base_conv = 5000
        video_info.thumbnails[2].base_imp = 5000
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.0, 'ctr': 0.0, 'n1': 1.0})

        # If the default one wins, we show the baseline for the holdback
        video_info.thumbnails[2].base_conv = 0
        video_info.thumbnails[2].base_imp = 1000
        video_info.thumbnails[1].base_conv = 5000
        video_info.thumbnails[1].base_imp = 5000
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'bc': 0.98, 'ctr': 0.0, 'n1': 0.02})

    def test_always_show_baseline(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', always_show_baseline=True,
                                        exp_frac=0.01, holdback_frac=0.02))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=1,
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1', rank=3,
                                           ttype='neon', model_score=3.1)),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='random')),
             build_thumb(ThumbnailMetadata('bc1', 'vid1', chosen=True,
                                           ttype='brightcove'))],
             score_type=ScoreType.CLASSICAL)

        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(directive['bc1'], 0.99)
        for val in directive.values():
            self.assertGreater(val, 0.0)
        self.assertAlmostEqual(sum(directive.values()), 1.0)

        # Now don't show the baseline in the experiment
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', always_show_baseline=False,
                                        exp_frac=0.01, holdback_frac=0.02))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertAlmostEqual(directive['bc1'], 0.99)
        self.assertAlmostEqual(directive['ctr'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['n2'], 0.0)

    def test_multiple_chosen_thumbs(self):
        video_info = VideoInfo('acct1', True, [], score_type=ScoreType.CLASSICAL)

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
                                      model_score=5.8))],
                score_type=ScoreType.CLASSICAL)
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
                                          ttype='random')))
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
                                               ttype='random')),
                 build_thumb(ThumbnailMetadata('bc1', 'vid1', rank=1,
                                               ttype='brightcove')),
                 build_thumb(ThumbnailMetadata('bc2', 'vid1', rank=2,
                                               ttype='brightcove'))],
                 score_type=ScoreType.CLASSICAL))[1]
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
                                               ttype='random'),
                                               phash=64),
                 build_thumb(ThumbnailMetadata('bc1', 'vid1', rank=1,
                                               ttype='brightcove')),
                 build_thumb(ThumbnailMetadata('cust', 'vid1', chosen=True,
                                               ttype='customupload'),
                                               phash=67)],
                 score_type=ScoreType.CLASSICAL))[1]
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
                                               ttype='random')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1',
                                               ttype='brightcove'))],
                 score_type=ScoreType.CLASSICAL))[1]

        self.assertAlmostEqual(directive['n2'], 0.99)
        self.assertAlmostEqual(directive['bc'], 0.0)
        self.assertAlmostEqual(directive['n1'], directive['ctr'])
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
                                            ttype='random')),
              build_thumb(ThumbnailMetadata('bc', 'vid1',
                                            ttype='brightcove'))],
              score_type=ScoreType.CLASSICAL)

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
            'acct1', ExperimentStrategy('acct1', exp_frac=0.01,
            holdback_frac=0.01, only_exp_if_chosen=True))

        video_info = VideoInfo('acct1', True, [], score_type=ScoreType.CLASSICAL)
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
                                          ttype='random')))
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
        video_info = VideoInfo('acct1', True, [], score_type=ScoreType.CLASSICAL)
        self.assertIsNone(
            self.mastermind._calculate_current_serving_directive(
                video_info))

        # A Neon video is the only one that isn't disabled, so show it
        # at all times and throw a warning.
        video_info.thumbnails.extend(
            [ build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
              build_thumb(ThumbnailMetadata('ctr', 'vid1', enabled=False,
                                            ttype='random')),
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

    def test_invalid_strategy(self):
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', experiment_type='unknown'))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8))],
            score_type=ScoreType.CLASSICAL)

        with self.assertLogExists(logging.ERROR, 'Invalid experiment type'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    video_info))

    def test_no_strategy_set(self):

        with self.assertLogExists(logging.ERROR,
                                  'Could not find the experimental strategy'):
            self.assertIsNone(
                self.mastermind._calculate_current_serving_directive(
                    VideoInfo('acct2', True, [
                        build_thumb(ThumbnailMetadata('n1', 'vid1',
                                                      ttype='neon'))],
                        score_type=ScoreType.CLASSICAL)))

    def test_winner_found_override_editor(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', holdback_frac=0.02))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5))],
             score_type=ScoreType.CLASSICAL)

        def _set_winner(thumb_name):
            for thumb in video_info.thumbnails:
                thumb.base_conv = 5000 if thumb.id == thumb_name else 0
                thumb.base_imp = 5000 if thumb.id == thumb_name else 1000

        _set_winner('n2')

        # In the default case with no editor selection or baseline,
        # just show the winner 100% of the time
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 1.0})

        # Now add a baseline, which should be shown as the holdback
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='random')))
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
                                        exp_frac=0.01, 
                                        override_when_done=False))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5))],
             score_type=ScoreType.CLASSICAL)

        def _set_winner(thumb_name):
            for thumb in video_info.thumbnails:
                thumb.base_conv = 5000 if thumb.id == thumb_name else 0
                thumb.base_imp = 5000 if thumb.id == thumb_name else 1000

        _set_winner('n2')
        # There is no editor choice, so show the winner for 100%
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual(directive, {'n1': 0.0, 'n2': 1.0})

        # Now add a baseline, which should be shown as the majority
        video_info.thumbnails.append(
            build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                          ttype='random')))
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
                                               ttype='random')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))],
                 score_type=ScoreType.CLASSICAL)
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertItemsEqual(directive.keys(), ['n2', 'ctr', 'bc', 'n1'])

        # Add some stats where n2 starts to bubble up but doesn't win
        video_info.thumbnails[0].base_imp = 1000
        video_info.thumbnails[0].base_conv = 43
        video_info.thumbnails[1].base_imp = 1000
        video_info.thumbnails[1].base_conv = 50
        video_info.thumbnails[2].base_imp = 1000
        video_info.thumbnails[2].base_conv = 35
        video_info.thumbnails[3].base_imp = 1000
        video_info.thumbnails[3].base_conv = 38

        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        # With the new serving calculation, model score is used to encourage
        # high serving percentages for high score thumbnails.
        # Chosen one gets 5% lift. 
        self.assertEqual(sorted(directive.keys(), key=lambda x: directive[x]),
                         ['ctr', 'bc', 'n1', 'n2'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

    def test_not_enough_impressions_for_winner(self):
        # There needs to be 500 impressions of the winner in order to declare
        # it
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy(
                'acct1', exp_frac=1.0,
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                         base_impressions=50, base_conversions=20),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5)),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='random')),
             build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                           ttype='brightcove'))],
             score_type=ScoreType.CLASSICAL)
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]

        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertAlmostEqual(max(directive.values()), directive['n1'])
        for val in directive.values():
            self.assertGreater(val, 0.0)

        # Now test with a large enough number that floating point
        # numbers go to zero
        video_info.thumbnails[0].base_imp=499
        video_info.thumbnails[0].base_conv=200
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
            ], score_type=ScoreType.CLASSICAL)

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
                                               ttype='random')),
                 build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                               ttype='brightcove'))],
                 score_type=ScoreType.CLASSICAL))[1]
        self.assertItemsEqual(
            sorted(directive.keys(), key=lambda x: directive[x])[2:],
            ['n3', 'ctr', 'bc', 'n1'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertAlmostEqual(directive['n2'], 0.0)
        self.assertAlmostEqual(directive['n4'], 0.0)
        self.assertGreater(directive['n1'], 0.0)
        self.assertGreater(directive['n3'], 0.0)
        self.assertGreater(directive['ctr'], 0.0)
        self.assertGreater(directive['bc'], 0.0)

    def test_not_enough_baseline_impressions(self):
        # There needs to be enough impressions of each thumb in order
        # to shut them off.
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0,
                                        frac_adjust_rate=1.0))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                         base_impressions=3000, base_conversions=700),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5),
                         base_impressions=400, base_conversions=1),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='random'),
                         base_impressions=100, base_conversions=40),
             build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                           ttype='brightcove'),
                         base_impressions=100, base_conversions=40)],
             score_type=ScoreType.CLASSICAL)
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]

        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertGreater(0.001, directive['n1'])
        self.assertGreater(directive['ctr'], 0.05)
        self.assertGreater(directive['bc'], 0.05)
        self.assertGreater(directive['n2'], 0.05)

    def test_much_worse_than_prior(self):
        # When the real ctr is much worse than the prior, for thumbs
        # that we don't know anything about, we drive a lot of
        # traffice there.
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy(
                'acct1', exp_frac=1.0, frac_adjust_rate=1.0,
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8),
                         base_impressions=6000, base_conversions=2),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5)),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='random'),
                         base_impressions=350, base_conversions=1),
             build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                           ttype='brightcove'),
                         base_impressions=1200, base_conversions=2)],
             score_type=ScoreType.CLASSICAL)
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]

        self.assertAlmostEqual(sum(directive.values()), 1.0)
        self.assertAlmostEqual(max(directive.values()), directive['n2'])
        self.assertGreater(0.01, directive['n1'])
        self.assertGreater(0.01, directive['bc'])
        self.assertGreater(directive['ctr'], 0.05) # Not enough imp

    def test_min_conversion_effect(self):
        # The min_conversion number will affect how quickly the
        # experiment comes to conclusion.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', exp_frac=1.0, min_conversion = 50))

        # Total conversion is lower than 50, but the experiment doesn't end.
        # In the following test, without the restriction, the value_left will
        # make the experiment complete.
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=25,
                                               base_impressions=1000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=1,
                                               base_impressions=1000)]))

        self.assertEquals(experiment_state, 'running')
        self.assertLess(value_left, Mastermind.VALUE_THRESHOLD)

        # reduce the min_conversion to 0
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', exp_frac=1.0, min_conversion = 0))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=25,
                                               base_impressions=1000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=1,
                                               base_impressions=1000)]))

        self.assertEquals(experiment_state, 'complete')
        self.assertLess(value_left, Mastermind.VALUE_THRESHOLD)

        # increase the min_conversion to 200
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', exp_frac=1.0, min_conversion = 200))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=120,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=79,
                                               base_impressions=2000)]))
        self.assertEquals(experiment_state, 'running')
        self.assertLess(value_left, Mastermind.VALUE_THRESHOLD)

        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', exp_frac=1.0, min_conversion = 200))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=120,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=80,
                                               base_impressions=2000)]))
        self.assertEquals(experiment_state, 'complete')
        self.assertLess(value_left, Mastermind.VALUE_THRESHOLD)

    def test_min_impressions(self):
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', exp_frac=1.0, min_impressions=500))

        # Total impressions is lower than 500 for each thumb so the
        # experiment shouldn't end
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=100,
                                               base_impressions=400),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=1,
                                               ttype='neon'),
                                               base_conversions=1,
                                               base_impressions=400)]))

        self.assertEquals(experiment_state, 'running')
        self.assertEquals(run_frac, {'n1' : 0.5, 'n2': 0.5 })

        # Now the expermeriment should finish
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', exp_frac=1.0, min_impressions=100))

        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=100,
                                               base_impressions=400),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=1,
                                               ttype='neon'),
                                               base_conversions=1,
                                               base_impressions=400)]))
        self.assertEquals(experiment_state, neondata.ExperimentState.COMPLETE)
        self.assertEquals(run_frac, {'n1' : 1.0, 'n2': 0.0 })

    def test_frac_adjust_rate(self):
        # We can progressively change how the fractions are distributed.
        # frac_adjust_rate=1.0 is true to Thompson Sampling percentage
        # result, frac_adjust_rate = 0.0, then equally distributed.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy(
                'acct1', frac_adjust_rate=1.0,
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=100,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=110,
                                               base_impressions=2000)]))
        self.assertLess(run_frac['n1'], 0.3)

        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy(
                'acct1', frac_adjust_rate=0.,
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=100,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=110,
                                               base_impressions=2000)]))
        self.assertAlmostEqual(run_frac['n1'], 0.5)
        self.assertAlmostEqual(run_frac['n2'], 0.5)

        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy(
                'acct1', frac_adjust_rate=0.5,
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=100,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=110,
                                               base_impressions=2000)]))
        self.assertGreater(run_frac['n1'], 0.3)
        self.assertLess(run_frac['n1'], 0.5)

        # Testing frac_adjust_rate=0.0, but there are a baseline thumbnail.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy(
                'acct1', frac_adjust_rate=0.,
                holdback_frac=0.01, exp_frac=0.01,
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=100,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=110,
                                               base_impressions=2000),

                 build_thumb(ThumbnailMetadata('b1', 'vid1', rank=0,
                                               ttype='random'),
                                               base_conversions=110,
                                               base_impressions=2000)]))
        self.assertAlmostEqual(run_frac['b1'], 0.99)
        self.assertAlmostEqual(run_frac['n1'], 0.005)
        self.assertAlmostEqual(run_frac['n2'], 0.005)

        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy(
                'acct1', frac_adjust_rate=0.0,
                exp_frac = '1.0',
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=100,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon'),
                                               base_conversions=110,
                                               base_impressions=2000),

                 build_thumb(ThumbnailMetadata('b1', 'vid1', rank=0,
                                               ttype='random'),
                                               base_conversions=110,
                                               base_impressions=2000)]))
        self.assertAlmostEqual(run_frac['b1'], 1.0/3.0)
        self.assertAlmostEqual(run_frac['n1'], 1.0/3.0)
        self.assertAlmostEqual(run_frac['n2'], 1.0/3.0)

    def test_frac_with_high_model_score_low_conversion_high_frac(self):
        # In this case, even though the CTR is lower, the higher model score
        # leads to higher serving frac.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1',
                               frac_adjust_rate=1.0,
                               exp_frac='1.0'))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 5.0),
                                               base_conversions=10,
                                               base_impressions=200),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 0.3),
                                               base_conversions=12,
                                               base_impressions=200),
                 build_thumb(ThumbnailMetadata('b1', 'vid1', rank=0,
                                               ttype='random',
                                               model_score = 0.1),
                                               base_conversions=11,
                                               base_impressions=200)],
                score_type = ScoreType.RANK_CENTRALITY))
        self.assertEqual(sorted(run_frac.keys(), key=lambda x: run_frac[x]),
                         ['b1', 'n2', 'n1'])

    def test_frac_with_model_score_prior_but_half_bandit(self):
        # Try the similar setup but frac_adjust_rate = 0.5
        # The base_conversions/impressions are set the same.
        # The fractions will still be determined by the model scores.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy(
                'acct1', frac_adjust_rate=0.5,
                exp_frac = '1.0',
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 5.0),
                                               base_conversions=11,
                                               base_impressions=200),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 3.0),
                                               base_conversions=11,
                                               base_impressions=200),
                 build_thumb(ThumbnailMetadata('b1', 'vid1', rank=0,
                                               ttype='random',
                                               model_score = 0.2),
                                               base_conversions=11,
                                               base_impressions=200)],
                score_type = ScoreType.RANK_CENTRALITY))
        self.assertEqual(sorted(run_frac.keys(), key=lambda x: run_frac[x]),
                         ['b1', 'n2', 'n1'])

    def test_frac_with_model_score_prior_but_full_bandit(self):
        # Try the similar setup but frac_adjust_rate = 1.0
        # When frac_adjust_rate is 0, the fractions are based on model scores.
        # When frac_adjust_rate is 1, the fractions are based on stats.
        # The winner should be the higher conversion ones, not the higher score ones.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy(
                'acct1', frac_adjust_rate=1.0,
                exp_frac = '1.0',
                experiment_type=ExperimentStrategy.MULTIARMED_BANDIT))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 5.0),
                                               base_conversions=105,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 3.0),
                                               base_conversions=110,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('b1', 'vid1', rank=0,
                                               ttype='random',
                                               model_score = 0.2),
                                               base_conversions=115,
                                               base_impressions=2000)],
                score_type = ScoreType.RANK_CENTRALITY))
        self.assertEqual(sorted(run_frac.keys(), key=lambda x: run_frac[x]),
                         ['n1', 'n2', 'b1'])

    def test_frac_with_model_score_prior_with_non_1_exp_frac_and_t_test(self):
        # adding non_exp_thumb is not none case.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', frac_adjust_rate=0.0,
                               exp_frac = '0.5'))
        experiment_state, run_frac, value_left, winner_tid = \
            self.mastermind._calculate_current_serving_directive(
            VideoInfo(
                'acct1', True,
                [build_thumb(ThumbnailMetadata('n1', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 5.0),
                                               base_conversions=100,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('n2', 'vid1', rank=0,
                                               ttype='neon',
                                               model_score = 3.0),
                                               base_conversions=110,
                                               base_impressions=2000),
                 build_thumb(ThumbnailMetadata('b1', 'vid1', rank=0,
                                               ttype='random',
                                               model_score = 0.2),
                                               base_conversions=110,
                                               base_impressions=2000)],
                score_type = ScoreType.RANK_CENTRALITY))
        # All the serving fractions should be the same because prior
        # is ignored in this case.
        self.assertAlmostEqual(run_frac['b1'], 0.5)
        self.assertAlmostEqual(run_frac['n1'], 0.25)
        self.assertAlmostEqual(run_frac['n2'], 0.25)

    def test_sequential_strategy_priors(self):
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1',
                               experiment_type=ExperimentStrategy.SEQUENTIAL,
                               frac_adjust_rate=1.0,
                               exp_frac=0.20))

        video_info = VideoInfo(
            'acct1', True,
            [build_thumb(ThumbnailMetadata('n1', 'vid1',
                                           ttype='neon', model_score=5.8)),
             build_thumb(ThumbnailMetadata('n2', 'vid1',
                                           ttype='neon', model_score=3.5)),
             build_thumb(ThumbnailMetadata('ctr', 'vid1',
                                           ttype='random')),
             build_thumb(ThumbnailMetadata('bc', 'vid1', chosen=True,
                                           ttype='brightcove'))],
             score_type=ScoreType.CLASSICAL)

        # The priors should give precendence to the higher model scores
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertItemsEqual(sorted(directive.keys(),
                                     key=lambda x: directive[x]),
                                     ['n2', 'ctr', 'n1', 'bc'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)
        self.assertEqual(directive['bc'], 0.80)

        # With frac_adjust_rate=0.0, all the thumbs in the experiment
        # should run the same serving fraction
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1',
                               experiment_type=ExperimentStrategy.SEQUENTIAL,
                               frac_adjust_rate=0.0,
                               exp_frac=0.20))
        directive = self.mastermind._calculate_current_serving_directive(
            video_info)[1]
        self.assertEqual({'bc': 0.80, 'n1': 0.2/3, 'n2': 0.2/3, 'ctr': 0.2/3},
                         directive)

    def test_monte_carlo_sequential_strategy(self):
        # Runs a monte carlo test on the sequential strategy where we
        # simulate the experiment running and make sure we don't make
        # the wrong decision too often.
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1',
                               experiment_type=ExperimentStrategy.SEQUENTIAL,
                               frac_adjust_rate=1.0,
                               holdback_frac=0.0,
                               exp_frac=1.0))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_rand', 'acct1_vid1',
                               ttype='random'),
             ThumbnailMetadata('acct1_vid1_n1', 'acct1_vid1',
                               ttype='neon', rank=0),
             ThumbnailMetadata('acct1_vid1_n2', 'acct1_vid1',
                               ttype='neon', rank=1),
             ThumbnailMetadata('acct1_vid1_n3', 'acct1_vid1',
                               ttype='neon', rank=0),
                               ], True)

        TRUE_CTRS = pandas.Series({
            'acct1_vid1_rand' : 0.04,
            'acct1_vid1_n1' : 0.05,
            'acct1_vid1_n2' : 0.03,
            'acct1_vid1_n3' : 0.01
            })
        N_SIMS = 100
        IMP_PER_STEP = 500
        CTR_DECAY_RATE = 0.98
        turned_off = pandas.Series(0.0, index=TRUE_CTRS.index)
        turned_back_on = pandas.Series(0.0, index=TRUE_CTRS.index)
        winner = pandas.Series(0.0, index=TRUE_CTRS.index)
        exp_finished = 0
        
        for sim in range(N_SIMS):
            impressions = pandas.Series(0, index=TRUE_CTRS.index)
            clicks = pandas.Series(0, index=TRUE_CTRS.index)
            ctrs = TRUE_CTRS.copy()
            last_directive = None
        
            for i in range(150):
                cur_directive = self.mastermind.get_directives(
                    ['acct1_vid1']).next()
                cur_directive = pandas.Series(dict(cur_directive[1]))
                # Check for the experiment being done
                if (self.mastermind.experiment_state['acct1_vid1'] == 
                    neondata.ExperimentState.COMPLETE):
                    exp_finished +=1
                    winner[np.argmax(cur_directive)] += 1 
                    break

                # Check for a thumb being turned off
                if last_directive is None:
                    turned_off[cur_directive == 0.0] += 1
                else:
                    turned_off[(cur_directive == 0.0) & 
                               (last_directive > 0.0)] += 1
                    turned_back_on[(cur_directive > 0.0) & 
                                   (last_directive == 0.0)] += 1
                last_directive = cur_directive                

                # Simulate the next bunch of impressions
                new_impressions = np.round(IMP_PER_STEP * cur_directive)
                impressions += new_impressions
                for tid in new_impressions.index:
                    clicks[tid] += np.sum(np.random.random(
                        new_impressions[tid]) < ctrs[tid])
                
                self.mastermind.update_stats_info([
                    ('acct1_vid1', tid, impressions[tid], 0, clicks[tid], 0)
                    for tid in TRUE_CTRS.index])

                ctrs *= CTR_DECAY_RATE

            # Reset mastermind
            self.mastermind.experiment_state['acct1_vid1'] = \
              neondata.ExperimentState.RUNNING
            self.mastermind.update_stats_info([
                    ('acct1_vid1', tid, 0, 0, 0, 0)
                    for tid in TRUE_CTRS.index])

        # Check the results finished is a statistically valid way from
        # what we would expect.
        self.assertGreater(exp_finished, 0.90*N_SIMS)
        self.assertGreater(winner['acct1_vid1_n1'], 0.95*exp_finished)
        self.assertEquals(turned_off['acct1_vid1_n1'], 0)
        self.assertLess(turned_off['acct1_vid1_n2'], 0.01*N_SIMS)
        self.assertGreater(turned_off['acct1_vid1_n3'], 0.5*N_SIMS)
        self.assertEquals(turned_off['acct1_vid1_rand'], 0)
        self.assertEquals(np.max(turned_back_on), 0)        

class TestUpdatingFuncs(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestUpdatingFuncs, self).setUp()
        numpy.random.seed(1984934)

        # Mock out the redis connection so that it doesn't throw an error
        self.redis_patcher = patch(
            'cmsdb.neondata.blockingRedis.StrictRedis')
        self.redis_mock = self.redis_patcher.start()
        self.redis_mock().get.return_value = None
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)

        self.mastermind = Mastermind()
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1', 
            holdback_frac=0.01, exp_frac=0.01))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='random'),
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

    def test_update_with_bad_experiment_strategy_fields(self):
        with self.assertLogExists(logging.ERROR,
                                  'Invalid entry in experiment strategy'):
            self.mastermind.update_experiment_strategy(
                'acct1', ExperimentStrategy('acct1', exp_frac=''))
            self.assertIsNotNone(self.mastermind.experiment_strategy['acct1'])

        with self.assertLogExists(logging.ERROR,
                                  'Invalid entry in experiment strategy'):
            self.mastermind.update_experiment_strategy(
                'acct1', ExperimentStrategy('acct1', holdback_frac=''))
            self.assertIsNotNone(self.mastermind.experiment_strategy['acct1'])

        with self.assertLogExists(logging.ERROR,
                                  'Invalid entry in experiment strategy'):
            self.mastermind.update_experiment_strategy(
                'acct1', ExperimentStrategy('acct1', frac_adjust_rate=''))
            self.assertIsNotNone(self.mastermind.experiment_strategy['acct1'])

        with self.assertLogExists(logging.ERROR,
                                  'Invalid entry in experiment strategy'):
            self.mastermind.update_experiment_strategy(
                'acct1', ExperimentStrategy('acct1', min_conversion=''))
            self.assertIsNotNone(self.mastermind.experiment_strategy['acct1'])

        with self.assertLogExists(logging.ERROR,
                                  'Invalid entry in experiment strategy'):
            self.mastermind.update_experiment_strategy(
                'acct1', ExperimentStrategy('acct1', max_neon_thumbs=''))
            self.assertIsNotNone(self.mastermind.experiment_strategy['acct1'])

    def test_update_experiment_strategy(self):
        with self.assertLogExists(logging.INFO, 'strategy has changed'):
            self.mastermind.update_experiment_strategy(
                'acct2', ExperimentStrategy('acct2'))

        self.mastermind.update_video_info(
            VideoMetadata('acct2_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct2_vid1',
                               ttype='random'),
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
                'acct2', ExperimentStrategy('acct2', 
                    holdback_frac=0.03, 
                    exp_frac=0.75))
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
                               ttype='random'),
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
                               ttype='random'),
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
            [ThumbnailMetadata('tid1', 'acct1_vid1', ttype='random')],
             True)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 1.0)])

    def test_disable_testing_as_param(self):
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='random'),
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
                               ttype='random'),
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
                               ttype='random', enabled=False),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1', ttype='neon',
                               enabled=False)],
             testing_enabled=True)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 0.99),
                                                 ('acct1_vid1_tid2', 0.01)])

    def test_update_video_with_thumbnail_but_no_directive_changes(self):
        #
        self.mastermind.experiment_state['acct1_vid1'] = \
          neondata.ExperimentState.COMPLETE

        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', holdback_frac=0.01, exp_frac=1.0))

        updated_state = self.mastermind.experiment_state['acct1_vid1']
        self.assertEqual(updated_state, neondata.ExperimentState.COMPLETE)

        directives = [x for x in self.mastermind.get_directives()]
        # Directives doesn't change because the experiment has ened.
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 0.99),
                                                 ('acct1_vid1_tid2', 0.01)])
        # First test a case with update video info, but the experiemnt
        # state should stay COMPLETE.
        
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='random'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1',
                               ttype='neon')],
             testing_enabled=True)
        updated_state = self.mastermind.experiment_state['acct1_vid1']
        self.assertEqual(updated_state, neondata.ExperimentState.COMPLETE)
        directives = [x for x in self.mastermind.get_directives()]
        # Directives doesn't change because the experiment has ened.
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 0.99),
                                                 ('acct1_vid1_tid2', 0.01)])

    def test_update_video_with_new_random_thumbnail(self):
        # If we add a new random thumbnail. We don't change the
        # experiment state nor the serving directives.
        self.mastermind.experiment_state['acct1_vid1'] = \
          neondata.ExperimentState.COMPLETE

        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))

        before_directives = [x for x in self.mastermind.get_directives()]
        before_directive_dict = dict((x, y) for x, y in before_directives[0][1])
        updated_state = self.mastermind.experiment_state['acct1_vid1']
        self.assertEqual(updated_state, neondata.ExperimentState.COMPLETE)

        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='random'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1',
                               ttype='neon'),
             ThumbnailMetadata('acct1_vid1_tid3', 'acct1_vid1',
                               ttype='random', chosen=True)],
             testing_enabled=True)
        after_directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(after_directives), 1)
        self.assertEqual(after_directives[0][0], ('acct1', 'acct1_vid1'))
        after_directive_dict = dict((x, y) for x, y in after_directives[0][1])
        self.assertEquals(before_directive_dict, after_directive_dict)
        updated_state = self.mastermind.experiment_state['acct1_vid1']
        self.assertEqual(updated_state, neondata.ExperimentState.COMPLETE)

    def test_update_video_with_new_editor_thumbnail(self):
        #
        self.mastermind.experiment_state['acct1_vid1'] = \
          neondata.ExperimentState.COMPLETE

        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0))

        updated_state = self.mastermind.experiment_state['acct1_vid1']
        self.assertEqual(updated_state, neondata.ExperimentState.COMPLETE)

        directives = [x for x in self.mastermind.get_directives()]
        # Directives doesn't change because the experiment has ened.
        self.assertItemsEqual(directives[0][1], [('acct1_vid1_tid1', 0.99),
                                                 ('acct1_vid1_tid2', 0.01)])

        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_tid1', 'acct1_vid1',
                               ttype='random'),
             ThumbnailMetadata('acct1_vid1_tid2', 'acct1_vid1',
                               ttype='neon'),
             ThumbnailMetadata('acct1_vid1_tid3', 'acct1_vid1',
                               ttype='brightcove', chosen=True)],
             testing_enabled=True)
        updated_state = self.mastermind.experiment_state['acct1_vid1']
        self.assertEqual(updated_state, neondata.ExperimentState.RUNNING)
        directives = [x for x in self.mastermind.get_directives()]
        self.assertEqual(len(directives), 1)
        self.assertEqual(directives[0][0], ('acct1', 'acct1_vid1'))
        # directive changes since the experiement restarted, and
        # the strategy is changed with exp_frac=1.0
        directive_dict = dict((x, y) for x, y in directives[0][1])
        self.assertAlmostEqual(directive_dict['acct1_vid1_tid1'], 
                               directive_dict['acct1_vid1_tid2'])
        self.assertAlmostEqual(directive_dict['acct1_vid1_tid3'],
                               directive_dict['acct1_vid1_tid2'])

        updated_state = self.mastermind.experiment_state['acct1_vid1']
        self.assertEqual(updated_state, neondata.ExperimentState.RUNNING)


class TestStatUpdating(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestStatUpdating, self).setUp()
        numpy.random.seed(1984935)

        # Mock out the redis connection so that it doesn't throw an error
        self.redis_patcher = patch(
            'cmsdb.neondata.blockingRedis.StrictRedis')
        self.redis_mock = self.redis_patcher.start()
        self.redis_mock().get.return_value = None
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)

        self.mastermind = Mastermind()

        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', 
            holdback_frac=0.01, exp_frac=0.01))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_v1t1', 'acct1_vid1',
                               ttype='random'),
             ThumbnailMetadata('acct1_vid1_v1t2', 'acct1_vid1', ttype='neon')])
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid2'),
            [ThumbnailMetadata('acct1_vid2_v2t1', 'acct1_vid2',
                               ttype='random'),
             ThumbnailMetadata('acct1_vid2_v2t2', 'acct1_vid2',
                               ttype='neon', rank=0),
             ThumbnailMetadata('acct1_vid2_v2t3', 'acct1_vid2',
                               ttype='neon', rank=3)])

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.redis_patcher.stop()
        
    def test_initial_stats_update(self):
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 1000, 0, 5, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 1000, 0, 100, 0),
            ('acct1_vid2', 'acct1_vid2_v2t1', 10, 0, 5, 0),
            ('acct1_vid2', 'acct1_vid2_v2t2', 400, 0, 100, 0),
            ('acct1_vid2', 'acct1_vid2_v2t3', 400, 0, 100, 0)])

        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_v1t1', 0.01),
                               ('acct1_vid1_v1t2', 0.99)])
        for val in [x[1] for x in directives[('acct1', 'acct1_vid2')]]:
            self.assertGreater(val, 0.0)

    def test_incremental_stats_update(self):
        # Initial stats
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 100, 0, 1, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 100, 0, 2, 0),
            ('acct1_vid2', 'acct1_vid2_v2t1', 10, 0, 5, 0),
            ('acct1_vid2', 'acct1_vid2_v2t2', 100, 0, 1, 0),
            ('acct1_vid2', 'acct1_vid2_v2t3', 100, 0, 2, 0)])
        directives = dict([x for x in self.mastermind.get_directives()])
        for val in [x[1] for x in directives.values()]:
            self.assertGreater(val, 0.0)

        # Do an incremental update
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', None, 900, None, 4),
            ('acct1_vid1', 'acct1_vid1_v1t2', None, 900, None, 98),
            ('acct1_vid2', 'acct1_vid2_v2t1', None, 0, None, 0),
            ('acct1_vid2', 'acct1_vid2_v2t2', None, 400, None, 150),
            ('acct1_vid2', 'acct1_vid2_v2t3', None, 400, None, 150)])

        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_v1t1', 0.01),
                               ('acct1_vid1_v1t2', 0.99)])
        for val in [x[1] for x in directives[('acct1', 'acct1_vid2')]]:
            self.assertGreater(val, 0.0)

    def test_decimal_from_db(self):
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', decimal.Decimal(1000),
             None, decimal.Decimal(5), None),
            ('acct1_vid1', 'acct1_vid1_v1t2', decimal.Decimal(1000),
             None, decimal.Decimal(100), None),
            ('acct1_vid2', 'acct1_vid2_v2t1', decimal.Decimal(10),
             None, decimal.Decimal(5), None),
            ('acct1_vid2', 'acct1_vid2_v2t2', decimal.Decimal(400),
             None, decimal.Decimal(100), None),
            ('acct1_vid2', 'acct1_vid2_v2t3', decimal.Decimal(400),
             None, decimal.Decimal(99), decimal.Decimal(1))])

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
                ('acct1_unknown', 'v1t1', 1000, None, 5, None)
                ])

    def test_update_stats_for_unknown_thumb(self):
        with self.assertLogExists(logging.WARNING,
                                  'Could not find information for thumbnail'):

            self.mastermind.update_stats_info([
                ('acct1_vid1', 'v1t_where', 1000, None, 5, None)
                ])

    def test_unexpected_strategy_error(self):
        self.mastermind._calculate_new_serving_directive = MagicMock()
        self.mastermind._calculate_new_serving_directive.side_effect = [
            Exception('Ooops')]

        with self.assertLogExists(logging.ERROR, 'Unexpected exception'):
            self.mastermind.update_experiment_strategy(
                'acct1',
                ExperimentStrategy(
                    'acct1',
                    experiment_type=ExperimentStrategy.SEQUENTIAL,
                    frac_adjust_rate=1.0,
                    holdback_frac=0.0,
                    exp_frac=1.0))

class TestExperimentState(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestExperimentState, self).setUp()
        numpy.random.seed(1984937)

        # Mock out the redis connection so that it doesn't throw an error
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)

        self.mastermind = Mastermind()

    def tearDown(self):
        self.redis.stop()
        super(TestExperimentState, self).tearDown()

    def test_update_stats_when_experiment_not_complete(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1'))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_v1t1', 'acct1_vid1',
                               ttype='neon'),
             ThumbnailMetadata('acct1_vid1_v1t2', 'acct1_vid1', ttype='neon')])
        self.mastermind.wait_for_pending_modifies()
        new_video_status = neondata.VideoStatus.get('acct1_vid1')
        self.assertEquals(new_video_status.experiment_state,
                          neondata.ExperimentState.RUNNING)
        
        # Initial stats
        # The acct1_vid1_v1t2 should be favored, but not complete the experiment
        # So, if we do an update, it is possible for acct1_vid1_v1t1 to win
        # again.
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 2000, 0, 130, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 2000, 0, 135, 0)])
        self.mastermind.wait_for_pending_modifies()
        
        directives = dict([x for x in self.mastermind.get_directives()])
        fractions = directives[('acct1', 'acct1_vid1')]
        fractions_test1 = dict([x for x in fractions])
        self.assertEqual(fractions_test1['acct1_vid1_v1t2'], 0.5)
        self.assertEqual(fractions_test1['acct1_vid1_v1t1'], 0.5)
        video_status = neondata.VideoStatus.get('acct1_vid1')
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.RUNNING)
        self.assertEqual(self.mastermind.experiment_state['acct1_vid1'],
                         neondata.ExperimentState.RUNNING)
        self.assertIsNone(video_status.winner_tid)

        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 2000, 0, 300, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 2000, 0, 130, 0)])
        self.mastermind.wait_for_pending_modifies()
         
        directives = dict([x for x in self.mastermind.get_directives()])
        fractions = directives[('acct1', 'acct1_vid1')]
        fractions_test2 = dict([x for x in fractions])
        self.assertGreater(fractions_test2['acct1_vid1_v1t1'],
            fractions_test2['acct1_vid1_v1t2'])
        video_status = neondata.VideoStatus.get('acct1_vid1')
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.COMPLETE)
        self.assertEqual(self.mastermind.experiment_state['acct1_vid1'],
                         neondata.ExperimentState.COMPLETE)
        self.assertEqual(fractions_test2['acct1_vid1_v1t2'], 0.0)
        self.assertEqual(fractions_test2['acct1_vid1_v1t1'], 1.0)
        self.assertEqual(video_status.winner_tid, 'acct1_vid1_v1t1')


    def test_update_stats_when_experiment_complete(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1'))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_v1t1', 'acct1_vid1',
                               ttype='neon'),
             ThumbnailMetadata('acct1_vid1_v1t2', 'acct1_vid1', ttype='neon')])
        self.mastermind.wait_for_pending_modifies()
        new_video_status = neondata.VideoStatus.get('acct1_vid1')
        self.assertEquals(new_video_status.experiment_state,
                          neondata.ExperimentState.RUNNING)
        
        # Initial stats
        # The acct1_vid1_v1t2 win and complete the experiment.
        # So, if we do an update, it is not possible for acct1_vid1_v1t1 to win
        # again.
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 2000, 0, 100, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 2000, 0, 135, 0)])
        self.mastermind.wait_for_pending_modifies()
        
        directives = dict([x for x in self.mastermind.get_directives()])
        fractions = directives[('acct1', 'acct1_vid1')]
        fractions = dict([x for x in fractions])
        video_status = neondata.VideoStatus.get('acct1_vid1')
        self.assertGreater(fractions['acct1_vid1_v1t2'],
            fractions['acct1_vid1_v1t1'])
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.COMPLETE)
        self.assertEqual(self.mastermind.experiment_state['acct1_vid1'],
                         neondata.ExperimentState.COMPLETE)
        self.assertEqual(video_status.winner_tid, 'acct1_vid1_v1t2')
        self.assertEqual(fractions['acct1_vid1_v1t1'], 0.0)
        self.assertEqual(fractions['acct1_vid1_v1t2'], 1.0)

        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 2000, 0, 300, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 2000, 0, 130, 0)])
        self.mastermind.wait_for_pending_modifies()

        # The experiment was already completed, so the winner should still
        # be the same
        directives = dict([x for x in self.mastermind.get_directives()])
        directives = dict([x for x in self.mastermind.get_directives()])
        fractions = directives[('acct1', 'acct1_vid1')]
        fractions = dict([x for x in fractions])
        self.assertEquals(fractions['acct1_vid1_v1t1'], 0.0)
        self.assertEquals(fractions['acct1_vid1_v1t2'], 1.0)
        video_status = neondata.VideoStatus.get('acct1_vid1')
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.COMPLETE)
        self.assertEqual(self.mastermind.experiment_state['acct1_vid1'],
                         neondata.ExperimentState.COMPLETE)
        self.assertEqual(video_status.winner_tid, 'acct1_vid1_v1t2')

    def test_update_experiment_state_directive(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1'))
        self.mastermind.update_video_info(
            VideoMetadata('acct1_vid1'),
            [ThumbnailMetadata('acct1_vid1_v1t1', 'acct1_vid1',
                               ttype='neon'),
             ThumbnailMetadata('acct1_vid1_v1t2', 'acct1_vid1', ttype='neon')])
        self.mastermind.wait_for_pending_modifies()
        new_video_status = neondata.VideoStatus.get('acct1_vid1')
        self.assertEquals(new_video_status.experiment_state,
                          neondata.ExperimentState.RUNNING)
        
        # Set the experiment state to be complete
        thumbnail_status_1 = neondata.ThumbnailStatus(
            'acct1_vid1_v1t1',
            serving_frac = 0.30,
            ctr = 0.02)
        thumbnail_status_2 = neondata.ThumbnailStatus(
            'acct1_vid1_v1t2',
            serving_frac = 0.70,
            ctr = 0.03)
        video_status = neondata.VideoStatus('acct1_vid1',
            neondata.ExperimentState.COMPLETE, 'acct1_vid1_v1t2', 0.01)
        self.mastermind.update_experiment_state_directive(
            'acct1_vid1', video_status,
            [thumbnail_status_1, thumbnail_status_2])
        directives = dict([x for x in self.mastermind.get_directives()])
        fractions = directives[('acct1', 'acct1_vid1')]
        fractions = dict([x for x in fractions])
        # check the fractions
        self.assertEquals(fractions['acct1_vid1_v1t1'], 0.3)
        self.assertEquals(fractions['acct1_vid1_v1t2'], 0.7)

        # run the update
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 2000, 0, 100, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 2000, 0, 135, 0)])
        self.mastermind.wait_for_pending_modifies()
        directives = dict([x for x in self.mastermind.get_directives()])
        fractions = directives[('acct1', 'acct1_vid1')]
        fractions = dict([x for x in fractions])
        # The fractions should not have changed, because the
        # experiment was complete
        self.assertEquals(fractions['acct1_vid1_v1t1'], 0.3)
        self.assertEquals(fractions['acct1_vid1_v1t2'], 0.7)

        # Set the experiment state to be not complete
        video_status = neondata.VideoStatus(
            'acct1_vid1', neondata.ExperimentState.UNKNOWN,
            'acct1_vid1_v1t2', 0.01)
        self.mastermind.update_experiment_state_directive(
            'acct1_vid1', video_status,
            [thumbnail_status_1, thumbnail_status_2])

        # run the update
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_v1t1', 2000, 10, 100, 0),
            ('acct1_vid1', 'acct1_vid1_v1t2', 2000, 10, 135, 0)])
        self.mastermind.wait_for_pending_modifies()
        
        directives = dict([x for x in self.mastermind.get_directives()])
        fractions = directives[('acct1', 'acct1_vid1')]
        fractions = dict([x for x in fractions])
        # The fractions should have been recalculated
        self.assertNotEquals(fractions['acct1_vid1_v1t1'], 0.3)
        self.assertNotEquals(fractions['acct1_vid1_v1t2'], 0.7)

    def test_update_experiment_state_directive_none_frac(self):
        # Set the experiment state to be complete
        thumbnail_status_1 = neondata.ThumbnailStatus(
            'acct1_vid1_v1t1',
            serving_frac = None,
            ctr = 0.02)
        thumbnail_status_2 = neondata.ThumbnailStatus(
            'acct1_vid1_v1t2',
            serving_frac = 0.70,
            ctr = 0.03)
        video_status = neondata.VideoStatus('acct1_vid1',
            neondata.ExperimentState.COMPLETE, 'acct1_vid1_v1t2', 0.01)

        self.mastermind.update_experiment_state_directive(
            'acct1_vid1', video_status,
            [thumbnail_status_1, thumbnail_status_2])

        # The experiment state is unknown, 
        self.assertEquals(self.mastermind.experiment_state['acct1_vid1'],
                          neondata.ExperimentState.UNKNOWN)
        self.assertEquals(len([x for x in self.mastermind.get_directives()]),0)
            

    def test_update_experiment_state_directive_wrong_thumbnail_status(self):
        with self.assertLogExists(
                logging.ERROR,
                'ThumbnailStatus video id acct1_vid3 does not match'):
            # Set the experiment state to be complete
            thumbnail_status_1 = neondata.ThumbnailStatus(
                'acct1_vid3_v1t1',
                serving_frac = 0.30,
                ctr = 0.02)
            thumbnail_status_2 = neondata.ThumbnailStatus(
                'acct1_vid1_v1t2',
                serving_frac = 0.70,
                ctr = 0.03)
            video_status = neondata.VideoStatus('acct1_vid1',
                neondata.ExperimentState.COMPLETE, 'acct1_vid1_v1t2', 0.01)
            
            self.mastermind.update_experiment_state_directive(
                'acct1_vid1', video_status,
                [thumbnail_status_1, thumbnail_status_2])
            

            self.assertEquals(self.mastermind.experiment_state['acct1_vid1'],
                              neondata.ExperimentState.UNKNOWN)
            self.assertEquals(
                len([x for x in self.mastermind.get_directives()]), 0)

    def test_update_experiment_state_directive_not_sum_1(self):
        with self.assertLogExists(
                logging.ERROR,
                'ThumbnailStatus of video id acct1_vid1 does not sum to 1.0'):
            # Set the experiment state to be complete
            thumbnail_status_1 = neondata.ThumbnailStatus(
                'acct1_vid1_v1t1',
                serving_frac = 0.31,
                ctr = 0.02)
            thumbnail_status_2 = neondata.ThumbnailStatus(
                'acct1_vid1_v1t2',
                serving_frac = 0.70,
                ctr = 0.03)
            video_status = neondata.VideoStatus('acct1_vid1',
                neondata.ExperimentState.COMPLETE, 'acct1_vid1_v1t2', 0.01)
            
            self.mastermind.update_experiment_state_directive(
                'acct1_vid1', video_status,
                [thumbnail_status_1, thumbnail_status_2])
            
            self.assertEquals(self.mastermind.experiment_state['acct1_vid1'],
                              neondata.ExperimentState.UNKNOWN)
            self.assertEquals(
                len([x for x in self.mastermind.get_directives()]), 0)

    def test_update_experiment_state_no_thumb_info(self):
        video_status = neondata.VideoStatus(
            'acct1_vid1',
            neondata.ExperimentState.COMPLETE, 'acct1_vid1_v1t2', 0.01)

        self.mastermind.update_experiment_state_directive(
            'acct1_vid1', video_status,
            [])

        self.assertEquals(len([x for x in self.mastermind.get_directives()]),0)

    def test_update_experiment_state_empty_string_serving_frac(self):
        # Set the experiment state to be complete
        thumbnail_status_1 = neondata.ThumbnailStatus(
            'acct1_vid1_v1t1',
            serving_frac = '',
            ctr = 0.02)
        thumbnail_status_2 = neondata.ThumbnailStatus(
            'acct1_vid1_v1t2',
            serving_frac = 0.70,
            ctr = 0.03)
        video_status = neondata.VideoStatus('acct1_vid1',
            neondata.ExperimentState.COMPLETE, 'acct1_vid1_v1t2', 0.01)

        self.mastermind.update_experiment_state_directive(
            'acct1_vid1', video_status,
            [thumbnail_status_1, thumbnail_status_2])

        # The experiment state is unknown, 
        self.assertEquals(self.mastermind.experiment_state['acct1_vid1'],
                          neondata.ExperimentState.UNKNOWN)
        self.assertEquals(len([x for x in self.mastermind.get_directives()]),0)

    def test_update_experiment_state_missing_thumb_data(self):
        # Set the experiment state to be complete
        thumbnail_status_2 = neondata.ThumbnailStatus(
            'acct1_vid1_v1t2',
            serving_frac = 0.70,
            ctr = 0.03)
        video_status = neondata.VideoStatus('acct1_vid1',
            neondata.ExperimentState.COMPLETE, 'acct1_vid1_v1t2', 0.01)

        self.mastermind.update_experiment_state_directive(
            'acct1_vid1', video_status,
            [None, thumbnail_status_2])

        # The experiment state is unknown, 
        self.assertEquals(self.mastermind.experiment_state['acct1_vid1'],
                          neondata.ExperimentState.UNKNOWN)
        self.assertEquals(len([x for x in self.mastermind.get_directives()]),0)

class TestStatusUpdatesInDb(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(TestStatusUpdatesInDb, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()
        self.addCleanup(neondata.DBConnection.clear_singleton_instance)

        # Mock out the http callback
        self.http_patcher = patch('mastermind.core.utils.http')
        self.http_mock = self._future_wrap_mock(
            self.http_patcher.start().send_request,
            require_async_kw=True)
        self.http_mock.side_effect = \
          lambda x, **kw: tornado.httpclient.HTTPResponse(x, 200)

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
                              ttype='random'),
            ThumbnailMetadata('acct1_vid1_bc', 'acct1_vid1', chosen=True,
                              ttype='brightcove')]

        ThumbnailMetadata.save_all(self.thumbnails)
        self.video_metadata = VideoMetadata(
            'acct1_vid1', request_id='job1',
            tids=[x.key for x in self.thumbnails])
        self.video_metadata.save()
        self.request = neondata.NeonApiRequest('job1', 'acct1', 'vid1')
        self.request.state = neondata.RequestState.FINISHED
        self.request.save()
        
        self.mastermind.update_video_info(self.video_metadata, self.thumbnails)
        self._wait_for_db_updates()

        self.http_mock.reset_mock()

    def tearDown(self):
        self.mastermind.wait_for_pending_modifies()
        self.http_patcher.stop()
        self.redis.stop()
        super(TestStatusUpdatesInDb, self).tearDown()

    def _wait_for_db_updates(self):
        self.mastermind.wait_for_pending_modifies()

    def test_db_when_experiment_running(self):
        video = VideoMetadata.get('acct1_vid1')
        thumbs = neondata.ThumbnailStatus.get_many(video.thumbnail_ids)
        directive = dict([(x.get_id(), x.serving_frac) for x in thumbs])
        self.assertItemsEqual(directive.keys(),
                         ['acct1_vid1_n2', 'acct1_vid1_ctr', 'acct1_vid1_bc',
                          'acct1_vid1_n1'])
        self.assertAlmostEqual(sum(directive.values()), 1.0)
        for val in directive.values():
            self.assertGreater(val, 0.0)

        video_status = neondata.VideoStatus.get(video.key)
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.RUNNING)
        self.assertGreater(video_status.experiment_value_remaining,
                           0.10)
        self.assertIsNone(video_status.winner_tid)

    def test_db_experiment_disabled(self):
        self.mastermind.update_video_info(self.video_metadata, self.thumbnails,
                                          False)
        self._wait_for_db_updates()

        thumbs = neondata.ThumbnailStatus.get_many(
            self.video_metadata.thumbnail_ids)
        video = neondata.VideoStatus.get('acct1_vid1')
        directive = dict([(x.get_id(), x.serving_frac) for x in thumbs])
        self.assertEqual(directive, {'acct1_vid1_bc':1.0,
                                     'acct1_vid1_n1':0.0,
                                     'acct1_vid1_n2':0.0,
                                     'acct1_vid1_ctr':0.0})
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.DISABLED)
        self.assertIsNone(video.winner_tid)

    def test_experiment_state_no_change(self):
        video = neondata.VideoStatus.get('acct1_vid1')
        video.experiment_state = neondata.ExperimentState.RUNNING
        video.save()

        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_n1', 100, None, 1, None),
            ('acct1_vid1', 'acct1_vid1_bc', 110, None, 1, None),
            ])
        self._wait_for_db_updates()

        video = neondata.VideoStatus.get('acct1_vid1')
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.RUNNING)

        # Make sure no callback was sent
        self.assertEquals(self.http_mock.call_count, 0)

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
        video = neondata.VideoStatus.get('acct1_vid1')
        self.assertEqual(video.experiment_state,
                         neondata.ExperimentState.DISABLED)
        self.assertIsNone(video.winner_tid)

    def test_db_experiment_finished(self):
        self.request.callback_url = 'http://some_callback.url'
        self.request.state = neondata.RequestState.SERVING
        self.request.save()
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'n2', 5000, 0, 200, 0),
            ('acct1_vid1', 'n1', 5000, 0, 50, 0),
            ('acct1_vid1', 'bc', 5000, 0, 10, 0),
            ('acct1_vid1', 'ctr', 5000, 0, 20, 0)])
        self._wait_for_db_updates()

        video = VideoMetadata.get('acct1_vid1')
        thumbs = neondata.ThumbnailStatus.get_many(video.thumbnail_ids)
        directive = dict([(x.get_id(), x.serving_frac) for x in thumbs])
        self.assertEqual(directive, {'acct1_vid1_bc':0.0,
                                     'acct1_vid1_n1':0.0,
                                     'acct1_vid1_n2':0.98,
                                     'acct1_vid1_ctr':0.02})

        video_status = neondata.VideoStatus.get(video.key)
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.COMPLETE)
        self.assertLess(video_status.experiment_value_remaining,
                        0.05)
        self.assertEqual(video_status.winner_tid, 'acct1_vid1_n2')
        ctrs = dict([(x.get_id(), x.ctr) for x in thumbs])
        self.assertAlmostEqual(ctrs['acct1_vid1_bc'], 10./5000)
        self.assertAlmostEqual(ctrs['acct1_vid1_n1'], 50./5000)
        self.assertAlmostEqual(ctrs['acct1_vid1_n2'], 200./5000)
        self.assertAlmostEqual(ctrs['acct1_vid1_ctr'], 20./5000)

        # Check the callback was sent
        self.assertEquals(self.http_mock.call_count, 1)
        cargs, kwargs = self.http_mock.call_args
        cb_request = cargs[0]
        self.assertEquals(cb_request.url, 'http://some_callback.url')
        self.assertDictContainsSubset(
            { 'experiment_state' : neondata.ExperimentState.COMPLETE,
              'winner_thumbnail' : 'acct1_vid1_n2'
              },
            json.loads(cb_request.body))

    def test_db_experiment_finished_not_serving(self):
        self.request.callback_url = 'http://some_callback.url'
        self.request.state = neondata.RequestState.FINISHED
        self.request.save()
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'n2', 5000, 0, 200, 0),
            ('acct1_vid1', 'n1', 5000, 0, 50, 0),
            ('acct1_vid1', 'bc', 5000, 0, 10, 0),
            ('acct1_vid1', 'ctr', 5000, 0, 20, 0)])
        self._wait_for_db_updates()

        video = VideoMetadata.get('acct1_vid1')
        thumbs = neondata.ThumbnailStatus.get_many(video.thumbnail_ids)
        directive = dict([(x.get_id(), x.serving_frac) for x in thumbs])
        self.assertEqual(directive, {'acct1_vid1_bc':0.0,
                                     'acct1_vid1_n1':0.0,
                                     'acct1_vid1_n2':0.98,
                                     'acct1_vid1_ctr':0.02})

        video_status = neondata.VideoStatus.get(video.key)
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.COMPLETE)
        self.assertLess(video_status.experiment_value_remaining,
                        0.05)
        self.assertEqual(video_status.winner_tid, 'acct1_vid1_n2')
        ctrs = dict([(x.get_id(), x.ctr) for x in thumbs])
        self.assertAlmostEqual(ctrs['acct1_vid1_bc'], 10./5000)
        self.assertAlmostEqual(ctrs['acct1_vid1_n1'], 50./5000)
        self.assertAlmostEqual(ctrs['acct1_vid1_n2'], 200./5000)
        self.assertAlmostEqual(ctrs['acct1_vid1_ctr'], 20./5000)

        # Check the callback was not sent
        self.assertEquals(self.http_mock.call_count, 0)
        

    def test_db_override_thumb(self):
        self.mastermind.update_experiment_strategy(
            'acct1', ExperimentStrategy('acct1', exp_frac=1.0,
                                        holdback_frac=0.02,
                                        chosen_thumb_overrides=True))
        self._wait_for_db_updates()

        video = VideoMetadata.get('acct1_vid1')
        thumbs = neondata.ThumbnailStatus.get_many(video.thumbnail_ids)
        directive = dict([(x.get_id(), x.serving_frac) for x in thumbs])
        self.assertEqual(directive, {'acct1_vid1_bc':1.0, 'acct1_vid1_n1':0.0,
                                     'acct1_vid1_n2':0.0,
                                     'acct1_vid1_ctr':0.0})
        video_status = neondata.VideoStatus.get(video.key)
        self.assertEqual(video_status.experiment_state,
                         neondata.ExperimentState.OVERRIDE)
        self.assertIsNone(video_status.winner_tid)

    def test_sequential_strategy_turn_off_bad_thumbs(self):
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1',
                               experiment_type=ExperimentStrategy.SEQUENTIAL,
                               frac_adjust_rate=0.0,
                               exp_frac=1.0))

        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_bc', 1000, 0, 90, 0),
            ('acct1_vid1', 'acct1_vid1_n1', 1000, 0, 100, 0),
            ('acct1_vid1', 'acct1_vid1_n2', 1000, 0, 69, 0),
            ('acct1_vid1', 'acct1_vid1_ctr', 1000, 0, 80, 0),])
        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_bc', 1./3),
                               ('acct1_vid1_n1', 1./3),
                               ('acct1_vid1_n2', 0.0),
                               ('acct1_vid1_ctr', 1./3)])
        self.mastermind.wait_for_pending_modifies()

        # Now, if more stats come in that drop the CTR of the running
        # thumbs, we should not turn on the thumbnail that was turned
        # off.
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_bc', 1000, 1000, 90, 45),
            ('acct1_vid1', 'acct1_vid1_n1', 1000, 1000, 100, 50),
            ('acct1_vid1', 'acct1_vid1_n2', 1000, 0, 69, 0),
            ('acct1_vid1', 'acct1_vid1_ctr', 1000, 1000, 80, 40),])
        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_bc', 1./3),
                               ('acct1_vid1_n1', 1./3),
                               ('acct1_vid1_n2', 0.0),
                               ('acct1_vid1_ctr', 1./3)])

        self.mastermind.wait_for_pending_modifies()

        # Now check the thumbnail status
        tstatus1 = neondata.ThumbnailStatus.get('acct1_vid1_n1')
        self.assertAlmostEquals(tstatus1.serving_frac, 1./3)
        self.assertAlmostEquals(tstatus1.ctr, 0.075)
        self.assertEquals(tstatus1.imp, 2000)
        self.assertEquals(tstatus1.conv, 150)
        self.assertEquals([x[1] for x in tstatus1.serving_history],
                          [0.25, 1./3])
        tstatus2 = neondata.ThumbnailStatus.get('acct1_vid1_n2')
        self.assertAlmostEquals(tstatus2.serving_frac, 0.0)
        self.assertAlmostEquals(tstatus2.ctr, 0.069)
        self.assertEquals(tstatus2.imp, 1000)
        self.assertEquals(tstatus2.conv, 69)
        self.assertEquals([x[1] for x in tstatus2.serving_history],
                          [0.25, 0.0])

        # Next, make another thumb bad. It shouldn't turn off because
        # we want to keep 3 thumbs running.
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_bc', 1000, 1000, 90, 45),
            ('acct1_vid1', 'acct1_vid1_n1', 1000, 1000, 100, 50),
            ('acct1_vid1', 'acct1_vid1_n2', 1000, 0, 69, 0),
            ('acct1_vid1', 'acct1_vid1_ctr', 1000, 2000, 80, 40),])
        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_bc', 1./3),
                               ('acct1_vid1_n1', 1./3),
                               ('acct1_vid1_n2', 0.0),
                               ('acct1_vid1_ctr', 1./3)])

        # Finally remove an off thumb from the list and make sure it's
        # handled properly.
        del self.thumbnails[1]
        self.mastermind.update_video_info(self.video_metadata,
                                          self.thumbnails)
        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_bc', 1./3),
                               ('acct1_vid1_n1', 1./3),
                               ('acct1_vid1_ctr', 1./3)])

    def test_sequential_strategy_found_winner(self):
        self.mastermind.update_experiment_strategy(
            'acct1',
            ExperimentStrategy('acct1',
                               experiment_type=ExperimentStrategy.SEQUENTIAL,
                               frac_adjust_rate=0.0,
                               holdback_frac=0.01,
                               exp_frac=1.0))

        # This should not have a winner. Pairwise it would, but on
        # aggregate, we don't hit a p-value of 0.95
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_bc', 1000, 0, 78, 0),
            ('acct1_vid1', 'acct1_vid1_n1', 1000, 0, 100, 0),
            ('acct1_vid1', 'acct1_vid1_n2', 1000, 0, 78, 0),
            ('acct1_vid1', 'acct1_vid1_ctr', 1000, 0, 78, 0),])
        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_bc', 0.25),
                               ('acct1_vid1_n1', 0.25),
                               ('acct1_vid1_n2', 0.25),
                               ('acct1_vid1_ctr', 0.25)])
        self.mastermind.wait_for_pending_modifies()

        vstatus = neondata.VideoStatus.get('acct1_vid1')
        self.assertEquals(vstatus.experiment_state,
                          neondata.ExperimentState.RUNNING)
        self.assertIsNone(vstatus.winner_tid)
        self.assertEquals(len(vstatus.state_history), 1)

        # Now we will have a winner
        self.mastermind.update_stats_info([
            ('acct1_vid1', 'acct1_vid1_bc', 1000, 0, 78, 0),
            ('acct1_vid1', 'acct1_vid1_n1', 1100, 0, 120, 0),
            ('acct1_vid1', 'acct1_vid1_n2', 1000, 0, 78, 0),
            ('acct1_vid1', 'acct1_vid1_ctr', 1000, 0, 78, 0),])
        directives = dict([x for x in self.mastermind.get_directives()])
        self.assertItemsEqual(directives[('acct1', 'acct1_vid1')],
                              [('acct1_vid1_bc', 0.00),
                               ('acct1_vid1_n1', 0.99),
                               ('acct1_vid1_n2', 0.0),
                               ('acct1_vid1_ctr', 0.01)])
        self.mastermind.wait_for_pending_modifies()

        vstatus = neondata.VideoStatus.get('acct1_vid1')
        self.assertEquals(vstatus.experiment_state,
                          neondata.ExperimentState.COMPLETE)
        self.assertEquals(vstatus.winner_tid, 'acct1_vid1_n1')
        self.assertEquals([x[1] for x in vstatus.state_history], 
                          [neondata.ExperimentState.RUNNING,
                           neondata.ExperimentState.COMPLETE])

        # Check the thumbnail status
        tstatus_bc = neondata.ThumbnailStatus.get('acct1_vid1_bc')
        self.assertAlmostEqual(tstatus_bc.serving_frac, 0.00)
        self.assertAlmostEqual(tstatus_bc.ctr, 0.078)
        self.assertEqual(tstatus_bc.imp, 1000)
        self.assertEqual(tstatus_bc.conv, 78)
        self.assertEqual(zip(*tstatus_bc.serving_history)[1],
                         (0.25, 0.00))
        tstatus_n1 = neondata.ThumbnailStatus.get('acct1_vid1_n1')
        self.assertAlmostEqual(tstatus_n1.serving_frac, 0.99)
        self.assertAlmostEqual(tstatus_n1.ctr, 120./1100)
        self.assertEqual(tstatus_n1.imp, 1100)
        self.assertEqual(tstatus_n1.conv, 120)
        self.assertEqual(zip(*tstatus_n1.serving_history)[1],
                         (0.25, 0.99))
        tstatus_ctr = neondata.ThumbnailStatus.get('acct1_vid1_ctr')
        self.assertAlmostEqual(tstatus_ctr.serving_frac, 0.01)
        self.assertAlmostEqual(tstatus_ctr.ctr, 0.078)
        self.assertEqual(tstatus_ctr.imp, 1000)
        self.assertEqual(tstatus_ctr.conv, 78)
        self.assertEqual(zip(*tstatus_ctr.serving_history)[1],
                         (0.25, 0.01))
        

class TestModifyDatabase(test_utils.neontest.TestCase):
    def setUp(self):
        self.neondata_patcher = patch('mastermind.core.neondata')
        self.datamock = self.neondata_patcher.start()
        super(TestModifyDatabase, self).setUp()

    def tearDown(self):
        self.neondata_patcher.stop()
        super(TestModifyDatabase, self).tearDown()

    def test_unexpected_exception_video_modify(self):
        self.datamock.VideoStatus.modify.side_effect = [
            IOError('Some weird error')]
        with self.assertLogExists(logging.ERROR,
                                  'Unhandled exception when updating video'):
            with self.assertRaises(IOError):
                mastermind.core._modify_video_info(MagicMock(),
                                                   'vid1',
                                                   'state',
                                                   7.6,
                                                   None)

    def test_unexpected_exception_serving_frac_modify(self):
        self.datamock.ThumbnailStatus.modify_many.side_effect = [
            IOError('Some weird error')]
        with self.assertLogExists(logging.ERROR,
                                  'Unhandled exception when updating thumbs'):
            with self.assertRaises(IOError):
                mastermind.core._modify_many_serving_fracs(
                    MagicMock(),
                    'vid1',
                    {'t1': 0.0, 't2': 0.99},
                    mastermind.core.VideoInfo(
                        'acct1', True,
                        [build_thumb(ThumbnailMetadata('t1', 'vid1')),
                         build_thumb(ThumbnailMetadata('t2', 'vid1'))],
                         score_type=ScoreType.CLASSICAL))

if __name__ == '__main__':
    utils.neon.InitNeon()
    test_utils.neontest.main()
