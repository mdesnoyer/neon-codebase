#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
from model.features import GistGenerator
from model.predictor import KFlannPredictor
import unittest
import model
import numpy as np
import fake_filesystem
import fake_tempfile
import sys
import random

class TestSaveModel(unittest.TestCase):
    def setUp(self):
        self.real_tempfile = sys.modules['tempfile']
        self.real_os = sys.modules['os']
        self.filesystem = fake_filesystem.FakeFilesystem()
        sys.modules['tempfile'] = fake_tempfile.FakeTempfileModule(
            self.filesystem)
        sys.modules['os'] = fake_filesystem.FakeOsModule(self.filesystem)
        random.seed(32452)
        

    def tearDown(self):
        sys.modules['tempfile'] = self.real_tempfile
        sys.modules['os'] = self.real_os

    def createRandomImage(self, shape=(256,256,3)):
        image = np.array(256*np.random.random(shape),
                         dtype=np.uint8)
        return image

    def trainPredictor(self, predictor):
        '''Train the predictor with some random feature vectors.'''
        for i in range(30):
            predictor.add_image(self.createRandomImage(),
                                np.random.random())

        predictor.train()

    def test_kflann_model(self):
        predictor = KFlannPredictor(GistGenerator(), seed=20472)

        mod = model.Model(predictor, None)

        self.trainPredictor(predictor)

        model.save_model(mod, 'test.model')
        loaded = model.load_model('test.model')

         # Make sure that the loaded model does the same predictions as
        # the original for some random images
        for i in range(10):
            cur_image = self.createRandomImage()
            self.assertAlmostEqual(mod.score(cur_image),
                                   loaded.score(cur_image))

    def test_kflann_untrained(self):
        predictor = KFlannPredictor(GistGenerator(), seed=20472)

        mod = model.Model(predictor, None)
        model.save_model(mod, 'test.model')
        loaded = model.load_model('test.model')

        self.assertEquals(mod.predictor.__class__.__name__,
                          loaded.predictor.__class__.__name__)
        self.assertEquals(mod.predictor.is_trained,
                          loaded.predictor.is_trained)
        self.assertFalse(mod.predictor.is_trained)
        self.assertEquals(len(mod.predictor.data), 0)
        self.assertEquals(len(mod.predictor.data),
                          len(loaded.predictor.data))
        self.assertEquals(len(mod.predictor.scores), 0)
        self.assertEquals(len(mod.predictor.scores),
                          len(loaded.predictor.scores))

class TestThumbnailSelector(unittest.TestCase):
    def setUp(self):
        self.model = model.load_model(os.path.join(os.path.dirname(__file__),
                                                   'simple_model.model'))

    def tearDown(self):
        pass

    def test_smoke_test(self):
        '''Looking for smoke when running the model on a small video.'''
        mov = cv2.VideoCapture(os.path.join(os.path.dirname(__file__),
                                            'test_videos',
                                            'swimmer.mp4'))

        thumb_data = self.model.choose_thumbnails(mov, 5.0)

        self.assertGreater(len(thumb_data), 0)
        self.assertGreater(thumb_data[0][1], thumb_data[1][1])

if __name__ == '__main__':
    unittest.main()
