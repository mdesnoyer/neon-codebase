#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cvutils.imageutils import PILImageUtils
import logging
from mock import MagicMock, patch
from model.aquila_inference_pb2 import AquilaRequest, AquilaResponse
import model.predictor
import numpy as np
import numpy.testing
import test_utils.neontest
import tornado.testing
import unittest
import utils.neon

_log = logging.getLogger(__name__)

class TestDeepnetPredictorGoodConnection(test_utils.neontest.AsyncTestCase):
    '''Tests the Deepnet Predictor but assumes connection is good.

    Test for connection issues are handled seperately.
    '''
    def setUp(self):
        self.aq_conn_mock = MagicMock()
        self.aq_conn_mock.get_ip.return_value = { '10.0.56.1' }
        self.predictor = model.predictor.DeepnetPredictor(
            aquila_connection=self.aq_conn_mock)

        # Fake a good connection
        self.predictor.stub = MagicMock()
        self.predictor._ready.set()
        self.mock_regress_call = self._future_wrap_mock(
            self.predictor.stub.Regress.future)

        self.image = PILImageUtils.to_cv(PILImageUtils.create_random_image(
            480, 640))
        
        logging.getLogger('model.predictor').reset_sample_counters()

        super(TestDeepnetPredictorGoodConnection, self).setUp()

    def tearDown(self):
        del self.predictor
        super(TestDeepnetPredictorGoodConnection, self).tearDown()

    @tornado.testing.gen_test
    def test_aquilav1_response(self):
        response = AquilaResponse()
        response.valence.append(0.42)
        self.mock_regress_call.side_effect = [response]

        score, vec, vers = yield self.predictor.predict(self.image,
                                                        async=True)

        self.assertAlmostEquals(score, 0.42)
        self.assertIsNone(vec)
        self.assertEquals(vers, 'aqv1.1.250')

        self.assertEquals(self.predictor.active, 0)

    @tornado.testing.gen_test
    def test_aquilav2_known_model(self):
        features = np.random.randn(1024)
        
        response = AquilaResponse()
        response.valence.extend(features)
        response.model_version = '20160713-test'
        self.mock_regress_call.side_effect = [response]

        score, vec, vers = yield self.predictor.predict(self.image,
                                                        async=True)

        self.assertIsNotNone(score)
        numpy.testing.assert_allclose(features, vec)
        self.assertEquals(vers, '20160713-test')

    @tornado.testing.gen_test
    def test_aquilav2_unknown_model(self):
        features = np.random.randn(1024)
        
        response = AquilaResponse()
        response.valence.extend(features)
        response.model_version = '20160707-whoami'
        self.mock_regress_call.side_effect = [response]

        with self.assertLogExists(logging.ERROR, 'valid model'):
            score, vec, vers = yield self.predictor.predict(self.image,
                                                            async=True)

        self.assertIsNone(score)
        numpy.testing.assert_allclose(features, vec)
        self.assertEquals(vers, '20160707-whoami')

    @tornado.testing.gen_test
    def test_aquilav2_unknown_demographic(self):
        features = np.random.randn(1024)
        
        response = AquilaResponse()
        response.valence.extend(features)
        response.model_version = '20160707-test'
        self.mock_regress_call.side_effect = [response]
        self.predictor.gender = 'alien'

        with self.assertLogExists(logging.WARNING, 'Unknown demographic'):
            score, vec, vers = yield self.predictor.predict(self.image,
                                                            async=True)

        self.assertIsNone(score)
        numpy.testing.assert_allclose(features, vec)
        self.assertEquals(vers, '20160707-test')

    @tornado.testing.gen_test
    def test_aquilav2_different_demographics(self):
        features = np.random.randn(1024)

        response = AquilaResponse()
        response.valence.extend(features)
        response.model_version = '20160707-test'
        self.mock_regress_call.return_value = response

        score1, vec1, vers1 = yield self.predictor.predict(self.image,
                                                           async=True)

        self.predictor.gender = 'M'
        self.predictor.age = '18-19'

        score2, vec2, vers2 = yield self.predictor.predict(self.image,
                                                           async=True)

        self.assertNotEquals(score1, score2)
        numpy.testing.assert_allclose(vec2, vec1)
        self.assertEquals(vers1, vers2)

    @tornado.testing.gen_test
    def test_shutting_down(self):

        self.predictor.shutdown()

        with self.assertRaises(model.errors.PredictionError) as e:
            yield self.predictor.predict(self.image, base_time=0.0, async=True)

        self.assertEquals(e.exception.message, 'Object is shutting down.')
        self.assertEquals(self.predictor.active, 0)

    @tornado.testing.gen_test
    def test_rpc_error(self):
        self.mock_regress_call.side_effect = [IOError('Oops connection bad')]

        with self.assertLogExists(logging.ERROR, 'RPC Error:'):
            with self.assertRaises(model.errors.PredictionError) as e:
                yield self.predictor.predict(self.image, base_time=0.0,
                                             async=True)

        self.assertEquals(self.predictor.active, 0)
        
    # TODO(Nick): Add more tests

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
