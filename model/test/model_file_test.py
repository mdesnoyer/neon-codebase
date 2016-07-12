#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import os
import pandas as pd
import test_utils.neontest
import unittest
import utils.neon

_log = logging.getLogger(__name__)

class TestAllModelFiles(test_utils.neontest.TestCase):
    '''Verifies that all the model files in model/demographics are of the correct format.'''

    def _get_model_dir(self):
        return os.path.join(os.path.dirname(__file__), '..', 'demographics')

    def test_filenames(self):
        # Checks that all the files are of the form YYYYMMDD_<tag>.pkl
        for fn in os.listdir(self._get_model_dir()):
            self.assertRegexpMatches(fn, '20[0-9]{6}_[a-zA-Z0-9]+\.pkl')

    def test_pandas_array(self):
        model_dir = self._get_model_dir()
        for fn in os.listdir(model_dir):
            _log.info('Checking %s' % fn)
            mat = pd.read_pickle(os.path.join(model_dir, fn))

            self.assertEquals(mat.columns.names, ['gender', 'age'])

            # Makes sure that we get a vector when indexing by gender and age
            self.assertEquals(mat['M']['None'].shape, (1024,))

            # Makes sure that there are None (i.e. generic) entries
            mat['None']['None']

            # Makes sure that each vector is normalized to 1
            self.assertTrue(((mat.sum() - 1.0).abs() < 1e-10).all())

            with self.assertRaises(KeyError):
                mat['Alien']

            with self.assertRaises(KeyError):
                mat['M']['babies']

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
