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

from glob import glob

_log = logging.getLogger(__name__)

class TestAllModelFiles(test_utils.neontest.TestCase):
    '''Verifies that all the model files in model/demographics are of the correct format.'''

    def _get_model_dir(self):
        return os.path.join(os.path.dirname(__file__), '..', 'demographics')

    def _test_pandas_array(self, fn, exp_size):
        """All the files have a uniform format, but variable data size.
        This is a generic test for them, given a filename and a size.
        - fn is the filename
        - exp_size is the expected size of a complete index"""
        _log.info('Checking %s' % fn.split('/')[-1])
        mat = pd.read_pickle(fn)
        self.assertEquals(mat.columns.names, ['gender', 'age'])

        # Makes sure that we get a vector when indexing by gender and age
        self.assertEquals(mat['M']['None'].shape, exp_size)

        # Makes sure that there are None (i.e. generic) entries
        mat['None']['None']

        with self.assertRaises(KeyError):
            mat['Alien']

        with self.assertRaises(KeyError):
            mat['M']['babies']

    def test_filenames(self):
        # Checks that all the files are of the form YYYYMMDD-<tag>
        for fn in os.listdir(self._get_model_dir()):
            self.assertRegexpMatches(
                fn, '20[0-9]{6}-[a-zA-Z0-9]-[a-zA-Z0-9]+\.pkl')

    def test_pandas_array(self):
        model_dir = self._get_model_dir()
        mtype = 'weight.pkl'
        exp_size = (1024,)
        _log.info('Checking %s type files' % mtype)
        for fn in glob(os.path.join(model_dir, '*%s' % mtype)):
            self._test_pandas_array(fn, exp_size)

        mtype = 'score.pkl'
        exp_size = (100,)
        _log.info('Checking %s type files' % mtype)
        for fn in glob(os.path.join(model_dir, '*%s' % mtype)):
            self._test_pandas_array(fn, exp_size)

        mtype = 'bias.pkl'
        exp_size = (1,)
        _log.info('Checking %s type files' % mtype)
        for fn in glob(os.path.join(model_dir, '*%s' % mtype)):
            self._test_pandas_array(fn, exp_size)


if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
