#!/usr/bin/env python
'''
Unittests for the cmsapiv2 client

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cmsapiv2.client
import logging
from mock import MagicMock, patch
import test_utils.neontest
import unittest

class AuthTest(test_utils.neontest.AsyncTestCase):
    def setUp(self):
        super(AuthTest, self).setUp()

    def tearDown(self):
        super(AuthTest, self).tearDown()

if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
