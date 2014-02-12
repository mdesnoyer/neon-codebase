#!/usr/bin/env python
'''
Test functionality of the click log server.
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import unittest
from utils.options import options

class TestConfigFiles(unittest.TestCase):
    '''
    Test the set of config files are valid YAML files 
    '''
    def setUp(self):
        self.config_files = ["config/neon-local.conf", "config/neon-prod.conf"]

    def tearDown(self):
        pass

    def test_parsing_config_files(self):
        for cf in self.config_files:
            with open(__base_path__ + "/" + cf, "r") as stream:
                try:
                    options._parse_config_file(stream)
                except Exception, e:
                    #invalid yaml file
                    self.assertEqual(1, 2)


if __name__ == '__main__':
    unittest.main()
