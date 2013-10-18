#!/usr/bin/env python
'''
Unittests for the options module

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import os.path
import sys
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')))

import unittest
import utils.options
import options_test_module as test_mod

class TestCommandLineParsing(unittest.TestCase):
    def setUp(self):
        self.parser = utils.options.OptionParser()

    def test_module_namespace_hiding(self):
        test_mod.define(self.parser, 'an_int', default=6, type=int)

        self.parser.parse_options(['--utils.test.options_test_module.an_int',
                                   '10'])

        self.assertEqual(test_mod.get(self.parser, 'an_int'), 10)

    def test_main_namespace(self):
        self.parser.define('an_int', default=6, type=int)

        self.parser.parse_options(['--an_int', '10'])

        self.assertEqual(self.parser.an_int, 10)

    def test_different_types(self):
        self.parser.define('a_float', default=6.5, type=float)
        self.parser.define('an_implied_string', default='here')
        self.parser.define('a_string', default='now_here', type=str)

        self.parser.parse_options(['--a_float', '10.8',
                                   '--an_implied_string', 'there',
                                   '--a_string', 'everywhere'])

        self.assertEqual(self.parser.a_float, 10.8)
        self.assertEqual(self.parser.an_implied_string, 'there')
        self.assertEqual(self.parser.a_string, 'everywhere')

if __name__ == '__main__':
    unittest.main()
