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

import fake_filesystem
import os
from StringIO import StringIO
import options_test_module as test_mod
import unittest
import utils.options

class TestCommandLineParsing(unittest.TestCase):
    def setUp(self):
        self.parser = utils.options.OptionParser()
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.open_func = sys.modules['__builtin__'].open
        sys.modules['__builtin__'].open = \
          fake_filesystem.FakeFileOpen(self.filesystem)

    def tearDown(self):
        sys.modules['__builtin__'].open = self.open_func
        
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

    def test_incorrect_type_definition(self):
        self.assertRaises(TypeError, self.parser.define, 'an_int',
                          type='int', default=6)

    def test_bool_unsupported(self):
        self.assertRaises(TypeError, self.parser.define, 'abool',
                          type=bool, default=True)

    def test_main_config_variables(self):
        '''Testing variables defined in __main__ that are set in the config file.'''
        self.parser.define('a_float', default=6.5, type=float)
        self.parser.define('an_implied_string', default='here')
        self.parser.define('a_string', default='now_here', type=str)
        self.parser.define('an_int', default=6, type=int)

        config_stream = StringIO('a_float: 10.8\n'
                                 'an_implied_string: world\n'
                                 'a_string: multi word\n'
                                 'an_int: 3')
        self.parser.parse_options(['--an_implied_string', 'monkey'],
                                  config_stream=config_stream)

        self.assertEqual(self.parser.a_float, 10.8)
        self.assertEqual(self.parser.an_implied_string, 'monkey')
        self.assertEqual(self.parser.a_string, 'multi word')
        self.assertEqual(self.parser.an_int, 3)

    def test_module_namespace_config_stream(self):
        test_mod.define(self.parser, 'an_int', default=6, type=int)

        config_stream = StringIO('utils:\n'
                                 '  test:\n'
                                 '    options_test_module:\n'
                                 '      an_int: 10')

        self.parser.parse_options([], config_stream)

        self.assertEqual(test_mod.get(self.parser, 'an_int'), 10)

    def test_config_stream_single_space(self):
        test_mod.define(self.parser, 'an_int', default=6, type=int)

        config_stream = StringIO('utils:\n'
                                 ' test:\n'
                                 '  options_test_module:\n'
                                 '   an_int: 10')

        self.parser.parse_options([], config_stream)

        self.assertEqual(test_mod.get(self.parser, 'an_int'), 10)

    def test_finding_config_file(self):
        self.parser.define('a_float', default=6.5, type=float)

        config_file = self.filesystem.CreateFile(
            'my_config.yaml', 
            contents='a_float: 10.8')

        self.parser.parse_options(['--config', 'my_config.yaml'])

        self.assertEqual(self.parser.a_float, 10.8)
        

if __name__ == '__main__':
    unittest.main()
