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

import boto
import boto.exception
import fake_filesystem
import mock
from mock import patch
import os
from StringIO import StringIO
import options_test_module as test_mod
import time
import unittest
import utils.options

class FileStringIO(StringIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False

class TestCommandLineParsing(unittest.TestCase):
    def setUp(self):
        self.parser = utils.options.OptionParser()
        self.filesystem = fake_filesystem.FakeFilesystem()
        self.open_func = sys.modules['__builtin__'].open
        sys.modules['__builtin__'].open = \
          fake_filesystem.FakeFileOpen(self.filesystem)

    def tearDown(self):
        sys.modules['__builtin__'].open = self.open_func

    def test_define_twice(self):
        self.parser.define('an_int', default=6, type=int,
                           help='help me')

        # Repeating the identical definition again is ok
        self.parser.define('an_int', default=6, type=int,
                           help='help me')

        with self.assertRaises(utils.options.Error):
            self.parser.define('an_int', default=3, type=int,
                               help='help me')
        
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

    def test_unknown_variable_in_config(self):
        '''Testing variables defined in __main__ that are set in the config file.'''
        self.parser.define('a_float', default=6.5, type=float)

        config_stream = StringIO('a_floaty: 10.8')
        self.parser.parse_options(config_stream = config_stream)

        # TODO(mdesnoyer): Test that a warning was logged
        self.assertEqual(self.parser.a_float, 6.5)

    def test_bad_type_in_config(self):
        '''Testing variables defined in __main__ that are set in the config file.'''
        self.parser.define('a_float', default=6.5, type=float)

        config_stream = StringIO('a_float: power')
        self.assertRaises(TypeError,
                          self.parser.parse_options, [],
                          config_stream=config_stream)

    def test_unknown_var_in_config(self):
        self.parser.define('a_float', default=6.5, type=float)

        config_stream = StringIO('a_float2: 3.0')
        self.parser.parse_options(config_stream = config_stream)

        self.assertEqual(self.parser.a_float, 6.5)

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


    @patch('utils.options.os.path.getmtime', return_value=int(time.time()))
    def test_finding_config_file(self, mtime_mock):
        self.parser.define('a_float', default=6.5, type=float)

        config_file = self.filesystem.CreateFile(
            'my_config.yaml', 
            contents='a_float: 10.8')

        self.parser.parse_options(['--config', 'my_config.yaml'])

        self.assertEqual(self.parser.a_float, 10.8)

    def test_changing_config_file(self):
        '''Test when the config file changes. We want to update the options.'''
        self.parser.define('a_float', default=6.5, type=float)
        self.parser.define('an_int', default=3, type=int)
        self.parser.define('a_string', default='cow')

        config_file = self.filesystem.CreateFile(
            'my_config.yaml', 
            contents='a_float: 10.8\nan_int: 6')

        with patch('utils.options.os.path.getmtime',
                   return_value=int(time.time())) :
            self.parser.parse_options(['--config', 'my_config.yaml',
                                       '--an_int', '10'])
        
        self.assertEqual(self.parser.a_float, 10.8)
        self.assertEqual(self.parser.an_int, 10)
        self.assertEqual(self.parser.a_string, 'cow')

        # Simulate a file change
        config_file.SetContents('a_float: 20.9\nan_int: 1')

        with patch('utils.options.os.path.getmtime',
                   return_value=int(time.time() + 10)):
            self.parser._process_new_config_file(path='my_config.yaml')

        # the int shouldn't change because the command line has precendence
        self.assertEqual(self.parser.a_float, 20.9)
        self.assertEqual(self.parser.an_int, 10)
        self.assertEqual(self.parser.a_string, 'cow')

class TestS3ConfigFiles(unittest.TestCase):

    def setUp(self):
        self.parser = utils.options.OptionParser()

        self.mock_key = mock.MagicMock()
        self.mock_key.last_modified = '2012-03-13T03:54:07.000Z'
        self.mock_bucket = mock.MagicMock()
        self.mock_connection = mock.MagicMock()
        self.mock_connection.get_bucket = mock.MagicMock(
            return_value = self.mock_bucket)
        self.mock_bucket.get_key = mock.MagicMock(return_value=self.mock_key)
        
        self.connect_func = boto.connect_s3
        boto.connect_s3 = mock.MagicMock(return_value = self.mock_connection)

    def tearDown(self):
        boto.connect_s3 = self.connect_func

    def test_good_connection(self):
        self.parser.define('an_int', default=6, type=int)
        
        self.mock_key.open = mock.MagicMock(return_value=FileStringIO(
            'an_int: 8'))

        self.parser.parse_options(['--config', 
                                   's3://bucket.now/path/for/me/config.yaml'])

        self.assertEqual(self.parser.an_int, 8)
        self.mock_connection.get_bucket.assert_called_with('bucket.now')
        self.mock_bucket.get_key.assert_called_with('path/for/me/config.yaml')

    def test_changing_file_on_s3(self):
        self.parser.define('a_float', default=6.5, type=float)
        self.parser.define('an_int', default=6, type=int)
        self.parser.define('a_string', default='cow')

        self.mock_key.open = mock.MagicMock(return_value=FileStringIO(
            'an_int: 8\na_float: 10.1'))

        self.parser.parse_options([
            '--config', 's3://bucket.now/path/for/me/config.yaml',
            '--an_int', '1'])

        self.assertEqual(self.parser.an_int, 1)
        self.assertEqual(self.parser.a_float, 10.1)
        self.assertEqual(self.parser.a_string, 'cow')

        # Now simulate an update
        self.mock_key.open = mock.MagicMock(return_value=FileStringIO(
            'an_int: 7\na_float: 13.2'))
        self.mock_key.last_modified = '2012-03-19T03:54:07.000Z'

        self.parser._process_new_config_file(
            path='s3://bucket.now/path/for/me/config.yaml')

        self.assertEqual(self.parser.an_int, 1)
        self.assertEqual(self.parser.a_float, 13.2)
        self.assertEqual(self.parser.a_string, 'cow')

    def test_key_not_in_bucket(self):
        self.parser.define('an_int', default=6, type=int)
        self.mock_bucket.get_key.return_value = None

        self.assertRaises(KeyError,
                          self.parser.parse_options,
                          ['--config', 
                          's3://bucket.now/path/for/me/config.yaml'])

    def test_problem_getting_bucket(self):
        '''Make sure that errors are passed up.'''
        self.parser.define('an_int', default=6, type=int)
        self.mock_connection.get_bucket.side_effect = [
            boto.exception.S3ResponseError('status', 'reason'),
            IOError]

        self.assertRaises(boto.exception.S3ResponseError,
                          self.parser.parse_options,
                          ['--config', 
                          's3://bucket.now/path/for/me/config.yaml'])
        self.assertRaises(IOError,
                          self.parser.parse_options,
                          ['--config', 
                          's3://bucket.now/path/for/me/config.yaml'])
                                                                 
        

if __name__ == '__main__':
    unittest.main()
