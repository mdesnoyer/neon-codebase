#!/usr/bin/env python
'''
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
        sys.path.insert(0, base_path)

import gzip
from mock import MagicMock, patch
from StringIO import StringIO
import unittest
from utils import gzipstream 

class TestGzipStream(unittest.TestCase):

    def setUp(self):
        pass

    def test_gzip_stream(self):
        '''
        Compress the string using the class and test
        un-gziping
        '''
        data = "teststring" * 100
        gzip_output = gzipstream.GzipStream().read(StringIO(data))
        gz = gzip.GzipFile(fileobj=StringIO(gzip_output), mode='rb')
        self.assertEqual(data, gz.read())

    def test_large_gzip_stream(self):
        data = "teststring" * 500 * 100000 # 500MB
        gzip_output = gzipstream.GzipStream().read(StringIO(data))
        gz = gzip.GzipFile(fileobj=StringIO(gzip_output), mode='rb')
        self.assertEqual(data, gz.read())

    def test_empty_stream(self):
        data = ''
        gzip_output = gzipstream.GzipStream().read(StringIO(data))
        gz = gzip.GzipFile(fileobj=StringIO(gzip_output), mode='rb')
        self.assertEqual(data, gz.read())
    
if __name__ == '__main__':
    unittest.main()
