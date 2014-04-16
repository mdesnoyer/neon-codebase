#!/usr/bin/env python
'''
Test S3 file
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
        sys.path.insert(0, base_path)

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import dateutil.parser
from mock import MagicMock, patch
import StringIO
import unittest
import test_utils.mock_boto_s3 as boto_mock
import utils.s3

class TestS3(unittest.TestCase):

    def setUp(self):
        self.test_string = "1241356236"
        self.output_bucket = "ob"
        self.conn = boto_mock.MockConnection()

    def test_set_contents_from_string(self):
        bucket = self.conn.create_bucket(self.output_bucket)
        for i in range(10):
            k = boto_mock.MockKey(bucket, "tkey-%s"%i) 
            ret = utils.s3.set_contents_from_string(k, self.test_string, {}, 3)
            self.assertEqual(ret, len(self.test_string))

        for key in self.conn.buckets[self.output_bucket].get_all_keys():
            self.assertEqual(self.test_string, key.get_contents_as_string())

    def test_retries(self):
        '''
        Test errors with set_contents_from_string
        '''

        #retries 3 times to save and fails
        mock_key = MagicMock()
        mock_bucket = MagicMock()
        self.conn.buckets[self.output_bucket] = mock_bucket
        mock_bucket.new_key.return_value = mock_key
        s3RespError = boto.exception.S3ResponseError(500, 'An error') 
        mock_key.set_contents_from_string.side_effect = s3RespError 
        ret = utils.s3.set_contents_from_string(mock_key, self.test_string, {}, 3)
        self.assertIsNone(ret)
        self.assertEqual(mock_key.set_contents_from_string.call_count, 3) 
        
        #Induce failure on first try
        mock_key = MagicMock()
        mock_bucket.new_key.return_value = mock_key
        mock_key.set_contents_from_string.side_effect = \
                                        [s3RespError, len(self.test_string)] 
        ret = utils.s3.set_contents_from_string(mock_key, self.test_string, {}, 3)
        self.assertEqual(ret, len(self.test_string))
        self.assertEqual(mock_key.set_contents_from_string.call_count, 2) 

if __name__ == '__main__':
    unittest.main()
