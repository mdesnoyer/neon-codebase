#!/usr/bin/env python
'''
Test S3 Log aggregator
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
import gzip
from mock import MagicMock, patch
import StringIO
import unittest
import test_utils.mock_boto_s3 as boto_mock
import utils.s3logaggregator

class TestLogAggregator(unittest.TestCase):

    def setUp(self):
        self.test_string = "1241356236"

    @patch('utils.s3logaggregator.S3Connection')
    def test_s3_gzip_aggregation(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn
        input_bucket = "input_bucket"
        output_bucket = "output_bucket"
        conn.create_bucket(input_bucket)
        conn.create_bucket(output_bucket)
        
        #Insert test data
        s3bucket = conn.get_bucket(input_bucket)
        for i in range(10):
            k = s3bucket.new_key("tkey-%s"%i)  
            k.set_contents_from_string(self.test_string)

        utils.s3logaggregator.main(input_bucket, output_bucket, 5, None, None)
        for key in conn.buckets[output_bucket].get_all_keys():
            gzip_output = key.get_contents_as_string()
            gz = gzip.GzipFile(fileobj=StringIO.StringIO(gzip_output), mode='rb')
            self.assertEqual(gz.read(), "\n".join([self.test_string]*5)+"\n")

    @patch('utils.s3logaggregator.S3Connection')
    def test_datewise_aggregation(self, mock_conntype):
        conn = boto_mock.MockConnection()
        mock_conntype.return_value = conn
        input_bucket = "input_bucket"
        output_bucket = "output_bucket"
        conn.create_bucket(input_bucket)
        conn.create_bucket(output_bucket)
       
        N = 10
        #Insert test data
        s3bucket = conn.get_bucket(input_bucket)
        for i in range(N):
            k = s3bucket.new_key("tkey-%s"%i)  
            k.set_contents_from_string(self.test_string)
  
        #Data beyond a certain date
        for i in range (N):
            k = s3bucket.new_key("delayed_key%s" %i)
            k.last_modified = 'Wed, 06 Oct 2011 05:11:54 GMT'
            k.set_contents_from_string("ENDEND")

        s_date = dateutil.parser.parse(
                        'Wed, 04 Oct 2010 05:11:54 GMT').replace(tzinfo=None)
        e_date = dateutil.parser.parse(
                        'Wed, 04 Oct 2011 05:11:54 GMT').replace(tzinfo=None)

        utils.s3logaggregator.main(input_bucket, output_bucket, N, s_date, e_date)
        for key in conn.buckets[output_bucket].get_all_keys():
            gzip_output = key.get_contents_as_string()
            gz = gzip.GzipFile(fileobj=StringIO.StringIO(gzip_output), mode='rb')
            self.assertEqual(gz.read(), "\n".join([self.test_string]*N)+"\n")

if __name__ == '__main__':
    unittest.main()
