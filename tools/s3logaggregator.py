#!/usr/bin/env python
'''
Aggregate smaller s3 logs in to bigger chunks

Supports a date range in UTC 
./s3logaggregator.py --input neon-tracker-logs --output neon-test --start_date 'Fri, 01 May 2014 00:00:00 GMT' --end_date 'Fri, 07 May 2014 12:00:00 GMT'

'''

USAGE='%prog input_bucket n_chunks'

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import dateutil.parser
import gzip
from optparse import OptionParser
import sys
import shortuuid
from StringIO import StringIO
import time

#n_chunks = 100 # no of keys to be merged. 
#input_bucket = 'neon-tracker-logs-test'
#output_bucket = 'neon-test'
s3key = 'AKIAJ5G2RZ6BDNBZ2VBA'
s3secret = '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'

def _generate_log_filename():
    ''' Generate log file name'''

    folder = time.strftime("%d%m%Y")
    return '%s/%s_%s_aggregate.log.gz' % (folder, 
            time.strftime('%S%M%H%d%m%Y', time.gmtime()),
            shortuuid.uuid())

def get_data(key):
    '''
    Get data from a S3 Key
    '''
    for i in range(3):
        try:
            data = key.get_contents_as_string()
            return data
        except:
            continue
        return ''    

def aggregate_and_save_data(keyset, output_bucket, s_date=None, e_date=None):
    ''' Aggregate data from keys and save the data in a new key'''

    aggr_data = ''
    gz_data = ''

    for key in keyset:
        #Remove timezone awareness, Assume UTC timestamp   
        key_modified_date = dateutil.parser.parse(key.last_modified).replace(tzinfo=None)
        if s_date and e_date:
            if key_modified_date <= s_date or key_modified_date >= e_date:
                continue

        data = get_data(key)
        if len(data) == 0:
            print key
        else: 
            data += "\n" #add newline to end of log
            gz_data += GzipStream().read(StringIO(data))

    if len(gz_data) > 0:
        #save to s3
        conn = S3Connection(s3key, s3secret)
        bucket = conn.get_bucket(output_bucket)
        keyname = _generate_log_filename()
        k = bucket.new_key(keyname)
        k.set_contents_from_string(gz_data,
                            headers={'Content-Type' : 'application/x-gzip'})

class GzipStream(StringIO):
    CHUNCK_SIZE = 65536

    def __init__(self, name="data"):
        StringIO.__init__(self)

        self.source_eof = False
        self.gz_buffer = ""
        self.zipfile = gzip.GzipFile(name, 'wb', 9, self)

    def write(self, data):
        self.gz_buffer += data

    def read(self, source, size = -1):
        while ((len(self.gz_buffer) < size) or (size == -1)) and not self.source_eof:
            if source is None: 
                break
            chunk = source.read(GzipStream.CHUNCK_SIZE)
            self.zipfile.write(chunk)
            if (len(chunk) < GzipStream.CHUNCK_SIZE):
                self.source_eof = True
                self.zipfile.flush()
                self.zipfile.close()
                break

        if size == 0:
            result = ""
        if size >= 1:
            result = self.gz_buffer[0:size]
            self.gz_buffer = self.gz_buffer[size:]
        else:
            result = self.gz_buffer
            self.gz_buffer = ""

        return result

def main(input_bucket, output_bucket, n_chunks, s_date, e_date):
    ''' Get keys from a bucket and do work '''

    conn = S3Connection(s3key, s3secret)
    bucket = conn.get_bucket(input_bucket)
    all_keys = []
    for key in bucket.list():
        all_keys.append(key)

    #Sort the keys based on last modified time 
    all_keys.sort(key=lambda x: x.last_modified)
    for i in range(len(all_keys)/ n_chunks):
        keyset = all_keys[i*n_chunks: (i+1)*n_chunks]
        aggregate_and_save_data(keyset, output_bucket, s_date, e_date)

    remainder = len(all_keys) % n_chunks 
    if remainder != 0:
        keyset = all_keys[ -1 * remainder : ]
        aggregate_and_save_data(keyset, output_bucket, s_date, e_date)

if __name__ == "__main__":
    parser = OptionParser(usage=USAGE)
    parser.add_option('--input', default=None,
                      help='Input s3 bucket')
    parser.add_option('--output', default=None,
                      help='Output bucket')
    parser.add_option('--nchunks', default=100, type='int',
                      help='Number of chunks to stitch')
    parser.add_option('--start_date', default=None,
                      help='Start date of the logs in UTC')
    parser.add_option('--end_date', default=None,
                      help='End date of the logs in UTC')
    options, args = parser.parse_args()
    #TODO: Checks
    s_date = dateutil.parser.parse(options.start_date).replace(tzinfo=None)
    e_date = dateutil.parser.parse(options.end_date).replace(tzinfo=None)

    main(options.input, options.output, options.nchunks, s_date, e_date) 
