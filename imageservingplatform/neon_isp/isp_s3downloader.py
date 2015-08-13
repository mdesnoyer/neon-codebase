#!/usr/bin/env python

'''
Download the Serving directive file from S3 

NOTE: The s3 credentials come from IAM role

'''

import boto.s3.connection
import hashlib
import gzip
import re
import sys
from boto.s3.key import Key
from boto.exception import S3ResponseError
from optparse import OptionParser
import os
import os.path
from StringIO import StringIO

def main(options):
    s3re = re.compile("^s3://([^/]+)/?(.*)", re.IGNORECASE) 
    match = s3re.match(options.s3URL)
    if not match:
        print "Not a valid S3 URL" 
        sys.exit(1)

    bucket_name, basename = match.groups() 
    if not isinstance(basename, basestring):
        print "Not a valid basename" 
        sys.exit(1)

    # Download destination
    destination = options.destination

    args = []
    kwargs = {}
    if options.s3host and options.s3port:
        kwargs['aws_access_key_id'] = 'dummy'
        kwargs['aws_secret_access_key'] = 'dummy' 
        kwargs['is_secure'] = False
        kwargs['calling_format'] = boto.s3.connection.OrdinaryCallingFormat()
        kwargs['port'] = int(options.s3port)
        kwargs['host'] = options.s3host
    
    conn = boto.s3.connection.S3Connection(*args, **kwargs)
    bucket = conn.get_bucket(bucket_name)
    k = Key(bucket)
    k.name = basename

    try:
        # This overwrites the destination file
        gzip_fn = '%s.gz' % destination
        print 'Downloading %s' % options.s3URL
        if os.path.exists(gzip_fn):
            os.remove(gzip_fn)
        def _print_status(rec, tot):
            print 'Received %i of %i bytes' % (rec, tot)
        k.get_contents_to_filename(
            gzip_fn,
            cb=_print_status)
        try:
            ctype = k.content_type
            if ctype and ctype == "application/x-gzip":
                print 'Opening gzip file %s' % gzip_fn
                gz = gzip.GzipFile(gzip_fn, mode='rb')
                with open(destination, 'w') as f:
                    f.writelines(gz)
            else:
                with open(destination, 'w') as f:
                    try:
                        gz = gzip.GzipFile(gzip_fn, mode='rb')
                        f.writelines(gz)
                    except IOError, e:
                        with open(gzip_fn) as in_stream:
                            f.writelines(in_stream)
        finally:
            os.remove(gzip_fn)

        # TODO (Sunil/Pierre) : test and refactor md5 check
        #downloaded_md5 = hashlib.md5(open(destination).read()).hexdigest()
        #if downloaded_md5 != k.md5:
        #    print "Error MD5 mismatch of the s3file and downloaded file"

        print 'Successfully downloaded mastermind file'
    except Exception, e:
        #TODO: more friendly exception messages
        print "Error downloading or writing the file %s" % e
        sys.exit(1)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-u", "--s3url", dest="s3URL",
                        default="s3://neon-image-serving-directives/mastermind",
                        help="s3location")

    parser.add_option("-d", "--dest", dest="destination",
                        default="/tmp/mastermind",
                        help="write to FILE")

    # Test configurations
    parser.add_option("-s", "--host", dest="s3host", default=None,
                        help="host ip")
    
    parser.add_option("-p", "--port", dest="s3port", default=None,
                        help="port")

    (options, pargs) = parser.parse_args()

    main(options)

