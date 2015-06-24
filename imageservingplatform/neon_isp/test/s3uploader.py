import boto.s3.connection
import hashlib
import gzip
import re
import sys
from boto.s3.key import Key
from boto.exception import S3ResponseError
from optparse import OptionParser
from StringIO import StringIO

port = 11111
host = 'localhost'
fname = 'kfmastermind.api.test'

def main():
   args = []
   kwargs = {}
   kwargs['aws_access_key_id'] = 'dummy'
   kwargs['aws_secret_access_key'] = 'dummy' 
   kwargs['is_secure'] = False
   kwargs['calling_format'] = boto.s3.connection.OrdinaryCallingFormat()
   kwargs['port'] = port 
   kwargs['host'] = host 
   
   conn = boto.s3.connection.S3Connection(*args, **kwargs)
   bucket = conn.create_bucket('neon-test')
   k = Key(bucket)
   k.name = 'mastermind.api.test' 
   policy = 'public-read'

   with open(fname, 'r') as f:
       data = f.read().strip()

   s3data = StringIO()
   s3data.write(data)
   s3data.seek(0)

   try:
       k.set_contents_from_file(s3data, policy=policy)
   except Exception, e:
       #TODO: More specific exceptions
       print "Error writing the file", e

main()
