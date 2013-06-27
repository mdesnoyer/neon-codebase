#!/bin/python
import properties
import sys
import os

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

download_directory = '/Users/sunilmallya/s3download' 
try:
    api_key = sys.argv[1]
    job_key = sys.argv[2]

    download_directory = '/Users/sunilmallya/s3download' + '/' + api_key 
    if os.path.exists(download_directory) == False:
        os.mkdir(download_directory)

    dir = download_directory + "/" + job_key
    if os.path.exists(dir) == False:
        os.mkdir(dir)
except:
    print "./download <api key> <job id>"
    exit(0)

s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
s3bucket_name = properties.S3_BUCKET_NAME
s3bucket = Bucket(name = s3bucket_name, connection = s3conn)
k = Key(s3bucket)
bucket = s3conn.lookup(s3bucket_name)

files_to_download = ['request.txt','response.txt','result.tar.gz','video_metadata.txt']

for file in files_to_download:
    try:
        fname = job_key + "/" + file 
        k.key = api_key + "/" + fname
        download_fname = download_directory + "/" + fname
        if not os.path.exists(download_fname):
            k.get_contents_to_filename(download_fname)
    except Exception,e:
        print e

#download latest file
l = [ k for k in bucket if api_key in k]
print l 
#key_to_download = sorted(l, cmp=lambda x,y: cmp(x[0], y[0]))[-1][1]
#key_to_download.get_contents_to_filename('myfile')
