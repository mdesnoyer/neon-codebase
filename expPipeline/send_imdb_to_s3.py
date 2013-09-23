#!/usr/bin/env python
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import csv
import glob
import logging
import os
import os.path
from optparse import OptionParser

_log = logging.getLogger(__name__)

def IterateImdb(dbFile):
    '''Yields (image_id, video_url, frame_no, aspect)'''
    with open(dbFile) as f:
        reader = csv.reader(dbFile, delimiter=' ')
        for row in reader:
            yield (row[0], row[1], int(row[2]), float(row[3]))

def UploadImage(bucket, directory, imageFile):
    key = Key(bucket=bucket, name=imageFile, content_type='image/jpeg')
    key.set_contents_from_filename(os.path.join(directory, imageFile),
                                   replace=False)

def main(options):
    s3conn = S3Connection()

    _log.info('Uploading to bucket %s' % options.s3_bucket)
    bucket = s3conn.lookup(options.s3_bucket)
    if bucket is None:
        bucket = s3conn.create_bucket(options.s3_bucket)

    nImages = 0
    for imData in IterateImdb(os.path.join(options.input, 'image.db')):
        if nImages % 1000 == 0:
            _log.info('Uploaded %i images' % nImages)
        UploadImage(bucket, os.path.join(options.input, 'images'),
                    '%s.jpg' % imData[0])

        nImages++

    _log.info('Transfering database file')
    dbKey = Key(bucket=bucket, name='image.db', content_type='text/plain')
    dbKey.set_contents_from_filename(os.path.join(options.input, 'image.db'),
                                     replace=True)

    _log.info('Transfering description of stimuli sets')
    stimsetDir = os.path.join(options.input, 'stimuli_set')
    for stimDir in os.listdir(stimsetDir):
        curDir = os.path.join(stimDir, stimsetDir)
        if not os.path.isdir(curDir):
            continue

        imageList = glob.glob('%s/*.jpg' % curDir)

        key = Key(bucket=bucket, name=('%s.txt' % stimsetDir),
                  content_type='text/plain')
        key.set_contents_from_string('\n'.join(imageList), replace=True)
            
    

if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option('-i', '--input', default=None,
                      help='Staging directory')
    parser.add_option('--s3_bucket', default='neon-image-library',
                      help='S3 Bucket')
    
    options,args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(options)
