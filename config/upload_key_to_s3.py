#!/usr/bin/env python
'''
Script that uploads an ssh key to S3 securely. The key is stored in an encrypted format server side so that random disk dumps can't read it.

Author: Mark Desnoyer (desnoyer@neon-labs.com)
Copyright: Neon Labs 2014
'''
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging
from optparse import OptionParser
import os.path

_log = logging.getLogger(__name__)

def main(options):
    s3conn = S3Connection()

    if options.private_file is None or options.public_file is None:
        _log.fatal('Sorry, both the public and private key must be uploaded at once.')
        return
    if not os.path.exists(options.private_file):
        raise IOError('%s does not exist' % options.private_file)
    if not os.path.exists(options.public_file):
        raise IOError('%s does not exist' % options.public_file)
    key_name = (options.key_name or 
                os.path.basename(options.public_file).partition('.')[0])

    _log.info('Uploading to bucket %s' % options.s3_bucket)
    bucket = s3conn.lookup(options.s3_bucket)
    if bucket is None:
        bucket = s3conn.create_bucket(options.s3_bucket)

    _log.info('Uploading key %s' % key_name)
    pubKey = Key(bucket=bucket, name='%s.pub' % key_name)
    privateKey = Key(bucket=bucket, name='%s.pem' % key_name)
    if pubKey.exists() or privateKey.exists():
        _log.fatal('Key %s already exists on the server. Aborting' % key_name)
        return
    pubKey.set_contents_from_filename(options.public_file,
                                      encrypt_key=True,
                                      policy='private')
    privateKey.set_contents_from_filename(options.private_file,
                                          encrypt_key=True,
                                          policy='private')
    
        

if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option('--private_file', default=None,
                      help='Input private key file')
    parser.add_option('--public_file', default=None,
                      help='Input public key file')
    parser.add_option('--key_name', default=None,
                      help=('Name for the key in S3. Defaults to the '
                            'basename of the public file without the suffix.'))
    parser.add_option('--s3_bucket', default='neon-keys',
                      help='S3 Bucket')
    
    options,args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(options)
