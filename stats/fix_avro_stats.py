#!/usr/bin/env python
''''
A script that fixes an avro event file.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import avro.io
import avro.datafile
import avro.schema
from boto.s3.connection import S3Connection
import copy
import cStringIO as StringIO
from contextlib import closing
import multiprocessing
import re
import tempfile
import utils.neon

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("input",  default=None, help="File to fix")
define("local_temp_dir", default="tmp",
       help="Prefix for where to store temporary files.")

s3AddressRe = re.compile(r's3://([^/]+)/(\S+)')

__MAX_MEM_FILESIZE__ = 67108864

def main():
    local_temp_prefix = options.local_temp_dir
    if local_temp_prefix != "tmp":
        local_temp_prefix += '/'
    
    bucket_name, key_name = s3AddressRe.match(options.input).groups()

    s3conn = S3Connection()
    bucket = s3conn.get_bucket(bucket_name)
    key = bucket.get_key(key_name, validate=True)

    with tempfile.SpooledTemporaryFile(__MAX_MEM_FILESIZE__,
                                       prefix=local_temp_prefix) as streamf:
        _log.info('Downloading %s' % key.name)
        key.get_contents_to_file(streamf)

        streamf.seek(0,0)

        # Open the file for reading
        with closing(avro.datafile.DataFileReader(
                        streamf, avro.io.DatumReader())) as reader:
            schema = avro.schema.parse(reader.get_meta('avro.schema'))

            # Open a temporary file to write to
            with tempfile.NamedTemporaryFile(
                    prefix=local_temp_prefix) as local_file:
                # Make a writer to append to the file
                writer = avro.datafile.DataFileWriter(
                    local_file, avro.io.DatumWriter(), schema, 'snappy')

                # Write the entries out
                try:
                    n_entries = 0
                    for entry in reader:
                        writer.append(entry)
                        n_entries += 1
                except Exception as e:
                    _log.error('Found error after %i entries.' % n_entries)

                writer.flush()

                val = raw_input(
                    'Found %i entries. Press ENTER to upload, Ctrl-c'
                    % n_entries)

                # Upload the new file
                local_file.seek(0, 0)
                _log.info('Uploading fixed file to %s' % key.name)
                key.set_contents_from_file(local_file)
        
    
if __name__ == '__main__':
    utils.neon.InitNeon()
    main()
