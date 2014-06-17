#!/usr/bin/env python
'''
 This script compiles a avdl schema definition into the JSON avsc
 file needed by Avro. We only keep the avsc file for the record that
 is the same name as the avdl filename. e.g. In the
 TrackerEvent.avdl, we will only grab the record called TrackerEvent.

 Also, uploads the schema to s3 with the name <md5 hash>.avsc

 You should never edit the avsc files directly and instead use this
 script to make them.

 Author: Mark Desnoyer (desnoyer@neon-labs.com)
 Copyright 2014 Neon Labs Inc.
'''
import avro.io
import avro.schema
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import hashlib
import logging
from optparse import OptionParser
import os
import os.path
import shutil
import subprocess
import tempfile

_log = logging.getLogger(__name__)

def check_schemas(old_schema_path, new_schema_path):
    '''Makes sure that the new schema is compatible with one already there.'''
    if not os.path.exists(old_schema_path):
        # It's a new schema so we're done
        return

    with open(old_schema_path) as f:
        old_schema = avro.schema.parse(f.read())

    with open(new_schema_path) as f:
        new_schema = avro.schema.parse(f.read())

    if not avro.io.DatumReader.match_schemas(old_schema, new_schema):
        raise avro.io.SchemaResolutionException(
            'New schema is not compatible.', old_schema, new_schema)

def main(options):
    if options.input is None:
        raise Exception("Must specify an input file")
    avsc_file = os.path.basename(options.input).split('.')[0] + '.avsc'
    
    output_dir = options.output
    if options.output is None:
        output_dir = os.path.dirname(options.input)
    
    # Create a temporary directory used to extract the avsc files to
    tempdir = tempfile.mkdtemp()
    try:

        # Compile the avdl file
        exec_params = [ "/usr/bin/java", "-jar", options.avro_tools, 
                        "idl2schemata", options.input, tempdir]
        retcode = subprocess.call(exec_params)

        if retcode != 0:
            raise Exception('Error running %s' % ' '.join(exec_params))

        check_schemas(os.path.join(output_dir, avsc_file),
                      os.path.join(tempdir, avsc_file),)

        # Move the avsc file to the output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        shutil.copy(os.path.join(tempdir, avsc_file), output_dir)     
        _log.info('%s written' % os.path.join(output_dir, avsc_file))
    finally:
        shutil.rmtree(tempdir)

    # Get the schema
    with open(os.path.join(output_dir, avsc_file)) as f:
        schema_str = f.read()
        schema_hash = hashlib.md5(schema_str).hexdigest()

    # Now upload the schema file to S3
    s3conn = S3Connection()
    bucket = s3conn.lookup(options.s3_bucket)

    _log.info('Uploading schema to S3 s3://%s/%s' % 
              (options.s3_bucket, '%s.avsc' % schema_hash))
    key = Key(bucket=bucket, name='%s.avsc' % schema_hash)
    key.content_type = 'application/json'
    key.set_contents_from_string(schema_str, replace=False,
                                 policy='public-read')

if __name__ == "__main__":

    parser = OptionParser()

    parser.add_option('-i', '--input', default=None,
                      help='avdl file to compile')
    parser.add_option('-o', '--output', default=None,
                      help=('Output directory. Defaults to the same '
                            'directory as the input file'))
    parser.add_option('--s3_bucket', default='neon-avro-schema',
                      help='S3 Bucket')
    parser.add_option('--avro_tools', default='/usr/lib/avro/avro-tools.jar',
                      help='Path to the avro-tools jar')
    
    options,args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(options)
