#!/usr/bin/env python
''''
A script that merges all the avro files in a directory on s3.

This will do any merges on subdirectories from tree_root. We only
merge the directories that actually contain the .avro files (e.g. the
leaf directories)

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
define("tree_root", default="s3://neon-tracker-logs-v2/v2.2/",
       help="S3 path to do merges.")
define("temp_s3_dir", default="s3://neon-test/temp/",
       help="S3 path to put temporary files in")
define("backup_root", default="s3://neon-tracker-logs-v2/orig/v2.2/",
       help="Location to backup put the original avro files")

s3AddressRe = re.compile(r's3://([^/]+)/(\S+)')

__MAX_MEM_FILESIZE__ = 67108864

class S3Dir:
    def __init__(self, path):
        s3conn = S3Connection()
        bucket_name, self.dir_name = s3AddressRe.match(path).groups()
        self.bucket = s3conn.get_bucket(bucket_name)

        self._lock = multiprocessing.RLock()

        if not self.dir_name.endswith('/'):
            self.dir_name += '/'

    def get_key(self, filename, validate=True):
        '''Returns the boto Key object for the file in this directory.'''
        with self._lock:
            return self.bucket.get_key('%s%s' % (self.dir_name, filename),
                                       validate=validate)

    def subdirs(self):
        '''Get a list of all the (subdirectories, relative_path).

        The subdirectories are S3Dir objects and the relative path is a string
        '''
        with self._lock:
            retval = []
            for key in self.bucket.list(self.dir_name, '/'):
                if key.name.endswith('/'):
                    relpath = key.name[len(self.dir_name):]
                    retval.append((self.build_subdir(relpath), relpath))
            return retval

    def build_subdir(self, subname):
        '''Builds a S3Dir object for the subdirectory of dirname.'''
        return S3Dir('s3://%s/%s%s' % (self.bucket.name,
                                       self.dir_name,
                                       subname))

    def files(self):
        '''Get a list of all the files in the directory.

        The files are boto Key objects
        '''
        retval = []
        with self._lock:
            for key in self.bucket.list(self.dir_name, '/'):
                if not key.name.endswith('/'):
                    retval.append(key)
        return retval

    def name(self):
        return 's3://%s/%s' % (self.bucket.name, self.dir_name)

    def copy_key_into(self, key, new_name=None):
        '''Copies a key into this directory'''
        with self._lock:
            if new_name is None:
                new_name = os.path.basename(key.name)
            key.copy(self.bucket, '%s%s' % (self.dir_name, new_name))

_cpu_guard = multiprocessing.Semaphore(multiprocessing.cpu_count())
def merge_files_in_directory(root_dir, backup_dir, temp_dir):
    '''Merges any of the files in this directory.'''
    with _cpu_guard:
        try:
            _merge_files_in_directory_impl(root_dir, backup_dir, temp_dir)
        except Exception as e:
            _log.exception('Unexpected Error %s' % e)
    
def _merge_files_in_directory_impl(root_dir, backup_dir, temp_dir):
    to_merge = [x for x in root_dir.files() if x.name.endswith('.avro')]
    to_merge = sorted(to_merge, key=lambda x: x.name)
    if len(to_merge) > 1:
        _log.info('Merging all the files in %s' % root_dir.name())

        # Grab the schema of the last file
        with tempfile.SpooledTemporaryFile(__MAX_MEM_FILESIZE__) as cur_file:
            _log.info('Getting schema from %s' % to_merge[-1].name)
            to_merge[-1].get_contents_to_file(cur_file)
            cur_file.seek(0, 0)
            with closing(avro.datafile.DataFileReader(
                    cur_file, avro.io.DatumReader())) as reader:
                schema = avro.schema.parse(reader.get_meta('avro.schema'))

        merged_name = 'merged.%s' % os.path.basename(to_merge[0].name)

        # Open a temporary file to write to
        with tempfile.NamedTemporaryFile() as local_file:
            # Make a writer to append to the file
            writer = avro.datafile.DataFileWriter(
                local_file, avro.io.DatumWriter(), schema, 'snappy')
            
            # Stream the data from the avro files one at a time
            for small_avro in to_merge:
                _log.info('Reading data from %s. Size %f MB' % 
                          (small_avro.name, small_avro.size/(1024.*1024)))
                with tempfile.SpooledTemporaryFile(__MAX_MEM_FILESIZE__) \
                    as streamf:
                    small_avro.get_contents_to_file(streamf)
                    streamf.seek(0, 0)
                    with closing(avro.datafile.DataFileReader(
                            streamf, avro.io.DatumReader())) as reader:
                        for entry in reader:
                            writer.append(entry)

            # Now upload the merged file to a temporary location
            merged_key = temp_dir.get_key(os.path.basename(local_file.name),
                                          validate=False)
            local_file.seek(0, 0)
            _log.info('Uploading merged file to %s' % merged_key.name)
            merged_key.set_contents_from_file(local_file)

        # Move the split files to the backup location
        _log.info('Moving small files to %s' % backup_dir.name())
        for split_file in to_merge:
            backup_dir.copy_key_into(split_file)
            split_file.delete()

        # Move the merged file to the directory
        _log.info('Moving the merged file from %s to %s' % (merged_key.name,
                                                            root_dir.name()))
        root_dir.copy_key_into(merged_key, new_name=merged_name)
        merged_key.delete() 

_sub_procs = []

def merge_all_subdirectories(root_dir, backup_dir, temp_dir):
    _log.info('Entering %s' % root_dir.name())
    
    # First merge any files in this directory
    #merge_files_in_directory(root_dir, backup_dir, temp_dir)
    proc = multiprocessing.Process(
        target=merge_files_in_directory,
        args=(root_dir, backup_dir, temp_dir))
    proc.daemon = True
    proc.start()
    _sub_procs.append(proc)
        
    # Merge the subdirectories
    for subdir, rel_path in root_dir.subdirs():
        new_backup_dir = backup_dir.build_subdir(rel_path)
        merge_all_subdirectories(subdir, new_backup_dir, temp_dir)

def main():
    root_dir = S3Dir(options.tree_root)
    backup_dir = S3Dir(options.backup_root)
    temp_dir = S3Dir(options.temp_s3_dir)
    merge_all_subdirectories(root_dir, backup_dir, temp_dir)
    
    for proc in _sub_procs:
        proc.join()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
